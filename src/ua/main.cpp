// Copyright (C) 2013-2016 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may
//   not use this file except in compliance with the License. You may obtain a
//   copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//   License for the specific language governing permissions and limitations
//   under the License.

#include <cstdint>
#include <iostream>
#include <queue>

#include <curl/curl.h>

#include <boost/thread.hpp>
#include <boost/filesystem.hpp>
#include <boost/version.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#ifdef WINDOWS_BUILD
#include <windows.h>
#include <psapi.h>
#else
#include <unistd.h>
#endif
#ifdef MAC_BUILD
#include <mach/mach.h>
#endif

#include "dxcpp/dxcpp.h"
#include "dxcpp/bqueue.h"
#include "api_helper.h"
#include "options.h"
#include "chunk.h"
#include "file.h"
#include "dxcpp/dxlog.h"
#include "import_apps.h"
#include "mime.h"
#include "round_robin_dns.h"
#include "common_utils.h"

// http://www.boost.org/doc/libs/1_48_0/libs/config/doc/html/boost_config/boost_macro_reference.html
#if ((BOOST_VERSION / 100000) < 1 || ((BOOST_VERSION/100000) == 1 && ((BOOST_VERSION / 100) % 1000) < 48))
  #error "Cannot compile Upload Agent using Boost version < 1.48"
#endif

#if !WINDOWS_BUILD
#if ((LIBCURL_VERSION_MAJOR < 7) || (LIBCURL_VERSION_MAJOR == 7 && LIBCURL_VERSION_MINOR < 31))
  #error "From UA v1.4.1 onwards, we expect to compile UA on libcurl v7.31+. If you need to override this behavior, edit main.cpp"
#endif
#endif


using namespace std;
using namespace dx;
namespace fs = boost::filesystem;

// Definition of forceRefresh global variables (used in round_robin_dns.cpp)
bool forceRefreshDNS = true;
boost::mutex forceRefreshDNSMutex;

int curlInit_call_count = 0;

Options opt;

/*
 * Mutex for bytesUploaded member of File class and bytesUploadedSinceStart
 * global variable.
 */
boost::mutex bytesUploadedMutex;

/* Keep track of total number of bytes uploaded since starting of the program */
int64_t bytesUploadedSinceStart = 0;

/* Keep track of time since program started (i.e., just before creating worker threads)*/
std::time_t startTime;

/* This variable is used as an additional mechanism for terminating
 * uploadProgressThread.*/
bool keepShowingUploadProgress = true;

/*
 * The Upload Agent operates as a collection of threads, operating on a set
 * of queues of Chunk objects.
 *
 * A Chunk represents a range of bytes within a file. Each file to be
 * uploaded is split into a set of chunks, each containing the name of the
 * local file; the ID of the file object being created; and the start and
 * end of the chunk within the file.
 *
 * Chunks are initially added to the queue chunksToRead.
 */

unsigned int totalChunks = 0;

BlockingQueue<Chunk*> chunksToRead;
BlockingQueue<Chunk*> chunksToCompress;
BlockingQueue<Chunk*> chunksToUpload;
BlockingQueue<Chunk*> chunksFinished;
BlockingQueue<Chunk*> chunksFailed;

vector<boost::thread> readThreads;
vector<boost::thread> compressThreads;
vector<boost::thread> uploadThreads;

int NUMTRIES_g; // Number of max tries for a chunk (to be given by user)

string userAgentString; // definition (declared in chunk.h)

// Max number of times to check if a chunk is complete.
int NUM_CHUNK_CHECKS = 3;

bool finished() {
  return (chunksFinished.size() + chunksFailed.size() == totalChunks);
}

void curlCleanup() {
  // http://curl.haxx.se/libcurl/c/curl_global_cleanup.html
  for (;curlInit_call_count > 0; --curlInit_call_count) {
    curl_global_cleanup();
  }
}

// The flag below is used to ensure that handle_bad_alloc() is called only once
// (since if program runs out of memory, all read/upload/compress threads call the
//  function simultaneously, and thus leading to interleaved "cerr" outputs)
boost::once_flag bad_alloc_once = BOOST_ONCE_INIT;
void handle_bad_alloc(const std::bad_alloc &e) {
  curlCleanup();
  DXLOG(logUSERINFO) << endl << "*********" << endl << "FATAL ERROR: The program ran out of memory. You may try following steps to avoid this problem: " << endl
    << "1. Try decreasing number of upload/compress/read threads (Try ./ua --help to see how to set them) - Recommended solution" << endl
    << "2. Reduce the chunk-size (--chunk-size options). Note: Trying with a different chunk size will not resume your previous upload" << endl
    << endl << "If you still face problem, please contact DNAnexus support." << endl
    << "\nError details (for advanced users only): '" << e.what() << "'" << endl << "*********" << endl;
  exit(1);
}

// General note:
// Assuming that actual upload stage is the bottleneck (which is mostly the case), memory
// consumption of UA will roughly be the following:
//
// Memory footprint = [#read-threads + 2 * (#compress-threads + #upload-threads)] * chunk-size
//
// The approach to manage the memory usage is done is these steps:
// 1. At the begining determine how much memeory is available in the system and set
//    a resident set size (RSS) limit to 80% of that.
// 2. Check the current RSS in the read threads before reading new data. If the RSS is larger
//    than the limit, let the thread sleep for 2 seconds initially, and back-off exponentially
//    up to a maximum of 16 seconds.

long getAvailableSystemMemory()
{
#ifdef WINDOWS_BUILD
  MEMORYSTATUSEX status;
  status.dwLength = sizeof(status);
  GlobalMemoryStatusEx(&status);
  return status.ullTotalPhys;
#elif MAC_BUILD
  task_basic_info_data_t info;
  mach_msg_type_number_t infoCount = TASK_BASIC_INFO_COUNT;
  if ( task_info( mach_task_self( ), TASK_BASIC_INFO, (task_info_t)&info, &infoCount ) != KERN_SUCCESS ) {
    return (long)0L;
  }
  return (long)info.virtual_size;
#else
  long pages = sysconf(_SC_AVPHYS_PAGES);
  long page_size = sysconf(_SC_PAGE_SIZE);
  return pages * page_size;
#endif
}

// rssLimit is the maximum amount of memory the program may use.
// It is set to be 80% of the system's free memory at program startup.
// If the limit is reached, the read threads are delayed
static long rssLimit = 0;

// A mutex used to prevent the multiple read thread to check memory at the same time
boost::mutex memCheckMutex;

void initializeRSSLimit() {
  long freeMemory = getAvailableSystemMemory();
  rssLimit = freeMemory*8/10;
  DXLOG(logINFO) << "Resident Set Size Limit (RSS): " << rssLimit;
}

// Get the current memory usage
long getRSS() {
#ifdef WINDOWS_BUILD
  PROCESS_MEMORY_COUNTERS info;
  GetProcessMemoryInfo( GetCurrentProcess( ), &info, sizeof(info) );
  DWORD err = GetLastError();
  if (err != 0 ) {
    DXLOG(logWARNING) << "Unable to get process' memory usage, error code " << err;
      return 0;
  }
  return (long)info.WorkingSetSize;
#elif MAC_BUILD
  task_basic_info_data_t info;
  mach_msg_type_number_t infoCount = TASK_BASIC_INFO_COUNT;
  if ( task_info( mach_task_self( ), TASK_BASIC_INFO, (task_info_t)&info, &infoCount ) != KERN_SUCCESS ) {
    DXLOG(logWARNING) << "Unable to get process' memory usage";
    return (long)0L;
  }
  return (long)info.resident_size;
#else
  ifstream statStream("/proc/self/statm",ios_base::in);
  if (!statStream.good()) {
    DXLOG(logWARNING) << "Unable to get process' memory usage";
    return 0;
  }

  long s = 0;
  long rss = 0;
  statStream >> s >> rss;
  statStream.close();
  return rss * sysconf(_SC_PAGE_SIZE);
#endif
}


bool isMemoryUseNormal() {
  // if RSS limit is not set, don't check memory usage
  if (rssLimit <= 0) {
    return true;
  }

  boost::mutex::scoped_lock lock(memCheckMutex);

  long residentSet = getRSS();
  long freeMemory = getAvailableSystemMemory();
  DXLOG(logDEBUG4) << "Free Memory: " << freeMemory << " rss " << residentSet ;

  if(freeMemory*8/10 > rssLimit) {
    rssLimit = freeMemory*8/10;
    DXLOG(logDEBUG4) << "New RSS Limit: " << rssLimit;
  }

  if (residentSet > rssLimit) {
    return false;
  }
  return true;
}

void readChunks() {
  try {
    int delay = 1;
    while (true) {
      // If the upload  is using a lot of memory delay the read thread for a bit.
      if (!isMemoryUseNormal()) {
	delay = min(delay*2, 16);	
	DXLOG(logWARNING) << "RSS larger than limit. Delaying read thread by " << delay << "secs";
	boost::this_thread::sleep(boost::posix_time::seconds(delay));
	continue;
      }
      delay = 1; //Reset the delay
      Chunk * c = chunksToRead.consume();

      c->log("Reading...");
      c->read();

      c->log("Finished reading");
      chunksToCompress.produce(c);

      // Sleep for tiny amount of time, to make sure we yield to other threads.
      // Note: boost::this_thread::yield() is not a valid interruption point,
      //       so we have to use sleep()
      boost::this_thread::sleep(boost::posix_time::microseconds(100));
    }
  } catch(std::bad_alloc &e) {
    boost::call_once(bad_alloc_once, boost::bind(&handle_bad_alloc, e));
  } catch (boost::thread_interrupted &ti) {
    return;
  }
}

void compressChunks() {
  try {
    while (true) {
      Chunk * c = chunksToCompress.consume();

      if (c->toCompress) {
        c->log("Compressing...");
        c->compress();
        c->log("Finished compressing");
      } else {
        c->log("Not compressing");
      }

      chunksToUpload.produce(c);

      // Sleep for tiny amount of time, to make sure we yield to other threads.
      // Note: boost::this_thread::yield() is not a valid interruption point,
      //       so we have to use sleep()
      boost::this_thread::sleep(boost::posix_time::microseconds(100));
    }
  } catch(std::bad_alloc &e) {
    boost::call_once(bad_alloc_once, boost::bind(&handle_bad_alloc, e));
  } catch (boost::thread_interrupted &ti) {
    return;
  }
}

bool is_chunk_complete(Chunk *c, JSON &fileDescription) {
    string partIndex = boost::lexical_cast<string>(c->index + 1); // minimum part index is 1

    return (fileDescription["parts"].has(partIndex) && fileDescription["parts"][partIndex]["state"].get<string>() == "complete");
}

void uploadChunks(vector<File> &files) {
  try {
    while (true) {
      Chunk * c = chunksToUpload.consume();

      c->log("Uploading...");

      bool uploaded = false;
      try {
        c->upload(opt);
        uploaded = true;
      } catch (runtime_error &e) {
        ostringstream msg;
        msg << "Upload failed: " << e.what();
        c->log(msg.str(), logERROR);
      }

      if (uploaded) {
        c->log("Upload succeeded!");
        int64_t size_of_chunk = c->data.size(); // this can be different than (c->end - c->start) because of compression
        c->clear();
        chunksFinished.produce(c);
        // Update number of bytes uploaded in parent file object
        boost::mutex::scoped_lock boLock(bytesUploadedMutex);
        files[c->parentFileIndex].bytesUploaded += (c->end - c->start);
        files[c->parentFileIndex].atleastOnePartDone = true;
        bytesUploadedSinceStart += size_of_chunk;
        boLock.unlock();
      } else if (c->triesLeft > 0) {
        int numTry = NUMTRIES_g - c->triesLeft + 1; // find out which try is it
        int timeout = (numTry > 6) ? 256 : 4 << numTry; // timeout is always between [8, 256] seconds
        c->log("Will retry reading and uploading this chunks in " + boost::lexical_cast<string>(timeout) + " seconds", logWARNING);
        if (!opt.noRoundRobinDNS) {
          boost::mutex::scoped_lock forceRefreshLock(forceRefreshDNSMutex);
          c->log("Setting forceRefreshDNS = true in main.cpp:uploadChunks()");
          forceRefreshDNS = true; // refresh the DNS list in next call to getRandomIP()
        }
        --(c->triesLeft);
        c->clear(); // we will read & compress data again
        boost::this_thread::sleep(boost::posix_time::milliseconds(timeout * 1000));
        // We push the chunk to retry to "chunksToRead" and not "chunksToUpload"
        // Since chunksToUpload queue is bounded, and chunksToUpload.produce() can block,
        // thus giving rise to deadlock
        chunksToRead.produce(c);
      } else {
        c->log("Not retrying", logERROR);
        // TODO: Should we print it on stderr or DXLOG (verbose only) ??
        DXLOG(logUSERINFO) << "Failed to upload Chunk [" << c->start << " - " << c->end << "] for local file ("
             << files[c->parentFileIndex].localFile << "). APIServer response for last try: '" << c->respData << "'" << endl;
        c->clear();
        chunksFailed.produce(c);
      }
      // Sleep for tiny amount of time, to make sure we yield to other threads.
      // Note: boost::this_thread::yield() is not a valid interruption point,
      //       so we have to use sleep()
      boost::this_thread::sleep(boost::posix_time::microseconds(100));
    }
  } catch(std::bad_alloc &e) {
    boost::call_once(bad_alloc_once, boost::bind(&handle_bad_alloc, e));
  } catch (boost::thread_interrupted &ti) {
    return;
  }
}

void monitor() {
  while (true) {
    boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
    {
      DXLOG(logINFO) << "[monitor]"
          << "  to read: " << chunksToRead.size()
          << "  to compress: " << chunksToCompress.size()
          << "  to upload: " << chunksToUpload.size()
          << "  finished: " << chunksFinished.size()
          << "  failed: " << chunksFailed.size();

      if (finished()) {
        return;
      }
    }
  }
}

bool fileDone(File &file) {
  if (file.failed)
    return true;
  if (!file.waitOnClose)
    return true;
  if (file.waitOnClose && file.closed)
    return true;
  return false;
}

bool allFilesDone(vector<File> &files) {
  for (unsigned int i = 0; i < files.size(); ++i) {
    if (!fileDone(files[i])) {
      return false;
    }
  }
  return true;
}

void updateFileState(vector<File> &files) {
  for (unsigned int i = 0; i < files.size(); ++i) {
    if (!files[i].failed) {
      files[i].updateState();
    }
  }
}

void waitOnClose(vector<File> &files) {
  do {
    boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
    updateFileState(files);
  } while (!allFilesDone(files));
}

void uploadProgressHelper(vector<File> &files) {
  // Print individual file progress
  std::ostringstream oss;
  boost::mutex::scoped_lock boLock(bytesUploadedMutex);
  for (unsigned i = 0; i < files.size(); ++i) {
    double percent = (files[i].size == 0 && files[i].atleastOnePartDone) ? 100.0 : 0.0;
    percent =  (files[i].size != 0) ? ((double(files[i].bytesUploaded) / files[i].size) * 100.0) : percent;

    oss << files[i].localFile << " " << setw(6) << setprecision(2) << std::fixed
         << percent << "% complete";
    if ((i + 1) != files.size()) {
      oss << ", ";
    }
  }
  DXLOG(logUSERINFO) << oss.str();

  // Print average transfer rate
  int64_t timediff  = std::time(0) - startTime;
  double mbps = (timediff > 0) ? (double(bytesUploadedSinceStart) / (1024.0 * 1024.0)) / timediff : 0.0;
  boLock.unlock();
  DXLOG(logUSERINFO) << " ... Average transfer speed = " << setw(6) << setprecision(2) << std::fixed << mbps << " MB/sec";

  // Print instantaneous transfer rate
  boost::mutex::scoped_lock queueLock(instantaneousBytesMutex);
  double mbps2 = 0.0;
  if (!instantaneousBytesAndTimestampQueue.empty()) {

    int64_t oldestElemTime = instantaneousBytesAndTimestampQueue.front().first;
    int64_t timediff2 = std::time(0) - oldestElemTime;
    if (timediff2 >= 90) {
      // If lastupdated time was older than 90seconds, we are lagging too behind to call it
      // "instantaneous", so clear the previous data and start fresh again.
      // Note: If this happens to often on a system, then we need to decrease MAX_QUEUE_SIZE in chunk.cpp
      queue<pair<time_t, int64_t> > empty;
      swap(instantaneousBytesAndTimestampQueue, empty);
      sumOfInstantaneousBytes = 0;
      mbps2 = 0.0;
    }
    // Note if timediff2 = 0 too often on some system, then we need to increase MAX_QUEUE_SIZE in chunk.cpp
    // because this implies that queue is filling very quickly (in less than second).
    if (timediff2 > 0) {
      mbps2 = (double(sumOfInstantaneousBytes) / (1024.0 * 1024.0)) / timediff2;
    }
  }
  queueLock.unlock();
  DXLOG(logUSERINFO) << " ... Instantaneous transfer speed = " << setw(6) << setprecision(2) << std::fixed << mbps2 << " MB/sec";

  if (opt.throttle >= 0) {
    DXLOG(logUSERINFO) << " (throttled to " << opt.throttle << " bytes/sec)";
  }
}

void uploadProgress(vector<File> &files) {
  try {
    do {
      uploadProgressHelper(files);
      boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
    } while (keepShowingUploadProgress);
    uploadProgressHelper(files);
    return;
  } catch (boost::thread_interrupted &ti) {
    // Call upload helper once at least, else message for "100%"
    // might not be displayed ever;
    uploadProgressHelper(files);
    return;
  }
}

void createWorkerThreads(vector<File> &files) {
  DXLOG(logINFO) << "Creating worker threads:";

  DXLOG(logINFO) << " read...";
  for (int i = 0; i < opt.readThreads; ++i) {
    readThreads.push_back(boost::thread(readChunks));
  }

  DXLOG(logINFO) << " compress...";
  for (int i = 0; i < opt.compressThreads; ++i) {
    compressThreads.push_back(boost::thread(compressChunks));
  }

  DXLOG(logINFO) << " upload...";
  for (int i = 0; i < opt.uploadThreads; ++i) {
    uploadThreads.push_back(boost::thread(uploadChunks, boost::ref(files)));
  }
}

void interruptWorkerThreads() {
  DXLOG(logINFO) << "Interrupting worker threads:";

  DXLOG(logINFO) << " read...";
  for (int i = 0; i < (int) readThreads.size(); ++i) {
    readThreads[i].interrupt();
  }

  DXLOG(logINFO) << " compress...";
  for (int i = 0; i < (int) compressThreads.size(); ++i) {
    compressThreads[i].interrupt();
  }

  DXLOG(logINFO) << " upload...";
  for (int i = 0; i < (int) uploadThreads.size(); ++i) {
    uploadThreads[i].interrupt();
  }
}

void joinWorkerThreads() {
  DXLOG(logINFO) << "Joining worker threads:";

  DXLOG(logINFO) << " read...";
  for (int i = 0; i < (int) readThreads.size(); ++i) {
    readThreads[i].join();
  }

  DXLOG(logINFO) << " compress...";
  for (int i = 0; i < (int) compressThreads.size(); ++i) {
    compressThreads[i].join();
  }

  DXLOG(logINFO) << " upload...";
  for (int i = 0; i < (int) uploadThreads.size(); ++i) {
    uploadThreads[i].join();
  }
}

void curlInit() {
  DXLOG(logINFO) << "Initializing HTTP library...";
  CURLcode code = curl_global_init(CURL_GLOBAL_ALL);
  if (code != 0) {
    ostringstream msg;
    msg << "An error occurred when initializing the HTTP library (" << curl_easy_strerror(code) << ")" << endl;
    throw runtime_error(msg.str());
  }
  DXLOG(logINFO) << " done.";
  curlInit_call_count++;
}


void markFileAsFailed(vector<File> &files, const string &fileID) {
  for (unsigned int i = 0; i < files.size(); ++i) {
    if (files[i].fileID == fileID) {
      files[i].failed = true;
      return;
    }
  }
}


map<string, fs::path> hashTable; // A map for hash string to index in files vector
map<string, string> projectTable; // A map to map project names to ids.
void resolveProjects(const vector<string> &projects){
  // Insert unique projects into the table
  for (std::vector<string>::const_iterator proj= projects.begin();
        proj!= projects.end(); ++proj) {
    projectTable[*proj] = "";
  }
  // Resolve each unique project
  for (map<string, string>::iterator proj = projectTable.begin(); proj!= projectTable.end(); ++proj) {
    proj->second = resolveProject(proj->first);
    DXLOG(logDEBUG3) << "Proj : " << proj->first << " " << proj->second;
  }
}
/*
 * This function throws a runtime_error if two or more files have the same
 * signature, and are being uploaded to the same project. The signature is
 * a <project, size, last_write_time, filename> tuple, like we use to
 * detect resumable uploads.
 */
void disallowDuplicateFiles(const vector<string> &files, const vector<string> &projects) {
  for (unsigned i = 0; i < files.size(); ++i) {
    string hash = projectTable[projects[i]] + " ";
    fs::path p(files[i]);

    if(fs::is_directory(p)) {
      fs::directory_iterator end_itr;
      vector<string> filesInDir, newProjects;
      for(fs::directory_iterator itr(p); itr != end_itr; ++itr) {
        filesInDir.push_back(itr->path().string());
        // For files in a directory, we assume they use the project that goes with
        // the directory itself.
        newProjects.push_back(projects[i]);
      }
      if (opt.recursive)
        disallowDuplicateFiles(filesInDir, newProjects);
      continue;
    }
    hash += boost::lexical_cast<string>(boost::filesystem::file_size(p)) + " ";
    hash += boost::lexical_cast<string>(boost::filesystem::last_write_time(p)) + " ";
    hash += fs::canonical(p).string();
    DXLOG(logDEBUG3) << "File hash: " << hash;
    if (hashTable.count(hash) > 0) {
      throw runtime_error("File \"" + files[i] + "\" and \"" + hashTable[hash].string() + "\" have same Signature. You cannot upload"
                           " two files with same signature to same project without using '--do-not-resume' flag");
    }
    hashTable[hash] = p;
  }
}

// This function sets two kind of user agent strings
//  1) For Upload Agent libcurl calls (/UPLOAD/xxxx calls)
//  2) For dxcpp libcurl calls (using dx::config::USER_AGENT_STRING())
void setUserAgentString() {
  // Include these things in user agent string: UA version, GIT version, a random hash (which will be unique per instance of UA)
  // For windows build, also include that info
  srand(clock() + time(NULL));
  int r1 = rand(), r2 = rand();
  stringstream iHash;
  iHash << std::hex << r1 << "-" << std::hex << r2;
  userAgentString = string("DNAnexus-Upload-Agent/") + UAVERSION + "/" + string(DXTOOLKIT_GITVERSION);

  string platform = "unknown";
#if WINDOWS_BUILD
  platform = "windows";
#elif LINUX_BUILD
  platform = "linux";
#elif MAC_BUILD
  platform = "mac";
#endif
  userAgentString += " platform/" + platform;
#if OLD_KERNEL_SUPPORT
  userAgentString += "/old-kernel-support";
#endif
  userAgentString += " uid/" + iHash.str();
  // Now append the agent string from dxcpp
  userAgentString += string(" ") + dx::config::USER_AGENT_STRING();

  // Now set user agent string of both UA & dxcpp to same thing
  dx::config::USER_AGENT_STRING() = userAgentString;
}

// This function should be called before opt.setApiserverDxConfig() is called,
// since opt::setApiserverDxConfig() changes the value of dx::config::*, based on command line args
void printEnvironmentInfo() {
  using namespace dx::config;

  cout << "Upload Agent v" << UAVERSION << ", environment info:" << endl
       << "  API server protocol: " << APISERVER_PROTOCOL() << endl
       << "  API server host: " << APISERVER_HOST() << endl
       << "  API server port: " << APISERVER_PORT() << endl;

  if (SECURITY_CONTEXT().size() != 0)
    cout << "  Auth token: " << SECURITY_CONTEXT()["auth_token"].get<string>() << endl;
  else
    cout << "  Auth token: " << endl;

  string projID = CURRENT_PROJECT();
  try {
    string projName = getProjectName(projID);
    cout << "  Project: " << projName << " (" << projID << ")" << endl;
  } catch (DXAPIError &e) {
    cout << "  Project: " << projID << endl;
  }
}

// There is currently the possibility of a race condition if a chunk
// upload timed-out.  It's possible that a second upload succeeds,
// has the chunk marked as "complete" and then the first request makes
// its way through the queue and marks the chunk as pending again.
// Since we are just about to close the file, we'll check to see if any
// chunks are marked as pending, and if so, we'll retry them.
void check_for_complete_chunks(vector<File> &files) {
  for (int currCheckNum=0; currCheckNum < NUM_CHUNK_CHECKS; ++currCheckNum){
    map<string, JSON> fileDescriptions;
    while (!chunksFinished.empty()) {
      Chunk *c = chunksFinished.consume();

      // Cache file descriptions so we only have to do once per file,
      // not once per chunk.
      if (fileDescriptions.find(c->fileID) == fileDescriptions.end())
        fileDescriptions[c->fileID] = fileDescribe(c->fileID);

      if (!is_chunk_complete(c, fileDescriptions[c->fileID])) {
        // After the chunk was uploaded, it was cleared, removing the data
        // from the buffer.  We need to reload if we're going to upload again.
        chunksToRead.produce(c);
      }
    }
    // All of the chunks were marked as complete, so let's exit and we
    // should be safeish to close the file.
    if(chunksToRead.size() == 0)
      return;

    // Set the totalChunks variable to the # of chunks we're going
    // to retry now plus the number of chunks in the failed queue.  The monitor
    // thread will be busy until the size of chunksFinished + chunksFailed
    // equals totalChunks.
    DXLOG(logINFO) << "Retrying " << chunksToRead.size() << " chunks that did not complete.";
    totalChunks = chunksToRead.size() + chunksFailed.size();
    // Read, compress, and upload the chunks which weren't marked as complete.
    createWorkerThreads(files);

    boost::thread monitorThread(monitor);
    monitorThread.join();

    interruptWorkerThreads();
    joinWorkerThreads();
  }

  // We have tried to upload incomplete chunks NUM_CHUNK_CHECKS times!
  // Check to see if there are any chunks still not complete and if so,
  // print warning.
  map<string, JSON> fileDescriptions;
  while (!chunksFinished.empty()) {
    Chunk *c = chunksFinished.consume();

    // Cache file descriptions so we only have to do once per file,
    // not once per chunk.
    if (fileDescriptions.find(c->fileID) == fileDescriptions.end())
        fileDescriptions[c->fileID] = fileDescribe(c->fileID);

    if (!is_chunk_complete(c, fileDescriptions[c->fileID])) {
        DXLOG(logUSERINFO) << "Chunk " << c->index << " of file " << c->fileID << " did not complete.  This file will not be accessible.  PLease try to upload this file again." << endl;
    }
  }
}

File createFile(const std::string &filePath,
                const std::string &project,
                const std::string &folders,
                const std::string &name,
                const unsigned int &fileIndex) {
  DXLOG(logINFO) << "Getting MIME type for local file " << filePath << "...";
  string mimeType = getMimeType(filePath);
  DXLOG(logINFO) << "MIME type for local file " << filePath << " is '" << mimeType << "'.";
  bool toCompress;
  if (!opt.doNotCompress) {
    bool is_compressed = isCompressed(mimeType);
    toCompress = !is_compressed;
    if (is_compressed)
      DXLOG(logINFO) << "File " << filePath << " is already compressed, so won't try to compress it any further.";
    else
      DXLOG(logINFO) << "File " << filePath << " is not compressed, will compress it before uploading.";
  } else {
    toCompress = false;
  }
  if (toCompress) {
    mimeType = "application/x-gzip";
  }
  return File(filePath, project, folders, name, opt.visibility,
         opt.properties, opt.type, opt.tags, opt.details,
         toCompress, !opt.doNotResume, mimeType, opt.chunkSize, fileIndex);
}

void traverseDirectory(const fs::path &localDirPath,
                       const std::string &project,
                       const fs::path &_folders,
                       const fs::path &dirName,
                       std::vector<File> &files) {
  fs::path remoteFolders(_folders);

  if (dirName != ".") {
    remoteFolders /= dirName;
  }

  fs::directory_iterator it(localDirPath), end;
  for (; it != end; ++it) {
    fs::path currPath(it->path());
    if (fs::is_directory(currPath)) {
      if (opt.recursive) {
        traverseDirectory(currPath, project, remoteFolders, currPath.filename(), files);
      }
    } else if (fs::is_regular_file(currPath)) {
      unsigned int fileIndex = files.size();
      files.push_back(createFile(currPath.string(), project, remoteFolders.string(), currPath.filename().string(), fileIndex));
      totalChunks += files[fileIndex].createChunks(chunksToRead, opt.tries);
      cerr << endl;
    } else {
      DXLOG(logWARNING) << "Unable to upload non regular file \"" << currPath.string() << "\"";
    }
  }
}

int main(int argc, char * argv[]) {
#if LINUX_BUILD
  LC_ALL_Hack::set_LC_ALL_C();
#endif
  try {
    // Note: Verbose mode logging is enabled (if requested) by options parse()
    opt.parse(argc, argv);
  } catch (exception &e) {
    DXLOG(logUSERINFO) << "Error processing arguments: " << e.what() << endl;
    opt.printHelp(argv[0]);
    return 1;
  }

  if (opt.env()) {
    opt.setApiserverDxConfig();  // needed for 'ua --env' to report project name
    printEnvironmentInfo();
    return 0;
  }

  if (opt.version()) {
    cout << "Upload Agent Version: " << UAVERSION;
#if OLD_KERNEL_SUPPORT
    cout << " (old-kernel-support)";
#endif
    cout << endl
         << "git version: " << DXTOOLKIT_GITVERSION << endl
         << "libboost version: " << (BOOST_VERSION / 100000) << "." << ((BOOST_VERSION / 100) % 1000) << "." << (BOOST_VERSION % 100) << endl
         << "libcurl version: " << LIBCURL_VERSION_MAJOR << "." << LIBCURL_VERSION_MINOR << "." << LIBCURL_VERSION_PATCH << endl;
    return 0;
  } else if (opt.help() || opt.files.empty()) {
    opt.printHelp(argv[0]);
    return (opt.help()) ? 0 : 1;
  }

  setUserAgentString(); // also sets dx::config::USER_AGENT_STRING()
  DXLOG(logINFO) << "DNAnexus Upload Agent " << UAVERSION << " (git version: " << DXTOOLKIT_GITVERSION << ")";
  DXLOG(logINFO) << "Upload agent's User Agent string: '" << userAgentString << "'";
  DXLOG(logINFO) << "dxcpp's User Agent string: '" << dx::config::USER_AGENT_STRING() << "'";
  DXLOG(logINFO) << opt;

  try {
    opt.setApiserverDxConfig();
    opt.validate();

    /*
     * Check for updates, and terminate execution if necessary. This also
     * has the side effect of verifying that we can connect to the API
     * server, and that the authentication token is valid.
     */
    try {
      checkForUpdates();
    } catch (runtime_error &e) {
      DXLOG(logUSERINFO) << "ERROR: " << e.what() << endl;
      return 3;
    }
    if (!opt.doNotResume) {
      resolveProjects(opt.projects);
      disallowDuplicateFiles(opt.files, opt.projects);
    }
  } catch (exception &e) {
    DXLOG(logUSERINFO) << "ERROR: " << e.what() << endl;
    return 1;
  }

  const bool anyImportAppToBeCalled = (opt.reads || opt.pairedReads || opt.mappings || opt.variants);

  chunksToCompress.setCapacity(opt.compressThreads);
  chunksToUpload.setCapacity(opt.uploadThreads);
  int exitCode = 0;
  try {
    curlInit(); // for curl requests to be made by upload chunk request

    NUMTRIES_g = opt.tries;

    vector<File> files;

    for (unsigned int i = 0; i < opt.files.size(); ++i) {
      if (fs::is_directory(opt.files[i])) {
        traverseDirectory(fs::path(opt.files[i]), opt.projects[i], fs::path(opt.folders[i]), fs::path(opt.names[i]), files);
      } else {
        unsigned int fileIndex = files.size();
        files.push_back(createFile(opt.files[i], opt.projects[i], opt.folders[i], opt.names[i], fileIndex));
        totalChunks += files[fileIndex].createChunks(chunksToRead, opt.tries);
      }
    }

    if (opt.waitOnClose) {
      for (unsigned int i = 0; i < files.size(); ++i) {
        files[i].waitOnClose = true;
      }
    }

    // Take this point as the starting time for program operation
    // (to calculate average transfer speed)
    startTime = std::time(0);

    DXLOG(logINFO) << "Created " << totalChunks << " chunks.";

    initializeRSSLimit();
    createWorkerThreads(files);

    DXLOG(logINFO) << "Creating monitor thread..";
    boost::thread monitorThread(monitor);

    boost::thread uploadProgressThread;
    if (opt.progress) {
      DXLOG(logINFO) << "Creating Upload Progress thread..";
      uploadProgressThread = boost::thread(uploadProgress, boost::ref(files));
    }

    DXLOG(logINFO) << "Joining monitor thread...";
    monitorThread.join();
    DXLOG(logINFO) << "Monitor thread finished.";

    if (opt.progress) {
      DXLOG(logINFO) << "Joining Upload Progress thread..";
      keepShowingUploadProgress = false;
      uploadProgressThread.interrupt();
      uploadProgressThread.join();
      DXLOG(logINFO) << "Upload Progress thread finished.";
    }


    interruptWorkerThreads();
    joinWorkerThreads();

    check_for_complete_chunks(files);

    while (!chunksFailed.empty()) {
      Chunk * c = chunksFailed.consume();
      c->log("Chunk failed", logERROR);
      markFileAsFailed(files, c->fileID);
    }

    for (unsigned int i = 0; i < files.size(); ++i) {
      if (files[i].failed) {
        DXLOG(logUSERINFO) << "File \""<< files[i].localFile << "\" could not be uploaded." << endl;
      } else {
        DXLOG(logUSERINFO) << "File \"" << files[i].localFile << "\" was uploaded successfully. Closing..." << endl;
        if (files[i].isRemoteFileOpen) {
          files[i].close();
        }
      }
      if (files[i].failed)
        files[i].fileID = "failed";
    }

    DXLOG(logINFO) << "Waiting for files to be closed...";
    boost::thread waitOnCloseThread(waitOnClose, boost::ref(files));
    DXLOG(logINFO) << "Joining wait-on-close thread...";
    waitOnCloseThread.join();
    DXLOG(logINFO) << "Wait-on-close thread finished.";
    if (anyImportAppToBeCalled) {
      runImportApps(opt, files);
    }
    for (unsigned i = 0; i < files.size(); ++i) {
      cout << files[i].fileID;
      if (files[i].fileID == "failed")
        exitCode = 1;
      if (anyImportAppToBeCalled) {
        if (files[i].jobID == "failed")
          exitCode = 1;
        cout << "\t" << files[i].jobID;
      }
      cout << endl;
    }
    curlCleanup();

    DXLOG(logINFO) << "Exiting.";
  } catch (bad_alloc &e) {
    boost::call_once(bad_alloc_once, boost::bind(&handle_bad_alloc, e));
  } catch (exception &e) {
    curlCleanup();
    DXLOG(logUSERINFO) << endl << "ERROR: " << e.what() << endl;
    return 1;
  }

#if LINUX_BUILD
  LC_ALL_Hack::reset_LC_ALL();
#endif
  return exitCode;
}
