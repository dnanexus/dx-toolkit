// Copyright (C) 2013 DNAnexus, Inc.
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

#include "dxcpp/dxcpp.h"
#include "dxcpp/bqueue.h"
#include "api_helper.h"
#include "options.h"
#include "chunk.h"
#include "file.h"
#include "dxcpp/dxlog.h"
#include "import_apps.h"
#include "mime.h"

// http://www.boost.org/doc/libs/1_48_0/libs/config/doc/html/boost_config/boost_macro_reference.html
#if ((BOOST_VERSION / 100000) < 1 || ((BOOST_VERSION/100000) == 1 && ((BOOST_VERSION / 100) % 1000) < 48))
  #error "Cannot compile Upload Agent using Boost version < 1.48"
#endif

using namespace std;
using namespace dx;


int curlInit_call_count = 0;

Options opt;

/* Mutex for "bytesUploaded" member variable of "File" class
 * , as well as bytesUploadedSinceStart global variable.
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

unsigned int totalChunks;

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

bool finished() {
  return (chunksFinished.size() + chunksFailed.size() == totalChunks);
}

void readChunks() {
  try {
    while (true) {
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
  } catch (boost::thread_interrupted &ti) {
    return;
  }
}

void uploadChunks(vector<File> &files) {
  try {
    while (true) {
      Chunk * c = chunksToUpload.consume();

      c->log("Uploading...");

      bool uploaded = false;
      try {
        c->upload();
        uploaded = true;
      } catch (runtime_error &e) {
        ostringstream msg;
        msg << "Upload failed: " << e.what();
        c->log(msg.str());
      }

      if (uploaded) {
        c->log("Upload succeeded!");
        c->clear();
        chunksFinished.produce(c);
        // Update number of bytes uploaded in parent file object
        boost::mutex::scoped_lock boLock(bytesUploadedMutex);
        files[c->parentFileIndex].bytesUploaded += (c->end - c->start);
        files[c->parentFileIndex].atleastOnePartDone = true;
        bytesUploadedSinceStart += (c->end - c->start);
        boLock.unlock();
      } else if (c->triesLeft > 0) {
        int numTry = NUMTRIES_g - c->triesLeft + 1; // find out which try is it
        int timeout = (numTry > 6) ? 256 : 4 << numTry; // timeout is always between [8, 256] seconds
        c->log("Will retry reading and uploading this chunks in " + boost::lexical_cast<string>(timeout) + " seconds");
        --(c->triesLeft);
        c->clear(); // we will read & compress data again
        boost::this_thread::sleep(boost::posix_time::milliseconds(timeout * 1000));
        // We push the chunk to retry to "chunksToRead" and not "chunksToUpload"
        // Since chunksToUpload queue is bounded, and chunksToUpload.produce() can block,
        // thus giving rise to deadlock
        chunksToRead.produce(c);
      } else {
        c->log("Not retrying");
        // TODO: Should we print it on stderr or DXLOG (verbose only) ??
        cerr << "\nFailed to upload Chunk [" << c->start << " - " << c->end << "] for local file ("
             << files[c->parentFileIndex].localFile << "). APIServer response for last try: '" << c->respData << "'" << endl;
        c->clear();
        chunksFailed.produce(c);
      }
      // Sleep for tiny amount of time, to make sure we yield to other threads.
      // Note: boost::this_thread::yield() is not a valid interruption point,
      //       so we have to use sleep()
      boost::this_thread::sleep(boost::posix_time::microseconds(100));
    }
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
  cerr << "\r";

  // Print individual file progress
  boost::mutex::scoped_lock boLock(bytesUploadedMutex);
  for (unsigned i = 0; i < files.size(); ++i) {
    double percent = (files[i].size == 0 && files[i].atleastOnePartDone) ? 100.0 : 0.0;
    percent =  (files[i].size != 0) ? ((double(files[i].bytesUploaded) / files[i].size) * 100.0) : percent;

    cerr << files[i].localFile << " " << setw(6) << setprecision(2) << std::fixed
         << percent << "% complete";
    if ((i + 1) != files.size()) {
      cerr << ", ";
    }
  }

  // Print average transfer rate
  int64_t timediff  = std::time(0) - startTime;
  double mbps = (timediff > 0) ? (double(bytesUploadedSinceStart) / (1024.0 * 1024.0)) / timediff : 0.0;
  boLock.unlock();
  cerr << " ... Average transfer speed = " << setw(6) << setprecision(2) << std::fixed << mbps << " MB/sec";
  
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
  cerr << " ... Instantaneous transfer speed = " << setw(6) << setprecision(2) << std::fixed << mbps2 << " MB/sec";
}

void uploadProgress(vector<File> &files) {
  cerr << endl;
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
    cerr << endl;
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

void curlCleanup() {
  // http://curl.haxx.se/libcurl/c/curl_global_cleanup.html
  for (;curlInit_call_count > 0; --curlInit_call_count) {
    curl_global_cleanup();
  }
}

void markFileAsFailed(vector<File> &files, const string &fileID) {
  for (unsigned int i = 0; i < files.size(); ++i) {
    if (files[i].fileID == fileID) {
      files[i].failed = true;
      return;
    }
  }
}

/* This function throws a runtime_error if two or more file 
 * have same "signature", and are being uploaded to same project.
 * Note: - Signature is: <project, size, last_write_time, filename> tuple
 *         Same as what we use for resuming.
 */
void disallowDuplicateFiles(const vector<string> &files, const vector<string> &prjs) {
  map<string, int> hashTable; // a map for - hash string to index in files vector
  for (unsigned i = 0; i < files.size(); ++i) {
    //TODO: This results in calling "resolveProject" twice for each file -- not a big deal,
    //      but ideally we should reuse the value retrieved in first call
    string hash = resolveProject(prjs[i]) + " ";
    
    boost::filesystem::path p(files[i]);
    
    hash += boost::lexical_cast<string>(boost::filesystem::file_size(p)) + " ";
    hash += boost::lexical_cast<string>(boost::filesystem::last_write_time(p)) + " ";
    hash += p.filename().string();
    if (hashTable.count(hash) > 0) {
      throw runtime_error("File \"" + files[i] + "\" and \"" + files[hashTable[hash]] + "\" have same Signature. You cannot upload"
                           " two files with same signature to same project without using '--do-not-resume' flag");
    }
    hashTable[hash] = i;
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

  cout << "  Project: " << CURRENT_PROJECT() << endl;
}

int main(int argc, char * argv[]) {
  try {
    // Note: Verbose mode logging is enabled (if requested) by options parse()
    opt.parse(argc, argv);
  } catch (exception &e) {
    cerr << "Error processing arguments: " << e.what() << endl;
    opt.printHelp(argv[0]);
    return 1;
  }
 
  if (opt.env()) {
    printEnvironmentInfo();
    return 0;
  }
  if (opt.version()) {
    cout << "Upload Agent Version: " << UAVERSION << endl
         << "git version: " << DXTOOLKIT_GITVERSION << endl;
    return 0;
  } else if (opt.help() || opt.files.empty()) {
    opt.printHelp(argv[0]);
    return 1;
  }
  
  setUserAgentString(); // also sets dx::config::USER_AGENT_STRING()
  
  DXLOG(logINFO) << "DNAnexus Upload Agent " << UAVERSION << " (git version: " << DXTOOLKIT_GITVERSION << ")";
  DXLOG(logINFO) << "Upload agent's User Agent string: '" << userAgentString << "'";
  DXLOG(logINFO) << "dxcpp's User Agent string: '" << dx::config::USER_AGENT_STRING() << "'";
  DXLOG(logINFO) << opt;
  try {
    opt.setApiserverDxConfig();
    opt.validate();
    
    // Check for updates, and terminate execution if necessary
    // Note: - It's important to call this function before testServerConnection()
    //         because, if the client is too old, testServerConnection() would fail with "ClientTooOld" error (since it calls /system/findUsers).
    //       - If server is unreachable, checkForUpdates() will just print a warning on DXLOG
    //         the actual unreachability of server (and subsequent action) will still be determined by testServerConnection()
    // TODO: Once production has /system/greet route, we can subsume testServerConnection() into checkForUpdates()
    //       , i.e., we can use /system/greet route to test apiserver connection as well (instead of findUsers).
    //       But one important point to be noted: /findUsers route check the fact that auth token is not from public,
    //       whereas /system/greet does not (so we must find another way to test that fact for auth token).
    //       (for example: later calls, like creating file, polling project, etc will fail)
    try {
      checkForUpdates();
    } catch (runtime_error &e) {
      cerr << endl << e.what() << endl;
      return 3;
    }
    testServerConnection();
    if (!opt.doNotResume) {
      disallowDuplicateFiles(opt.files, opt.projects);
    }
  } catch (exception &e) {
    cerr << endl << "ERROR: " << e.what() << endl;
    return 1;
  }

  const bool anyImportAppToBeCalled = (opt.reads || opt.pairedReads || opt.mappings || opt.variants);
 
/*// JM can now accept files which are not in "closed" state as inputs. So we no longer need to wait for them to close first.
  if (anyImportAppToBeCalled) {
    DXLOG(logINFO) << "User requested an import app to be called at the end of upload. Will explicitly turn on --wait-on-close flag (if not present already)";
    opt.waitOnClose = true;
  }
*/
  chunksToCompress.setCapacity(opt.compressThreads);
  chunksToUpload.setCapacity(opt.uploadThreads);
  int exitCode = 0; 
  try {
    curlInit(); // for curl requests to be made by upload chunk request

    NUMTRIES_g = opt.tries;

    vector<File> files;

    for (unsigned int i = 0; i < opt.files.size(); ++i) {
      DXLOG(logINFO) << "Getting MIME type for local file " << opt.files[i] << "...";
      string mimeType = getMimeType(opt.files[i]);
      DXLOG(logINFO) << "MIME type for local file " << opt.files[i] << " is '" << mimeType << "'.";
      bool toCompress;
      if (!opt.doNotCompress) {
        bool is_compressed = isCompressed(mimeType);
        toCompress = !is_compressed;
        if (is_compressed)
          DXLOG(logINFO) << "File " << opt.files[i] << " is already compressed, so won't try to compress it any further.";
        else
          DXLOG(logINFO) << "File " << opt.files[i] << " is not compressed, will compress it before uploading.";
      } else {
        toCompress = false;
      }
      if (toCompress) {
        mimeType = "application/x-gzip";
      }
      files.push_back(File(opt.files[i], opt.projects[i], opt.folders[i], opt.names[i], toCompress, !opt.doNotResume, mimeType, opt.chunkSize, i));
      totalChunks += files[i].createChunks(chunksToRead, opt.tries);
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

    while (!chunksFailed.empty()) {
      Chunk * c = chunksFailed.consume();
      c->log("Chunk failed");
      markFileAsFailed(files, c->fileID);
    }

    for (unsigned int i = 0; i < files.size(); ++i) {
      if (files[i].failed) {
        cerr << endl << "File \""<< files[i].localFile << "\" could not be uploaded." << endl;
      } else {
        cerr << endl << "File \"" << files[i].localFile << "\" was uploaded successfully. Closing..." << endl;
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
    curlCleanup();
    cerr << endl << "*********" << endl << "FATAL ERROR: The program ran out of memory. You may try following steps to avoid this problem: " << endl;
    cerr << "1. Try decreasing number of upload/compress/read threads (Try ./ua --help to see how to set them) - Recommended solution" << endl;
    cerr << "2. Reduce the chunk-size (--chunk-size options). Note: Trying with a different chunk size will not resume your previous upload" << endl;
    cerr << endl << "If you still face problem, please contact DNAnexus support." << endl;
    cerr << "\nError details (for advanced users only): '" << e.what() << "'" << endl << "*********" << endl;
    return 1;
  } catch (exception &e) {
    curlCleanup();
    cerr << endl << "ERROR: " << e.what() << endl;
    return 1;
  }

  return exitCode;
}
