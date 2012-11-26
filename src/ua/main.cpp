#include <cstdint>
#include <iostream>
#include <queue>

#include <curl/curl.h>
#include <magic.h>

#include <boost/thread.hpp>

#include "dxcpp/bqueue.h"

#include "api_helper.h"
#include "options.h"
#include "chunk.h"
#include "File.h"
#include "log.h"
#include "dxcpp/dxcpp.h"
#include "import_apps.h"

#include <boost/filesystem.hpp>

using namespace std;

#ifdef WINDOWS_BUILD
  // This additional code is required for Windows build, since Magic database is not present
  // by default, and rather packaged with the distribution
  #include <windows.h>
  string MAGIC_DATABASE_PATH;	
#endif

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
      } catch (exception &e) {
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
        c->clear();
        chunksFailed.produce(c);
      }
    }
  } catch (boost::thread_interrupted &ti) {
    return;
  }
}

void monitor() {
  while (true) {
    boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
    {
      LOG << "[monitor]"
          << "  to read: " << chunksToRead.size()
          << "  to compress: " << chunksToCompress.size()
          << "  to upload: " << chunksToUpload.size()
          << "  finished: " << chunksFinished.size()
          << "  failed: " << chunksFailed.size() << endl;

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
  try {
    do {
      uploadProgressHelper(files);
      boost::this_thread::sleep(boost::posix_time::milliseconds(250));
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
  LOG << "Creating worker threads:" << endl;

  LOG << " read..." << endl;
  for (int i = 0; i < opt.readThreads; ++i) {
    readThreads.push_back(boost::thread(readChunks));
  }

  LOG << " compress..." << endl;
  for (int i = 0; i < opt.compressThreads; ++i) {
    compressThreads.push_back(boost::thread(compressChunks));
  }

  LOG << " upload..." << endl;
  for (int i = 0; i < opt.uploadThreads; ++i) {
    uploadThreads.push_back(boost::thread(uploadChunks, boost::ref(files)));
  }
}

void interruptWorkerThreads() {
  LOG << "Interrupting worker threads:" << endl;

  LOG << " read..." << endl;
  for (int i = 0; i < (int) readThreads.size(); ++i) {
    readThreads[i].interrupt();
  }

  LOG << " compress..." << endl;
  for (int i = 0; i < (int) compressThreads.size(); ++i) {
    compressThreads[i].interrupt();
  }

  LOG << " upload..." << endl;
  for (int i = 0; i < (int) uploadThreads.size(); ++i) {
    uploadThreads[i].interrupt();
  }
}

void joinWorkerThreads() {
  LOG << "Joining worker threads:" << endl;

  LOG << " read..." << endl;
  for (int i = 0; i < (int) readThreads.size(); ++i) {
    readThreads[i].join();
  }

  LOG << " compress..." << endl;
  for (int i = 0; i < (int) compressThreads.size(); ++i) {
    compressThreads[i].join();
  }

  LOG << " upload..." << endl;
  for (int i = 0; i < (int) uploadThreads.size(); ++i) {
    uploadThreads[i].join();
  }
}

void curlInit() {
  LOG << "Initializing HTTP library...";
  CURLcode code = curl_global_init(CURL_GLOBAL_ALL);
  if (code != 0) {
    ostringstream msg;
    msg << "An error occurred when initializing the HTTP library (" << curl_easy_strerror(code) << ")" << endl;
    throw runtime_error(msg.str());
  }
  LOG << " done." << endl;
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

#ifdef WINDOWS_BUILD
void setMagicDBPath() {
  if (MAGIC_DATABASE_PATH.size() > 0)
    return;

  // Find out the current process's directory
  char buffer[32768] = {0}; // Maximum path length in windows (approx): http://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx#maxpath
  if (!GetModuleFileName(NULL, buffer, 32767)) {
    throw runtime_error("Unable to get current process's directory using GetModuleFileName() .. GetLastError() = " + boost::lexical_cast<string>(GetLastError()) + "\n");
  }
  string processPath = buffer;
  size_t found = processPath.find_last_of("\\");
  found = (found != string::npos) ? found : 0;
  MAGIC_DATABASE_PATH = processPath.substr(0, found) + "\\resources\\magic";
}

#endif

/* 
 * - Returns the MIME type for a file (of this format: "type/subType")
 * - Symlinks are followed (and MIME type of actual file being pointed is returned)
 * - We do not try to uncompress an archive, rather return the mime type for compressed file.
 * - Throw runtime_error if the file path (fpath) is invalid, or if some other
 *   internal error occurs.
 */
string getMimeType(string filePath) {
  // It's necessary to check file's existence
  // because if an invalid path is given,
  // then, libmagic silently Seg faults.
  if (!boost::filesystem::exists(boost::filesystem::path(filePath)))
    throw runtime_error("Local file '" + filePath + "' does not exist");
  
  string magic_output;
  magic_t magic_cookie;
  magic_cookie = magic_open(MAGIC_MIME | MAGIC_NO_CHECK_COMPRESS | MAGIC_SYMLINK);
  if (magic_cookie == NULL) {
    throw runtime_error("error allocating magic cookie (libmagic)");
  }
#ifndef WINDOWS_BUILD
	const char *ptr_to_db = NULL; // NULL means look in default location (fine for POSIX systems)
#else
	setMagicDBPath();
	const char *ptr_to_db = MAGIC_DATABASE_PATH.c_str();
#endif
  if (magic_load(magic_cookie, ptr_to_db) != 0) {
    string errMsg = magic_error(magic_cookie);
    magic_close(magic_cookie);
#ifndef WINDOWS_BUILD
    throw runtime_error("cannot load magic database - '" + errMsg + "'");
#else
    throw runtime_error("cannot load magic database - '" + errMsg + "'" + " Magic DB path = '" + MAGIC_DATABASE_PATH + "'");
#endif
  }

  magic_output = magic_file(magic_cookie, filePath.c_str());
  magic_close(magic_cookie);
  // magic_output will be of this format: "type/subType; charset=.."
  // we just want to return "type/subType"
  return magic_output.substr(0, magic_output.find(';'));
}

/* 
 * - Returns true iff the file is detected as 
 *   one of the compressed types.
 */
bool isCompressed(string mimeType) {
  // This list is mostly compiled from: http://en.wikipedia.org/wiki/List_of_archive_formats
  // Some of the items are added by trying libmagic with few common file formats
  // Note: application/x-empty and inode/x-empty are added to treat
  //       the special case of empty file (i.e., not compress them).
  const char* compressed_mime_types[] = {
    "application/x-bzip2",
    "application/zip",
    "application/x-gzip",
    "application/x-lzip",
    "application/x-lzma",
    "application/x-lzop",
    "application/x-xz",
    "application/x-compress",
    "application/x-7z-compressed",
    "application/x-ace-compressed",
    "application/x-alz-compressed",
    "application/x-astrotite-afa",
    "application/x-arj",
    "application/x-cfs-compressed",
    "application/x-lzx",
    "application/x-lzh",
    "application/x-gca-compressed",
    "application/x-apple-diskimage",
    "application/x-dgc-compressed",
    "application/x-dar",
    "application/vnd.ms-cab-compressed",
    "application/x-rar-compressed",
    "application/x-stuffit",
    "application/x-stuffitx",
    "application/x-gtar",
    "application/x-zoo",
    "application/x-empty",
    "inode/x-empty"
  };
  unsigned numElems = sizeof compressed_mime_types/sizeof(compressed_mime_types[0]);
  for (unsigned i = 0; i < numElems; ++i) {
    if (mimeType == string(compressed_mime_types[i]))
      return true;
  }
  return false;
}

/* This function throws a runtime_error if two or more file 
 * have same "signature", and are being uploaded to same project.
 * Note: - Signature is: <project, size, last_write_time, filename> tuple
 *         Same as what we use for resuming.
 */
void disallowDuplicateFiles(const vector<string> &files, const vector<string> &prjs) {
  map<string, int> hashTable; // a map for - hash string to index in files vector
  for (unsigned i = 0; i < files.size(); ++i) {
    string hash = prjs[i] + " ";
    
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

void setUserAgentString() {
  bool windows_env = false;
#ifdef WINDOWS_BUILD
  windows_env = true;
#endif
  // Include these things in user agent string: UA version, GIT version, a random hash (which will be unique per instance of UA)
  // For windows build, also include that info
  srand(clock() + time(NULL));
  int r1 = rand(), r2 = rand();
  stringstream iHash;
  iHash << std::hex << r1 << "-" << std::hex << r2;
  userAgentString = string("DNAnexus-Upload-Agent/") + UAVERSION;
  userAgentString += (windows_env) ? " (WINDOWS_BUILD=true)" : "";
  userAgentString += string(" git-version/") + GITVERSION + " Instance-Hash/" + iHash.str();
}

void printEnvironmentInfo() {
  cout << "Environment info:" << endl
       << "  API server protocol: " << opt.apiserverProtocol << endl
       << "  API server host: " << opt.apiserverHost << endl
       << "  API server port: " << opt.apiserverPort << endl
       << "  Auth token: " << opt.authToken << endl; 
  cout << "  Project:  " << ((opt.projects.size() > 0) ? opt.projects[0] : "") << endl; 
}

int main(int argc, char * argv[]) {
  try {
    opt.parse(argc, argv);
  } catch (exception &e) {
    cerr << "Error processing arguments: " << e.what() << endl;
    opt.printHelp(argv[0]);
    return 1;
  }
  // Note: Verbose mode logging is now enabled by options parse()
  if (opt.env()) {
    printEnvironmentInfo();
    return 0;
  }
  if (opt.version()) {
    cout << "Upload Agent Version: " << UAVERSION << endl
         << "git version: " << GITVERSION << endl;
    return 0;
  } else if (opt.help() || opt.files.empty()) {
    opt.printHelp(argv[0]);
    return 1;
  }

  setUserAgentString();

  LOG << "DNAnexus Upload Agent " << UAVERSION << " (git version: " << GITVERSION << ")" << endl;
  LOG << "User Agent string: '" << userAgentString << "'" << endl;
  LOG << opt;
  try {
    opt.validate();
    if (!opt.doNotResume) {
      disallowDuplicateFiles(opt.files, opt.projects);
    }
  } catch (exception &e) {
    cerr << "ERROR: " << e.what() << endl;
    return 1;
  }
  const bool anyImportAppToBeCalled = (opt.reads || opt.pairedReads || opt.mappings || opt.variants);
  if (anyImportAppToBeCalled) {
    LOG << "User requested an import app to be called at the end of upload. Will explicitly turn on --wait-on-close flag (if not present already)" << endl;
    opt.waitOnClose = true;
  }
  apiInit(opt.apiserverHost, opt.apiserverPort, opt.apiserverProtocol, opt.authToken);

  chunksToCompress.setCapacity(opt.compressThreads);
  chunksToUpload.setCapacity(opt.uploadThreads);
  int exitCode = 0; 
  try {
    curlInit();

    testServerConnection();
    NUMTRIES_g = opt.tries;

    vector<File> files;

    for (unsigned int i = 0; i < opt.files.size(); ++i) {
      LOG << "Getting MIME type for local file " << opt.files[i] << "..." << endl;
      string mimeType = getMimeType(opt.files[i]);
      LOG << "MIME type for local file " << opt.files[i] << " is '" << mimeType << "'." << endl;
      bool toCompress;
      if (!opt.doNotCompress) {
        bool is_compressed = isCompressed(mimeType);
        toCompress = !is_compressed;
        if (is_compressed)
          LOG << "File " << opt.files[i] << " is already compressed, so won't try to compress it any further." << endl;
        else
          LOG << "File " << opt.files[i] << " is not compressed, will compress it before uploading." << endl;
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

    LOG << "Created " << totalChunks << " chunks." << endl;
    
    createWorkerThreads(files);

    LOG << "Creating monitor thread.." << endl;
    boost::thread monitorThread(monitor);
    
    boost::thread uploadProgressThread;
    if (opt.progress) {
      LOG << "Creating Upload Progress thread.." << endl;
      uploadProgressThread = boost::thread(uploadProgress, boost::ref(files));
    }

    LOG << "Joining monitor thread..." << endl;
    monitorThread.join();
    LOG << "Monitor thread finished." << endl;

    if (opt.progress) {
      LOG << "Joining Upload Progress thread.." << endl;
      keepShowingUploadProgress = false;
      uploadProgressThread.interrupt();
      uploadProgressThread.join();
      LOG << "Upload Progress thread finished." << endl;
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
        cerr << "File \""<< files[i].localFile << "\" could not be uploaded." << endl;
      } else {
        cerr << "File \"" << files[i].localFile << "\" was uploaded successfully. Closing...";
        if (files[i].isRemoteFileOpen) {
          files[i].close();
          
          // The Code commented below, used to deal with the case that a chunk was compressed to less than 5MB.
          // This case should never happen now (see chunk.cpp for details).
          /*
          try {
            files[i].close();
          } catch (DXAPIError &e) {
            if (e.name == "InvalidState") {
              // TODO: Make sure, that a file can never be in "InvalidState" other than < 5MB case
              cerr << "One of the chunks for file \"" << files[i].localFile << "\" was compressed to less than 5MB. Upload to fileID " 
                   << files[i].fileID << ", cannot be completed (will remove the incomplete remote file)" << endl
                   << "Here are some of the things you can try for uploading file: \"" << files[i].localFile << "\":" << endl
                   << "  1. Upload without compression (--do-not-compress flag)" << endl
                   << "  2. Try increasing chunk size to a larger value. (--chunk-size option)" << endl;
              removeFromProject(files[i].projectID, files[i].fileID);
              files[i].failed = true;
            } else {
              throw;
            }
          }*/
        }
        cerr << endl;
      }
      if (files[i].failed)
        files[i].fileID = "failed";
    } 

    LOG << "Waiting for files to be closed..." << endl;
    boost::thread waitOnCloseThread(waitOnClose, boost::ref(files));
    LOG << "Joining wait-on-close thread..." << endl;
    waitOnCloseThread.join();
    LOG << "Wait-on-close thread finished." << endl;
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

    LOG << "Exiting." << endl;
  } catch (exception &e) {
    curlCleanup();
    cerr << "ERROR: " << e.what() << endl;
    return 1;
  }

  return exitCode;
}
