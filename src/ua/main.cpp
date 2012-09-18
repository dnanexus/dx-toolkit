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

#include <boost/filesystem.hpp>

using namespace std;

Options opt;

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

void uploadChunks() {
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
      } else if (c->triesLeft > 0) {
        c->log("Retrying");
        --(c->triesLeft);
        chunksToUpload.produce(c);
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

void createWorkerThreads() {
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
    uploadThreads.push_back(boost::thread(uploadChunks));
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
}

void curlCleanup() {
  curl_global_cleanup();
}

void markFileAsFailed(vector<File> &files, const string &fileID) {
  for (unsigned int i = 0; i < files.size(); ++i) {
    if (files[i].fileID == fileID) {
      files[i].failed = true;
      return;
    }
  }
}

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
  // then libmagic silently Seg faults.
  if (!boost::filesystem::exists(boost::filesystem::path(filePath)))
    throw runtime_error("Local file '" + filePath + "' does not exist");
  
  string magic_output;
  magic_t magic_cookie;
  magic_cookie = magic_open(MAGIC_MIME | MAGIC_NO_CHECK_COMPRESS | MAGIC_SYMLINK);

  if (magic_cookie == NULL) {
    throw runtime_error("error allocating magic cookie (libmagic)");
  }

  if (magic_load(magic_cookie, NULL) != 0) {
    string errMsg = magic_error(magic_cookie);
    magic_close(magic_cookie);
    throw runtime_error("cannot load magic database - '" + errMsg + "'");
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

int main(int argc, char * argv[]) {
  try {
    opt.parse(argc, argv);
  } catch (exception &e) {
    cerr << "Error processing arguments: " << e.what() << endl;
    opt.printHelp(argv[0]);
    return 1;
  }

  if (opt.version()) {
    cout << GITVERSION << endl;
    return 0;
  } else if (opt.help() || opt.files.empty()) {
    opt.printHelp(argv[0]);
    return 1;
  }

  Log::enabled = opt.verbose;

  LOG << "DNAnexus Upload Agent " << GITVERSION << endl;

  LOG << opt;
  try {
    opt.validate();
  } catch (exception &e) {
    LOG << "ERROR: " << e.what() << endl;
    return 1;
  }

  apiInit(opt.apiserverHost, opt.apiserverPort, opt.apiserverProtocol, opt.authToken);

  chunksToCompress.setCapacity(opt.compressThreads);
  chunksToUpload.setCapacity(opt.uploadThreads);

  try {
    curlInit();

    testServerConnection();

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
      files.push_back(File(opt.files[i], opt.projects[i], opt.folders[i], opt.names[i], toCompress, !opt.doNotResume, mimeType, opt.chunkSize));
      totalChunks += files[i].createChunks(chunksToRead, opt.tries);
    }

    if (opt.waitOnClose) {
      for (unsigned int i = 0; i < files.size(); ++i) {
        files[i].waitOnClose = true;
      }
    }

    LOG << "Created " << totalChunks << " chunks." << endl;

    createWorkerThreads();

    LOG << "Creating monitor thread.." << endl;
    boost::thread monitorThread(monitor);
    LOG << "Joining monitor thread..." << endl;
    monitorThread.join();
    LOG << "Monitor thread finished." << endl;

    interruptWorkerThreads();
    joinWorkerThreads();

    while (!chunksFailed.empty()) {
      Chunk * c = chunksFailed.consume();
      c->log("Chunk failed");
      markFileAsFailed(files, c->fileID);
    }

    for (unsigned int i = 0; i < files.size(); ++i) {
      if (files[i].failed) {
        cerr << "File " << files[i] << " could not be uploaded." << endl;
      } else {
        cerr << "File " << files[i] << " was uploaded successfully. Closing...";
        if (files[i].isRemoteFileOpen) {
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
          }
        }
        cerr << endl;
      }
    }

    LOG << "Waiting for files to be closed..." << endl;
    boost::thread waitOnCloseThread(waitOnClose, boost::ref(files));
    LOG << "Joining wait-on-close thread..." << endl;
    waitOnCloseThread.join();
    LOG << "Wait-on-close thread finished." << endl;

    curlCleanup();

    LOG << "Exiting." << endl;
  } catch (exception &e) {
    LOG << "ERROR: " << e.what() << endl;
    return 1;
  }

  return 0;
}
