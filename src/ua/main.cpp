#include <cstdint>
#include <iostream>
#include <queue>

#include <curl/curl.h>

#include <boost/thread.hpp>

#include "dxcpp/bqueue.h"

#include "api_helper.h"
#include "options.h"
#include "chunk.h"
#include "File.h"
#include "log.h"

#include "SSLThreads.h"

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

      if (opt.compress) {
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
    SSLThreadsSetup();
    curlInit();

    testServerConnection();

    File file(opt.files[0], opt.projects[0], opt.folders[0], opt.names[0]);
    totalChunks += file.createChunks(chunksToRead, opt.chunkSize, opt.tries);

    LOG << "Created " << totalChunks << " chunks." << endl;

    createWorkerThreads();

    LOG << "Creating monitor thread.." << endl;
    boost::thread monitorThread(monitor);

    LOG << "Joining monitor thread..." << endl;
    monitorThread.join();
    LOG << "Monitor thread finished." << endl;

    interruptWorkerThreads();
    joinWorkerThreads();

    if (chunksFailed.empty()) {
      cerr << "Upload was successful! Closing file...";
      file.close();
      cerr << endl;
    } else {
      int failed = chunksFailed.size();
      cerr << "Upload failed. " << failed << " " << (failed == 1 ? "chunk" : "chunks")
           << " could not be uploaded." << endl;
    }

    curlCleanup();
    SSLThreadsCleanup();

    LOG << "Exiting." << endl;
  } catch (exception &e) {
    LOG << "ERROR: " << e.what() << endl;
    return 1;
  }

  return 0;
}
