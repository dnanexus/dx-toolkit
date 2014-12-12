// Copyright (C) 2013-2014 DNAnexus, Inc.
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
#include "dxcpp/bqueue.h"

#include "options.h"
#include "chunk.h"
#include "File.h"
#include "log.h"
#include "dxcpp/dxcpp.h"
//#include "import_apps.h"

#include <boost/filesystem.hpp>

#include <boost/version.hpp>
// http://www.boost.org/doc/libs/1_48_0/libs/config/doc/html/boost_config/boost_macro_reference.html
#if ((BOOST_VERSION / 100000) < 1 || ((BOOST_VERSION/100000) == 1 && ((BOOST_VERSION / 100) % 1000) < 48))
  #error "Cannot compile dx-verify-file using Boost version < 1.48"
#endif

using namespace std;
using namespace dx;

#ifdef WINDOWS_BUILD
  // This additional code is required for Windows build, since Magic database is not present
  // by default, and rather packaged with the distribution
  #include <windows.h>
  string MAGIC_DATABASE_PATH;	
#endif

int curlInit_call_count = 0;

Options opt;

/* Keep track of time since program started (i.e., just before creating worker threads)*/
std::time_t startTime;

unsigned int totalChunks;

BlockingQueue<Chunk*> chunksToRead;
BlockingQueue<Chunk*> chunksToComputeMD5;

BlockingQueue<Chunk*> chunksFinished;
BlockingQueue<Chunk*> chunksFailed;
BlockingQueue<Chunk*> chunksSkipped;

vector<boost::thread> readThreads;
vector<boost::thread> md5Threads;

bool finished() {
  return (chunksFinished.size() + chunksFailed.size() + chunksSkipped.size() == totalChunks);
}

void readChunks(const vector<File> &files) {
  try {
    while (true) {
      Chunk * c = chunksToRead.consume();
      if (files[c->parentFileIndex].matchStatus == File::Status::FAILED_TO_MATCH_REMOTE_FILE) {
        // We have already marked file as a non-match, don't waste time reading more chunks from it
        c->log("File status == FAILED_TO_MATCH_REMOTE_FILE, Skipping the read...");
        chunksSkipped.produce(c);
      } else {
        c->log("Reading...");
        c->read();

        c->log("Finished reading");
        chunksToComputeMD5.produce(c);
      }
    }
  } catch (boost::thread_interrupted &ti) {
    return;
  }
}

void verifyChunkMD5(vector<File> &files) {
  try {
    while (true) {
      Chunk * c = chunksToComputeMD5.consume();
      if (files[c->parentFileIndex].matchStatus == File::Status::FAILED_TO_MATCH_REMOTE_FILE) {
        // We have already marked file as a non-match, don't waste time reading more chunks from it
        c->log("File status == FAILED_TO_MATCH_REMOTE_FILE, Skipping the MD5 compute...");
        c->clear();
        chunksSkipped.produce(c);
      } else {
        c->log("Computing MD5...");
        string computedMD5 = c->computeMD5();
        c->clear();
        if (c->expectedMD5 != computedMD5) {
          c->log("MISMATCH between expected MD5 '" + c->expectedMD5 + "', and computed MD5 '" + computedMD5 + "' ... marking the file as Mismatch");
          files[c->parentFileIndex].matchStatus = File::Status::FAILED_TO_MATCH_REMOTE_FILE;
          chunksFailed.produce(c);
        } else {
          c->log("Expected and computed MD5 match!");
          chunksFinished.produce(c);
        }
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
          << "  to compute md5: " << chunksToComputeMD5.size()
          << "  skipped:  " << chunksSkipped.size()
          << "  finished: " << chunksFinished.size()
          << "  failed: " << chunksFailed.size() << endl;

      if (finished()) {
        return;
      }
    }
  }
}

void createWorkerThreads(vector<File> &files) {
  LOG << "Creating worker threads:" << endl;

  LOG << " read..." << endl;
  for (int i = 0; i < opt.readThreads; ++i) {
    readThreads.push_back(boost::thread(readChunks, boost::ref(files)));
  }

  LOG << " md5..." << endl;
  for (int i = 0; i < opt.md5Threads; ++i) {
    md5Threads.push_back(boost::thread(verifyChunkMD5, boost::ref(files)));
  }
}

void interruptWorkerThreads() {
  LOG << "Interrupting worker threads:" << endl;

  LOG << " read..." << endl;
  for (int i = 0; i < (int) readThreads.size(); ++i) {
    readThreads[i].interrupt();
  }

  LOG << " md5..." << endl;
  for (int i = 0; i < (int) md5Threads.size(); ++i) {
    md5Threads[i].interrupt();
  }
}

void joinWorkerThreads() {
  LOG << "Joining worker threads:" << endl;

  LOG << " read..." << endl;
  for (int i = 0; i < (int) readThreads.size(); ++i) {
    readThreads[i].join();
  }

  LOG << " md5..." << endl;
  for (int i = 0; i < (int) md5Threads.size(); ++i) {
    md5Threads[i].join();
  }
}

// This function should be called before opt.setApiserverDxConfig() is called,
// since opt::setApiserverDxConfig() changes the value of dx::config::*, based on command line args
void printEnvironmentInfo() {
  using namespace dx::config;

  cout << "Environment info:" << endl
       << "  API server protocol: " << APISERVER_PROTOCOL() << endl
       << "  API server host: " << APISERVER_HOST() << endl
       << "  API server port: " << APISERVER_PORT() << endl;
  if (SECURITY_CONTEXT().size() != 0)
    cout << "  Auth token: " << SECURITY_CONTEXT()["auth_token"].get<string>() << endl;
  else
    cout << "  Auth token: " << endl;
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
    cout << "dx-verify-file Version: " << DX_VERIFY_FILE_VERSION << endl
         << "git version: " << DXTOOLKIT_GITVERSION << endl;
    return 0;
  } else if (opt.help()) {
    opt.printHelp(argv[0]);
    return 0;
  }

  LOG << "dx-verify-file" << DX_VERIFY_FILE_VERSION << " (git version: " << DXTOOLKIT_GITVERSION << ")" << endl;
  LOG << opt;
  try {
    opt.setApiserverDxConfig();
    opt.validate();
  } catch (exception &e) {
    cerr << "ERROR: " << e.what() << endl;
    opt.printHelp(argv[0]);
    return 1;
  }
 
  chunksToComputeMD5.setCapacity(opt.md5Threads);
  int exitCode = 0; 
  try {
    vector<File> files;

    for (unsigned int i = 0; i < opt.localFiles.size(); ++i) {
      files.push_back(File(opt.localFiles[i], opt.remoteFiles[i], files.size()));
      totalChunks += files[i].createChunks(chunksToRead);
    }

    LOG << "Created " << totalChunks << " chunks." << endl;
    
    createWorkerThreads(files);

    LOG << "Creating monitor thread.." << endl;
    boost::thread monitorThread(monitor);
    
    LOG << "Joining monitor thread..." << endl;
    monitorThread.join();
    LOG << "Monitor thread finished." << endl;

    interruptWorkerThreads();
    joinWorkerThreads();

    for (unsigned int i = 0; i < files.size(); ++i) {
      if (files[i].matchStatus != File::Status::FAILED_TO_MATCH_REMOTE_FILE) {
        cout << "identical" << endl;
      } else {
        exitCode = 4;
        cout << "mismatch" << endl;
      }
    }
    LOG << "Exiting." << endl;
  } catch (exception &e) {
    cerr << "ERROR: " << e.what() << endl;
    return 1;
  }

  return exitCode;
}
