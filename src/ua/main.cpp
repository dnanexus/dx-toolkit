#include <cstdint>
#include <iostream>
#include <queue>

#include <curl/curl.h>

#include <boost/filesystem.hpp>
#include <boost/thread.hpp>

namespace fs = boost::filesystem;

#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"

#include "options.h"
#include "chunk.h"
#include "dxcpp/bqueue.h"
#include "log.h"

#include "SSLThreads.h"

using namespace std;
using namespace dx;

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

unsigned int createChunks(const string &filename, const string &fileID) {
  LOG << "Creating chunks:" << endl;
  fs::path p(filename);
  const int64_t size = fs::file_size(p);
  unsigned int numChunks = 0;
  for (int64_t start = 0; start < size; start += opt.chunkSize) {
    int64_t end = min(start + opt.chunkSize, size);
    Chunk * c = new Chunk(filename, fileID, numChunks, opt.tries, start, end);
    c->log("created");
    chunksToRead.produce(c);
    ++numChunks;
  }
  return numChunks;
}

bool finished() {
  return (chunksFinished.size() + chunksFailed.size() == totalChunks);
}

void readChunks() {
  while (true) {
    Chunk * c = chunksToRead.consume();

    c->log("Reading...");
    c->read();

    c->log("Finished reading");
    chunksToCompress.produce(c);
  }
}

void compressChunks() {
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
}

void uploadChunks() {
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

JSON securityContext(const string &authToken) {
  JSON ctx(JSON_OBJECT);
  ctx["auth_token_type"] = "Bearer";
  ctx["auth_token"] = authToken;
  return ctx;
}

void testServerConnection() {
  LOG << "Testing connection to API server...";
  try {
    JSON result = systemFindProjects();
    LOG << " success." << endl;
  } catch (exception &e) {
    LOG << " failure." << endl;
    throw;
  }
}

/*
 * Given a project specifier (name or ID), resolves it to a project ID.
 *
 * To determine whether projectSpec is an ID, we call describe. If this
 * succeeds or throws PermissionDenied, we have verified that a project
 * with that ID exists (though we may not have permission to access it).
 *
 * If projectSpec is not an ID, we try to determine whether it is a name by
 * calling findProjects on projectSpec, with minimum permission level
 * "CONTRIBUTE". If no results are returned, the projectSpec cannot be
 * resolved; if more than one project is returned, projectSpec is
 * ambiguous; otherwise, the projectSpec is unambiguously a project name.
 */
string resolveProject(const string &projectSpec) {
  LOG << "Resolving project specifier " << projectSpec << "...";
  string projectID;

  try {
    JSON desc = projectDescribe(projectSpec);
    projectID = desc["id"].get<string>();
  } catch (DXAPIError &e) {
    if (e.name == "PermissionDenied") {
      // the project exists, though we don't have access to it
      projectID = projectSpec;
    }
  }

  if (!projectID.empty()) {
    LOG << " found project ID " << projectID << endl;
    return projectID;
  }

  try {
    JSON params(JSON_OBJECT);
    params["name"] = projectSpec;
    params["level"] = "CONTRIBUTE";

    JSON findResult = systemFindProjects(params);
    JSON projects = findResult["results"];

    if (projects.size() == 0) {
      LOG << " failure." << endl;
      throw runtime_error("\"" + projectSpec + "\" is not a valid project name or ID");
    } else if (projects.size() > 1) {
      LOG << " failure." << endl;
      throw runtime_error("\"" + projectSpec + "\" does not uniquely identify a project");
    } else {
      projectID = projects[0]["id"].get<string>();
      LOG << " found project ID " << projectID << endl;
      return projectID;
    }
  } catch (DXAPIError &e) {
    LOG << "Call to findProjects failed." << endl;
    throw;
  }
}

/*
 * Ensure that we have at least CONTRIBUTE access to the project.
 */
void testProjectPermissions(const string &projectID) {
  LOG << "Testing permissions on project " << projectID << "...";
  try {
    JSON desc = projectDescribe(projectID);
    string level = desc["level"].get<string>();

    if ((level == "CONTRIBUTE") || (level == "ADMINISTER")) {
      LOG << " success." << endl;
      return;
    } else {
      LOG << " failure." << endl;
      throw runtime_error("Permission level " + level + " is not sufficient to create files in project " + projectID);
    }
  } catch (DXAPIError &e) {
    LOG << " call to projectDescribe failed." << endl;
    throw;
  }
}

void testFileExists(const string &filename) {
  LOG << "Testing existence of local file " << filename << "...";
  fs::path p(filename);
  if (fs::exists(p)) {
    LOG << " success." << endl;
  } else {
    LOG << " failure." << endl;
    throw runtime_error("Local file " + filename + " does not exist.");
  }
}

/*
 * Create the folder in which the file object(s) will be created, including
 * any parent folders.
 */
void createFolder(const string &projectID, const string &folder) {
  LOG << "Creating folder " << folder << " and parents in project " << projectID << "...";
  try {
    JSON params(JSON_OBJECT);
    params["folder"] = folder;
    params["parents"] = true;
    projectNewFolder(projectID, params);
    LOG << " success." << endl;
  } catch (DXAPIError &e) {
    LOG << " failure." << endl;
    throw runtime_error("Could not create folder " + folder + " in project " + projectID + " (" + e.what() + ")");
  }
}

/*
 * Create the file object. The object is created in the project and folder
 * specified in opt, and with the specified name. The folder and any parent
 * folders are created if they do not exist.
 */
string createFileObject() {
  JSON params(JSON_OBJECT);
  params["project"] = opt.project;
  params["folder"] = opt.folder;
  params["name"] = opt.name;
  params["parents"] = true;
  LOG << "Creating new file with parameters " << params.toString() << endl;

  JSON result = fileNew(params);
  LOG << "Got result " << result.toString() << endl;

  return result["id"].get<string>();
}

void interruptWorkerThreads(boost::thread &readThread, vector<boost::thread> &compressThreads, vector<boost::thread> &uploadThreads) {
  LOG << "Interrupting worker threads:";
  LOG << " read...";
  readThread.interrupt();
  LOG << " compress...";
  for (int i = 0; i < (int) compressThreads.size(); ++i) {
    compressThreads[i].interrupt();
  }
  LOG << " upload...";
  for (int i = 0; i < (int) uploadThreads.size(); ++i) {
    uploadThreads[i].interrupt();
  }
  LOG << endl;
}

void joinWorkerThreads(boost::thread &readThread, vector<boost::thread> &compressThreads, vector<boost::thread> &uploadThreads) {
  LOG << "Joining worker threads:";
  LOG << " read...";
  readThread.join();
  LOG << " compress...";
  for (int i = 0; i < (int) compressThreads.size(); ++i) {
    compressThreads[i].join();
  }
  LOG << " upload...";
  for (int i = 0; i < (int) uploadThreads.size(); ++i) {
    uploadThreads[i].join();
  }
  LOG << endl;
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
    opt.printHelp();
    return 1;
  }

  if (opt.version()) {
    cout << GITVERSION << endl;
    return 0;
  } else if (opt.help() || opt.file.empty()) {
    opt.printHelp();
    return 1;
  }

  Log::enabled = opt.verbose;

  LOG << "DNAnexus Upload Agent " << GITVERSION << endl;

  LOG << opt;
  opt.validate();

  setAPIServerInfo(opt.apiserverHost, opt.apiserverPort, "http");
  setSecurityContext(securityContext(opt.authToken));
  setProjectContext(opt.project);

  chunksToCompress.setCapacity(opt.compressThreads);
  chunksToUpload.setCapacity(opt.uploadThreads);

  try {
    SSLThreadsSetup();
    curlInit();

    testServerConnection();
    string projectID = resolveProject(opt.project);
    testProjectPermissions(projectID);
    createFolder(projectID, opt.folder);

    testFileExists(opt.file);

    string fileID = createFileObject();
    LOG << "fileID is " << fileID << endl;

    cerr << endl
         << "Uploading file " << opt.file << " to DNAnexus file object " << fileID << endl;

    totalChunks = createChunks(opt.file, fileID);
    LOG << "Created " << totalChunks << " chunks." << endl;

    LOG << "Creating read thread..." << endl;
    boost::thread readThread(readChunks);

    vector<boost::thread> compressThreads;
    LOG << "Creating compress threads.." << endl;
    for (int i = 0; i < opt.compressThreads; ++i) {
      compressThreads.push_back(boost::thread(compressChunks));
    }

    vector<boost::thread> uploadThreads;
    LOG << "Creating upload threads.." << endl;
    for (int i = 0; i < opt.uploadThreads; ++i) {
      uploadThreads.push_back(boost::thread(uploadChunks));
    }

    LOG << "Creating monitor thread.." << endl;
    boost::thread monitorThread(monitor);

    LOG << "Joining monitor thread..." << endl;
    monitorThread.join();
    LOG << "Monitor thread finished." << endl;

    interruptWorkerThreads(readThread, uploadThreads, compressThreads);
    joinWorkerThreads(readThread, uploadThreads, compressThreads);

    if (chunksFailed.empty()) {
      cerr << "Upload was successful! Closing file...";
      fileClose(fileID);
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
