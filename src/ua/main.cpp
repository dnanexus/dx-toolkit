#include <cstdint>
#include <iostream>
#include <queue>

#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"

#include <boost/filesystem.hpp>
#include <boost/thread.hpp>

namespace fs = boost::filesystem;

#include "options.h"
#include "chunk.h"
#include "bqueue.h"

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

BlockingQueue chunksToRead;
BlockingQueue chunksToCompress;
BlockingQueue chunksToUpload;
BlockingQueue chunksFinished;
BlockingQueue chunksFailed;

boost::mutex outputMutex;

void logChunk(Chunk * c, const string &message) {
  boost::unique_lock<boost::mutex> lock(outputMutex);
  cerr << "Chunk " << (*c) << ": " << message << endl;
}

unsigned int createChunks(const string &filename, const string &fileID) {
  cerr << "Creating chunks:" << endl;
  fs::path p(filename);
  const int64_t size = fs::file_size(p);
  unsigned int numChunks = 0;
  for (int64_t start = 0; start < size; start += opt.chunkSize) {
    int64_t end = min(start + opt.chunkSize, size);
    Chunk * c = new Chunk(filename, fileID, opt.tries, start, end);
    logChunk(c, "created");
    chunksToRead.produce(c);
    ++numChunks;
  }
  return numChunks;
}

void readChunks() {
  while (true) {
    Chunk * c = chunksToRead.consume();

    logChunk(c, "Reading...");
    c->readData();

    logChunk(c, "Finished reading.");
    chunksToCompress.produce(c);
  }
}

void compressChunks() {
  while (true) {
    Chunk * c = chunksToCompress.consume();

    logChunk(c, "Compressing...");
    // TODO: compress the chunk

    logChunk(c, "Finished compressing.");
    chunksToUpload.produce(c);
  }
}

void uploadChunks() {
  while (true) {
    Chunk * c = chunksToUpload.consume();

    logChunk(c, "Uploading...");
    // TODO: upload the chunk
    c->clearData();

    logChunk(c, "Finished uploading.");
    chunksFinished.produce(c);
  }
}

void monitor() {
  while (true) {
    boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
    {
      boost::unique_lock<boost::mutex> lock(outputMutex);
      cerr << "In monitor thread." << endl;
      cerr << "  to read: " << chunksToRead.size() << endl
           << "  to compress: " << chunksToCompress.size() << endl
           << "  to upload: " << chunksToUpload.size() << endl
           << "  finished: " << chunksFinished.size() << endl
           << "  failed: " << chunksFailed.size() << endl;

      if (chunksFinished.size() + chunksFailed.size() == totalChunks) {
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
  cerr << "Testing connection to API server...";
  try {
    JSON result = systemFindProjects();
    cerr << " success." << endl;
  } catch (exception &e) {
    cerr << " failure." << endl;
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
  cerr << "Resolving project specifier " << projectSpec << "...";
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
    cerr << " found project ID " << projectID << endl;
    return projectID;
  }

  try {
    JSON params(JSON_OBJECT);
    params["name"] = projectSpec;
    params["level"] = "CONTRIBUTE";

    JSON findResult = systemFindProjects(params);
    JSON projects = findResult["results"];

    if (projects.size() == 0) {
      cerr << " failure." << endl;
      throw runtime_error("\"" + projectSpec + "\" is not a valid project name or ID");
    } else if (projects.size() > 1) {
      cerr << " failure." << endl;
      throw runtime_error("\"" + projectSpec + "\" does not uniquely identify a project");
    } else {
      projectID = projects[0]["id"].get<string>();
      cerr << " found project ID " << projectID << endl;
      return projectID;
    }
  } catch (DXAPIError &e) {
    cerr << "Call to findProjects failed." << endl;
    throw;
  }
}

/*
 * Ensure that we have at least CONTRIBUTE access to the project.
 */
void testProjectPermissions(const string &projectID) {
  cerr << "Testing permissions on project " << projectID << "...";
  try {
    JSON desc = projectDescribe(projectID);
    string level = desc["level"].get<string>();

    if ((level == "CONTRIBUTE") || (level == "ADMINISTER")) {
      cerr << " success." << endl;
      return;
    } else {
      cerr << " failure." << endl;
      throw runtime_error("Permission level " + level + " is not sufficient to create files in project " + projectID);
    }
  } catch (DXAPIError &e) {
    cerr << " call to projectDescribe failed." << endl;
    throw;
  }
}

void testFileExists(const string &filename) {
  cerr << "Testing existence of local file " << filename << "...";
  fs::path p(filename);
  if (fs::exists(p)) {
    cerr << " success." << endl;
  } else {
    cerr << " failure." << endl;
    throw runtime_error("Local file " + filename + " does not exist.");
  }
}

/*
 * Create the folder in which the file object(s) will be created, including
 * any parent folders.
 */
void createFolder(const string &projectID, const string &folder) {
  cerr << "Creating folder " << folder << " and parents in project " << projectID << "...";
  try {
    JSON params(JSON_OBJECT);
    params["folder"] = folder;
    params["parents"] = true;
    projectNewFolder(projectID, params);
    cerr << " success." << endl;
  } catch (DXAPIError &e) {
    cerr << " failure." << endl;
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
  cerr << "Creating new file with parameters " << params.toString() << endl;

  JSON result = fileNew(params);
  cerr << "Got result " << result.toString() << endl;

  return result["id"].get<string>();
}

int main(int argc, char * argv[]) {
  cerr << "DNAnexus Upload Agent" << endl;

  opt.parse(argc, argv);

  if (opt.help() || opt.file.empty()) {
    opt.printHelp();
    return 1;
  }

  cerr << opt;
  opt.validate();

  setAPIServerInfo(opt.apiserverHost, opt.apiserverPort);
  setSecurityContext(securityContext(opt.authToken));
  setProjectContext(opt.project);

  chunksToCompress.setCapacity(opt.threads);
  chunksToUpload.setCapacity(opt.threads);

  try {
    testServerConnection();
    string projectID = resolveProject(opt.project);
    testProjectPermissions(projectID);
    createFolder(projectID, opt.folder);

    testFileExists(opt.file);

    string fileID = createFileObject();
    cerr << "fileID is " << fileID << endl;

    totalChunks = createChunks(opt.file, fileID);
    cerr << "Created " << totalChunks << " chunks." << endl;

    cerr << "Creating read thread..." << endl;
    boost::thread readThread(readChunks);

    cerr << "Creating compress thread.." << endl;
    boost::thread compressThread(compressChunks);

    cerr << "Creating upload thread.." << endl;
    boost::thread uploadThread(uploadChunks);

    cerr << "Creating monitor thread.." << endl;
    boost::thread monitorThread(monitor);

    cerr << "Joining monitor thread..." << endl;
    monitorThread.join();
    cerr << "Monitor thread finished." << endl;

    cerr << "Interrupting worker threads:" << endl;
    cerr << "  Read...";
    readThread.interrupt();
    cerr << endl;
    cerr << "  Compress...";
    compressThread.interrupt();
    cerr << endl;
    cerr << "  Upload...";
    uploadThread.interrupt();
    cerr << endl;

    cerr << "Exiting." << endl;

    // fileClose(fileID);
  } catch (exception &e) {
    cerr << "ERROR: " << e.what() << endl;
    return 1;
  }

  return 0;
}
