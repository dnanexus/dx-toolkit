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

#include "File.h"
#include "dxcpp/dxcpp.h"

#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>

namespace fs = boost::filesystem;

#include "log.h"

using namespace std;
using namespace dx;

void testLocalFileExists(const string &filename) {
  LOG << "Testing existence of local file " << filename << "...";
  fs::path p(filename);
  if (fs::exists(p)) {
    LOG << " success." << endl;
  } else {
    LOG << " failure." << endl;
    throw runtime_error("Local file " + filename + " does not exist.");
  }
}

int numberOfCompletedParts(const dx::JSON &parts) {
  int64_t numParts = 0;
  for (dx::JSON::const_object_iterator it = parts.object_begin(); it != parts.object_end(); ++it) {
    if (it->second["state"].get<string>() == "complete") {
      numParts++;
    }
  }
  return numParts;
}

File::File(const std::string &localFile_, const std::string &remoteFile_, const unsigned int fileIndex_):
            localFile(localFile_), remoteFile(remoteFile_), fileIndex(fileIndex_) {
  init();
}

void File::init() {
  using namespace dx;
  testLocalFileExists(localFile);
  matchStatus = Status::MATCH_SUCCESSFUL_OR_IN_PROGRESS;
  fs::path p(localFile);
  size = fs::file_size(p);

  //Call file describe on remote file to get "parts" size
  JSON inp(JSON_HASH);
  inp["parts"] = true;
  JSON out;
  try {
    out = fileDescribe(remoteFile, inp);
  } catch (exception &e) {
    throw runtime_error("Call to describe remote file (" + remoteFile + ") failed. Error message: " + e.what());
  }
  if (!out.has("parts") || out["parts"].type() != JSON_HASH) {
    throw runtime_error("Describe call output does not contain 'parts' key (or it's not a hash): Unexpected. Output from describe call: '" + out.toString() + "'");
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Now do a quick sanity check
  //  - Remote file size must match local file size (if file is "closed")
  //  - All parts must be in "completed" state, and sum of total size of all parts == local file size
  // If any of these check fail, mark the file as a non-match directly
  assert(out.has("state") && out["state"].type() == JSON_STRING);
  if (out["state"].get<string>() == "closed") {
    assert(out.has("size") && out["size"].type() == JSON_INTEGER);
    if (out["size"].get<int64_t>() != size) {
      LOG << "Size of local file '" << localFile << "' & remote file '" << remoteFile << "' differ. Marking it as a non-match" << endl;
      matchStatus = Status::FAILED_TO_MATCH_REMOTE_FILE;
      return;
    }
  } else {
      throw runtime_error("Remote file ('" + remoteFile + "') is not in 'closed' state.\n"
                          "This program should only be used for 'closed' files.");
  }

  int64_t totalPartSize = 0;
  for (JSON::object_iterator it = out["parts"].object_begin(); it != out["parts"].object_end(); ++it) {
    // Assert the structure of each value in "parts" hash
    assert(it->second.type() == JSON_HASH);
    assert (it->second.has("state") && it->second["state"].type() == JSON_STRING);
    if (it->second["state"].get<string>() != "complete") {
      throw runtime_error("Part ID: '" + it->first + "' of remote file ('" + remoteFile + "') is not in 'complete' state.\n"
                          "This program should only be used once all parts are in 'complete' state");
    }
    assert(it->second.has("size") && it->second.has("md5") &&
           it->second["size"].type() == JSON_INTEGER && it->second["md5"].type() == JSON_STRING);
    totalPartSize += it->second["size"].get<int64_t>();
  }
  if (totalPartSize != size) {
    LOG << "Size of local file '" << localFile << "' & sum of all part sizes of remote file '" << remoteFile << "' differ. Marking it as a non-match" << endl;
    matchStatus = Status::FAILED_TO_MATCH_REMOTE_FILE;
    return;
  }
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  parts = out["parts"]; // copy "parts" hash in a local variable in File class
}

unsigned int File::createChunks(BlockingQueue<Chunk *> &queue) {
  using namespace dx;
  // For creating chunks:
  //  1) We sort the part IDs in increasing order (by pushing them all in a std::map after converting to int)
  //  2) We start creating file chunks

  if (matchStatus == Status::FAILED_TO_MATCH_REMOTE_FILE) {
    return 0; // we have already marked the file as a non-match, no need to create chunks
  }

  // We will do it in 2 pass .. first create all "keys" in the map .. then in second pass we will add their sizes etc
  map<int, JSON> chunkInfo;
  // Note: We are not asserting for "parts" JSON structure, since it has been already done in init()
  for (JSON::object_iterator it = parts.object_begin(); it != parts.object_end(); ++it) {
    chunkInfo[boost::lexical_cast<int>(it->first)] = JSON::parse("{\"md5\": \"" + it->second["md5"].get<string>() + "\"}");
  }

  // second pass: Add start and end location for chunks in the local file
  int64_t start = 0;
  for (map<int, JSON>::iterator it = chunkInfo.begin(); it != chunkInfo.end(); ++it) {
    it->second["start"] = start;
    start = it->second["end"] = (start + parts[boost::lexical_cast<string>(it->first)]["size"].get<int64_t>());
  }
  // TODO: Sanity check that this works for empty file as well

  LOG << "Creating chunks:" << endl;
  fs::path p(localFile);
  int actualChunksCreated = 0;
  for (map<int, JSON>::iterator it = chunkInfo.begin(); it != chunkInfo.end(); ++it) {
    Chunk *c = new Chunk(localFile, it->second["md5"].get<string>(), it->second["start"].get<int64_t>(), it->second["end"].get<int64_t>(), fileIndex);
    c->log("created");
    queue.produce(c);
    actualChunksCreated++;
  }
  return actualChunksCreated;
}

ostream &operator<<(ostream &out, const File &file) {
  out << file.localFile << " (" << file.remoteFile << ")";
  return out;
}
