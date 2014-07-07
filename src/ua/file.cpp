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

#include "file.h"
#include "dxcpp/dxcpp.h"
#include "options.h"

#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>

namespace fs = boost::filesystem;

#include "api_helper.h"
#include "dxcpp/dxlog.h"

using namespace std;
using namespace dx;

string File::createResumeInfoString(const int64_t fileSize, const int64_t modifiedTimestamp, const bool toCompress, const int64_t chunkSize, const string &path) {
  using namespace boost;
  string toReturn;
  toReturn += lexical_cast<string>(fileSize) + " ";
  toReturn += lexical_cast<string>(modifiedTimestamp) + " ";
  toReturn += lexical_cast<string>(toCompress) + " ";
  toReturn += lexical_cast<string>(chunkSize) + " ";
  toReturn += path;
  return toReturn;
}

void testLocalFileExists(const string &filename) {
  DXLOG(logINFO) << "Testing existence of local file " << filename << "...";
  fs::path p(filename);
  if (fs::exists(p)) {
    DXLOG(logINFO) << " success.";
  } else {
    DXLOG(logINFO) << " failure.";
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

/* Returns percentage of the file already uploaded. */
double percentageComplete(const dx::JSON &parts, const int64_t size, const int64_t chunkSize) {
  if (size == 0) {
    return ((parts.has("1") && parts["1"]["state"].get<string>() == "complete") ? 100.0 : 0.0);
  }
  const int completed = numberOfCompletedParts(parts);
  const int lastPartIndex = ((size % chunkSize) == 0) ? int(size / chunkSize) : int(ceil(double(size) / chunkSize));
  bool lastPartDone = false;
  if (parts.has(boost::lexical_cast<string>(lastPartIndex)) && parts[boost::lexical_cast<string>(lastPartIndex)]["state"].get<string>() == "complete") {
    lastPartDone = true;
  }
  const int64_t lastPartSize = size % chunkSize;
  const int64_t totalBytesUploaded = (lastPartDone) ? (((completed - 1) * chunkSize) + lastPartSize) :  (completed * chunkSize);
  return (double(totalBytesUploaded) / size) * 100.0;
}

File::File(const string &localFile_, const string &projectSpec_, const string &folder_, const string &name_,
           const bool toCompress_, const bool tryResuming, const string &mimeType_,
           const int64_t chunkSize_, const unsigned fileIndex_)
  : localFile(localFile_), projectSpec(projectSpec_), folder(folder_), name(name_),
    failed(false), waitOnClose(false), closed(false), toCompress(toCompress_), mimeType(mimeType_),
    chunkSize(chunkSize_), bytesUploaded(0), fileIndex(fileIndex_), atleastOnePartDone(false), jobID() {

  init(tryResuming);
}

void File::init(const bool tryResuming) {
  projectID = resolveProject(projectSpec);

  testLocalFileExists(localFile);

  fs::path p(localFile);
  size = fs::file_size(p);
  if (size == 0) {
    // Never try to compress empty file!
    toCompress = false;
  }
  string remoteFileName = name;

  if (toCompress) 
    remoteFileName += ".gz";

  const int64_t modifiedTimestamp = static_cast<int64_t>(fs::last_write_time(p));
  dx::JSON properties(dx::JSON_OBJECT);

  // Add property {FILE_SIGNATURE_PROPERTY: "<size> <modified time stamp> <toCompress> <chunkSize> <name of file>"
  properties[FILE_SIGNATURE_PROPERTY] = File::createResumeInfoString(size, modifiedTimestamp, toCompress, chunkSize, fs::canonical(p).string());

  DXLOG(logINFO) << "Resume info string: '" << properties[FILE_SIGNATURE_PROPERTY].get<string>() << "'"; 
  dx::JSON findResult;
  if (tryResuming) {
    // Now check if a resumable file already exist in the project
    findResult = findResumableFileObject(projectID, properties[FILE_SIGNATURE_PROPERTY].get<string>());
    if (findResult.size() == 1) {
      fileID = findResult[0]["id"].get<string>();
      double completePercentage;
      string state = findResult[0]["describe"]["state"].get<string>();
      if (state == "closing" || state == "closed") {
        isRemoteFileOpen = false;
        bytesUploaded = size;
        completePercentage = 100.0;
      } else {
        completePercentage = percentageComplete(findResult[0]["describe"]["parts"], size, chunkSize);
        isRemoteFileOpen = true;
      }
      DXLOG(logINFO) << "A resume target is found .. " << endl;
      cerr << "Signature of file " << localFile << " matches remote file " << findResult[0]["describe"]["name"].get<string>() 
           << " (" << fileID << "), which is " << completePercentage << "% complete. Will resume uploading to it." << endl;
      DXLOG(logINFO) << "Remote resume target is in state: \"" << state << "\"";
    }
    if (findResult.size() > 1) {
      cerr << endl << "More than one resumable targets for local file \"" << localFile << "\" found in the project '" + projectID + "', candidates: " << endl;
      for (unsigned i = 0; i < findResult.size(); ++i) {
        cerr << "\t" << (i + 1) << ". " << findResult[i]["describe"]["name"].get<string>() << " (" << findResult[i]["id"].get<string>() << ")" << endl;
      }
      cerr << "Unable to upload: \"" << localFile << "\"" << endl
           << "Please either clean up the potential candidate files, or run upload agent with '--do-not-resume' option" << endl;
      failed = true;
    }
  }
  if (!tryResuming || (findResult.size() == 0)) {
    // Note: It's fine if mimeType is empty string "" (since default for /file/new is anyway empty media type)
    fileID = createFileObject(projectID, folder, remoteFileName, mimeType, properties);
    isRemoteFileOpen = true;
    DXLOG(logINFO) << "fileID is " << fileID << endl;

    cerr << "Uploading file " << localFile << " to file object " << fileID;
  }
}

unsigned int File::createChunks(dx::BlockingQueue<Chunk *> &queue, const int tries) {
  if (failed || (!isRemoteFileOpen)) {
    // This is the case when:
    // 1. Multiple resumable targets exist for a file (an do-not-resume is not set).
    // 2. OR, Remote resumable target is already in "closing" or "closed" state.
    return 0;
  }
  const dx::JSON desc = dx::fileDescribe(fileID);
  // sanity check
  assert(desc["state"].get<string>() == "open");

  // Treat special case of empty file here
  if (size == 0) {
    if (desc["parts"].has("1") && desc["parts"]["1"]["state"].get<string>() == "complete") {
      DXLOG(logINFO) << "Part index 1 for fileID " << fileID << " is in complete state. Will not create an upload chunk for it.";
      atleastOnePartDone = true;
      return 0;
    }
    Chunk * c = new Chunk(localFile, fileID, 0, tries, 0, 0, toCompress, true, fileIndex);
    c->log("created");
    queue.produce(c);
    return 1;
  }

  DXLOG(logINFO) << "Creating chunks:";
  fs::path p(localFile);
  unsigned int countChunks = 0; // to iterate over chunks
  unsigned int actualChunksCreated = 0; // is not incremented for chunks which are already in "complete" state (when resuming)

  for (int64_t start = 0; start < size; start += chunkSize) {
    string partIndex = boost::lexical_cast<string>(countChunks + 1); // minimum part index is 1
    const int64_t end = min(start + chunkSize, size);
    if (desc["parts"].has(partIndex) && desc["parts"][partIndex]["state"].get<string>() == "complete") {
      DXLOG(logINFO) << "Part index " << partIndex << " for fileID " << fileID << " is in complete state. Will not create an upload chunk for it.";
      bytesUploaded += (end - start);
      atleastOnePartDone = true;
    } else { 
      const bool lastChunk = ((start + chunkSize) >= size);
      Chunk * c = new Chunk(localFile, fileID, countChunks, tries, start, end, toCompress, lastChunk, fileIndex);
      c->log("created");
      queue.produce(c);
      actualChunksCreated++;
    }
    ++countChunks;
  }
  return actualChunksCreated++;
}

void File::close(void) {
  closeFileObject(fileID);
}

void File::updateState(void) {
  string state = getFileState(fileID);
  if (state == "closed") {
    DXLOG(logINFO) << "File " << fileID << " is closed.";
  }
  closed = (state == "closed");
}

ostream &operator<<(ostream &out, const File &file) {
  out << file.localFile << " (" << file.fileID << ")";
  return out;
}
