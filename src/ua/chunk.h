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

#ifndef UA_CHUNK_H
#define UA_CHUNK_H

#include <queue>
#include <ctime>

#include <boost/thread.hpp>

#include "dxjson/dxjson.h"
#include "dxcpp/dxlog.h"
#include "dxcpp/bqueue.h"

#include "options.h"

class Chunk; // forward declaration

/*
 * The variables below are used for computing instanteneous transfer speed: 
 *  1) instantaneousBytesAndTimestampQueue: A queue for keeping track of bytes transferred
 *     (and the timestamp for the same).
 *     (This queue is size limited to a fixed value (see chunk.cpp), so older
 *      values are constantly flushed out, giving an "instanteous" speed).
 *  2) sumOfInstantaneousBytes: maintains the sum of all bytes uploaded in current queue
 *     This allow us to computer average quickly (without traversing the queue and computing
 *     sum every time in uploadProgress function).
 *  3) instantBytesMutex: Mutex for above 2 variables.
 * Note: They are all intialized in chunk.cpp
 */
extern std::queue<std::pair<std::time_t, int64_t> > instantaneousBytesAndTimestampQueue;
extern int64_t sumOfInstantaneousBytes;
extern boost::mutex instantaneousBytesMutex;

/* Upload Agent string (declaration) */
extern std::string userAgentString;

/*
 * These variables are "extern"ed to enable throttling (definition present
 * in main.cpp).
 */
extern unsigned int totalChunks;
extern dx::BlockingQueue<Chunk*> chunksFinished;
extern dx::BlockingQueue<Chunk*> chunksFailed;

class Chunk {
public:

  Chunk(const std::string &localFile_, const std::string &fileID_, const unsigned int index_,
        const unsigned int triesLeft_, const int64_t start_, const int64_t end_, const bool toCompress_, const bool lastChunk_, const unsigned parentFileIndex_)
    : localFile(localFile_), fileID(fileID_), index(index_),
      triesLeft(triesLeft_), start(start_), end(end_), uploadOffset(0), toCompress(toCompress_), lastChunk(lastChunk_), parentFileIndex(parentFileIndex_)
  {
  }
  
  /* Name of the local file of which this chunk is a part */
  std::string localFile;

  /* ID of file object being uploaded */
  std::string fileID;

  /* Index of this chunk within the file */
  unsigned int index;

  /* Number of times we should try to upload this chunk */
  unsigned int triesLeft;

  // TODO: What is the proper type for offsets within a file?

  /* Offset of the beginning of this chunk within the file */
  int64_t start;

  /* Offset of the end of this chunk within the file */
  int64_t end;

  /* Chunk data -- the bytes to be uploaded */
  std::vector<char> data;

  /* While uploading, the offset of the next byte to give to libcurl */
  int64_t uploadOffset;
  
  /* If true, then the chunk will be compressed, else not */
  bool toCompress;

  /* true, if this chunk will be uploaded to last part index in the file */
  bool lastChunk;

  /* Index of parent file in Files vector (in main.cpp) */
  unsigned parentFileIndex;
  
  /* This stores the HTTP response body */
  std::string respData;
 
  /* This stores the md5 sum of chunk (computed by UA) */
  std::string expectedMD5;
  
  /*
   * These variables (hostName and resolvedIP) facilitate DNS round robin
   * scheme in UA.
   */

  /* Host name, extracted from URL returned by /file-xxxx/upload call */
  std::string hostName;
  
  /* Resolved IP for the hostName (using a random IP selector function) */
  std::string resolvedIP;

  void read();
  void compress();
  void upload(Options &opt);
  void clear();

  void log(const std::string &message, const dx::LogLevel level = dx::logINFO) const;
  friend std::ostream &operator<<(std::ostream &out, const Chunk &chunk);

private:

  std::pair<std::string, dx::JSON> uploadURL(Options &opt);
};

#endif
