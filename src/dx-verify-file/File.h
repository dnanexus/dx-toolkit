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

#ifndef UA_FILE_H
#define UA_FILE_H

#include <string>

#include "dxcpp/bqueue.h"
#include "chunk.h"
#include "dxjson/dxjson.h"

class File {
public:
  enum Status {
    FAILED_TO_MATCH_REMOTE_FILE,
    MATCH_SUCCESSFUL_OR_IN_PROGRESS
  };

  File(const std::string &localFile_, const std::string &remoteFile_, const unsigned int fileIndex_);

  void init();

  unsigned int createChunks(dx::BlockingQueue<Chunk *> &queue);

  void updateState(void);

  /* Path of the local file to be uploaded. */
  std::string localFile;
  
  /* ID of the remote file */
  std::string remoteFile;
  
  /* Set to value from enum in File class */
  Status matchStatus;

  /* Parts JSON from file-xxxx/describe call */
  dx::JSON parts;

  /* Size of the local file to be uploaded */
  int64_t size;
 
  /* Index of this File object in the Files vector (in main.cpp) */
  unsigned int fileIndex;
  
  friend std::ostream &operator<<(std::ostream &out, const File &file);
  
};

#endif
