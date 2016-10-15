// Copyright (C) 2013-2016 DNAnexus, Inc.
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

  File(const std::string &localFile_,
       const std::string &projectSpec_, const std::string &folder_, const std::string &name_,
       const std::string &visibility, const dx::JSON &properties_, const dx::JSON &type_,
       const dx::JSON &tags_, const dx::JSON &details,
       const bool toCompress_, const bool tryResuming, const std::string &mimeType_, 
       const int64_t chunkSize, const unsigned int fileIndex_, const bool standardInput_);

  void init();
  void init(const bool tryResuming);

  unsigned int createChunks(dx::BlockingQueue<Chunk *> &queue, const int tries);
  unsigned int readStdin(dx::BlockingQueue<Chunk *> &queue, const int tries);

  void close(void);

  void updateState(void);

  /* Name of the local file to be uploaded. */
  std::string localFile;

  /* File object ID. (or string: "failed" if file upload couldn't be finished succesfuly) */
  std::string fileID;

  /* Destination project specifier (name or ID). */
  std::string projectSpec;

  /* Destination project ID. */
  std::string projectID;

  /* Destination folder name. */
  std::string folder;

  /* Destination file name. */
  std::string name;

  /* Visibility */
  std::string visibility;

  /* JSON object containing the file's properties. */
  dx::JSON properties;
  
  /* List of types specified for this file. */
  dx::JSON type;

  /* List of tags specified for this file. */
  dx::JSON tags;

  /* JSON object containing details specified for this file. */
  dx::JSON details;

  /* Set to true if one or more chunks of the file fails to be uploaded. */
  bool failed;

  /* Whether to wait for this file to be closed before exiting. */
  bool waitOnClose;

  /* Whether this file is in the closed state. */
  bool closed;
  
  /* true if all chunks in the file should be compressed before uploading*/
  bool toCompress;
 
  /* true if remote file is in open state, false otherwise.
   * This variable is used for noting whether a resumed upload
   * is already in "closing"/"closed" state or not.
   */
  bool isRemoteFileOpen;

  /* Stores the mime-type of file (as identified by libmagic) */
  std::string mimeType;

  /* chunk size for this file*/
  uint64_t chunkSize;

  /* Size of the local file to be uploaded */
  uint64_t size;

  /* Number of bytes uploaded succesfuly so far from local file */
  uint64_t bytesUploaded;
  
  /* Index of this File object in the Files vector (in main.cpp) */
  unsigned int fileIndex;
  
  /* This is a hack for displaying percentage complete for "empty" file correctly
   * As the name suggests, any succesful part upload will set it to true */
  bool atleastOnePartDone;
  
  /*
   * Job ID of the import app called for this file, or:
   *
   * - the string "failed" if the import app could not be invoked for some
   *   reason (e.g., file upload couldn't finish, no reference genome
   *   found, etc.);
   *
   * - the empty string if no import app was asked by user to be called.
   */
  std::string jobID;

  /* File content comes from stdin.*/
  bool standardInput;

  friend std::ostream &operator<<(std::ostream &out, const File &file);
  
  /* 
   * Returns a string with all the input parameters serialized in order
   * (with space as delimiter). This string is used for identifying whether
   * an upload can be resumed or not.
   */
  static std::string createResumeInfoString(const uint64_t fileSize, const int64_t modifiedTimestamp,
                                            const bool toCompress, const uint64_t chunkSize, const std::string &path);

};

#endif
