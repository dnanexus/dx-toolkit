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

  unsigned int createChunks(BlockingQueue<Chunk *> &queue);

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
