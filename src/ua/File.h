#ifndef UA_FILE_H
#define UA_FILE_H

#include <string>

#include "dxcpp/bqueue.h"

#include "chunk.h"

class File {
public:

  File(const std::string &localFile_,
       const std::string &projectSpec_, const std::string &folder_, const std::string &name_);

  void init(void);

  unsigned int createChunks(BlockingQueue<Chunk *> &queue, const int chunkSize, const int tries);

  void close(void);

  void updateState(void);

  /* Name of the local file to be uploaded. */
  std::string localFile;

  /* File object ID. */
  std::string fileID;

  /* Destination project specifier (name or ID). */
  std::string projectSpec;

  /* Destination project ID. */
  std::string projectID;

  /* Destination folder name. */
  std::string folder;

  /* Destination file name. */
  std::string name;

  /* Set to true if one or more chunks of the file fails to be uploaded. */
  bool failed;

  /* Whether to wait for this file to be closed before exiting. */
  bool waitOnClose;

  /* Whether this file is in the closed state. */
  bool closed;

  friend std::ostream &operator<<(std::ostream &out, const File &file);

};

#endif
