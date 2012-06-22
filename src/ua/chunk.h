#ifndef UA_CHUNK_H
#define UA_CHUNK_H

#include <string>
#include <vector>

class Chunk {
public:

  Chunk(const std::string &localFile_, const std::string &fileID_, const unsigned int triesLeft_, const int64_t start_, const int64_t end_)
    : localFile(localFile_), fileID(fileID_), triesLeft(triesLeft_), start(start_), end(end_)
  {
  }

  std::string localFile;
  std::string fileID;
  unsigned int triesLeft;
  // TODO: What is the proper type for offsets within a file?
  int64_t start;
  int64_t end;
  std::vector<char> data;
};

#endif
