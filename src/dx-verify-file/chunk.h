#ifndef UA_CHUNK_H
#define UA_CHUNK_H

#include <cstdint>
#include <string>
#include <vector>
#include <iostream>
#include <ctime>
#include <queue>
#include <boost/thread.hpp>

class Chunk {
public:

  Chunk(const std::string &localFile_, const std::string &md5, const int64_t start_, const int64_t end_, const unsigned parentFileIndex_)
    : localFile(localFile_), expectedMD5(md5), start(start_), end(end_), parentFileIndex(parentFileIndex_)
  {
  }

  /* Path of the local file of which this chunk is a part */
  std::string localFile;
  
  /* Expected MD5 of the chunk */
  std::string expectedMD5;

  /* Offset of the beginning of this chunk within the file */
  int64_t start;

  /* Offset of the end of this chunk within the file */
  int64_t end;

  /* Chunk data -- the bytes read from local file (compute md5 for these) */
  std::vector<char> data;

  /* Index of parent file in Files vector (in main.cpp) */
  unsigned parentFileIndex;

  void read();
  std::string computeMD5();
  void clear();

  void log(const std::string &message) const;
  
  friend std::ostream &operator<<(std::ostream &out, const Chunk &chunk);

private:

  std::string uploadURL() const;
};

#endif
