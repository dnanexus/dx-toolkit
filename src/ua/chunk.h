#ifndef UA_CHUNK_H
#define UA_CHUNK_H

#include <cstdint>
#include <string>
#include <vector>
#include <iostream>

class Chunk {
public:

  Chunk(const std::string &localFile_, const std::string &fileID_, const unsigned int index_,
        const unsigned int triesLeft_, const int64_t start_, const int64_t end_)
    : localFile(localFile_), fileID(fileID_), index(index_),
      triesLeft(triesLeft_), start(start_), end(end_), uploadOffset(0)
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

  void read();
  void compress();
  void upload();
  void clear();

  void log(const std::string &message) const;

  friend std::ostream &operator<<(std::ostream &out, const Chunk &chunk);

private:

  std::string uploadURL() const;
};

#endif
