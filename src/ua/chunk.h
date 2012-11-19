#ifndef UA_CHUNK_H
#define UA_CHUNK_H

#include <cstdint>
#include <string>
#include <vector>
#include <iostream>
#include <ctime>
#include <queue>
#include <boost/thread.hpp>
#include "SimpleHttp.h"

/** The variables below are used for computing instanteneous transfer speed: 
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

// Upload Agent string (declaration)
extern std::string userAgentString;

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
