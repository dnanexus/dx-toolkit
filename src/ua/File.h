#ifndef UA_FILE_H
#define UA_FILE_H

#include <string>

#include "dxcpp/bqueue.h"
#include "chunk.h"

class File {
public:

  File(const std::string &localFile_,
       const std::string &projectSpec_, const std::string &folder_, const std::string &name_, const bool toCompress_, const bool tryResuming, const std::string &mimeType_, 
       const int64_t chunkSize, const unsigned int fileIndex_);

  void init(const bool tryResuming);

  unsigned int createChunks(BlockingQueue<Chunk *> &queue, const int tries);

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
  
  /* true if all chunks in the file should be compressed before uploading*/
  bool toCompress;
 
  /* true if remote file is in open state, false otherwise.
   * This variable is used for noting  wheter a resumed upload
   * is already in "closing"/"closed" state or not.
   */
  bool isRemoteFileOpen;

  /* Stores the mime-type of file (as identified by libmagic) */
  std::string mimeType;

  /* chunk size for this file*/
  int64_t chunkSize;

  /* Size of the local file to be uploaded */
  int64_t size;

  /* Number of bytes uploaded succesfuly so far from local file */
  int64_t bytesUploaded;
  
  /* Index of this File object in the Files vector (in main.cpp) */
  unsigned int fileIndex;

  friend std::ostream &operator<<(std::ostream &out, const File &file);
  
  /* Returns a string with all the input parameteres serialized in order 
   * (with space as delimiter). This string is used for identifying whether 
   * an upload can be resumed or not 
   */
  static std::string createResumeInfoString(const int64_t fileSize, const int64_t modifiedTimestamp, const bool toCompress, const int64_t chunkSize, const std::string &name);

/*  ~File() {
    delete bytesUploadedMutex;
  }*/
};

#endif
