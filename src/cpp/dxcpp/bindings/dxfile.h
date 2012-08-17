#ifndef DXCPP_BINDINGS_DXFILE_H
#define DXCPP_BINDINGS_DXFILE_H

#include <fstream>
#include <sstream>
#include <boost/thread.hpp>
#include "../bqueue.h"
#include "../bindings.h"

/**
 * Remote file handler class.
 *
 * Three important rules to remember:
 * - A file in <b>"open"</b> state can only be used for writing (no "reads"). See: is_open()
 * - A file in <b>"closed"</b> state can only be used for reading data (no "writes"). See: is_closed()
 * - A file in <b>"closing"</b> state is unusable, and it must move into "closed" state
 *   for any meaningful operation (only "read") to be performed over it. See: waitOnClose()
 */
class DXFile: public DXDataObject {
 private:
  dx::JSON describe_(const std::string &s)const{return fileDescribe(dxid_,s);}
  void addTypes_(const std::string &s)const{fileAddTypes(dxid_,s);}
  void removeTypes_(const std::string &s)const{fileRemoveTypes(dxid_,s);}
  dx::JSON getDetails_(const std::string &s)const{return fileGetDetails(dxid_,s);}
  void setDetails_(const std::string &s)const{fileSetDetails(dxid_,s);}
  void setVisibility_(const std::string &s)const{fileSetVisibility(dxid_,s);}
  void rename_(const std::string &s)const{fileRename(dxid_,s);}
  void setProperties_(const std::string &s)const{fileSetProperties(dxid_,s);}
  void addTags_(const std::string &s)const{fileAddTags(dxid_,s);}
  void removeTags_(const std::string &s)const{fileRemoveTags(dxid_,s);}
  void close_(const std::string &s)const{fileClose(dxid_,s);}
  dx::JSON listProjects_(const std::string &s)const{return fileListProjects(dxid_,s);}
  
  // For async write() ///////////////////
  void joinAllWriteThreads_();
  void writeChunk_();
  void createWriteThreads_();
  ///////////////////////////////////////
 
  // For linear query ///////////////////////////////////////////////
  void readChunk_() const;
  void getChunkHttp_(int64_t start, int64_t end, std::string& result) const;
  ///////////////////////////////////////////////////////////////////

  /**
   * For use when reading closed remote files; stores the current
   * position (in bytes from the beginning of the file) from which
   * future read() calls will begin.
   */
  int64_t pos_;

  /**
   * Stores the number of bytes read in the last call to read().
   */
  int64_t gcount_;

  /**
   * For use when reading closed remote files; stores length of the
   * file so that accurate byte ranges can be requested.
   */
  int64_t file_length_;

  /**
   * For use when writing remote files; stores a buffer of data that
   * will be periodically flushed to the API server.
   */
  std::stringstream buffer_;

  /**
   * For use when writing remote files; stores the part index to be
   * used on the next part to be uploaded to the API server.
   */
  int cur_part_;

  /**
   * Indicates when end of file has been reached when reading a remote
   * file.
   */
  bool eof_;
 
  /**
   * This is used for disallowing seek() when file is in not "closed" state
   * Initially it should always be set to false.
   * Every is_closed() call sets the value of this variable to it's return value.
   * And if is_closed_ is set to true, then all subsequent call to is_closed() return true
   * without actually making the HTTP request.
   * to actually check the state of file, since a file once "closed" cannot be un-"closed".
   *
   * Defined as "mutable", since const member function is_closed() modified it's value.
   */
  mutable bool is_closed_;

  void init_internals_();

  // TODO: Determine if this should be user-defined.
  static const int64_t max_buf_size_;
  
  // To allow interleaving (without compiler optimization possibly changing order)
  // we use std::atomic (a c++11 feature)
  // Ref https://parasol.tamu.edu/bjarnefest/program/boehm-slides.pdf (page 7)
  // Update: Since CLang does not support atomics yet, we are using locking 
  //         mechanism with alongwith volatile
  volatile int countThreadsWaitingOnConsume, countThreadsNotWaitingOnConsume;
  boost::mutex countThreadsMutex;
  std::vector<boost::thread> writeThreads;
  static const int MAX_WRITE_THREADS = 5;
  BlockingQueue<std::pair<std::string, int> > uploadPartRequestsQueue;
  
  // For linear query
  mutable std::map<int64_t, std::string> lq_results_;
  mutable int64_t lq_chunk_limit_;
  mutable int64_t lq_query_start_;
  mutable int64_t lq_query_end_;
  mutable unsigned lq_max_chunks_;
  mutable int64_t lq_next_result_;
  mutable std::string lq_url;
  mutable std::vector<boost::thread> lq_readThreads_;
  mutable boost::mutex lq_results_mutex_, lq_query_start_mutex_;

 public:

  DXFile() { init_internals_(); }

  DXFile(const DXFile& to_copy) : DXDataObject(to_copy) {
    setIDs(to_copy.dxid_, to_copy.proj_);
  }

  DXFile(const std::string &dxid, const std::string &proj=g_WORKSPACE_ID) {
    setIDs(dxid, proj);
  }

  DXFile& operator=(const DXFile& to_copy) {
    if (this == &to_copy)
      return *this;

    this->setIDs(to_copy.dxid_, to_copy.proj_);
    return *this;
  }
  
  ~DXFile() {
    flush();
    stopLinearQuery();
  }
  // File-specific functions

  /**
   * Sets the remote object ID associated with the remote file
   * handler.  If the handler had data stored in its internal buffer
   * to be written to the remote file, that data will be flushed.
   *
   * @param dxid Object ID of the remote file to be accessed
   */
  void setIDs(const std::string &dxid, const std::string &proj=g_WORKSPACE_ID);

  /**
   * Creates a new remote file object.  Sets the object ID for the
   * DXFile instance which can then be used for writing only.
   *
   * @param media_type String representing the media type of the file.
   * @param data_obj_fields JSON containing the optional fields with
   * which to create the object ("project", "types", "details",
   * "hidden", "name", "properties", "tags")
   */
  void create(const std::string &media_type="",
	      const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

  /**
   * Reads the next n bytes in the remote file object (or however many
   * are left in the file if there are fewer than n), and stores the
   * downloaded data at ptr.  Note that eof() will return whether the
   * end of the file was reached, and gcount() will return how many
   * bytes were actually read.
   *
   * @param ptr Location to which data should be written
   * @param n The maximum number of bytes to retrieve
   */
   // TODO: Make clear that it's user's responsibility to allocate memory
   //       before calling this function
  void read(char* ptr, int64_t n);

  /**
   * @return The number of bytes read by the last call to read().
   */
  int64_t gcount() const;

  /**
   * When reading a remote file using read(), returns whether the end of the file
   * has been reached.  If the end of the file has been reached but
   * seek() has been called to set the cursor to appear before the end
   * of the file, then the flag is unset.
   *
   * @return Boolean: true if the end of the file has been reached;
   * false otherwise.
   */
  bool eof() const;

  /**
   * Changes the position of the reading cursor (for read()) to the specified byte
   * location.  Note that writing is append-only. So calling this function
   * is file is not in "closed" state will throw object of class DXFileError.
   *
   * @note This function does not affect reading via startLinearQuery(), getNextChunk()
   * @see read()
   * \throw DXFileError
   * @param pos New byte position of the read cursor
   */
  void seek(const int64_t pos);

  /**
   * Ensures that all the data send to preceding write() calls is flushed from buffer uploaded
   * to remote file. As a result write Buffer is empty after the call finishes, and all write
   * threads are terminated (after finishing pending uploads). Blocks until then. Idempotent.
   *
   * @note Since this function terminates the thread pool at the end. Thus it is wise to
   * use it less frequently (for ex: at the end of all write() requests, to ensure that
   * data is actually uploaded to remote file).
   * @see write(const char*, int64_t)
   */
  void flush();

  /**
   * Appends the data stored at ptr to an internal buffer that is
   * periodically flushed to be appended to the remote file.
   *
   * For increasing throughput, this function uses multiple threads 
   * for uploading data in background. It will block only if MAX_WRITE_THREADS 
   * number of threads (i.e., all the workers) are already busy completing 
   * previous HTTP request(s), else it will pass on the task to one of 
   * the free worker thread and return immediatly.
   *
   * @warning Do *NOT* mix and match with uploadPart()
   * @see flush()
   * @param ptr Location of data to be written
   * @param n Number of bytes to write
   */
  void write(const char* ptr, int64_t n);

  /**
   * Appends data to the file. Same functionality as write(const char*, int64_t).
   * See write(const char*, int64_t) for more details.
   *
   * @see write(const char*, int64_t)
   * @see flush() 
   * @param data String to write to the file
   */
  void write(const std::string &data);

  /**
   * Uploads data as a part. Same functionality as uploadPart(const char*, int64_t, const int).
   * See uploadPart(const char*, int64_t, const int) for details.
   *
   * @see uploadPart(const char*, int64_t, const int)
   *
   * @param data String containing the data to append
   * @param index Number with which to label the uploaded part
   */
  void uploadPart(const std::string &data, const int index=-1);

  /**
   * Uploads the n bytes stored at ptr to the remote file and appends
   * it to the existing content.  If index is not given, it will not
   * be passed to the API server. Blocks until the request is completed.
   * 
   * @warning Do *NOT* mix and match with write()
   *
   * @param ptr Pointer to the location of data to be sent
   * @param n The number of bytes to send
   * @param index Number with which to label the part of the file to
   * be uploaded.  This will be automatically generated if the given
   * value is negative.
   */
  void uploadPart(const char* ptr, int64_t n, const int index=-1);

  /**
   * @return Boolean: true if the remote file is in the "open" state.
   */
  bool is_open() const;

  /**
   * @return Boolean: true if the remote file is in the "closed"
   * state.
   */
  bool is_closed() const;

  /**
   * Calls flush() and issue request for closing the remote file.
   *
   * @param block Boolean indicating whether the process should block
   * until the remote file is in the "closed" state (true), or not
   * (false).
   */
  void close(const bool block=false);

  /**
   * Waits until the remote file object is in the "closed" state.
   */
  void waitOnClose() const;

  /** 
   * Start fetching data in chunks of specified bytes from the file in background.
   * After calling this function, getNextChunk() can be use to access chunks in a
   * linear manner.
   * 
   * @note - Calling this function, invalidates any previous call to the function
   * (all previously started fetching of chunks is stopped).
   * @note - The queries performed by this function will *NOT* update eof() status.
   * @param start_byte Location (0-indexed) starting from which
   * data will be fetched.
   * @param num_bytes Number of bytes to be fetched
   * @param chunk_size Number of bytes to be fetched in each chunk
   * (except possibly the last one, which can be shorter)
   * @param max_chunks An indicative number for chunks to be kept in memory
   * at any time. Note number of real chunks in memory would be < (max_chunks + thread_count)
   * @param thread_count Number of threads to be used for fetching data.
   * @see stopLinearQuery(), getNextChunk()
   */
  void startLinearQuery(const int64_t start_byte=-1,
                        const int64_t num_bytes=-1,
                        const int64_t chunk_size=10*1024*1024,
                        const unsigned max_chunks=20,
                        const unsigned thread_count=5) const;
  
  /**
   * All fetching of chunks in background is stopped, and read threads terminated.
   * - Invalidates previous call to startLinearQuery() (if any).
   * - Idempotent.
   * @see startLinearQuery(), getNextChunk()
   */
  void stopLinearQuery() const;
  
  /**
   * This function is used after calling startLinearQuery() to get next chunk of bytes
   * (in order). If startLinearQuery() was not called, or all chunks asked for
   * have been exhausted then it returns "false".
   *
   * @note - The queries performed by this function will *NOT* update eof() status.
   * @note - Calling seek() will not affect this function.
   * @param chunk If function returns with "true", then this string will be populated
   * with data from next chunk. If "false" is returned, then
   * this variable remain untouched.
   *
   * @return "true" if another chunk is available for processing (value of chunk is
   * copied to string passed in as input param "chunk"). "false" if all chunks
   * have exhausted, or no call to startLinearQuery() was made.
   * @see startLinearQuery(), stopLinearQuery()
   */
  bool getNextChunk(std::string &chunk) const;
 
  /**
   * Shorthand for creating a DXFile remote file handler with the
   * given object id.
   *
   * @param dxid Object id of the file to open.
   * @return DXFile remote handler for the requested file object
   */
  static DXFile openDXFile(const std::string &dxid);

  /**
   * Shorthand for creating a DXFile remote file handler for a new
   * empty remote file ready for writing.
   *
   * @param media_type String representing the media type of the file.
   * @param data_obj_fields JSON containing the optional fields with
   * which to create the object ("project", "types", "details",
   * "hidden", "name", "properties", "tags")
   * @return DXFile remote file handler for a new remote file
   */
  static DXFile newDXFile(const std::string &media_type="",
                          const dx::JSON &data_obj_fields=
                          dx::JSON(dx::JSON_OBJECT));

  /**
   * Shorthand for downloading a remote file to a local location.
   * 
   * File is downloaded using startLinearQuery() and getNextChunk() 
   * semantics. Thus effectively multiple threads fetch data from remote
   * file at the same time for faster download.
   * @note Should be called only after the remote file is in "closed" state,
   * else an error of type DXFileError will be thrown. 
   * @param dxfile Object handler or id of the file to download.
   * @param filename Local path for writing the downloaded data.
   * @param chunksize Size of the chunks with which to divide up the
   * download (in bytes).
   */
  static void downloadDXFile(const std::string &dxid,
                             const std::string &filename,
                             int64_t chunksize=1048576);

  /**
   * Shorthand for uploading a local file and closing it when done.
   *  
   * @param filename Local path for the file to upload.
   * @param media_type String representing the media type of the file.
   * @param data_obj_fields JSON containing the optional fields with
   * which to create the object ("project", "types", "details",
   * "hidden", "name", "properties", "tags")
   * @param waitForClose If set to true, then function returns only after
   * uploaded file is in closed state, else, may return with file in "closing"
   * state as well.
   * @return DXFile remote file handler for the newly uploaded file.
   */
  static DXFile uploadLocalFile(const std::string &filename,
                                const std::string &media_type="",
                                const dx::JSON &data_obj_fields=
                                dx::JSON(dx::JSON_OBJECT),
                                bool waitForClose=false);

  /**
   * Clones the associated object into the specified project and folder.
   *
   * @param dest_proj_id ID of the project to which the object should
   * be cloned
   * @param dest_folder Folder route in which to put it in the
   * destination project.
   * @return New object handler with the associated project set to
   * dest_proj_id.
   */
  DXFile clone(const std::string &dest_proj_id,
               const std::string &dest_folder="/") const;

  /**
   * TODO: Consider writing a uploadString or uploadCharBuffer which
   * uploads the data and closes it after.
   */
};

#endif
