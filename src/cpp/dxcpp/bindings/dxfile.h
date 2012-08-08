#ifndef DXCPP_BINDINGS_DXFILE_H
#define DXCPP_BINDINGS_DXFILE_H

#include <fstream>
#include <sstream>
#include <atomic>
#include <boost/thread.hpp>
#include "../bqueue.h"
#include "../bindings.h"

/**
 * TODO: Talk about how "open" is only for writing, "closed" is only
 * for reading, and "closing" is a useless state.
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
 
  // For linear query ///////////////////////////////////////////////
  void readChunk_();
  void getChunkHttp_(int64_t start, int64_t end, std::string& result);
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

  void init_internals_();

  // TODO: Determine if this should be user-defined.
  static const int64_t max_buf_size_;
  
  // For linear query
  std::map<int64_t, std::string> lq_results_;
  int64_t lq_chunk_limit_;
  int64_t lq_query_start_;
  int64_t lq_query_end_;
  unsigned lq_max_chunks_;
  int64_t lq_next_result_;
  std::string lq_url;
  std::vector<boost::thread> lq_readThreads_;
  boost::mutex lq_results_mutex_, lq_query_start_mutex_;

 public:

  DXFile() {}

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
   * When reading a remote file, returns whether the end of the file
   * has been reached.  If the end of the file has been reached but
   * seek() has been called to set the cursor to appear before the end
   * of the file, then the flag is unset.
   *
   * @return Boolean: true if the end of the file has been reached;
   * false otherwise.
   */
  bool eof() const;

  /**
   * Changes the position of the reading cursor to the specified byte
   * location.  Note that writing is append-only, so calling this
   * function when writing will fail.
   *
   * @param pos New byte position of the read cursor
   */
  void seek(const int64_t pos);

  /**
   * Appends the contents of the internal buffer to the remote file.
   */
  void flush();

  /**
   * Appends the data stored at ptr to an internal buffer that is
   * periodically flushed to be appended to the remote file.
   *
   * @param ptr Location of data to be written
   * @param n Number of bytes to write
   */
  void write(const char* ptr, int64_t n);

  /**
   * Appends data to the file.
   *
   * @see DXFile::write(const char*, int)
   *
   * @param data String to write to the file
   */
  void write(const std::string &data);

  /**
   * Uploads data as a part.
   *
   * @see DXFile::uploadPart(const char*, int, int)
   *
   * @param data String containing the data to append
   * @param index Number with which to label the uploaded part
   */
  void uploadPart(const std::string &data, const int index=-1);

  /**
   * Uploads the n bytes stored at ptr to the remote file and appends
   * it to the existing content.  If index is not given, it will not
   * be passed to the API server.
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
   * Flushes the buffer and closes the remote file to further writes.
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
   * Note: Calling this function, invalidates any previous call to the function.
   * @param start_byte Location (0-indexed) starting from which
   * data will be fetched.
   * @param num_bytes Number of bytes to be fetched
   * @param chunk_size Number of bytes to be fetched in each chunk
   * (except possibly the last one, which can be shorter)
   * @param max_chunks An indicative number for chunks to be kept in memory
   * at any time. Note number of real chunks in memory would be < (max_chunks + thread_count)
   * @param thread_count Number of threads to be used for fetching data.
   */
  void startLinearQuery(const int64_t start_byte=-1,
                        const int64_t num_bytes=-1,
                        const int64_t chunk_size=1*1024*1024,
                        const unsigned max_chunks=20,
                        const unsigned thread_count=5);
  
  /**
   * Invalidates previous call to startLinearQuery() (if any).
   * All processing is stopped, and threads terminated.
   * Idempotent.
   */
  void stopLinearQuery();
  
  /**
   * This function is used after calling startLinearQuery() to get next chunk of bytes
   * (in order). If startLinearQuery() was not called, then it returns "false"
   * 
   * @param chunk If function returns with "true", then this string will be populated
   * with data from next chunk. If "false" is returned, then
   * this object remain untouched.
   *
   * @return "true" if another chunk is available for processing (value of chunk is
   * copied to string passed in as input param "chunk"). "false" if all chunks
   * have exhausted, or no call to startLinearQuery() was made.
   */
  bool getNextChunk(std::string &chunk);

  
  // TODO: Provide streaming operators for all reasonable types
  /**
   * Streaming operator for writing
   */
/*  template<typename T>
    DXFile & operator<<(const T& x) {
    buffer_ << x;
    if (buffer_.tellp() >= max_buf_size_)
      flush();
    return *this;
  }

  typedef std::basic_ostream<char, std::char_traits<char> > couttype;
  typedef couttype& (*stdendline)(couttype&);
  DXFile & operator<<(stdendline manipulator) {
    buffer_ << manipulator;
    if (buffer_.tellp() >= max_buf_size_)
      flush();
    return *this;
  }
*/
  /**
   * Things that need figuring out: 1) Would need to buffer reading.
   * pos_ would have to be managed carefully between uses of >> and
   * read().  2) How many bytes are necessary to grab the next thing?
   * In any case, we'd want a read buffer.  And maybe to store the
   * entire thing as a stringstream??
   */
/*  template<typename T>
    DXFile & operator>>(const T& x) {
    throw DXNotImplementedError();
  }
*/

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
   * TODO: Decide whether to provide blocking.
   *
   * @param filename Local path for the file to upload.
   * @param media_type String representing the media type of the file.
   * @param data_obj_fields JSON containing the optional fields with
   * which to create the object ("project", "types", "details",
   * "hidden", "name", "properties", "tags")
   * @return DXFile remote file handler for the newly uploaded file.
   */
  static DXFile uploadLocalFile(const std::string &filename,
                                const std::string &media_type="",
                                const dx::JSON &data_obj_fields=
                                dx::JSON(dx::JSON_OBJECT));

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
