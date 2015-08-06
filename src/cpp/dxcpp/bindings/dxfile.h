// Copyright (C) 2013-2014 DNAnexus, Inc.
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

/** \file
 *
 * \brief File handles.
 */

#ifndef DXCPP_BINDINGS_DXFILE_H
#define DXCPP_BINDINGS_DXFILE_H

#include <fstream>
#include <sstream>
#include <boost/thread.hpp>
#include "../bqueue.h"
#include "../bindings.h"
#include "../utils.h"

namespace dx {
  //! A remote file handler.

  ///
  /// A File represents an opaque array of bytes (see the <a
  /// href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Files">API specification</a> for more
  /// info). DXFile supports multithreaded uploading and downloading for high performance.
  ///
  /// When a File object is initialized, it is empty, in the "open" state, and writable. In order to
  /// support reliable upload of large files, File objects in the DNAnexus Platform may be written in
  /// multiple parts (possibly in parallel). After you have written all the data you like to the
  /// File, you may <em>close</em> it. The File goes into the "closing" state for finalization. Some
  /// time later, the File goes into the "closed" state and can be used for reading.
  ///
  /// There are three important rules to remember:
  /// - A file in the <b>"open"</b> state can only be used for writing (no "reads"). See: is_open()
  /// - A file in the <b>"closing"</b> state is unusable until it moves into the "closed" state. See: waitOnClose()
  /// - A file in the <b>"closed"</b> state can only be used for reading data (no "writes"). See: is_closed()
  ///
  /// You can write a File object in the following ways, which you may <b>not</b> mix and match:
  ///
  /// - If you are generating a single stream of output, use write() (and flush() when you are done
  ///   writing all the data). %DXFile automatically buffers your data, splits it into parts, and
  ///   uses parallel HTTP requests to upload them to the Platform. Requests to %write() only block
  ///   when the internal buffers are full and all threads are busy.
  /// - If you prefer to have explicit control of parallel writes, you can use uploadPart(). This
  ///   allows you to generate the file data out of order (you can specify a part ID to be associated
  ///   with each write, and the parts are concatenated in increasing order of part ID in order to
  ///   produce the final File). Each uploadPart() call makes one HTTP request and blocks until that
  ///   part has been written.
  /// - Use the static function uploadLocalFile() to write the contents of a local file to a File
  ///   object.
  ///
  /// When you are finished writing data to the File, call close(). If you wish to wait until the
  /// File has been closed before proceeding, you can supply the <code>block=true</code> parameter to
  /// close(); call waitOnClose(); or poll the File's status yourself, using describe().
  ///
  /// To read files, do one of the following (these may all be used concurrently and operate
  /// independently of each other):
  ///
  /// - Use startLinearQuery() to start fetching data in the background in fixed-size chunks (in
  ///   parallel), and getNextChunk() to read it.
  /// - Use read() and gcount() to read from the file (%gcount() returns the number of bytes that
  ///   were read). Use eof() to detect the end of the file and seek() to position the cursor
  ///   manually.
  /// - Use the static function downloadDXFile() to write the contents of a File object to a local
  ///   file (this uses parallel HTTP requests for faster download).
  ///
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
     * Will be true if "upload" method has been called at least once.
     * This allows us to create empty files.
     */
    bool hasAnyPartBeenUploaded;

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
    
    void reset_data_processing_();
    void reset_config_variables_();
    void reset_everything_();
    void copy_config_variables_(const DXFile &to_copy);
    void init_internals_();

    int64_t max_buf_size_;
    int max_write_threads_;

    // To allow interleaving (without compiler optimization possibly changing order)
    // we use std::atomic (a c++11 feature)
    // Ref https://parasol.tamu.edu/bjarnefest/program/boehm-slides.pdf (page 7)
    // Update: Since CLang does not support atomics yet, we are using locking 
    //         mechanism with alongwith volatile
    volatile int countThreadsWaitingOnConsume, countThreadsNotWaitingOnConsume;
    boost::mutex countThreadsMutex;
    std::vector<boost::thread> writeThreads;
    static const int DEFAULT_WRITE_THREADS = 5;
    static const int64_t DEFAULT_BUFFER_MAXSIZE = 100 * 1024 * 1024; // 100 MB
    BlockingQueue<std::pair<std::string, int> > uploadPartRequestsQueue;
    
    // For linear query
    mutable std::map<int64_t, std::string> lq_results_;
    mutable int64_t lq_chunk_limit_;
    mutable int64_t lq_query_start_;
    mutable int64_t lq_query_end_;
    mutable unsigned lq_max_chunks_;
    mutable int64_t lq_next_result_;
    mutable std::string lq_url;
    mutable dx::JSON lq_headers;
    mutable std::vector<boost::thread> lq_readThreads_;
    mutable boost::mutex lq_results_mutex_, lq_query_start_mutex_;

   public:

    DXFile(): DXDataObject() {
      reset_everything_(); 
    }

    /**
     * Copy constructor.
     */
    DXFile(const DXFile& to_copy) : DXDataObject() {
      reset_everything_();
      setIDs(to_copy.dxid_, to_copy.proj_);
      copy_config_variables_(to_copy);
    }
    
    /**
     * Creates a %DXFile handler for the specified File object.
     *
     * @param dxid File object ID.
     * @param proj ID of the project in which to access the object (if NULL, then default workspace will be used).
     */
    DXFile(const char *dxid, const char *proj=NULL): DXDataObject() {
      reset_everything_();
      setIDs(std::string(dxid), (proj == NULL) ? config::CURRENT_PROJECT() : std::string(proj));
    }
   
    /**
     * Creates a %DXFile handler for the specified File object.
     *
     * @param dxid File object ID.
     * @param proj ID of the project in which the File should be accessed.
     */
    DXFile(const std::string &dxid, const std::string &proj=config::CURRENT_PROJECT()): DXDataObject() {
      reset_everything_();
      setIDs(dxid, proj);
    }
    
    /**
     * Creates a %DXFile handler for the specified File object.
     *
     * @param dxlink A JSON representing a <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Details-and-Links#Linking">DNAnexus link</a>.
     *  You may also use the extended form: {"$dnanexus_link": {"project": proj-id, "id": obj-id}}.
     */
    DXFile(const dx::JSON &dxlink): DXDataObject() {
      reset_everything_();
      setIDs(dxlink);
    }

    /**
     * Assignment operator.
     *
     * @note Only ID of the file/project, and config params (max write threads, buffer size) are copied.
     * No state information such as read pointer location, next part ID to upload, etc are copied.
     */
    DXFile& operator=(const DXFile& to_copy) {
      if (this == &to_copy)
        return *this;

      this->setIDs(to_copy.dxid_, to_copy.proj_); // setIDs() will call reset_data_processing_() & init_internals_()
      this->copy_config_variables_(to_copy);
      return *this;
    }

    ~DXFile() {
      flush();
      stopLinearQuery();
    }
    // File-specific functions

    /**
     * Sets the remote File ID associated with this file handler. If the handler had data stored in
     * its internal buffer to be written to the remote file, that data will be flushed.
     *
     * @param dxid new File object ID
     * @param proj ID of the project in which to access the File (if NULL, then default workspace will be used).
     */
    void setIDs(const std::string &dxid, const std::string &proj=config::CURRENT_PROJECT());
    
    /**
     * Sets the remote File ID associated with this file handler. If the handler had data stored in
     * its internal buffer to be written to the remote file, that data will be flushed.
     *
     * @param dxid new File object ID
     * @param proj ID of project in which to access the File.
     */ 
    void setIDs(const char *dxid, const char *proj = NULL);

    /**
     * Sets the remote File ID associated with this file handler. If the handler had data stored in
     * its internal buffer to be written to the remote file, that data will be flushed.
     *
     * @param dxlink A JSON representing a <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Details-and-Links#Linking">DNAnexus link</a>.
     *  You may also use the extended form: {"$dnanexus_link": {"project": proj-id, "id": obj-id}}.
     */
    void setIDs(const dx::JSON &dxlink);

    /**
     * Creates a new remote file object and sets the object ID. Initially the object may be used for
     * writing only.
     *
     * @param media_type String representing the media type of the file.
     * @param data_obj_fields JSON hash containing the optional fields with which to create the
     * object ("project", "types", "details", "hidden", "name", "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Files#API-method%3A-%2Ffile%2Fnew">/file/new</a>
     * API method.
     */
    void create(const std::string &media_type="",
                const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));
    
    /**
     * Returns the buffer size (in bytes) that must be reached before
     * data is flushed.
     *
     * @returns Buffer size, in bytes.
     */
    int64_t getMaxBufferSize() const {
      return max_buf_size_;
    }

    /**
     * Sets the buffer size (in bytes) that must
     * be reached before data is flushed.
     *
     * @param buf_size New buffer size, in bytes, to use (must be >= 5242880 (5MB))
     * \throw DXFileError() if buf_size < 5242880
     */
    void setMaxBufferSize(const int64_t buf_size) {
      if (buf_size < (5 * 1024 * 1024)) { 
        throw DXFileError("Maximum buffer size for DXFile must be >= 5242880 (5MB)");
      }
      max_buf_size_ = buf_size;
    }
    
    /**
     * Returns maximum number of write threads used by parallelized write()
     * operation.
     *
     * @returns Number of threads
     */
    int getNumWriteThreads() const {
      return max_write_threads_;
    }

    /**
     * Sets the maximum number of threads used by parallelized write()
     * operation.
     *
     * @param numThreads Number of threads
     */
    void setNumWriteThreads(const int numThreads) {
      max_write_threads_ = numThreads;
    }

    /**
     * Reads the next <code>n</code> bytes in the remote file object (or all the bytes up to the end
     * of file if there are fewer than <code>n</code>), and stores the downloaded data at
     * <code>ptr</code>. After read() is called, eof() will return whether the end of the file was
     * reached, and gcount() will return how many bytes were actually read.
     *
     * @param ptr Location to which data should be written
     * (user <b>must</b> ensure that sufficient amount of memory has been allocated for data to be written starting at "ptr",
     * before calling this function)
     * @param n The maximum number of bytes to retrieve
     */
    void read(char* ptr, int64_t n);

    /**
     * @return The number of bytes read by the last call to read().
     */
    int64_t gcount() const;

    /**
     * When reading a remote file using read(), returns whether the end of the file has been reached.
     * Calling seek() to set the cursor to before the end of the file causes this flag to be unset.
     *
     * @return Boolean: true if and only if the cursor is at the end of file.
     */
    bool eof() const;

    /**
     * Changes the position of the reading cursor (for read()) to the specified byte offset.
     *
     * Calling this function on a File that is not in the "closed" state will throw an object of
     * class DXFileError.
     *
     * @note This function does not affect reading via startLinearQuery() or getNextChunk().
     * @see read()
     * \throw DXFileError
     * @param pos New byte position of the read cursor
     */
    void seek(const int64_t pos);

    /**
     * Ensures that all the data sent via previous write() calls has been flushed from the buffers
     * and uploaded to the remote File. Finishes all pending uploads and terminates all write
     * threads. This function blocks until the above has completed.
     *
     * This function is idempotent.
     *
     * @note Since this function terminates the thread pool, use it sparingly (for example, only you
     * have finished all your write() requests, to force the data to be written).
     *
     * @see write(const char*, int64_t)
     */
    void flush();

    /**
     * Appends the data stored at <code>ptr</code> to the remote File.
     *
     * The data is written to an internal buffer that is uploaded to the remote file when full.
     *
     * For increased throughput, this function uses multiple threads for uploading data in the
     * background. It will block only if the internal buffer is full and all available workers
     * (MAX_WRITE_THREADS threads) are already busy with HTTP requests. Otherwise, it returns
     * immediately.
     *
     * If any of the threads fails then std::terminate() will be called.
     *
     * @warning Do <b>not</b> mix and match with uploadPart().
     *
     * @see flush()
     * @param ptr Location of data to be written
     * @param n Number of bytes to write
     */
    void write(const char* ptr, int64_t n);

    /**
     * Appends the data in the specified string to the remote File.
     *
     * Same functionality as write(const char*, int64_t).
     *
     * @warning Do <b>not</b> mix and match with uploadPart().
     *
     * @see write(const char*, int64_t)
     * @see flush()
     * @param data String to write to the file
     */
    void write(const std::string &data);

    /**
     * Uploads data as a part. Same functionality as uploadPart(const char*, int64_t, const int).
     *
     * @warning Do <b>not</b> mix and match with write().
     *
     * @see uploadPart(const char*, int64_t, const int)
     *
     * @param data String containing the data to append.
     * @param index Number with which to label the uploaded part.
     */
    void uploadPart(const std::string &data, const int index=-1);

    /**
     * Uploads the <code>n</code> bytes stored at <code>ptr</code> as a part to the remote File.
     * Blocks until the request is completed.
     *
     * If there are multiple requests to write to the same part, the last one to finish "wins".
     *
     * If <code>index</code> is not provided, it defaults to 1 (therefore, possibly overwriting data
     * from other uploadPart() calls that do not specify an <code>index</code>).
     *
     * @warning Do <b>not</b> mix and match with write().
     *
     * @param ptr Pointer to the location of data to be written.
     * @param n The number of bytes to write.
     * @param index Number with which to label the part of the file to be uploaded. If not specified,
     * part 1 is written.
     */
    void uploadPart(const char* ptr, int64_t n, const int index=-1);

    /**
     * @return Boolean: true if and only if the remote file is in the "open" state.
     */
    bool is_open() const;

    /**
     * @return Boolean: true if and only if the remote file is in the "closed" state.
     */
    bool is_closed() const;

    /**
     * Calls flush() and issues a request to close the remote File.
     *
     * See the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Files#API-method%3A-%2Ffile-xxxx%2Fclose">/file-xxxx/close</a>
     * API method for more info.
     *
     * @param block Boolean indicating whether the process should block until the remote file is in
     * the "closed" state (true), or to return immediately (false).
     */
    void close(const bool block=false);

    /**
     * Waits until the remote File object is in the "closed" state.
     */
    void waitOnClose() const;

    /**
     * Starts fetching data in chunks (of the specified byte size) from the remote File in the
     * background. After calling this function, getNextChunk() can be use to access the chunks in
     * order.
     *
     * @note - Calling this function invalidates any previous call to the function
     * (all previously started fetching of chunks is stopped).
     * @note - The queries performed by this function will <b>not</b> update the eof() status.
     *
     * @param start_byte Starting byte offset (0-indexed) from which data will be fetched. Defaults to reading from the beginning of the file.
     * @param num_bytes Total number of bytes to be fetched. If not specified, all data to the end of the file is read.
     * @param chunk_size Number of bytes to be fetched in each chunk. (Each chunk will be this length, except possibly the last one, which may be shorter.)
     * @param max_chunks Number of fetched chunks to be kept in memory at any time. Note that the number of real chunks in memory could be as high as (max_chunks + thread_count).
     * @param thread_count Number of threads to be used for fetching data.
     *
     * @see stopLinearQuery(), getNextChunk()
     */
    void startLinearQuery(const int64_t start_byte=0,
                          const int64_t num_bytes=-1,
                          const int64_t chunk_size=10*1024*1024,
                          const unsigned max_chunks=20,
                          const unsigned thread_count=5) const;

    /**
     * Stops background fetching of all chunks. Terminates all read threads. Any previous call to
     * startLinearQuery() is invalidated.
     *
     * This function is idempotent.
     *
     * @see startLinearQuery(), getNextChunk()
     */
    void stopLinearQuery() const;

    /**
     * Obtains the next chunk of bytes after a call to startLinearQuery(). Returns false if
     * %startLinearQuery() was not called, or if all the requested chunks from the last call to
     * %startLinearQuery() have been exhausted.
     *
     * @note - The queries performed by this function and by startLinearQuery() will <b>not</b> update the eof() status.
     * @note - Calling seek() will not affect this function.
     *
     * @param chunk If this function returns with "true", then this string will be populated with
     * data from next chunk. Otherwise, this string remains untouched.
     *
     * @return "true" if another chunk is available for processing (in which case the value of chunk
     * is copied to the input string). "false" if all chunks have exhausted, or no call to
     * startLinearQuery() was made.
     *
     * @see startLinearQuery(), stopLinearQuery()
     */
    bool getNextChunk(std::string &chunk) const;

    /**
     * Shorthand for creating a DXFile remote File handler with the given object id.
     *
     * @param dxid Object id of the file to open.
     * @return DXFile remote handler for the requested file object
     */
    static DXFile openDXFile(const std::string &dxid);

    /**
     * Shorthand for creating a DXFile remote File handler for a new empty remote File. The newly
     * initialized File is ready for writing.
     *
     * @param media_type String representing the media type of the file.
     * @param data_obj_fields JSON hash containing the optional fields with which to create the
     * object ("project", "types", "details", "hidden", "name", "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Files#API-method%3A-%2Ffile%2Fnew">/file/new</a>
     * API method.
     * @return DXFile remote file handler for a new remote file.
     */
    static DXFile newDXFile(const std::string &media_type="",
                            const dx::JSON &data_obj_fields=
                            dx::JSON(dx::JSON_OBJECT));

    /**
     * Shorthand for downloading a remote File to a local file.
     *
     * The File is downloaded using startLinearQuery() and getNextChunk() semantics. Multiple threads
     * with concurrent HTTP requests are used to fetch the data for higher throughput.
     *
     * @note This should be called only after the remote File is in the "closed" state; otherwise, an
     * error of type DXFileError will be thrown.
     *
     * @param dxid Object handler or id of the file to download.
     * @param filename Local path for writing the downloaded data.
     * @param chunksize Size, in bytes, of each chunk when downloading the file.
     */
    static void downloadDXFile(const std::string &dxid,
                               const std::string &filename,
                               int64_t chunksize=1048576);

    /**
     * Shorthand for uploading a local file and closing it when done.
     * Sets the name to be equal to the filename if no name is provided
     * in data_obj_fields.
     *
     * @param filename Local path for the file to upload.
     * @param media_type String representing the media type of the file.
     * @param data_obj_fields JSON hash containing the optional fields with which to create the
     * object ("project", "types", "details", "hidden", "name", "properties", "tags"), as provided to the <a
     * href="https://wiki.dnanexus.com/API-Specification-v1.0.0/Files#API-method%3A-%2Ffile%2Fnew">/file/new</a>
     * API method.
     * @param waitForClose If set to true, then function returns only after uploaded file is in the
     * "closed" state. Otherwise, returns directly after initiating the file close (the uploaded file
     * will be in the "closing" or "closed" state).
     *
     * @return A remote File handler for the newly uploaded File.
     */
    static DXFile uploadLocalFile(const std::string &filename,
                                  const std::string &media_type="",
                                  const dx::JSON &data_obj_fields=
                                  dx::JSON(dx::JSON_OBJECT),
                                  bool waitForClose=false);

    /**
     * Clones the associated object into the specified project and folder.
     *
     * @param dest_proj_id ID of the project to which the object should be cloned.
     * @param dest_folder Folder route in which to put it in the destination project.
     *
     * @return New object handler with the associated project set to dest_proj_id.
     */
    DXFile clone(const std::string &dest_proj_id,
                 const std::string &dest_folder="/") const;

    /**
     * TODO: Consider writing a uploadString or uploadCharBuffer which
     * uploads the data and closes it after.
     */
  };
}
#endif
