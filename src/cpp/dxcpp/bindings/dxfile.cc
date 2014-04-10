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

#include <vector>
#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp> //include all types plus i/o
#include "dxfile.h"
#include "../utils.h"
#include "SimpleHttp.h"
#include "../dxlog.h"

using namespace std;

namespace dx {
  // A helper function for making http requests with retry logic
  void makeHTTPRequestForFileReadAndWrite(HttpRequest &resp, const string &url, const HttpHeaders &headers, const HttpMethod &method, const char *data = NULL, const size_t size=0u, const int MAX_TRIES = 5) {
    DXLOG(logDEBUG) << "In makeHTTPRequestForFileReadAndWrite(), inputs:" << endl
                    << " --url = '" << url << "'" << endl
                    << " --MAX_TRIES = " << MAX_TRIES << endl
                    << " --data size = '" << size << "'" << endl
                    << " --method = '" << int(method) << "'";
    int retries = 0;
    bool someThingWentWrong = false;
    string wrongThingDescription = "";
    while (true) {
      try {
        DXLOG(logDEBUG) << "Attempting the actual HTTP request ...";
        resp = HttpRequest::request(method, url, headers, data, size);
        DXLOG(logDEBUG) << "Request completed, responseCode = '" << resp.responseCode << "'";
      } catch(HttpRequestException e) {
        DXLOG(logDEBUG) << "HttpRequestException thrown ... message = '" << e.what() << "'";
        someThingWentWrong = true;
        wrongThingDescription = e.what();
      }
      if (!someThingWentWrong && (resp.responseCode < 200 || resp.responseCode >= 300)) {
        someThingWentWrong = true;
        wrongThingDescription = "Response code: '" + boost::lexical_cast<string>(resp.responseCode) + "', Response body: '" + resp.respData + "'";
      }
      if (someThingWentWrong) {
        retries++;
        DXLOG(logDEBUG) << "someThingWentWrong = " << someThingWentWrong << ", retries = " << retries;
        if (retries >= MAX_TRIES) {
          vector<string> hvec = headers.getAllHeadersAsVector();
          string headerStr = "HTTP Headers sent with request:";
          headerStr += (hvec.size() == 0) ? " None\n" : "\n";
          for (int i = 0; i < hvec.size(); ++i) {
            headerStr += "\t" + boost::lexical_cast<string>(i + 1) + ")" +  hvec[i] + "\n";
          }
          throw DXFileError(string("\nERROR while performing : '") + getHttpMethodName(method) + " " + url + "'" + ".\n" + headerStr + "Giving up after " + boost::lexical_cast<string>(retries) + " tries.\nError message: " + wrongThingDescription + "\n");
        }

        DXLOG(logWARNING) << "Retry #" << retries << ": Will start retrying '" << getHttpMethodName(method) << " " << url << "' in " << (1<<retries) << " seconds. Error in previous try: " << wrongThingDescription;
        boost::this_thread::interruption_point();
        _internal::sleepUsingNanosleep(1<<retries);
        DXLOG(logDEBUG) << "Sleep finished, will continue retrying the makeHTTPRequestForFileReadAndWrite() request ...";
        someThingWentWrong = false;
        wrongThingDescription.clear();
        continue; // repeat the same request
      }
      DXLOG(logDEBUG) << "Exiting makeHTTPRequestForFileReadAndWrite() successfully...";
      return;
    }
  }

  void DXFile::init_internals_() {
    pos_ = 0;
    file_length_ = -1;
    buffer_.str(string());
    buffer_.clear();
    cur_part_ = 1;
    eof_ = false;
    is_closed_ = false;
    hasAnyPartBeenUploaded = false;
  }

  void DXFile::reset_config_variables_() {
    max_buf_size_ = DEFAULT_BUFFER_MAXSIZE;
    max_write_threads_ = DEFAULT_WRITE_THREADS;
  }

  void DXFile::copy_config_variables_(const DXFile &to_copy) {
    max_buf_size_ = to_copy.max_buf_size_;
    max_write_threads_ = to_copy.max_write_threads_;
  }

  void DXFile::reset_data_processing_() {
    flush(); // flush will call reset_buffer_() as well
    stopLinearQuery();
    countThreadsWaitingOnConsume = 0;
    countThreadsNotWaitingOnConsume = 0;
  }

  void DXFile::reset_everything_() {
    reset_data_processing_();
    init_internals_();
    reset_config_variables_();
  }

  void DXFile::setIDs(const string &dxid, const string &proj) {
    reset_data_processing_();
    init_internals_();
    DXDataObject::setIDs(dxid, proj);
  }

  void DXFile::setIDs(const char *dxid, const char *proj) {
    if (proj == NULL) {
      setIDs(string(dxid));
    } else {
      setIDs(string(dxid), string(proj));
    }
  }

  void DXFile::setIDs(const JSON &dxlink) {
    reset_data_processing_();
    init_internals_();
    DXDataObject::setIDs(dxlink);
  }

  void DXFile::create(const std::string &media_type,
                      const dx::JSON &data_obj_fields) {
    JSON input_params = data_obj_fields;
    if (!data_obj_fields.has("project"))
      input_params["project"] = config::CURRENT_PROJECT();
    if (media_type != "")
      input_params["media"] = media_type;
    const JSON resp = fileNew(input_params);

    setIDs(resp["id"].get<string>(), input_params["project"].get<string>());
  }

  void DXFile::read(char* ptr, int64_t n) {
    gcount_ = 0;
    JSON req(JSON_HASH);
    req["preauthenticated"] = false;
    const JSON dlResp = fileDownload(dxid_, req);
    const string url = dlResp["url"].get<string>();

    // TODO: make sure all lower-case works.
    if (file_length_ < 0) {
      JSON desc = describe();
      file_length_ = desc["size"].get<int64_t>();
    }

    if (pos_ >= file_length_) {
      gcount_ = 0;
      return;
    }

    int64_t endbyte = file_length_ - 1;
    if (pos_ + n - 1 < endbyte)
      endbyte = pos_ + n - 1;
    else
      eof_ = true;

    HttpHeaders headers;
    headers["Range"] = "bytes=" + boost::lexical_cast<string>(pos_) + "-" + boost::lexical_cast<string>(endbyte);
    for (JSON::const_object_iterator it = dlResp["headers"].object_begin(); it != dlResp["headers"].object_end(); ++it)
      headers[it->first] = it->second.get<string>();

    pos_ = endbyte + 1;

    HttpRequest resp;
    makeHTTPRequestForFileReadAndWrite(resp, url, headers, HTTP_GET);

    memcpy(ptr, resp.respData.data(), resp.respData.length());
    gcount_ = resp.respData.length();
  }

  /////////////////////////////////////////////////////////////////////////////////
  void DXFile::startLinearQuery(const int64_t start_byte,
                        const int64_t num_bytes,
                        const int64_t chunk_size,
                        const unsigned max_chunks,
                        const unsigned thread_count) const {
    if (is_closed() == false)
      throw DXFileError("ERROR: Cannot call DXFile::startLinearQuery() on a file in non-closed state");
    stopLinearQuery(); // Stop any previously running linear query
    lq_query_start_ = (start_byte == -1) ? 0 : start_byte;
    lq_query_end_ = (num_bytes == -1) ? describe()["size"].get<int64_t>() : lq_query_start_ + num_bytes;
    lq_chunk_limit_ = chunk_size;
    lq_max_chunks_ = max_chunks;
    lq_next_result_ = lq_query_start_;
    lq_results_.clear();
    lq_headers.clear();
    
    JSON req(JSON_HASH);
    req["preauthenticated"] = false;
    const JSON dlResp = fileDownload(dxid_, req);
    lq_url = dlResp["url"].get<string>();
    lq_headers = dlResp["headers"];

    for (unsigned i = 0; i < thread_count; ++i)
      lq_readThreads_.push_back(boost::thread(boost::bind(&DXFile::readChunk_, this)));
  }

  // Do *NOT* call this function with value of "end" past the (last - 1) byte of file, i.e.,
  // the Range: [start,end] should be a valid byte range in file (shouldn't be past the end of file)
  void DXFile::getChunkHttp_(int64_t start, int64_t end, string &result) const {
    int64_t last_byte_in_result = start - 1;

    while (last_byte_in_result < end) {
      HttpHeaders headers;
      string range = boost::lexical_cast<string>(last_byte_in_result + 1) + "-" + boost::lexical_cast<string>(end);
      headers["Range"] = "bytes=" + range;
      for (JSON::const_object_iterator it = lq_headers.object_begin(); it != lq_headers.object_end(); ++it)
        headers[it->first] = it->second.get<string>();

      HttpRequest resp;
      makeHTTPRequestForFileReadAndWrite(resp, lq_url, headers, HTTP_GET);

      if (result == "")
        result = resp.respData;
      else
        result.append(resp.respData);

      last_byte_in_result += resp.respData.size();
    }
    assert(result.size() == (end - start + 1));
  }

  void DXFile::readChunk_() const {
    int64_t start;
    while (true) {
      boost::mutex::scoped_lock qs_lock(lq_query_start_mutex_);
      if (lq_query_start_ >= lq_query_end_)
        break; // We are done fetching all chunks

      start = lq_query_start_;
      lq_query_start_ += lq_chunk_limit_;
      qs_lock.unlock();

      int64_t end = std::min((start + lq_chunk_limit_ - 1), lq_query_end_ - 1);

      std::string tmp;
      getChunkHttp_(start, end, tmp);

      boost::mutex::scoped_lock r_lock(lq_results_mutex_);
      while (lq_next_result_ != start && lq_results_.size() >= lq_max_chunks_) {
        r_lock.unlock();
        boost::this_thread::sleep(boost::posix_time::milliseconds(1));
        r_lock.lock();
      }
      lq_results_[start] = tmp;
      r_lock.unlock();
      boost::this_thread::interruption_point();
    }
  }

  bool DXFile::getNextChunk(string &chunk) const {
    if (lq_readThreads_.size() == 0) // Linear query was not called
      return false;

    boost::mutex::scoped_lock r_lock(lq_results_mutex_);
    if (lq_next_result_ >= lq_query_end_)
      return false;

    while (lq_results_.size() == 0 || (lq_results_.begin()->first != lq_next_result_)) {
      r_lock.unlock();
      usleep(100);
      r_lock.lock();
    }
    chunk = lq_results_.begin()->second;
    lq_results_.erase(lq_results_.begin());
    lq_next_result_ += chunk.size();
    r_lock.unlock();
    return true;
  }

  void DXFile::stopLinearQuery() const {
    if (lq_readThreads_.size() == 0)
      return;
    for (unsigned i = 0; i < lq_readThreads_.size(); ++i) {
      lq_readThreads_[i].interrupt();
      lq_readThreads_[i].join();
    }
    lq_readThreads_.clear();
    lq_results_.clear();
  }
  /////////////////////////////////////////////////////////////////////////////////

  int64_t DXFile::gcount() const {
    return gcount_;
  }

  bool DXFile::eof() const {
    return eof_;
  }

  void DXFile::seek(const int64_t pos) {
    // Check if a file is closed before "seeking"
    if (is_closed() == false) {
      throw DXFileError("ERROR: Cannot call DXFile::seek() when a file is not in 'closed' state");
    }
    pos_ = pos;
    if (pos_ < file_length_)
      eof_ = false;
  }

  ///////////////////////////////////////////////////////////////////////

  void DXFile::joinAllWriteThreads_() {
    /* This function ensures that all pending requests are executed and all
     * worker thread are closed after that
     * Brief notes about functioning:
     * --> uploadPartRequestsQueue.size() == 0, ensures that request queue is empty, i.e.,
     *     some worker has picked the last request (note we use term "pick", because
     *     the request might still be executing the request).
     * --> Once we know that request queue is empty, we issue interrupt() to all threads
     *     Note: interrupt() will only terminate threads, which are waiting on new request.
     *           So only threads which are blocked by .consume() operation will be terminated
     *           immediately.
     * --> Now we use a condition based on two interleaved counters to wait until all the
     *     threads have finished the execution. (see writeChunk_() for understanding their usage)
     * --> Once we are sure that all threads have finished the requests, we join() them.
     *     Since interrupt() was already issued, thus join() terminates them instantly.
     *     Note: Most of them would have been already terminated (since after issuing
     *           interrupt(), they will be terminated when they start waiting on consume()).
     *           It's ok to join() terminated threads.
     * --> We clear the thread pool (vector), and reset the counters.
     */

    if (writeThreads.size() == 0) {
      // if no writeThreads are present, uploadPartRequestsQueue should be empty : sanity check
      assert(uploadPartRequestsQueue.size() == 0);
      return; // Nothing to do (no thread has been started)
    }
    // To avoid race condition
    // particularly the case when produce() has been called, but thread is still waiting on consume()
    // we don't want to incorrectly issue interrupt() that time
    while (uploadPartRequestsQueue.size() != 0) {
      usleep(100);
    }

    for (unsigned i = 0; i < writeThreads.size(); ++i)
      writeThreads[i].interrupt();

    boost::mutex::scoped_lock cl(countThreadsMutex);
    cl.unlock();
    while (true) {
      cl.lock();
      if ((countThreadsNotWaitingOnConsume == 0) &&
          (countThreadsWaitingOnConsume == (int) writeThreads.size())) {
        cl.unlock();
        break;
      }
      cl.unlock();
      usleep(100);
    }

    for (unsigned i = 0; i < writeThreads.size(); ++i)
      writeThreads[i].join();

    writeThreads.clear();
    // Reset the counts
    countThreadsWaitingOnConsume = 0;
    countThreadsNotWaitingOnConsume = 0;
  }

  // This function is what each of the worker thread executes
  void DXFile::writeChunk_() {
    try {
      boost::mutex::scoped_lock cl(countThreadsMutex);
      cl.unlock();
      /* This function is executed throughout the lifetime of an addRows worker thread
       * Brief note about various constructs used in the function:
       * --> uploadPartRequestsQueue.consume() will block if no pending requests to be
       *     executed are available.
       * --> uploadPart() does the actual upload of rows.
       * --> We use two interleaved counters (countThread{NOT}WaitingOnConsume) to
       *     know when it is safe to terminate the threads (see joinAllWriteThreads_()).
       *     We want to terminate only when thread is waiting on .consume(), and not
       *     when gtableAddRows() is being executed.
       */
       // See C++11 working draft for details about atomics (used for counters)
       // http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2012/n3337.pdf
      while (true) {
        cl.lock();
        countThreadsWaitingOnConsume++;
        cl.unlock();
        pair<string, int> elem = uploadPartRequestsQueue.consume();
        cl.lock();
        countThreadsNotWaitingOnConsume++;
        countThreadsWaitingOnConsume--;
        cl.unlock();
        uploadPart(elem.first.data(), elem.first.size(), elem.second);
        cl.lock();
        countThreadsNotWaitingOnConsume--;
        cl.unlock();
      }
    }
    catch (const boost::thread_interrupted &ti)
    {
      return;
    }

  }

  /* This function creates new worker thread for addRows
   * Usually it will be called once for a series of addRows() and then close() request
   * However, if we call flush() in between, then we destroy any existing threads, thus
   * threads will be recreated for any further addRows() request
   */
  void DXFile::createWriteThreads_() {
    if (writeThreads.size() == 0) {
      //reset counters if no previous threads exist
      countThreadsWaitingOnConsume = 0;
      countThreadsNotWaitingOnConsume = 0;
      uploadPartRequestsQueue.setCapacity(max_write_threads_);
      for (int i = 0; i < max_write_threads_; ++i) {
        writeThreads.push_back(boost::thread(boost::bind(&DXFile::writeChunk_, this)));
      }
    }
  }

  // NOTE: If needed, optimize in the future to not have to copy to
  // append to buffer_ before uploading the next part.
  void DXFile::write(const char* ptr, int64_t n) {
    int64_t remaining_buf_size = max_buf_size_ - buffer_.tellp();
    if (n < remaining_buf_size) {
      buffer_.write(ptr, n);
    } else {
      buffer_.write(ptr, remaining_buf_size);
      // Create thread pool (if not already created)
      if (writeThreads.size() == 0)
        createWriteThreads_();
      
      // add upload request for this part to blocking queue
      uploadPartRequestsQueue.produce(make_pair(buffer_.str(), cur_part_));
      buffer_.str(string()); // clear the buffer
      cur_part_++; // increment the part number for next request

      // Add remaining data to buffer (will be added in next call)
      write(ptr + remaining_buf_size, n - remaining_buf_size);
      hasAnyPartBeenUploaded = true;
    }
  }

  void DXFile::write(const string &data) {
    write(data.data(), data.size());
  }

  void DXFile::flush() {
    if (buffer_.tellp() > 0) {
      // We have some data to flush before joining all the threads
      // Create thread pool (if not already created)
      if (writeThreads.size() == 0)
         createWriteThreads_();
      uploadPartRequestsQueue.produce(make_pair(buffer_.str(), cur_part_));
      cur_part_++;
      hasAnyPartBeenUploaded = true;
    }
    // Now join all write threads
    joinAllWriteThreads_();
    buffer_.str(string());
  }

  //////////////////////////////////////////////////////////////////////

  void DXFile::uploadPart(const string &data, const int index) {
    uploadPart(data.data(), data.size(), index);
  }

  void DXFile::uploadPart(const char *ptr, int64_t n, const int index) {
    JSON input_params(JSON_OBJECT);
    if (index >= 1)
      input_params["index"] = index;
  
    int MAX_TRIES = 5;
    
    for (int tries = 1; true; ++tries) {
      // we exit this loop in one of the two cases:
      //  1) Total number of tries are exhausted (in which case we "throw")
      //  2) Request is completed (in which case we "break" from the loop)

      HttpHeaders req_headers;
      
      const JSON resp = fileUpload(dxid_, input_params);
      for (JSON::const_object_iterator it = resp["headers"].object_begin(); it != resp["headers"].object_end(); ++it)
        req_headers[it->first] = it->second.get<string>();
      
      req_headers["Content-Length"] = boost::lexical_cast<string>(n);
      req_headers["Content-Type"] = ""; // this is necessary because libcurl otherwise adds "Content-Type: application/x-www-form-urlencoded"
      req_headers["Content-MD5"] = getHexifiedMD5(reinterpret_cast<const unsigned char*>(ptr), n); // Add the content MD5 header 
      HttpRequest resp2;
      try {
        DXLOG(logDEBUG) << "In uploadPart(), index = " << index << ", calling makeHTTPRequestForFileReadAndWrite() ...";
        makeHTTPRequestForFileReadAndWrite(resp2, resp["url"].get<string>(), req_headers, HTTP_POST, ptr, n, 1);
        DXLOG(logDEBUG) << "In uploadPart(), index = " << index << ", makeHTTPRequestForFileReadAndWrite() finished";
        break; // request successfully completed, break from the loop
      } catch (DXFileError &e) {
        DXLOG(logDEBUG) << "DXFileError thrown, tries = " << tries << ", MAX_TRIES = " << MAX_TRIES;
        if (tries >= MAX_TRIES)
          throw DXFileError("POST '" + resp["url"].get<string>() + "' failed after " + boost::lexical_cast<string>(tries) + " number of tries. Giving up. Error message in last try: '" + e.what() + "'");
        int sleep = (1<<tries);
        DXLOG(logWARNING) << "POST '" << resp["url"].get<string>() << "' failed in try #" << tries << " of " << MAX_TRIES << ". Retrying in " << sleep << " seconds ... Error message: '" << e.what() << "'";
        boost::this_thread::interruption_point();
        _internal::sleepUsingNanosleep(sleep);
        DXLOG(logDEBUG) << "Sleep finished, will continue retrying the uploadPart() request...";
      }
    }
    hasAnyPartBeenUploaded = true;
  }

  bool DXFile::is_open() const {
    // If is_closed_ is true, then file cannot be "open"
    // Since initial value of is_closed_ = false, and file cannot be "open" after
    // being "closed" once.
    if (is_closed_ == true)
      return false;
    const JSON resp = describe();
    return (resp["state"].get<string>() == "open");
  }

  bool DXFile::is_closed() const {
    // If is_closed_ is set to true, then we do not need to check
    // since a file cannot be reopened after closing.
    if (is_closed_ == true)
      return true;

    const JSON resp = describe();
    return (is_closed_ = (resp["state"].get<string>() == "closed"));
  }

  void DXFile::close(const bool block) {
    flush();
    // If not part has been uploaded, upload an empty part.
    // This allows creation of empty files
    if (hasAnyPartBeenUploaded == false)
      uploadPart(std::string(""), 1);
    fileClose(dxid_);
    if (block)
      waitOnState("closed");
  }

  void DXFile::waitOnClose() const {
    waitOnState("closed");
  }

  DXFile DXFile::openDXFile(const string &dxid) {
    return DXFile(dxid);
  }

  DXFile DXFile::newDXFile(const string &media_type,
                           const JSON &data_obj_fields) {
    DXFile dxfile;
    dxfile.create(media_type, data_obj_fields);
    return dxfile;
  }

  void DXFile::downloadDXFile(const string &dxid, const string &filename,
                              int64_t chunksize) {
    DXFile dxfile(dxid);
    if (!dxfile.is_closed())
      throw DXFileError("Error: Remote file must be in 'closed' state before it can be downloaded");

    ofstream localfile(filename.c_str());
    dxfile.startLinearQuery(-1, -1, chunksize);
    std::string chunk;
    while (dxfile.getNextChunk(chunk))
      localfile.write(chunk.data(), chunk.size());

    localfile.close();
  }

  static string getBaseName(const string& filename) {
    size_t lastslash = filename.find_last_of("/\\");
    return filename.substr(lastslash+1);
  }

  DXFile DXFile::uploadLocalFile(const string &filename, const string &media_type,
                                 const JSON &data_obj_fields, bool waitForClose) {
    DXFile dxfile = newDXFile(media_type, data_obj_fields);
    ifstream localfile(filename.c_str());
    const int64_t buf_size = dxfile.getMaxBufferSize();
    char * buf = new char [buf_size];
    try {
      while (!localfile.eof()) {
        localfile.read(buf, buf_size);
        int64_t num_bytes = localfile.gcount();
        dxfile.write(buf, num_bytes);
      }
    } catch (...) {
      delete [] buf;
      localfile.close();
      throw;
    }
    delete[] buf;
    localfile.close();
    if (!data_obj_fields.has("name")) {
      dxfile.rename(getBaseName(filename));
    }
    dxfile.close(waitForClose);
    return dxfile;
  }

  DXFile DXFile::clone(const string &dest_proj_id,
                       const string &dest_folder) const {
    clone_(dest_proj_id, dest_folder);
    return DXFile(dxid_, dest_proj_id);
  }
}
