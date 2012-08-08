#include <vector>
#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>
#include "dxfile.h"
#include "SimpleHttp.h"

using namespace std;
using namespace dx;

const int64_t DXFile::max_buf_size_ = 104857600;

void DXFile::init_internals_() {
  pos_ = 0;
  file_length_ = -1;
  buffer_.str(string());
  cur_part_ = 1;
  eof_ = false;
}

void DXFile::setIDs(const string &dxid, const string &proj) {
  if (buffer_.tellp() > 0)
    flush();

  init_internals_();

  DXDataObject::setIDs(dxid, proj);
}

void DXFile::create(const std::string &media_type,
		    const dx::JSON &data_obj_fields) {
  JSON input_params = data_obj_fields;
  if (!data_obj_fields.has("project"))
    input_params["project"] = g_WORKSPACE_ID;
  if (media_type != "")
    input_params["media"] = media_type;
  const JSON resp = fileNew(input_params);

  setIDs(resp["id"].get<string>(), input_params["project"].get<string>());
}

void DXFile::read(char* ptr, int64_t n) {
  gcount_ = 0;
  const JSON get_DL_url = fileDownload(dxid_);
  const string url = get_DL_url["url"].get<string>();

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
  pos_ = endbyte + 1;

  HttpRequest resp = HttpRequest::request(HTTP_GET, url, headers);
  if ((resp.responseCode < 200) ||
      (resp.responseCode >= 300)) {
    throw DXFileError("HTTP Response code: " +
		      boost::lexical_cast<string>(resp.responseCode) +
		      " when downloading.");
  }

  memcpy(ptr, resp.respData.data(), resp.respData.length());
  gcount_ = resp.respData.length();
}

/////////////////////////////////////////////////////////////////////////////////
void DXFile::startLinearQuery(const int64_t start_byte,
                      const int64_t num_bytes,
                      const int64_t chunk_size,
                      const unsigned max_chunks,
                      const unsigned thread_count) {
  stopLinearQuery(); // Stop any previously running linear query
  
  lq_query_start_ = (start_byte == -1) ? 0 : start_byte;
  lq_query_end_ = (num_bytes == -1) ? describe()["size"].get<int64_t>() : lq_query_start_ + num_bytes;
  lq_chunk_limit_ = chunk_size;
  lq_max_chunks_ = max_chunks;
  lq_next_result_ = lq_query_start_;

  const JSON get_DL_url = fileDownload(dxid_);
  lq_url = get_DL_url["url"].get<string>();

  for (unsigned i = 0; i < thread_count; ++i)
    lq_readThreads_.push_back(boost::thread(boost::bind(&DXFile::readChunk_, this)));
}

// Do *NOT* call this function with value of "end" past the (last - 1) byte of file, i.e.,
// the Range: [start,end] should be a valid byte range in file (shouldn't be past the end of file)
void DXFile::getChunkHttp_(int64_t start, int64_t end, string &result) {
  int64_t last_byte_in_result = start - 1;
  while(last_byte_in_result < end) {
    HttpHeaders headers;
    string range = boost::lexical_cast<string>(last_byte_in_result + 1) + "-" + boost::lexical_cast<string>(end);
    headers["Range"] = "bytes=" + range;
    HttpRequest resp = HttpRequest::request(HTTP_GET, lq_url, headers);
    if (resp.responseCode < 200 || resp.responseCode >= 300) {
      throw DXFileError("HTTP Response code: " +
            boost::lexical_cast<string>(resp.responseCode) +
            " when trying to download Range: [" + range + "] from remote file (url: '" + lq_url + "')");
    }
    if (resp.respData.size() == 0) {
      throw new DXFileError("Unable to download Range: [" + range + "] from url: '" + lq_url + "'. Server returned 200 status code, but response size = 0 bytes (unexpected)");
    }
    if (result == "")
      result = resp.respData;
    else
      result.append(resp.respData);

    last_byte_in_result += resp.respData.size();
  }
  assert(result.size() == (end - start + 1));
}

void DXFile::readChunk_() {
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
    while(lq_next_result_ != start && lq_results_.size() >= lq_max_chunks_) {
      r_lock.unlock();
      usleep(100);
      r_lock.lock();
    }
    lq_results_[start] = tmp;
    r_lock.unlock();
  }
}

bool DXFile::getNextChunk(string &chunk) {
  if (lq_readThreads_.size() == 0) // Linear query was not called
    return false;

  boost::mutex::scoped_lock r_lock(lq_results_mutex_);
  if (lq_next_result_ >= lq_query_end_)
    return false;
  while(lq_results_.size() == 0 || (lq_results_.begin()->first != lq_next_result_)) {
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

void DXFile::stopLinearQuery() {
  if (lq_readThreads_.size() == 0)
    return;
  for (unsigned i = 0; i < lq_readThreads_.size(); ++i) {
    lq_readThreads_[i].interrupt();
    lq_readThreads_[i].join();
  }
  lq_readThreads_.clear();

  //TODO: Ensure that all locks are released at this point
}
/////////////////////////////////////////////////////////////////////////////////

int64_t DXFile::gcount() const {
  return gcount_;
}

bool DXFile::eof() const {
  return eof_;
}

// TODO: Make this fail when writing and use the pos_ to figure out
// how far along in the buffer we are?
void DXFile::seek(const int64_t pos) {
  pos_ = pos;
  if (pos_ < file_length_)
    eof_ = false;
}

void DXFile::flush() {
  uploadPart(buffer_.str(), cur_part_);
  buffer_.str(string());
  cur_part_++;
}

// NOTE: If needed, optimize in the future to not have to copy to
// append to buffer_ before uploading the next part.
void DXFile::write(const char* ptr, int64_t n) {
  int64_t remaining_buf_size = max_buf_size_ - buffer_.tellp();
  if (n < remaining_buf_size) {
    buffer_.write(ptr, n);
  } else {
    buffer_.write(ptr, remaining_buf_size);
    flush();
    write(ptr + remaining_buf_size, n - remaining_buf_size);
  }
}

void DXFile::write(const string &data) {
  write(data.data(), data.size());
}

void DXFile::uploadPart(const string &data, const int index) {
  uploadPart(data.data(), data.size(), index);
}

void DXFile::uploadPart(const char *ptr, int64_t n, const int index) {
  JSON input_params(JSON_OBJECT);
  if (index >= 1)
    input_params["index"] = index;

  const JSON resp = fileUpload(dxid_, input_params);
  HttpHeaders req_headers;
  req_headers["Content-Length"] = boost::lexical_cast<string>(n);

  HttpRequest req = HttpRequest::request(HTTP_POST,
					 resp["url"].get<string>(),
					 req_headers,
					 ptr, n);

  if (req.responseCode != 200) {
    throw DXFileError();
  }
}

bool DXFile::is_open() const {
  const JSON resp = describe();
  return (resp["state"].get<string>() == "open");
}

bool DXFile::is_closed() const {
  const JSON resp = describe();
  return (resp["state"].get<string>() == "closed");
}

void DXFile::close(const bool block) {
  if (buffer_.tellp() > 0)
    flush();

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
                               const JSON &data_obj_fields) {
  DXFile dxfile = newDXFile(media_type, data_obj_fields);
  ifstream localfile(filename.c_str());
  char * buf = new char [DXFile::max_buf_size_];
  try {
    while (!localfile.eof()) {
      localfile.read(buf, DXFile::max_buf_size_);
      int64_t num_bytes = localfile.gcount();
      dxfile.write(buf, num_bytes);
    }
  } catch (...) {
    // TODO: Make sure this captures all exceptions.
    delete [] buf;
    localfile.close();
    throw;
  }
  delete[] buf;
  localfile.close();

  JSON name_prop(JSON_OBJECT);
  name_prop["name"] = getBaseName(filename);
  dxfile.setProperties(name_prop);
  dxfile.close();
  return dxfile;
}

DXFile DXFile::clone(const string &dest_proj_id,
                     const string &dest_folder) const {
  clone_(dest_proj_id, dest_folder);
  return DXFile(dxid_, dest_proj_id);
}
