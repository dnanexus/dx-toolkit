#include <boost/lexical_cast.hpp>
#include "dxfile.h"
#include "SimpleHttp.h"

using namespace std;
using namespace dx;

const int DXFile::max_buf_size_ = 104857600;

void DXFile::init_internals_() {
  pos_ = 0;
  file_length_ = -1;
  buffer_ = "";
  cur_part_ = 1;
  eof_ = false;
}

void DXFile::setID(const string &dxid) {
  if (buffer_.length() > 0)
    flush();

  init_internals_();

  DXClass::setID(dxid);
}

void DXFile::create(const std::string &media_type) {
  JSON input_params(JSON_OBJECT);
  if (media_type != "")
    input_params["media"] = media_type;
  const JSON resp = fileNew(input_params);

  setID(resp["id"].get<string>());
}

void DXFile::read(char* s, int n) {
  const JSON get_DL_url = fileDownload(dxid_);
  string url = get_DL_url["url"].get<string>();

  // TODO: make sure all lower-case works.
  if (file_length_ < 0) {
    HttpClientRequest get_length = HttpClientRequest::head(url);
    file_length_ = boost::lexical_cast<int>(get_length.h_resp["content-length"]);
  }

  if (pos_ >= file_length_) {
    gcount_ = 0;
    return;
  }

  int endbyte = file_length_ - 1;
  if (pos_ + n - 1 < endbyte)
    endbyte = pos_ + n - 1;
  else
    eof_ = true;
  gcount_ = endbyte - pos_ + 1;

  HttpHeaders headers;
  headers["Range"] = "bytes=" + boost::lexical_cast<string>(pos_) + "-" + boost::lexical_cast<string>(endbyte);
  pos_ = endbyte + 1;

  HttpClientRequest resp = HttpClientRequest::get(url, headers);
  if (resp.responseCode != 200)
    throw DXFileError();
}

int DXFile::gcount() const {
  return gcount_;
}

bool DXFile::eof() const {
  return eof_;
}

// TODO: Make this fail when writing and use the pos_ to figure out
// how far along in the buffer we are?
void DXFile::seek(const int pos) {
  pos_ = pos;
  if (pos_ < file_length_)
    eof_ = false;
}

void DXFile::flush() {
  uploadPart(buffer_, cur_part_);
  buffer_ = "";
  cur_part_++;
}

// NOTE: If needed, optimize in the future to not have to copy to
// append to buffer_ before uploading the next part.
void DXFile::write(const char* s, int n) {
  int remaining_buf_size = max_buf_size_ - buffer_.size();
  if (n < remaining_buf_size) {
    buffer_.append(s, n);
  } else {
    buffer_.append(s, remaining_buf_size);
    flush();
    write(s + remaining_buf_size, n - remaining_buf_size);
  }
}

void DXFile::write(const string &data) {
  write(data.data(), data.size());
}

void DXFile::uploadPart(const string &data, const int index) {
  uploadPart(data.data(), data.size(), index);
}

void DXFile::uploadPart(const char *ptr, int n, const int index) {
  JSON input_params(JSON_OBJECT);
  if (index >= 0)
    input_params["index"] = index;

  const JSON resp = fileUpload(dxid_, input_params);
  HttpHeaders req_headers;
  req_headers["Content-Length"] = n;

  HttpClientRequest req = HttpClientRequest::post(resp["url"].get<string>(),
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
  if (buffer_.size() > 0)
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

DXFile DXFile::newDXFile(const string &media_type) {
  DXFile dxfile;
  dxfile.create(media_type);
  return dxfile;
}

void DXFile::downloadDXFile(const string &dxid, const string &filename, int chunksize) {
  DXFile dxfile(dxid);
  ofstream localfile(filename.c_str());
  char chunkbuf[chunksize];
  while (!dxfile.eof_) {
    dxfile.read(chunkbuf, chunksize);
    int num_bytes = dxfile.gcount();
    localfile.write(chunkbuf, num_bytes);
  }
  localfile.close();
}

DXFile DXFile::uploadLocalFile(const string &filename, const string &media_type) {
  DXFile dxfile = newDXFile(media_type);
  ifstream localfile(filename.c_str());
  char * buf = new char [DXFile::max_buf_size_];
  try {
    while (!localfile.eof()) {
      localfile.read(buf, DXFile::max_buf_size_);
      int num_bytes = localfile.gcount();
      dxfile.uploadPart(buf, num_bytes);
    }
  } catch (int e) {
    // TODO: Make sure this captures all exceptions.
    delete [] buf;
    localfile.close();
    throw e;
  }
  delete[] buf;
  // TODO: Make sure I need to do this.
  localfile.close();
  return dxfile;
}
