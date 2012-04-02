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
  const JSON resp = fileDownload(dxid_);
  string url = resp["url"].get<string>();
  throw DXNotImplementedError();
}

bool DXFile::eof() const {
  return eof_;
}

// TODO: Make this fail when writing and use the pos_ to figure out
// how far along in the buffer we are?
void DXFile::seek(const int pos) {
  pos_ = pos;
}

void DXFile::flush() {
  uploadPart(buffer_, cur_part_);
  buffer_ = "";
  cur_part_++;
}

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
  JSON input_params(JSON_OBJECT);
  if (index >= 0)
    input_params["index"] = index;

  const JSON resp = fileUpload(dxid_, input_params);
  HttpHeaders req_headers;
  req_headers["Content-Length"] = data.size();

  HttpClientRequest req;

  req.setUrl(resp["url"].get<string>());
  req.setReqData(data.data(), data.size());
  req.setMethod("POST");
  req.setHeaders(req_headers);
  req.send();

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
    waitOnState();
}

void DXFile::waitOnClose() const {
  waitOnState();
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
  char chunkbuff[chunksize];
  while (!dxid.eof_) {
    read(chunkbuff, chunksize);
    localfile.write(chunkbuff, chunksize);
    // TODO: but how many bytes were read before reaching eof_?
  }
  throw DXNotImplementedError();
}

DXFile DXFile::uploadLocalFile(const string &filename, const string &media_type) {
  throw DXNotImplementedError();
  return DXFile();
}

DXFile DXFile::uploadString(const string &to_upload, const string &media_type) {
  throw DXNotImplementedError();
  return DXFile();
}
