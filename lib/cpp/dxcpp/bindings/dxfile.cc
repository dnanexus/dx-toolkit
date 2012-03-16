#include "dxfile.h"

void DXFile::setID(const string &dxid) {
}

void DXFile::create() {
}

string DXFile::read() {
  return "";
}

void DXFile::seek() {
}

void DXFile::flush() {
}

void DXFile::write() {
}

void DXFile::upload_part() {
}

bool DXFile::is_open() const {
  return true;
}

void DXFile::close(const bool block) const {
}

void DXFile::wait_on_close() const {
  this->wait_on_state();
}

DXFile openDXFile(const string &dxid) {
  return DXFile();
}

DXFile newDXFile(const string &mediaType) {
  return DXFile();
}

void downloadDXFile(const string &dxid, const string &filename, int chunksize) {
}

DXFile uploadLocalFile(const string &filename, const string &media_type) {
  return DXFile();
}

DXFile uploadString(const string &to_upload, const string &media_type) {
  return DXFile();
}
