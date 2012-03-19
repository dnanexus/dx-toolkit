#include "dxfile.h"

void DXFile::setID(const string &dxid) {
}

void DXFile::create() {
}

void DXFile::read(char* s, int n) {
}

bool DXFile::eof() const {
  return eof_;
}

void DXFile::seek(const int pos) {
}

void DXFile::flush() {
}

void DXFile::write(const char* s, int n) {
}

void DXFile::uploadPart() {
}

bool DXFile::is_open() const {
  return true;
}

void DXFile::close(const bool block) const {
}

void DXFile::waitOnClose() const {
  waitOnState();
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
