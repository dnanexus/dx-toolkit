// Copyright (C) 2009-2011, 2013 DNAnexus, Inc.
//
// This file is part of wig_importer.
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

#ifndef COMPRESS_H
#define COMPRESS_H

#include <string>
#include <fstream>
#include "Exceptions.h"
#include "bzlib.h"
#include "zlib.h"

using namespace std;

namespace Compress
{

  // Exceptions
  class FileOpenError : public RuntimeError
  {
    public:
      FileOpenError(const string &message) : RuntimeError(message) { }
      ~FileOpenError() throw() { }
  };

  class FileTypeError : public RuntimeError
  {
    public:
      FileTypeError(const string &message) : RuntimeError(message) { }
      ~FileTypeError() throw() { }
  };

  class FileWriteError : public RuntimeError
  {
    public:
      FileWriteError(const string &message) : RuntimeError(message) { }
      ~FileWriteError() throw() { }
  };

  class AllocError : public RuntimeError
  {
    public:
      AllocError(const string &message) : RuntimeError(message) { }
      ~AllocError() throw() { }
  };

  // internal classes

  class gzstreambuf : public streambuf
  {
  public:
    gzstreambuf();
    ~gzstreambuf();
    void Init(FILE *f_, bool write_);
    void Done();

    bool Success() const { return !error; }

    void Open(FILE *f_);
    void Close();
    void Reset();

  protected:
    virtual int overflow(int c = EOF);
    virtual int underflow();
    virtual int sync();

  private:
    int fd;
    bool open;
    gzFile gz;
    bool write, error;
    static const int bufsize = 65536;
    char *buf;
  };

  class gzbase : virtual public ios
  {
  protected:
    bool gzOpen(FILE *f_, bool write_);
    bool gzClose();
    void gzReset();

    gzstreambuf buf;
  };

  // gzip input file stream

  class gzifstream : public gzbase, public istream
  {
  public:
    gzifstream();
    gzifstream(const string &gz_filename_);
    ~gzifstream();

    void open(const string &gz_filename_);
    void close();
    void reset();

  private:
    string gz_filename;
    FILE *f;
  };

  // gzip output file stream

  class gzofstream : public gzbase, public ostream
  {
  public:
    gzofstream();
    gzofstream(const string &gz_filename_);
    ~gzofstream();

    void open(const string &gz_filename_);
    void close();

  private:
    string gz_filename;
    FILE *f;
  };

  // internal classes

  class bz2streambuf : public streambuf
  {
  public:
    bz2streambuf();
    ~bz2streambuf();
    void Init(FILE *f_, bool write_);
    void Done();

    bool Success() const { return !error; }

    void Open(FILE *f_);
    void Close();
    void Reset();

  protected:
    virtual int overflow(int c = EOF);
    virtual int underflow();
    virtual int sync();

  private:
    FILE *f;
    BZFILE *bzfile;
    int bzerror;
    bool write, error;
    static const int bufsize = 65536;
    char *buf;
  };

  class bz2base : virtual public ios
  {
  protected:
    bool bzOpen(FILE *f_, bool write_);
    bool bzClose();
    void bzReset();

    bz2streambuf buf;
  };

  // bzip2 input file stream

  class bz2ifstream : public bz2base, public istream
  {
  public:
    bz2ifstream();
    bz2ifstream(const string &bz2_filename_);
    ~bz2ifstream();

    void open(const string &bz2_filename_);
    void close();
    void reset();

  private:
    string bz2_filename;
    FILE *f;
  };

  // bzip2 output file stream

  class bz2ofstream : public bz2base, public ostream
  {
  public:
    bz2ofstream();
    bz2ofstream(const string &bz2_filename_);
    ~bz2ofstream();

    void open(const string &bz2_filename_);
    void close();

  private:
    string bz2_filename;
    FILE *f;
  };

  class FileSniffer
  {
  public:
    FileSniffer();
    istream * Open(const string &filename);
  private:
    bz2ifstream bz_stream;
    gzifstream gz_stream;
    ifstream normal_stream;
    istream * stream_ptr;
  };

}

#endif
