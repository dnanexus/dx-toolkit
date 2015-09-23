// Copyright (C) 2009-2011, 2013-2015 DNAnexus, Inc.
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

#include "Compress.h"
#include <stdlib.h>
#include <cassert>

#define Assert assert

namespace Compress
{
  gzstreambuf::gzstreambuf()
  {
    error = true;
    open = false;
    buf = NULL;
  }

  gzstreambuf::~gzstreambuf()
  {
    Assert(!buf);
  }

  void gzstreambuf::Init(FILE *f_, bool write_)
  {
    write = write_;
    Open(f_);
    if (error)
      return;

    buf = (char *)malloc(sizeof(char) * bufsize);
    if (!buf)
      throw AllocError("Out of memory");
    //Assert(setbuf(buf, bufsize));

    if (write)
      setp(buf, buf + bufsize - 1);
    else
      setg(buf, buf + bufsize, buf + bufsize);

    error = false;
  }

  void gzstreambuf::Done()
  {
    Close();

    if (buf)
    {
      free((void *)buf);
      buf = NULL;
    }
  }

  void gzstreambuf::Open(FILE *f_)
  {
    fd = fileno(f_);
    if (!write)
      gz = gzdopen(fd, "rb");
    else
      gz = gzdopen(fd, "wb");
    error = (gz == NULL);
    open = !error;
  }

  void gzstreambuf::Close()
  {
    if (open)
    {
      if (sync() != 0)
        error = true;

      int result = gzclose(gz);
      if (result != Z_OK)
        error = true;

      open = false;
    }
  }

  void gzstreambuf::Reset()
  {
    Assert(!write);
    int result = gzrewind(gz);
    if (result != Z_OK)
      error = true;
    else
      setg(buf, buf + bufsize, buf + bufsize);
  }

  int gzstreambuf::overflow(int c)
  {
    Assert(write);

    int w = pptr() - pbase();
    if (c != EOF)
    {
      *pptr() = c;
      ++w;
    }

    int byteswritten = gzwrite(gz, (void *)pbase(), w * sizeof(char));
    if (byteswritten == (w * sizeof(char)))
    {
      setp(buf, buf + bufsize - 1);
      return 0;
    }
    else
    {
      error = true;
      setp(0, 0);
      return EOF;
    }
  }

  int gzstreambuf::underflow()
  {
    Assert(!write);

    int bytesread = gzread(gz, (void *)buf, bufsize * sizeof(char));
    if (bytesread >= 0)
    {
      if (bytesread == 0)
      {
        setg(0, 0, 0);
        return EOF;
      }
      else
      {
        setg(buf, buf, buf + (bytesread / sizeof(char)));
        return *buf & 0xFF;
      }
    }
    else
    {
      error = true;
      setg(0, 0, 0);
      return EOF;
    }
  }

  int gzstreambuf::sync()
  {
    if (write)
    {
      if (pptr() && pptr() > pbase())
        return overflow(EOF);
    }

    return 0;
  }

  bool gzbase::gzOpen(FILE *f_, bool write_)
  {
    buf.Init(f_, write_);
    return buf.Success();
  }

  bool gzbase::gzClose()
  {
    buf.Done();
    return buf.Success();
  }

  void gzbase::gzReset()
  {
    buf.Reset();
  }

  gzifstream::gzifstream() : istream(&buf), f(NULL)
  {
  }

  gzifstream::gzifstream(const string &gz_filename_) : istream(&buf), f(NULL)
  {
    open(gz_filename_);
  }

  void gzifstream::open(const string &gz_filename_)
  {
    close();
    gz_filename = gz_filename_;
    f = fopen(gz_filename.c_str(), "rb");
    if (!f)
      throw FileOpenError(gz_filename);

    if (!gzOpen(f, false))
    {
      fclose(f);
      f = NULL;
      throw FileTypeError(gz_filename);
    }
  }

  gzifstream::~gzifstream()
  {
    close();
  }

  void gzifstream::close()
  {
    if (f)
    {
      gzClose();
      fclose(f);
      f = NULL;
    }
  }

  void gzifstream::reset()
  {
    gzReset();
  }


  gzofstream::gzofstream() : ostream(&buf), f(NULL)
  {
  }

  gzofstream::gzofstream(const string &gz_filename_) : ostream(&buf), f(NULL)
  {
    open(gz_filename_);
  }

  void gzofstream::open(const string &gz_filename_)
  {
    close();
    gz_filename = gz_filename_;
    f = fopen(gz_filename.c_str(), "wb");
    if (!f)
      throw FileOpenError(gz_filename);

    if (!gzOpen(f, true))
    {
      fclose(f);
      f = NULL;
      throw FileOpenError(gz_filename);
    }
  }

  gzofstream::~gzofstream()
  {
    close();
  }

  void gzofstream::close()
  {
    if (f)
    {
      bool write_error = !gzClose();

      fclose(f);
      f = NULL;

      if (write_error)
        throw FileWriteError(gz_filename);
    }
  }

  bz2streambuf::bz2streambuf()
  {
    error = true;
    buf = NULL;
    bzfile = NULL;
  }

  bz2streambuf::~bz2streambuf()
  {
  }

  void bz2streambuf::Init(FILE *f_, bool write_)
  {
    write = write_;
    Open(f_);
    if (error)
      return;

    buf = (char *)malloc(sizeof(char) * bufsize);
    if (!buf)
      throw AllocError("Out of memory");
    //Assert(setbuf(buf, bufsize));

    if (write)
      setp(buf, buf + bufsize - 1);
    else
      setg(buf, buf + bufsize, buf + bufsize);

    error = false;
  }

  void bz2streambuf::Done()
  {
    Close();

    if (buf)
    {
      free((void *)buf);
      buf = NULL;
    }
  }

  void bz2streambuf::Open(FILE *f_)
  {
    f = f_;
    if (!write)
      bzfile = BZ2_bzReadOpen(&bzerror, f, 0, 0, NULL, 0);
    else
      bzfile = BZ2_bzWriteOpen(&bzerror, f, 5, 0, 0);
    error = (bzerror != BZ_OK);
  }

  void bz2streambuf::Close()
  {
    if (bzfile)
    {
      if (sync() != 0)
        error = true;

      if (!write)
        BZ2_bzReadClose(&bzerror, bzfile);
      else
      {
        unsigned int nbytes_in, nbytes_out;
        BZ2_bzWriteClose(&bzerror, bzfile, error ? 1 : 0,
                         &nbytes_in, &nbytes_out);
      }
      bzfile = NULL;
    }
  }

  void bz2streambuf::Reset()
  {
    Assert(!write);
    Close();
    fseek(f, 0, SEEK_SET);
    Open(f);
    setg(buf, buf + bufsize, buf + bufsize);
  }

  int bz2streambuf::overflow(int c)
  {
    Assert(write);

    int w = pptr() - pbase();
    if (c != EOF)
    {
      *pptr() = c;
      ++w;
    }

    BZ2_bzWrite(&bzerror, bzfile, pbase(), w * sizeof(char));
    if (bzerror == BZ_OK)
    {
      setp(buf, buf + bufsize - 1);
      return 0;
    }
    else
    {
      error = true;
      setp(0, 0);
      return EOF;
    }
  }

  int bz2streambuf::underflow()
  {
    Assert(!write);

    int charsread = BZ2_bzRead(&bzerror, bzfile, (void *)buf, bufsize * sizeof(char)) / sizeof(char);
    if ((bzerror == BZ_OK) || (bzerror == BZ_STREAM_END))
    {
      if (charsread == 0)
      {
        setg(0, 0, 0);
        return EOF;
      }
      else
      {
        setg(buf, buf, buf + charsread);
        return *buf & 0xFF;
      }
    }
    else
    {
      error = true;
      setg(0, 0, 0);
      return EOF;
    }
  }

  int bz2streambuf::sync()
  {
    if (write)
    {
      if (pptr() && pptr() > pbase())
        return overflow(EOF);
    }

    return 0;
  }

  bool bz2base::bzOpen(FILE *f_, bool write_)
  {
    buf.Init(f_, write_);
    return buf.Success();
  }

  bool bz2base::bzClose()
  {
    buf.Done();
    return buf.Success();
  }

  void bz2base::bzReset()
  {
    buf.Reset();
  }

  bz2ifstream::bz2ifstream() : istream(&buf), f(NULL)
  {
  }

  bz2ifstream::bz2ifstream(const string &bz2_filename_) : istream(&buf), f(NULL)
  {
    open(bz2_filename_);
  }

  void bz2ifstream::open(const string &bz2_filename_)
  {
    close();
    bz2_filename = bz2_filename_;
    f = fopen(bz2_filename.c_str(), "rb");
    if (!f)
      throw FileOpenError(bz2_filename);

    if (!bzOpen(f, false))
    {
      fclose(f);
      f = NULL;
      throw FileTypeError(bz2_filename);
    }
  }

  bz2ifstream::~bz2ifstream()
  {
    close();
  }

  void bz2ifstream::close()
  {
    if (f)
    {
      bzClose();
      fclose(f);
      f = NULL;
    }
  }

  void bz2ifstream::reset()
  {
    bzReset();
  }

  bz2ofstream::bz2ofstream() : ostream(&buf), f(NULL)
  {
  }

  bz2ofstream::bz2ofstream(const string &bz2_filename_) : ostream(&buf), f(NULL)
  {
    open(bz2_filename_);
  }

  void bz2ofstream::open(const string &bz2_filename_)
  {
    close();
    bz2_filename = bz2_filename_;
    f = fopen(bz2_filename.c_str(), "wb");
    if (!f)
      throw FileOpenError(bz2_filename);

    if (!bzOpen(f, true))
    {
      fclose(f);
      f = NULL;
      throw FileOpenError(bz2_filename);
    }
  }

  bz2ofstream::~bz2ofstream()
  {
    close();
  }

  void bz2ofstream::close()
  {
    if (f)
    {
      bool write_error = !bzClose();

      fclose(f);
      f = NULL;

      if (write_error)
        throw FileWriteError(bz2_filename);
    }
  }

  FileSniffer::FileSniffer()
  {
    stream_ptr = NULL;
  }

  istream * FileSniffer::Open(const string &filename)
  {
    ifstream sniff_stream(filename.c_str(), ios_base::in | ios_base::binary);
    if (!sniff_stream.good())
      return NULL;
    string magic = "xx";
    sniff_stream.read(&magic[0], 2);
    sniff_stream.close();

    if (magic == "BZ")
    {
      bz_stream.open(filename);
      stream_ptr = &bz_stream;
    }
    else if (magic[0] == (char)0x1f && magic[1] == (char)0x8b)
    {
      gz_stream.open(filename);
      stream_ptr = &gz_stream;
    }
    else
    {
      normal_stream.open(filename.c_str());
      stream_ptr = &normal_stream;
    }

    return stream_ptr;
  }
}
