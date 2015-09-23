// Copyright (C) 2013-2015 DNAnexus, Inc.
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

#include "chunk.h"

#include <stdexcept>
#include <fstream>
#include <sstream>

#include <boost/thread.hpp>
#include <boost/lexical_cast.hpp>

#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"
#include "dxcpp/utils.h"

#include "log.h"

using namespace std;

void Chunk::read() {
  const int64_t len = end - start;
  data.clear();
  data.resize(len);
  if (len == 0) {
    // For empty file case (empty chunk)
    return;
  }
#if WINDOWS_BUILD
  // For windows we use fseeko64() & fread(): since we
  // compile a 32bit UA version, and standard library functions
  // do not allow to read > 2GB locations in file
  FILE *fp = fopen(localFile.c_str(), "rb");
  if (!fp) {
    ostringstream msg;
    msg << "file('" << localFile.c_str() << "') cannot be opened for reading (errno=" << errno
        << ")... readdata failed on chunk " << (*this);
    throw runtime_error(msg.str());
  }
  if(fseeko64(fp, off64_t(start), SEEK_SET) != 0) {
    ostringstream msg;
    msg << "unable to seek to location '" << off64_t(start) << "' in the file '" << localFile.c_str()
        << "' (errno=" << errno << ")... readdata failed on chunk " << (*this);
    fclose(fp);
    throw runtime_error(msg.str());
  }
  fread(&(data[0]), 1, len, fp);
  int errflg = ferror(fp); // get error status before we close the file handler
  fclose(fp);
  if (errflg) {
    ostringstream msg;
    msg << "unable to read '" << len << "' bytes from location '" << off64_t(start) << "' in the file '"
        << localFile.c_str() << "' (errno=" << errno << ")... readdata failed on chunk " << (*this);
    throw runtime_error(msg.str());
  }
#else
  ifstream in(localFile.c_str(), ifstream::in | ifstream::binary);
  if (!in) {
    ostringstream msg;
    msg << "file('" << localFile.c_str() << "') cannot be opened for reading..." <<
           "readdata failed on chunk " << (*this);
    throw runtime_error(msg.str());
  }
  in.seekg(start);
  if (!in.good()) {
    ostringstream msg;
    msg << "unable to seek to location '" << start << "' in the file '" << localFile.c_str()
        << "' (fail bit = " << in.fail() << ", bad bit = " << in.bad() << ", eofbit = "
        << in.eof() <<")... readdata failed on chunk " << (*this);
    throw runtime_error(msg.str());
  }
  in.read(&(data[0]), len);
  if (!in.good()) {
    ostringstream msg;
    msg << "unable to read '" << len << "' bytes from location '" << start << "' in the file '" 
        << localFile.c_str() << "' (fail bit = " << in.fail() << ", bad bit = " << in.bad() 
        << ", eofbit = " << in.eof() <<")... readdata failed on chunk " << (*this);
    throw runtime_error(msg.str());
  }
#endif
}

string Chunk::computeMD5() {
  return dx::getHexifiedMD5(data); 
}

void Chunk::clear() {
  // A trick for forcing a vector's contents to be deallocated: swap the
  // memory from data into v; v will be destroyed when this function exits.
  vector<char> v;
  data.swap(v);
}

/*
 * Logs a message about this chunk.
 */
void Chunk::log(const string &message) const {
  LOG << "Thread " << boost::this_thread::get_id() << ": " << "Chunk " << (*this) << ": " << message << endl;
}

ostream &operator<<(ostream &out, const Chunk &chunk) {
  out << "[" << chunk.localFile << ":" << chunk.start << "-" << chunk.end << "]";
  return out;
}
