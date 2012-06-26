#include "chunk.h"

#include <stdexcept>
#include <fstream>
#include <sstream>

using namespace std;

void Chunk::read() {
  // cerr << "Reading data for chunk " << (*this) << "...";
  const int64_t len = end - start;
  data.clear();
  data.resize(len);
  ifstream in(localFile.c_str(), ifstream::in | ifstream::binary);
  in.seekg(start);
  in.read(&(data[0]), len);
  if (in) {
    // cerr << " success." << endl;
  } else {
    // cerr << " failure." << endl;
    ostringstream msg;
    msg << "readData failed on chunk " << (*this);
    throw runtime_error(msg.str());
  }
}

void Chunk::compress() {
  // TODO: compress the data into a new buffer, then swap that with the
  // uncompressed data
}

void Chunk::upload() {
  // TODO: get the upload URL for this chunk; upload the data
}

void Chunk::clear() {
  // A trick for forcing a vector's contents to be deallocated: swap the
  // memory from data into v; v will be destroyed when this function exits.
  vector<char> v;
  data.swap(v);
}

ostream &operator<<(ostream &out, const Chunk &chunk) {
  out << "[" << chunk.localFile << ":" << chunk.start << "-" << chunk.end
      << " -> " << chunk.fileID << "[" << chunk.index << "]"
      << ", tries=" << chunk.triesLeft << ", data_size=" << chunk.data.size()
      << "]";
  return out;
}
