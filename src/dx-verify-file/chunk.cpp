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
  ifstream in(localFile.c_str(), ifstream::in | ifstream::binary);
  in.seekg(start);
  in.read(&(data[0]), len);
  if (in) {
  } else {
    ostringstream msg;
    msg << "readData failed on chunk " << (*this);
    throw runtime_error(msg.str());
  }
}

string Chunk::computeMD5() {
  return getHexifiedMD5(data); 
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
