#include "chunk.h"

#include <stdexcept>
#include <fstream>
#include <sstream>

#include <curl/curl.h>
#include <boost/thread.hpp>

#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"

#include "log.h"

using namespace std;
using namespace dx;

void Chunk::read() {
  const int64_t len = end - start;
  data.clear();
  data.resize(len);
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

void Chunk::compress() {
  // TODO: compress the data into a new buffer, then swap that with the
  // uncompressed data
}

void Chunk::upload() {
  // TODO: get the upload URL for this chunk; upload the data
  string url = uploadURL();
  LOG << "Upload URL: " << url << endl;

  CURL * curl = curl_easy_init();
}

void Chunk::clear() {
  // A trick for forcing a vector's contents to be deallocated: swap the
  // memory from data into v; v will be destroyed when this function exits.
  vector<char> v;
  data.swap(v);
}

string Chunk::uploadURL() const {
  JSON params(JSON_OBJECT);
  params["index"] = index + 1;  // minimum part index is 1
  JSON result = fileUpload(fileID, params);
  return result["url"].get<string>();
}

/*
 * Logs a message about this chunk.
 */
void Chunk::log(const string &message) const {
  LOG << "Thread " << boost::this_thread::get_id() << ": " << "Chunk " << (*this) << ": " << message << endl;
}

ostream &operator<<(ostream &out, const Chunk &chunk) {
  out << "[" << chunk.localFile << ":" << chunk.start << "-" << chunk.end
      << " -> " << chunk.fileID << "[" << chunk.index << "]"
      << ", tries=" << chunk.triesLeft << ", data_size=" << chunk.data.size()
      << "]";
  return out;
}
