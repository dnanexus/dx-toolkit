// Copyright (C) 2013 DNAnexus, Inc.
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

#include <curl/curl.h>
#include <boost/thread.hpp>
#include <boost/lexical_cast.hpp>

#include "options.h"
#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"
#include "dxcpp/utils.h"

#include "SimpleHttpLib/ignore_sigpipe.h"

extern "C" {
#include "compress.h"
}

#include "dxcpp/dxlog.h"

using namespace std;

/* Initialize the extern variables, decalred in chunk.h */
queue<pair<time_t, int64_t> > instantaneousBytesAndTimestampQueue;
int64_t sumOfInstantaneousBytes = 0;
boost::mutex instantaneousBytesMutex;
// it takes roughly 30sec to reach queue size = 5000 on my computer.
// Unfortunately, standard C++ does not allow time resolution
// smaller than seconds, so we need to set it to some higher value
// (like ~30sec) to mitigate rounding effect.
const size_t MAX_QUEUE_SIZE = 5000;

// Replace contents of "dest" with gzip of empty string 
void get_empty_string_gzip(vector<char> &dest) {
  DXLOG(dx::logINFO) << "Computing gzip of zero length string...";
  int64_t destLen = gzCompressBound(0);
  dest.clear();
  dest.resize(destLen);
  const char *data = "";
  int compressStatus = gzCompress((Bytef *) (&(dest[0])), (uLongf *) &destLen,
                                  (const Bytef *) (data), 0ul, Z_DEFAULT_COMPRESSION);

  if (compressStatus == Z_MEM_ERROR) {
    throw runtime_error("compression failed: not enough memory");
  } else if (compressStatus == Z_BUF_ERROR) {
    throw runtime_error("compression failed: output buffer too small");
  } else if (compressStatus != Z_OK) {
    throw runtime_error("compression failed: " + boost::lexical_cast<string>(compressStatus));
  }
  if (destLen < (int64_t) dest.size()) {
    dest.resize(destLen);
  }
  DXLOG(dx::logINFO) << "Gzip of zero length string computed to be " << dest.size() << "bytes long";
}

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

void Chunk::compress() {
  int64_t sourceLen = data.size();
  if (sourceLen == 0) {
    // Empty file case (empty chunk)
    return;
  }
  int64_t destLen = gzCompressBound(sourceLen);
  vector<char> dest(destLen);

  int compressStatus = gzCompress((Bytef *) (&(dest[0])), (uLongf *) &destLen,
                                  (const Bytef *) (&(data[0])), (uLong) sourceLen,
                                  Z_DEFAULT_COMPRESSION);  // use default compression level value from ZLIB (usually 6)

  if (compressStatus == Z_MEM_ERROR) {
    throw runtime_error("compression failed: not enough memory");
  } else if (compressStatus == Z_BUF_ERROR) {
    throw runtime_error("compression failed: output buffer too small");
  } else if (compressStatus != Z_OK) {
    throw runtime_error("compression failed: " + boost::lexical_cast<string>(compressStatus));
  }
  
  if (destLen < (int64_t) dest.size()) {
    dest.resize(destLen);
  }
   
  const size_t MIN_CHUNK_SIZE = 5 * 1024 * 1024;
  /* Special case: If the chunk is compressed below 5MB, append appropriate
   *               number of chunks representing gzip of empty string.
   */
  if (!lastChunk && dest.size() < MIN_CHUNK_SIZE) {
    log("Compression at level Z_DEFAULT_COMPRESSION (usually 6), resulted in data size = " + boost::lexical_cast<string>(dest.size()) + " bytes. " +
        "We cannot upload data less than 5MB in any chunk (except last). So will append approppriate number of gzipped chunks of empty string.", dx::logWARNING);
    vector<char> zeroLengthGzip;
    get_empty_string_gzip(zeroLengthGzip);
    if (zeroLengthGzip.empty()) {
      throw runtime_error("Size of empty string's gzip is 0 bytes .. unexpected");
    }
    dest.reserve(MIN_CHUNK_SIZE + zeroLengthGzip.size()); // Reserve memory in advance
    int count = 0;
    while (dest.size() < MIN_CHUNK_SIZE) {
      count++;
      std::copy(zeroLengthGzip.begin(), zeroLengthGzip.end(), std::back_inserter(dest));
    }
    log ("Pushed empty string's gzip to 'dest' " + boost::lexical_cast<string>(count) + " number of times, Final length = " + boost::lexical_cast<string>(dest.size()) + " bytes");
  }
  data.swap(dest);
}

void checkConfigCURLcode(CURLcode code, char *errorBuffer) {
  string errMsg = errorBuffer; // copy it, since "errorBuffer" is a local variable of upload() and will be deleted after we throw
  if (code != 0) {
    ostringstream msg;
    msg << "An error occurred while configuring the HTTP request(" << code << ": " << curl_easy_strerror(code)
        << "). Curl error buffer: '" << errMsg << "'" << endl;
    throw runtime_error(msg.str());
  }
}

void checkPerformCURLcode(CURLcode code, char *errorBuffer) {
  string errMsg = errorBuffer; // copy it, since "errorBuffer" is a local variable of upload() and will be deleted after we throw
  if (code != 0) {
    ostringstream msg;
    msg << "An error occurred while performing the HTTP request(" << code << ": " << curl_easy_strerror(code)
        << "). Curl error buffer: '" << errMsg << "'" << endl;
    throw runtime_error(msg.str());
  }
}

/*
 * This function is the callback invoked by libcurl when it needs more data
 * to send to the server (CURLOPT_READFUNCTION). userdata is a pointer to
 * the chunk; we copy at most size * nmemb bytes of its data into ptr and
 * return the amount of data copied.
 */
size_t curlReadFunction(void * ptr, size_t size, size_t nmemb, void * userdata) {
  Chunk * chunk = (Chunk *) userdata;
  int64_t bytesLeft = chunk->data.size() - chunk->uploadOffset;
  size_t bytesToCopy = min<size_t>(bytesLeft, size * nmemb);

  if (bytesToCopy > 0) {
    memcpy(ptr, &((chunk->data)[chunk->uploadOffset]), bytesToCopy);
    chunk->uploadOffset += bytesToCopy;
  }

  return bytesToCopy;
}

// This structure is used for passing data to progress_func(),
// via CURL's PROGRESSFUNCTION option.
struct myProgressStruct {
  int64_t uploadedBytes;
  CURL *curl;
};

int progress_func(void* ptr, double UNUSED(TotalToDownload), double UNUSED(NowDownloaded), double UNUSED(TotalToUpload), double NowUploaded) {
  myProgressStruct *myp = static_cast<myProgressStruct*>(ptr);

  boost::mutex::scoped_lock lock(instantaneousBytesMutex);
  if (instantaneousBytesAndTimestampQueue.size() >= MAX_QUEUE_SIZE) {
    pair<time_t, int64_t> elem = instantaneousBytesAndTimestampQueue.front();
    sumOfInstantaneousBytes -= elem.second;
    instantaneousBytesAndTimestampQueue.pop();
  }
  int64_t uploadedThisTime = int64_t(NowUploaded) - myp->uploadedBytes;
  myp->uploadedBytes = int64_t(NowUploaded);
  instantaneousBytesAndTimestampQueue.push(make_pair(std::time(0), uploadedThisTime));
  sumOfInstantaneousBytes += uploadedThisTime;
  
  lock.unlock();
  return 0;
}

/* Callback for response data */
static size_t write_callback(void *buffer, size_t size, size_t nmemb, void *userdata) {
  char *buf = reinterpret_cast<char*>(buffer);
  std::string *response = reinterpret_cast<std::string*>(userdata);
  size_t result = 0u;
  if (userdata != NULL) {
    response->append(buf, size * nmemb);
    result = size * nmemb;
  }
  return result;
}

// This function will catch the sigpipe and ignore it (after printing it in the logs)
static void sigpipe_catcher(int sig) {
  DXLOG(dx::logINFO) << "UA Chunk.cpp => Caught SIGPIPE(signal_num = " << sig << ")... will ignore";
}

void Chunk::upload() {
  string url = uploadURL();
  log("Upload URL: " + url);

  uploadOffset = 0;
  
  CURL * curl = curl_easy_init();
  if (curl == NULL) {
    throw runtime_error("An error occurred when initializing the HTTP connection");
  }
  char errorBuffer[CURL_ERROR_SIZE + 1] = {0}; // setting to zero (since it can be the case that despite an error, nothing is written to the buffer)
  // Set errorBuffer to recieve human readable error messages from libcurl
  // http://curl.haxx.se/libcurl/c/curl_easy_setopt.html#CURLOPTERRORBUFFER
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errorBuffer), errorBuffer);
 
  // g_DX_CA_CERT is set by dxcppp (using env variable: DX_CA_CERT)
  if (dx::config::CA_CERT() == "NOVERIFY") {
    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0), errorBuffer);
  } else {
    if (!dx::config::CA_CERT().empty()) {
      checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_CAINFO, dx::config::CA_CERT().c_str()), errorBuffer);
    } else {
      // Set verify on, and use default path for certificate
      checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1), errorBuffer);
    }
  }

  // Set time out to infinite
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_TIMEOUT, 0l), errorBuffer);
  
  if (!dx::config::LIBCURL_VERBOSE().empty() && dx::config::LIBCURL_VERBOSE() != "0") { 
    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_VERBOSE, 1), errorBuffer);
  }

  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_USERAGENT, userAgentString.c_str()), errorBuffer);
  // Internal CURL progressmeter must be disabled if we provide our own callback
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0), errorBuffer);
  // Install the callback function
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_PROGRESSFUNCTION, progress_func), errorBuffer);
  myProgressStruct prog;
  prog.curl = curl;
  prog.uploadedBytes = 0;
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_PROGRESSDATA, &prog), errorBuffer);
  
  /* Setting this option, since libcurl fails in multi-threaded environment otherwise */
  /* See: http://curl.haxx.se/libcurl/c/libcurl-tutorial.html#Multi-threading */
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1l), errorBuffer);

  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_POST, 1), errorBuffer);
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_URL, url.c_str()), errorBuffer);
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_READFUNCTION, curlReadFunction), errorBuffer);
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_READDATA, this), errorBuffer);
  
  // Set callback for recieving the response data
  /** set callback function */
  respData.clear();
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback) , errorBuffer);
  /** "respData" is a member variable of Chunk class*/
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_WRITEDATA, &respData), errorBuffer);

  struct curl_slist * slist = NULL;
  /*
   * Set the Content-Length header.
   */
  {
    ostringstream clen;
    clen << "Content-Length: " << data.size();
    slist = curl_slist_append(slist, clen.str().c_str());
  }
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_HTTPHEADER, slist), errorBuffer);

  // Compute the MD5 sum of data, and add the Content-MD5 header
  expectedMD5 = dx::getHexifiedMD5(data);
  {
    ostringstream cmd5;
    cmd5 << "Content-MD5: " << expectedMD5;
    slist = curl_slist_append(slist, cmd5.str().c_str());
  }

  log("Starting curl_easy_perform...\n");
  
  SIGPIPE_VARIABLE(pipe1);
  sigpipe_ignore(&pipe1, sigpipe_catcher);
  try {
    checkPerformCURLcode(curl_easy_perform(curl), errorBuffer);
  } catch(...) {
    sigpipe_restore(&pipe1);
    throw;
  }
  sigpipe_restore(&pipe1);
  
  long responseCode;
  
  checkPerformCURLcode(curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode), errorBuffer);
  log("Returned from curl_easy_perform; responseCode is " + boost::lexical_cast<string>(responseCode));

  log("Performing curl cleanup");
  curl_slist_free_all(slist);

  SIGPIPE_VARIABLE(pipe2);
  sigpipe_ignore(&pipe2, sigpipe_catcher);
  curl_easy_cleanup(curl);
  sigpipe_restore(&pipe2);
  
  if ((responseCode < 200) || (responseCode >= 300)) {
    log("Runtime not in 2xx range ... throwing runtime_error", dx::logERROR);
    ostringstream msg;
    msg << "Request failed with HTTP status code " << responseCode << ", server Response: '" << respData << "'";
    throw runtime_error(msg.str());
  }
  
  /************************************************************************************************************************************/
  /*********** Assertions for testing APIservers checksum logic (in case of succesful /UPLOAD/xxxx request) ***************************/
  /*********** Can be removed later (when we are relatively confident of apisever's checksum logic) ***********************************/
  
  // We check that /UPLOAD/xxxx returned back a hash of form {md5: xxxxx},
  // and that value is equal to md5 we computed (and sent as Content-MD5 header).
  // If the values differ - it's a MAJOR apiserver bug (since server must have rejected request with incorrect Content-MD5 anyway)
  dx::JSON apiserverResp;
  try {
    apiserverResp = dx::JSON::parse(respData);
  } catch(dx::JSONException &jexcp) {
    cerr << "\nUNEXPECTED FATAL ERROR: Response from /UPLOAD/xxxx route could not be parsed as valid JSON" << endl
         << "JSONException = '" << jexcp.what() << "'" << endl
         << "APIServer response = '" << respData << "'" << endl;
    assert(false); // This should not happen (apiserver response could not be parsed as JSON)
    throw jexcp;
  }
  assert(apiserverResp.type() == dx::JSON_HASH);
  assert(apiserverResp.has("md5") && apiserverResp["md5"].type() == dx::JSON_STRING);
  assert(apiserverResp["md5"].get<string>() == expectedMD5);
  /*************************************************************************************************************************************/
  /*************************************************************************************************************************************/
}

void Chunk::clear() {
  // A trick for forcing a vector's contents to be deallocated: swap the
  // memory from data into v; v will be destroyed when this function exits.
  vector<char> v;
  data.swap(v);
  respData.clear();
}

string Chunk::uploadURL() const {
  dx::JSON params(dx::JSON_OBJECT);
  params["index"] = index + 1;  // minimum part index is 1
  log("Generating Upload URL for index = " + boost::lexical_cast<string>(params["index"].get<int>()));
  dx::JSON result = fileUpload(fileID, params);
  return result["url"].get<string>();
}

/*
 * Logs a message about this chunk.
 */
void Chunk::log(const string &message, const enum dx::LogLevel level) const {
  DXLOG(level) << "Chunk " << (*this) << ": " << message;
}

ostream &operator<<(ostream &out, const Chunk &chunk) {
  out << "[" << chunk.localFile << ":" << chunk.start << "-" << chunk.end
      << " -> " << chunk.fileID << "[" << chunk.index << "]"
      << ", tries=" << chunk.triesLeft << ", data.size=" << chunk.data.size()
      << ", compress="<< ((chunk.toCompress) ? "true": "false")
      << "]";
  return out;
}
