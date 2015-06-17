// Copyright (C) 2013-2014 DNAnexus, Inc.
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

#include <fstream>

#include <boost/regex.hpp>

#include "dxcpp/utils.h"
#include "dxcpp/dxcpp.h"

extern "C" {
#include "compress.h"
}

#include "round_robin_dns.h"

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

void upload_cleanup(CURL **curl, curl_slist **l1, curl_slist **l2) {
  if (*curl != NULL) {
    curl_easy_cleanup(*curl);
    *curl = NULL;
  }
  if (*l1 != NULL) {
    curl_slist_free_all(*l1);
    *l1 = NULL;
  }
  if (*l2 != NULL) {
    curl_slist_free_all(*l2);
    *l2 = NULL;
  }
}

void Chunk::upload(Options &opt) {
  CURL *curl = NULL;
  struct curl_slist *slist_resolved_ip = NULL;
  struct curl_slist *slist_headers = NULL;
  long responseCode;
  try {
    uploadOffset = 0;
    pair<string, dx::JSON> uploadResp = uploadURL(opt);
    const string &url = uploadResp.first;
    const dx::JSON &headersToSend = uploadResp.second;

    log("Upload URL: " + url);

    curl = curl_easy_init();
    if (curl == NULL) {
      throw runtime_error("An error occurred when initializing the HTTP connection");
    }
    char errorBuffer[CURL_ERROR_SIZE + 1] = {0}; // setting to zero (since it can be the case that despite an error, nothing is written to the buffer)
    // Set errorBuffer to recieve human readable error messages from libcurl
    // http://curl.haxx.se/libcurl/c/curl_easy_setopt.html#CURLOPTERRORBUFFER
    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errorBuffer), errorBuffer);

    if (!hostName.empty() && !resolvedIP.empty()) { // Will never be true when compiling on windows
      log("Adding ip '" + resolvedIP + "' to resolve list for hostname '" + hostName + "'");
      slist_resolved_ip = curl_slist_append(slist_resolved_ip, (hostName + ":443:" + resolvedIP).c_str());
      slist_resolved_ip = curl_slist_append(slist_resolved_ip, (hostName + ":80:" + resolvedIP).c_str());
      checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_RESOLVE, slist_resolved_ip), errorBuffer);
      // Note: We don't remove this extra host name resolution info by setting "-HOST:PORT:IP" at the end,
      // since we don't reuse the curl handle anyway
    } else {
      log("Not adding any explicit IP address using CURLOPT_RESOLVE. resolvedIP = '" + resolvedIP + "', hostName = '" + hostName + "'", dx::logWARNING);
    }

    // g_DX_CA_CERT is set by dxcpp (from environment variable DX_CA_CERT)
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

    if (opt.throttle > 0) {
      const int totalChunksRemaining = totalChunks - chunksFinished.size() + chunksFailed.size();
      assert(totalChunksRemaining > 0);
      curl_off_t tval = static_cast<curl_off_t>(double(opt.throttle) / std::min(opt.uploadThreads, totalChunksRemaining)) + 1;
      log("Setting CURLOPT_MAX_SEND_SPEED_LARGE = " + boost::lexical_cast<string>(tval), dx::logINFO);
      checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_MAX_SEND_SPEED_LARGE, tval), errorBuffer);
    }

    // Abort if we cannot connect within 30 seconds
    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30l), errorBuffer);

    // Time out after 30 minutes. That should be plenty of time to upload a part
    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_TIMEOUT, 1800l), errorBuffer);

    // If the average bytes per second is below 1 over a 60 second window, abort the request
    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1l), errorBuffer);
    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, 60l), errorBuffer);

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

    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_UPLOAD, 1), errorBuffer);
    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_URL, url.c_str()), errorBuffer);
    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_READFUNCTION, curlReadFunction), errorBuffer);
    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_READDATA, this), errorBuffer);

    // Set callback for recieving the response data
    respData.clear();
    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback) , errorBuffer);
    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_WRITEDATA, &respData), errorBuffer);

    // Remove the Content-Type header (libcurl sets "Content-Type: application/x-www-form-urlencoded" by default for POST)
    slist_headers = curl_slist_append(slist_headers, "Content-Type:");

    // Append additional headers requested by /file-xxxx/upload call
    for (dx::JSON::const_object_iterator it = headersToSend.object_begin(); it != headersToSend.object_end(); ++it) {
      ostringstream tempStream;
      tempStream << it->first << ": " << it->second.get<string>();
      slist_headers = curl_slist_append(slist_headers, tempStream.str().c_str());
    }

    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_HTTPHEADER, slist_headers), errorBuffer);

    // curl wants to know this (otherwise it uses chunked transfer), even
    // though we have set the content-length header above
    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)data.size()), errorBuffer);

    log("Starting curl_easy_perform...");

    checkPerformCURLcode(curl_easy_perform(curl), errorBuffer);

    checkPerformCURLcode(curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode), errorBuffer);
    log("Returned from curl_easy_perform; responseCode is " + boost::lexical_cast<string>(responseCode));

    upload_cleanup(&curl, &slist_headers, &slist_resolved_ip);
  } catch (...) {
    // This catch is only intended for cleanup (when checkPerformCURLcode() or checkConfigCURLcode() throw)
    // We will rethrow the error again.
    upload_cleanup(&curl, &slist_headers, &slist_resolved_ip);
    throw;
  }

  if ((responseCode < 200) || (responseCode >= 300)) {
    log("Response code not in 2xx range ... throwing runtime_error", dx::logERROR);
    ostringstream msg;
    msg << "Request failed with HTTP status code " << responseCode << ", server Response: '" << respData << "'";
    throw runtime_error(msg.str());
  }

  assert(respData == "");
}

void Chunk::clear() {
  // A trick for forcing a vector's contents to be deallocated: swap the
  // memory from data into v; v will be destroyed when this function exits.
  vector<char> v;
  data.swap(v);
  respData.clear();
}

// HACK! HACK! HACK!
// Returns hostname from /UPLOAD urls
// We use regexp (really!) to parse the URL & extract host name, so certainly
// not a general enough function.
//
// Returns empty string if regexp fails to parse url string for some reason
static string extractHostFromURL(const string &url) {
  static const boost::regex expression("^http[s]{0,1}://([^/:]+)(/|:)", boost::regex::perl);
  boost::match_results<string::const_iterator> what;
  if (!boost::regex_search(url.begin(), url.end(), what, expression, boost::match_default)) {
    return "";
  }
  if (what.size() != 3) {
    return "";
  }
  return what[1];
}

// This function looks at the hostname extracted from the url, and decides if we want to resolve the
// ip address explicitly or not, e.g., we do not attempt to resolve a hostname if it is already an ip address
// (which is actually the case when UA is run from within a job in DNAnexus)
// Note: The regexp for IP address we use is quite lenient, and matches some non-valid ips, but that's
//       fine for our purpose here, since:
//       1) Not resolving a hostname explicitly does not break anything (but the opposite can be dangerous),
//       2) The input received by this function is not arbitrary but rather something decided by apiserver
//          (i.e., output of /file-xxxx/upload call), so we know what to expect.
static bool attemptExplicitDNSResolve(const string &host) {
  static const boost::regex expression("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$", boost::regex::perl);
  boost::match_results<string::const_iterator> what;
  return !boost::regex_search(host.begin(), host.end(), what, expression, boost::match_default);
}

pair<string, dx::JSON> Chunk::uploadURL(Options &opt) {
  dx::JSON params(dx::JSON_OBJECT);
  params["index"] = index + 1;  // minimum part index is 1
  params["size"] = data.size();
  params["md5"] = dx::getHexifiedMD5(data);
  log("Generating Upload URL for index = " + boost::lexical_cast<string>(params["index"].get<int>()));
  dx::JSON result = fileUpload(fileID, params);
  pair<string, dx::JSON> toReturn = make_pair(result["url"].get<string>(), result["headers"]);
  const string &url = toReturn.first;
  log("/" + fileID + "/upload call returned this url: " + url);

  if (!opt.noRoundRobinDNS) {
    // Now, try to resolve the host name in url to an ip address (for explicit round robin DNS)
    // If we are unable to do so, just leave the resolvedIP variable an empty string
    resolvedIP.clear();
    hostName = extractHostFromURL(url);
    log("Host name extracted from URL ('" + url + "'): '" + hostName + "'");

    if (attemptExplicitDNSResolve(hostName)) {
      resolvedIP = getRandomIP(hostName);
      log("Call to getRandomIP() returned: '" + resolvedIP + "'", dx::logWARNING);
    } else {
      log("Not attempting to resolve hostname '" + hostName + "'");
    }
  } else {
    log("Flag --no-round-robin-dns was set, so won't try to explicitly resolve ip address");
  }
  return toReturn;
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
