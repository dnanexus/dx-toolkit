#include "chunk.h"

#include <stdexcept>
#include <fstream>
#include <sstream>

#include <curl/curl.h>
#include <boost/thread.hpp>
#include <boost/lexical_cast.hpp>

#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"

extern "C" {
#include "compress.h"
}

#include "log.h"

using namespace std;

// A macro to allow unused variable (without throwing warning)
// Does work for GCC, will need to be expanded for other compilers.
#ifdef UNUSED
#elif defined(__GNUC__) 
# define UNUSED(x) UNUSED_ ## x __attribute__((unused)) 
#elif defined(__LCLINT__) 
# define UNUSED(x) /*@unused@*/ x 
#else 
# define UNUSED(x) x 
#endif

/* Initialize the extern variables, decalred in chunk.h */
queue<pair<time_t, int64_t> > instantaneousBytesAndTimestampQueue;
int64_t sumOfInstantaneousBytes = 0;
boost::mutex instantaneousBytesMutex;
// it takes roughly 30sec to reach queue size = 5000 on my computer.
// Unfortunately, standard C++ does not allow time resolution
// smaller than seconds, so we need to set it to some higher value
// (like ~30sec) to mitigate rounding effect.
const size_t MAX_QUEUE_SIZE = 5000;

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
                                  3);  // 3 is the compression level -- fast, not good

  if (compressStatus == Z_MEM_ERROR) {
    throw runtime_error("compression failed: not enough memory");
  } else if (compressStatus == Z_BUF_ERROR) {
    throw runtime_error("compression failed: output buffer too small");
  } else if (compressStatus != Z_OK) {
    throw runtime_error("compression failed: " + boost::lexical_cast<string>(compressStatus));
  }

  /* Special case: If the chunk is compressed below 5MB, compress it with
   *               level 0
   */
  if (!lastChunk && destLen < 5 * 1024 * 1024) {
    log("Compression at level 3, resulted in data size =" + boost::lexical_cast<string>(destLen) + " bytes. " +
        "We cannot upload data less than 5MB in any chunk (except last). So will compress at level 0 now (i.e., no compression).");
    destLen = gzCompressBound(sourceLen);
    dest.clear();
    dest.resize(destLen);
    destLen = gzCompressBound(sourceLen);
    compressStatus = gzCompress((Bytef *) (&(dest[0])), (uLongf *) &destLen,
                                    (const Bytef *) (&(data[0])), (uLong) sourceLen,
                                    0);  // 0 => no compression

    if (compressStatus == Z_MEM_ERROR) {
      throw runtime_error("compression failed: not enough memory");
    } else if (compressStatus == Z_BUF_ERROR) {
      throw runtime_error("compression failed: output buffer too small");
    } else if (compressStatus != Z_OK) {
      throw runtime_error("compression failed: " + boost::lexical_cast<string>(compressStatus));
    }
    
    assert (destLen >= 5 * 1024 * 1024); // Chunk size should never decrease when 'compressing' at level 0, and chunk size is always >= 5mb

/*    if (destLen < 5 * 1024 * 1024) {
      log("Compression at level 1, resulted in data size = " + boost::lexical_cast<string>(destLen) + " bytes. " +
          "We cannot upload data less than 5MB in any chunk (except last). The remote file: \"" + boost::lexical_cast<string>(fileID) + 
          "\" will fail to close at the end.");
    }*/
  }

  if (destLen < (int64_t) dest.size()) {
    dest.resize(destLen);
  }

  data.swap(dest);
}

void checkConfigCURLcode(CURLcode code) {
  if (code != 0) {
    ostringstream msg;
    msg << "An error occurred while configuring the HTTP request (" << curl_easy_strerror(code) << ")" << endl;
    throw runtime_error(msg.str());
  }
}

void checkPerformCURLcode(CURLcode code) {
  if (code != 0) {
    ostringstream msg;
    msg << "An error occurred while performing the HTTP request (" << curl_easy_strerror(code) << ")" << endl;
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

void Chunk::upload() {
  string url = uploadURL();
  log("Upload URL: " + url);

  uploadOffset = 0;
  
  CURL * curl = curl_easy_init();
  if (curl == NULL) {
    throw runtime_error("An error occurred when initializing the HTTP connection");
  }
  // g_DX_CA_CERT is set by dxcppp (using env variable: DX_CA_CERT)
  if (get_g_DX_CA_CERT() == "NOVERIFY") {
    checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0));
  } else {
    if (!get_g_DX_CA_CERT().empty()) {
      checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_CAINFO, get_g_DX_CA_CERT().c_str()));
    } else {
      // Set verify on, and use default path for certificate
      checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1));
    }
  }
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_USERAGENT, userAgentString.c_str()));
  // Internal CURL progressmeter must be disabled if we provide our own callback
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0));
  // Install the callback function
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_PROGRESSFUNCTION, progress_func));
  myProgressStruct prog;
  prog.curl = curl;
  prog.uploadedBytes = 0;
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_PROGRESSDATA, &prog));

  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_POST, 1));
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_URL, url.c_str()));
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_READFUNCTION, curlReadFunction));
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_READDATA, this));

  struct curl_slist * slist = NULL;
  /*
   * Set the Content-Length header.
   */
  {
    ostringstream clen;
    clen << "Content-Length: " << data.size();
    slist = curl_slist_append(slist, clen.str().c_str());
  }
  checkConfigCURLcode(curl_easy_setopt(curl, CURLOPT_HTTPHEADER, slist));

  log("Starting curl_easy_perform...");
  checkPerformCURLcode(curl_easy_perform(curl));

  long responseCode;
  checkPerformCURLcode(curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode));
  log("Returned from curl_easy_perform; responseCode is " + boost::lexical_cast<string>(responseCode));

  log("Performing curl cleanup");
  curl_slist_free_all(slist);
  curl_easy_cleanup(curl);

  if ((responseCode < 200) || (responseCode >= 300)) {
    log("Throwing runtime_error");
    ostringstream msg;
    msg << "Request failed with HTTP status code " << responseCode;
    throw runtime_error(msg.str());
  }
}

void Chunk::clear() {
  // A trick for forcing a vector's contents to be deallocated: swap the
  // memory from data into v; v will be destroyed when this function exits.
  vector<char> v;
  data.swap(v);
}

string Chunk::uploadURL() const {
  dx::JSON params(dx::JSON_OBJECT);
  params["index"] = index + 1;  // minimum part index is 1
  dx::JSON result = fileUpload(fileID, params);
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
      << ", tries=" << chunk.triesLeft << ", data.size=" << chunk.data.size()
      << ", compress="<< ((chunk.toCompress) ? "true": "false")
      << "]";
  return out;
}
