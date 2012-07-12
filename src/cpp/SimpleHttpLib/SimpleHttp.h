#ifndef SIMPLEHTTP_H
#define SIMPLEHTTP_H

#include <iostream>
#include <vector>
#include <string>
#include <cstdlib>
#include <cctype>
#include <cstring>
#include <curl/curl.h>
#include <exception>
#include <cassert>
#include <sstream>
#include <unistd.h>

#include "SimpleHttpHeaders.h"
#include "Utility.h"

enum HttpMethod {
  HTTP_POST = 0,
  HTTP_HEAD = 1,
  HTTP_GET = 2,
  HTTP_DELETE = 3,
  HTTP_PUT = 4
};

class HttpRequest {
private:

  CURL *curl;

public:

  HttpHeaders reqHeader, respHeader;
  HttpMethod method;
  std::string url;
  long responseCode;
  //http://curl.haxx.se/libcurl/c/curl_easy_setopt.html#CURLOPTERRORBUFFER
  char errorBuffer[CURL_ERROR_SIZE + 1];

  // User must provide size and number of bytes in request data,
  // So that we do not terminate string by "\0"
  struct reqData_struct {
    char *data;
    size_t length; // number of bytes
    reqData_struct(): data(NULL), length(0) {};
  } reqData;

  // We are using std::string for storing a binary buffer.
  // It might feel more natural to use vector<char> for binary buffer instead
  // but it doesn't change performance in any way.
  // Storing as std::string allows access to some handy string functions, which might be of use
  // if the response data is actually to be interpreted as text string (which is quite often).
  // For ex, we can print std::string by cout<<, etc.
  //
  // Also note: C++11 guarantees a string to be stored contiguously, and already most
  // implementation store it as contiguous storage, so no performance loss there.
  std::string respData;

  HttpRequest()
    : curl(NULL), method(HTTP_POST), responseCode(-1) {
      memset(errorBuffer, 0, CURL_ERROR_SIZE + 1); // Reset error buffer to zero
  }

  void setHeaders(const HttpHeaders& _reqHeader) { reqHeader = _reqHeader; }
  void setUrl(const std::string& _url) { url = _url; }

  void setReqData(const char* _data, const size_t& _length) {
    reqData.data = const_cast<char*>(_data);
    reqData.length = _length;
  }

  void setMethod(const HttpMethod& _method) { method = _method; }

  void buildRequest(const HttpMethod& _method, const std::string& _url,
                    const HttpHeaders& _reqHeader = HttpHeaders(), const char* _data = NULL,
                    const size_t& _length = 0u) {
    setUrl(_url);
    setMethod(_method);
    setHeaders(_reqHeader);
    setReqData(_data, _length);
  }

  void send();

  const HttpHeaders& getRespHeaders() const {
    return respHeader;
  }

  const HttpHeaders& getReqHeaders() const {
    return reqHeader;
  }

  void clear() {
    respHeader.clear(); reqHeader.clear();
    reqData.data = NULL; reqData.length = 0u;
    respData = "";
    responseCode = -1;
    curl = NULL;
    method = HTTP_POST;
    url = "";
  }

  ~HttpRequest() {
    if (curl != NULL) {
      curl_easy_cleanup(curl);
    }
  }
  
  void assertLibCurlFunctions(CURLcode retVal, const std::string &msg);
  
  static HttpRequest request(const HttpMethod& _method, const std::string& _url,
                             const HttpHeaders& _reqHeader = HttpHeaders(), const char* _data = NULL, const size_t& _length = 0u) {
    HttpRequest hr;
    hr.buildRequest(_method, _url, _reqHeader, _data, _length);
    hr.send();
    return hr;
  }

};

class HttpRequestException : public std::exception {
public:

  std::string err;

  HttpRequestException()
    : err("Unknown error occured while using HttpRequest class") {
  }

  HttpRequestException(const std::string &err)
    : err(err) {
  }

  virtual const char* what() const throw() {
    return (const char*) err.c_str();
  }

  virtual ~HttpRequestException() throw() {
  }
};

#endif
