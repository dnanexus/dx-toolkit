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
#include "SimpleHttpHeaders.h"
#include "Utility.h"

typedef std::string HttpMethod;

class HttpClientRequest {
private:
  CURL *curl;

public:
  HttpHeaders h_req, h_resp;
  HttpMethod method;
  std::string url;

  int responseCode;

  // User must provide size and number of bytes in request data,
  // So that we do not terminate string by "\0" (which is a valid byte for binary request data)
  struct reqData_struct {
    char *data;
    size_t length; // number of bytes
    reqData_struct(): data(NULL), length(0) {};
  }reqData;

  // Using std::string for storing a binary buffer.
  // It might be more natural to use vector<char> instead, but there is no 
  // performance degradation 
  // (only possible meta-objection is: "string" does not feel like a binary buffer naturally).
  // But storing as std::string allows access to some string functions, which might be of use
  // if the response data was a text string (and not a binary buffer - though storage will work 
  // in both cases). For ex, we can print std::string by cout<<, etc.
  // Also note, C++11 guarantees a string to be stored contiguously, and already most
  // implementation store it as contiguous storage, so no performance loss there.
  std::string respData;

  HttpClientRequest();

  void setHeaders(const HttpHeaders &h) { h_req = h; }
  void setUrl(const std::string &u) { url = u; }
  void setReqData(const char *ptr, const size_t &bytes) { reqData.data = const_cast<char*>(ptr); reqData.length = bytes; }
  void setMethod(const HttpMethod &m);
  void send();
  
  const HttpHeaders& getRespHeaders() const { 
    return h_resp;
  }
  
  const HttpHeaders& getReqHeaders() const {
    return h_req;
  }

  void clear() {
    h_req.clear(); h_resp.clear();
    reqData.data = NULL; reqData.length = 0u;
    respData = "";
    responseCode = -1;
    curl = NULL;
    method = "POST";
    url = "";
  }

  ~HttpClientRequest() { if (curl != NULL) { curl_easy_cleanup(curl); } } 
};

class HttpClientRequestException : public std::exception {
public:
  std::string err;
  HttpClientRequestException(): err("Unknown error occured while using HttpClientRequest class") { }
  HttpClientRequestException(const std::string &err): err(err) { }
  virtual const char* what() const throw() {
    return (const char*)err.c_str();
  }

  virtual ~HttpClientRequestException() throw() { }
};
#endif
