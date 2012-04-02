// This is a Header only library created for purpose of storing HTTPHeaders
#ifndef SIMPLEHTTPHEADERS_H
#define SIMPLEHTTPHEADERS_H

#include <map>
#include <string>
#include <vector>
#include "Utility.h"

class HttpHeaders {
public:
  // See: http://www.w3.org/Protocols/rfc2616/rfc2616-sec6.html
  // See: http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
  //      for rationale behind "statusLine" and separating headers on colons, etc
  std::map<std::string, std::string> header;
  std::string statusLine; // Makes sense only for response headers
  
  void addHeaderLine(const std::string &s);
  
  size_t count() const { 
    return header.size();
  }
  
  // It is user's responsbility to add content to already existing header with ","
  // See: http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
  // Please note that calling this function as a RValue will create a blank header
  // if the header didn't exist already
  std::string& operator[] (const std::string &key) {
    return header[key];
  }

  // TODO: Should we modify to be case-insensitive?
  bool isPresent(const std::string &key) const {
    return (header.count(key) > 0);
  }
  
  const std::string getStatusLine() const {
    return statusLine;
  }
  
  void setStatusLine(const std::string &l) {
    statusLine = l;
  }
  
  void appendHeaderString(const std::string &s);

  // Currently an inefficient way to get back all headers
  // Should provide iterators in later versions of this library
  const std::map<std::string, std::string>& getLowLevelAccess() const {
    return header;
  }

  // This copies the content of "map" into vector and sends it back
  // HIGHLY INEFFICENT for obvious reasons. Provided only as a quick hack
  std::vector<std::string> getAllHeadersAsVector() const; 

  void clear() {
    header.clear();
    statusLine = "";
  }
};
#endif
