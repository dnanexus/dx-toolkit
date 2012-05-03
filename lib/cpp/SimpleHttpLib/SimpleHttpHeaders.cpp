#include "SimpleHttpHeaders.h"

void HttpHeaders::appendHeaderString(const std::string &s) {
  using namespace std;
  using namespace HttpHelperUtils;

  // See: http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
  pair<string, std::string> h = splitOnFirstColon(s);
  h.second = stripWhitespaces(h.second);

  // If header field-name is already present, append the content with ","
  if (isPresent(h.first))
    header[h.first] += "," + h.second;
  else
    header[h.first] = h.second;
}

std::vector<std::string> HttpHeaders::getAllHeadersAsVector() const {
  using namespace std;
  vector<string> out;
  for (map<string,string>::const_iterator it = header.begin(); it != header.end(); ++it)
    out.push_back(it->first + ": " + it->second);
  return out;
}
