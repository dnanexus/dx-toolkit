#include "Utility.h"

// This function remove leading and trailing whitespaces from a string
// One sample use case: http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
//    - Removing leading/trailing whitespaces in header field
std::string HttpHelperUtils::stripWhitespaces(const string &s) {
  const unsigned length = s.size();
  unsigned firstNWS = length, lastNWS = length;

  for (unsigned i = 0; i < length ; ++i) {
    if (!isspace(s[i])) {
      lastNWS = i;
      if (firstNWS == length) {
        firstNWS = i;
      }
    }
  }
  return (firstNWS == length) ? "" : s.substr(firstNWS, (lastNWS-firstNWS+1u));
}

// Split a string on first ":" sign
// One sample use-case: http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
//    - Splitting header on field-name : field-value
//
// Returns a pair of <string, string>, where first string contain the part before colon(:)
// and second element contain part after the colon(:). Note: colon == first colon in string
// If no colon is present, then first string in output pair
// contains the complete input string, and second string is empty
std::pair<std::string, std::string> HttpHelperUtils::splitOnFirstColon(const string &s) {
  pair<string, string> out;
  if (s.size() == 0u)
    return out;

  size_t colonPos = s.find(':');

  if (colonPos == string::npos) {
    out.first = s;
    return out;
  }
  out.first = (colonPos != 0u) ? s.substr(0u, colonPos) : "";
  out.second = (colonPos != (s.size() - 1u)) ? s.substr(colonPos + 1) : "";
  return out;
}
