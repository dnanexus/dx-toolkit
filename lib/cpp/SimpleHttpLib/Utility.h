#ifndef UTILITY_H
#define UTILITY_H

#include <string>
#include <vector>
#include <algorithm>
#include <cctype>

namespace HttpHelperUtils {
  using namespace std;

  // This function remove leading and trailing whitespaces from a string
  // One sample use case: http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
  //    - Removing leading/trailing whitespaces in header field
  string stripWhitespaces(const string &s);

  // Split a string on first ":" sign
  // One sample use-case: http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
  //    - Splitting header on field-name : field-value
  //
  // Returns a pair of <string, string>, where first string contain the part before colon(:)
  // and second element contain part after the colon(:). Note: colon == first colon in string
  // If no colon is present, then first string in output pair
  // contains the complete input string, and second string is empty
  pair<string, string> splitOnFirstColon(const string &s);
};

#endif
