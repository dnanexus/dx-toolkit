// Copyright (C) 2013-2015 DNAnexus, Inc.
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

#include "Utility.h"

namespace dx {
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
}
