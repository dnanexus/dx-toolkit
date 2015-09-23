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

#ifndef UTILITY_H
#define UTILITY_H

#include <string>
#include <vector>
#include <algorithm>
#include <cctype>

namespace dx {
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
  }
}
#endif
