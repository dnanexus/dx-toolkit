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

#include "SimpleHttpHeaders.h"
#include <boost/algorithm/string/predicate.hpp> // for case-insensitive string comparison

namespace dx {
  using namespace std;
  void HttpHeaders::appendHeaderString(const string &s) {
    using namespace std;
    using namespace HttpHelperUtils;

    // See: http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
    pair<string, string> h = splitOnFirstColon(s);
    h.second = stripWhitespaces(h.second);

    // If header field-name is already present, append the content with ","
    if (isPresent(h.first))
      header[h.first] += "," + h.second;
    else
      header[h.first] = h.second;
  }

  vector<string> HttpHeaders::getAllHeadersAsVector() const {
    using namespace std;
    vector<string> out;
    for (map<string,string>::const_iterator it = header.begin(); it != header.end(); ++it)
      out.push_back(it->first + ": " + it->second);
    return out;
  }

  // get value of a header (case-insensitive match)
  bool HttpHeaders::getHeaderString(const string &key, string &val) {
    for (map<string,string>::const_iterator it = header.begin(); it != header.end(); ++it) {
      if (boost::iequals(it->first, key)) {
        val = it->second;
        return true;
      }
    }
    return false;
  }
}
