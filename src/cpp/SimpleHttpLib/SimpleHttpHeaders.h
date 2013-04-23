// Copyright (C) 2013 DNAnexus, Inc.
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

// This is a Header only library created for purpose of storing HTTPHeaders
#ifndef SIMPLEHTTPHEADERS_H
#define SIMPLEHTTPHEADERS_H

#include <map>
#include <string>
#include <vector>
#include "Utility.h"

namespace dx {
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
    
    // Looks for the header "key" in list of all headers (in case-insensitive manner)
    // Returns "true", if the header is found (and value is set in param "val"),
    // otherwise returns "false" ("val" is untouched in this case)
    bool getHeaderString(const std::string &key, std::string &val);
    
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
}
#endif
