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

#include "search.h"
#include <ctime>

using namespace std;

namespace dx {
  JSON getApiTimeStamp(const JSON &t) {
    if (t.type() == JSON_STRING) {
      std::string str = t.get<string>();
      if (str.length() == 0)
        throw DXError("Invalid timestamp string: Cannot be zero length");
      char suffix = str[str.length() - 1];
      str.erase(str.end() - 1);
      double val;
      try {
        val = boost::lexical_cast<double>(str);
      } catch(...) {
        throw DXError("Invalid timestamp string");
      }
      int64_t initial = (val >= 0.0) ? 0 : (std::time(NULL) * 1000);
      switch(tolower(suffix)) {
        case 's': return static_cast<int64_t>(initial + val*1000);
        case 'm': return static_cast<int64_t>(initial + val*1000*60);
        case 'h': return static_cast<int64_t>(initial + val*1000*60*60);
        case 'd': return static_cast<int64_t>(initial + val*1000*60*60*24);
        case 'w': return static_cast<int64_t>(initial + val*1000*60*60*24*7);
        case 'y': return static_cast<int64_t>(initial + val*1000*60*60*24*7*365);
        default: throw DXError("Invalid timestamp string: Invalid suffix");
      }
    } else {
      int64_t val = t.get<int64_t>();
      return (val >= 0) ? val : ((std::time(NULL) * 1000) + val);
    }
  }

  // Assume the structure of json to be: {"after": ... , "before": ...}
  // Since it's a often repeated pattern (for "created", and "modified")
  // Return back a resolved (all timestamp in the way api expect) json
  JSON getTimestampAdjustedField(const JSON &j) {
    JSON to_ret(JSON_OBJECT);
    if (j.has("after"))
      to_ret["after"] = getApiTimeStamp(j["after"]);
    if (j.has("before"))
      to_ret["before"] = getApiTimeStamp(j["before"]);
    return to_ret;
  }

  JSON DXSystem::findDataObjects(JSON query) {
    if (query.has("modified")) {
      query["modified"] = getTimestampAdjustedField(query["modified"]);
    }
    if (query.has("created")) {
      query["created"] = getTimestampAdjustedField(query["created"]);
    }
    if (query.has("scope")) {
      if (!query["scope"].has("project")) {
        if (config::CURRENT_PROJECT() == "")
          throw DXError("config::CURRENT_PROJECT() is not set, but call to DXSystem::findDataObjects() is missing input['scope']['project']");
        query["scope"]["project"] = config::CURRENT_PROJECT();
      }
    }
    return systemFindDataObjects(query); 
  }

  JSON DXSystem::findOneDataObject(JSON query) {
    query["limit"] = 1;
    JSON res = findDataObjects(query);
    if (res["results"].size() > 0)
      return res["results"][0];
    // No object matched the search criteria
    return JSON(JSON_NULL);
  }

  JSON DXSystem::findJobs(JSON query) {
    if (query.has("created"))
      query["created"] = getTimestampAdjustedField(query["created"]);

    return systemFindJobs(query); 
  }

  JSON DXSystem::findProjects(JSON query) {
    return systemFindProjects(query);
  }

  JSON DXSystem::findApps(JSON query) {
    if (query.has("modified"))
      query["modified"] = getTimestampAdjustedField(query["modified"]);

    if (query.has("created"))
      query["created"] = getTimestampAdjustedField(query["created"]);
    
    return systemFindApps(query); 
  }
}
