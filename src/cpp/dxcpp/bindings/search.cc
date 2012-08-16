#include "search.h"
#include <ctime>

using namespace std;
using namespace dx;

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
      if (g_WORKSPACE_ID == "")
        throw DXError("g_WORKSPACE_ID is not set, but call to DXSystem::findDataObjects() is missing input['scope']['project']");
      query["scope"]["project"] = g_WORKSPACE_ID;
    }
  }
//  std::cerr<<"\nQuery = "<<query.toString()<<std::endl;
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

JSON DXSystem::findJobs(const JSON &query) {
  JSON newQuery(JSON_NULL);
  if (query.has("created")) {
    if (newQuery.type() == JSON_NULL)
      newQuery = query;
    newQuery["created"] = getTimestampAdjustedField(query["created"]);
  }
  return systemFindJobs(newQuery); 
}

JSON DXSystem::findProjects(const JSON &query) {
  return systemFindProjects(query);
}

JSON DXSystem::findApps(const JSON &query) {
  JSON newQuery(JSON_NULL);
  if (query.has("modified")) {
    if (newQuery.type() == JSON_NULL)
      newQuery = query;
    newQuery["modified"] = getTimestampAdjustedField(query["modified"]);
  }
  if (query.has("created")) {
    if (newQuery.type() == JSON_NULL)
      newQuery = query;
    newQuery["created"] = getTimestampAdjustedField(query["created"]);
  }
  return systemFindApps(newQuery); 
}
