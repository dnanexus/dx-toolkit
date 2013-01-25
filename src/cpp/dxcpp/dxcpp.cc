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

#include <algorithm>
#include <boost/thread.hpp>
#include <boost/regex.hpp>

#include "dxcpp.h"
#include "SimpleHttp.h"
#include "utils.h"

#include <boost/version.hpp>
// http://www.boost.org/doc/libs/1_48_0/libs/config/doc/html/boost_config/boost_macro_reference.html
#if ((BOOST_VERSION / 100000) < 1 || ((BOOST_VERSION/100000) == 1 && ((BOOST_VERSION / 100) % 1000) < 48))
  #error "Cannot compile dxcpp using Boost version < 1.48"
#endif

// Example environment variables
//
// DX_APISERVER_PORT=8124
// DX_APISERVER_HOST=localhost
// DX_SECURITY_CONTEXT='{"auth_token":"outside","auth_token_type":"Bearer"}'

using namespace std;
using namespace dx;

const bool PRINT_ENV_VAR_VALUES_WHEN_LOADED = (getenv("DXCPP_DEBUG") != NULL && string(getenv("DXCPP_DEBUG")) == "true");

const string g_API_VERSION = "1.0.0";

bool g_APISERVER_SET = false;
bool g_SECURITY_CONTEXT_SET = false;

bool g_dxcpp_mute_retry_cerrs = false; // dirty hack -> only used by UA (for muting the "cerr" by DXHTTPRequest()). Figure a better mechanism soon.

string g_APISERVER_HOST;
string g_APISERVER_PORT;
string g_APISERVER;
JSON g_SECURITY_CONTEXT;
string g_JOB_ID;
string g_WORKSPACE_ID;
string g_PROJECT_CONTEXT_ID;
string g_APISERVER_PROTOCOL;

map<string, string> g_config_file_contents;

const unsigned int NUM_MAX_RETRIES = 5u; // For DXHTTPRequest()

boost::mutex g_loadFromEnvironment_mutex;
volatile bool g_loadFromEnvironment_finished = false;

static bool isAlwaysRetryableHttpCode(int c) {
  return (c >= 500 && c<=599); // assumption: always retry if a 5xx HTTP status code is received (irrespective of the route)
}

static bool isAlwaysRetryableCurlError(int c) {
  // Return true, iff it is always safe to retry on given CURLerror.

  // Ref: http://curl.haxx.se/libcurl/c/libcurl-errors.html
  // TODO: Add more retryable errors to this list (and sanity check existing ones)
  return (c == 2 || c == 5 || c == 6 || c == 7 || c == 35);
}

// load environment variables at very start
bool dummy = loadFromEnvironment();

// Note: We only consider 200 as a successful response, all others are considered "failures"
JSON DXHTTPRequest(const string &resource, const string &data,
                   const bool alwaysRetry, const map<string, string> &headers) {
  // We use an std::atomic variable (C++11 feature) to avoid acquiring a lock
  // every time in DXHTTPRequest(). Lock is instead acquired in loadFromEnvironment().
  // By checking g_loadFromEnvironment_finished value, we avoid calling
  // loadFromEnvironment() every time (and acquiring the expensive lock)
  // Note: In this case a regular variable instead of atomic, will also work correctly.
  //       (except can result in few extra short-circuited calls to loadFromEnvironment()).
  //
  // Update: Removed "std::atomic" from everywhere, because Clang do not support them yet :(
  if (g_loadFromEnvironment_finished == false) {
    loadFromEnvironment();
  }
  // General Note: We try and use a single call to operator <<() while outputting to std::cerr.
  //               (so that output in multi-threading env) has less "chance" of being garbled
  //               Not sure if a single call to operator <<() is thread safe from C++11, but
  //               certainly multiple calls can mix output of various threads.
  //               For some relevant commentary see -> dxfile.cc: getChunkHttp_()

  if (!g_APISERVER_SET) {
    throw DXError("dxcpp::DXHTTPRequest()-> API server information not found (g_APISERVER is empty). Please set DX_APISERVER_HOST, DX_APISERVER_PORT, and DX_APISERVER_PROTOCOL.");
  }
  if (!g_SECURITY_CONTEXT_SET) {
    throw DXError("dxcpp::DXHTTPRequest()-> No Authentication token found. Please set DX_SECURITY_CONTEXT.");
  }

  string url = g_APISERVER + resource;

  HttpHeaders req_headers;
  JSON secContext = g_SECURITY_CONTEXT;
  req_headers["Authorization"] = secContext["auth_token_type"].get<string>() + " " + secContext["auth_token"].get<string>();
  req_headers["DNAnexus-API"] = g_API_VERSION;

  string headername;

  // TODO: Reconsider this; would we rather just set the content-type
  // to json always?
  bool content_type_set = false;
  for (map<string, string>::const_iterator iter = headers.begin(); iter != headers.end(); iter++) {
    headername = iter->first;
    req_headers[headername] = iter->second;

    transform(headername.begin(), headername.end(), headername.begin(), ::tolower);
    if (headername == "content-type") {
      content_type_set = true;
    }
  }

  if (!content_type_set)
    req_headers["Content-Type"] = "application/json";

  // TODO: Load retry parameters (wait time, max number of retries, etc)
  // from some config file

  unsigned int countTries;
  HttpRequest req;
  unsigned int sec_to_wait = 2; // number of seconds to wait before retrying first time. Will keep on doubling the wait time for each subsequent retry.
  bool reqCompleted; // did last request actually went through, i.e., some response was received)
  HttpRequestException hre;

  // The HTTP Request is always executed at least once,
  // a maximum of NUM_MAX_RETRIES number of subsequent tries are made, if required and feasible.
  for (countTries = 0u; countTries <= NUM_MAX_RETRIES; ++countTries, sec_to_wait *= 2u) {
    bool toRetry; // whether or not the request should be retried on failure
    
    // TODO: Add check for Content-Length header (we should retry if content-length header mismatches with actual data received)

    reqCompleted = true; // will explicitly set it to false in case request couldn't be completed
    try {
      // Attempt a POST request
      req = HttpRequest::request(HTTP_POST, url, req_headers, data.data(), data.size());
    } catch (HttpRequestException &e) {
      // Retry the request in any of these three scenarios:
      //  - alwaysRetry = true in the call to this function
      //  - errorCode returned by HttpRequestException is < 0 (which implies that request was never made to the server)server
      //  - isAlwaysRetryableCurlError() - A list of curl codes, which are *ALWAYS* safe to retry (irrespective of the request being made - idempotent or not, etc).
      toRetry = alwaysRetry || (e.errorCode < 0) || isAlwaysRetryableCurlError(e.errorCode);
      reqCompleted = false;
      hre = e;
    }

    if (reqCompleted) {
      if (req.responseCode != 200) {
        toRetry = isAlwaysRetryableHttpCode(req.responseCode);
      } else {
        // Everything is fine, the request went through and 200 received
        // So return back the response now
        if (countTries != 0u) // if at least one retry was made, print eventual success on stderr
          if (!g_dxcpp_mute_retry_cerrs)
            cerr << "\nRequest completed successfully in Retry #" << countTries << endl;

        try {
          return JSON::parse(req.respData); // we always return json output
        } catch (JSONException &je) {
          ostringstream errStr;
          errStr << "\nERROR: Unable to parse output returned by Apiserver as JSON" << endl;
          errStr << "HttpRequest url: " << url << "; response code: " << req.responseCode + "; response body: '" + req.respData + "'" << endl;
          errStr << "JSONException: " << je.what() << endl;
          throw DXError(errStr.str());
        }
      }
    }

    if (toRetry && (countTries < NUM_MAX_RETRIES)) {
      if (reqCompleted) {
        if (!g_dxcpp_mute_retry_cerrs)
          cerr << "\nWARNING: POST " << url << " returned with HTTP code " << req.responseCode << " and body: '" << req.respData << "'" << endl;
      } else {
        if (!g_dxcpp_mute_retry_cerrs)
          cerr << "\nWARNING: Unable to complete request: POST " << url << ". Details: '" << hre.what() << "'" << endl;
      }
      if (!g_dxcpp_mute_retry_cerrs)
        cerr << "\n... Waiting " << sec_to_wait << " seconds before retry " << (countTries + 1) << " of " << NUM_MAX_RETRIES << " ..." << endl;

      boost::this_thread::sleep(boost::posix_time::milliseconds(sec_to_wait * 1000));
    } else {
      countTries++;
      break;
    }
  }

  // We are here, implies, All retries were exhausted (or not attempted) with failure.
  if (reqCompleted) {
    if (!g_dxcpp_mute_retry_cerrs)
      cerr << "\nERROR: POST " + url + " returned non-200 http code in (at least) last of " << countTries << " attempts. Will throw." << endl;

    JSON respJSON;
    try {
      respJSON = JSON::parse(req.respData);
    } catch (...) {
      // If invalid json, throw general DXError
      throw DXError("Server's response code: '" + boost::lexical_cast<string>(req.responseCode) + "', response: '" + req.respData + "'");
    }
    throw DXAPIError(respJSON["error"]["type"].get<string>(),
                     respJSON["error"]["message"].get<string>(),
                     req.responseCode);
  } else {
    if (!g_dxcpp_mute_retry_cerrs)
      cerr << "\nERROR: Unable to complete request: POST " << url << " in " << countTries << " attempts. Will throw DXError." << endl;

    throw DXError("An exception was thrown while trying to make the request: POST " + url + " . Details: '" + hre.err + "'. ");
  }
  // Unreachable line
}

void setAPIServerInfo(const string &host, int port, const string &protocol) {
  g_APISERVER_HOST = host;
  char portstr[10];
  sprintf(portstr, "%d", port);
  g_APISERVER_PORT = string(portstr);
  g_APISERVER = protocol + "://" + host + ":" + g_APISERVER_PORT;
  g_APISERVER_PROTOCOL = protocol; 
  g_APISERVER_SET = true;
}

void setSecurityContext(const JSON &security_context) {
  g_SECURITY_CONTEXT = security_context;

  g_SECURITY_CONTEXT_SET = true;
}

void setJobID(const string &job_id) {
  g_JOB_ID = job_id;
}

void setWorkspaceID(const string &workspace_id) {
  g_WORKSPACE_ID = workspace_id;
}

void setProjectContext(const string &project_id) {
  g_PROJECT_CONTEXT_ID = project_id;
}

// This function populates input param "val" with the value of particular
// field "key" in the config file.
// If the key is not found (or file does not exist) then "false" is returned (else "true")
// Note: Reads the config file only the first time (save contents in a global variable)
// Note: If key is not found in config file, then "val" remain unchanged
bool getVariableFromConfigFile(string fname, string key, string &val) {

  // Read file only if it hasn't been successfully read before
  if (g_config_file_contents[fname].size() == 0) {
    // Try reading in the contents of config file
    ifstream fp(fname.c_str());
    if (!fp.is_open()) // file could not be opened
      return false;
    // Reserve memory for string upfront (to avoid having reallocation multiple time)
    fp.seekg(0, ios::end);
    g_config_file_contents[fname].reserve(fp.tellg());
    fp.seekg(0, ios::beg);

    // copy the contents of file into the string
    // Note: the extra parentheses around first parameter
    //       are required (due to "most vexing parsing" problem in C++)
    g_config_file_contents[fname].assign((istreambuf_iterator<char>(fp)), istreambuf_iterator<char>());
    fp.close();
  }
  // Since regex (C++11 feature) are not implemented in g++ yet,
  // we use boost::regex
  boost::regex expression(string("^\\s*export\\s*") + key + string("\\s*=\\s*'([^'\\r\\n]+)'$"), boost::regex::perl);
  boost::match_results<string::const_iterator> what;
  string::const_iterator itb = g_config_file_contents[fname].begin();
  string::const_iterator ite = g_config_file_contents[fname].end();

  if (!boost::regex_search(itb, ite, what, expression, boost::match_default)) {
    return false;
  }
  if (what.size() < 2) {
    return false;
  }
  val = what[what.size() - 1];
  return true;
}

// First look in env variable to find the value
// If not found, then look into user's config file (in home directory) for the value
// If still not found, then look into /opt/dnanexus/environment
//
// Returns false if not found in either of the 3 places, else true
// "val" contain the value of variable if function returned "true", unchanged otherwise
bool getFromEnvOrConfig(string key, string &val) {
  if (getenv(key.c_str()) != NULL) {
    val = getenv(key.c_str());
    if (PRINT_ENV_VAR_VALUES_WHEN_LOADED) {
      cerr << "Reading '" << key << "' value from environment variables. Value = '" << val << "'" << endl;
    }
    return true;
  }
  const string user_config_file_path = joinPath(getUserHomeDirectory(), ".dnanexus_config", "environment");
  if (getVariableFromConfigFile(user_config_file_path, key, val)) {
    if (PRINT_ENV_VAR_VALUES_WHEN_LOADED) {
      cerr << "Reading '" << key << "' value from file: '" << user_config_file_path << "'. Value = '" << val + "'" << endl;
    } 
    return true;
  }

  const string default_config_file_path = "/opt/dnanexus/environment";
  if (getVariableFromConfigFile(default_config_file_path, key, val)) {
    if (PRINT_ENV_VAR_VALUES_WHEN_LOADED) {
      cerr << "Reading '" << key << "' value from file: '" << default_config_file_path << "'. Value = '" << val + "'" << endl;
    }
    return true;
  }

  return false;
}

string getVariableForPrinting(string s) {
  if (s == "") {
    return "NOT SET";
  } else {
    return string("'") + s + string("'");
  }
}

string getVariableForPrinting(const JSON &j) {
  if (j.type() == JSON_UNDEFINED) {
    return "NOT SET";
  } else {
    return string("'") + j.toString() + string("'");
  }
}

// Returns a dummy value from this function (always true)
bool loadFromEnvironment() {
  // Mutex's aim: To ensure that environment variable are loaded only once.
  //              All other calls to loadFromEnvironment() will be short circuited.
  boost::mutex::scoped_lock glock(g_loadFromEnvironment_mutex);
  // It is important to acquire lock before checking g_loadFromEnvironment_finished == true
  // condition, since other instance of the function might be running in parallel thread,
  // we must wait for it to finish (and set g_loadFromEnvironment_finished = true)
  if (g_loadFromEnvironment_finished == true)
    return true; // Short circuit this call - env variables already loaded


  // initialized with default values, will be overridden by env variable/config file (if present)
  string apiserver_host = "api.dnanexus.com";
  string apiserver_port = "443";
  string apiserver_protocol = "https";

  getFromEnvOrConfig("DX_APISERVER_HOST", apiserver_host);
  getFromEnvOrConfig("DX_APISERVER_PORT", apiserver_port);
  getFromEnvOrConfig("DX_APISERVER_PROTOCOL", apiserver_protocol);
  getFromEnvOrConfig("DX_CA_CERT", get_g_DX_CA_CERT());

  setAPIServerInfo(apiserver_host, boost::lexical_cast<int>(apiserver_port), apiserver_protocol);

  string tmp;
  if (getFromEnvOrConfig("DX_SECURITY_CONTEXT", tmp)) {
    setSecurityContext(JSON::parse(tmp));
  }

  if (getFromEnvOrConfig("DX_JOB_ID", tmp)) {
    setJobID(tmp);
    if (getFromEnvOrConfig("DX_WORKSPACE_ID", tmp))
      setWorkspaceID(tmp);
    if (getFromEnvOrConfig("DX_PROJECT_CONTEXT_ID", tmp))
      setProjectContext(tmp);
  } else {
    if (getFromEnvOrConfig("DX_PROJECT_CONTEXT_ID", tmp))
      setWorkspaceID(tmp);
  }
  if (PRINT_ENV_VAR_VALUES_WHEN_LOADED) {
    cerr << "\n***** In dxcpp.cc::loadFromEnvironment() - Will set Global Variables for dxcpp *****" << endl;
    cerr << "These values will be used by dxcpp library now:" << endl;
    cerr << "1. g_APISERVER_HOST: " << getVariableForPrinting(g_APISERVER_HOST) << endl;
    cerr << "2. g_APISERVER_PORT: " << getVariableForPrinting(g_APISERVER_PORT) << endl;
    cerr << "3. g_APISERVER_PROTOCOL: " << getVariableForPrinting(g_APISERVER_PROTOCOL) << endl;
    cerr << "4. g_APISERVER: " << getVariableForPrinting(g_APISERVER) << endl;
    cerr << "5. g_SECURITY_CONTEXT: " << getVariableForPrinting(g_SECURITY_CONTEXT) << endl;
    cerr << "6. g_JOB_ID: " << getVariableForPrinting(g_JOB_ID) << endl;
    cerr << "7. g_WORKSPACE_ID: " << getVariableForPrinting(g_WORKSPACE_ID) << endl;
    cerr << "8. g_PROJECT_CONTEXT_ID: " << getVariableForPrinting(g_PROJECT_CONTEXT_ID) << endl;
    cerr << "9. g_API_VERSION: " << getVariableForPrinting(g_API_VERSION) << endl;
    cerr << "10. g_DX_CA_CERT: " << getVariableForPrinting(get_g_DX_CA_CERT()) << endl;
    cerr << "***** Will exit dxcpp.cc::loadFromEnvironment() - Global Variable set as noted above *****" << endl;
  }
  g_config_file_contents.clear(); // Remove the contents of config file - we no longer need them
  g_loadFromEnvironment_finished = true;


  return true;
}
