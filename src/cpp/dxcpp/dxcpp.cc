// Copyright (C) 2013-2016 DNAnexus, Inc.
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
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/linear_congruential.hpp>

#include "dxlog.h"
#include "dxcpp.h"
#include "SimpleHttp.h"
#include "ignore_sigpipe.h"
#include "utils.h"

#include <boost/version.hpp>
// http://www.boost.org/doc/libs/1_48_0/libs/config/doc/html/boost_config/boost_macro_reference.html
#if ((BOOST_VERSION / 100000) < 1 || ((BOOST_VERSION/100000) == 1 && ((BOOST_VERSION / 100) % 1000) < 48))
  #error "Cannot compile dxcpp using Boost version < 1.48"
#endif

#ifndef DXTOOLKIT_GITVERSION
  #error  "Macro DXTOOLKIT_GITVERSION must be defined"
#endif

using namespace std;

namespace dx {
  namespace config {
    string& APISERVER_HOST() {
      static string local = "api.dnanexus.com"; //default value
      return local;
    }
    string& APISERVER_PORT() {
      static string local = "443"; // default value
      return local;
    }
    string& APISERVER_PROTOCOL() {
      static string local = "https"; // default value
      return local;
    }
    // Value of the variable returned is guaranteed to be either a syntactically valid auth token,
    // or an empty hash
    JSON& SECURITY_CONTEXT() {
      static JSON local(JSON_HASH);
      return local;
    }
    string& JOB_ID() {
      static string local = "";
      return local;
    }
    string& WORKSPACE_ID() {
      static string local = "";
      return local;
    }
    string& PROJECT_CONTEXT_ID() {
      static string local = "";
      return local;
    }
    
    string& CURRENT_PROJECT() {
      if (!JOB_ID().empty())
        return WORKSPACE_ID();
      return PROJECT_CONTEXT_ID();
    }
    string APISERVER() {
      if (APISERVER_HOST().empty() || APISERVER_PORT().empty() || APISERVER_PROTOCOL().empty())
        return "";
      return APISERVER_PROTOCOL() + "://" + APISERVER_HOST() + ":" + APISERVER_PORT();
    }
    const string& API_VERSION() {
      const static string local = "1.0.0";
      return local;
    }
  }
 
  // Example environment variables
  //
  // DX_APISERVER_PORT=8124
  // DX_APISERVER_HOST=localhost
  // DX_SECURITY_CONTEXT='{"auth_token":"outside","auth_token_type":"Bearer"}'


  static bool isAlwaysRetryableHttpCode(int c) {
    return (c >= 500 && c<=599); // assumption: always retry if a 5xx HTTP status code is received (irrespective of the route)
  }

  static bool isAlwaysRetryableCurlError(int c) {
    // Return true, iff it is always safe to retry on given CURLerror.

    // Ref: http://curl.haxx.se/libcurl/c/libcurl-errors.html
    // TODO: Add more retryable errors to this list (and sanity check existing ones)
    return (c == 2 || c == 5 || c == 6 || c == 7 || c == 35);
  }

  // Note: We only consider 200 as a successful response, all others are considered "failures"
  JSON DXHTTPRequest(const string &resource, const string &data, const bool safeToRetry, const map<string, string> &headers) {
    DXLOG(logDEBUG) << "In DXHTTPRequest(), inputs:" << endl
                  << " --resources = '" << resource << "'" << endl
                  << " --safeToRetry = " << safeToRetry << endl
                  << " --data = '" << data.substr(0, 100) << "'" << endl
                  << " --headers = '" << JSON(headers).toString() << "'";
    const unsigned int NUM_MAX_RETRIES = 5u; // maximum number of retries for an individual request

    if (config::APISERVER().empty()) {
      throw DXError("dxcpp::DXHTTPRequest()-> API server information not found. Please set DX_APISERVER_HOST, DX_APISERVER_PORT, and DX_APISERVER_PROTOCOL.", "ApiserverInfoMissing");
    }

    const JSON &ctx = config::SECURITY_CONTEXT();
    if (ctx.type() != JSON_HASH
        || !ctx.has("auth_token_type") || ctx["auth_token_type"].type() != JSON_STRING
        || !ctx.has("auth_token") || ctx["auth_token"].type() != JSON_STRING) {
      if (ctx.type() != JSON_HASH || ctx.size() == 0) {
        throw DXError("dxcpp::DXHTTPRequest()-> DX_SECURITY_CONTEXT is either not set, or not a valid JSON", "SecurityContextNotFound");
      } else {
        throw DXError("dxcpp::DXHTTPRequest()-> Invalid DX_SECURITY_CONTEXT string: '" + ctx.toString() + "'", "BadSecurityContextString");
      }
    }

    string url = config::APISERVER() + resource;
    HttpHeaders req_headers;
    req_headers["Authorization"] = ctx["auth_token_type"].get<string>() + " " + ctx["auth_token"].get<string>();
    req_headers["DNAnexus-API"] = config::API_VERSION();

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
      req_headers["Content-Type"] = "application/json; charset=utf-8";

    // TODO: Load retry parameters (wait time, max number of retries, etc)
    // from some config file

    unsigned int countTries = 0u;
    HttpRequest req;
    unsigned int sec_to_wait = 2; // number of seconds to wait before retrying first time. Will keep on doubling the wait time for each subsequent retry.
    bool reqCompleted; // did last request actually went through, i.e., some response was received)
    bool contentLengthMismatch;
    bool contentLengthMissing;
    unsigned int retryAfterSeconds = 60u; // Number of seconds to retry after,
                                          // in the event of a 503 response
    HttpRequestException hre;

    // The HTTP Request is always executed at least once,
    // a maximum of NUM_MAX_RETRIES number of subsequent tries are made, if required and feasible.
    while (true) {

      // Variable "toRetry" indicates whether or not the request should be retried on failure
      // Note: Initial value of "false" is just a dummy value, toRetry will always be re-init before being used.
      //       This dummy initial value is provided, to prevent some spurious warnings from clang
      bool toRetry = false;
      // True if the request returns with a 503
      bool serviceUnavailable = false;

      reqCompleted = true; // will explicitly set it to false in case request couldn't be completed
      try {
        DXLOG(logDEBUG) << "Attempting the actual HTTP request (countTries = " << countTries << ")...";
        // Attempt a POST request
        req = HttpRequest::request(HTTP_POST, url, req_headers, data.data(), data.size());
        DXLOG(logDEBUG) << "Request completed, responseCode = '" << req.responseCode << "'";
      } catch (HttpRequestException &e) {
        DXLOG(logDEBUG) << "HttpRequestException thrown ... message = '" << e.what() << "'";
        // Retry the request in any of these three scenarios:
        //
        //  - safeToRetry is true
        //  - errorCode returned by HttpRequestException is < 0 (implies that
        //    the request was never made to the server)
        //  - isAlwaysRetryableCurlError() - A list of curl codes, which are
        //    *ALWAYS* safe to retry (irrespective of the request being made -
        //    idempotent or not, etc).
        toRetry = safeToRetry || (e.errorCode < 0) || isAlwaysRetryableCurlError(e.errorCode);
        reqCompleted = false;
        hre = e;
      }

      if (reqCompleted) {
        if (req.responseCode != 200) {
          DXLOG(logWARNING) << "POST '" << url << "' returned with HTTP code '" << req.responseCode << "'; and body: '" << req.respData << "'";
          toRetry = isAlwaysRetryableHttpCode(req.responseCode);
          if (req.responseCode == 503) {
            serviceUnavailable = true;
            string retryAfterHeader;
            bool retryAfterMissing;
            retryAfterMissing = !req.respHeader.getHeaderString("Retry-After", retryAfterHeader);
            retryAfterSeconds = retryAfterMissing ? 60 : boost::lexical_cast<size_t>(retryAfterHeader);
          }
        } else {
          // We are here => The request went thru, we got 200 and a response
          string clHeader; // content-length header
          contentLengthMissing = !req.respHeader.getHeaderString("Content-Length", clHeader);
          contentLengthMismatch = !contentLengthMissing && (boost::lexical_cast<size_t>(clHeader) != req.respData.size());
          if (contentLengthMismatch) {
            // This is an error situation for us, retry only if explicitly asked
            toRetry = safeToRetry;
            DXLOG(logWARNING) << "POST '" << url << "': Expected Content-Length to be '" << clHeader << "' (from Content-Length header)"
                              << "but received " << req.respData.size() << ", retry = " << ((safeToRetry) ? "true" : "false");
          } else {
            try {
              JSON out = JSON::parse(req.respData);
              if (countTries != 0u) {
                // if at least one retry was made, print eventual success on stderr
                DXLOG(logWARNING) << "Request completed successfully in Retry #" << countTries;
              }
              DXLOG(logDEBUG) << "Exiting DXHTTPRequest() successfully";
              return out;
            } catch (JSONException &je) {
              if (contentLengthMissing) {
                DXLOG(logWARNING) << "POST '" << url << "': Unable to parse response from server as valid JSON, and no 'Content-Length' header was found either"
                                  << "retry = true";
                toRetry = true;
              } else {
                DXLOG(logERROR) << "POST '" << url << "': Unable to parse response from server as valid JSON, and Content-Length header was present ("
                                << clHeader << "). Will throw DXError()";
                ostringstream errStr;
                errStr << "\nERROR: Unable to parse output returned by Apiserver as JSON (and 'Content-length' header was present = " << clHeader << ")" << endl;
                errStr << "HttpRequest url: " << url << "; response code: " << req.responseCode << "; response size: '" << req.respData.size()
                       << "; response body: '" << req.respData.substr(0, 1000) << "'" << endl; // return at most 1000 characters from response (don't overwhelm user!)
                errStr << "JSONException: '" << je.what() << "'" << endl;
                throw DXError(errStr.str(), "UnableToParseAsJSON");
              }
            }
          }
        }
      }

      if (!toRetry || (countTries >= NUM_MAX_RETRIES)) {
        // Code after this loop wants to know how many attempts were made, so
        // update to reflect the most recent attempt.
        countTries++;
        break;
      }

      // 503 with Retry-After-- do not count such responses against the allowed
      // number of retries
      if (serviceUnavailable) {
        DXLOG(logWARNING) << "Service unavailable, waiting for " << retryAfterSeconds << " seconds : POST '" << url << "'";
        boost::this_thread::interruption_point();
        _internal::sleepUsingNanosleep(retryAfterSeconds);
        DXLOG(logDEBUG) << "Sleep finished, will recheck for service availability";
        continue;
      }

      assert(countTries < NUM_MAX_RETRIES);

      if (!reqCompleted) {
        DXLOG(logWARNING) << "Unable to complete request: POST '" << url << "' (in retry #" << (countTries + 1) << "). Details: '" << hre.what() << "'";
      }
      DXLOG(logWARNING) << "Waiting ... " << sec_to_wait << " seconds before retry " << (countTries + 1) << " of " << NUM_MAX_RETRIES << " ...";
      boost::this_thread::interruption_point();
      _internal::sleepUsingNanosleep(sec_to_wait);
      DXLOG(logDEBUG) << "Sleep finished, will go & retry the request";

      countTries++;
      sec_to_wait *= 2u;
    }

    // We are here, implies, All retries were exhausted (or not attempted) with failure.
    if (reqCompleted) {
      DXLOG(logERROR) << "POST '" + url + "': failed after " << countTries << "' attempts. Response code: " << req.responseCode;
      JSON respJSON;
      try {
        respJSON = JSON::parse(req.respData);
      } catch (JSONException &e) {
        // If invalid json, throw general DXError
        throw DXError("Server's response code: '" + boost::lexical_cast<string>(req.responseCode) + "', response: '" + req.respData.substr(0,1000) + 
                      "'. Could not parse response as valid JSON, error = '" + e.what() + "'", "UnableToParseAsJSON");
      }
      throw DXAPIError(respJSON["error"]["message"].get<string>(),
                       respJSON["error"]["type"].get<string>(),
                       req.responseCode);
    } else {
      DXLOG(logERROR) << "Unable to complete request: POST '" << url << "' in '" << countTries << "' attempts." << endl;
      throw DXConnectionError("Was unable to make the request: POST '" + url + "' . Details: '" + hre.err + "'.", hre.errorCode);
    }
    // Unreachable line
  }

  // This sub-namespace contains loadFromEnvironment(), and several other helper functions/variables,
  // which are used for reading dxcpp configuration when the library is loaded
  // -> Configuration is read by a constructor of a global variable (so before main() is loaded)
  // -> Configuration is read only once (as explained above). You should *not* explicitly call these functions.
  namespace _internal {
    map<string, string> g_config_file_contents_old; // hack
    JSON g_json_config_file_contents; // hack


    // This function populates input param "val" with the value of particular
    // field "key" in the config file.
    // If the key is not found (or file does not exist) then "false" is returned (else "true")
    // Note: Reads the config file only the first time (save contents in a global variable)
    // Note: If key is not found in config file, then "val" remain unchanged
    bool getVariableFromConfigFile_old(string fname, string key, string &val) {
      // Read file only if it hasn't been read before
      if (g_config_file_contents_old.count(fname) == 0) {
        g_config_file_contents_old[fname] = "";
        // Try reading in the contents of config file
        ifstream fp(fname.c_str());
        if (!fp.is_open()) // file could not be opened
          return false;
        // Reserve memory for string upfront (to avoid having reallocation multiple time)
        fp.seekg(0, ios::end);
        g_config_file_contents_old[fname].reserve(fp.tellg());
        fp.seekg(0, ios::beg);

        // copy the contents of file into the string
        // Note: the extra parentheses around first parameter
        //       are required (due to "most vexing parsing" problem in C++)
        g_config_file_contents_old[fname].assign((istreambuf_iterator<char>(fp)), istreambuf_iterator<char>());
        fp.close();
      }
      // Since regex (C++11 feature) are not implemented in g++ yet,
      // we use boost::regex
      boost::regex expression(string("^\\s*export\\s*") + key + string("\\s*=\\s*'([^'\\r\\n]+)'$"), boost::regex::perl);
      boost::match_results<string::const_iterator> what;
      string::const_iterator itb = g_config_file_contents_old[fname].begin();
      string::const_iterator ite = g_config_file_contents_old[fname].end();
      if (!boost::regex_search(itb, ite, what, expression, boost::match_default)) {
        return false;
      }
      if (what.size() < 2) {
        return false;
      }
      val = what[what.size() - 1];
      return true;
    }

    // This function populates input param "val" with the value of particular
    // field "key" in the config file.
    // If the key is not found, or file does not exist, or file is invalid JSON, then "false" is returned (else "true")
    // Note: Reads the config file only the first time (save contents in a global variable)
    // Note: If key is not found in config file, then "val" remain unchanged
    bool getVariableFromJsonConfigFile(string fname, string key, string &val) {
      if (g_json_config_file_contents.type() == JSON_NULL) {
        // Already tried opening/parsing the file, and failed
        // don't try again, just return false
        return false;
      }
      if (g_json_config_file_contents.type() == JSON_UNDEFINED) {
        // This is the first time this function is called, so parse the file, sanity check etc   
        ifstream fp(fname, std::fstream::in);
        if (!fp.is_open()) {
          // file not found
          g_json_config_file_contents = JSON(JSON_NULL);
          return false;
        }
        try {
          g_json_config_file_contents.read(fp);
        } catch (JSONException &j) {
          DXLOG(logWARNING) << "An error occured while trying to parse the JSON file '" << fname << "'. Will ignore contents of this file."
               << "Error = '" << j.what() << "'";
          g_json_config_file_contents = JSON(JSON_NULL); // don't attempt to parse file again
          return false;
        }
        if (g_json_config_file_contents.type() != JSON_HASH) {
          DXLOG(logWARNING) << "The file '" << fname << "' does not contain a valid JSON hash. Will ignore contents of this file.";
          g_json_config_file_contents = JSON(JSON_NULL); // don't attempt to parse file again
          return false;
        }
        // Sanity check thru file, assert that values are either strings or integers
        // If anything else if found print error, and ignore file conents
        for (JSON::object_iterator it = g_json_config_file_contents.object_begin(); it != g_json_config_file_contents.object_end(); ++it) {
          if (it->second.type() != JSON_STRING && it->second.type() != JSON_INTEGER) {
            DXLOG(logWARNING) << "The file '" << fname << "' contains a an invalid key (neither string, nor integer). Will ignore contents of this file.";
            g_json_config_file_contents = JSON(JSON_NULL); // don't attempt to parse file again
            return false;
          }
          if (it->second.type() == JSON_INTEGER) {
            // convert to integer
            it->second = boost::lexical_cast<string>(it->second.get<int64_t>());
            assert(it->second.type() == JSON_STRING); // just a sanity check
          }
        }
      } 
      if (!g_json_config_file_contents.has(key))
        return false;
      val = g_json_config_file_contents[key].get<string>();
      return true;
    }

    // Order of evaluation
    // 1) Env variables
    // 2) New style (JSON) config file in user's home directory: environment.json
    // 3) Old style (export=BLAH) config file in user's home directory: environment
    //
    // Returns false if not found in either of the 3 places, else true
    // "val" contain the value of variable if function returned "true", unchanged otherwise
    // Note: We have DISCONTINUED looking into "/opt/dnanexus/environment" file
    bool getFromEnvOrConfig(string key, string &val) {
      if (getenv(key.c_str()) != NULL) {
        val = getenv(key.c_str());
        DXLOG(logINFO) << "Reading '" << key << "' value from environment variables. Value = '" << val << "'";
        return true;
      }
      const string json_config_file_path = joinPath(getUserHomeDirectory(), ".dnanexus_config", "environment.json");
      if (getVariableFromJsonConfigFile(json_config_file_path, key, val)) {
        DXLOG(logINFO) << "Reading '" << key << "' value from file: '" << json_config_file_path << "'. Value = '" << val + "'";
        return true;
      }
      const string user_config_file_path = joinPath(getUserHomeDirectory(), ".dnanexus_config", "environment");
      if (getVariableFromConfigFile_old(user_config_file_path, key, val)) {
        DXLOG(logINFO) << "Reading '" << key << "' value from file: '" << user_config_file_path << "'. Value = '" << val + "'";
        return true;
      }
      return false;
    }

    string getVariableForPrinting(const string& s) {
      if (s.empty()) {
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
    void loadFromEnvironment() {
      static int hasBeenCalled = 0;
      if (hasBeenCalled == 0)
        hasBeenCalled = 1;
      else {
        assert("loadFromEnvironment() should not have been called more than once. Unexpcted");
      }
      
      using namespace dx::config;
      
      getFromEnvOrConfig("DX_APISERVER_HOST", APISERVER_HOST());
      getFromEnvOrConfig("DX_APISERVER_PORT", APISERVER_PORT());
      getFromEnvOrConfig("DX_APISERVER_PROTOCOL", APISERVER_PROTOCOL());
      getFromEnvOrConfig("DX_CA_CERT", CA_CERT());
      getFromEnvOrConfig("DX_LIBCURL_VERBOSE", LIBCURL_VERBOSE());
      getFromEnvOrConfig("DX_JOB_ID", JOB_ID());
      getFromEnvOrConfig("DX_WORKSPACE_ID", WORKSPACE_ID());
      getFromEnvOrConfig("DX_PROJECT_CONTEXT_ID", PROJECT_CONTEXT_ID());
      
      string tmp;
      if (getFromEnvOrConfig("DX_SECURITY_CONTEXT", tmp)) {
        try {
          JSON ctx = JSON::parse(tmp);
          if (ctx.type() != JSON_HASH
              || !ctx.has("auth_token_type") || ctx["auth_token_type"].type() != JSON_STRING
              || !ctx.has("auth_token") || ctx["auth_token"].type() != JSON_STRING) {
            // Auth token is syntactically incorrect
            // TODO: LOG a warning message (invalid security context string), when logger class is available
            SECURITY_CONTEXT() = JSON_HASH;
          } else {
            SECURITY_CONTEXT() = ctx;
          }
        } catch (JSONException &jerr) {
          // TODO: Log a warning message to in this case (when logger class is implemented)
          SECURITY_CONTEXT() = JSON_HASH; // reset it to default value (a blank hash)
        }
      }
      // Append dxcpp info to the default user agent string (set by dxhttp)
      USER_AGENT_STRING() = "dxcpp/" DXTOOLKIT_GITVERSION" " + USER_AGENT_STRING(); 
      DXLOG(logINFO) << "***** In dxcpp.cc::loadFromEnvironment() - Following global config parameters have been set for dxcpp *****";
      DXLOG(logINFO) << "These values will be used by dxcpp library now:";
      DXLOG(logINFO) << "1. APISERVER_HOST: " << getVariableForPrinting(APISERVER_HOST());
      DXLOG(logINFO) << "2. APISERVER_PORT: " << getVariableForPrinting(APISERVER_PORT());
      DXLOG(logINFO) << "3. APISERVER_PROTOCOL: " << getVariableForPrinting(APISERVER_PROTOCOL());
      DXLOG(logINFO) << "4. APISERVER: " << getVariableForPrinting(APISERVER());
      DXLOG(logINFO) << "5. SECURITY_CONTEXT: " << getVariableForPrinting(SECURITY_CONTEXT());
      DXLOG(logINFO) << "6. JOB_ID: " << getVariableForPrinting(JOB_ID());
      DXLOG(logINFO) << "7. WORKSPACE_ID: " << getVariableForPrinting(WORKSPACE_ID());
      DXLOG(logINFO) << "8. PROJECT_CONTEXT_ID: " << getVariableForPrinting(PROJECT_CONTEXT_ID());
      DXLOG(logINFO) << "9. API_VERSION: " << getVariableForPrinting(API_VERSION());
      DXLOG(logINFO) << "10. CA_CERT: " << getVariableForPrinting(CA_CERT());
      DXLOG(logINFO) << "11. Current Project: " << getVariableForPrinting(CURRENT_PROJECT());
      DXLOG(logINFO) << "12. User Agent String: " << getVariableForPrinting(USER_AGENT_STRING());
      DXLOG(logINFO) << "13. Libcurl verbose: " << getVariableForPrinting(LIBCURL_VERBOSE());
      DXLOG(logINFO) << "***** Will exit loadFromEnvironment() function in dxcpp.cc *****";
      
      g_config_file_contents_old.clear(); // Remove the contents of config file - we no longer need them
      if (g_json_config_file_contents.type() == JSON_HASH)
        g_json_config_file_contents.clear(); // Remove the contents of config file - we no longer need them
    }
    
    // This structure is used for static initization of dxcpp configuration
    // (read env variables, and stuff)
    // -> Only one variable of this struct is created, which triggers the constructor (which calls loadFromEnvironment())
    // -> Do *not* create any other variables of this structure anywhere else
    struct dxcpp_init {
      dxcpp_init() {
        Log::Init();
        loadFromEnvironment();
      }
    }the_only_instance;
  }

  // Ignore SIGPIPE to deal with PTFM-8366 & PTFM-7251 (Also see: http://sourceforge.net/p/curl/bugs/1180/)
  namespace _internal {
    #if !WINDOWS_BUILD
    // This function will called when SIGPIPE is caught
    static void sigpipe_catcher(int sig) {
      DXLOG(dx::logINFO) << "Caught SIGPIPE(signal_num = " << sig << ")... will ignore";
    }
    class IgnoreSIGPIPE {
      SIGPIPE_VARIABLE(pipe);
      public:
        IgnoreSIGPIPE() {
          DXLOG(logINFO) << "Ignoring SIGPIPE globally..."; // Note: won't be printed, unless env variable DXCPP_DEBUG is set
          sigpipe_ignore(&pipe, sigpipe_catcher);
        }
        ~IgnoreSIGPIPE() {
          DXLOG(logINFO) << "Restoring original SIGPIPE handler...";
          sigpipe_restore(&pipe);
        }
    };
    IgnoreSIGPIPE IgnoreSIGPIPE_static_initializer;
    #endif
  }

  namespace Nonce {
    using namespace boost::posix_time;
    ptime currTime() {
      return microsec_clock::local_time();
    }

    long getMicroSecs() {
      ptime now = currTime();
      ptime t0(now.date(), hours(0));
      time_duration td = now - t0;
      return td.total_microseconds();
    }

    static boost::minstd_rand generator(getMicroSecs());
    static boost::random::uniform_int_distribution<> uniform(0, 15);
    static const std::string hexchars = "0123456789abcdef";

    string nonce() {
      std::stringstream ss;
      for (int i=0; i<64;++i){
	       ss << hexchars[uniform(generator)];
      }
      ss << " " << currTime();
      return ss.str();
    }

    JSON updateNonce(const JSON &input_params) {
      JSON input_params_cp(input_params);
      if (!(input_params_cp.has("nonce") && !input_params_cp["nonce"].get<string>().empty())) {
        input_params_cp["nonce"] = nonce();
      }
      return input_params_cp;
    }
  }
}
