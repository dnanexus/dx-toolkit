#include <algorithm>
#include <boost/thread.hpp>
#include <atomic>
#include "dxcpp.h"
#include "SimpleHttp.h"

// Example environment variables
//
// DX_APISERVER_PORT=8124
// DX_APISERVER_HOST=localhost
// DX_SECURITY_CONTEXT='{"auth_token":"outside","auth_token_type":"Bearer"}'

using namespace std;
using namespace dx;

const string g_API_VERSION = "1.0.0";

bool g_APISERVER_SET = false;
bool g_SECURITY_CONTEXT_SET = false;
bool g_WORKSPACE_ID_SET = false;

string g_APISERVER_HOST;
string g_APISERVER_PORT;
string g_APISERVER;
JSON g_SECURITY_CONTEXT;
string g_JOB_ID;
string g_WORKSPACE_ID;
string g_PROJECT_CONTEXT_ID;
string g_APISERVER_PROTOCOL;

const unsigned int NUM_MAX_RETRIES = 5u; // For DXHTTPRequest()

boost::mutex loadFromEnvironment_mutex_g;
std::atomic<bool> loadFromEnvironment_finished_g(false);

static bool isRetriableHttpCode(int c) {
  // Ref: Python bindings
  return (c == 500 || c == 502 || c == 503 || c == 504);
}

static bool isRetriableCurlError(int c) {
  // Return true, iff it is always safe to retry on given CURLerror.

  // Ref: http://curl.haxx.se/libcurl/c/libcurl-errors.html
  // TODO: Add more retriable errors to this list (and sanity check existing ones)
  return (c == 2 || c == 5 || c == 6 || c == 7 || c == 35);
}

JSON DXHTTPRequest(const string &resource, const string &data,
       const bool alwaysRetry,
		   const map<string, string> &headers) {

  // We use an atomic variable (C++11 feature) to avoid acquiring a lock
  // every time in DXHTTPRequest(). Lock is instead acquired in loadFromEnvironment().
  // By checking loadFromEnvironment_finished_g value, we avoid calling
  // loadFromEnvironment() every time (and acquiring the expensive lock)
  // Note: In this case a regular variable instead of atomic, will also work correctly.
  //       (except can result in few extra short-circuited calls to loadFromEnvironment()).
  if (loadFromEnvironment_finished_g.load() == false) {
    loadFromEnvironment();
  }
  if (!g_APISERVER_SET || !g_SECURITY_CONTEXT_SET) {
    std::cerr << "Error: API server information (DX_APISERVER_HOST and DX_APISERVER_PORT) and/or security context (DX_SECURITY_CONTEXT) not set." << std::endl;
    throw;
  }

  string url = g_APISERVER + resource;

  HttpHeaders req_headers;
  JSON secContext = g_SECURITY_CONTEXT;
  req_headers["Authorization"] = secContext["auth_token_type"].get<string>() +
    " " + secContext["auth_token"].get<string>();
  req_headers["DNAnexus-API"] = g_API_VERSION;

  string headername;

  // TODO: Reconsider this; would we rather just set the content-type
  // to json always?
  bool content_type_set = false;
  for (map<string, string>::const_iterator iter = headers.begin();
       iter != headers.end();
       iter++) {
    headername = iter->first;
    req_headers[headername] = iter->second;

    transform(headername.begin(), headername.end(), headername.begin(), ::tolower);
    if (headername == "content-type")
      content_type_set = true;
  }

  if (!content_type_set)
    req_headers["Content-Type"] = "application/json";
  
  // TODO: Load retry parameteres (wait time, max number of retries, etc) from some config file

  unsigned int countTries;
  HttpRequest req;
  unsigned int sec_to_wait = 2; // number of seconds to wait before retrying first time. will keep on doubling the wait time for each subsequent retry.
  bool reqCompleted; // did last request actually went through (i.e., some response was recieved)
  HttpRequestException hre;

  // The HTTP Request is always executed at least once,
  // a maximum of NUM_MAX_RETRIES number of subsequent tries are made, if required and feasible.
  for (countTries = 0u; countTries <= NUM_MAX_RETRIES; ++countTries, sec_to_wait *= 2u) {
    bool toRetry = alwaysRetry; // whether or not the request should be retried on failure
    reqCompleted = true; // will explicitly set it to false in case request couldn't be completed
    try {
      // Attempt a POST request
      req = HttpRequest::request(HTTP_POST,
               url,
               req_headers,
               data.data(),
               data.size());
    } catch(HttpRequestException &e) {
      toRetry = toRetry || (e.errorCode < 0) || isRetriableCurlError(e.errorCode);
      reqCompleted = false;
      hre = e;
    }

    if (reqCompleted) {
      if (req.responseCode != 200) {
        toRetry = toRetry || isRetriableHttpCode(req.responseCode);
      }
      else {
        // Everything is fine, the request went through (and 200 recieved)
        // So return back the response now
        if (countTries != 0u) // if atleast one retry was made, print eventual success on stderr
          std::cerr << "\nRequest completed succesfuly in Retry #" << countTries;

        try {
          return JSON::parse(req.respData); // we always return json output
        } 
        catch (JSONException &je) {
          string errStr = "ERROR: Unable to parse output returned by APIServer as JSON";
          errStr += "\nHttpRequest url: " + url + " , response code = " + boost::lexical_cast<string>(req.responseCode) + ", response body: '" + req.respData + "'";
          errStr += "\nJSONException: " + std::string(je.what());
          throw DXError(errStr);
        }
      }
    }
    if (toRetry && countTries < NUM_MAX_RETRIES) {
      if (reqCompleted)
        std::cerr << "\nWARNING: POST " << url << ": returned with HTTP code " << req.responseCode << " and body: '" << req.respData << "'";
      else
        std::cerr << "\nWARNING: Unable to complete request -> POST " << url << " . Details: '" << hre.what() << "'";
  
      std::cerr << "\n... Waiting " << sec_to_wait << " seconds before retry " << countTries + 1 << " of " << NUM_MAX_RETRIES << " ...";

      // TODO: Should we use select() instead of sleep() - as sleep will return immediatly if a signal is passed to program ?
      // (http://www.delorie.com/gnu/docs/glibc/libc_445.html)
      // Also we do not check for buffer overflow while doubling sec_to_wait, since we would have to sleep ~6537year before hitting the limit!
      sleep(sec_to_wait);
    }
    else {
      countTries++;
      break;
    }
  }
  // We are here, implies, All retries were exhausted (or not made) with failure.
  
  if (reqCompleted) {
    std::cerr << "\nERROR: POST " << url << " returned non-200 http code in (at least) last of " << countTries << " attempt. Will throw DXAPIError.\n"; 
    JSON respJSON = JSON::parse(req.respData);
    throw DXAPIError(respJSON["error"]["type"].get<string>(),
             respJSON["error"]["message"].get<string>(),
             req.responseCode);
  } else {
    std::cerr << "\nERROR: Unable to complete request -> POST " << url << " in " << countTries << " attempts. Will throw DXError.\n";
    throw DXError("An exception was thrown while trying to make the request: POST " + url + " . Details: '" + hre.err + "'. ");
  }
  // Unreachable line
}

void setAPIServerInfo(const string &host,
		      int port,
		      const string &protocol) {
  g_APISERVER_HOST = host;
  char portstr[10];
  sprintf(portstr, "%d", port);
  g_APISERVER_PORT = string(portstr);
  g_APISERVER = protocol + "://" + host + ":" + g_APISERVER_PORT;

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

void loadFromEnvironment() {
  // Mutex's aim: To ensure that enviornment variable are loaded only once.
  //              All other calls to loadFromEnvironment() must be short circuited.
  boost::mutex::scoped_lock glock(loadFromEnvironment_mutex_g);
  
  // It is important to acquire lock before checking loadFromEnvironment_finished_g == true 
  // condition, since other instance of the function might be running in parallel thread, 
  // we must wait for it to finish (and set loadFromEnvironment_finished_g = true)
  if (loadFromEnvironment_finished_g.load() == true)
    return; // Short circuit this call - env variables already loaded

  if (!g_APISERVER_SET &&
      (getenv("DX_APISERVER_HOST") != NULL) &&
      (getenv("DX_APISERVER_PORT") != NULL)) {
    if (getenv("DX_APISERVER_PROTOCOL") != NULL) {
      setAPIServerInfo(getenv("DX_APISERVER_HOST"),
                       atoi(getenv("DX_APISERVER_PORT")),
                       getenv("DX_APISERVER_PROTOCOL"));
    } else {
      setAPIServerInfo(getenv("DX_APISERVER_HOST"),
                       atoi(getenv("DX_APISERVER_PORT")));
    }
  }

  if (!g_SECURITY_CONTEXT_SET &&
      getenv("DX_SECURITY_CONTEXT") != NULL)
    setSecurityContext(JSON::parse(getenv("DX_SECURITY_CONTEXT")));

  if (!g_WORKSPACE_ID_SET) {
    if (getenv("DX_JOB_ID") != NULL) {
      setJobID(getenv("DX_JOB_ID"));
      if (getenv("DX_WORKSPACE_ID") != NULL)
	      setWorkspaceID(getenv("DX_WORKSPACE_ID"));
      if (getenv("DX_PROJECT_CONTEXT_ID") != NULL)
	      setProjectContext(getenv("DX_PROJECT_CONTEXT_ID"));
    } else if (getenv("DX_PROJECT_CONTEXT_ID") != NULL) {
      setWorkspaceID(getenv("DX_PROJECT_CONTEXT_ID"));
    }
  }
  loadFromEnvironment_finished_g.store(true);
}
