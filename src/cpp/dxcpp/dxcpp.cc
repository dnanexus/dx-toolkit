#include <algorithm>
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

JSON DXHTTPRequest(const string &resource, const string &data,
		   const map<string, string> &headers) {
  if (!g_APISERVER_SET || !g_SECURITY_CONTEXT_SET) {
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

  // Attempt a POST request
  HttpRequest req = HttpRequest::request(HTTP_POST,
					 url,
					 req_headers,
					 data.data(),
					 data.size());

  if (req.responseCode != 200) {
    JSON respJSON = JSON::parse(req.respData);
    throw DXAPIError(respJSON["error"]["type"].get<string>(),
    		     respJSON["error"]["message"].get<string>(),
    		     req.responseCode);
  }

  return JSON::parse(req.respData);
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
  if (!g_APISERVER_SET &&
      (getenv("DX_APISERVER_HOST") != NULL) &&
      (getenv("DX_APISERVER_PORT") != NULL))
    setAPIServerInfo(getenv("DX_APISERVER_HOST"),
		     atoi(getenv("DX_APISERVER_PORT")));

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
}
