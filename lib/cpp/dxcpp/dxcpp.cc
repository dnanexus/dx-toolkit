#include <algorithm>
#include "dxcpp.h"
#include "SimpleHttp.h"

// Example environment variables
//
// APISERVER_PORT=8124
// APISERVER_HOST=localhost
// SECURITY_CONTEXT='{"auth_token":"outside","auth_token_type":"Bearer"}'

using namespace std;
using namespace dx;

bool g_ENV_LOADED = false;

string g_APISERVER_HOST;
string g_APISERVER_PORT;
string g_APISERVER;
JSON g_SECURITY_CONTEXT;

JSON DXHTTPRequest(const string &resource, const string &data,
		   const map<string, string> &headers) {
  if (!g_ENV_LOADED) {
    loadFromEnvironment();
    g_ENV_LOADED = true;
  }

  string url = g_APISERVER + resource;
  
  HttpHeaders req_headers;
  JSON secContext = g_SECURITY_CONTEXT;
  req_headers["Authorization"] = secContext["auth_token_type"].get<string>() +
    " " + secContext["auth_token"].get<string>();

  string headername;
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

  HttpClientRequest req;

  // Attempt a POST request
  req.setUrl(url);
  req.setReqData(data.data(), data.size());
  req.setMethod("POST");
  req.setHeaders(req_headers);
  req.send();

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
}

void setSecurityContext(const JSON &security_context) {
  g_SECURITY_CONTEXT = security_context;
}

void loadFromEnvironment() {
  if ((getenv("APISERVER_HOST") != NULL) and
      (getenv("APISERVER_PORT") != NULL))
    setAPIServerInfo(getenv("APISERVER_HOST"),
		     atoi(getenv("APISERVER_PORT")));

  if (getenv("SECURITY_CONTEXT") != NULL)
    g_SECURITY_CONTEXT = JSON::parse(getenv("SECURITY_CONTEXT"));
}
