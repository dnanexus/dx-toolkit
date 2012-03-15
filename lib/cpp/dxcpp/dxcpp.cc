#include <stdlib.h>
#include <algorithm>
#import "dxcpp.h"
#import "../SimpleHttpLib/SimpleHttp.h"

// APISERVER_PORT=8124
// APISERVER_HOST=localhost
// SECURITY_CONTEXT='{"auth_token":"outside","auth_token_type":"Bearer"}'

JSON dxpy::DXHTTPRequest(const string &resource, const string &data) {
  string url = g_APISERVER + resource;
  
  HttpHeaders toSend;
  JSON secContext(g_SECURITY_CONTEXT);
  // TODO: Uncomment after JSON available
  // toSend["Authorization"] = secContext["auth_token_type"] +
  //   " " + secContext["auth_token"];
  toSend["Content-Type"] = "application/json";

  // string headername();
  // for (map<string, string>::const_iterator iter = headers.begin();
  //      iter++;
  //      iter != headers.end()) {
  //   if (transform((iter->first).begin(), (iter->first).end(), headername.begin(), tolower) != "content-type")
  //     toSend[iter->first] = iter->second;
  // }

  HttpClientRequest req;

  // Attempt a POST request
  req.setUrl(url);
  req.setReqData(data.data(), data.size());
  req.setMethod("POST");
  req.send();

  if (req.responseCode != 200) {
    // TODO: uncomment when JSON is available
    // throw DXAPIError(response["error"]["type"],
    // 		     response["error"]["message"],
    // 		     req.responseCode);
  }

  return JSON(req.respData);

  // TODO: Check for response code and throw something if necessary
}

void dxpy::setAPIServerInfo(const string &host,
			    int port,
			    const string &protocol) {
  g_APISERVER_HOST = host;
  // TODO: FIXME
  //  g_APISERVER_PORT = string(itoa(port));
  g_APISERVER = protocol + "://" + host + g_APISERVER_PORT;
}

void dxpy::setSecurityContext(JSON security_context) {
  // TODO: Write this
}

void dxpy::loadFromEnvironment() {
  if ((getenv("APISERVER_HOST") != NULL) and
      (getenv("APISERVER_PORT") != NULL))
    setAPIServerInfo(getenv("APISERVER_HOST"),
		     atoi(getenv("APISERVER_PORT")));

  if (getenv("SECURITY_CONTEXT") != NULL)
    g_SECURITY_CONTEXT = JSON(getenv("SECURITY_CONTEXT"));
}
