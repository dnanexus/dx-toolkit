#ifndef DXCPP_H
#define DXCPP_H

#include <stdlib.h>
#include <map>
#import "json.h"

using namespace std;

string g_APISERVER_HOST;
string g_APISERVER_PORT;
string g_APISERVER;
JSON g_SECURITY_CONTEXT;

// TODO: Put HTTP stuff here?

// (resource, data, method='POST', headers={}, auth=None,
//   jsonify_data=True, want_full_response=False, **kwargs):
// Want to consider returning a string especially after a getRows call
//

namespace dxpy {

  JSON DXHTTPRequest(const string &resource, const string &data);

  void setAPIServerInfo(const string &host=string("localhost"),
			int port=8124,
			const string &protocol=string("http"));

  void setSecurityContext(JSON security_context);

  void loadFromEnvironment();
}

#endif
