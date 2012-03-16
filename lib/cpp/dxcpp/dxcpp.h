#ifndef DXCPP_H
#define DXCPP_H

#include <stdlib.h>
#include <map>
#include <string>
#include "json.h"
#include "exceptions.h"

using namespace std;

// TODO: Put HTTP stuff here?

// (resource, data, method='POST', headers={}, auth=None,
//   jsonify_data=True, want_full_response=False, **kwargs):
// Want to consider returning a string especially after a getRows call
//

JSON DXHTTPRequest(const string &resource, const string &data,
		   const map<string, string> &headers=map<string, string>());

void setAPIServerInfo(const string &host="localhost",
		      int port=8124,
		      const string &protocol="http");

void setSecurityContext(const JSON &security_context);

void loadFromEnvironment();

#include "api.h"
#include "bindings.h"

#endif
