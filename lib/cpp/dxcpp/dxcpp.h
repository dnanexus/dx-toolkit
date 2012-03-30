#ifndef DXCPP_H
#define DXCPP_H

#include <stdlib.h>
#include <map>
#include <string>
#include "dxjson/dxjson.h"
#include "exceptions.h"

// TODO: Put HTTP stuff here?

// (resource, data, method='POST', headers={}, auth=None,
//   jsonify_data=True, want_full_response=False, **kwargs):
// Want to consider returning a string especially after a getRows call
//

dx::JSON DXHTTPRequest(const std::string &resource, const std::string &data,
		   const std::map<std::string, std::string> &headers=std::map<std::string, std::string>());

void setAPIServerInfo(const std::string &host="localhost",
		      int port=8124,
		      const std::string &protocol="http");

void setSecurityContext(const dx::JSON &security_context);

void loadFromEnvironment();

#include "api.h"
#include "bindings.h"

#endif
