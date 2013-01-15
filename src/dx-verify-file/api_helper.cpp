#include "api_helper.h"
#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"
#include "log.h"

using namespace std;

dx::JSON securityContext(const string &authToken) {
  dx::JSON ctx(dx::JSON_OBJECT);
  ctx["auth_token_type"] = "Bearer";
  ctx["auth_token"] = authToken;
  return ctx;
}

void apiInit(const string &apiserverHost, const int apiserverPort, const string &apiserverProtocol, const string &authToken) {
  setAPIServerInfo(apiserverHost, apiserverPort, apiserverProtocol);
  setSecurityContext(securityContext(authToken));
}
