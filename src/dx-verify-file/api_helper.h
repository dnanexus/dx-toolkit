#ifndef UA_API_HELPER_H
#define UA_API_HELPER_H

#include <string>
#include "dxjson/dxjson.h"

void apiInit(const std::string &apiserverHost, const int apiserverPort, const std::string &apiserverProtocol, const std::string &authToken);

#endif
