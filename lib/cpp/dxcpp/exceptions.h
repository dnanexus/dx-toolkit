#ifndef DXCPP_EXCEPTIONS_H
#define DXCPP_EXCEPTIONS_H

#include <exception>
#include <string>

using namespace std;

class DXError: public exception {
};

class DXAPIError: public DXError {
  DXAPIError(string name, string msg, int code);
};

class DXFileError: public DXError {
};

class DXTableError: public DXError {
};

#endif
