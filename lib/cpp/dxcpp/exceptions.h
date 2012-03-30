#ifndef DXCPP_EXCEPTIONS_H
#define DXCPP_EXCEPTIONS_H

#include <exception>
#include <string>

class DXError: public std::exception {
};

class DXAPIError: public DXError {
 public:
  DXAPIError(std::string name, std::string msg, int code) {}
};

class DXFileError: public DXError {
};

class DXTableError: public DXError {
};

class DXNotImplementedError: public DXError {
};

#endif
