// Copyright (C) 2013 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may
//   not use this file except in compliance with the License. You may obtain a
//   copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//   License for the specific language governing permissions and limitations
//   under the License.

/** \file
 *
 * \brief Exception classes.
 */

#ifndef DXCPP_EXCEPTIONS_H
#define DXCPP_EXCEPTIONS_H

#include <exception>
#include <string>
#include <boost/lexical_cast.hpp>

namespace dx {
  //! Generic exception for the DNAnexus C++ library.
  class DXError: public std::exception {
   public:
    std::string msg;

    DXError(): msg("Unknown error occured while using DNAnexus C++ library.") { }
    DXError(const std::string &msg): msg(msg) { }
    virtual const char* what() const throw() {
      return (const char*)msg.c_str();
    }

    virtual ~DXError() throw() { }

  };

  //! Represents errors returned by the API server.

  ///
  /// This exception is thrown when a request made to the API server results in
  /// an HTTP response code other than 200.
  ///
  class DXAPIError: public DXError {
   public:
    std::string name;
    int resp_code; 
    mutable std::string error_msg;

    DXAPIError(std::string name, std::string msg, int code) :
    DXError(msg), name(name), resp_code(code) {}

    virtual const char* what() const throw() {
      error_msg = name + std::string(": ") + msg + std::string(", HTTP code = ") + boost::lexical_cast<std::string>(resp_code);
      return (const char*)error_msg.c_str();
    }

    virtual ~DXAPIError() throw() { }
  };

  //! Represents errors relating to the DXFile class.
  class DXFileError: public DXError {
   public:
    DXFileError(): DXError("Unknown error occured while using DXFile class.") { }
    DXFileError(const std::string &msg): DXError(msg) { }

    virtual ~DXFileError() throw() { }
  };

  //! Represents errors relating to the DXGTable class.
  class DXGTableError: public DXError {
   public:
    DXGTableError(): DXError("Unknown error occured while using DXGTable class.") { }
    DXGTableError(const std::string &msg): DXError(msg) { }

    virtual ~DXGTableError() throw() { }
  };

  //! Thrown by methods that are not yet implemented.

  ///
  /// For development purposes only.
  ///
  class DXNotImplementedError: public DXError {
   public:
    DXNotImplementedError(): DXError("Not yet implemented.") { }
    DXNotImplementedError(const std::string &msg): DXError(msg) { }

    virtual ~DXNotImplementedError() throw() { }
  };
}

#endif
