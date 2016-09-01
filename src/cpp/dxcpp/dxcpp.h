// Copyright (C) 2013-2016 DNAnexus, Inc.
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

#ifndef DXCPP_H
#define DXCPP_H

#include <stdlib.h>
#include <stdint.h>
#include <map>
#include <string>
#include "dxjson/dxjson.h"

// A macro to allow unused variable (without throwing warning)
// Does work for GCC, will need to be expanded for other compilers.
#ifdef UNUSED
#elif defined(__GNUC__) 
# define UNUSED(x) UNUSED_ ## x __attribute__((unused)) 
#elif defined(__LCLINT__) 
# define UNUSED(x) /*@unused@*/ x 
#else 
# define UNUSED(x) x 
#endif

namespace dx {
  namespace config {
    std::string& APISERVER_HOST();
    std::string& APISERVER_PORT();
    std::string& APISERVER_PROTOCOL();
    JSON& SECURITY_CONTEXT();
    std::string& JOB_ID();
    std::string& WORKSPACE_ID();
    std::string& PROJECT_CONTEXT_ID();
    
    /**
     * Returns a concatenation of APISERVER_PROTOCOL, APISERVER_HOST & APISERVER_PORT
     * (in correct order), representing full apiserver path.
     * @note Returns an empty string, if any of the above variables have not been set
     */
    std::string APISERVER();
    
    /** Returns a mutable reference to current project
     * (which is equal to WORKSPACE_ID() if running within a job,
     * and PROJECT_CONTEXT_ID() otherwise)
     */
    std::string& CURRENT_PROJECT();
    const std::string& API_VERSION();
  }

  /**
   * This is a low-level function for making an HTTP request to the API
   * server using the information that has been set by
   * setAPIServerInfo() and setSecurityContext().
   *
   * @param resource API server route to access, e.g. "/file/new"
   * @param data Data to send in the request
   * @param safeToRetry If true, indicates that the request is idempotent and that a failed request may be retried. Defaults to false.
   * @param headers Additional HTTP headers to include in the request
   * @return The response from the API server, parsed as a JSON
   */
  dx::JSON DXHTTPRequest(const std::string &resource, const std::string &data, const bool safeToRetry = false,
                         const std::map<std::string, std::string> &headers = std::map<std::string, std::string>());

  /**
   * Loads the data from environment variables and calls setAPIServerInfo(),
   * setSecurityContext(), setWorkspaceID(), and setProjectContext() as
   * appropriate.
   */
  bool loadFromEnvironment();

  namespace Nonce {
    std::string nonce();
    dx::JSON updateNonce(const dx::JSON &input_params);
  }
}

#include "exceptions.h"
#include "exec_utils.h"
#include "api.h"
#include "bindings.h"

#endif
