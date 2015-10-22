// Copyright (C) 2013-2015 DNAnexus, Inc.
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
 * \brief Useful utilities for interfacing with app(let)'s execution environment 
 */

#ifndef DXCPP_EXEC_UTILS_H
#define DXCPP_EXEC_UTILS_H

#include <string>

#include "dxjson/dxjson.h"

namespace dx {
  /**
   * This function reads the local file job_input.json and loads the
   * JSON contents into the given JSON variable.
   *
   * @param input JSON variable to be set
   */
  void dxLoadInput(dx::JSON &input);

  /**
   * This function serializes the given JSON variable and saves it to
   * the local file job_output.json.
   *
   * @param output JSON variable to be serialized
   */
  void dxWriteOutput(const dx::JSON &output);

  /**
   * This function records the given error message into the local file
   * job_error.json and exits with a nonzero exit code.
   *
   * @param message Error message which will be shown to the user
   * @param internal Whether the error should be reported as an
   * AppInternalError instead of AppError
   */
  void dxReportError(const std::string &message, const bool internal=false);
}
#endif
