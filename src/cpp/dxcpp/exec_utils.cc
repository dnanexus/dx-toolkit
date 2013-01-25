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

#include <fstream>

#include "exec_utils.h"
#include "utils.h"

using namespace std;
using namespace dx;

void dxLoadInput(JSON &input) {
  ifstream ifs(joinPath(getUserHomeDirectory(), "job_input.json"));
  input.read(ifs);
}

void dxWriteOutput(const JSON &output) {
  ofstream ofs(joinPath(getUserHomeDirectory(), "job_output.json"));
  ofs << output.toString() << endl;
  ofs.close();
}

void dxReportError(const string &message, const bool internal) {
  ofstream ofs(joinPath(getUserHomeDirectory(), "job_error.json"));
  JSON error_json = JSON(JSON_HASH);
  error_json["error"] = JSON(JSON_HASH);
  error_json["error"]["type"] = internal ? "AppInternalError" : "AppError";
  error_json["error"]["message"] = message;
  ofs << error_json.toString() << endl;
  ofs.close();
  exit(1);
}
