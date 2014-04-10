// Copyright (C) 2013-2014 DNAnexus, Inc.
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

#include "execution_common_helper.h"
using namespace std;

namespace dx {
  // Appends required parameters to "input"
  // This function is for internal use only.
  void appendDependsOnAndInstanceType(JSON &input, const vector<string> &depends_on, const string fn_name, const JSON &instance_type) {
    if (input.type() == JSON_UNDEFINED)
      input = JSON(JSON_HASH);

    if (depends_on.size() > 0)
      input["dependsOn"] = depends_on;

    if (instance_type.type() != JSON_NULL) {
      input["systemRequirements"] = JSON(JSON_HASH);
      if (instance_type.type() == JSON_STRING) {
        input["systemRequirements"][fn_name] = JSON(JSON_HASH);
        input["systemRequirements"][fn_name]["instanceType"] = instance_type;
      } else {
        if (instance_type.type() == JSON_OBJECT && instance_type.size() > 0) {
          for (JSON::const_object_iterator it = instance_type.object_begin(); it != instance_type.object_end(); ++it) {
            if (it->second.type() != JSON_STRING)
              throw DXError("Invalid JSON as argument to parameter 'instance_type'. Expected Key '" + it->first + 
                            "' to contain a string value, but rather found JSON_TYPE = " + boost::lexical_cast<string>(it->second.type()), "InvalidInstanceType");
            input["systemRequirements"][it->first] = JSON(JSON_HASH);
            input["systemRequirements"][it->first]["instanceType"] = it->second;
          }
        } else {
          throw DXError("Invalid JSON as argument to parameter 'instance_type'."
                        "Must be either: a non-empty Hash (string -> string), or a single string.", "InvalidInstanceType");
        }
      }
    }
  }
}
