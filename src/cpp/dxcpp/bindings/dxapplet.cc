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

#include "dxapplet.h"
#include "execution_common_helper.h"

using namespace std;

namespace dx {
  void DXApplet::create(JSON inp) {
    if (!inp.has("project")) 
      inp["project"] = config::CURRENT_PROJECT();
    setIDs(appletNew(inp)["id"].get<string>(), inp["project"].get<string>());
  }

  DXJob DXApplet::run(const JSON &applet_input,
                       const string &output_folder,
                       const vector<string> &depends_on,
                       const dx::JSON &instance_type,
                       const string &project_context
                       ) const {
    JSON input_params(JSON_OBJECT);
    input_params["input"] = applet_input;
    if (config::JOB_ID().empty())
      input_params["project"] = project_context;
    input_params["folder"] = output_folder;
    appendDependsOnAndInstanceType(input_params, depends_on, "main", instance_type);   
    const JSON resp = appletRun(dxid_, input_params);
    return DXJob(resp["id"].get<string>());
  }

  DXApplet DXApplet::clone(const string &dest_proj_id,
                           const string &dest_folder) const {
    clone_(dest_proj_id, dest_folder);
    return DXApplet(dxid_, dest_proj_id);
  }
}
