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

#include "dxrecord.h"

using namespace std;

namespace dx {
    
  void DXRecord::create(const JSON &data_obj_fields) {
    JSON input_params = data_obj_fields;
    if (!data_obj_fields.has("project"))
      input_params["project"] = config::CURRENT_PROJECT();
    const JSON resp = recordNew(input_params);
    setIDs(resp["id"].get<string>(), input_params["project"].get<string>());
  }

  void DXRecord::create(const DXRecord &init_from,
                        const JSON &data_obj_fields) {
    JSON input_params = data_obj_fields;
    input_params["initializeFrom"] = JSON(JSON_OBJECT);
    input_params["initializeFrom"]["id"] = init_from.getID();
    input_params["initializeFrom"]["project"] = init_from.getProjectID();

    if (!data_obj_fields.has("project"))
      input_params["project"] = config::CURRENT_PROJECT();
    const JSON resp = recordNew(input_params);
    setIDs(resp["id"].get<string>(), input_params["project"].get<string>());
  }

  DXRecord DXRecord::newDXRecord(const JSON &data_obj_fields) {
    DXRecord dxrecord;
    dxrecord.create(data_obj_fields);
    return dxrecord;
  }

  DXRecord DXRecord::newDXRecord(const DXRecord &init_from,
                                 const JSON &data_obj_fields) {
    DXRecord dxrecord;
    dxrecord.create(init_from, data_obj_fields);
    return dxrecord;
  }

  DXRecord DXRecord::clone(const string &dest_proj_id,
                           const string &dest_folder) const {
    clone_(dest_proj_id, dest_folder);
    return DXRecord(dxid_, dest_proj_id);
  }
}
