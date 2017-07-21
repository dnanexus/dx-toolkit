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

#include "dxproject.h"

using namespace std;

namespace dx {
  JSON DXContainer::describe(bool folders) const {
    if (folders) {
      return projectDescribe(getID(), string("{\"folders\": true}"));
    }
    //else
    return projectDescribe(getID(), string("{}"));
  }

  // Generic
  void DXContainer::move(const JSON &objects,
                         const JSON &folders,
                         const string &dest_folder) const {
    JSON input_params(JSON_OBJECT);
    input_params["objects"] = objects;
    input_params["folders"] = folders;
    input_params["destination"] = dest_folder;
    projectMove(getID(), input_params.toString());
  }

  void DXContainer::clone(const JSON &objects,
                          const JSON &folders,
                          const string &dest_container,
                          const string &dest_folder) const {
    JSON input_params(JSON_OBJECT);
    input_params["objects"] = objects;
    input_params["folders"] = folders;
    input_params["project"] = dest_container;
    input_params["destination"] = dest_folder;
    projectClone(getID(), input_params.toString());
  }

  // Folder-specific
  void DXContainer::newFolder(const string &folder, bool parents) const {
    stringstream input_hash;
    input_hash << "{\"folder\": \"" + folder + "\",";
    input_hash << "\"parents\": " << (parents ? "true" : "false") << "}";
    projectNewFolder(getID(), input_hash.str());
  }

  JSON DXContainer::listFolder(const string &folder) const {
    return projectListFolder(getID(), "{\"folder\": \"" + folder + "\"}");
  }

  void DXContainer::moveFolder(const string &folder,
                               const string &dest_folder) const {
    projectMove(getID(), "{\"folders\": [\"" + folder + "\"]," + "\"destination\": \"" + dest_folder + "\"}");
  }

  void DXContainer::removeFolder(const string &folder, const bool recurse) const {
    string input = recurse ?
      "{\"folder\": \"" + folder + "\", \"partial\": true, \"recurse\": true}" :
      "{\"folder\": \"" + folder + "\", \"partial\": true}";
    bool completed = false;
    while(!completed) {
      JSON response = projectRemoveFolder(getID(), input);
      if (!response.has("completed")) {
        throw DXError("Error removing folder");
      }
      completed = response["completed"];
    }
  }

  // Objects-specific
  void DXContainer::removeObjects(const JSON &objects) const {
    projectRemoveObjects(getID(), "{\"objects\":" + objects.toString() + "}");
  }

  // Project methods

  void DXProject::update(const JSON &to_update) const {
    projectUpdate(getID(), to_update);
  }

  void DXProject::destroy() const {
    projectDestroy(getID());
  }

  void DXProject::invite(const string &invitee, const string &level) const {
    projectInvite(getID(), "{\"invitee\":\"" + invitee + "\",\"level\":\"" + level + "\"}");
  }

  void DXProject::decreasePerms(const string &member, const string &level) const {
    projectDecreasePermissions(getID(), "{\"" + member + "\":\"" + level + "\"}");
  }
}
