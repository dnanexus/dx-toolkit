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

//#include <unistd.h>
#include "bindings.h"
#include "dxlog.h"

using namespace std;

namespace dx {
  void DXDataObject::setIDs(const std::string &dxid, const std::string &proj) {
    dxid_ = dxid;
    if (proj == "default")
      proj_ = config::CURRENT_PROJECT(); 
    else
      proj_ = proj;
  }

  void DXDataObject::setIDs(const char *dxid, const char *proj) {
    if (proj == NULL) {
      setIDs(string(dxid));
    } else {
      setIDs(string(dxid), string(proj));
    }
  }

  void DXDataObject::setIDs(const dx::JSON &dxlink) {
    const string err_str = "The variable 'dxlink' must be a valid JSON hash of one of these two forms: \n"
                            "1. {\"$dnanexus_link\": \"obj_id\"} \n"
                            "2. {\"$dnanexus_link\": {\"project\": \"proj-id\", \"id\": \"obj-id\"}";
    if (dxlink.type() != JSON_HASH || dxlink.size() != 1) {
      DXLOG(logERROR) << "Not a hash, or has more than one key: '" << dxlink.toString() << "'";
      throw DXError(err_str, "InvalidDXLink");
    }
    if (dxlink["$dnanexus_link"].type() == JSON_STRING) {
      dxid_ = dxlink["$dnanexus_link"].get<string>();
      proj_ = config::CURRENT_PROJECT();
    } else {
      if (dxlink["$dnanexus_link"].type() == JSON_HASH && dxlink["$dnanexus_link"].size() == 2) {
        if (dxlink["$dnanexus_link"]["project"].type() != JSON_STRING || dxlink["$dnanexus_link"]["id"].type() != JSON_STRING) {
          DXLOG(logERROR) << "At least one of the keys: 'project', or 'id' are not string" << endl;
          throw DXError(err_str, "InvalidDXLink");
        }
        dxid_ = dxlink["$dnanexus_link"]["id"].get<string>();
        proj_ = dxlink["$dnanexus_link"]["project"].get<string>();
      } else {
        DXLOG(logERROR) << "The given dnanexus_link is neither a string, nor a valid dxlink hash." << endl;
        throw DXError(err_str, "InvalidDXLink");
      }
    }
  }

  void DXDataObject::waitOnState(const string &state, const int timeout) const {
    int elapsed = 0;
    string cur_state;
    do {
      cur_state = describe()["state"].get<string>();
      if (cur_state == state)
        return;
      sleep(2);
      elapsed += 2;
    } while (elapsed <= timeout);
  }

  JSON DXDataObject::describe(bool incl_properties, bool incl_details) const {
    stringstream input_hash;
    input_hash << "{";
    if (proj_ != "")
      input_hash << "\"project\": \"" << proj_ << "\",";
    input_hash << "\"properties\": " << (incl_properties ? "true" : "false")<<",";
    input_hash << "\"details\": " << ((incl_details) ? "true" : "false")<< "}";
    return describe_(input_hash.str());
  }

  void DXDataObject::addTypes(const dx::JSON &types) const {
    stringstream input_hash;
    input_hash << "{\"types\":" << types.toString() << "}";
    addTypes_(input_hash.str());
  }

  void DXDataObject::removeTypes(const dx::JSON &types) const {
    stringstream input_hash;
    input_hash << "{\"types\":" << types.toString() << "}";
    removeTypes_(input_hash.str());
  }

  JSON DXDataObject::getDetails() const {
    return getDetails_("{}");
  }

  void DXDataObject::setDetails(const dx::JSON &details) const {
    setDetails_(details.toString());
  }

  void DXDataObject::hide() const {
    setVisibility_("{\"hidden\":true}");
  }

  void DXDataObject::unhide() const {
    setVisibility_("{\"hidden\":false}");
  }

  void DXDataObject::rename(const std::string &name) const {
    stringstream input_hash;
    input_hash << "{\"project\": \"" << proj_ << "\",";
    input_hash << "\"name\": \"" << name << "\"}";
    rename_(input_hash.str());
  }

  void DXDataObject::setProperties(const dx::JSON &properties) const {
    stringstream input_hash;
    input_hash << "{\"project\": \"" << proj_ << "\",";
    input_hash << "\"properties\": " << properties.toString() << "}";
    setProperties_(input_hash.str());
  }

  dx::JSON DXDataObject::getProperties() const {
    return describe(true)["properties"];
  }

  void DXDataObject::addTags(const dx::JSON &tags) const {
    stringstream input_hash;
    input_hash << "{\"project\": \"" << proj_ << "\",";
    input_hash << "\"tags\": " << tags.toString() << "}";
    addTags_(input_hash.str());
  }

  void DXDataObject::removeTags(const dx::JSON &tags) const {
    stringstream input_hash;
    input_hash << "{\"project\": \"" << proj_ << "\",";
    input_hash << "\"tags\": " << tags.toString() << "}";
    removeTags_(input_hash.str());
  }

  void DXDataObject::close() const {
    close_("{}");
  }

  JSON DXDataObject::listProjects() const {
    return listProjects_("{}");
  }

  void DXDataObject::clone_(const string &dest_proj_id,
                            const string &dest_folder) const {
    stringstream input_hash;
    input_hash << "{\"objects\": [\"" << dxid_ << "\"],";
    input_hash << "\"project\": \"" << dest_proj_id << "\",";
    input_hash << "\"destination\": \"" << dest_folder << "\"}";
    projectClone(proj_, input_hash.str());
  }

  void DXDataObject::move(const string &dest_folder) const {
    stringstream input_hash;
    input_hash << "{\"objects\":[\"" << dxid_ << "\"],";
    input_hash << "\"destination\":\"" << dest_folder << "\"}";
    projectMove(proj_, input_hash.str());
  }

  void DXDataObject::remove() {
    projectRemoveObjects(proj_, "{\"objects\":[\"" + dxid_ + "\"]}");
    setIDs(string(""));
  }

  JSON DXLink(const std::string &dxid, const std::string &proj) {
    JSON link(JSON_OBJECT);
    if (proj == "") {
      link["$dnanexus_link"] = dxid;
    } else {
      link["$dnanexus_link"] = JSON(JSON_OBJECT);
      link["$dnanexus_link"]["project"] = proj;
      link["$dnanexus_link"]["id"] = dxid;
    }
    return link;
  }
}
