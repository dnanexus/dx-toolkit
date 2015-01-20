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

#include "Resolver.h"
#include "dxcpp/dxcpp.h"

namespace dx {
  vector<string> Split(const string &s, char delim)
  {
    vector<string> results;
    string token = "";
    for (unsigned i = 0; i != s.size(); ++i)
    {
      if (s[i] == delim)
      {
        results.push_back(token);
        token = "";
      }
      else
      {
        token += s[i];
      }
    }
    results.push_back(token);

    return results;
  }

  ObjectInfo::ObjectInfo(const string &path_, const string &default_project)
  {
    path = path_;
    vector<string> chunks = Split(path, ':');
    if (chunks.size() == 1)
      path = chunks[0];
    else if (chunks.size() == 2)
    {
      project.name = chunks[0];
      path = chunks[1];
    }
    else
      throw "Too many ':' in path '" + path + "'";

    object.folder = "/";
    chunks = Split(path, '/');
    for (unsigned i = 0; i != chunks.size(); ++i)
    {
      if (object.name != "")
        object.folder += (object.folder[object.folder.size()-1] != '/' ? "/" : "") + object.name;
      object.name = chunks[i];
    }

    if (object.name == "")
      throw "Empty name in path '" + path_ + "'";

    if (project.name == "")
      project.name = default_project;

    if (ObjectInfo::IsProjectId(project.name))
    {
      project.id = project.name;
      project.name = "";
    }

    if (ObjectInfo::IsObjectId(object.name))
    {
      object.id = object.name;
      object.name = "";
    }

    path = path_;
  }

  bool ObjectInfo::IsId(const string &s)
  {
    const char alphabet[] = "0123456789BFGJKPQVXYZbfgjkpqvxyz";

    if (s.size() != 24)
      return false;

    for (unsigned i = 0; i != s.size(); ++i)
    {
      bool found = false;
      for (unsigned j = 0; j != sizeof(alphabet); ++j)
      {
        if (s[i] == alphabet[j])
        {
          found = true;
          break;
        }
      }
      if (!found)
        return false;
    }
    
    return true;
  }

  bool ObjectInfo::IsObjectId(const string &s)
  {
    vector<string> pieces  = Split(s, '-');
    return ((pieces.size() == 2) &&
            (pieces[0] == "record" || pieces[0] == "gtable" || pieces[0] == "file" || pieces[0] == "applet") &&
            (IsId(pieces[1])));
  }

  bool ObjectInfo::IsProjectId(const string &s)
  {
    vector<string> pieces  = Split(s, '-');
    return ((pieces.size() == 2) &&
            (pieces[0] == "project") &&
            (IsId(pieces[1])));
  }

  ObjectInfo Resolver::ParsePath(const string &path) const
  {
    return ObjectInfo(path, default_project);
  }

  ObjectInfo Resolver::DestinationPath(const string &path) const
  {
    ObjectInfo oi = ObjectInfo(path, default_project);
    if (oi.project.id == "")
      oi.project.id = FindProject(oi.project.name);

    return oi;
  }

  string Resolver::FindProject(const string &project) const
  {
    if (ObjectInfo::IsProjectId(project))
      return project;

    dx::JSON input = dx::JSON(dx::JSON_HASH);
    input["describe"] = true;
    input["level"] = "VIEW";
    input["name"] = project;
    dx::JSON results = DXHTTPRequest("/system/findProjects", input.toString())["results"];
    if (results.size() > 0)
      return results[0]["id"].get<string>();

    return "";
  }

  string Resolver::EnsureProject(const string &project) const
  {
    string project_id = FindProject(project);
    if (project_id == "")
      project_id = CreateProject(project);

    return project_id;
  }

  ObjectInfo Resolver::FindPath(const string &path) const
  {
    ObjectInfo oi = ParsePath(path);

    if (oi.object.id != "")
    {
      if (oi.project.id == "")
        oi.project.id = FindProject(oi.project.name);

      DescribeObject(oi.project.id, oi.object.id, oi);

      oi.project.name = GetProjectName(oi.project.id);
    }
    else
    {
      if (oi.project.id == "")
        oi.project.id = FindProject(oi.project.name);
      else
        oi.project.name = GetProjectName(oi.project.id);

      if (oi.project.id != "")
        LookupPath(oi.project.id, oi.object.name, oi.object.folder, oi);
    }
    return oi;
  }

  ObjectInfo Resolver::PreparePath(const string &path) const
  {
    ObjectInfo oi = ParsePath(path);
    if (oi.project.id == "")
      oi.project.id = EnsureProject(oi.project.name);

    oi = FindPath(path);

    if (oi.object.id != "")
      DeleteObject(oi.project.id, oi.object.id);
    
    oi.object.id = "";
    return oi;
  }

  void Resolver::LookupPath(const string &project_id, const string &name, const string &folder, ObjectInfo &oi) const
  {
    dx::JSON input = dx::JSON(dx::JSON_HASH);
    input["name"] = name;
    input["visibility"] = "either";
    input["describe"] = true;
    input["scope"] = dx::JSON(dx::JSON_HASH);
    input["scope"]["project"] = project_id;
    input["scope"]["folder"] = folder;
    input["scope"]["recurse"] = true;
    dx::JSON results = DXHTTPRequest("/system/findDataObjects", input.toString());
    if (results["results"].size() == 0)
    {
      oi.object.id = "";
      return;
    }
    if (results["results"].size() > 1)
      throw "Object collision";

    oi.object.id = results["results"][0]["id"].get<string>();
    oi.object.folder = results["results"][0]["describe"]["folder"].get<string>();
  }

  void Resolver::DeleteObject(const string &project_id, const string &object_id) const
  {
    DXHTTPRequest("/" + project_id + "/removeObjects", "{\"objects\": [\"" + object_id + "\"], \"includeOrphanedHiddenLinks\": false}");
  }

  string Resolver::CreateProject(const string &name) const
  {
    dx::JSON input = dx::JSON(dx::JSON_HASH);
    input["name"] = name;
    return DXHTTPRequest("/project/new", input.toString())["id"].get<string>();
  }

  string Resolver::GetProjectName(const string &project_id) const
  {
    return DXHTTPRequest("/" + project_id + "/describe", "{}")["name"].get<string>();
  }

  void Resolver::DescribeObject(const string &project_id, const string &object_id, ObjectInfo &oi) const
  {
    string input = "{}";
    if (project_id != "")
      input = "{\"project\": \"" + project_id + "\"}";
    dx::JSON desc = DXHTTPRequest("/" + object_id + "/describe", input);

    oi.project.id = desc["project"].get<string>();
    oi.object.name = desc["name"].get<string>();
    oi.object.folder = desc["folder"].get<string>();
  }
}
