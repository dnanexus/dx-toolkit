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

#ifndef RESOLVER_RESOLVER_H
#define RESOLVER_RESOLVER_H

#include <map>
#include <string>
#include <vector>

using namespace std;

namespace dx {
  // Resolver
  //
  // Resolver is an unofficial, thin layer that facilitates symbolic name resolution.
  // Resolver can be used to resolve object paths, and other operations describe below.
  //
  // An object path is a string of the form:
  //
  // [ <project_id_or_name> ":" ] [ <folder_prefix> ] object_id_or_name
  //
  // Examples of object paths:
  // ------------------------
  //
  //   hg18
  //   /hg18 (same as above)
  //   genomes/hg18
  //   /genomes/hg18 (same as above)
  //   seed:hg18
  //   seed:/hg18 (same as above)
  //   seed:genomes/hg18
  //   seed:/genomes/hg18 (same as above)
  //   project-000000000000000000000001:hg18
  //   record-9z4Qpfpyjv9FbvB5qVb00012
  //   project-9yzy0b2yjv9P7Zyv2v2Q00FK:record-9z4Qpfpyjv9FbvB5qVb00012
  //
  // How to use resolver:
  // -------------------
  //
  // To initialize a resolver with a default project:
  //
  //   Resolver resolver(default_project);
  //   Resolver resolver("seed");
  //
  // Object lookup helpers:
  // ----------------------
  //
  // To look up a path (object must exist):
  //
  //   ObjectInfo oi = resolver.FindPath(object_path)
  //
  //   ObjectInfo contains the following string fields:
  //
  //     project.id
  //     project.name
  //     object.id
  //     object.name
  //     object.folder
  //
  //   Lookup works in the following way:
  //
  //   Using object names:
  //
  //   object_name -> alias for default_project:/object_name
  //   project_name:object_name -> alias for project_name:/object_name
  //   project_id:object_name -> alias for project_id:/object_name
  //   project_name:/some/folder/object_name -> resolves project_name to project.id using findProjects
  //                                            fills in rest of details using findDataObject with:
  //                                              name: object_name
  //                                              scope.project: project.id
  //                                              scope:folder: /some/folder
  //                                              scope:recurse: true
  //   project_id:/some/folder/object_name -> fills in project.name with project-xxxx/describe
  //                                          fills in rest of details using findDataObject as above
  //
  //   Using object ids: (folders in the path are ignored, but real folder is returned in ObjectInfo)
  //
  //   object_id -> alias for default_project:object_id
  //   project_name:object_id -> resolves project_name to project.id using findProjects
  //                             calls describe with particular object_id and project.id
  //                             overwrites project.id from describe's response
  //                             calls project-xxxx/describe to fill in project.name (may be different than input)
  //   project_id:object_id -> calls describe with particular object_id and project_id
  //                           overwrites project.id from describe's response
  //                           calls project-xxxx/describe to fill in project.name (may be different than input)
  //
  // To use the resolver to convert from project name to project id:
  //
  //   // Returns "" if project not found
  //   string project_id = resolver.FindProject("seed");
  //
  // To use the resolver to convert from project name to project id,
  // or create a new project if it doesn't already exist:
  //
  //   // Returns an existing or new project_id
  //   string project_id = resolver.EnsureProject("seed");
  //
  // Object creation helpers:
  // ------------------------
  //
  // (The following functions work only with object names, not object ids)
  //
  // To use the resolver to "prepare for the creation of a path", that is, to ensure
  // that a particular project exists but also that a particular object does NOT
  // exist:
  //
  //   ObjectInfo oi = resolver.PreparePath(object_path)
  //   // Creates project "seed" if it doesn't exist, and removes object "hg18"
  //   // if it exists in that project.
  //   
  //   ObjectInfo in that case does not contain object.id, and the object.name and
  //   object.folder are taken from parsing the path
  //
  // Why would you want this? Symbolic lookup is only possible if you have a single
  // copy of an object with a particular name, otherwise the lookup is ambiguous.
  // Therefore, often if you want to create FOO, you most likely want to remove any
  // existing FOOs from the system prior to making FOO (it is equivalent to what
  // "command > FOO" would do on UNIX; it always 'overwrites' FOO, and the way to
  // emulate this behavior here is by first calling PreparePath before you make
  // a new FOO).
  //
  // To parse a path (no API calls are made):
  //
  //   ObjectInfo oi = resolver.ParsePath(object_path);
  //   
  //   Fills in only one of project.id/project.name, plus object.name, object.folder
  //
  // To parse a path and resolve a project:
  //
  //   ObjectInfo oi = resolver.DestinationPath(object_path);
  //
  //   Fills in project.id (and project.name, if given), object.name, object.folder

  class ObjectInfo
  {
    public:

    class Project
    {
      public:

      string id;
      string name;
    };

    class Object
    {
      public:

      string id;
      string name;
      string folder;
    };

    Project project;
    Object object;
    string path;

    ObjectInfo() { }
    ObjectInfo(const string &path_, const string &default_project = "");

    static bool IsObjectId(const string &s);
    static bool IsProjectId(const string &s);
    static bool IsId(const string &s);
  };

  class Resolver
  {
    public:

    Resolver() { }
    Resolver(const string &default_project_) : default_project(default_project_) { }

    ObjectInfo FindPath(const string &path) const;
    string FindProject(const string &project) const;
    string EnsureProject(const string &project) const;

    ObjectInfo ParsePath(const string &path) const;
    ObjectInfo DestinationPath(const string &path) const;
    ObjectInfo PreparePath(const string &path) const;

    private:

    void DescribeObject(const string &project_id, const string &object_id, ObjectInfo &oi) const;
    void LookupPath(const string &project_id, const string &name, const string &folder, ObjectInfo &oi) const;
    void DeleteObject(const string &project_id, const string &object_id) const;
    string CreateProject(const string &name) const;
    string GetProjectName(const string &project_id) const;

    string default_project;
  };
}
#endif
