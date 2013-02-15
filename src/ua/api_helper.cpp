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

#include "api_helper.h"

#include <curl/curl.h>

#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"

#include "log.h"

using namespace std;
using namespace dx;

JSON securityContext(const string &authToken) {
  JSON ctx(JSON_OBJECT);
  ctx["auth_token_type"] = "Bearer";
  ctx["auth_token"] = authToken;
  return ctx;
}

void testServerConnection() {
  LOG << "Testing connection to API server...";
  try {
    JSON result = systemFindUsers(JSON::parse("{\"limit\": 1}"), false); // don't retry this request
    LOG << " success." << endl;
  } catch (DXAPIError &aerr) {
    LOG << " failure." << endl;
    if (aerr.resp_code == 401) {
      throw runtime_error("Invalid Authentication token, please provide a correct auth token (you may use --auth-token option). (" + string(aerr.what()) + ")");
    }
    throw runtime_error("Unable to connect to apiserver -- an unexpected error occurred. (" + string(aerr.what()) + ")");
  }
  catch (exception &e) {
    LOG << " failure." << endl;
    throw runtime_error("Unable to connect to DNAnexus apiserver. Please list your environment variables (--env flag) to see the current apiserver configuration.\n\n"
                        "Detailed message (for advanced users only):\n" + string(e.what()));
  }
}

string urlEscape(const string &str) {
  const char * cStr = str.c_str();
  char * cStrEsc = curl_easy_escape(NULL, cStr, str.length());
  string strEsc(cStrEsc);
  curl_free(cStrEsc);
  return strEsc;
}

/*
 * Given a project specifier (name or ID), resolves it to a project ID.
 * Important: Only projects with >=CONTRIBUTE access are considered 
 *            for resolution. Thus, this function is guranteed to do
 *            exactly one of the following:
 *            1) Throw an error if no such project exist.
 *            2) Throw an error, if multiple projects match the criteria.
 *            3) Return a project ID with >=CONTRIBUTE access.
 *
 * We use following procedure to revolve project specifier to a project ID:
 *
 * Start with an empty project list (matchingProjectIdToName), then
 * 1) If project specifier represent ID of a project with >=CONTRIBUTE
 *    access, add it to project list.
 * 2) Add all project whose name matches project specifier, and
 *    >= CONTRIBUTE access is available, to project list.
 * Now,
 * - If project list's size > 2, then project specifier does not uniquely
 *   identify the project. (error is thrown)
 * - If project list's size == 0, then project specifier does not represent
 *   a project's ID or name (with >=CONTRIBUTE access). (error is thrown)
 * - If project list's size == 1, then we return the project ID.
 */
string resolveProject(const string &projectSpec) {
  LOG << "Resolving project specifier " << projectSpec << "...";
  string projectID;
  map<string, string> matchingProjectIdToName;

  try {
    JSON desc = projectDescribe(urlEscape(projectSpec));
    string level = desc["level"].get<string>();
    if ((level == "CONTRIBUTE") || (level == "ADMINISTER")) {
      matchingProjectIdToName[projectSpec] = desc["name"].get<string>();
    }
  } catch (DXAPIError &e) {
    // Ignore the error (we will check for matching project name)
  }
  
  try {
    JSON params(JSON_OBJECT);
    params["name"] = projectSpec;
    params["level"] = "CONTRIBUTE";
    
    JSON findResult = systemFindProjects(params);
    JSON projects = findResult["results"];
    for (unsigned i = 0; i < projects.size(); ++i) {
      matchingProjectIdToName[projects[i]["id"].get<string>()] = projectSpec;
    }
  } catch (DXAPIError &e) {
    LOG << "Call to findProjects failed." << endl;
    throw;  
  }

  if (matchingProjectIdToName.size() == 0) {
    LOG << " failure." << endl;
    throw runtime_error("\"" + projectSpec + "\" does not represent a valid project name or ID (with >=CONTRIBUTE access)");
  }

  if (matchingProjectIdToName.size() > 1) {
    LOG << "failure. " << matchingProjectIdToName.size() << " projects (with >=CONTRIBUTE access) match the identifer: \"" + projectSpec + "\":" << endl;
    int i =  1;
    for (map<string, string>::const_iterator it = matchingProjectIdToName.begin(); it != matchingProjectIdToName.end(); ++it, ++i) {
      LOG << "\t" << i << ". \"" << it->second << "\" (ID = \"" << it->first << "\")" << endl;
    }
    throw runtime_error("\"" + projectSpec + "\" does not uniquely identify a project (multiple matches found)");
  }
  
  LOG << " found project: \"" << matchingProjectIdToName.begin()->second << "\" (ID = \"" << matchingProjectIdToName.begin()->first << "\") corrosponding to project identifer \"" << projectSpec << "\"" << endl;
  return matchingProjectIdToName.begin()->first;
}

/*
 * Ensure that we have at least CONTRIBUTE access to the project.
 */
void testProjectPermissions(const string &projectID) {
  LOG << "Testing permissions on project " << projectID << "...";
  try {
    JSON desc = projectDescribe(projectID);
    string level = desc["level"].get<string>();

    if ((level == "CONTRIBUTE") || (level == "ADMINISTER")) {
      LOG << " success." << endl;
      return;
    } else {
      LOG << " failure." << endl;
      throw runtime_error("Permission level " + level + " is not sufficient to create files in project " + projectID);
    }
  } catch (DXAPIError &e) {
    LOG << " call to projectDescribe failed." << endl;
    throw;
  }
}

/*
 * Create the folder in which the file object(s) will be created, including
 * any parent folders.
 */
void createFolder(const string &projectID, const string &folder) {
  LOG << "Creating folder " << folder << " and parents in project " << projectID << "...";
  try {
    JSON params(JSON_OBJECT);
    params["folder"] = folder;
    params["parents"] = true;
    projectNewFolder(projectID, params);
    LOG << " success." << endl;
  } catch (DXAPIError &e) {
    LOG << " failure." << endl;
    throw runtime_error("Could not create folder with path '" + folder + "' in project '" + projectID + "' (" + e.what() + ")");
  }
}

/*
 * Create the file object. The object is created in the given project and
 * folder, and with the specified name. The folder and any parent folders
 * are created if they do not exist.
 */
string createFileObject(const string &project, const string &folder, const string &name, const string &mimeType, const JSON &properties) {
  JSON params(JSON_OBJECT);
  params["project"] = project;
  params["folder"] = folder;
  params["name"] = name;
  params["parents"] = true;
  params["media"] = mimeType;
  params["properties"] = properties;

  LOG << "Creating new file with parameters " << params.toString() << endl;

  JSON result = fileNew(params);
  LOG << "Got result " << result.toString() << endl;

  return result["id"].get<string>();
}

/* 
 * Returns output["results"] array from /findDataObjects call, to search for
 * all files in "project" with the given file signature. Describe output
 * is also returned.
 * Note: Hidden files are searched as well.
 */
JSON findResumableFileObject(string project, string signature) {
  JSON query(JSON_OBJECT);
  query["class"] = "file";
  query["properties"] = JSON(JSON_OBJECT);
  query["properties"][FILE_SIGNATURE_PROPERTY] = signature;
  query["scope"] = JSON(JSON_OBJECT);
  query["scope"]["project"] = project;
  query["scope"]["folder"] = "/";
  query["scope"]["recurse"] = true;
  query["visibility"] = "either";
  query["describe"] = JSON::parse("{\"project\": \"" + project + "\"}");

  JSON output;
  try {
    output = systemFindDataObjects(query);
  } catch (exception &e) {
    LOG << " failure while running findDataObjects with this input query: " << query.toString() << endl;
    throw;
  }
  return output["results"];
}

void closeFileObject(const string &fileID) {
  fileClose(fileID);
}

void removeFromProject(const string &projID, const string &objID) {
  projectRemoveObjects(projID, JSON::parse("{\"objects\": [\"" + objID + "\"]}"));
}

string getFileState(const string &fileID) {
  JSON result = fileDescribe(fileID);
  return result["state"].get<string>();
}
