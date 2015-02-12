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

#include "api_helper.h"

#include <boost/thread.hpp>
#include <curl/curl.h>

#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"

#include "dxcpp/dxlog.h"

using namespace std;
using namespace dx;

JSON securityContext(const string &authToken) {
  JSON ctx(JSON_OBJECT);
  ctx["auth_token_type"] = "Bearer";
  ctx["auth_token"] = authToken;
  return ctx;
}

/*
 * Calls the /system/greet route to get update info. This also serves to
 * verify that we can connect to the API server, and that the
 * authentication token is valid.
 *
 * - If the API call fails with a known error, we report it and exit.
 * - If the API call fails with an unknown error, we proceed (after logging the failure if verbose mode is on)
 * - If UA is up to date, we log this fact if verbose mode is on
 * - If a required update is available, we throw runtime_error()
 * - If a recommended update is available, we print the info on stderr (irresepctive of verbose mode status)
 */
void checkForUpdates() {
  JSON inp(JSON_HASH);
  inp["client"] = "dnanexus-upload-agent";
  inp["version"] = UAVERSION;
  string platform;
#if WINDOWS_BUILD
  platform = "windows";
#elif LINUX_BUILD
#if OLD_KERNEL_SUPPORT
  platform = "linux-old-kernel";
#else
  platform = "linux";
#endif
#elif MAC_BUILD
  platform = "mac";
#endif
  if (!platform.empty()) {
    inp["platform"] = platform;
  }

  JSON res;
  DXLOG(logINFO) << "Checking for updates (calling /system/greet), inp = '" << inp.toString() << "' ...";
  try {
    res = systemGreet(inp, false); // don't retry this requests, not that essential
  } catch (DXAPIError &aerr) {
    DXLOG(logINFO) << " failure.";
    if (aerr.resp_code == 401) {
      throw runtime_error("Invalid authentication token. Please provide a correct token by running 'dx login' or using the '--auth-token' option). (" + string(aerr.what()) + ")");
    } else {
      throw runtime_error("Unable to connect to API server: an unexpected error occurred. (" + string(aerr.what()) + ")");
    }
  } catch (DXConnectionError &cerr) {
    DXLOG(logINFO) << " failure.";
    #if WINDOWS_BUILD
    if (cerr.curl_code = 35 && string(cerr.what()).find("schannel") != string::npos) {
      throw runtime_error("This is a known issue on Microsoft Windows. Please download this hotfix from Microsoft to fix this problem: http://support.microsoft.com/kb/975858/en-us"
                          "\nTechnical details (for advanced users): \n'" + string(cerr.what()) + "'\nIf you still encounter the problem after installing the hotfix, please contact DNAnexus support.");
    }
    #endif
    throw runtime_error("Unable to connect to API server. Run 'ua --env' to see the current configuration.\n\n"
                        "Detailed message (for advanced users only):\n" + string(cerr.what()));
  } catch (DXError &e) {
    DXLOG(logINFO) << " failure.";
    throw runtime_error("Unable to connect to API server. Run 'ua --env' to see the current configuration.\n\n"
                        "Detailed message (for advanced users only):\n" + string(e.what()));
  } catch (exception &aerr) {
    // If an error is thrown while calling /system/greet, we don't treat it as fatal
    // but instead just log it to stderr (if verbose mode was on).
    DXLOG(logINFO) << " failure (call failed), reason: '" << aerr.what() << "'";
    return;
  }

  if (res["update"]["available"].get<bool>() == false) {
    DXLOG(logINFO) << " Hurray! Your copy of Upload Agent is up to date.";
    return;
  }
  string ver = res["update"]["version"].get<string>();
  string url = res["update"]["url"].get<string>();
  if (res["update"]["level"].get<string>() == "required") {
    throw runtime_error(string("**********\nUpload Agent being used is too old to continue.") +
                        "\nPlease download latest version (v" + ver + ") from " + url + "\n**********");
  }
  // If we are here => A recommended update is available. Show user a message to that effect
  DXLOG(logINFO);
  cerr <<"*********** Update Available ***********" << endl
       << "A new version of Upload Agent (v" << ver << ") is available for your platform!" << endl
       << "It's highly recommended that you download the latest version from here " << url << endl
       << "****************************************" << endl;
  return;
}

string urlEscape(const string &str) {
  const char * cStr = str.c_str();
  char * cStrEsc = curl_easy_escape(NULL, cStr, str.length());
  string strEsc(cStrEsc);
  curl_free(cStrEsc);
  return strEsc;
}


boost::mutex resolveProjectMutex;

/*
 * Given a project specifier (name or ID), resolves it to a project ID.
 * Important: Only projects with >=UPLOAD access are considered
 *            for resolution. Thus, this function is guranteed to do
 *            exactly one of the following:
 *            1) Throw an error if no such project exist.
 *            2) Throw an error, if multiple projects match the criteria.
 *            3) Return a project ID with >=UPLOAD access.
 *
 * We use following procedure to revolve project specifier to a project ID:
 *
 * Start with an empty project list (matchingProjectIdToName), then
 * 1) If project specifier represent ID of a project with >=UPLOAD
 *    access, add it to project list.
 * 2) Add all project whose name matches project specifier, and
 *    >=UPLOAD access is available, to project list.
 * Now,
 * - If project list's size > 2, then project specifier does not uniquely
 *   identify the project. (error is thrown)
 * - If project list's size == 0, then project specifier does not represent
 *   a project's ID or name (with >=UPLOAD access). (error is thrown)
 * - If project list's size == 1, then we return the project ID.
 */
string resolveProject(const string &projectSpec) {
  // Maps project specs (names or IDs) to resolved IDs
  static std::map<string, string> cache;

  boost::mutex::scoped_lock resolveProjectLock(resolveProjectMutex);

  DXLOG(logINFO) << "Resolving project specifier " << projectSpec << "...";
  if (cache.count(projectSpec) > 0) {
    DXLOG(logINFO) << "The project specifier was resolved previously, will just return value from cache('" << cache[projectSpec] << "')";
    return cache[projectSpec];
  }
  string projectID;
  map<string, string> matchingProjectIdToName;

  try {
    JSON desc = projectDescribe(urlEscape(projectSpec));
    string level = desc["level"].get<string>();
    if ((level == "UPLOAD") || (level == "CONTRIBUTE") || (level == "ADMINISTER")) {
      matchingProjectIdToName[projectSpec] = desc["name"].get<string>();
    }
  } catch (DXAPIError &e) {
    // Ignore the error (we will check for matching project name)
  }

  try {
    JSON params(JSON_OBJECT);
    params["name"] = projectSpec;
    params["level"] = "UPLOAD";

    JSON findResult = systemFindProjects(params);
    JSON projects = findResult["results"];
    for (unsigned i = 0; i < projects.size(); ++i) {
      matchingProjectIdToName[projects[i]["id"].get<string>()] = projectSpec;
    }
  } catch (DXAPIError &e) {
    DXLOG(logINFO) << "Call to findProjects failed.";
    throw;
  }

  if (matchingProjectIdToName.size() == 0) {
    DXLOG(logINFO) << " failure.";
    throw runtime_error("\"" + projectSpec + "\" does not represent a valid project name or ID (with >=UPLOAD access). Please check the project name/ID given and whether you have >= UPLOAD permission to project specified.");
  }

  if (matchingProjectIdToName.size() > 1) {
    DXLOG(logINFO) << "failure. " << matchingProjectIdToName.size() << " projects (with >=UPLOAD access) match the identifier: \"" + projectSpec + "\":";
    int i =  1;
    for (map<string, string>::const_iterator it = matchingProjectIdToName.begin(); it != matchingProjectIdToName.end(); ++it, ++i) {
      DXLOG(logINFO) << "\t" << i << ". \"" << it->second << "\" (ID = \"" << it->first << "\")";
    }
    throw runtime_error("\"" + projectSpec + "\" does not uniquely identify a project (multiple matches found)");
  }

  DXLOG(logINFO) << " found project: \"" << matchingProjectIdToName.begin()->second << "\" (ID = \"" << matchingProjectIdToName.begin()->first << "\") corresponding to project identifier \"" << projectSpec << "\"";
  return (cache[projectSpec] = matchingProjectIdToName.begin()->first);
}

/*
 * Create the folder in which the file object(s) will be created, including
 * any parent folders.
 */
void createFolder(const string &projectID, const string &folder) {
  DXLOG(logINFO) << "Creating folder " << folder << " and parents in project " << projectID << "...";
  try {
    JSON params(JSON_OBJECT);
    params["folder"] = folder;
    params["parents"] = true;
    projectNewFolder(projectID, params);
    DXLOG(logINFO) << " success.";
  } catch (DXAPIError &e) {
    DXLOG(logINFO) << " failure.";
    throw runtime_error("Could not create folder with path '" + folder + "' in project '" + projectID + "' (" + e.what() + ")");
  }
}

/*
 * For each i, creates folder folders[i] in project projects[i]. Uniquifies
 * the project/folder pairs before calling createFolder. This avoids
 * redundant API calls when multiple files are being uploaded to the same
 * folder in the same project.
 */
void createFolders(const vector<string> &projects, const vector<string> &folders) {
  // Maps each project ID to the set of folders to be created in that
  // project.
  map<string, set<string> > uniqueFolders;

  // This should probably be checked in Options
  assert(projects.size() == folders.size());

  for (unsigned int i = 0; i < projects.size(); ++i) {
    string projectID = resolveProject(projects[i]);
    uniqueFolders[projectID].insert(folders[i]);
  }

  for (map<string, set<string> >::iterator i = uniqueFolders.begin(); i != uniqueFolders.end(); ++i) {
    string projectID = i->first;
    set<string> folders = i->second;
    for (set<string>::iterator j = folders.begin(); j != folders.end(); ++j) {
      string folder = (*j);
      createFolder(projectID, folder);
    }
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

  DXLOG(logINFO) << "Creating new file with parameters " << params.toString();

  JSON result = fileNew(params);
  DXLOG(logINFO) << "Got result " << result.toString();

  return result["id"].get<string>();
}

string getProjectName(const string &projectID) {
  JSON params(JSON_OBJECT);
  JSON result = projectDescribe(projectID, params, false);  // no retry
  return result["name"].get<string>();
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
    DXLOG(logINFO) << " failure while running findDataObjects with this input query: " << query.toString();
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
