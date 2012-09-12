#include "api_helper.h"

#include <curl/curl.h>

#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"

#include "log.h"

using namespace std;

dx::JSON securityContext(const string &authToken) {
  dx::JSON ctx(dx::JSON_OBJECT);
  ctx["auth_token_type"] = "Bearer";
  ctx["auth_token"] = authToken;
  return ctx;
}

void apiInit(const string &apiserverHost, const int apiserverPort, const string &apiserverProtocol, const string &authToken) {
  setAPIServerInfo(apiserverHost, apiserverPort, apiserverProtocol);
  setSecurityContext(securityContext(authToken));
}

void testServerConnection() {
  LOG << "Testing connection to API server...";
  try {
    dx::JSON result = systemFindProjects();
    LOG << " success." << endl;
  } catch (exception &e) {
    LOG << " failure." << endl;
    throw;
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
 *            1) Throw an error is no such project exist.
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
    dx::JSON desc = projectDescribe(urlEscape(projectSpec));
    string level = desc["level"].get<string>();
    if ((level == "CONTRIBUTE") || (level == "ADMINISTER")) {
      matchingProjectIdToName[projectSpec] = desc["name"].get<string>();
    }
  } catch (DXAPIError &e) {
    // Ignore the error (we will check for matching project name)
  }
  
  try {
    dx::JSON params(dx::JSON_OBJECT);
    params["name"] = projectSpec;
    params["level"] = "CONTRIBUTE";
    
    dx::JSON findResult = systemFindProjects(params);
    dx::JSON projects = findResult["results"];
    LOG<< "\nstringified = "<< findResult.toString() << "\n\n";
    for (unsigned i = 0; i < projects.size(); ++i) {
      matchingProjectIdToName[projects[i]["id"].get<string>()] = projectSpec;
    }
  } catch (DXAPIError &e) {
    LOG << "Call to findProjects failed." << endl;
    throw;  
  }

  if (matchingProjectIdToName.size() == 0) {
    LOG << " failure." << endl;
    throw runtime_error("\"" + projectSpec + "\" is not a valid project name or ID (with >=CONTRIBUTE access)");
  }

  if (matchingProjectIdToName.size() > 1) {
    LOG << "failure. " << matchingProjectIdToName.size() << " projects (with >=CONTRIBUTE access) match the identifer: \"" + projectSpec + "\":" << endl;
    int i =  1;
    for (map<string, string>::const_iterator it = matchingProjectIdToName.begin(); it != matchingProjectIdToName.end(); ++it, ++i) {
      LOG << "\t" << i << ". \"" << it->second << "\" (ID = \"" << it->first << "\")" << endl;
    }
    throw runtime_error("\"" + projectSpec + "\" does not uniquely identify a project");
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
    dx::JSON desc = projectDescribe(projectID);
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
    dx::JSON params(dx::JSON_OBJECT);
    params["folder"] = folder;
    params["parents"] = true;
    projectNewFolder(projectID, params);
    LOG << " success." << endl;
  } catch (DXAPIError &e) {
    LOG << " failure." << endl;
    throw runtime_error("Could not create folder " + folder + " in project " + projectID + " (" + e.what() + ")");
  }
}

/*
 * Create the file object. The object is created in the given project and
 * folder, and with the specified name. The folder and any parent folders
 * are created if they do not exist.
 */
string createFileObject(const string &project, const string &folder, const string &name, const string &mimeType) {
  dx::JSON params(dx::JSON_OBJECT);
  params["project"] = project;
  params["folder"] = folder;
  params["name"] = name;
  params["parents"] = true;
  params["media"] = mimeType;
  LOG << "Creating new file with parameters " << params.toString() << endl;

  dx::JSON result = fileNew(params);
  LOG << "Got result " << result.toString() << endl;

  return result["id"].get<string>();
}

void closeFileObject(const string &fileID) {
  fileClose(fileID);
}

string getFileState(const string &fileID) {
  dx::JSON result = fileDescribe(fileID);
  return result["state"].get<string>();
}
