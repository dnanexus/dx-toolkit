#include "api_helper.h"

#include <curl/curl.h>

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
 *
 * To determine whether projectSpec is an ID, we call describe. If this
 * succeeds or throws PermissionDenied, we have verified that a project
 * with that ID exists (though we may not have permission to access it).
 *
 * If projectSpec is not an ID, we try to determine whether it is a name by
 * calling findProjects on projectSpec, with minimum permission level
 * "CONTRIBUTE". If no results are returned, the projectSpec cannot be
 * resolved; if more than one project is returned, projectSpec is
 * ambiguous; otherwise, the projectSpec is unambiguously a project name.
 */
string resolveProject(const string &projectSpec) {
  LOG << "Resolving project specifier " << projectSpec << "...";
  string projectID;

  try {
    dx::JSON desc = projectDescribe(urlEscape(projectSpec));
    projectID = desc["id"].get<string>();
  } catch (DXAPIError &e) {
    if (e.name == "PermissionDenied") {
      // the project exists, though we don't have access to it
      projectID = projectSpec;
    }
  }

  if (!projectID.empty()) {
    LOG << " found project ID " << projectID << endl;
    return projectID;
  }

  try {
    dx::JSON params(dx::JSON_OBJECT);
    params["name"] = projectSpec;
    params["level"] = "CONTRIBUTE";

    dx::JSON findResult = systemFindProjects(params);
    dx::JSON projects = findResult["results"];

    if (projects.size() == 0) {
      LOG << " failure." << endl;
      throw runtime_error("\"" + projectSpec + "\" is not a valid project name or ID");
    } else if (projects.size() > 1) {
      LOG << " failure." << endl;
      throw runtime_error("\"" + projectSpec + "\" does not uniquely identify a project");
    } else {
      projectID = projects[0]["id"].get<string>();
      LOG << " found project ID " << projectID << endl;
      return projectID;
    }
  } catch (DXAPIError &e) {
    LOG << "Call to findProjects failed." << endl;
    throw;
  }
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
string createFileObject(const string &project, const string &folder, const string &name) {
  dx::JSON params(dx::JSON_OBJECT);
  params["project"] = project;
  params["folder"] = folder;
  params["name"] = name;
  params["parents"] = true;
  LOG << "Creating new file with parameters " << params.toString() << endl;

  dx::JSON result = fileNew(params);
  LOG << "Got result " << result.toString() << endl;

  return result["id"].get<string>();
}

void closeFileObject(const string &fileID) {
  fileClose(fileID);
}
