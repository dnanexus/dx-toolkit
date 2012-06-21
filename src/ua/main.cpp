#include <iostream>

#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"

#include "options.h"

using namespace std;
using namespace dx;

// /*
//  * Ensure that the project specified in opt exists. This incidentally
//  * ensures that the user has LIST permission on the project, and an error
//  * will be reported if not. However, file creation will still fail if the
//  * user does not have CONTRIBUTE permission.
//  */
// bool validateProject(const Options &opt) {
//   try {
//     JSON projDesc = projectDescribe(opt.project);
//     cerr << "Project: " << projDesc.toString() << endl;
//   } catch (DXAPIError &e) {
//     cerr << "An error occurred:" << endl
//          << " name: " << e.name << endl
//          << " resp_code: " << e.resp_code << endl;

//     if (e.name == "ResourceNotFound") {
//       cerr << "ERROR: Project " << opt.project << " does not exist." << endl;
//     } else if (e.name == "PermissionDenied") {
//       cerr << "ERROR: Project " << opt.project << " is not accessible." << endl;
//     } else {
//       cerr << "ERROR: " << e.what() << endl;
//     }

//     return false;
//   }
//   return true;
// }

JSON securityContext(const string &authToken) {
  JSON ctx(JSON_OBJECT);
  ctx["auth_token_type"] = "Bearer";
  ctx["auth_token"] = authToken;
  return ctx;
}

void testServerConnection() {
  cerr << "Testing connection to API server...";
  try {
    JSON result = systemFindProjects();
    cerr << " success." << endl;
  } catch (exception &e) {
    cerr << " failed." << endl;
    throw e;
  }
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
  cerr << "Resolving project specifier " << projectSpec << "...";
  string projectID;

  try {
    JSON desc = projectDescribe(projectSpec);
    projectID = desc["id"].get<string>();
  } catch (DXAPIError &e) {
    if (e.name == "PermissionDenied") {
      // the project exists, though we don't have access to it
      projectID = projectSpec;
    }
  }

  if (!projectID.empty()) {
    cerr << " found project ID " << projectID << endl;
    return projectID;
  }

  try {
    JSON params(JSON_OBJECT);
    params["name"] = projectSpec;
    params["level"] = "CONTRIBUTE";

    JSON findResult = systemFindProjects(params);
    JSON projects = findResult["results"];

    if (projects.size() == 0) {
      cerr << " failed" << endl;
      throw runtime_error("\"" + projectSpec + "\" is not a valid project name or ID");
    } else if (projects.size() > 1) {
      cerr << " failed" << endl;
      throw runtime_error("\"" + projectSpec + "\" does not uniquely identify a project");
    } else {
      projectID = projects[0]["id"].get<string>();
      cerr << " found project ID " << projectID << endl;
      return projectID;
    }
  } catch (DXAPIError &e) {
    cerr << "Call to findProjects failed." << endl;
    throw e;
  }
}

/*
 * Create the file object. The object is created in the project and folder
 * specified in opt, and with the specified name. The folder and any parent
 * folders are created if they do not exist.
 */
string createFileObject(const Options &opt) {
  JSON params(JSON_OBJECT);
  params["project"] = opt.project;
  params["folder"] = opt.folder;
  params["name"] = opt.name;
  params["parents"] = true;
  cerr << "Creating new file with parameters " << params.toString() << endl;

  JSON result = fileNew(params);
  cerr << "Got result " << result.toString() << endl;

  return result["id"].get<string>();
}

int main(int argc, char * argv[]) {
  cerr << "DNAnexus Upload Agent" << endl;

  Options opt;
  opt.parse(argc, argv);

  if (opt.help() || opt.getFile().empty()) {
    opt.printHelp();
    return 1;
  }

  cerr << opt;
  opt.validate();

  setAPIServerInfo(opt.apiserverHost, opt.apiserverPort);
  setSecurityContext(securityContext(opt.authToken));
  setProjectContext(opt.project);

  /*
   * TODO:
   *
   * (+) Verify that the API server host and port and the auth token are
   *     valid (i.e., that we can connect to the API server.
   *
   * (>) Resolve the project specifier to a project ID
   *
   * (*) Verify that we have CONTRIBUTE permissions on the project.
   *
   * (*) Create the folder (could be done automatically in fileNew).
   */

  try {
    testServerConnection();
    string projectID = resolveProject(opt.project);

    // string fileID = createFileObject(opt);
    // cerr << "fileID is " << fileID << endl;

    // fileClose(fileID);
  } catch (exception &e) {
    cerr << "ERROR: " << e.what() << endl;
    return 1;
  }

  return 0;
}
