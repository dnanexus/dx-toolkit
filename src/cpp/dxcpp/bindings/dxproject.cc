#include "dxproject.h"

using namespace std;
using namespace dx;

JSON DXWorkspace::describe(bool folders) const {
  if (folders) {
    return DXHTTPRequest("/" + dxid_ + "/describe",
                         "{\"folders\": true}");
  } else {
    return DXHTTPRequest("/" + dxid_ + "/describe",
                         "{}");
  }
}

// Generic
void DXWorkspace::move(const JSON &objects,
                       const JSON &folders,
                       const string &dest_folder) const {
  JSON input_params(JSON_OBJECT);
  input_params["objects"] = objects;
  input_params["folders"] = folders;
  input_params["destination"] = dest_folder;
  DXHTTPRequest("/" + dxid_ + "/move",
                input_params.toString());
}

void DXWorkspace::clone(const JSON &objects,
                        const JSON &folders,
                        const string &dest_proj,
                        const string &dest_folder) const {
  JSON input_params(JSON_OBJECT);
  input_params["objects"] = objects;
  input_params["folders"] = folders;
  input_params["project"] = dest_proj;
  input_params["destination"] = dest_folder;
  DXHTTPRequest("/" + dxid_ + "/clone",
                input_params.toString());
}

// Folder-specific
void DXWorkspace::newFolder(const string &folder, bool parents) const {
  stringstream input_hash;
  input_hash << "{\"folder\": \"" + folder + "\",";
  input_hash << "\"parents\": " << (parents ? "true" : "false") << "}";
  DXHTTPRequest("/" + dxid_ + "/newFolder",
                input_hash.str());
}

JSON DXWorkspace::listFolder(const string &folder) const {
  return DXHTTPRequest("/" + dxid_ + "/listFolder",
                       "{\"folder\": \"" + folder + "\"}");
}

void DXWorkspace::moveFolder(const string &folder,
                             const string &dest_folder) const {
  DXHTTPRequest("/" + dxid_ + "/move",
                "{\"folders\": [\"" + folder + "\"]," +
                "\"destination\": \"" + dest_folder + "\"}");
}

void DXWorkspace::removeFolder(const string &folder, const bool recurse) const {
  string input = recurse ?
    "{\"folder\": \"" + folder + "\", \"recurse\": true}" :
    "{\"folder\": \"" + folder + "\"}";

  DXHTTPRequest("/" + dxid_ + "/removeFolder", input);
}

// Objects-specific
void DXWorkspace::removeObjects(const JSON &objects) const {
  DXHTTPRequest("/" + dxid_ + "/removeObjects",
                "{\"objects\":" + objects.toString() + "}");
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
