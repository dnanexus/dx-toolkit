#include "dxproject.h"

using namespace std;
using namespace dx;

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
    "{\"folder\": \"" + folder + "\", \"recurse\": true}" :
    "{\"folder\": \"" + folder + "\"}";
  projectRemoveFolder(getID(), input);
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
