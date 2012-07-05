#include "dxproject.h"

using namespace std;
using namespace dx;

JSON DXProject::describe(bool folders) const {
  if (folders)
    return projectDescribe(dxid_, string("{\"folders\": true}"));
  else
    return projectDescribe(dxid_);
}

void DXProject::update(const JSON &to_update) const {
  projectUpdate(dxid_, to_update);
}

void DXProject::destroy() const {
  projectDestroy(dxid_);
}

// Generic
void DXProject::move(const JSON &objects,
                     const JSON &folders,
                     const string &dest_folder) const {
  JSON input_params(JSON_OBJECT);
  input_params["objects"] = objects;
  input_params["folders"] = folders;
  input_params["destination"] = dest_folder;
  projectMove(dxid_, input_params);
}

void DXProject::clone(const JSON &objects,
                      const JSON &folders,
                      const string &dest_proj,
                      const string &dest_folder) const {
  JSON input_params(JSON_OBJECT);
  input_params["objects"] = objects;
  input_params["folders"] = folders;
  input_params["project"] = dest_proj;
  input_params["destination"] = dest_folder;
  projectClone(dxid_, input_params);
}

// Folder-specific
void DXProject::newFolder(const string &folder, bool parents) const {
  stringstream input_hash;
  input_hash << "{\"folder\": \"" + folder + "\",";
  input_hash << "\"parents\": " << (parents ? "true" : "false") << "}";
  projectNewFolder(dxid_, input_hash.str());
}

JSON DXProject::listFolder(const string &folder) const {
  return projectListFolder(dxid_, "{\"folder\": \"" + folder + "\"}");
}

void DXProject::moveFolder(const string &folder,
                           const string &dest_folder) const {
  projectMove(dxid_, "{\"folders\": [\"" + folder + "\"]," +
              "\"destination\": \"" + dest_folder + "\"}");
}

void DXProject::removeFolder(const string &folder, const bool recurse) const {
  if (recurse) {
    projectRemoveFolder(dxid_, "{\"folder\": \"" + folder + "\", \"recurse\": true}");
  } else {
    projectRemoveFolder(dxid_, "{\"folder\": \"" + folder + "\"}");
  }
}

// Objects-specific
void DXProject::removeObjects(const JSON &objects) const {
  projectRemoveObjects(dxid_, "{\"objects\":" + objects.toString() + "}");
}

