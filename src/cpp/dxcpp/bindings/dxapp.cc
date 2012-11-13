#include "dxapp.h"
#include "execution_common_helper.h"

using namespace std;
using namespace dx;

JSON DXApp::describe() const {
  if (dxid_ != "") {
    return appDescribe(dxid_);
  } else if (name_ != "") {
    return appDescribeWithAlias(string("app-") + name_, alias_);
  } else {
    throw DXError("No ID is set for this DXApp handler");
  }
}

void DXApp::update(const dx::JSON &to_update) const {
  if (dxid_ != "") {
    appUpdate(dxid_, to_update);
  } else if (name_ != "") {
    appUpdateWithAlias(string("app-") + name_, alias_, to_update);
  } else {
    throw DXError("No ID is set for this DXApp handler");
  }
}

void DXApp::addCategories(const dx::JSON &categories) const {
  stringstream input_hash;
  input_hash << "{\"categories\":" << categories.toString() << "}";
  if (dxid_ != "") {
    appAddCategories(dxid_, input_hash.str());
  } else if (name_ != "") {
    appAddCategoriesWithAlias(string("app-") + name_, alias_, input_hash.str());
  } else {
    throw DXError("No ID is set for this DXApp handler");
  }
}

void DXApp::removeCategories(const dx::JSON &categories) const {
  stringstream input_hash;
  input_hash << "{\"categories\":" << categories.toString() << "}";
  if (dxid_ != "") {
    appRemoveCategories(dxid_, input_hash.str());
  } else if (name_ != "") {
    appRemoveCategoriesWithAlias(string("app-") + name_, alias_, input_hash.str());
  } else {
    throw DXError("No ID is set for this DXApp handler");
  }
}


void DXApp::addTags(const dx::JSON &tags) const {
  stringstream input_hash;
  input_hash << "{\"tags\":" << tags.toString() << "}";
  if (dxid_ != "") {
    appAddTags(dxid_, input_hash.str());
  } else if (name_ != "") {
    appAddTagsWithAlias(string("app-") + name_, alias_, input_hash.str());
  } else {
    throw DXError("No ID is set for this DXApp handler");
  }
}

void DXApp::removeTags(const dx::JSON &tags) const {
  stringstream input_hash;
  input_hash << "{\"tags\":" << tags.toString() << "}";
  if (dxid_ != "") {
    appRemoveTags(dxid_, input_hash.str());
  } else if (name_ != "") {
    appRemoveTagsWithAlias(string("app-") + name_, alias_, input_hash.str());
  } else {
    throw DXError("No ID is set for this DXApp handler");
  }
}

void DXApp::install() const {
  if (dxid_ != "") {
    appInstall(dxid_);
  } else if (name_ != "") {
    appInstallWithAlias(string("app-") + name_, alias_);
  } else {
    throw DXError("No ID is set for this DXApp handler");
  } 
}

void DXApp::uninstall() const {
  if (dxid_ != "") {
    appUninstall(dxid_);
  } else if (name_ != "") {
    appUninstallWithAlias(string("app-") + name_, alias_);
  } else {
    throw DXError("No ID is set for this DXApp handler");
  } 
}

JSON DXApp::get() const {
  if (dxid_ != "") {
    return appGet(dxid_);
  } else if (name_ != "") {
    return appGetWithAlias(string("app-") + name_, alias_);
  } else {
    throw DXError("No ID is set for this DXApp handler");
  } 
}

void DXApp::publish(bool makeDefault) const {
  string inp = string("{\"makeDefault\": ") + ((makeDefault) ? string("true") : string("false")) + string("}");
  if (dxid_ != "") {
    appPublish(dxid_, inp);
  } else if (name_ != "") {
    appPublishWithAlias(string("app-") + name_, alias_, inp);
  } else {
    throw DXError("No ID is set for this DXApp handler");
  } 
}

void DXApp::remove() const {
  if (dxid_ != "") {
    appDelete(dxid_);
  } else if (name_ != "") {
    appDeleteWithAlias(string("app-") + name_, alias_);
  } else {
    throw DXError("No ID is set for this DXApp handler");
  } 
}

DXJob DXApp::run(const JSON &app_input,
                 const string &output_folder,
                 const vector<string> &depends_on,
                 const dx::JSON &instance_type,
                 const string &project_context) const {
  JSON input_params(JSON_OBJECT);
  input_params["input"] = app_input;
  if (g_JOB_ID == "")
    input_params["project"] = project_context;

  input_params["folder"] = output_folder;
  appendDependsOnAndInstanceType(input_params, depends_on, "main", instance_type);    
  const JSON resp = appRun(dxid_, input_params);
  return DXJob(resp["id"].get<string>());
}
