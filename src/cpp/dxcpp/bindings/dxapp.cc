#include "dxapp.h"

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

void DXApp::addCategories(const dx::JSON &categories) const {
  stringstream input_hash;
  input_hash << "{\"categories\":" << categories.toString() << "}";
  if (dxid_ != "") {
    appAddCategories(dxid_);
  } else if (name_ != "") {
    appAddCategoriesWithAlias(string("app-") + name_, alias_);
  } else {
    throw DXError("No ID is set for this DXApp handler");
  }
}

void DXApp::removeCategories(const dx::JSON &categories) const {
  stringstream input_hash;
  input_hash << "{\"categories\":" << categories.toString() << "}";
  if (dxid_ != "") {
    appRemoveCategories(dxid_);
  } else if (name_ != "") {
    appRemoveCategoriesWithAlias(string("app-") + name_, alias_);
  } else {
    throw DXError("No ID is set for this DXApp handler");
  }
}


void DXApp::addTags(const dx::JSON &tags) const {
  stringstream input_hash;
  input_hash << "{\"tags\":" << tags.toString() << "}";
  if (dxid_ != "") {
    appAddTags(dxid_);
  } else if (name_ != "") {
    appAddTagsWithAlias(string("app-") + name_, alias_);
  } else {
    throw DXError("No ID is set for this DXApp handler");
  }
}

void DXApp::removeTags(const dx::JSON &tags) const {
  stringstream input_hash;
  input_hash << "{\"tags\":" << tags.toString() << "}";
  if (dxid_ != "") {
    appRemoveTags(dxid_);
  } else if (name_ != "") {
    appRemoveTagsWithAlias(string("app-") + name_, alias_);
  } else {
    throw DXError("No ID is set for this DXApp handler");
  }
}

DXJob DXApp::run(const JSON &app_input,
                     const string &project_context,
                     const string &output_folder) const {
  JSON input_params(JSON_OBJECT);
  input_params["input"] = app_input;
  if (g_JOB_ID == "")
    input_params["project"] = project_context;
  input_params["folder"] = output_folder;
  const JSON resp = appRun(dxid_, input_params);
  return DXJob(resp["id"].get<string>());
}
