#include <unistd.h>
#include "bindings.h"

using namespace std;
using namespace dx;

void DXDataObject::setIDs(const std::string &dxid,
			  const std::string &proj) {
  dxid_ = dxid;
  if (proj == "default")
    proj_ = g_WORKSPACE_ID;
  else
    proj_ = proj;
}

void DXDataObject::waitOnState(const string &state,
			  const int timeout) const {
  int elapsed = 0;
  string cur_state;
  do {
    cur_state = describe()["state"].get<string>();
    if (cur_state == state)
      return;
    sleep(2);
    elapsed += 2;
  } while (elapsed <= timeout);
}

JSON DXDataObject::describe(bool incl_properties) const {
  stringstream input_hash;
  input_hash << "{";
  if (proj_ != "")
    input_hash << "\"project\": \"" << proj_ << "\",";
  input_hash << "\"properties\": " << (incl_properties ? "true" : "false") << "}";
  return describe_(input_hash.str());
}

void DXDataObject::addTypes(const dx::JSON &types) const {
  stringstream input_hash;
  input_hash << "{\"types\":" << types.toString() << "}";
  addTypes_(input_hash.str());
}

void DXDataObject::removeTypes(const dx::JSON &types) const {
  stringstream input_hash;
  input_hash << "{\"types\":" << types.toString() << "}";
  removeTypes_(input_hash.str());
}

JSON DXDataObject::getDetails() const {
  return getDetails_("{}");
}

void DXDataObject::setDetails(const dx::JSON &details) const {
  setDetails_(details.toString());
}

void DXDataObject::setVisibility(bool hidden) const {
  stringstream input_hash;
  input_hash << "{\"hidden\":" << (hidden ? "true": "false") << "}";
  setVisibility_(input_hash.str());
}

void DXDataObject::rename(const std::string &name) const {
  stringstream input_hash;
  input_hash << "{";
  if (proj_ != "")
    input_hash << "\"project\": \"" << proj_ << "\",";
  input_hash << "\"name\": " << name << "}";
  rename_(input_hash.str());
}

void DXDataObject::setProperties(const dx::JSON &properties) const {
  stringstream input_hash;
  input_hash << "{";
  if (proj_ != "")
    input_hash << "\"project\": \"" << proj_ << "\",";
  input_hash << "\"properties\": " << properties.toString() << "}";
  setProperties_(input_hash.str());
}

void DXDataObject::addTags(const dx::JSON &tags) const {
  stringstream input_hash;
  input_hash << "{";
  if (proj_ != "")
    input_hash << "\"project\": \"" << proj_ << "\",";
  input_hash << "\"tags\": " << tags.toString() << "}";
  addTags_(input_hash.str());
}

void DXDataObject::removeTags(const dx::JSON &tags) const {
  stringstream input_hash;
  input_hash << "{";
  if (proj_ != "")
    input_hash << "\"project\": \"" << proj_ << "\",";
  input_hash << "\"tags\": " << tags.toString() << "}";
  removeTags_(input_hash.str());
}

void DXDataObject::close() const {
  close_("{}");
}

JSON DXDataObject::listProjects() const {
  return listProjects_("{}");
}

void DXDataObject::remove() {
  projectRemoveObjects(proj_, "{\"objects\":[" + dxid_ + "]}");
}
