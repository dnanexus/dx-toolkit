#ifndef DXCPP_BINDINGS_DXTABLE_H
#define DXCPP_BINDINGS_DXTABLE_H

#include "../bindings.h"

class DXTable: public DXDataObject {
 private:
  dx::JSON describe_(const std::string &input_params) const {
    return tableDescribe(dxid_, input_params);
  }
  void addTypes_(const std::string &input_params) const {
    tableAddTypes(dxid_, input_params);
  }
  void removeTypes_(const std::string &input_params) const {
    tableRemoveTypes(dxid_, input_params);
  }
  dx::JSON getDetails_(const std::string &input_params) const {
    return tableGetDetails(dxid_, input_params);
  }
  void setDetails_(const std::string &input_params) const {
    tableSetDetails(dxid_, input_params);
  }
  void setVisibility_(const std::string &input_params) const {
    tableSetVisibility(dxid_, input_params);
  }
  void rename_(const std::string &input_params) const {
    tableRename(dxid_, input_params);
  }
  void setProperties_(const std::string &input_params) const {
    tableSetProperties(dxid_, input_params);
  }
  void addTags_(const std::string &input_params) const {
    tableAddTags(dxid_, input_params);
  }
  void removeTags_(const std::string &input_params) const {
    tableRemoveTags(dxid_, input_params);
  }
  void close_(const std::string &input_params) const {
    tableClose(dxid_, input_params);
  }
  dx::JSON listProjects_(const std::string &input_params) const {
    return tableListProjects(dxid_, input_params);
  }

 public:
  // Table-specific functions

  DXTable() {}
  DXTable(const std::string &dxid) { setIDs(dxid); }
  void create(const dx::JSON &to_store);

  static DXTable newDXTable(const dx::JSON &to_store);
};

#endif
