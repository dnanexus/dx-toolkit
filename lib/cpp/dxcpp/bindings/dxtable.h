#ifndef DXCPP_BINDINGS_DXTABLE_H
#define DXCPP_BINDINGS_DXTABLE_H

#include "../bindings.h"

class DXTable: public DXDataObject {
 private:
  dx::JSON describe_(const std::string &s)const{return tableDescribe(dxid_,s);}
  void addTypes_(const std::string &s)const{tableAddTypes(dxid_,s);}
  void removeTypes_(const std::string &s)const{tableRemoveTypes(dxid_,s);}
  dx::JSON getDetails_(const std::string &s)const{return tableGetDetails(dxid_,s);}
  void setDetails_(const std::string &s)const{tableSetDetails(dxid_,s);}
  void setVisibility_(const std::string &s)const{tableSetVisibility(dxid_,s);}
  void rename_(const std::string &s)const{tableRename(dxid_,s);}
  void setProperties_(const std::string &s)const{tableSetProperties(dxid_,s);}
  void addTags_(const std::string &s)const{tableAddTags(dxid_,s);}
  void removeTags_(const std::string &s)const{tableRemoveTags(dxid_,s);}
  void close_(const std::string &s)const{tableClose(dxid_,s);}
  dx::JSON listProjects_(const std::string &s)const{return tableListProjects(dxid_,s);}

 public:
  // Table-specific functions

  DXTable() {}
  DXTable(const std::string &dxid) { setIDs(dxid); }
  void create(const dx::JSON &to_store);

  static DXTable newDXTable(const dx::JSON &to_store);
};

#endif
