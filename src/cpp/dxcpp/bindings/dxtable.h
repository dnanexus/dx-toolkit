#ifndef DXCPP_BINDINGS_DXTABLE_H
#define DXCPP_BINDINGS_DXTABLE_H

#include "../bindings.h"
/**
 *@brief NOT IMPLEMENTED
 *
 *Not implemented yet. DO NOT USE.
 */
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
  DXTable(const std::string &dxid,
          const std::string &proj=g_WORKSPACE_ID) { setIDs(dxid, proj); }
  DXTable(const dx::JSON &dxlink) { setIDs(dxlink); }
  
  void create();

  static DXTable newDXTable();

  /**
   * NOT IMPLEMENTED
   *
   * Clones the associated object into the specified project and folder.
   *
   * @param dest_proj_id ID of the project to which the object should
   * be cloned
   * @param dest_folder Folder route in which to put it in the
   * destination project.
   * @return New object handler with the associated project set to
   * dest_proj_id.
   */
  DXTable clone(const std::string &dest_proj_id,
                const std::string &dest_folder="/") const;
};

#endif
