#ifndef DXCPP_BINDINGS_DXRECORD_H
#define DXCPP_BINDINGS_DXRECORD_H

#include "../bindings.h"

class DXRecord: public DXDataObject {
 private:
  dx::JSON describe_(const std::string &s)const{return recordDescribe(dxid_,s);}
  void addTypes_(const std::string &s)const{recordAddTypes(dxid_,s);}
  void removeTypes_(const std::string &s)const{recordRemoveTypes(dxid_,s);}
  dx::JSON getDetails_(const std::string &s)const{return recordGetDetails(dxid_,s);}
  void setDetails_(const std::string &s)const{recordSetDetails(dxid_,s);}
  void setVisibility_(const std::string &s)const{recordSetVisibility(dxid_,s);}
  void rename_(const std::string &s)const{recordRename(dxid_,s);}
  void setProperties_(const std::string &s)const{recordSetProperties(dxid_,s);}
  void addTags_(const std::string &s)const{recordAddTags(dxid_,s);}
  void removeTags_(const std::string &s)const{recordRemoveTags(dxid_,s);}
  void close_(const std::string &s)const{recordClose(dxid_,s);}
  dx::JSON listProjects_(const std::string &s)const{return recordListProjects(dxid_,s);}

 public:
  // Record-specific functions

  DXRecord() { }
  DXRecord(const std::string &dxid,
	   const std::string &proj=g_WORKSPACE_ID) { setIDs(dxid, proj); }

  /**
   * Creates a new remote record object.  The handler is updated with
   * the object ID.
   *
   */
  void create(const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

  /**
   * Create a new remote record object.
   *
   * @return A DXRecord remote object handler.
   */
  static DXRecord newDXRecord(const dx::JSON &data_obj_fields=
			      dx::JSON(dx::JSON_OBJECT));
};

#endif
