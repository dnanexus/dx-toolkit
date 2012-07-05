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
   * @param data_obj_fields JSON containing the optional fields with
   * which to create the object ("project", "types", "details",
   * "hidden", "name", "properties", "tags")
   */
  void create(const dx::JSON &data_obj_fields=dx::JSON(dx::JSON_OBJECT));

  /**
   * Create a new remote record object.
   *
   * @param data_obj_fields JSON containing the optional fields with
   * which to create the object ("project", "types", "details",
   * "hidden", "name", "properties", "tags")
   * @return A DXRecord remote object handler.
   */
  static DXRecord newDXRecord(const dx::JSON &data_obj_fields=
			      dx::JSON(dx::JSON_OBJECT));

  /**
   * Clones the associated object into the specified project and folder.
   *
   * @param dest_proj_id ID of the project to which the object should
   * be cloned
   * @param dest_folder Folder route in which to put it in the
   * destination project.
   * @return New object handler with the associated project set to
   * dest_proj_id.
   */
  DXRecord clone(const std::string &dest_proj_id,
                 const std::string &dest_folder="/") const;
};

#endif
