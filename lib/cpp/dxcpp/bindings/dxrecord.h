#ifndef DXCPP_BINDINGS_DXRECORD_H
#define DXCPP_BINDINGS_DXRECORD_H

#include "../bindings.h"

class DXRecord: public DXDataObject {
 private:
  dx::JSON describe_(const std::string &input_params) const {
    return recordDescribe(dxid_, input_params);
  }
  void addTypes_(const std::string &input_params) const {
    recordAddTypes(dxid_, input_params);
  }
  void removeTypes_(const std::string &input_params) const {
    recordRemoveTypes(dxid_, input_params);
  }
  dx::JSON getDetails_(const std::string &input_params) const {
    return recordGetDetails(dxid_, input_params);
  }
  void setDetails_(const std::string &input_params) const {
    recordSetDetails(dxid_, input_params);
  }
  void setVisibility_(const std::string &input_params) const {
    recordSetVisibility(dxid_, input_params);
  }
  void rename_(const std::string &input_params) const {
    recordRename(dxid_, input_params);
  }
  void setProperties_(const std::string &input_params) const {
    recordSetProperties(dxid_, input_params);
  }
  void addTags_(const std::string &input_params) const {
    recordAddTags(dxid_, input_params);
  }
  void removeTags_(const std::string &input_params) const {
    recordRemoveTags(dxid_, input_params);
  }
  void close_(const std::string &input_params) const {
    recordClose(dxid_, input_params);
  }
  dx::JSON listProjects_(const std::string &input_params) const {
    return recordListProjects(dxid_, input_params);
  }

 public:
  // Record-specific functions

  DXRecord() { }
  DXRecord(const std::string &dxid,
	   const std::string &proj="default") { setIDs(dxid, proj); }

  /**
   * Creates a new remote record object.  The handler is updated with
   * the object ID.
   *
   */
  void create();

  /**
   * Create a new remote record object.
   *
   * @return A DXRecord remote object handler.
   */
  static DXRecord newDXRecord();
};

#endif
