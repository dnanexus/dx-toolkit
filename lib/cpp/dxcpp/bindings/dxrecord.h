#ifndef DXCPP_BINDINGS_DXRECORD_H
#define DXCPP_BINDINGS_DXRECORD_H

#include "../bindings.h"

class DXRecord: public DXClass {
 public:
  dx::JSON describe() const { return jsonDescribe(dxid_); }
  dx::JSON getProperties(const dx::JSON &keys=dx::JSON()) const { return jsonGetProperties(dxid_, keys); }
  void setProperties(const dx::JSON &properties) const { jsonSetProperties(dxid_, properties); }
  void addTypes(const dx::JSON &types) const { jsonAddTypes(dxid_, types); }
  void removeTypes(const dx::JSON &types) const { jsonRemoveTypes(dxid_, types); }
  void destroy() { jsonDestroy(dxid_); }

  // Record-specific functions

  DXRecord() {}
  DXRecord(const std::string &dxid) { setID(dxid); }
  void create(const dx::JSON &to_store);
  dx::JSON get() const;
  void set(const dx::JSON &to_store) const;

  static DXRecord newDXRecord(const dx::JSON &to_store);
};

#endif
