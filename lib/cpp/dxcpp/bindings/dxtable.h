#ifndef DXCPP_BINDINGS_DXCOLLECTION_H
#define DXCPP_BINDINGS_DXCOLLECTION_H

#include "../bindings.h"

class DXCollection: public DXClass {
 public:
  dx::JSON describe() const { return collectionDescribe(dxid_); }
  dx::JSON getProperties(const dx::JSON &keys) const { return collectionGetProperties(dxid_, keys); }
  void setProperties(const dx::JSON &properties) const { collectionSetProperties(dxid_, properties); }
  void addTypes(const dx::JSON &types) const { collectionAddTypes(dxid_, types); }
  void removeTypes(const dx::JSON &types) const { collectionRemoveTypes(dxid_, types); }
  void destroy() { collectionDestroy(dxid_); }

  // Collection-specific functions

  DXCollection() {}
  DXCollection(const std::string &dxid) { setID(dxid); }
  void create(const dx::JSON &to_store);
  dx::JSON get() const;
  void set(const dx::JSON &to_store) const;

  static DXCollection newDXCollection(const dx::JSON &to_store);
};

#endif
