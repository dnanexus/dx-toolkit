#ifndef DXCPP_BINDINGS_DXCOLLECTION_H
#define DXCPP_BINDINGS_DXCOLLECTION_H

#include "../bindings.h"

class DXCollection: public DXClass {
 public:
  JSON describe() const { return collectionDescribe(dxid_); }
  JSON getProperties(const JSON &keys) const { return collectionGetProperties(dxid_, keys); }
  void setProperties(const JSON &properties) const { collectionSetProperties(dxid_, properties); }
  void addTypes(const JSON &types) const { collectionAddTypes(dxid_, types); }
  void removeTypes(const JSON &types) const { collectionRemoveTypes(dxid_, types); }
  void destroy() { collectionDestroy(dxid_); }

  // Collection-specific functions

  DXCollection() {}
  DXCollection(const string &dxid) { this->setID(dxid); }
  void create(const JSON &to_store);
  JSON get() const;
  void set(const JSON &to_store) const;
};

DXCollection newDXCollection(const JSON &to_store);

#endif
