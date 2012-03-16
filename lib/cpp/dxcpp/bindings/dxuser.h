#ifndef DXCPP_BINDINGS_DXUSER_H
#define DXCPP_BINDINGS_DXUSER_H

#include "../bindings.h"

class DXUser: public DXClass {
 public:
  JSON describe() const { return userDescribe(dxid_); }
  JSON getProperties(const JSON &keys) const { return userGetProperties(dxid_, keys); }
  void setProperties(const JSON &properties) const { userSetProperties(dxid_, properties); }
  void addTypes(const JSON &types) const { userAddTypes(dxid_, types); }
  void removeTypes(const JSON &types) const { userRemoveTypes(dxid_, types); }
  void destroy() { throw DXError(); }
};

#endif
