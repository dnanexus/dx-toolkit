#ifndef DXCPP_BINDINGS_DXUSER_H
#define DXCPP_BINDINGS_DXUSER_H

#include "../bindings.h"

class DXUser: public DXClass {
 public:
  dx::JSON describe() const { return userDescribe(dxid_); }
  dx::JSON getProperties(const dx::JSON &keys) const { return userGetProperties(dxid_, keys); }
  void setProperties(const dx::JSON &properties) const { userSetProperties(dxid_, properties); }
  void addTypes(const dx::JSON &types) const { userAddTypes(dxid_, types); }
  void removeTypes(const dx::JSON &types) const { userRemoveTypes(dxid_, types); }
  void destroy() { throw DXError(); }
};

#endif
