#ifndef DXCPP_BINDINGS_DXGROUP_H
#define DXCPP_BINDINGS_DXGROUP_H

#include "../bindings.h"

class DXGroup: public DXClass {
 public:
  dx::JSON describe() const { return groupDescribe(dxid_); }
  dx::JSON getProperties(const dx::JSON &keys) const { return groupGetProperties(dxid_, keys); }
  void setProperties(const dx::JSON &properties) const { groupSetProperties(dxid_, properties); }
  void addTypes(const dx::JSON &types) const { groupAddTypes(dxid_, types); }
  void removeTypes(const dx::JSON &types) const { groupRemoveTypes(dxid_, types); }
  void destroy() { groupDestroy(dxid_); }

  // Group-specific functions

  void create();
  dx::JSON getMembers();
  void addMembers(dx::JSON members);
  void removeMembers(dx::JSON members);
};

#endif
