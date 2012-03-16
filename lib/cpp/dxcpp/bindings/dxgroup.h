#ifndef DXCPP_BINDINGS_DXGROUP_H
#define DXCPP_BINDINGS_DXGROUP_H

#include "../bindings.h"

class DXGroup: public DXClass {
 public:
  JSON describe() const { return groupDescribe(dxid_); }
  JSON getProperties(const JSON &keys) const { return groupGetProperties(dxid_, keys); }
  void setProperties(const JSON &properties) const { groupSetProperties(dxid_, properties); }
  void addTypes(const JSON &types) const { groupAddTypes(dxid_, types); }
  void removeTypes(const JSON &types) const { groupRemoveTypes(dxid_, types); }
  void destroy() { groupDestroy(dxid_); }

  // Group-specific functions

  void create();
  JSON getMembers();
  void addMembers(JSON members);
  void removeMembers(JSON members);
};

#endif
