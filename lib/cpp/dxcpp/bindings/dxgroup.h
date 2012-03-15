#ifndef DXCPP_BINDINGS_DXGROUP_H
#define DXCPP_BINDINGS_DXGROUP_H

#import "bindings.h"

namespace dxpy {
  using namespace dxpy;

  class DXGroup: public DXClass {
  public:
    JSON describe() { return groupDescribe(dxid); }
    JSON getProperties() { return groupGetProperties(dxid); }
    void setProperties() { groupSetProperties(dxid); }
    void addTypes() { groupAddTypes(dxid); }
    void removeTypes() { groupRemoveTypes(dxid); }
    void destroy() { groupDestroy(dxid); }

    // Group-specific functions

    void create();
    JSON getMembers();
    void addMembers(JSON members);
    void removeMembers(JSON members);
  };
}

#endif
