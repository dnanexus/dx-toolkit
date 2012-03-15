#ifndef DXCPP_BINDINGS_DXUSER_H
#define DXCPP_BINDINGS_DXUSER_H

#import "bindings.h"

namespace dxpy {
  using namespace dxpy;

  class DXUser: public DXClass {
  public:
    JSON describe() { return userDescribe(dxid); }
    JSON getProperties() { return userGetProperties(dxid); }
    void setProperties() { userSetProperties(dxid); }
    void addTypes() { userAddTypes(dxid); }
    void removeTypes() { userRemoveTypes(dxid); }
    void destroy() { throw; }
  };
}

#endif
