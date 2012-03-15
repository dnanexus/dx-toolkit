#ifndef DXCPP_BINDINGS_DXCOLLECTION_H
#define DXCPP_BINDINGS_DXCOLLECTION_H

#import "../bindings.h"

namespace dxpy {
  using namespace dxpy;

  class DXCollection: public DXClass {
  public:
    JSON describe() { return collectionDescribe(dxid); }
    JSON getProperties() { return collectionGetProperties(dxid); }
    void setProperties() { collectionSetProperties(dxid); }
    void addTypes() { collectionAddTypes(dxid); }
    void removeTypes() { collectionRemoveTypes(dxid); }
    void destroy() { collectionDestroy(dxid); }

    // Collection-specific functions

    DXCollection(JSON to_store);
    void create(JSON to_store);
    void get();
    void set();
  };
}

#endif
