#ifndef DXCPP_BINDINGS_DXJSON_H
#define DXCPP_BINDINGS_DXJSON_H

#import "../bindings.h"

namespace dxpy {
  using namespace dxpy;

  class DXJSON: public DXClass {
  public:
    JSON describe() { return jsonDescribe(dxid); }
    JSON getProperties() { return jsonGetProperties(dxid); }
    void setProperties() { jsonSetProperties(dxid); }
    void addTypes() { jsonAddTypes(dxid); }
    void removeTypes() { jsonRemoveTypes(dxid); }
    void destroy() { jsonDestroy(dxid); }

    // JSON-specific functions

    DXJSON(JSON to_store);
    void create(JSON to_store);
    void get();
    void set();
  };
}

#endif
