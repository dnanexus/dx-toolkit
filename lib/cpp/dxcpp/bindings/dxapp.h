#ifndef DXCPP_BINDINGS_DXAPP_H
#define DXCPP_BINDINGS_DXAPP_H

#import "bindings.h"

namespace dxpy {
  using namespace dxpy;

  class DXApp: public DXClass {
  public:
    JSON describe() { return appDescribe(dxid); }
    JSON getProperties() { return appGetProperties(dxid); }
    void setProperties() { appSetProperties(dxid); }
    void addTypes() { appAddTypes(dxid); }
    void removeTypes() { appRemoveTypes(dxid); }
    void destroy() { appDestroy(dxid); }

    // App-specific functions

    void createFromFile(string codefile);
    void createFromString(string codestring);
    void run();
  };
}

#endif
