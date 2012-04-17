#ifndef DXCPP_BINDINGS_DXAPP_H
#define DXCPP_BINDINGS_DXAPP_H

#include "../bindings.h"

class DXApp: public DXClass {
 public:
  dx::JSON describe() const { return appDescribe(dxid_); }
  dx::JSON getProperties(const dx::JSON &keys) const { return appGetProperties(dxid_, keys); }
  void setProperties(const dx::JSON &properties) const { appSetProperties(dxid_, properties); }
  void addTypes(const dx::JSON &types) const { appAddTypes(dxid_, types); }
  void removeTypes(const dx::JSON &types) const { appRemoveTypes(dxid_, types); }
  void destroy() { appDestroy(dxid_); }

  // App-specific functions

  void createFromFile(const std::string &codefile) const;
  void createFromString(const std::string &codestring) const;
  void run() const;
};

#endif
