#ifndef DXCPP_BINDINGS_DXAPP_H
#define DXCPP_BINDINGS_DXAPP_H

#include "../bindings.h"

class DXApp: public DXClass {
 public:
  JSON describe() const { return appDescribe(dxid_); }
  JSON getProperties(const JSON &keys) const { return appGetProperties(dxid_, keys); }
  void setProperties(const JSON &properties) const { appSetProperties(dxid_, properties); }
  void addTypes(const JSON &types) const { appAddTypes(dxid_, types); }
  void removeTypes(const JSON &types) const { appRemoveTypes(dxid_, types); }
  void destroy() { appDestroy(dxid_); }

  // App-specific functions

  void createFromFile(const string &codefile) const;
  void createFromString(const string &codestring) const;
  void run() const;
};

#endif
