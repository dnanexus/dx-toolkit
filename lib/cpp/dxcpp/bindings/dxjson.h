#ifndef DXCPP_BINDINGS_DXJSON_H
#define DXCPP_BINDINGS_DXJSON_H

#include "../bindings.h"

class DXJSON: public DXClass {
 public:
  JSON describe() const { return jsonDescribe(dxid_); }
  JSON getProperties(const JSON &keys=JSON()) const { return jsonGetProperties(dxid_, keys); }
  void setProperties(const JSON &properties) const { jsonSetProperties(dxid_, properties); }
  void addTypes(const JSON &types) const { jsonAddTypes(dxid_, types); }
  void removeTypes(const JSON &types) const { jsonRemoveTypes(dxid_, types); }
  void destroy() { jsonDestroy(dxid_); }

  // JSON-specific functions

  DXJSON() {}
  DXJSON(const string &dxid) { this->setID(dxid); }
  void create(const JSON &to_store);
  JSON get() const;
  void set(const JSON &to_store) const;
};

DXJSON newDXJSON(const JSON &to_store);

#endif
