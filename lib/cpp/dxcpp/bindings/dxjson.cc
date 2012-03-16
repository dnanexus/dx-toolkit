#include "dxjson.h"

void DXJSON::create(const JSON &to_store) {
  this->setID("json-12345678901234567890abcd");
}

JSON DXJSON::get() const {
  return JSON();
}

void DXJSON::set(const JSON &to_store) const {
}


DXJSON newDXJSON(const JSON &to_store) {
  DXJSON dxjson;
  dxjson.create(to_store);
  return dxjson;
}
