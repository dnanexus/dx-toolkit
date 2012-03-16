#include "dxcollection.h"

void DXCollection::create(const JSON &to_store) {
}

JSON DXCollection::get() const {
  return JSON();
}

void DXCollection::set(const JSON &to_store) const {
}


DXCollection newDXCollection(const JSON &to_store) {
  DXCollection dxcollection;
  dxcollection.create(to_store);
  return dxcollection;
}
