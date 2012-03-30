#include "dxcollection.h"

using namespace std;
using namespace dx;

void DXCollection::create(const JSON &to_store) {
}

JSON DXCollection::get() const {
  return JSON();
}

void DXCollection::set(const JSON &to_store) const {
}


DXCollection DXCollection::newDXCollection(const JSON &to_store) {
  DXCollection dxcollection;
  dxcollection.create(to_store);
  return dxcollection;
}
