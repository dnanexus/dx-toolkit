#include "dxrecord.h"

using namespace std;
using namespace dx;

void DXRecord::create(const JSON &to_store) {
  JSON resp = jsonNew(to_store);
  setID(resp["id"].get<string>());
}

JSON DXRecord::get() const {
  return jsonGet(dxid_);
}

void DXRecord::set(const JSON &to_store) const {
  jsonSet(dxid_, to_store);
}


DXRecord DXRecord::newDXRecord(const JSON &to_store) {
  DXRecord dxrecord;
  dxrecord.create(to_store);
  return dxrecord;
}
