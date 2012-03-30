#include "dxrecord.h"

using namespace std;
using namespace dx;

void DXRecord::create(const JSON &to_store) {
  setID("record-12345678901234567890abcd");
}

JSON DXRecord::get() const {
  return JSON();
}

void DXRecord::set(const JSON &to_store) const {
}


DXRecord DXRecord::newDXRecord(const JSON &to_store) {
  DXRecord dxrecord;
  dxrecord.create(to_store);
  return dxrecord;
}
