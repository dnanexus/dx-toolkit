#include "dxrecord.h"

using namespace std;
using namespace dx;

void DXRecord::create() {
  string input_hash = "{\"project\":\"project-000000000000000000000001\"}";
  const JSON resp = recordNew(input_hash);
  setIDs(resp["id"].get<string>(), "default");
}

DXRecord DXRecord::newDXRecord() {
  DXRecord dxrecord;
  dxrecord.create();
  return dxrecord;
}
