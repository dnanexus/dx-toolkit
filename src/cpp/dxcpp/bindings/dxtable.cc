#include "dxtable.h"

using namespace std;
using namespace dx;

void DXTable::create() {
}

DXTable DXTable::newDXTable() {
  DXTable dxtable;
  dxtable.create();
  return dxtable;
}

DXTable DXTable::clone(const string &dest_proj_id,
                         const string &dest_folder) const {
  clone_(dest_proj_id, dest_folder);
  return DXTable(dxid_, dest_proj_id);
}
