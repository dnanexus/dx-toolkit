#include "dxtable.h"

using namespace std;
using namespace dx;

void DXTable::create(const JSON &to_store) {
}

DXTable DXTable::newDXTable(const JSON &to_store) {
  DXTable dxtable;
  dxtable.create(to_store);
  return dxtable;
}
