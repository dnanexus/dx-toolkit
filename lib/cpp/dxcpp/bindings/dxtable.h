#ifndef DXCPP_BINDINGS_DXTABLE_H
#define DXCPP_BINDINGS_DXTABLE_H

#import "bindings.h"

namespace dxpy {
  using namespace dxpy;

  class DXTable: public DXClass {
  public:
    JSON describe() { return tableDescribe(dxid); }
    JSON getProperties() { return tableGetProperties(dxid); }
    void setProperties() { tableSetProperties(dxid); }
    void addTypes() { tableAddTypes(dxid); }
    void removeTypes() { tableRemoveTypes(dxid); }
    void destroy() { tableDestroy(dxid); }

    // Table-specific functions

    void create(JSON columns, string chr_col, string lo_col, string hi_col);

    DXTable extend(JSON columns);

    JSON getRows();
    void addRows(JSON data, int index);
    void addRows(JSON data); // For automatic index generation

    void close(bool block=false);
    void wait_on_close();
  };

  DXTable openDXTable(string dxid);

  DXTable newDXTable(JSON columns, string chr_col, string lo_col, string hi_col);

  DXTable extendDXTable(string dxid, JSON columns);
}

#endif
