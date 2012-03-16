#ifndef DXCPP_BINDINGS_DXTABLE_H
#define DXCPP_BINDINGS_DXTABLE_H

#include "../bindings.h"
#include "../api.h"

/**
 * @brief Remote table handler
 *
 */
class DXTable: public DXClass {
 public:
  /** Describes the object.
   * @see DXClass::describe()
   */
  JSON describe() const { return tableDescribe(dxid_); }
  JSON getProperties(const JSON &keys) const { return tableGetProperties(dxid_, keys); }
  void setProperties(const JSON &properties) const { tableSetProperties(dxid_, properties); }
  void addTypes(const JSON &types) const { tableAddTypes(dxid_, types); }
  void removeTypes(const JSON &types) const { tableRemoveTypes(dxid_, types); }
  void destroy() { tableDestroy(dxid_); }

  // Table-specific functions

  /**
   * 
   */
  void create(const JSON &columns);
  void create(const JSON &columns,
	      const string &chr_col,
	      const string &lo_col,
	      const string &hi_col);

  DXTable extend(const JSON &columns) const;

  JSON getRows(const string &chr, const int lo, const int hi) const;
  void addRows(const JSON &data, int index);
  void addRows(const JSON &data); // For automatic index generation

  /**
   * Attempts to close the remote table.
   * @param block if true, waits until the table has finished closing before returning
   */
  void close(const bool block=false) const;

  /**
   * Waits until the remote table has finished closing.
   */
  void wait_on_close() const;
};

DXTable openDXTable(const string &dxid);

DXTable newDXTable(const JSON &columns);

DXTable newDXTable(const JSON &columns,
		   const string &chr_col,
		   const string &lo_col,
		   const string &hi_col);

DXTable extendDXTable(const string &dxid, const JSON &columns);

#endif
