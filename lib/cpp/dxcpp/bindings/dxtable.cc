#include "dxtable.h"

void DXTable::create(const JSON &columns) {
}

void DXTable::create(const JSON &columns,
		     const string &chr_col,
		     const string &lo_col,
		     const string &hi_col) {
  string input = "{}";
  JSON resp = tableNew(input);
}

DXTable DXTable::extend(const JSON &columns) const {
  return DXTable();
}

JSON DXTable::getRows(const string &chr, const int lo, const int hi) const {
  return JSON();
}

void DXTable::addRows(const JSON &data, int index) {
}

// For automatic index generation
void DXTable::addRows(const JSON &data) {
}

/**
 * Attempts to close the remote table.
 * @param block if true, waits until the table has finished closing before returning
 */
void DXTable::close(const bool block) const {
}

/**
 * Waits until the remote table has finished closing.
 */
void DXTable::waitOnClose() const {
  waitOnState();
}

DXTable openDXTable(const string &dxid) {
  return DXTable();
}

DXTable newDXTable(const JSON &columns) {
  return DXTable();
}

DXTable newDXTable(const JSON &columns,
		   const string &chr_col,
		   const string &lo_col,
		   const string &hi_col) {
  return DXTable();
}

DXTable extendDXTable(const string &dxid, const JSON &columns) {
  return DXTable();
}
