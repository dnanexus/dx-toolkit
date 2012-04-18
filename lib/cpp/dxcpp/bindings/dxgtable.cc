#include "dxtable.h"
#include <boost/lexical_cast.hpp>

using namespace std;
using namespace dx;

void DXTable::setID(const std::string &dxid) {
  if (row_buffer_.length() > 0)
    flush();

  part_index_ = 0;

  DXClass::setID(dxid);
}

void DXTable::create(const JSON &columns) {
  JSON input_params(JSON_OBJECT);
  input_params["columns"] = columns;

  const JSON resp = tableNew(input_params);

  setID(resp["id"].get<string>());
}

void DXTable::create(const JSON &columns,
		     const string &chr_col,
		     const string &lo_col,
		     const string &hi_col) {
  JSON input_params(JSON_OBJECT);
  input_params["columns"] = columns;
  input_params["index"] = chr_col + "." + lo_col + "." + hi_col;

  const JSON resp = tableNew(input_params);

  setID(resp["id"].get<string>());
}

DXTable DXTable::extend(const JSON &columns) const {
  JSON input_params(JSON_OBJECT);
  input_params["columns"] = columns;

  const JSON resp = tableExtend(dxid_, input_params);

  return DXTable(resp["id"].get<string>());
}

JSON DXTable::getRows(const JSON &column_names, const int starting, const int limit) const {
  JSON input_params(JSON_OBJECT);
  if (column_names.type() == JSON_ARRAY)
    input_params["columns"] = column_names;
  if (starting >= 0)
    input_params["starting"] = starting;
  if (limit >= 0)
    input_params["limit"] = limit;

  return tableGet(dxid_, input_params);
}

JSON DXTable::getRows(const string &chr, const int lo, const int hi,
		      const JSON &column_names, const int starting, const int limit) const {
  JSON input_params(JSON_OBJECT);
  if (column_names.type() == JSON_ARRAY)
    input_params["columns"] = column_names;
  if (starting >= 0)
    input_params["starting"] = starting;
  if (limit >= 0)
    input_params["limit"] = limit;
  input_params["query"] = JSON(JSON_ARRAY);
  input_params["query"].push_back(chr);
  input_params["query"].push_back(lo);
  input_params["query"].push_back(hi);

  return tableGet(dxid_, input_params);
}

void DXTable::addRows(const JSON &data, int index) {
  JSON input_params(JSON_OBJECT);
  input_params["data"] = data;
  input_params["index"] = index;
  tableAddRows(dxid_, input_params);
}

// For automatic index generation
void DXTable::addRows(const JSON &data) {
  for (JSON::const_array_iterator iter = data.array_begin();
       iter != data.array_end();
       iter++) {
    row_buffer_.push_back(*iter);

    if (row_buffer_.length() >= row_buffer_maxsize_)
      flush();
  }
}

int DXTable::getUnusedPartIndex() {
  const JSON desc = describe();
  if (desc["parts"].length() == 250000)
    throw DXGTableError();//"250000 part indices already used."

  do {
    part_index_++;
    if (!desc["parts"].has(boost::lexical_cast<string>(part_index_)))
      return part_index_;
  } while (part_index_ < 250000);

  throw DXGTableError();//"Usable part index not found."
}

void DXTable::flush() {
  JSON input_params(JSON_OBJECT);
  input_params["data"] = row_buffer_;
  input_params["index"] = getUnusedPartIndex();

  tableAddRows(dxid_, input_params);

  row_buffer_ = JSON(JSON_ARRAY);
}

void DXTable::close(const bool block) {
  if (row_buffer_.length() > 0)
    flush();
  tableClose(dxid_);

  if (block)
    waitOnState();
}

void DXTable::waitOnClose() const {
  waitOnState();
}

DXTable DXTable::openDXTable(const string &dxid) {
  return DXTable(dxid);
}

DXTable DXTable::newDXTable(const JSON &columns) {
  DXTable table;
  table.create(columns);
  return table;
}

DXTable DXTable::newDXTable(const JSON &columns,
			    const string &chr_col,
			    const string &lo_col,
			    const string &hi_col) {
  DXTable table;
  table.create(columns, chr_col, lo_col, hi_col);
  return table;
}

DXTable DXTable::extendDXTable(const string &dxid, const JSON &columns) {
  DXTable table(dxid);
  return table.extend(columns);
}

JSON DXTable::columnDesc(const string &name,
		const string &type) {
  string col_desc = name + ":" + type;
  return JSON(col_desc);
}
