#include "dxgtable.h"
#include <boost/lexical_cast.hpp>

using namespace std;
using namespace dx;

void DXGTable::setID(const std::string &dxid) {
  if (row_buffer_.length() > 0)
    flush();

  part_index_ = 0;

  DXDataObject::setIDs(dxid);
}

void DXGTable::create(const JSON &columns) {
  JSON input_params(JSON_OBJECT);
  input_params["columns"] = columns;

  const JSON resp = gtableNew(input_params);

  setID(resp["id"].get<string>());
}

void DXGTable::create(const JSON &columns,
		     const string &chr_col,
		     const string &lo_col,
		     const string &hi_col) {
  JSON input_params(JSON_OBJECT);
  input_params["columns"] = columns;
  input_params["index"] = chr_col + "." + lo_col + "." + hi_col;

  const JSON resp = gtableNew(input_params);

  setID(resp["id"].get<string>());
}

DXGTable DXGTable::extend(const JSON &columns) const {
  JSON input_params(JSON_OBJECT);
  input_params["columns"] = columns;

  const JSON resp = gtableExtend(dxid_, input_params);

  return DXGTable(resp["id"].get<string>());
}

JSON DXGTable::getRows(const JSON &column_names, const int starting, const int limit) const {
  JSON input_params(JSON_OBJECT);
  if (column_names.type() == JSON_ARRAY)
    input_params["columns"] = column_names;
  if (starting >= 0)
    input_params["starting"] = starting;
  if (limit >= 0)
    input_params["limit"] = limit;

  return gtableGet(dxid_, input_params);
}

JSON DXGTable::getRows(const string &chr, const int lo, const int hi,
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

  return gtableGet(dxid_, input_params);
}

void DXGTable::addRows(const JSON &data, int index) {
  JSON input_params(JSON_OBJECT);
  input_params["data"] = data;
  input_params["index"] = index;
  gtableAddRows(dxid_, input_params);
}

// For automatic index generation
void DXGTable::addRows(const JSON &data) {
  for (JSON::const_array_iterator iter = data.array_begin();
       iter != data.array_end();
       iter++) {
    row_buffer_.push_back(*iter);

    if (row_buffer_.length() >= row_buffer_maxsize_)
      flush();
  }
}

int DXGTable::getUnusedPartIndex() {
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

void DXGTable::flush() {
  JSON input_params(JSON_OBJECT);
  input_params["data"] = row_buffer_;
  input_params["index"] = getUnusedPartIndex();

  tableAddRows(dxid_, input_params);

  row_buffer_ = JSON(JSON_ARRAY);
}

void DXGTable::close(const bool block) {
  if (row_buffer_.length() > 0)
    flush();
  tableClose(dxid_);

  if (block)
    waitOnState();
}

void DXGTable::waitOnClose() const {
  waitOnState();
}

DXGTable DXGTable::openDXGTable(const string &dxid) {
  return DXGTable(dxid);
}

DXGTable DXGTable::newDXGTable(const JSON &columns) {
  DXGTable table;
  table.create(columns);
  return table;
}

DXGTable DXGTable::newDXGTable(const JSON &columns,
			    const string &chr_col,
			    const string &lo_col,
			    const string &hi_col) {
  DXGTable table;
  table.create(columns, chr_col, lo_col, hi_col);
  return table;
}

DXGTable DXGTable::extendDXGTable(const string &dxid, const JSON &columns) {
  DXGTable table(dxid);
  return table.extend(columns);
}

JSON DXGTable::columnDesc(const string &name,
		const string &type) {
  string col_desc = name + ":" + type;
  return JSON(col_desc);
}
