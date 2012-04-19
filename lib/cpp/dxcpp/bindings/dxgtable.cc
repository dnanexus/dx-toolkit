#include "dxgtable.h"
#include <boost/lexical_cast.hpp>

using namespace std;
using namespace dx;

void DXGTable::reset_buffer_() {
  row_buffer_.str("{\"data\": [");
}

void DXGTable::setIDs(const std::string &dxid,
		      const std::string &proj) {
  flush();

  part_id_ = 0;

  DXDataObject::setIDs(dxid, proj);
}

void DXGTable::create(const vector<JSON> &columns,
                      const vector<JSON> &indices,
		      const JSON &data_obj_fields) {
  JSON input_params = data_obj_fields;
  if (!data_obj_fields.has("project"))
    input_params["project"] = g_WORKSPACE_ID;
  input_params["columns"] = columns;

  const JSON resp = gtableNew(input_params);

  setIDs(resp["id"].get<string>(), input_params["project"].get<string>());
}

DXGTable DXGTable::extend(const vector<JSON> &columns,
                          const vector<JSON> &indices,
                          const JSON &data_obj_fields) const {
  JSON input_params(JSON_OBJECT);
  input_params["columns"] = columns;

  const JSON resp = gtableExtend(dxid_, input_params);

  return DXGTable(resp["id"].get<string>(), input_params["project"].get<string>());
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

void DXGTable::addRows(const JSON &data, int part_id) {
  JSON input_params(JSON_OBJECT);
  input_params["data"] = data;
  input_params["index"] = part_id;
  gtableAddRows(dxid_, input_params);
}

// For automatic index generation
void DXGTable::addRows(const JSON &data) {
  for (JSON::const_array_iterator iter = data.array_begin();
       iter != data.array_end();
       iter++) {
    row_buffer_ << (*iter).toString() << ",";

    if (row_buffer_.tellp() >= row_buffer_maxsize_)
      flush();
  }
}

int DXGTable::getUnusedPartID() {
  const JSON desc = describe();
  if (desc["parts"].length() == 250000)
    throw DXGTableError();//"250000 part indices already used."

  do {
    part_id_++;
    if (!desc["parts"].has(boost::lexical_cast<string>(part_id_)))
      return part_id_;
  } while (part_id_ < 250000);

  throw DXGTableError();//"Usable part index not found."
}

void DXGTable::flush() {
  int pos = row_buffer_.tellp();
  if (pos > 10) {
    row_buffer_.seekp(pos - 1); // Erase the trailing comma
    row_buffer_ << "], \"index\": " << getUnusedPartID() << "}";

    tableAddRows(dxid_, row_buffer_.str());

    reset_buffer_();
  }
}

void DXGTable::close(const bool block) {
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

DXGTable DXGTable::newDXGTable(const vector<JSON> &columns,
                               const vector<JSON> &indices,
                               const JSON &data_obj_fields) {
  DXGTable table;
  table.create(columns, indices, data_obj_fields);
  return table;
}

DXGTable DXGTable::extendDXGTable(const string &dxid,
                                  const vector<JSON> &columns,
                                  const vector<JSON> &indices,
                                  const JSON &data_obj_fields) {
  DXGTable table(dxid);
  return table.extend(columns, indices, data_obj_fields);
}

JSON DXGTable::columnDesc(const string &name,
                          const string &type,
                          const int &length) {
  JSON col_desc(JSON_OBJECT);
  col_desc["name"] = name;
  col_desc["type"] = type;
  if (type == "string")
    col_desc["length"] = length;
  return col_desc;
}

JSON DXGTable::genomicRangeIndex(const string &chr,
                                 const string &lo,
                                 const string &hi,
                                 const string &name) {
  JSON index_desc(JSON_OBJECT);
  index_desc["name"] = name;
  index_desc["type"] = "genomic";
  index_desc["chr"] = chr;
  index_desc["lo"] = lo;
  index_desc["hi"] = hi;
  return index_desc;
}

JSON DXGTable::lexicographicIndex(const vector<vector<string> > &columns,
                                  const string &name) {
  JSON index_desc(JSON_OBJECT);
  index_desc["name"] = name;
  index_desc["type"] = "lexicographic";
  index_desc["columns"] = columns;
  return index_desc;
}

JSON DXGTable::substringIndex(const string &column, const string &name) {
  JSON index_desc(JSON_OBJECT);
  index_desc["name"] = name;
  index_desc["type"] = "substring";
  index_desc["column"] = column;
  return index_desc;
}

JSON DXGTable::genomicRangeQuery(const std::string &chr,
                                 const int lo,
                                 const int hi,
                                 const std::string &mode,
                                 const std::string &index) {
  JSON query(JSON_OBJECT);
  query["index"] = index;
  query["parameters"] = JSON(JSON_OBJECT);
  query["parameters"]["mode"] = mode;
  query["parameters"]["coords"] = JSON(JSON_ARRAY);
  query["parameters"]["coords"].push_back(chr);
  query["parameters"]["coords"].push_back(lo);
  query["parameters"]["coords"].push_back(hi);
  return query;
}

JSON DXGTable::lexicographicQuery(const JSON &mongo_query,
                                  const string &index) {
  JSON query(JSON_OBJECT);
  query["index"] = index;
  query["parameters"] = mongo_query;
  return query;
}

JSON DXGTable::substringQuery(const string &match,
                              const string &mode,
                              const string &index) {
  JSON query(JSON_OBJECT);
  query["index"] = index;
  query["parameters"] = JSON(JSON_OBJECT);
  if (mode == "equal")
    query["parameters"]["$eq"] = match;
  else if (mode == "substring")
    query["parameters"]["$substr"] = match;
  else if (mode == "prefix")
    query["parameters"]["$prefix"] = match;
  else
    throw DXGTableError("Unrecognized substring index query mode: " + mode);
  return query;
}
