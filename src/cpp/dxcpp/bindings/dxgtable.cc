#include "dxgtable.h"
#include <boost/lexical_cast.hpp>

using namespace std;
using namespace dx;

void DXGTable::reset_buffer_() {
  row_buffer_.str("");
  row_buffer_ << "{\"data\": [";
}

void DXGTable::setIDs(const string &dxid,
		      const string &proj) {
  flush();

  DXDataObject::setIDs(dxid, proj);
}

void DXGTable::create(const vector<JSON> &columns,
                      const vector<JSON> &indices,
		      const JSON &data_obj_fields) {
  JSON input_params = data_obj_fields;
  if (!data_obj_fields.has("project"))
    input_params["project"] = g_WORKSPACE_ID;
  input_params["columns"] = columns;
  if (indices.size() > 0) {
    input_params["indices"] = indices;
  }

  const JSON resp = gtableNew(input_params);

  setIDs(resp["id"].get<string>(), input_params["project"].get<string>());
}

void DXGTable::create(const DXGTable &init_from,
		      const JSON &data_obj_fields) {
  JSON input_params = data_obj_fields;
  input_params["initializeFrom"] = JSON(JSON_OBJECT);
  input_params["initializeFrom"]["id"] = init_from.getID();
  input_params["initializeFrom"]["project"] = init_from.getProjectID();
  if (!data_obj_fields.has("project")) {
    input_params["project"] = g_WORKSPACE_ID;
  }

  const JSON resp = gtableNew(input_params);

  setIDs(resp["id"].get<string>(), input_params["project"].get<string>());
}

void DXGTable::create(const DXGTable &init_from,
		      const vector<JSON> &columns,
		      const JSON &data_obj_fields) {
  JSON input_params = data_obj_fields;
  input_params["initializeFrom"] = JSON(JSON_OBJECT);
  input_params["initializeFrom"]["id"] = init_from.getID();
  input_params["initializeFrom"]["project"] = init_from.getProjectID();
  if (!data_obj_fields.has("project")) {
    input_params["project"] = g_WORKSPACE_ID;
  }
  input_params["columns"] = columns;

  const JSON resp = gtableNew(input_params);

  setIDs(resp["id"].get<string>(), input_params["project"].get<string>());
}

void DXGTable::create(const DXGTable &init_from,
		      const vector<JSON> &columns,
		      const vector<JSON> &indices,
		      const JSON &data_obj_fields) {
  JSON input_params = data_obj_fields;
  input_params["initializeFrom"] = JSON(JSON_OBJECT);
  input_params["initializeFrom"]["id"] = init_from.getID();
  input_params["initializeFrom"]["project"] = init_from.getProjectID();
  if (!data_obj_fields.has("project")) {
    input_params["project"] = g_WORKSPACE_ID;
  }
  if (columns.size() > 0) {
    input_params["columns"] = columns;
  }
  input_params["indices"] = indices;

  const JSON resp = gtableNew(input_params);

  setIDs(resp["id"].get<string>(), input_params["project"].get<string>());
}

DXGTable DXGTable::extend(const vector<JSON> &columns,
                          const vector<JSON> &indices,
                          const JSON &data_obj_fields) const {
  JSON input_params = data_obj_fields;
  if (!data_obj_fields.has("project"))
    input_params["project"] = g_WORKSPACE_ID;
  input_params["columns"] = columns;
  if (indices.size() > 0) {
    input_params["indices"] = indices;
  }

  const JSON resp = gtableExtend(dxid_, input_params);

  return DXGTable(resp["id"].get<string>(), input_params["project"].get<string>());
}

JSON DXGTable::getRows(const JSON &query, const JSON &column_names,
                       const int64_t starting, const int64_t limit) const {
  JSON input_params(JSON_OBJECT);
  if (query.type() != JSON_NULL)
    input_params["query"] = query;
  if (column_names.type() == JSON_ARRAY)
    input_params["columns"] = column_names;
  if (starting >= 0)
    input_params["starting"] = starting;
  if (limit >= 0)
    input_params["limit"] = limit;

  return gtableGet(dxid_, input_params);
}

void DXGTable::addRows(const JSON &data, int part_id) {
  JSON input_params(JSON_OBJECT);
  input_params["data"] = data;
  input_params["part"] = part_id;
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
  const JSON resp = gtableNextPart(dxid_);
  return resp["part"].get<int>();
}

void DXGTable::flush() {
  int64_t pos = row_buffer_.tellp();
  if (pos > 10) {
    row_buffer_.seekp(pos - 1); // Erase the trailing comma
    row_buffer_ << "], \"part\": " << getUnusedPartID() << "}";

    gtableAddRows(dxid_, row_buffer_.str());

    reset_buffer_();
  }
}

void DXGTable::close(const bool block) {
  flush();
  gtableClose(dxid_);

  if (block)
    waitOnState();
}

void DXGTable::waitOnClose() const {
  waitOnState();
}

DXGTable DXGTable::openDXGTable(const string &dxid,
				const string &proj) {
  return DXGTable(dxid, proj);
}

DXGTable DXGTable::newDXGTable(const vector<JSON> &columns,
                               const vector<JSON> &indices,
                               const JSON &data_obj_fields) {
  DXGTable gtable;
  gtable.create(columns, indices, data_obj_fields);
  return gtable;
}

DXGTable DXGTable::newDXGTable(const DXGTable &init_from,
			       const JSON &data_obj_fields) {
  DXGTable gtable;
  gtable.create(init_from, data_obj_fields);
  return gtable;
}

DXGTable DXGTable::newDXGTable(const DXGTable &init_from,
                               const vector<JSON> &columns,
                               const JSON &data_obj_fields) {
  DXGTable gtable;
  gtable.create(init_from, columns, data_obj_fields);
  return gtable;
}

DXGTable DXGTable::newDXGTable(const DXGTable &init_from,
                               const vector<JSON> &columns,
                               const vector<JSON> &indices,
                               const JSON &data_obj_fields) {
  DXGTable gtable;
  gtable.create(init_from, columns, indices, data_obj_fields);
  return gtable;
}

DXGTable DXGTable::extendDXGTable(const DXGTable &dxgtable,
                                  const vector<JSON> &columns,
                                  const vector<JSON> &indices,
                                  const JSON &data_obj_fields) {
  return dxgtable.extend(columns, indices, data_obj_fields);
}

JSON DXGTable::columnDesc(const string &name,
                          const string &type) {
  JSON col_desc(JSON_OBJECT);
  col_desc["name"] = name;
  col_desc["type"] = type;
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

JSON DXGTable::genomicRangeQuery(const string &chr,
                                 const int64_t lo,
                                 const int64_t hi,
                                 const string &mode,
                                 const string &index) {
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

DXGTable DXGTable::clone(const string &dest_proj_id,
                         const string &dest_folder) const {
  clone_(dest_proj_id, dest_folder);
  return DXGTable(dxid_, dest_proj_id);
}
