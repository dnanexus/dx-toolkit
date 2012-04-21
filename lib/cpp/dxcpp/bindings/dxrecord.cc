#include "dxrecord.h"

using namespace std;
using namespace dx;

void DXRecord::create(const JSON &data_obj_fields) {
  JSON input_params = data_obj_fields;
  if (!data_obj_fields.has("project"))
    input_params["project"] = g_WORKSPACE_ID;
  const JSON resp = recordNew(input_params);
  setIDs(resp["id"].get<string>(), input_params["project"].get<string>());
}

DXRecord DXRecord::newDXRecord(const JSON &data_obj_fields) {
  DXRecord dxrecord;
  dxrecord.create(data_obj_fields);
  return dxrecord;
}

DXRecord DXRecord::clone(const string &dest_proj_id,
                         const string &dest_folder) const {
  clone_(dest_proj_id, dest_folder);
  return DXRecord(dxid_, dest_proj_id);
}
