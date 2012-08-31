#include "dxvalidate_gtable.h"
#include "dxcpp/dxcpp.h"

using namespace dx;

bool GTableValidator::processColumns() { 
  columns->Init();
  columns->Add(desc["columns"]);

  string cols = columns->getColumnList(0);
  if (cols.size() > 0) {
    msg->setData(cols, 0);
    return msg->setError("COLUMNS_MISSING", true);
  }

  cols = columns->getColumnList(1);
  if (cols.size() > 0) {
    msg->setData(cols, 0);
    msg->addWarning("COLUMNS_MISSING", true);
  }

  cols = columns->getColumnList(2);
  if (cols.size() > 0) {
    msg->setData(cols, 0);
    return msg->setError("COLUMNS_INVALID_TYPES", true);
  }

  cols = columns->getColumnList(3);
  if (cols.size() > 0) {
    msg->setData(cols, 0);
    msg->addWarning("COLUMNS_NOT_RECOGNIZED", true);
  }

  return true;
}

bool GTableValidator::validateColumns() {
  columns = new ColumnsHandler();
  bool ret_val = processColumns();
  delete columns;
  return ret_val;
}

bool GTableValidator::fetchHead(const string &source_id) {
  table.setIDs(source_id);
  try {
    desc = table.describe();
    details = table.getDetails();
  } catch (DXAPIError &e) {
    if (e.resp_code == 404) {
      msg->setError("OBJECT_INVALID");
    } else {
      msg->setDXError(e.msg, "GTABLE_FETCH_FAIL");
    }
    return false;
  }

  
  if (desc["class"].get<string>() != "gtable") return msg->setError("CLASS_NOT_GTABLE");
  if (desc["state"].get<string>() != "closed") return msg->setError("GTABLE_NOT_CLOSED");
  if (details.type() != JSON_OBJECT) return msg->setError("DETAILS_NOT_HASH");
  
  types.Add(desc["types"]); 
  numRows = int64_t(desc["length"]);

  return true;
}

void GTableValidator::Validate(const string &source_id) {
  if (! fetchHead(source_id)) return;
  if (! validateTypes()) return;
  if (! validateDetails()) return;
  if (! validateColumns()) return;
  
  validateData();
}

JSON GTableValidator::Validate(const string &source_id, ValidateInfo *m) {
  msg = m;
  msg->setString("sourceId", source_id);

  Validate(source_id);

  return msg->getInfo();
}
