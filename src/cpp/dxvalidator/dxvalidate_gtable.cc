// Copyright (C) 2013-2015 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may
//   not use this file except in compliance with the License. You may obtain a
//   copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//   License for the specific language governing permissions and limitations
//   under the License.

#include "dxvalidate_gtable.h"
#include "dxcpp/dxcpp.h"

using namespace dx;

bool GTableValidator::processColumns() { 
  columns->Init();
  columns->Add(desc["columns"]);
  queryColumns = columns->getQueryColumns();

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

  cols = columns->getColumnList(4);
  if (cols.size() > 0) {
    msg->setData(cols, 0);
    return msg->setError("COLUMNS_FORBIDDEN", true);
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

bool GTableValidator::validateData() {
  cerr << "Total rows " << numRows << endl;

  setRowValidator();
  if (! rowV->isReady()) {
    delete rowV;
    return false;
  }
  
  table.startLinearQuery(queryColumns);
  int64_t offset = 0;
  int count = 0;
  
  JSON data;
  try {
    while (table.getNextChunk(data)) {
      for (int i = 0; i < data.size(); i++) {
        msg->setRowIndex(offset + i);
        if (! rowV->validateRow(data[i])) {
          delete rowV;
          return false;
        }
      }
      
      offset += data.size();
      count ++;
      if ( (count % 10) == 0)  cerr << offset << "\n";
    }
    
    rowV->finalValidate();
    table.stopLinearQuery();
  } catch(DXError &e) {
    delete rowV;
    return msg->setDXError(e.msg, "GTABLE_FETCH_FAIL");
  }
  
  delete rowV;
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
