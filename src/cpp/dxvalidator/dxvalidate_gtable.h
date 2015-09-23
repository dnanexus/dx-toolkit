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

#ifndef DX_GTABLE_VALIDATE_H
#define DX_GTABLE_VALIDATE_H

#include "dxvalidate_tools.h"
#include "dxcpp/dxcpp.h"

namespace dx {
  /** Basic class for validating a row in a gtable. To build a validator for a particular
    * type, extend this class and override function validateRow and finalValidate. Where
    *   a. validateRow: Validate a row based on the information of the row itself and stored
    *      information from previous fetched rows
    *   b. finalValidate: Shall be called when all rows are fetched. It may be used to
    *      perform validation require information from all rows.
    */
  class GTableRowValidator {
    protected:
      ValidateInfo *msg;
      bool ready;

    public:
      GTableRowValidator(ValidateInfo *m) : msg(m), ready(true) {};
      bool isReady() { return ready; }
      virtual bool validateRow(const JSON &row) { return row.type() != dx::JSON_NULL; }
      virtual bool finalValidate() { return true; }
  };

  /** Basic GTable validator. It validates a DNAnexus gtable object in the following steps
    * 1. fetchHead(): Fetch description and details of this gtable
    * 2. validateTypes(): Validate the types of this gtable with the designated TypesHandler
    * 3. validateDetails(): Validate the details of this gtable
    * 4. validateColumns(): Validate the schema of columns of this gtable with the designated
    *                       ColumnsHandler
    * 5. validateData(): Validate the actual content of this gtable
    *
    * To validate a child type of gtable, one shall
    * a. If needed, implement a particular TypesHandler by inheriting the basic TypesHandler
    * b. If needed, implement a particular ColumnsHandler by inheriting the basic ColumnsHandler
    * c. Inherite this class and override any the of above validate functions if needed
    * d. Implement a particular GTableRowValidator by inheriting the basic GTableRowValidator
    *    and overriding validateRow() and/or finalValidate() if needed
    */
  class GTableValidator {
    private:
      // Fetch description and details of source_id
      bool fetchHead(const string &source_id);

    protected:
      int64_t numRows; // Total number of rows in the gtable
      JSON desc, details, queryColumns;
      DXGTable table;
      
      TypesHandler types; 
      ValidateInfo *msg;
      ColumnsHandler *columns;
      GTableRowValidator *rowV;
      
      bool processColumns();

      // Set the proper row validator
      virtual void setRowValidator() { rowV = new GTableRowValidator(msg); }
      
      virtual bool validateTypes() { 
        if (types.HasDuplicate()) msg->addWarning("TYPE_DUPLICATE");
        return true;
      }

      virtual bool validateDetails() { return true; }
      virtual bool validateColumns();
      virtual bool validateData();

      /** Fetch individual rows and call rowV->validateRow() to validate each one
        * and call rowV->finalValidate() to perform all validation requires information from
        * all rows
        */
      virtual void Validate(const string &source_id);

    public:
      GTableValidator(){}

      // Public function perform the validation
      virtual JSON Validate(const string &source_id, ValidateInfo *m);
  };

  class GTableErrorMsg : public virtual ErrorMsg {
    public:
      GTableErrorMsg() {
        // Add error and warning messages for gtable
        errorMsg["OBJECT_INVALID"] = "Cannot find source object";
        errorMsg["GTABLE_FETCH_FAIL"] = "Internal error: {1}. Fail to fetch the description, details, or content of the object";
        errorMsg["CLASS_NOT_GTABLE"] = "Object is not a gtable";
        errorMsg["DETAILS_NOT_HASH"] = "'Details' of this object is not a hash";
        errorMsg["GTABLE_NOT_CLOSED"] = "Object is not closed";
        
        errorMsg["COLUMNS_MISSING"] = "Following columns are missing: ({1})";
        errorMsg["COLUMNS_INVALID_TYPES"] = "Following columns have wrong types (The proper type of each column is in the bracket): ({1})"; 
        errorMsg["COLUMNS_FORBIDDEN"] = "Following columns are forbidden: ({1})";

        warningMsg["COLUMNS_NOT_RECOGNIZED"] = "Following columns are not recognized: ({1})";
        warningMsg["COLUMNS_MISSING"] = "Following columns are missing: ({1})";
        warningMsg["TYPE_DUPLICATE"] = "Object has duplicated types";
      }
  };
}

#endif
