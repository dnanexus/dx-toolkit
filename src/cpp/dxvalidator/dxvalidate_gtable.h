#ifndef DX_GTABLE_VALIDATE_H
#define DX_GTABLE_VALIDATE_H

#include "dxvalidate_tools.h"
#include "dxcpp/dxcpp.h"

namespace dx {
  class GTableValidator {
    private:
      bool fetchHead(const string &source_id);

    protected:
      int64_t numRows;
      JSON desc, details, queryColumns;
      DXGTable table;
      
      TypesHandler types;
      ValidateInfo *msg;
      ColumnsHandler *columns;
      
      bool processColumns();
      
      virtual bool validateTypes() { 
        if (types.HasDuplicate()) msg->addWarning("TYPE_DUPLICATE");
        return true;
      }
      virtual bool validateDetails() { return true; }
      virtual bool validateColumns();
      virtual bool validateData() { return true; }
//      virtual bool finalValidate() { return true; }

    public:
      GTableValidator(){ }

      virtual JSON Validate(const string &source_id, ValidateInfo *m);
      virtual void Validate(const string &source_id);
  };

  class GTableErrorMsg : public ErrorMsg {
    public:
      GTableErrorMsg() {
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
      }
  };
};

#endif
