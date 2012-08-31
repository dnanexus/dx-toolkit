#ifndef GTABLE_VALIDATE_H
#define GTABLE_VALIDATE_H

#include "dxvalidate_tools.h"
#include "dxcpp/dxcpp.h"

namespace dx {
  class GTableValidator {
    private:
      bool fetchHead(const string &source_id);

    protected:
      int64_t numRows;
      JSON desc, details;
      DXGTable table;
      
      TypesHandler types;
      ValidateInfo *msg;
      ColumnsHandler *columns;
      
      bool processColumns();
      
      virtual bool validateTypes() { return true; }
      virtual bool validateDetails() { return true; }
      virtual bool validateColumns();
      virtual bool validateData() { return true; }

    public:
      GTableValidator(){ }

      virtual JSON Validate(const string &source_id, ValidateInfo *m);
      virtual void Validate(const string &source_id);
  };
};

#endif
