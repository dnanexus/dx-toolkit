#ifndef DX_VALIDATE_TOOLS_H
#define DX_VALIDATE_TOOLS_H

#include <boost/lexical_cast.hpp>
#include "dxjson/dxjson.h"
#include <string>
#include <fstream>
#include <boost/algorithm/string.hpp>
#include <set>

using namespace std;

typedef unsigned uint32;
typedef unsigned long long uint64;
typedef long long int64;

namespace dx {
  class TypesHandler {
    private:
      bool duplicate;
      set<string> types;

    public:
      TypesHandler() {};

      void Add(const JSON &t);
      bool HasDuplicate() { return duplicate; };
      bool Has(const string &type) { return (types.find(type) != types.end()); }
  };

  class ColumnsHandler {
    private:
      map<string, string> columnTypes[3];
      set<string> intTypes;

      vector<string> columnLists[4];
      JSON queryColumns;

      bool integerType(const string &type) { return (intTypes.find(type) != intTypes.end()); }
      bool identifyColumn(const string &name, const string &type);
      void findMissingColumns();

    protected:
      set<string> allColumns;

      void clearColumns();
      
      /** index
        * 0 - required columns
        * 1 - suggested columns
        * 2 - optional columns
        */
      void addColumn(const string &name, const string &type, int index) { columnTypes[index][name] = type; }

      virtual bool recognizeColumn(const string &n, const string &t) { return false; }

    public:
      ColumnsHandler();
      
      void virtual Init() { clearColumns(); }

      void virtual Add(const JSON &c);

      /** 0 - missing required columns
        * 1 - missing suggested columns
        * 2 - invalid columns
        * 3 - unrecognized columns
        */
      string getColumnList(int index);

      JSON getQueryColumns() { return queryColumns; }

      bool Has(const string &column) { return (allColumns.find(column) != allColumns.end()); }
  };

  class ErrorMsg {
    private:
      string msg;
      vector<string> msgData;
      
      string replaceStr();

    protected:
      JSON errorMsg, warningMsg;

    public: 
      ErrorMsg ();

      void SetData(const string &msgD, uint32_t pos);
      string GetError(const string &err, bool replace = false);
      string GetWarning(const string &w, bool replace = false);
  };

  string dataIndex(int64_t index);

  JSON readJSON(const string &filename);

  void writeJSON(const JSON &input, const string &filename);

  bool hasString(const JSON &json, const string &val);

  bool inline validASCII(char ch) { return (ch >= 33); }

  string myPath();
  
  bool exec(const string &cmd, string &out);

  class ValidateInfo {
    private:
      JSON info;
      ErrorMsg *msg;
      int64_t rowIndex;
      
    public:
      ValidateInfo(ErrorMsg &m);

      void setString(const string &key, const string &value) { info[key] = value; }
      void setBoolean(const string &key, bool value) { info[key] = value; }

      void addWarning(const string &w, bool additionalInfo = false);
      void addRowWarning(const string &w, uint32_t p = 0);
      void addRowWarning(const string &w, const string &colName, uint32_t p = 0);

      bool setError(const string &err, bool additionalInfo = false);
      bool setRowError(const string &err, uint32_t p = 0);
      bool setRowError(const string &err, const string &colName, uint32_t p = 0);

      bool setDXError(const string &m, const string &err);

      void setRowIndex(int64_t index) { rowIndex = index; }
      
      void setData(const string &data, uint32_t p) { msg->SetData(data, p); }
      void setDataIndex(int64_t index, uint32_t p) { msg->SetData(dataIndex(index), p); }
      
      JSON getInfo() { return info; }
  };
};

#endif
