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
  // class provide some basic operations on the types of a DNAnexus object
  class TypesHandler {
    private:
      bool duplicate;  // Whether or not there the object has duplicate types
      set<string> types; // List of types of the object

    public:
      TypesHandler() {};

      /** This function takes a JSON array as input t, which should be the types
        * of an DNAnexus object. It first clears the string set types and then
        * stores unique entries in the array to this set. It also sets duplicate
        * to be true if there are duplicate entries in the array.
        */
      void Add(const JSON &t);

      // Return whether or not the object has duplicate types
      bool HasDuplicate() { return duplicate; };

      // Return whether or not the object has a particular type
      bool Has(const string &type) { return (types.find(type) != types.end()); }
  };

  /** Basic class provide tools to validate columns of a gtable including two major
    * functions:
    *   1. Build the following list of columns for a particular type
    *        a. Required columns - columns MUST exist in the gtable
    *        b. Suggested columns - columns SHOULD exist in the gtable
    *        c. Optional columns - columns MAY or MAY NOT exist in the gtable
    *        d. Forbidden columns - columns MUST NOT exist in the gtable
    *      and for columns in lists a, b, and c, set their proper types
    *   2. Give a gtable with the corresponding type, populate the following arrays
    *      with columns in the table:
    *        a. Missing required columns
    *        b. Missing suggested columns
    *        c. Invalid columns - columns do not have proper types
    *        d. Unrecognized columns - columns that are neither required nor suggested nor optional
    *        e. Forbidden columns - columns that are forbidden
    */
  class ColumnsHandler {
    private:
      map<string, string> columnTypes[3];

      set<string> intTypes;

      vector<string> columnLists[5];
      JSON queryColumns;

      bool integerType() { return (intTypes.find(cType) != intTypes.end()); }
      bool floatType() { return ((cType == "float") || (cType == "double")); }
      bool identifyColumn();
      void findMissingColumns();

    protected:
      string cName, cType;
      set<string> allColumns;

      void clearColumns();
      
      /** index
        * 0 - required columns
        * 1 - suggested columns
        * 2 - optional columns
        */
      void addColumn(const string &name, const string &type, int index) { columnTypes[index][name] = type; }

      virtual bool isForbidden() { return false; }
      virtual bool isRecognized() { return false; }

    public:
      ColumnsHandler();
      
      void virtual Init() { clearColumns(); }

      void virtual Add(const JSON &c);

      /** 0 - missing required columns
        * 1 - missing suggested columns
        * 2 - invalid columns
        * 3 - unrecognized columns
        * 4 - forbidden columns
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
      
    public:
      int64_t rowIndex;
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
