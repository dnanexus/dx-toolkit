// Copyright (C) 2013 DNAnexus, Inc.
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

  /** Basic class for validating columns of a particular type of gtable. This
    * class using instance variables to store the type and category of related
    * columns. The type of a column corresponds to a column type supported by 
    * DNAnexus platform, such as string, boolean, or integer. A column may have
    * one of the following categories:
    *   a. Required: MUST exist in the gtable
    *   b. Suggested: SHOULD exist in the gtable
    *   c. Optional: MAY or MAY NOT exist in the gtable
    *   d. Forbidden: MUST NOT exist in the gtable
    *   
    * This class used stored information to populate the following lists of a
    * given gtable
    *   1. Missing required columns
    *   2. Missing suggested columns
    *   3. Invalid columns - columns do not have proper types
    *   4. Unrecognized columns - columns that SHOULD not appear in the gtable
    *   5. Forbidden columns - columns that MUST NOT appear in the gtable
    */
  class ColumnsHandler {
    private:
      // Maps storing the types of required, suggested, and optional columns
      map<string, string> columnTypes[3];

      // A list of all supported integer types
      set<string> intTypes;

      // 5 lists of columns that described above
      vector<string> columnLists[5];

      /** A JSON array contains all required, suggested, and optional columns
        * that will be used to query the gtable when validating rows in the
        * gtable
        */
      JSON queryColumns;

      bool integerType() { return (intTypes.find(cType) != intTypes.end()); }
      bool floatType() { return ((cType == "float") || (cType == "double")); }

      // Find missing required or suggested columns
      void findMissingColumns();

    protected:
      string cName, cType;

      // A list of all columns in a gtable being valiadated
      set<string> allColumns;

    private:
      /** Determine whether or not cName is the name of a column that is either
        * required, suggested, or optional. If so, verify whethor or not cType is
        * a proper type for this column
        */
      bool identifyColumn();

    protected:
      // Clear columnTypes, columnLists, query Columns, and all Columns
      void clearColumns();
      
      /** Add a column name with proper type to columnTypes, where index may be:
        * 0 - required columns
        * 1 - suggested columns
        * 2 - optional columns
        */
      void addColumn(const string &name, const string &type, int index) { columnTypes[index][name] = type; }

      /** Return whether or not a column is forbidden. This function may be
        * overridden in a derived class for a particular type
        */
      virtual bool isForbidden() { return false; }

      /** Return whether or not a column shall be added into the unrecognized columns
        * list. This function may be overridden in a derived class for a particular type
        */
      virtual bool isRecognized() { return false; }

    public:
      ColumnsHandler();
      
      void virtual Init() { clearColumns(); }

      // Populate columnLists with input JSON c representing the columns of a gtable
      void virtual Add(const JSON &c);

      /** Return one of the columnLists, where index may be:
        * 0 - missing required columns
        * 1 - missing suggested columns
        * 2 - invalid columns
        * 3 - unrecognized columns
        * 4 - forbidden columns
        */
      string getColumnList(int index);

      JSON getQueryColumns() { return queryColumns; }

      // Return whether not the gtable being validated contains a particular column
      bool Has(const string &column) { return (allColumns.find(column) != allColumns.end()); }
  };

  // A basic class handles validation error messages and warning messages 
  class ErrorMsg {
    private:
      string msg;
      vector<string> msgData;
      
      string replaceStr();

    protected:
      // JSON hashes storing errors and warnings and the correspoding messages
      JSON errorMsg, warningMsg;

    public: 
      ErrorMsg ();

      /** Put special messages to msgData that will be used to construct an error
        * or warning message dynamically.
        */
      void SetData(const string &msgD, uint32_t pos);

      // Return a particular message given a specific error. 
      string GetError(const string &err, bool replace = false);

      // Return a particular message given a specific warning. 
      string GetWarning(const string &w, bool replace = false);
  };

  /** Convert an integer index into a string, i.e. 1 -> 1st, 2-> 2nd, 3 -> 3rd
    * 4 -> 4th, and so on
    */
  string dataIndex(int64_t index);

  // Read a JSON object from a file
  JSON readJSON(const string &filename);

  // Write a JSON object to a file
  void writeJSON(const JSON &input, const string &filename);

  // Whether or not a JSON array of strings contains a particular value
  bool hasString(const JSON &json, const string &val);

  bool inline validASCII(char ch) { return (ch >= 33); }

  // Return the full path of the current executable
  string myPath();
  
  /** Run cmd in shall and store stdout to string out. Return whether or not
    * this command is executed successfully
    */
  bool exec(const string &cmd, string &out);

  // Class manages the outcome of validation 
  class ValidateInfo {
    private:
      // A JSON object stores validation outcome
      JSON info;

      ErrorMsg *msg;
      
    public:
      // The index of the current row of a gtable being validated
      int64_t rowIndex;

      // Initialize an intance with an instance of ErrorMsg
      ValidateInfo(ErrorMsg &m);

      void setString(const string &key, const string &value) { info[key] = value; }
      void setBoolean(const string &key, bool value) { info[key] = value; }

      // Add warning to info
      void addWarning(const string &w, bool additionalInfo = false);
      // Add warning that related to current row to info
      void addRowWarning(const string &w, uint32_t p = 0);
      // Add warning that related to current row and a particular column to info
      void addRowWarning(const string &w, const string &colName, uint32_t p = 0);

      // Set error to info
      bool setError(const string &err, bool additionalInfo = false);
      // Set error that related to current row to info
      bool setRowError(const string &err, uint32_t p = 0);
      // Set error that related to current row to info
      bool setRowError(const string &err, const string &colName, uint32_t p = 0);
 
      // Set a platform related error to info
      bool setDXError(const string &m, const string &err);

      void setRowIndex(int64_t index) { rowIndex = index; }
      
      /** Put special messages to msgData in msg that will be used to construct an error
        * or warning message dynamically.
        */
      void setData(const string &data, uint32_t p) { msg->SetData(data, p); }
      void setDataIndex(int64_t index, uint32_t p) { msg->SetData(dataIndex(index), p); }
      
      // Return validation outcome
      JSON getInfo() { return info; }
  };
}

#endif
