// Copyright (C) 2013-2014 DNAnexus, Inc.
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

#ifndef DX_GRI_VALIDATE_H
#define DX_GRI_VALIDATE_H

#include "dxvalidate_gtable.h"
#include "dxvalidate_contigset.h"

namespace dx {
  class GriColumnsHandler : public ColumnsHandler {
    protected:
      // Gri can have many other columns besides chr, hi, low
      virtual bool isRecognized() { return true; }

    public:
      // Add chr, lo, and hi as required columns with proper types
      void Init();
  };

  class GriErrorMsg : public GTableErrorMsg, public ContigsetErrorMsg {
    public:
      GriErrorMsg() : GTableErrorMsg(), ContigsetErrorMsg(true) {
        // Add gri specific error and warning messages
        errorMsg["TYPE_NOT_GRI"] = "Object is not a gri type";
        errorMsg["CONTIGSET_MISSING"] = "'Details' of this object does not contain 'original_contigset'";
        errorMsg["CONTIGSET_INVALID"] = "In object details, 'original_contigset' is not a valid DNAnexus link to a contigset object";
        errorMsg["CONTIGSET_FETCH_FAIL"] = "Internal error: {1}. Fail to fetch the details or content of the contigset";
        errorMsg["GRI_INDEX_MISSING"] = "Object does not have genomic range index named 'gri'";

        errorMsg["LO_TOO_SMALL"] = "In {1} row, {2} is negative";
        errorMsg["LO_TOO_LARGE"] = "In {1} row, {2} is larger than {3}";
        errorMsg["HI_TOO_LARGE"] = "In {1} row, {2} is larger than the size of the mapped contig";
        
        warningMsg["CHR_INVALID"] = "In some row, such as the {1} one, {2} does not match any contig name";
      }
  };

  // Gri row validator
  class GriRowValidator : public GTableRowValidator {
    private:
      vector<bool> chr_valid;
      vector<string> chrCols, loCols, hiCols;
      
    protected:
      int chrIndex;
      ContigSetReader *cReader;

      void addGri(const string &chr, const string &lo, const string &hi);

      // Validator whether or not chr, lo, and hi following gri type spec
      bool validateGri(const string &chr, int64_t lo, int64_t hi, int k);

    public:
      GriRowValidator(const string &contigset_id, ValidateInfo *m);
      virtual ~GriRowValidator() { delete cReader; }
      
      virtual bool validateRow(const JSON &row) { return (ready && validateGri(row[0].get<string>(), int64_t(row[1]), int64_t(row[2]), 0)); }
      virtual bool finalValidate() { return true; }
  };

  class GriValidator : public GTableValidator {
    private:
      // Return whether or not a object has genomic range index
      bool hasGenomicIndex();

    protected:
      virtual void setRowValidator() { rowV = new GriRowValidator(details["original_contigset"]["$dnanexus_link"].get<string>(), msg); }

    protected:
      virtual bool validateTypes();
      virtual bool validateColumns();
      virtual bool validateDetails();
  };
}

#endif
