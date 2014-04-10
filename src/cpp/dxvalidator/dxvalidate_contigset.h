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

#ifndef DX_CONTIGSET_VALIDATE_H
#define DX_CONTIGSET_VALIDATE_H

#include "dxvalidate_tools.h"
#include "dxcpp/dxcpp.h"

namespace dx {
  class ContigsetErrorMsg : public virtual ErrorMsg {
    public:
      ContigsetErrorMsg (bool gri = false) : ErrorMsg() {
        // Add specific error and warning messages for contigset
        string head = (gri) ? "Original contigset is invalid: " : "";
        errorMsg["CONTIGSET_INVALID"] = head + "Cannot find the contigset object";
        errorMsg["CONTIGSET_FETCH_FAIL"] = head + "Internal error: {1}. Fail to fetch the description or details of the contigset";
        errorMsg["CONTIGSET_NOT_RECORD"] = head + "Object is not a record";
        errorMsg["TYPE_NOT_CONTIGSET"] = head + "Object does not have type 'ContigSet'";
        errorMsg["CONTIGSET_DETAILS_NOT_HASH"] = head + "Details of the contigset is not a hash";
        
        errorMsg["CONTIGS_MISSING"] = head + "Object does not have contigs in its details";
        errorMsg["CONTIGS_NOT_HASH"] = head + "Contigs in object details are not stored in a hash";       
        
        errorMsg["CONTIGS_NAMES_MISSING"] = head + "In object details, contigs do not have names";
        errorMsg["CONTIGS_NAMES_NOT_ARRAY"] = head + "In object details, names of contigs are not stored in an array";
        errorMsg["CONTIGS_NAMES_EMPTY"] = head + "In object details, names of contigs is an empty array";
        errorMsg["CONTIGS_NAME_NOT_STRING"] = head + "In object details, the name of {1} contig is not a string";
        errorMsg["CONTIGS_NAME_EMPTY"] = head + "In object details, the name of {1} contig is empty";
        errorMsg["CONTIGS_NAME_INVALID_CHARACTER"] = head + "In object details, the name of {1} contig has invalid characters";
        errorMsg["CONTIGS_NAME_DUPLICATE"] = head + "In object details, {2} and {1} contig have the same name";
        
        errorMsg["CONTIGS_SIZES_MISSING"] = head + "In object details, contigs do not have sizes";
        errorMsg["CONTIGS_SIZES_NOT_ARRAY"] = head + "In object details, sizes of contigs are not stored in an array";
        errorMsg["CONTIGS_SIZES_NAMES_DIFFERENT_LENGTH"] = head + "In object details, names and sizes of contigs have different lengths";
        errorMsg["CONTIGS_SIZE_NOT_NON_NEGATIVE_INTEGER"] = head + "In object details, the size of {1} contig is not a non-negative integer";
        
        errorMsg["CONTIGS_OFFSETS_NOT_ARRAY"] = head + "In object details, offsets of contigs are not stored in an array";
        errorMsg["CONTIGS_OFFSETS_SIZES_NOT_MATCH"] = head + "In object details, offsets and sizes of contigs do not match";
        errorMsg["CONTIGS_OFFSET_NOT_NON_NEGATIVE_INTEGER"] = head + "In object details, the offset of {1} contig is not a non-negative integer";
        errorMsg["CONTIGS_OFFSETS_MISSING"] = head + "Object details has 'flat_sequence_file', but contigs do not have offsets";
        
        errorMsg["CONTIGSET_FLAT_INVALID"] = head + "In object details, 'flat_sequence_file' is not a valid DNAnexus link";
        errorMsg["CONTIGSET_FLAT_FETCH_FAIL"] = head + "Internal error: {1}. Fail to fetch the description or content flat sequence file";
        errorMsg["CONTIGSET_FLAT_NOT_FILE"] = head + "Flat sequence file, is not a file object";
        errorMsg["CONTIGSET_FLAT_NOT_CLOSED"] = head + "Flat sequence file, is not closed";
        
        errorMsg["CONTIGSET_FLAT_TOO_SHORT"] = head + "Flat sequence file has less sequences than what contigs have required";
        errorMsg["CONTIGSET_FLAT_INVALID_CHARACTER"] = head + "Flat sequence file contains an invalid character at position {2}";
      
        head = (gri) ? "Original contigset: " : "";
        warningMsg["CONTIGSET_NOT_CLOSED"] = head + "Object is not closed";
        warningMsg["CONTIGS_SIZE_ZERO"] = head + "In object details, 1 or multiple contigs have 0 size";
        warningMsg["CONTIGS_OFFSETS_NOT_START_WITH_ZERO"] = head + "In object details, the smallest offset of contigs is not 0";
        
        warningMsg["CONTIGSET_FLAT_NOT_HIDDEN"] = head + "Flat sequence file is not hidden";
        warningMsg["CONTIGSET_FLAT_TOO_LONG"] = head + "Flat sequence file has more sequences than what contigs have required";
        warningMsg["CONTIGSET_FLAT_LOWER_CASE"] = head + "Flat sequence file has 1 or multiple lowercase letters";
      }
  };

  /** Class handles validation of contigset and fetching of flat sequence. When creating
    * a new instance of this class with a contigset id, types and details of this contigset
    * are validated by the constructor and private variable ready is set to be false if
    * it detectes an error. Public function validateSequence() can be further called to
    * validate flat sequence file.
    */
  class ContigSetReader {
    private:
      bool ready, hasOffset, hasFlat;
      int64_t offsetShift;

      // variables stores contigset data
      vector<int64_t> offsets, sizes;
      map<string,int> names;

      DXFile flatFile;
      
      // Fetches description and details of a contigset
      bool fetchContigSet(const string &contigset_id);

      // Validates types and details of a contigset
      bool validateType();
      bool validateDetails();
      bool validateContigSetName();
      bool validateContigSetSize();
      bool validateContigSetOffset();

      // Fetches description of the flat sequence file and validates it
      bool initFlatFile(const JSON &details);

      // Determines whether or not a character in the flat sequence file is valid
      bool validateChar(char &ch, bool &lowerCase);

    protected:
      JSON desc, details, fileDesc;
      TypesHandler types;
      ValidateInfo *msg;

    public:
      // Validates the description and details of a contigset and sets ready accordingly
      ContigSetReader(const string &contigset_id, ValidateInfo *m);
      
      bool isReady() { return ready; }
      bool withOffset() { return hasOffset; }
      bool withFlat() { return hasFlat; }

      // Fetch sequences from the flat sequence file and stores them into buffer
      bool fetchSeq(int64_t pos, char *buffer, int bufSize);

      // Validate whether or not the flat sequence file containing valid characters
      bool validateSequence();

      // Return the chromosome index given its name
      int chrIndex(const string &name);

      // Return the size of a chromosome given its index
      int64_t chrSize(int i);

      // Return the offset of a chromosome given its index
      int64_t chrOffset(int i);
  };
}

#endif
