#ifndef DX_CONTIGSET_VALIDATE_H
#define DX_CONTIGSET_VALIDATE_H

#include "dxvalidate_tools.h"
#include "dxcpp/dxcpp.h"

namespace dx {
  class ContigsetErrorMsg : public virtual ErrorMsg {
    public:
      ContigsetErrorMsg (bool gri = false) : ErrorMsg() {
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

  class ContigSetReader {
    private:
      bool ready, hasOffset, hasFlat;
      int64_t offsetShift;

      vector<int64_t> offsets, sizes;
      map<string,int> names;

      DXFile flatFile;
      
      bool fetchContigSet(const string &contigset_id);
      bool validateType();
      bool validateDetails();
      bool validateContigSetName();
      bool validateContigSetSize();
      bool validateContigSetOffset();
      bool initFlatFile(const JSON &details);

      bool validateChar(char &ch, bool &lowerCase);

    protected:
      JSON desc, details, fileDesc;
      TypesHandler types;
      ValidateInfo *msg;

    public:
      ContigSetReader(const string &id, ValidateInfo *m);
      
      bool isReady() { return ready; }
      bool withOffset() { return hasOffset; }
      bool withFlat() { return hasFlat; }

      bool fetchSeq(int64_t pos, char *buffer, int bufSize);
      bool validateSequence();

      int chrIndex(const string &name);
      int chrSize(int i);
      int chrOffset(int i);
  };
};

#endif
