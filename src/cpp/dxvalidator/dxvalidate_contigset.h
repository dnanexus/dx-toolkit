#ifndef DX_CONTIGSET_VALIDATE_H
#define DX_CONTIGSET_VALIDATE_H

#include "dxvalidate_tools.h"
#include "dxcpp/dxcpp.h"

namespace dx {
  class ContigsetErrorMsg : public ErrorMsg {
    public:
      ContigsetErrorMsg () {
        errorMsg["CONTIGSET_INVALID"] = "Cannot find contigset object";
        errorMsg["CONTIGSET_FETCH_FAIL"] = "Internal error: {1}. Fail to fetch the description or details of the contigset";
        errorMsg["CONTIGSET_NOT_RECORD"] = "Contigset is not a record";
        errorMsg["TYPE_NOT_CONTIGSET"] = "Type of contigset does not have 'ContigSet'";
        errorMsg["CONTIGSET_DETAILS_NOT_HASH"] = "Details of contigset is not a hash";
        
        errorMsg["CONTIGS_MISSING"] = "Object does not have contigs in its details";
        errorMsg["CONTIGS_NOT_HASH"] = "Contigs in object details are not stored in a hash";       
        
        errorMsg["CONTIGS_NAMES_MISSING"] = "In object details, contigs do not have names";
        errorMsg["CONTIGS_NAMES_NOT_ARRAY"] = "In object details, names of contigs are not stored in an array";
        errorMsg["CONTIGS_NAMES_EMPTY"] = "In object details, names of contigs is an empty array";
        errorMsg["CONTIGS_NAME_NOT_STRING"] = "In object details, the name of {1} contig is not a string";
        errorMsg["CONTIGS_NAME_EMPTY"] = "In object details, the name of {1} contig is empty";
        errorMsg["CONTIGS_NAME_INVALID_CHARACTER"] = "In object details, the name of {1} contig has invalid characters";
        errorMsg["CONTIGS_NAME_DUPLICATE"] = "In object details, {2} and {1} contig have the same name";
        
        errorMsg["CONTIGS_SIZES_MISSING"] = "In object details, contigs do not have sizes";
        errorMsg["CONTIGS_SIZES_NOT_ARRAY"] = "In object details, sizes of contigs are not stored in an array";
        errorMsg["CONTIGS_SIZES_NAMES_DIFFERENT_LENGTH"] = "In object details, names and sizes of contigs have different lengths";
        errorMsg["CONTIGS_SIZE_NOT_NON_NEGATIVE_INTEGER"] = "In object details, the size of {1} contig is not a non-negative integer";
        
        errorMsg["CONTIGS_OFFSETS_NOT_ARRAY"] = "In object details, offsets of contigs are not stored in an array";
        errorMsg["CONTIGS_OFFSETS_SIZES_NOT_MATCH"] = "In object details, offsets and sizes of contigs do not match";
        errorMsg["CONTIGS_OFFSET_NOT_NON_NEGATIVE_INTEGER"] = "In object details, the offset of {1} contig is not a non-negative integer";
        errorMsg["CONTIGS_OFFSETS_MISSING"] = "Object details has 'flat_sequence_file', but contigs do not have offsets";
        
        errorMsg["CONTIGSET_FLAT_INVALID"] = "In object details, 'flat_sequence_file' is not a valid DNAnexus link";
        errorMsg["CONTIGSET_FLAT_FETCH_FAIL"] = "Internal error: {1}. Fail to fetch the description or content flat sequence file";
        errorMsg["CONTIGSET_FLAT_NOT_FILE"] = "Flat sequence file, is not a file object";
        errorMsg["CONTIGSET_FLAT_NOT_CLOSED"] = "Flat sequence file, is not closed";
        
        errorMsg["CONTIGSET_FLAT_TOO_SHORT"] = "Flat sequence file has less sequences than what contigs have required";
        errorMsg["CONTIGSET_FLAT_INVALID_CHARACTER"] = "Flat sequence file contains an invalid character at position {2}";
      
        warningMsg["CONTIGSET_NOT_CLOSED"] = "Contigset is not closed";
        warningMsg["CONTIGS_SIZE_ZERO"] = "In object details, 1 or multiple contigs have 0 size";
        warningMsg["CONTIGS_OFFSETS_NOT_START_WITH_ZERO"] = "In object details, the smallest offset of contigs is not 0";
        
        warningMsg["CONTIGSET_FLAT_NOT_HIDDEN"] = "Flat sequence file is not hidden";
        warningMsg["CONTIGSET_FLAT_TOO_LONG"] = "Flat sequence file has more sequences than what contigs have required";
        warningMsg["CONTIGSET_FLAT_LOWER_CASE"] = "Flat sequence file has 1 or multiple lowercase letters";
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
  };
};

#endif
