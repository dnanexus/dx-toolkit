#ifndef DX_GRI_VALIDATE_H
#define DX_GRI_VALIDATE_H

#include "dxvalidate_gtable.h"

namespace dx {
  class GriValidator : public GTableValidator {
    private: 
      bool hasGenomicIndex();
    
    protected:
      virtual bool validateTypes();
      virtual bool validateDetails();
  };

  class GriDataValidator {
    private:
      vector<bool> chr_valid;
      vector<string> chrCols, loCols, hiCols;
      DXFile flatFile;
      
      bool initFlatFile(const JSON &details);

    protected:
      int chrIndex;
      bool hasFlat, hasOffset;

      ValidateInfo *msg;
      vector<int64_t> offsets, sizes;
      map<string,int> indices;

    public:
      GriDataValidator(ValidateInfo *m);
      void AddGri(const string &chr, const string &lo, const string &hi);
      bool HasFlat() { return hasFlat; }

      bool FetchContigSets(const string &source_id); 
      bool FetchSeq(int64_t pos, char *buffer, int bufSize);
      bool ValidateGri(const string &chr, int64_t lo, int64_t hi, int k);
  };

  class GriErrorMsg : public GTableErrorMsg {
    public:
      GriErrorMsg() {              
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
};

#endif
