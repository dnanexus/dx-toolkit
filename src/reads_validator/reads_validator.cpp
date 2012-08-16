#include "reads_validate_helper.h"
#include "dxcpp/dxcpp.h"
#include <boost/regex.hpp>

using namespace std;
using namespace dx;

typedef unsigned uint32;
typedef unsigned long long uint64;
typedef long long int64;

string joinArray(const vector<string> &strs, const string &separator) {
  if (strs.size() == 0) return "";

  string ret_val = strs[0];
  for (int i = 1; i < strs.size(); i++)
    ret_val += separator + strs[i];

  return ret_val;
}

class ReadsInfo {
  private:
    JSON info;
    ReadsErrorMsg msg;

  public:
    ReadsInfo() {};

    void init(const string &id) {
      info = JSON(JSON_OBJECT);
      info["objectId"] = id;
      info["valid"] = true;
    }

    void setType(const string &type) { info["type"] = type; }

    void setPaired(bool paired) { info["paired"] = paired; }

    void addWarning(const string &w, bool additionalInfo = false) {
      string str = msg.GetWarning(w, additionalInfo);
      if (! info.has("warning")) info["warning"] = JSON(JSON_ARRAY);
      info["warning"].push_back(str);
    }

    bool setError(const string &err, bool additionalInfo = false) {
      info["error"] = msg.GetError(err, additionalInfo);
      info["valid"] = false;
      return false;
    }

    void setData(const string &data, uint32_t p) { msg.SetData(data, p); }

    void setDataIndex(int64_t index, uint32_t p) { msg.SetDataIndex(index, p); }

    JSON getInfo() { return info; }
};

class ReadsTypeValidator {
  private:
    TypesHandler types;

  public:
    ReadsTypeValidator(){};

    bool validate(const JSON &desc, const JSON &details, ReadsInfo &info) {
      if (desc["class"].get<string>() != "gtable") return info.setError("CLASS_NOT_GTABLE");
      if (desc["state"].get<string>() != "closed") return info.setError("GTABLE_NOT_CLOSED");

      types.Add(desc["types"]);
      if (types.HasDuplicate()) info.addWarning("TYPE_DUPLICATE");

      if (! types.Has("Reads")) return info.setError("TYPE_NOT_READS");

      int count = 0;
      if (types.Has("LetterReads")) info.setData("LetterReads", count++);
      if (types.Has("ColorReads")) info.setData("ColorReads", count++);
      if (types.Has("FlowReads")) info.setData("FlowReads", count++);

      if (count > 1) return info.setError("TYPE_CONFLICT", true);

      if (details.type() != JSON_OBJECT) return info.setError("DETAILS_NOT_HASH");

      if (details.has("original_files")) {
        if (details["original_files"].type() != JSON_ARRAY) {
          info.addWarning("ORIGINAL_FILES_INVALID");
        } else {
          for (int i = 0; i < details["original_files"].size(); i++) {
            if (details["original_files"][i].type() != JSON_OBJECT) {
              info.addWarning("ORIGINAL_FILE_INVALID");
              break;
            }
            if (! details["original_files"][i].has("$dnanexus_link")) {
              info.addWarning("ORIGINAL_FILE_INVALID");
              break;
            }
          }
        }
      }
       
      if (count == 0) {
        info.addWarning("TYPE_MISSING");
        return false;
      }

      return true;
    }
    
    string getType() {
      if (types.Has("LetterReads")) return "letter";
      if (types.Has("ColorReads")) return "color";
      if (types.Has("FlowReads")) return "flow";
      return "";
    };
};

class ReadsValidateTools {
  private:
    static void validatePairedData(const JSON details, ReadsInfo &info) {
      if (details.has("pair_orientation")) {
        if (details["pair_orientation"].type() != JSON_STRING) info.addWarning("PAIR_ORIENTATION_INVALID");
        else {
          string o = details["pair_orientation"].get<string>();
          if ((o != "FF") && (o != "FR") && (o != "RF") && (o != "RR")) info.addWarning("PAIR_ORIENTATION_INVALID");
        }
      }
      
      if (details.has("pair_min_dist")) {
        if ( (details["pair_min_dist"].type() != JSON_INTEGER) && (details["pair_min_dist"].type() != JSON_REAL) ) info.addWarning("PAIR_MIN_DIST_INVALID");
      }
      
      if (details.has("pair_max_dist")) {
        if ( (details["pair_max_dist"].type() != JSON_INTEGER) && (details["pair_max_dist"].type() != JSON_REAL) ) info.addWarning("PAIR_MAX_DIST_INVALID");
      }
      
      if (details.has("pair_avg_dist")) {
        if ( (details["pair_avg_dist"].type() != JSON_INTEGER) && (details["pair_avg_dist"].type() != JSON_REAL) ) info.addWarning("PAIR_AVG_DIST_INVALID");
      }
      
      if (details.has("pair_stddev_dist")) {
        if ( (details["pair_stddev_dist"].type() != JSON_INTEGER) && (details["pair_stddev_dist"].type() != JSON_REAL) ) info.addWarning("PAIR_STDDEV_DIST_INVALID");
      }
    }
    
  public:
    static bool validatePaired(const JSON &details, bool &paired, ReadsInfo &info) {
      paired = false;
      if (details.has("paired")) {
        if(details["paired"].type() != JSON_BOOLEAN) return info.setError("DETAILS_PAIRED_INVALID");
        paired = bool(details["paired"]);
      }

      if (paired) validatePairedData(details, info);

      return true;
    }
};

class ReadsDataValidator {
  private:
    bool valid_sequence, valid_name;
    int seq_qual_diff, warning_index;
    string name_pattern, seq_pattern;
    boost::regex re_name, re_seq;
    
    void setInvalidSeq(ReadsInfo &info) {
      info.setData(seq_pattern, 0);
      info.setDataIndex(offset + warning_index, 1);
      info.addWarning("SEQUENCE_INVALID", true);
      valid_sequence = false;
    }

    void setInvalidName(ReadsInfo &info) {
      info.setData(name_pattern, 0);
      info.setDataIndex(offset + warning_index, 1);
      info.addWarning("NAME_INVALID", true);
      valid_name = false;
    }

    bool validateString(const JSON &rows, const boost::regex &re, int index) {
      for (int i = 0; i< rows.size(); i++) {
        if (! boost::regex_match(rows[i][index].get<string>(), re)) {
          warning_index = i;
          return false;
        }
      }
      return true;
    }

    bool validateQualities(const JSON &rows, int index, int seq_index, ReadsInfo &info, const string &label = "") {
      for (int i = 0; i < rows.size(); i++) {
        string quality = rows[i][index].get<string>();
        if ((quality.size() + seq_qual_diff) != rows[i][seq_index].get<string>().size()) {
          info.setDataIndex(offset + i, 0);
          return info.setError("QUALITY" + label + "_SEQUENCE" + label + "_NOT_MATCH", true);
        }
        
        for (int j=0; j<quality.size(); j++) {
          if (! validASCII(quality.at(j))) {
            info.setDataIndex(offset + i, 0);
            return info.setError("QUALITY" + label + "_NOT_PHRED33", true);
          }
        }
      }
      
      return true;
    }

  protected:
    int64_t offset;

    void validateSeq(const JSON &queryColumns, const JSON &rows, ReadsInfo &info) {
      if (! valid_sequence) return;
      for (int i = 0; i < queryColumns.size(); i++) {
        string col = queryColumns[i].get<string>();
        if ((col == "sequence") || (col == "sequence2")) {
          if (! validateString(rows, re_seq, i)) {
            setInvalidSeq(info);
            return;
          }
        }
      }
    }

    bool validateRows(const JSON &queryColumns, const JSON &rows, ReadsInfo &info) {
      int seq_index = 0;
      for (int i = 0; i < queryColumns.size(); i++) {
        string col = queryColumns[i].get<string>();

        if ((col == "sequence") || (col == "sequence2")) {
          seq_index = i;

        } else if ((col == "name") || (col == "name2")) {
          if (valid_name) {
            if (! validateString(rows, re_name, i)) setInvalidName(info);
          }

        } else if (col == "quality") {
          if (! validateQualities(rows, i, seq_index, info)) return false;

        } else if (col == "quality2") {
          if (! validateQualities(rows, i, seq_index, info, "2")) return false;

        }
      }

      return true;
    }

  public:
    ReadsDataValidator() {};
    
    void Init(const string &name, const string &seq, int64_t offset_, int diff) {
      name_pattern = name; seq_pattern = seq;
      re_name.assign("^" + name + "$");
      re_seq.assign("^" + seq + "$");
      seq_qual_diff = diff;
      offset = offset_;
      valid_sequence = valid_name = true;
    }
 
    bool validate(const JSON &queryColumns, const JSON &rows, ReadsInfo &info) {
      if (! validateRows(queryColumns, rows, info)) return false;
      validateSeq(queryColumns, rows, info);

      offset += rows.size();
      return true;
    }

    int64_t getOffset() { return offset; }
};

class FlowReadsDataValidator : public ReadsDataValidator {
  private:
    bool valid_left_right;
    string flow_seq, flow_key, flow_seq2, flow_key2;
    boost::regex re_gram;

    vector<uint16_t> flow_indices;
    void parseFlowindices(const string &s) {
      stringstream ss;
      int n = s.size() / 2;
      flow_indices.resize(n);

      for (int i = 0; i < n; i++) {
        ss.clear();
        ss << std::hex << s.substr(i*2, 2);
        ss >> flow_indices[i];
      }
    }

    bool validateFlowgram(const JSON &rows, int index, ReadsInfo &info, const string &label = "") {
      int expt_len = (label == "") ? flow_seq.size() * 4 : flow_seq2.size() * 4;

      for (int i = 0; i < rows.size(); i++) {
        string flowgram = rows[i][index].get<string>();
        if (flowgram.size() != expt_len) {
          info.setDataIndex(offset + i, 0);
          return info.setError("FLOWGRAM" + label + "_INVALID_LENGTH", true);
        }

        if (! boost::regex_match(flowgram, re_gram)) {
          info.setDataIndex(offset + i, 0);
          return info.setError("FLOWGRAM" + label + "_INVALID_CHARACTER", true);
        }
      }

      return true;
    }

    bool validateFlowindices(const JSON &rows, int index, int seq_index, ReadsInfo &info, const string &label = "") {
      string fseq = (label == "") ? flow_seq : flow_seq2;
      string fkey = (label == "") ? flow_key : flow_key2;

      string sequence, indices;
      int n = fseq.size(), m = fkey.size();

      for (int i = 0; i < rows.size(); i++) {
        sequence = rows[i][seq_index].get<string>();
        if (sequence.substr(0, m) != fkey) {
          info.setDataIndex(offset + i, 0);
          return info.setError("FLOW_KEY" + label + "_SEQUENCE" + label + "_NOT_MATCH", true);
        }

        indices = rows[i][index].get<string>();
        if ((sequence.size()*2) != indices.size()) {
          info.setDataIndex(offset + i, 0);
          return info.setError("FLOW_INDICES" + label + "_INVALID_LENGTH", true);
        }

        if (! boost::regex_match(indices, re_gram)) {
          info.setDataIndex(offset + i, 0);
          return info.setError("FLOW_INDICES" + label + "_INVALID_CHARACTER", true);
        }

        parseFlowindices(indices);
        int k = -1;
        for (int j = 0; j < flow_indices.size(); j++) {
          k += flow_indices[j];
          if ((k < 0) || (k >= n)) {
            info.setDataIndex(offset + i, 0);
            return info.setError("FLOW_INDICES" + label + "_SEQUENCE" + label + "_NOT_MATCH", true);
          }
          if ((fseq.at(k) != sequence.at(j)) && (sequence.at(j) != 'N')) {
            info.setDataIndex(offset + i, 0);
            return info.setError("FLOW_INDICES" + label + "_SEQUENCE" + label + "_NOT_MATCH", true);
          }
        }
      }

      return true;
    }

    void validateFlowLeftRight(const JSON &rows, int qual_left_index, int qual_right_index, int adapter_left_index, int adapter_right_index, int seq_index, ReadsInfo &info) {
      int qual_left, left, right, qual_right, adapter_right;
      for (int i = 0; i < rows.size(); i++) {
        qual_left = int(rows[i][qual_left_index]);
        left = int(rows[i][adapter_left_index]);
        if (left < qual_left) left = qual_left;

        if (left == 0) continue;

        right = rows[i][seq_index].get<string>().size();
        qual_right = int(rows[i][qual_right_index]);
        if ((qual_right > 0) && (qual_right < right)) right = qual_right;
        adapter_right = int(rows[i][adapter_right_index]);
        if ((adapter_right > 0) && (adapter_right < right)) right = adapter_right;

        if (left > right) {
          info.setDataIndex(offset + i, 0);
          valid_left_right = false;
          info.addWarning("SEQUENCE_EMPTY_AFTER_TRIMMING", true);
          return;
        }
      }
    }
    
    bool validateRows(const JSON &queryColumns, const JSON &rows, ReadsInfo &info) {
      int seq_index = 0, qual_left_index = 0, qual_right_index = 0, adapter_left_index = 0;
      for (int i = 0; i < queryColumns.size(); i++) {
        string col = queryColumns[i].get<string>();
        if ((col == "sequence") || (col == "sequence2")) {
          seq_index = i;

        } else if (col == "flowgram") {
          if (! validateFlowgram(rows, i, info)) return false;

        } else if (col == "flowgram2") {
          if (! validateFlowgram(rows, i, info, "2")) return false;

        } else if (col == "flow_indices") {
          if (! validateFlowindices(rows, i, seq_index, info)) return false;

        } else if (col == "flow_indices2") {
          if (! validateFlowindices(rows, i, seq_index, info, "2")) return false;

        } else if ( (col == "clip_qual_left") || (col == "clip_qual_left2")) {
          qual_left_index = i;

        } else if ( (col == "clip_qual_right") || (col == "clip_qual_right2")) {
          qual_right_index = i;

        } else if ( (col == "clip_adapter_left") || (col == "clip_adapter_left2")) {
          adapter_left_index = i;

        } else if ((col == "clip_adapter_right") || (col == "clip_adapter_right2")) {
          if (valid_left_right) validateFlowLeftRight(rows, qual_left_index, qual_right_index, adapter_left_index, i, seq_index, info);

        }
      }

      return true;
    }

  public:
    FlowReadsDataValidator() {};

    void Init(int64_t offset_, const string &seq, const string &key) {
      ReadsDataValidator::Init("[!-?A-~]{1,255}", "[ACGTN]+", offset_, 0);
      valid_left_right = true;
      flow_seq = seq;
      flow_key = key;
      re_gram.assign("^[0-9a-fA-F]+$");
    }
    
    void Init(int64_t offset_, const string &seq, const string &key, const string &seq2, const string &key2) {
      Init(offset_, seq, key);
      flow_seq = seq; flow_seq2 = seq2;
      flow_key = key; flow_key2 = key2;
    }

    bool validate(const JSON &queryColumns, const JSON &rows, ReadsInfo &info) {
      if (! validateRows(queryColumns, rows, info)) return false;
      return ReadsDataValidator::validateRows(queryColumns, rows, info);
    }
};

class LetterColorReadsValidator {
  private:
    int64_t numRows;
    JSON queryColumns;
    bool color;
    ReadsDataValidator v;

    bool validateColumns(const JSON &desc, const JSON &details, ReadsInfo &info) {
      bool paired;
      if (! ReadsValidateTools::validatePaired(details, paired, info)) return false;
      info.setPaired(paired);
        
      ColumnsHandler columns;
      columns.Add(desc["columns"]);

      string columnType;

      queryColumns.resize_array(0);
      if (! columns.Has("sequence", columnType)) return info.setError("SEQUENCE_MISSING");
      if (columnType != "string") return info.setError("SEQUENCE_NOT_STRING");
      queryColumns.push_back("sequence");

      if (columns.Has("quality", columnType)) {
        if (columnType != "string") return info.setError("QUALITY_NOT_STRING");
        queryColumns.push_back("quality");
      }
      
      if (columns.Has("name", columnType)) {
        if (columnType != "string") return info.setError("NAME_NOT_STRING");
        queryColumns.push_back("name");
      }

      if (paired) {
        if (! columns.Has("sequence2", columnType)) return (color) ? info.setError("COLOR_SEQUENCE2_MISSING") : info.setError("LETTER_SEQUENCE2_MISSING");
        if (columnType != "string") return info.setError("SEQUENCE2_NOT_STRING");
        queryColumns.push_back("sequence2");

        if ((columns.Has("quality2", columnType)) && (! columns.Has("quality", columnType))) return (color) ? info.setError("COLOR_QUALITY_MISSING") : info.setError("LETTER_QUALITY_MISSING");

        if (columns.Has("quality", columnType)) {
          if (! columns.Has("quality2", columnType)) return (color) ? info.setError("COLOR_QUALITY2_MISSING") : info.setError("LETTER_QUALITY2_MISSING");
          if (columnType != "string") return info.setError("QUALITY2_NOT_STRING");
          queryColumns.push_back("quality2");
        }

        if (columns.Has("name2", columnType)) {
          if (columnType != "string") return info.setError("NAME2_NOT_STRING");
          queryColumns.push_back("name2");

          if (! columns.Has("name", columnType)) return info.setError("NAME_MISSING");
        }
      }

      if (color) {
        if (! details.has("sequence_type")) return info.setError("COLOR_SEQUENCE_TYPE_INVALID");
        if (details["sequence_type"].type() != JSON_STRING) return info.setError("COLOR_SEQUENCE_TYPE_INVALID");
        string t = details["sequence_type"].get<string>();
        if (t != "color") return info.setError("COLOR_SEQUENCE_TYPE_INVALID");
      } else if (details.has("sequence_type")) info.addWarning("LETTER_WITH_SEQUENCE_TYPE");

      numRows = int64_t(desc["length"]);

      return true;
    }

    bool validateData(const string &tableId, ReadsInfo &info) {
      cerr << "Total rows " << numRows << endl;
      DXGTable table(tableId);
      JSON query = JSON(JSON_NULL), data;
      int64_t offset = 0;

      (color) ? v.Init("[!-?A-~]{1,255}", "[ACGT][0-3.]+", offset, 1) : v.Init("[!-?A-~]{1,255}", "[ACGTN]+", offset, 0);

      int64_t limit = 100000;      
      while (offset < numRows) {
        try {
          data = table.getRows(query, queryColumns, offset, limit);
          if (int(data["length"]) == 0) break;
          if (! v.validate(queryColumns, data["data"], info)) return false;
          offset = v.getOffset();
        } catch(DXAPIError &e) {
          info.setData("API error: " + e.msg + ". ", 0);
          return info.setError("GTABLE_FETCH_FAIL", true);
        }
      }

      return true;
    }

  public:
    LetterColorReadsValidator(bool color_) {
      color = color_;
      queryColumns = JSON(JSON_ARRAY);
    };

    bool validate(const string &source_id, const JSON &desc, const JSON &details, ReadsInfo &info) {
      if (! validateColumns(desc, details, info)) return false;
      return validateData(source_id, info);
    };
};
 
class FlowReadsValidator {
  private:
    bool paired, second_seq;
    int64_t numRows, offset;
    string flow_seq, flow_key, flow_seq2, flow_key2;
    JSON queryColumns;
    boost::regex re_flow_seq;
    FlowReadsDataValidator v;
    
    bool validateFlowSequence(const string &seq, ReadsInfo &info, bool first) {
      info.setData(seq, 0);
      if (! boost::regex_match(seq, re_flow_seq)) 
        return (first) ? info.setError("FLOW__SEQUENCE_INVALID_CHARACTER") : info.setError("FLOW__SEQUENCE2_INVALID_CHARACTER");

      for (int i = 1; i<seq.size(); i++) {
        if (seq.at(i-1) == seq.at(i)) return (first) ? info.setError("FLOW__SEQUENCE_SAME_CONSECUTIVE") : info.setError("FLOW__SEQUENCE2_SAME_CONSECUTIVE");
      }
      return true;
    }
    
    bool validateDetails(const JSON &details, ReadsInfo &info) {
      if (! details.has("sequence_type")) return info.setError("FLOW_SEQUENCE_TYPE_INVALID");
      if (details["sequence_type"].type() != JSON_STRING) return info.setError("FLOW_SEQUENCE_TYPE_INVALID");
      if (details["sequence_type"].get<string>() != "flow") return info.setError("FLOW_SEQUENCE_TYPE_INVALID");

      if (! details.has("flow_sequence")) return info.setError("FLOW__SEQUENCE_MISSING");
      if (details["flow_sequence"].type() != JSON_STRING) return info.setError("FLOW__SEQUENCE_NOT_STRING");
      flow_seq = details["flow_sequence"].get<string>();
      if (! validateFlowSequence(flow_seq, info, true)) return false;

      if (! details.has("flow_key")) return info.setError("FLOW__KEY_MISSING");
      if (details["flow_key"].type() != JSON_STRING) return info.setError("FLOW__KEY_NOT_STRING");
      flow_key = details["flow_key"].get<string>();
      if (! boost::regex_match(flow_key, re_flow_seq)) return info.setError("FLOW__KEY_INVALID_CHARACTER");

      paired = false; 
      if (! ReadsValidateTools::validatePaired(details, paired, info)) return false;
      info.setPaired(paired);

      if (paired) {
        if (! details.has("pair_second_flow")) return info.setError("PAIR_SECOND_FLOW_MISSING");
        if (details["pair_second_flow"].type() != JSON_BOOLEAN) return info.setError("PAIR_SECOND_FLOW_NOT_BOOLEAN");
        second_seq = bool(details["pair_second_flow"]);

        if (second_seq) {
          if (! details.has("flow_sequence2")) return info.setError("FLOW__SEQUENCE2_MISSING");
          if (details["flow_sequence2"].type() != JSON_STRING) return info.setError("FLOW__SEQUENCE2_NOT_STRING");
          flow_seq2 = details["flow_sequence2"].get<string>();
          if (! validateFlowSequence(flow_seq2, info, false)) return false;
          
          if (! details.has("flow_key2")) return info.setError("FLOW__KEY2_MISSING");
          if (details["flow_key2"].type() != JSON_STRING) return info.setError("FLOW__KEY2_NOT_STRING");
          flow_key2 = details["flow_key2"].get<string>();
          if (! boost::regex_match(flow_key2, re_flow_seq)) return info.setError("FLOW__KEY2_INVALID_CHARACTER");
        }
      }

      return true;
    }

    bool validateColumns(const JSON &desc, ReadsInfo &info) {
      ColumnsHandler columns;
      columns.Add(desc["columns"]);

      string columnType;

      queryColumns.resize_array(0);
      if (! columns.Has("sequence", columnType)) return info.setError("SEQUENCE_MISSING");
      if (columnType != "string") return info.setError("SEQUENCE_NOT_STRING");
      queryColumns.push_back("sequence");

      if (! columns.Has("quality", columnType)) return info.setError("FLOW_QUALITY_MISSING");
      if (columnType != "string") return info.setError("QUALITY_NOT_STRING");
      queryColumns.push_back("quality");
      
      if (! columns.Has("flowgram", columnType)) return info.setError("FLOWGRAM_MISSING");
      if (columnType != "string") return info.setError("FLOWGRAM_NOT_STRING");
      queryColumns.push_back("flowgram");
      
      if (! columns.Has("flow_indices", columnType)) return info.setError("FLOW_INDICES_MISSING");
      if (columnType != "string") return info.setError("FLOW_INDICES_NOT_STRING");
      queryColumns.push_back("flow_indices");

      if (! columns.Has("clip_qual_left", columnType)) return info.setError("FLOW_CLIP_QUAL_LEFT_MISSING");
      if (columnType != "uint16") return info.setError("FLOW_CLIP_QUAL_LEFT_NOT_UINT16");
      queryColumns.push_back("clip_qual_left");
      
      if (! columns.Has("clip_qual_right", columnType)) return info.setError("FLOW_CLIP_QUAL_RIGHT_MISSING");
      if (columnType != "uint16") return info.setError("FLOW_CLIP_QUAL_RIGHT_NOT_UINT16");
      queryColumns.push_back("clip_qual_right");
      
      if (! columns.Has("clip_adapter_left", columnType)) return info.setError("FLOW_CLIP_ADAPTER_LEFT_MISSING");
      if (columnType != "uint16") return info.setError("FLOW_CLIP_ADAPTER_LEFT_NOT_UINT16");
      queryColumns.push_back("clip_adapter_left");
      
      if (! columns.Has("clip_adapter_right", columnType)) return info.setError("FLOW_CLIP_ADAPTER_RIGHT_MISSING");
      if (columnType != "uint16") return info.setError("FLOW_CLIP_ADAPTER_RIGHT_NOT_UINT16");
      queryColumns.push_back("clip_adapter_right");
      
      if (columns.Has("name", columnType)) {
        if (columnType != "string") return info.setError("NAME_NOT_STRING");
        queryColumns.push_back("name");
      }

      if (paired) {
        if (second_seq) {
          if (! columns.Has("sequence2", columnType)) return info.setError("FLOW_SEQUENCE2_MISSING");
          if (columnType != "string") return info.setError("SEQUENCE2_NOT_STRING");
          queryColumns.push_back("sequence2");
 
          if (! columns.Has("quality2", columnType)) return info.setError("FLOW_QUALITY2_MISSING");
          if (columnType != "string") return info.setError("QUALITY2_NOT_STRING");
          queryColumns.push_back("quality2");

          if (! columns.Has("flowgram2", columnType)) return info.setError("FLOWGRAM2_MISSING");
          if (columnType != "string") return info.setError("FLOWGRAM2_NOT_STRING");
          queryColumns.push_back("flowgram2");
          
          if (! columns.Has("flow_indices2", columnType)) return info.setError("FLOW_INDICES2_MISSING");
          if (columnType != "string") return info.setError("FLOW_INDICES2_NOT_STRING");
          queryColumns.push_back("flow_indices2");
        }

        if (! columns.Has("clip_qual_left2", columnType)) return info.setError("FLOW_CLIP_QUAL_LEFT2_MISSING");
        if (columnType != "uint16") return info.setError("FLOW_CLIP_QUAL_LEFT2_NOT_UINT16");
        queryColumns.push_back("clip_qual_left2");
        
        if (! columns.Has("clip_qual_right2", columnType)) return info.setError("FLOW_CLIP_QUAL_RIGHT2_MISSING");
        if (columnType != "uint16") return info.setError("FLOW_CLIP_QUAL_RIGHT2_NOT_UINT16");
        queryColumns.push_back("clip_qual_right2");
        
        if (! columns.Has("clip_adapter_left2", columnType)) return info.setError("FLOW_CLIP_ADAPTER_LEFT2_MISSING");
        if (columnType != "uint16") return info.setError("FLOW_CLIP_ADAPTER_LEFT2_NOT_UINT16");
        queryColumns.push_back("clip_adapter_left2");
        
        if (! columns.Has("clip_adapter_right2", columnType)) return info.setError("FLOW_CLIP_ADAPTER_RIGHT2_MISSING");
        if (columnType != "uint16") return info.setError("FLOW_CLIP_ADAPTER_RIGHT2_NOT_UINT16");
        queryColumns.push_back("clip_adapter_right2");

        if (columns.Has("name2", columnType)) {
          if (columnType != "string") return info.setError("NAME2_NOT_STRING");
          queryColumns.push_back("name2");

          if (! columns.Has("name", columnType)) return info.setError("NAME_MISSING");
        }
      }

      numRows = int64_t(desc["length"]);
      return true;
    }

    bool validateData(const string &tableId, ReadsInfo &info) {
      cerr << "Total rows " << numRows << endl;
      
      DXGTable table(tableId);
      JSON query = JSON(JSON_NULL), data;
      int64_t offset = 0;
      
      (second_seq) ? v.Init(offset, flow_seq, flow_key, flow_seq2, flow_key2): v.Init(offset, flow_seq, flow_key);
      
      int64_t limit = 100000;
      while (offset < numRows) {
        try {
          data = table.getRows(query, queryColumns, offset, limit);
          if (int(data["length"]) == 0) break;
          if (! v.validate(queryColumns, data["data"], info)) return false;
          offset = v.getOffset();
        } catch(DXAPIError &e) {
          info.setData("API error: " + e.msg + ". ", 0);
          return info.setError("GTABLE_FETCH_FAIL", true);
        }
      
        return true;
      }
      return true;
    }

  public:
    FlowReadsValidator() {
      queryColumns = JSON(JSON_ARRAY);
      re_flow_seq.assign("^[ACGT]+$");
    };

    bool validate(const string &source_id, const JSON &desc, const JSON &details, ReadsInfo &info) {
      if (! validateDetails(details, info)) return false;
      if (! validateColumns(desc, info)) return false;
      return validateData(source_id, info);
    };
};
 
/*    bool validateLetterReads() {

      "columns":[{"name":"quality","type":"string"},{"name":"sequence","type":"string"},{"name":"name","type":"string"},{"name":"quality2","type":"string"},{"name":"sequence2","type":"string"},{"name":"name2","type":"string"}],
      if (! details["contigs"].has("names")) return setError("NAMES_MISSING");
      
      if (details["contigs"]["names"].type() != JSON_ARRAY) return setError("NAMES_NOT_ARRAY");

      if (details["contigs"]["names"].size() == 0) return setError("NAMES_EMPTY");

      map<string, int> names;
      map<string, int>::iterator it;
      for (int i = 0; i < details["contigs"]["names"].size(); i++) {
        msg.SetDataIndex(i, 0);

        if (details["contigs"]["names"][i].type() != JSON_STRING) return setError("NAME_NOT_STRING", true);

        string name = details["contigs"]["names"][i].get<string>();
        if (name.size() == 0) return setError("NAME_EMPTY", true);

        for (int j = 0; j < name.size(); j++) {
          if (! validASCII(name.at(j))) return setError("NAME_INVALID_CHARACTER", true);
        }

        it = names.find(name);
        if (it != names.end()) {
          msg.SetDataIndex(it->second, 1);
          return setError("NAME_DUPLICATE", true);
        }

        names[name] = i;
      }

      return true;
    }*/

/*    bool validateSize() {
      if (! details["contigs"].has("sizes")) return setError("SIZES_MISSING");
      
      if (details["contigs"]["sizes"].type() != JSON_ARRAY) return setError("SIZES_NOT_ARRAY");

      if (details["contigs"]["sizes"].size() != details["contigs"]["names"].size()) return setError("SIZES_NAMES_DIFFERENT_LENGTH");
      
      bool positive = true;

      for (int i = 0; i < details["contigs"]["sizes"].size(); i++) {
        msg.SetDataIndex(i, 0);

        if (details["contigs"]["sizes"][i].type() != JSON_INTEGER) return setError("SIZE_NOT_NON_NEGATIVE_INTEGER", true);
        
        int k = int(details["contigs"]["sizes"][i]);
        if (k < 0) return setError("SIZE_NOT_NON_NEGATIVE_INTEGER", true);
        
        if (k == 0) positive = false;
      }

      if (! positive) addWarning("SIZE_ZERO");

      return true;
    }
    
    bool validateOffsets() {
      if (details["contigs"]["offsets"].type() != JSON_ARRAY) return setError("OFFSETS_NOT_ARRAY");
      if (details["contigs"]["offsets"].size() != details["contigs"]["sizes"].size()) return setError("OFFSETS_SIZES_NOT_MATCH");

      map<int, int> sizes;
      map<int, int>::iterator it;
      vector<int> offsets;

      for (int i = 0; i < details["contigs"]["offsets"].size(); i++) {
        msg.SetDataIndex(i, 0);

        int s = int(details["contigs"]["sizes"][i]);
        it = sizes.find(s);
        if (it == sizes.end()) sizes[s] = 1;
        else it->second ++;

        int o = int(details["contigs"]["offsets"][i]);
        if (o < 0) return setError("OFFSET_NOT_NON_NEGATIVE_INTEGER", true);
        offsets.push_back(o);
      }

      sort(offsets.begin(), offsets.end());

      shift = offsets[0];
      if (shift != 0) addWarning("OFFSETS_NOT_START_WITH_ZERO");

      for (int i = 0; i < (offsets.size() -1); i++) {
        int k = offsets[i+1] - offsets[i];
        it = sizes.find(k);
        if (it == sizes.end()) return setError("OFFSETS_SIZES_NOT_MATCH");
        if (it->second <= 0) return setError("OFFSETS_SIZES_NOT_MATCH");
        it->second --;
      }

      return true;
    }

    bool validateDetails() {
      if (! details.has("contigs")) return setError("CONTIGS_MISSING");

      if (details["contigs"].type() != JSON_OBJECT) return setError("CONTIGS_NOT_HASH");

      if (! validateName()) return false;

      if (! validateSize()) return false;

      if (details["contigs"].has("offsets")) return validateOffsets();

      return true;
    }

    bool validateOtherArray(const JSON &other, int chrSize) {
      if (other.size() == 0) {
        addWarning("PLOIDY_OTHER_VALUE_EMPTY_ARRAY", true);
        return true;
      }

      vector<PloidyRegion> regions;

      for (int i = 0; i < other.size(); i++) {
        msg.SetDataIndex(i, 2);

        if (other[i].type() != JSON_HASH) return setError("PLOIDY_OTHER_VALUE_NOT_HASH", true);

        if ((! other[i].has("start")) || (! other[i].has("end")) || (! other[i].has("ploidy"))) return setError("PLOIDY_OTHER_START_END_PLOIDY_MISSING", true);

        if (other[i]["start"].type() != JSON_INTEGER) return setError("PLOIDY_OTHER_START_NOT_NON_NEGATIVE_INTEGER", true);
        if (other[i]["end"].type() != JSON_INTEGER) return setError("PLOIDY_OTHER_END_NOT_NON_NEGATIVE_INTEGER", true);
        if (other[i]["ploidy"].type() != JSON_INTEGER) return setError("PLOIDY_OTHER_PLOIDY_NOT_NON_NEGATIVE_INTEGER", true);

        PloidyRegion r;
        r.start = int(other[i]["start"]); r.end = int(other[i]["end"]); r.ploidy = int(other[i]["ploidy"]); r.index = i;

        if (r.start < 0) return setError("PLOIDY_OTHER_START_NOT_NON_NEGATIVE_INTEGER", true);
        if (r.ploidy < 0) return setError("PLOIDY_OTHER_PLOIDY_NOT_NON_NEGATIVE_INTEGER", true);

        if ((r.start >= chrSize) || (r.start >= r.end) ) return setError("PLOIDY_OTHER_START_TOO_LARGE", true);
        if (r.end > chrSize) return setError("PLOIDY_OTHER_END_TOO_LARGE", true);

        regions.push_back(r);
      }

      sort(regions.begin(), regions.end());
      for (int i = 1; i < regions.size(); i++) {
        if (regions[i].start < regions[i-1].end) {
          if (regions[i].ploidy != regions[i-1].ploidy) {
            msg.SetDataIndex(regions[i-1].index, 2);
            msg.SetDataIndex(regions[i].index, 3);
            return setError("PLOIDY_OTHER_REGION_OVERLAP_DIFF", true);
          }

          if (regions[i].end < regions[i-1].end) {
            regions[i].end = regions[i-1].end;
            regions[i].index = regions[i-1].index;
          }
        }
      }

      return true;
    }

    bool validateOther(JSON &other) {
      if (other.type() != JSON_HASH) return setError("PLOIDY_OTHER_NOT_HASH", true);
      
      map<string, int>::iterator ch;
      for (JSON::object_iterator it = other.object_begin(); it != other.object_end(); it++) {
        msg.SetData(it->first, 1);

        ch = chromSize.find(it->first);
        if (ch == chromSize.end()) return setError("PLOIDY_OTHER_KEY_INVALID", true);

        if (it->second.type() == JSON_INTEGER) {
          if (int(it->second) < 0) return setError("PLOIDY_OTHER_VALUE_INVALID", true);
        } else if (it->second.type() == JSON_ARRAY) {
          if (! validateOtherArray(it->second, ch->second)) return false;
        } else {
          return setError("PLOIDY_OTHER_VALUE_INVALID", true);
        }
      }

      return true;
    }

    bool validateGender(const string &gender, JSON &hash) {
      msg.SetData(gender, 0);

      if (hash.type() != JSON_HASH) return setError("PLOIDY_GENDER_NOT_HASH", true);

      if (! hash.has("default")) return setError("PLOIDY_DEFAULT_MISSING", true);

      if (hash["default"].type() != JSON_INTEGER) return setError("PLOIDY_DEFAULT_NOT_NON_NEGATIVE_INTEGER", true);

      if (int(hash["default"]) < 0) return setError("PLOIDY_DEFAULT_NOT_NON_NEGATIVE_INTEGER", true);

      if (hash.has("other")) {
        if (! validateOther(hash["other"])) return false;
      }
        
      for (JSON::object_iterator it = hash.object_begin(); it != hash.object_end(); it++) {
        if ((it->first != "default") && (it->first != "other")) {
          addWarning("PLOIDY_UNKNOWN_KEY", true);
          break;
        }
      }

      return true;
    }

    bool validateGenome() {
      if (! details.has("ploidy")) return setError("PLOIDY_MISSING");
      
      if (details["ploidy"].type() != JSON_HASH) return setError("PLOIDY_NOT_HASH");

      chromSize.clear();
      for(int i = 0; i < details["contigs"]["names"].size(); i++)
        chromSize[details["contigs"]["names"][i].get<string>()] = int(details["contigs"]["sizes"][i]);
      
      for (JSON::object_iterator it = details["ploidy"].object_begin(); it != details["ploidy"].object_end(); it++) {
        if (! validateGender(it->first, it->second)) return false;
      }

      return true;
    }

    bool validateSequenceFile() {
      cerr << "Validating sequences ... " << endl;
      int64_t totalS = int64_t(fileDesc["size"]), k = shift;
      for (int i=0; i < details["contigs"]["sizes"].size(); i++)
        k += int64_t(details["contigs"]["sizes"][i]);

      if (k > totalS) return setError("FLAT_TOO_SHORT", true);
      if (k < totalS) addWarning("FLAT_TOO_LONG", true);

      int length = 1000000;
      int64_t count = 0, iteration = 0;
      vector<char> buffer(length);
      bool lowerCase = false;

      while(true) {
        flat_file.read(&(buffer[0]), length);
        int k = flat_file.gcount();
        if (k == 0) break;

        for (int i = 0; i < k; i++) {
          switch(buffer[i]) {
            case 'A':
            case 'C':
            case 'G':
            case 'T':
            case 'U':
            case 'R':
            case 'Y':
            case 'S':
            case 'W':
            case 'K':
            case 'M':
            case 'B':
            case 'D':
            case 'H':
            case 'V':
            case 'N':
            case '.':
            case '-':
              break;
            case 'a':
            case 'c':
            case 'g':
            case 't':
            case 'u':
            case 'r':
            case 'y':
            case 's':
            case 'w':
            case 'k':
            case 'm':
            case 'b':
            case 'd':
            case 'h':
            case 'v': lowerCase = true;
              break;
            default:
              msg.SetData(boost::lexical_cast<string>(count + i), 1);
              return setError("FLAT_INVALID_CHARACTER", true);
          }
        }
        iteration ++;
        count += k;

        if ((iteration % 10) == 0) cerr << count << " bytes of sequences validated" << endl;
      }

      if (lowerCase) addWarning("FLAT_LOWER_CASE", true);

      return true;
    }

    bool validateSequence() {
      if (! details["contigs"].has("offsets")) return setError("OFFSETS_MISSING");

      if (details["flat_sequence_file"].type() != JSON_OBJECT) return setError("FLAT_INVALID_OBJECT");
      if (! details["flat_sequence_file"].has("$dnanexus_link")) return setError("FLAT_INVALID_OBJECT");

      // This check is not necessary since API server verifies it
      //if (details["flat_sequence_file"]["$dnanexus_link"].type() != JSON_STRING) return setError(msg.Get("FLAT_INVALID_OBJECT"));
      
      string file_id = details["flat_sequence_file"]["$dnanexus_link"].get<string>();

      if (! validateOffsets()) return false;

      flat_file.setIDs(file_id);
      try {
        fileDesc = flat_file.describe();
      } catch(DXAPIError &e) {
        info["error"] = "API error: " + e.msg + ". " + msg.GetError("FLAT_FETCH_FAIL");
        return false;
      }

      msg.SetData(file_id, 0);

      if (fileDesc["class"].get<string>() != "file") return setError("FLAT_NOT_FILE", true);

      if (fileDesc["state"].get<string>() != "closed") return setError("FLAT_NOT_CLOSED", true);

      if (! bool(fileDesc["hidden"])) addWarning("FLAT_NOT_HIDDEN", true);

      try {
        return validateSequenceFile();
      } catch (DXAPIError &e) {
        msg.SetData(file_id, 0);
        info["error"] = "API error: " + e.msg + ". " + msg.GetError("FLAT_DOWNLOAD_FAIL", true);
        info.erase("valid");
        return false;
      }
    }*/
class ReadsValidator {
  public:
    ReadsValidator() {};

    JSON Validate(const string &source_id) {
      ReadsInfo info;
      info.init(source_id);
      JSON desc, details;

      cerr << "Start validating ... " << endl;
      DXGTable table(source_id);
      try {
        desc = table.describe();
        details = table.getDetails();
      } catch (DXAPIError &e) {
        info.setData("API error: " + e.msg + ". ", 0);
        info.setError("GTABLE_FETCH_FAIL", true);
        JSON output = info.getInfo();
        output.erase("valid");
        return output;
      }

      ReadsTypeValidator v1;
      if (! v1.validate(desc, details, info)) return info.getInfo();

      string type = v1.getType();
      info.setType(type);

      if (type == "letter") {
        LetterColorReadsValidator v2(false);
        if (! v2.validate(source_id, desc, details, info)) return info.getInfo();
      } else if (type == "color") {
        LetterColorReadsValidator v2(true);
        if (! v2.validate(source_id, desc, details, info)) return info.getInfo();
      } else if (type == "flow") {
        FlowReadsValidator v2;
        if (! v2.validate(source_id, desc, details, info)) return info.getInfo();
      } 

//      if (! validatePaired()) return info;
/*      if (types.Has("LetterReads")) {
        if (! validateLetterReads()) return info;
      }
      if (! validateDetails()) return info;

      if (! hasString(desc["types"], "Genome")) {
        if (details.has("ploidy")) addWarning("TYPE_NOT_GENOME");
      } else {
        if (! validateGenome()) return info;
      }

      if (details.has("flat_sequence_file")) {
        if (! validateSequence()) return info;
      }

      if (hasString(desc["types"], "Genome")) info["isGenome"] = true;*/
      return info.getInfo();
    } 
};

int Main(const vector<string> &args)
{
  ReadsValidator v;

  if ((args.size() == 1) && (args[0] == "--as-program")){
    JSON input = readJSON("job_input.json");
    string source_id = input["source"]["$dnanexus_link"].get<string>();
    JSON output = v.Validate(source_id);
    cerr << "Validation done" << endl;
    writeJSON(output, "job_output.json");
  } else if (args.size() == 1) {
    JSON output = v.Validate(args[0]);
    cerr << "Validation done" << endl;
    cout << output.toString() << endl;
  } else {
    cerr << "To run this as a program in the platform:" << endl
         << "  reads_validator --as-program" << endl << endl
         << "To run this as a command-line utility (requires environment variables "
         << "DX_APISERVER_HOST, DX_APISERVER_PORT, DX_SECURITY_CONTEXT):" << endl
         << "  reads_validator <source_id>" << endl;
    return 1;
  }

  return 0;
}

int main(int argc, char *argv[])
{
  vector<string> args;
  for (int i = 1; i != argc; ++i)
    args.push_back(argv[i]);

  return Main(args);
}
