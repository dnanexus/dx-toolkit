#ifndef CONTIGSET_ERRORMSG_H
#define CONTIGSET_ERRORMSG_H

#include <boost/lexical_cast.hpp>
#include "dxjson/dxjson.h"
#include <string>
#include <fstream>
#include <boost/algorithm/string.hpp>
#include <set>

using namespace dx;
using namespace std;

class TypesHandler {
  private:
    bool duplicate;
    set<string> types;
    set<string>::iterator it;

  public:
    TypesHandler() {};

    void Add(const JSON &t) {
      types.clear();
      duplicate = false;

      for (int i = 0; i < t.size(); i++) {
        string type = t[i].get<string>();
        if (Has(type)) {
          duplicate = true;
        } else {
          types.insert(type);
        }
      }
    };

    bool HasDuplicate() { return duplicate; };

    bool Has(const string &type) {
      it = types.find(type);
      return (it != types.end());
    };
};

class ColumnsHandler {
  private:
    map<string, string> columns;
    map<string, string>::iterator it;

  public:
    ColumnsHandler() {};

    void Add(const JSON &c) {
      columns.clear();
      for (int i = 0; i < c.size(); i++)
        columns[c[i]["name"].get<string>()] = c[i]["type"].get<string>();
    };

    bool Has(const string &c, string &type) {
      it = columns.find(c);
      if (it == columns.end()) return false;
      type = it->second;
      return true;
    };
};

class ReadsErrorMsg {
  private:
    JSON errorMsg;
    JSON warningMsg;
    string msg;
    vector<string> msgData;
    
    string replaceStr() {
      for (int i = 0; i < msgData.size(); i++) {
        string marker = "{" + boost::lexical_cast<string>(i+1) + "}";
        size_t found = msg.find(marker);
        if (found != string::npos) msg.replace(found, marker.size(), msgData[i]);
      }
      return msg;
    }

  public: 
    ReadsErrorMsg () {
      errorMsg = JSON(JSON_OBJECT);
      errorMsg["GTABLE_FETCH_FAIL"] = "Fail to fetch description and details of the object";
      errorMsg["CLASS_NOT_GTABLE"] = "Object is not a gtable";
      errorMsg["DETAILS_NOT_HASH"] = "Object 'details' is not a hash";
      errorMsg["GTABLE_NOT_CLOSED"] = "Object is not closed";
      errorMsg["TYPE_NOT_READS"] = "Object is not a Reads type";
      errorMsg["TYPE_CONFLICT"] = "Object cannot have type {1} and {2} at the same time";

      errorMsg["DETAILS_PAIRED_INVALID"] = "'paired' in object details is not a boolean value";
      
      errorMsg["COLOR_SEQUENCE_TYPE_INVALID"] = "Object contains color reads, but its sequence_type in the details is not 'color'";
      errorMsg["FLOW_SEQUENCE_TYPE_INVALID"] = "Object contains flow reads, but its sequence_type in the details is not 'flow'";

      errorMsg["SEQUENCE_MISSING"] = "Object does not have column 'sequence'";
      errorMsg["SEQUENCE_NOT_STRING"] = "Type of column 'sequence' is not string";
      errorMsg["LETTER_SEQUENCE2_MISSING"] = "Object contains paired letter reads, but does not have column 'sequence2'";
      errorMsg["COLOR_SEQUENCE2_MISSING"] = "Object contains paired color reads, but does not have column 'sequence2'";
      errorMsg["FLOW_SEQUENCE2_MISSING"] = "Object contains paired flow reads with 'pair_second_flow' set to be true in the details, but it does not have column 'sequence2'";
      errorMsg["SEQUENCE2_NOT_STRING"] = "Type of column 'sequence2' is not string";

      errorMsg["LETTER_QUALITY_MISSING"] = "Object is supposed to contain paired letter reads. It has column 'quality2', but does not have column 'quality'";
      errorMsg["COLOR_QUALITY_MISSING"] = "Object is supposed to contain paired color reads. It has column 'quality2', but does not have column 'quality'";
      errorMsg["FLOW_QUALITY_MISSING"] = "Object contains flow reads, but does not have column 'quality'";
      errorMsg["QUALITY_NOT_STRING"] = "Type of column 'quality' is not string";
      errorMsg["LETTER_QUALITY2_MISSING"] = "Object is supposed to contain paired letter reads. It has column 'quality', but does not have column 'quality2'";
      errorMsg["COLOR_QUALITY2_MISSING"] = "Object is supposed to contain paired color reads. It has column 'quality', but does not have column 'quality2'";
      errorMsg["FLOW_QUALITY2_MISSING"] = "Object contains paired flow reads with 'pair_second_flow' being true in the details, but it does not have column 'quality2'";
      errorMsg["QUALITY2_NOT_STRING"] = "Type of column 'quality2' is not string";
      
      errorMsg["NAME_NOT_STRING"] = "Type of column 'name' is not string";
      errorMsg["NAME2_NOT_STRING"] = "Type of column 'name2' is not string";
      errorMsg["NAME_MISSING"] = "Object has column 'name2', but does not have column 'name'";
       
      errorMsg["PAIR_SECOND_FLOW_MISSING"] = "Object contains paired flow reads, but does not have 'pair_second_flow' set in the details";
      errorMsg["PAIR_SECOND_FLOW_NOT_BOOLEAN"] = "'pair_second_flow' in the details is not a boolean value";

      errorMsg["FLOW__SEQUENCE_MISSING"] = "Object details do not contain 'flow_sequence'";
      errorMsg["FLOW__SEQUENCE_NOT_STRING"] = "'flow_sequence' in object details is not a string";
      errorMsg["FLOW__SEQUENCE_INVALID_CHARACTER"] = "In object details, 'flow_sequence' has characters other than {A, C, G, T}";
      errorMsg["FLOW__SEQUENCE_SAME_CONSECUTIVE"] = "In object details, some consecutive letters in 'flow_sequence' are the same";
      errorMsg["FLOW__SEQUENCE2_MISSING"] = "Object contains paired flow reads with 'pair_second_flow' set to be true, but it does not have 'flow_sequence2' in the dtails";
      errorMsg["FLOW__SEQUENCE2_NOT_STRING"] = "'flow_sequence2' in object details is not a string";
      errorMsg["FLOW__SEQUENCE2_INVALID_CHARACTER"] = "In object details, 'flow_sequence2' has characters other than {A, C, G, T}";
      errorMsg["FLOW__SEQUENCE2_SAME_CONSECUTIVE"] = "In object details, some consecutive letters in 'flow_sequence2' are the same";

      errorMsg["FLOW__KEY_MISSING"] = "Object details do not contain 'flow_key'";
      errorMsg["FLOW__KEY_NOT_STRING"] = "'flow_key' in object details is not a string";
      errorMsg["FLOW__KEY_INVALID_CHARACTER"] = "In object details, 'flow_key' has characters other than {A, C, G, T}";
      errorMsg["FLOW__KEY2_MISSING"] = "Object contains paired flow reads with 'pair_second_flow' set to be true, but it does not have 'flow_key2' in the details";
      errorMsg["FLOW__KEY2_NOT_STRING"] = "'flow_key' in object details is not a string";
      errorMsg["FLOW__KEY2_INVALID_CHARACTER"] = "In object details, 'flow_key2' has characters other than {A, C, G, T}";


      errorMsg["FLOWGRAM_MISSING"] = "Object contains flow reads, but does not have column 'flowgram'";
      errorMsg["FLOWGRAM_NOT_STRING"] = "Type of column 'flowgram' is not string";
      errorMsg["FLOWGRAM2_MISSING"] = "Object contains paired flow reads with 'pair_second_flow' being true in the details, but it  does not have column 'flowgram2'";
      errorMsg["FLOWGRAM2_NOT_STRING"] = "Type of column 'flowgram2' is not string";
      
      errorMsg["FLOW_INDICES_MISSING"] = "Object contains flow reads, does not have column 'flow_indices'";
      errorMsg["FLOW_INDICES_NOT_STRING"] = "Type of column 'flow_indices' is not string";
      errorMsg["FLOW_INDICES2_MISSING"] = "Object contains paired flow reads with 'pair_second_flow' begin true in the details, but it does not have column 'flow_indices'";
      errorMsg["FLOW_INDICES2_NOT_STRING"] = "Type of column 'flow_indices2' is not string";

      errorMsg["FLOW_CLIP_QUAL_LEFT_MISSING"] = "Object contains flow reads, but does not have column 'clip_qual_left'";
      errorMsg["FLOW_CLIP_QUAL_LEFT_NOT_UINT16"] = "Type of column 'clip_qual_left' is not uint16";
      errorMsg["FLOW_CLIP_QUAL_RIGHT_MISSING"] = "Object contains flow reads, but does not have column 'clip_qual_right'";
      errorMsg["FLOW_CLIP_QUAL_RIGHT_NOT_UINT16"] = "Type of column 'clip_qual_right' is not uint16";
      errorMsg["FLOW_CLIP_ADAPTER_LEFT_MISSING"] = "Object contains flow reads, but does not have column 'clip_adapter_left'";
      errorMsg["FLOW_CLIP_ADAPTER_LEFT_NOT_UINT16"] = "Type of column 'clip_adapter_left' is not uint16";
      errorMsg["FLOW_CLIP_ADAPTER_RIGHT_MISSING"] = "Object contains flow reads, but does not have column 'clip_adapter_right'";
      errorMsg["FLOW_CLIP_ADAPTER_RIGHT_NOT_UINT16"] = "Type of column 'clip_adapter_right' is not uint16";
      errorMsg["FLOW_CLIP_QUAL_LEFT2_MISSING"] = "Object contains paired flow reads, but does not have column 'clip_qual_left2'";
      errorMsg["FLOW_CLIP_QUAL_LEFT2_NOT_UINT16"] = "Type of column 'clip_qual_left2' is not uint16";
      errorMsg["FLOW_CLIP_QUAL_RIGHT2_MISSING"] = "Object contains paired flow reads, but does not have column 'clip_qual_right2'";
      errorMsg["FLOW_CLIP_QUAL_RIGHT2_NOT_UINT16"] = "Type of column 'clip_qual_right2' is not uint16";
      errorMsg["FLOW_CLIP_ADAPTER_LEFT2_MISSING"] = "Object contains paired flow reads, but does not have column 'clip_adapter_left2'";
      errorMsg["FLOW_CLIP_ADAPTER_LEFT2_NOT_UINT16"] = "Type of column 'clip_adapter_left2' is not uint16";
      errorMsg["FLOW_CLIP_ADAPTER_RIGHT2_MISSING"] = "Object contains paired flow reads, but does not have column 'clip_adapter_right2'";
      errorMsg["FLOW_CLIP_ADAPTER_RIGHT2_NOT_UINT16"] = "Type of column 'clip_adapter_right2' is not uint16";
      
      errorMsg["QUALITY_SEQUENCE_NOT_MATCH"] = "In {1} read, lengths of sequence and quality do not match";
      errorMsg["QUALITY2_SEQUENCE2_NOT_MATCH"] = "In {1} read, lengths of sequence2 and quality2 do not match";
      errorMsg["QUALITY_NOT_PHRED33"] = "Quality of {1} read is not encoded in ASCII PHRED-33";
      errorMsg["QUALITY2_NOT_PHRED33"] = "Quality2 of {1} read is not encoded in ASCII PHRED-33";
      
      errorMsg["FLOW_KEY_SEQUENCE_NOT_MATCH"] = "Sequence of {1} read does not start with 'flow_key' in object details";
      errorMsg["FLOW_KEY2_SEQUENCE2_NOT_MATCH"] = "Sequence2 of {1} read does not start with 'flow_key2' in object details";
      errorMsg["FLOWGRAM_INVALID_LENGTH"] = "In {1} read, the length of flowgram is not exactly 4 times the length of 'flow_sequence' in object details";
      errorMsg["FLOWGRAM_INVALID_CHARACTER"] = "In {1} read, flowgram is not a string of concatenated hex numbers";
      errorMsg["FLOWGRAM2_INVALID_LENGTH"] = "In {1} read, the length of flowgram2 is not exactly 4 times the length of 'flow_sequence' in object details";
      errorMsg["FLOWGRAM2_INVALID_CHARACTER"] = "In {1} read, flowgram2 is not a string of concatenated hex numbers";
      errorMsg["FLOW_INDICES_INVALID_LENGTH"] = "In {1} read, the length of flow_indices is not exactly 2 times the length of sequence";
      errorMsg["FLOW_INDICES_INVALID_CHARACTER"] = "In {1} read, flow_indices is not a string of concatenated hex numbers";
      errorMsg["FLOW_INDICES_SEQUENCE_NOT_MATCH"] = "Sequence and flow_indices of {1} reads do not match 'flow_sequence' in object details";
      errorMsg["FLOW_INDICES2_INVALID_LENGTH"] = "In {1} read, the length of flow_indices2 is not exactly 2 times the length of sequence2";
      errorMsg["FLOW_INDICES2_INVALID_CHARACTER"] = "In {1} read, flow_indices2 is not a string of concatenated hex numbers";
      errorMsg["FLOW_INDICES2_SEQUENCE2_NOT_MATCH"] = "Sequence2 and flow_indices2 of {1} read do match 'flow_sequence2' in object details";
      
      warningMsg = JSON(JSON_OBJECT);
      warningMsg["TYPE_MISSING"] = "Objects is neither LetterReads nor ColorReads nor FlowReads";
      warningMsg["ORIGINAL_FILES_INVALID"] = "'original_files' in object details is not an array of DNAnexus links";
      warningMsg["ORIGINAL_FILE_INVALID"] = "One or multiple entries in 'original_files' in object details are not a valid DNAnexus links to a file object";
      
      warningMsg["LETTER_WITH_SEQUENCE_TYPE"] = "LetterReads shall not have 'sequence_type' in the details";

      warningMsg["SEQUENCE_INVALID"] = "One or multiple read sequences do not conform to regular expression {1}, e.g., {2} read";
      warningMsg["NAME_INVALID"] = "One or multiple read names do not conform to regular expression {1}, e.g., {2} read";

      warningMsg["SEQUENCE_EMPTY_AFTER_TRIMMING"] = "In some reads, such as {1}, sequence/sequence2 would be empty after trimming";

      warningMsg["PAIR_ORIENTATION_INVALID"] = "'pair_orientation' in details is not a string of either 'FF', 'FR', 'RF', or 'RR'";
      warningMsg["PAIR_MIN_DIST_INVALID"] = "'pair_min_dist' in details is not a number";
      warningMsg["PAIR_MAX_DIST_INVALID"] = "'pair_max_dist' in details is not a number";
      warningMsg["PAIR_AVG_DIST_INVALID"] = "'pair_avg_dist' in details is not a number";
      warningMsg["PAIR_STDDEV_DIST_INVALID"] = "'pair_stddev_dist' in details is not a number";
    };

    void SetData(const string &msgD, uint32_t pos) {
      msgData.resize(pos+1);
      msgData[pos] = msgD;
    }

    void SetDataIndex(int64_t index, uint32_t pos) {
      string str = boost::lexical_cast<string>(index+1);
      switch(index) {
        case 0: str += "st";
          break;
        case 1: str += "nd";
          break;
        case 2: str += "rd";
          break;
        default: str += "th";
      }
      SetData(str, pos);
    }

    string GetError(const string &err, bool replace = false) { 
      msg =  errorMsg[err].get<string>();
      return (replace) ? replaceStr() : msg;
    }
    
    string GetWarning(const string &w, bool replace = false) {
      msg = warningMsg[w].get<string>();
      return (replace) ? replaceStr() : msg;
    }
};

JSON readJSON(const string &filename) {
  JSON input;
  ifstream in(filename.c_str());
  input.read(in);
  in.close();
  return input;
}

void writeJSON(const JSON &input, const string &filename) {
  ofstream out(filename.c_str());
  out << input.toString();
  out.close();
}

bool hasString(const JSON &json, const string &val) {
  if (json.type() != JSON_ARRAY) return false;
  for (int i = 0; i < json.size(); i++) {
    if (json[i].get<string>() == val) return true;
  }
  return false;
}

bool validASCII(char ch) {
  int c = ch;
  return (c >= 33) && (c <= 127);
}

string myPath() {
  char buff[10000];
  size_t len = readlink("/proc/self/exe", buff, 9999);
  buff[len] = '\0';
  string ret_val = string(buff);
  int k = ret_val.find_last_of('/');
  return ret_val.substr(0, k);
}

bool exec(const string &cmd, string &out) {
  cerr << cmd << endl;
  FILE* pipe = popen((cmd + " 2>/dev/null").c_str(), "r");
  if (!pipe) return false;
  char buffer[1024];
  out = "";
  while(!feof(pipe)) {
    if(fgets(buffer, 1024, pipe) != NULL) out += buffer;
  }
  pclose(pipe);

  boost::algorithm::trim(out);
  return true;
}

#endif
