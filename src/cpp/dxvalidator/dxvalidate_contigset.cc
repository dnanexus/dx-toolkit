#include "dxvalidate_contigset.h"

using namespace dx;

ContigSetReader::ContigSetReader(const string &id, ValidateInfo *m) {
  msg = m;

  ready = fetchContigSet(id);
  if (ready) ready = validateType();
  if (ready) ready = validateDetails();
  if (ready) {
    hasFlat = details.has("flat_sequence_file");
    if (hasFlat) ready = initFlatFile(details);
  }
}

bool ContigSetReader::fetchContigSet(const string &source_id) {
  DXRecord object(source_id);
  try {
    desc = object.describe();
    details = object.getDetails();
  } catch (DXAPIError &e) {
    if (e.resp_code == 404) {
      msg->setError("CONTIGSET_INVALID");
    } else {
      msg->setDXError(e.msg, "CONTIGSET_FETCH_FAIL");
    }
    return false;
  }

  return true;
}

bool ContigSetReader::validateType() {
  if (desc["class"].get<string>() != "record") return msg->setError("CONTIGSET_NOT_RECORD");

  types.Add(desc["types"]);
  if (! types.Has("ContigSet")) return msg->setError("TYPE_NOT_CONTIGSET");
  if (desc["state"].get<string>() != "closed") msg->addWarning("CONTIGSET_NOT_CLOSED");
  if (details.type() != JSON_OBJECT) return msg->setError("CONTIGSET_DETAILS_NOT_HASH");
  return true;
}

bool ContigSetReader::validateDetails() {
  if (! details.has("contigs")) return msg->setError("CONTIGS_MISSING");
  if (details["contigs"].type() != JSON_OBJECT) return msg->setError("CONTIGS_NOT_HASH");
  
  if (! validateContigSetName()) return false;
  if (! validateContigSetSize()) return false;

  hasOffset = details["contigs"].has("offsets");
  if (hasOffset) return validateContigSetOffset();
  return true;
}

bool ContigSetReader::validateContigSetName() {
  if (! details["contigs"].has("names")) return msg->setError("CONTIGS_NAMES_MISSING");
  if (details["contigs"]["names"].type() != JSON_ARRAY) return msg->setError("CONTIGS_NAMES_NOT_ARRAY");
  if (details["contigs"]["names"].size() == 0) return msg->setError("CONTIGS_NAMES_EMPTY");

  names.clear();
  map<string, int>::iterator it;
  for (int i = 0; i < details["contigs"]["names"].size(); i++) {
    msg->setDataIndex(i, 0);
    
    if (details["contigs"]["names"][i].type() != JSON_STRING) return msg->setError("CONTIGS_NAME_NOT_STRING", true);
    string name = details["contigs"]["names"][i].get<string>();
    if (name.size() == 0) return msg->setError("CONTIGS_NAME_EMPTY", true);
    
    for (int j = 0; j < name.size(); j++) {
      if (! validASCII(name.at(j))) return msg->setError("CONTIGS_NAME_INVALID_CHARACTER", true);
    }
    
    it = names.find(name);
    if (it != names.end()) {
      msg->setDataIndex(it->second, 1);
      return msg->setError("CONTIGS_NAME_DUPLICATE", true);
    }
    names[name] = i;
  }
  return true;
}

bool ContigSetReader::validateContigSetSize() {
  if (! details["contigs"].has("sizes")) return msg->setError("CONTIGS_SIZES_MISSING");
  if (details["contigs"]["sizes"].type() != JSON_ARRAY) return msg->setError("CONTIGS_SIZES_NOT_ARRAY");
  if (details["contigs"]["sizes"].size() != details["contigs"]["names"].size()) return msg->setError("CONTIGS_SIZES_NAMES_DIFFERENT_LENGTH");
  
  bool positive = true;
  sizes.resize(details["contigs"]["sizes"].size());

  for (int i = 0; i < details["contigs"]["sizes"].size(); i++) {
    msg->setDataIndex(i, 0);
    
    if (details["contigs"]["sizes"][i].type() != JSON_INTEGER) return msg->setError("CONTIGS_SIZE_NOT_NON_NEGATIVE_INTEGER", true);
    
    int64_t k = int64_t(details["contigs"]["sizes"][i]);
    if (k < 0) return msg->setError("CONTIGS_SIZE_NOT_NON_NEGATIVE_INTEGER", true);
    
    if (k == 0) positive = false;
    sizes[i] = k;
  }
  
  if (! positive) msg->addWarning("CONTIGS_SIZE_ZERO");
  return true;
}

bool ContigSetReader::validateContigSetOffset() {
  if (details["contigs"]["offsets"].type() != JSON_ARRAY) return msg->setError("CONTIGS_OFFSETS_NOT_ARRAY");
  if (details["contigs"]["offsets"].size() != details["contigs"]["sizes"].size()) return msg->setError("CONTIGS_OFFSETS_SIZES_NOT_MATCH");
  
  map<int64_t, int> t_sizes;
  map<int64_t, int>::iterator it;
  vector<int64_t> t_offsets;

  offsets.clear();
  
  for (int i = 0; i < details["contigs"]["offsets"].size(); i++) {
    msg->setDataIndex(i, 0);
    
    int64_t s = int64_t(details["contigs"]["sizes"][i]);
    it = t_sizes.find(s);
    if (it == t_sizes.end()) t_sizes[s] = 1;
    else it->second ++;
    
    int64_t o = int64_t(details["contigs"]["offsets"][i]);
    if (o < 0) return msg->setError("CONTIGS_OFFSET_NOT_NON_NEGATIVE_INTEGER", true);
    t_offsets.push_back(o);
    offsets.push_back(o);
  }
  
  sort(t_offsets.begin(), t_offsets.end());
  offsetShift = t_offsets[0];
  
  if (offsetShift != 0) msg->addWarning("CONTIGS_OFFSETS_NOT_START_WITH_ZERO");
  for (int i = 0; i < (t_offsets.size() -1); i++) {
    int64_t k = t_offsets[i+1] - t_offsets[i];
    it = t_sizes.find(k);
    if (it == t_sizes.end()) return msg->setError("CONTIGS_OFFSETS_SIZES_NOT_MATCH");
    if (it->second <= 0) return msg->setError("CONTIGS_OFFSETS_SIZES_NOT_MATCH");
    it->second --;
  }
  
  return true;
}

bool ContigSetReader::initFlatFile(const JSON &details) {
  if(! hasOffset) return msg->setError("CONTIGS_OFFSETS_MISSING");
  
  if (details["flat_sequence_file"].type() != JSON_OBJECT) return msg->setError("CONTIGSET_FLAT_INVALID");
  if (! details["flat_sequence_file"].has("$dnanexus_link")) return msg->setError("CONTIGSET_FLAT_INVALID");

  flatFile.setIDs(details["flat_sequence_file"]["$dnanexus_link"].get<string>());

  try {
    fileDesc = flatFile.describe();
  } catch (DXAPIError &e) {
    if (e.resp_code == 404) return msg->setError("CONTIGSET_FLAT_INVALID");
    return msg->setDXError(e.msg, "CONTIGSET_FLAT_FETCH_FAIL");
  }

  if (fileDesc["class"].get<string>() != "file") return msg->setError("CONTIGSET_FLAT_NOT_FILE");
  if (fileDesc["state"].get<string>() != "closed") return msg->setError("CONTIGSET_FLAT_NOT_CLOSED");
  if (! bool(fileDesc["hidden"])) msg->addWarning("CONTIGSET_FLAT_NOT_HIDDEN");
  
  int64_t totalS = int64_t(fileDesc["size"]), k = offsetShift;
  for (int i = 0; i < sizes.size(); i++)
    k += sizes[i];
  if (k > totalS) return msg->setError("CONTIGSET_FLAT_TOO_SHORT", true);
  if (k < totalS) msg->addWarning("CONTIGSET_FLAT_TOO_LONG", true);

  return true;
}

bool ContigSetReader::fetchSeq(int64_t pos, char *buffer, int bufSize) {
  try {
    flatFile.seek(pos);
    flatFile.read(buffer, bufSize);
  } catch (DXError &e) {
    return msg->setDXError(e.msg, "CONTIGSET_FLAT_SEQUENCE_FETCH_FAIL");
  }

  return true;
}

bool ContigSetReader::validateSequence() {
  cerr << "Validating sequences ... " << endl;
  
  bool lowerCase = false;
  int64_t count = 0, iteration = 0;
  string buffer;

  try {
    flatFile.startLinearQuery(-1, -1, 5000000, 1);

    while(flatFile.getNextChunk(buffer)) {
      int k = buffer.length();
      if (k == 0) break;

      for (int i = 0; i < k; i++) {
        if (! validateChar(buffer[i], lowerCase)) {
          msg->setData(boost::lexical_cast<string>(count + i), 1);
          return msg->setError("CONTIGSET_FLAT_INVALID_CHARACTER", true);
        }
      }

      iteration ++;
      count += k;
    
      if ((iteration % 20) == 0) cerr << count << " bytes of sequences validated" << endl;
    }
  } catch (DXError &e) {
    return msg->setDXError(e.msg, "CONTIGSET_FLAT_FETCH_FAIL");
  }

  cerr << count << " bytes of sequences validated" << endl;
  
  flatFile.stopLinearQuery();
  if (lowerCase) msg->addWarning("CONTIGSET_FLAT_LOWER_CASE", true);
  return true;
}

bool ContigSetReader::validateChar(char &ch, bool &lowerCase) {
  switch(ch) {
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
    case '-': break;
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
    default: return false;
  }

  return true;
}

int ContigSetReader::chrIndex(const string &name) {
  map<string, int>::iterator it;
  it = names.find(name);
  if (it == names.end()) return -1;
  return it->second;
}

int64_t ContigSetReader::chrSize(int i) {
  if ((i < 0) || (i >= sizes.size())) return -1;
  return sizes[i];
}

int64_t ContigSetReader::chrOffset(int i) {
  if (! hasOffset) return -1;
  if ((i < 0) || ( i >=offsets.size())) return -1;
  return offsets[i];
}
