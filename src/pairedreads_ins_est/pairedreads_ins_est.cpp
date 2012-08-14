#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <stdlib.h>
#include <algorithm>
#include <ctime>
#include <map>
#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

using namespace std;

dx::JSON errorMsg(const string &e) {
  dx::JSON ret_val(dx::JSON_OBJECT);
  ret_val["error"] = e;
  return ret_val;
}

dx::JSON readJSON(const string &filename) {
  dx::JSON ret_val;
  ifstream in(filename.c_str());
  ret_val.read(in);
  in.close();
  return ret_val;
}

bool hasValue(const dx::JSON &json, const string &val) {
  if (json.type() != dx::JSON_ARRAY) return false;
  
  for (int i = 0; i < json.size(); i++) {
    if (json[i].get<string>().compare(val) == 0) return true;
  }

  return false;
}

bool randomSample(int total, int n, vector<int> &samples) {
  if ((total <=0) || (n <=0) || (n > total)) return false;
  samples.clear();
  map<int, int> m;
  map<int, int>::iterator it;
  m.clear();

  srand((unsigned)time(0));
  while (samples.size() < n) {
    int index = int( (float) rand() / (float) RAND_MAX * (float) total);
    it = m.find(index);
    int actual_index = (it == m.end()) ? index : it->second;
    samples.push_back(actual_index);

    total--;
    it = m.find(total);
    m[index] = (it == m.end()) ? total : it->second;
  }

  sort(samples.begin(), samples.end());
  return true;
}

void hashIncrease(map<int, int> &hash, int k) {
  map<int, int>::iterator it = hash.find(k);
  if (it == hash.end()) hash[k] = 1;
  else it->second++;
}

void simpleHistStat(map<int, int> &hash, double &avg, double &std) {
  map<int, int>::iterator it;
  double v1 = 0, v2 = 0, count = 0;
  for (it = hash.begin(); it != hash.end(); it++) {
    double f1 = double (it->first), f2 = double(it->second);
    v1 += f1 * f2;
    v2 += f1 * f1 *f2;
    count += f2;
  }

  avg = v1 / count;
  std = sqrt(v2/count - avg*avg);
}

dx::JSON hist2JSON(map<int, int> &hash) {
  map<int, int>::iterator it;
  dx::JSON ret_val(dx::JSON_OBJECT);
  for (it = hash.begin(); it != hash.end(); it++)
    ret_val[boost::lexical_cast<string>(it->first)] = it->second;

  return ret_val;
}

class RandomReadsReader {
  private:
    int chunkSize, minReadLen, numReads;

    vector<int> samples;
    map<int, int> leftReadsH, rightReadsH;
    
    dx::JSON columns, data;

    DXGTable table;

    void reset() {
      leftReadsH.clear();
      rightReadsH.clear();
      numReads = 0;
    }

    bool fetchReads(const string &outputF, string errMsg) {
      reset();

      ofstream out(outputF);
      if (! out.is_open()) {
        errMsg = "Cannot open " + outputF + " to write reads";
        return false;
      }

      string s1, s2;
      int n1, n2, offset, i, j;
      for (i = 0; i < samples.size(); i++) {
        offset = samples[i] * chunkSize;
        data = table.getRows(dx::JSON_NULL, columns, offset, chunkSize);

        for (j = 0; j < data["data"].size(); j++) {
          s1 = data["data"][j][0].get<string>();
          s2 = data["data"][j][1].get<string>();
          n1 = s1.size(); n2 = s2.size();

          hashIncrease(leftReadsH, n1);
          hashIncrease(rightReadsH, n2);

          if ((n1 >= minReadLen) && (n2 >= minReadLen)) {
            out << ">" << ++numReads << "_1\n" << s1 << "\n";
            out << ">" << numReads << "_2\n" << s2 << "\n";
          }
        }
      }

      out.close();
      return true;
    }

  public:
    RandomReadsReader(int cSize = 100, int minRLen = 16) : chunkSize(cSize), minReadLen(minRLen) {
      reset();
      data = dx::JSON(dx::JSON_OBJECT);

      columns = dx::JSON(dx::JSON_ARRAY);
      columns.push_back("sequence");
      columns.push_back("sequence2");
    }

    bool Get(const string &tableId, int n, const string &outputF, string &errMsg) {
      try {
        if (n <= 0) {
          errMsg = "Number of reads to be fetched is not positive";
          return false;
        }

        table.setIDs(tableId);
        dx::JSON desc = table.describe();

        if (! hasValue(desc["types"], "LetterReads")) {
          errMsg = "Can only handl letter reads";
          return false;
        }

        if (! hasValue(desc["types"], "PairedReads")) {
          errMsg = tableId + " does not contain paired reads";
          return false;
        }
        
        int64_t num_reads = int64_t(desc["length"]);
        cerr << "Total reads " << num_reads << endl;
        int total_chunks = (num_reads - 1)/chunkSize + 1;
        if (total_chunks > n) total_chunks--;
        else n = total_chunks;

        randomSample(total_chunks, n, samples);
        return fetchReads(outputF, errMsg);
      } catch (DXError &e) {
        errMsg = e.msg;
        return false;
      }

      return true;
    }

    dx::JSON ReadsInfo() {
      dx::JSON ret_val(dx::JSON_OBJECT);
      ret_val["num_reads"] = numReads;
      if (numReads > 0) {
        double f[4];
        simpleHistStat(leftReadsH, f[0], f[1]);
        simpleHistStat(rightReadsH, f[2], f[3]);
        ret_val["avg_read_len"] = dx::JSON(dx::JSON_ARRAY);
        ret_val["avg_read_len"].push_back(f[0]);
        ret_val["avg_read_len"].push_back(f[2]);
        ret_val["stddev_read_len"] = dx::JSON(dx::JSON_ARRAY);
        ret_val["stddev_read_len"].push_back(f[1]);
        ret_val["stddev_read_len"].push_back(f[3]);
        ret_val["read_len_hist"] = dx::JSON(dx::JSON_ARRAY);
        ret_val["read_len_hist"].push_back(hist2JSON(leftReadsH));
        ret_val["read_len_hist"].push_back(hist2JSON(rightReadsH));
      }
      return ret_val;
    }
};

class GenomeDivider {
  private:
    static string getFilename(const string &fname, int index) {
      stringstream ss;
      ss << fname << index;
      return ss.str();
    }

    static bool getSeq(ifstream &in, ofstream &out, string &head, int &count) {
      count = 0;
      string line;

      while (!in.eof()) {
        getline(in, line);
        if (line.size() == 0) return false;
        if (line.at(0) == '>') {
           head = line;
           return true;
        }
  
        count += line.size();
        out << line << "\n";
      }

      return false;
    }

    static void divideGenome(const string &inputF, const string &outputF, vector<int> &chromLen) {
      chromLen.clear();
  
      int count, index = 0;
      string head;
      ifstream in(inputF.c_str());
      ofstream out((getFilename(outputF + "_", 0) + ".fa").c_str());

      while (getSeq(in, out, head, count)) {
        cerr << head << endl;
        chromLen.push_back(count);
        out.close();

        out.open((getFilename(outputF + "_", ++index) + ".fa").c_str());
        out << head << endl;
      }

      chromLen.push_back(count);
      out.close();
      in.close();
    }    
  
    static int mergeGenome(const string &inputF, const string &outputF, vector<int> &chromLen, int maxCount) {
      int c = 0, part = 0;
      string fileList = "";
      for (int i = 1; i < chromLen.size(); i++) {
        if (((c + chromLen[i]) > maxCount) && (fileList.size() > 0)) {
          cerr << fileList << " " << part << endl;
          system(("cat" + fileList + " >" + getFilename(outputF + ".part", ++part) + ".fa").c_str());
          fileList = "";
          c = 0;
        }

        fileList += " " + getFilename(inputF + "_", i) + ".fa";
        c += chromLen[i];
      }
  
      cerr << fileList << " " << part << "\n";
      if (fileList.size() > 0) system(("cat" + fileList + " >" + getFilename(outputF + ".part", ++part) + ".fa").c_str());
      system(("rm " + inputF + "*").c_str());
      return part;
    }

  public:
    static int run(const string &inputF, const string &outputF) {
      vector<int> chromLen;
      string tempF = outputF + "_temp";
      divideGenome(inputF, tempF, chromLen);
      return mergeGenome(tempF, outputF, chromLen, 400000000);
    //  return mergeGenome(tempF, outputF, chromLen, 40000);
    }
};

void runLastZ(const string &genome, const string &reads, const string &outputF) {
  string options = "--step=10 --seed=match12 --notransition --exact=20 --noytrim --match=1,5 --ambiguous=n --coverage=90 --identity=95 --format=general:name1,start1,length1,name2,strand2";
  string cmd = "lastz " + genome + "[multiple] " + reads + " " + options + ">" + outputF;
  cerr << cmd << endl;
  system(cmd.c_str());
}

struct MappedReads {
  string chr;
  int lo, hi;
  int id, template_id;
  bool forward;

  MappedReads(const string &chr_, int lo_, int hi_, int id_, int template_, bool forward_) : chr(chr_), lo(lo_), hi(hi_), id(id_), template_id(template_), forward(forward_) {};

  bool Set(const string &chr_, int lo_, int hi_, const string &name, const string &strand) {
    size_t found = name.find_first_of("_");
    if (found == string::npos) return false;

    chr = chr_; lo = lo_; hi = hi_;
    forward = (strand.at(0) == '+');
    id = atoi(name.substr(0, found).c_str()) - 1;
    template_id = atoi(name.substr(found+1, name.size() - found -1).c_str()) - 1;

    return true;
  };
};

// FF => 0, FR => 1, RF =>2, RR => 3
struct PairedMappedReads {
  private:
    static bool getOrientation(MappedReads &r1, MappedReads &r2) {
      bool indicator = (r1.forward == (r1.lo < r2.lo));
      if (r1.forward == r2.forward) return (indicator) ? 0 : 3;
      return (indicator) ? 1 : 2;
    }
  
    static int getInternalSize(MappedReads &r1, MappedReads &r2) {
      int s = (r1.hi < r2.hi) ? r1.hi : r2.hi;
      int e = (r1.lo < r2.lo) ? r2.lo : r1.lo;
      return (e-s);
    }
  
    static int getExternalSize(MappedReads &r1, MappedReads &r2) {
      int e = (r1.hi < r2.hi) ? r2.hi : r1.hi;
      int s = (r1.lo < r2.lo) ? r1.lo : r2.lo;
      return (e-s);
    }
  
  public:
    int internal_size, external_size, orientation;

    PairedMappedReads(MappedReads &r1, MappedReads &r2) {
      orientation = getOrientation(r1, r2);
      internal_size = getInternalSize(r1, r2);
      external_size = getExternalSize(r1, r2);
    }
};

class LastZMappReader {
  private:
    ifstream in;
    string line;
    vector<string> data;

  public:
    bool Open(const string &filename) {
      in.open(filename.c_str());
      return in.is_open();
    }

    bool Get(MappedReads &r) {
      if (! in.is_open()) return false;
      while (!in.eof()) {
        getline(in, line);
        if (line.size() == 0) return false;
        if (line.at(0) == '#') continue;
 
        boost::split(data, line, boost::is_any_of("\t "));
        if (data.size() < 5) continue;
        if (r.Set(data[0], atoi(data[1].c_str()), atoi(data[1].c_str()) + atoi(data[2].c_str()), data[3], data[4])) return true;
      }
      return false;
    }

    void Close() { in.close(); }
};

class ReadsDistEst {
  private:
    int numReads, orientation[4], defaultOrientation, totalMappings, mappedCount;
    vector<int> counter[2];
    map<int, int> external_size, internal_size;

    vector<MappedReads> reads[2];
    vector<PairedMappedReads> paired;

    void getMappedPair() {
      paired.clear();
      orientation[0] = orientation[1] = orientation[2] = orientation[3] = 0;
      defaultOrientation = -1;
 
      for (int i = 0; i < numReads; i++) {
        if ((counter[0][i] != 1) || (counter[1][i] != 1)) continue;
        if (reads[0][i].chr.compare(reads[1][i].chr) != 0) continue;
        //cerr << i << endl;

        PairedMappedReads r(reads[0][i], reads[1][i]);
        paired.push_back(r);
        orientation[r.orientation] ++;
      }

      for (int i = 0; i < 4; i++) {
        if ((orientation[i] * 10) > (paired.size() * 9)) defaultOrientation = i;
      }
    }

    int findInsertBound() {
      external_size.clear();
      mappedCount = 0;

      for (int i = 0; i < paired.size(); i++) {
        if ((defaultOrientation == -1) || (paired[i].orientation == defaultOrientation)) {
          hashIncrease(external_size, paired[i].external_size);
          mappedCount ++;
        }
      }

      int count = 0, len = 0;
      map<int, int>::iterator it;
      for (it = external_size.begin(); it != external_size.end(); it++) {
        len = it->first;
        count += it->second;
        if ((mappedCount - count) < (mappedCount/20)) {
          if (it->second >= (mappedCount/100)) return len+1;
          else return len;
        }
      }

      return len + 1;
    } 

    void getInsertSize() {
      int k = findInsertBound();

      external_size.clear();
      internal_size.clear();

      for (int i = 0; i < paired.size(); i++) {
        if (paired[i].external_size >= k) continue;
        if ((defaultOrientation == -1) || (paired[i].orientation == defaultOrientation)) {
          hashIncrease(external_size, paired[i].external_size);
          hashIncrease(internal_size, paired[i].internal_size);
        }
      }
    }

    dx::JSON insertSizeInfo() {
      dx::JSON ret_val(dx::JSON_OBJECT);
      ret_val["mapped_pairs"] = mappedCount;
      switch(defaultOrientation) {
        case 0: ret_val["pair_orientation"] = "FF";
                break;
        case 1: ret_val["pair_orientation"] = "FR";
                break;
        case 2: ret_val["pair_orientation"] = "RF";
                break;
        case 3: ret_val["pair_orientation"] = "RR";
                break;
        default: ret_val["pair_orientation"] = "NA";
      }

      if (mappedCount > 0) {
        double f[2];
        simpleHistStat(external_size, f[0], f[1]);
        ret_val["avg_ext_size"] = f[0];
        ret_val["stddev_ext_size"] = f[1];
        ret_val["ext_size_hist"] = hist2JSON(external_size);

        simpleHistStat(internal_size, f[0], f[1]);
        ret_val["avg_int_size"] = f[0];
        ret_val["stddev_int_size"] = f[1];
        ret_val["int_size_hist"] = hist2JSON(internal_size);
      }

      return ret_val;
    }

  public:
    ReadsDistEst(int num) {
      numReads = num;
      totalMappings = 0;

      counter[0].clear();
      counter[1].clear();
      reads[0].clear();
      reads[1].clear();

      MappedReads r("", 0, 0, 0, 0, true);
      for (int i = 0; i < numReads; i++) {
        counter[0].push_back(0);
        counter[1].push_back(0);
        reads[0].push_back(r);
        reads[1].push_back(r);
      }
    }

    void Add(MappedReads &r) {
      totalMappings ++;

      counter[r.template_id][r.id] ++;
      if (counter[r.template_id][r.id] == 1) reads[r.template_id][r.id] = r;
    }

    dx::JSON Estimate() {
      getMappedPair();
      getInsertSize();
      return insertSizeInfo();
    }
};

bool getLastZEst(int numReads, const string filename, int nPart, dx::JSON &info, string &errMsg) {
  LastZMappReader reader;

  ReadsDistEst est(numReads);

  MappedReads r("", 0, 0, 0, 0, true);
  for (int i = 0; i < nPart; i++) {
    string fname = filename + boost::lexical_cast<string>(i+1);
    if (! reader.Open(fname)) {
      errMsg = "Cannot open alignment file " + fname;
      return false;
    }
    while (reader.Get(r)) { est.Add(r); }
    reader.Close();
  }

  info = est.Estimate();
  return true;
}

void mergeJSON(dx::JSON &source, dx::JSON &target) {
  for (dx::JSON::object_iterator it = source.object_begin(); it != source.object_end(); it++)
    target[it->first] = it->second;
}

int main(int argc, char **argv) {
  if (argc < 2) {
    cerr << "Usage: pairedreads_ins_est input.conf" << endl;;
    exit(1);
  }

  int n;
  string tableId, genome_file, errMsg;
  try {
    dx::JSON config = readJSON(argv[1]);
    tableId = config["reads"]["$dnanexus_link"].get<string>();
    n = int(config["numSamples"])/100;
    if (config["genome"].type() == dx::JSON_STRING) {
      genome_file = config["genome"].get<string>();
    } else {
      system(("contigset2fasta " + config["genome"]["$dnanexus_link"].get<string>() + " genome.fa").c_str());
      genome_file = "genome.fa";
    } 
  } catch (dx::JSONException &e) {
    cout << errorMsg(e.err).toString() << endl;
    exit(1);
  }

  RandomReadsReader r;
  if (! r.Get(tableId, n, "temp.fa", errMsg)) {
    cout << errorMsg(errMsg).toString() << endl;
    exit(1);
  }

  int nPart = GenomeDivider::run(genome_file, "genome");
  for (int i = 0; i < nPart; i++) {
    string index = boost::lexical_cast<string>(i+1);
    runLastZ("genome.part" + index + ".fa", "temp.fa", "mapping.part" + index);
  }
 
  dx::JSON readsInfo = r.ReadsInfo(), output;

  getLastZEst(10000, "mapping.part", nPart, output, errMsg);
  mergeJSON(readsInfo, output);
  cout << output.toString() << endl;

  exit(0);
}
