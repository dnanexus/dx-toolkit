#include "reads_validate_helper.h"
#include "dxcpp/dxcpp.h"
#include <gtest/gtest.h>

using namespace std;
using namespace dx;

typedef unsigned uint32;
typedef unsigned long long uint64;
typedef long long int64;

ReadsErrorMsg readsErrorMsg;

bool run_readsV(const string &source, JSON &info) {
  string errMsg;
  if (! exec(myPath() + "/reads_validator " + source, errMsg)) return false;
  cerr << errMsg << endl;
  info = JSON::parse(errMsg);
  return true;
}

string createRecord(const JSON &data, bool close) {
  DXRecord record = DXRecord::newDXRecord(data);
  if (close) record.close();
  return record.getID();
}

string createTable(const JSON &data, bool close) {
  vector<JSON> columns;
  for (int i = 0; i < data["columns"].size(); i++) 
    columns.push_back(DXGTable::columnDesc(data["columns"][i][0].get<string>(), data["columns"][i][1].get<string>()));

  DXGTable table = DXGTable::newDXGTable(columns);

  table.addRows(data["data"],1);

  table.addTypes(data["types"]);
  table.setDetails(data["details"]);

  if (close) table.close(true);
  return table.getID();
}

void oneTest(JSON &data, JSON &info, bool close = true) {
  string id = createTable(data, close);
  bool ret_val = run_readsV(id, info);
  ASSERT_EQ(system(("dx rm " + id).c_str()), 0);
  ASSERT_TRUE(ret_val);
}

void errorTestOne(JSON &data, const string &tag) {
  JSON info;
  oneTest(data, info);
  ASSERT_FALSE(bool(info["valid"]));
  ASSERT_EQ(info["error"].get<string>(), readsErrorMsg.GetError(tag));
}

void errorTestOne(JSON &data, const string &tag, const vector<string> &replace) {
  JSON info;
  oneTest(data, info);
  ASSERT_FALSE(bool(info["valid"]));
  for (int i = 0; i < replace.size(); i++)
    readsErrorMsg.SetData(replace[i], i);
  ASSERT_EQ(info["error"].get<string>(), readsErrorMsg.GetError(tag, true));
}

void warningTestOne(JSON &data, const vector<string> &tags, bool close = true) {
  JSON info;
  oneTest(data, info, close);
  ASSERT_TRUE(bool(info["valid"]));
  for (int i = 0; i < tags.size(); i++)
    ASSERT_TRUE(hasString(info["warning"], readsErrorMsg.GetWarning(tags[i])));
}

void warningTestOne(JSON &record, const vector<string> &tags, const vector<string> &replace, bool close = true) {
  JSON info;
  oneTest(record, info, close);
  ASSERT_TRUE(bool(info["valid"]));
  for (int i = 0; i < replace.size(); i++)
    readsErrorMsg.SetData(replace[i], i);
  for (int i = 0; i < tags.size(); i++)
    ASSERT_TRUE(hasString(info["warning"], readsErrorMsg.GetWarning(tags[i], true)));
}

TEST(ReadsValidator, InvalidType) {
  string errMsg, tag;
  JSON info, data;

  tag = "GTABLE_FETCH_FAIL";
  ASSERT_TRUE(run_readsV("file-t", info));
  ASSERT_FALSE(info.has("valid"));
  size_t found = info["error"].get<string>().find(readsErrorMsg.GetError(tag));
  ASSERT_FALSE(found == string::npos);
  
  tag = "CLASS_NOT_GTABLE";
  string id = createRecord(JSON(JSON_OBJECT), true);
  ASSERT_TRUE(run_readsV(id, info));
  ASSERT_FALSE(bool(info["valid"]));
  ASSERT_EQ(info["error"].get<string>(), readsErrorMsg.GetError(tag));
  ASSERT_EQ(system(("dx rm " + id).c_str()), 0);

  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["details"] = JSON(JSON_ARRAY);
  errorTestOne(data, "DETAILS_NOT_HASH");

  tag = "GTABLE_NOT_CLOSED";
  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  oneTest(data, info, false);
  ASSERT_FALSE(bool(info["valid"]));
  ASSERT_EQ(info["error"].get<string>(), readsErrorMsg.GetError(tag));

  data["types"] = JSON(JSON_ARRAY);
  errorTestOne(data, "TYPE_NOT_READS");
  
  vector<string> tags;
  tags.push_back("TYPE_MISSING");
  tags.push_back("ORIGINAL_FILES_INVALID");
  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["types"].erase(1);
  data["details"]["original_files"] = JSON(JSON_OBJECT);
  data["details"]["original_files"]["name"] = "OK"; 
  warningTestOne(data, tags);

  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  tags.clear();
  tags.push_back("ORIGINAL_FILE_INVALID");
  data["details"]["original_files"] = JSON(JSON_ARRAY);
  data["details"]["original_files"].push_back(JSON(JSON_OBJECT));
  warningTestOne(data, tags);
  
  data["details"]["original_files"][0]["name"] = "OK";
  warningTestOne(data, tags);

  vector<string> replace;

  replace.push_back("LetterReads");
  replace.push_back("FlowReads");
  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["types"].push_back("FlowReads");
  errorTestOne(data, "TYPE_CONFLICT", replace);
  
  replace[0] = "ColorReads";
  data["types"][1] = "ColorReads";
  errorTestOne(data, "TYPE_CONFLICT", replace);

  replace[0] = "LetterReads";
  data["types"][1] = "LetterReads";
  errorTestOne(data, "TYPE_CONFLICT", replace);
}

TEST(ReadsValidator, InvalidPaired) {
  JSON data;
  vector<string> tags;
  tags.push_back("PAIR_ORIENTATION_INVALID");
  tags.push_back("PAIR_MIN_DIST_INVALID");
  tags.push_back("PAIR_MAX_DIST_INVALID");
  tags.push_back("PAIR_AVG_DIST_INVALID");
  tags.push_back("PAIR_STDDEV_DIST_INVALID");
  
  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["details"]["pair_orientation"] = JSON(JSON_OBJECT);
  data["details"]["pair_min_dist"] = JSON(JSON_ARRAY);
  data["details"]["pair_max_dist"] = JSON(JSON_BOOLEAN);
  data["details"]["pair_avg_dist"] = JSON(JSON_STRING);
  data["details"]["pair_stddev_dist"] = JSON(JSON_OBJECT);
  warningTestOne(data, tags);

  data["details"]["paired"] = JSON(JSON_OBJECT);
  errorTestOne(data, "DETAILS_PAIRED_INVALID");
  
  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  data["details"]["pair_orientation"] = "X";
  data["details"]["pair_min_dist"] = JSON(JSON_ARRAY);
  data["details"]["pair_max_dist"] = JSON(JSON_BOOLEAN);
  data["details"]["pair_avg_dist"] = JSON(JSON_STRING);
  data["details"]["pair_stddev_dist"] = JSON(JSON_OBJECT);
  warningTestOne(data, tags);
  
  data["details"]["paired"] = JSON(JSON_OBJECT);
  errorTestOne(data, "DETAILS_PAIRED_INVALID");
  
  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["details"]["pair_orientation"] = JSON(JSON_OBJECT);
  data["details"]["pair_min_dist"] = JSON(JSON_ARRAY);
  data["details"]["pair_max_dist"] = JSON(JSON_BOOLEAN);
  data["details"]["pair_avg_dist"] = JSON(JSON_STRING);
  data["details"]["pair_stddev_dist"] = JSON(JSON_OBJECT);
  warningTestOne(data, tags);
  
  data["details"]["paired"] = JSON(JSON_OBJECT);
  errorTestOne(data, "DETAILS_PAIRED_INVALID");
}

TEST(ReadsValidator, SequenceType) {
  JSON data;
  vector<string> tags;

  tags.push_back("LETTER_WITH_SEQUENCE_TYPE");
  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["details"]["sequence_type"] = "letter";
  warningTestOne(data, tags);

  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  data["details"]["sequence_type"] = JSON(JSON_OBJECT);
  errorTestOne(data, "COLOR_SEQUENCE_TYPE_INVALID");
  
  data["details"]["sequence_type"] = "flow";
  errorTestOne(data, "COLOR_SEQUENCE_TYPE_INVALID");
  
  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["details"]["sequence_type"] = JSON(JSON_OBJECT);
  errorTestOne(data, "FLOW_SEQUENCE_TYPE_INVALID");
  
  data["details"]["sequence_type"] = "color";
  errorTestOne(data, "FLOW_SEQUENCE_TYPE_INVALID");
}

TEST(ReadsValidator, ColumnSequence) {
  JSON data;
  
  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["columns"].erase(1);
  for (int i = 0; i < data["data"].size(); i++)
    data["data"][i].erase(1);
  errorTestOne(data, "SEQUENCE_MISSING");
  
  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["columns"][1][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][1] = 0;
  errorTestOne(data, "SEQUENCE_NOT_STRING");

  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["columns"].erase(4);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(4);
  errorTestOne(data, "LETTER_SEQUENCE2_MISSING");

  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["columns"][4][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][4] = 0;
  errorTestOne(data, "SEQUENCE2_NOT_STRING");

  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  data["columns"].erase(1);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(1);
  errorTestOne(data, "SEQUENCE_MISSING");

  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  data["columns"][1][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][1] = 0;
  errorTestOne(data, "SEQUENCE_NOT_STRING");

  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  data["columns"].erase(4);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(4);
  errorTestOne(data, "COLOR_SEQUENCE2_MISSING");

  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  data["columns"][4][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][4] = 0;
  errorTestOne(data, "SEQUENCE2_NOT_STRING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(0);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(0);
  errorTestOne(data, "SEQUENCE_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][0][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][0] = 0;
  errorTestOne(data, "SEQUENCE_NOT_STRING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(9);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(9);
  errorTestOne(data, "FLOW_SEQUENCE2_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][9][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][9] = 0;
  errorTestOne(data, "SEQUENCE2_NOT_STRING");
}

TEST(ReadsValidator, ColumnQuality) {
  JSON data;

  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["columns"].erase(2);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(2);
  errorTestOne(data, "LETTER_QUALITY_MISSING");

  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["columns"][2][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][2] = 0;
  errorTestOne(data, "QUALITY_NOT_STRING");

  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["columns"].erase(5);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(5);
  errorTestOne(data, "LETTER_QUALITY2_MISSING");

  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["columns"][5][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][5] = 0;
  errorTestOne(data, "QUALITY2_NOT_STRING");

  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  data["columns"].erase(2);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(2);
  errorTestOne(data, "COLOR_QUALITY_MISSING");

  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  data["columns"][2][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][2] = 0;
  errorTestOne(data, "QUALITY_NOT_STRING");

  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  data["columns"].erase(5);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(5);
  errorTestOne(data, "COLOR_QUALITY2_MISSING");

  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  data["columns"][5][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][5] = 0;
  errorTestOne(data, "QUALITY2_NOT_STRING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(1);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(1);
  errorTestOne(data, "FLOW_QUALITY_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][1][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][1] = 0;
  errorTestOne(data, "QUALITY_NOT_STRING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(10);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(10);
  errorTestOne(data, "FLOW_QUALITY2_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][10][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][10] = 0;
  errorTestOne(data, "QUALITY2_NOT_STRING");
}

TEST(ReadsValidator, ColumnName) {
  JSON data;

  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["columns"][0][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][0] = 0;
  errorTestOne(data, "NAME_NOT_STRING");

  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["columns"].erase(0);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(0);
  errorTestOne(data, "NAME_MISSING");

  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  data["columns"][3][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][3] = 0;
  errorTestOne(data, "NAME2_NOT_STRING");

  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  data["columns"][0][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][0] = 0;
  errorTestOne(data, "NAME_NOT_STRING");

  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  data["columns"].erase(0);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(0);
  errorTestOne(data, "NAME_MISSING");

  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  data["columns"][3][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][3] = 0;
  errorTestOne(data, "NAME2_NOT_STRING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(8);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(8);
  errorTestOne(data, "NAME_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][8][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][8] = 0;
  errorTestOne(data, "NAME_NOT_STRING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][17][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][17] = 0;
  errorTestOne(data, "NAME2_NOT_STRING");
}

TEST(ContigsetValidator, PairSecondFlow) {
  JSON data;

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["details"].erase("pair_second_flow");
  errorTestOne(data, "PAIR_SECOND_FLOW_MISSING");

  data["details"]["pair_second_flow"] = JSON(JSON_OBJECT);
  errorTestOne(data, "PAIR_SECOND_FLOW_NOT_BOOLEAN");

  data = readJSON(myPath() + "/flowReads.paired.oneseq.valid.js");
  data["details"].erase("pair_second_flow");
  errorTestOne(data, "PAIR_SECOND_FLOW_MISSING");

  data["details"]["pair_second_flow"] = JSON(JSON_OBJECT);
  errorTestOne(data, "PAIR_SECOND_FLOW_NOT_BOOLEAN");
}

TEST(ContigsetValidator, FlowSequence) {
  JSON data;

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["details"].erase("flow_sequence");
  errorTestOne(data, "FLOW__SEQUENCE_MISSING");

  data["details"]["flow_sequence"] = JSON(JSON_OBJECT);
  errorTestOne(data, "FLOW__SEQUENCE_NOT_STRING");

  data["details"]["flow_sequence"] = "ACGTNACGTACGTACGT";
  errorTestOne(data, "FLOW__SEQUENCE_INVALID_CHARACTER");

  data["details"]["flow_sequence"] = "ACGTAACGTACGTACGT";
  errorTestOne(data, "FLOW__SEQUENCE_SAME_CONSECUTIVE");
  
  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["details"].erase("flow_sequence2");
  errorTestOne(data, "FLOW__SEQUENCE2_MISSING");

  data["details"]["flow_sequence2"] = JSON(JSON_OBJECT);
  errorTestOne(data, "FLOW__SEQUENCE2_NOT_STRING");

  data["details"]["flow_sequence2"] = "ACGTNACGTACGTACGT";
  errorTestOne(data, "FLOW__SEQUENCE2_INVALID_CHARACTER");

  data["details"]["flow_sequence2"] = "ACGTAACGTACGTACGT";
  errorTestOne(data, "FLOW__SEQUENCE2_SAME_CONSECUTIVE");
}

TEST(ContigsetValidator, FlowKey) {
  JSON data;

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["details"].erase("flow_key");
  errorTestOne(data, "FLOW__KEY_MISSING");

  data["details"]["flow_key"] = JSON(JSON_OBJECT);
  errorTestOne(data, "FLOW__KEY_NOT_STRING");

  data["details"]["flow_key"] = "ACGTNACGTACGTACGT";
  errorTestOne(data, "FLOW__KEY_INVALID_CHARACTER");
  
  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["details"].erase("flow_key2");
  errorTestOne(data, "FLOW__KEY2_MISSING");

  data["details"]["flow_key2"] = JSON(JSON_OBJECT);
  errorTestOne(data, "FLOW__KEY2_NOT_STRING");

  data["details"]["flow_key2"] = "ACGTNACGTACGTACGT";
  errorTestOne(data, "FLOW__KEY2_INVALID_CHARACTER");
}

TEST(ReadsValidator, ColumnFlowGramIndices) {
  JSON data;

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(2);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(2);
  errorTestOne(data, "FLOWGRAM_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][2][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][2] = 0;
  errorTestOne(data, "FLOWGRAM_NOT_STRING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(11);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(11);
  errorTestOne(data, "FLOWGRAM2_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][11][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][11] = 0;
  errorTestOne(data, "FLOWGRAM2_NOT_STRING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(3);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(3);
  errorTestOne(data, "FLOW_INDICES_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][3][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][3] = 0;
  errorTestOne(data, "FLOW_INDICES_NOT_STRING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(12);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(12);
  errorTestOne(data, "FLOW_INDICES2_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][12][1] = "int32";
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i][12] = 0;
  errorTestOne(data, "FLOW_INDICES2_NOT_STRING");
}

TEST(ReadsValidator, ColumnQualAdapterLeftRight) {
  JSON data;

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(4);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(4);
  errorTestOne(data, "FLOW_CLIP_QUAL_LEFT_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][4][1] = "int32";
  errorTestOne(data, "FLOW_CLIP_QUAL_LEFT_NOT_UINT16");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(13);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(13);
  errorTestOne(data, "FLOW_CLIP_QUAL_LEFT2_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][13][1] = "int32";
  errorTestOne(data, "FLOW_CLIP_QUAL_LEFT2_NOT_UINT16");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(5);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(5);
  errorTestOne(data, "FLOW_CLIP_QUAL_RIGHT_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][5][1] = "int32";
  errorTestOne(data, "FLOW_CLIP_QUAL_RIGHT_NOT_UINT16");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(14);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(14);
  errorTestOne(data, "FLOW_CLIP_QUAL_RIGHT2_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][14][1] = "int32";
  errorTestOne(data, "FLOW_CLIP_QUAL_RIGHT2_NOT_UINT16");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(6);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(6);
  errorTestOne(data, "FLOW_CLIP_ADAPTER_LEFT_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][6][1] = "int32";
  errorTestOne(data, "FLOW_CLIP_ADAPTER_LEFT_NOT_UINT16");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(15);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(15);
  errorTestOne(data, "FLOW_CLIP_ADAPTER_LEFT2_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][15][1] = "int32";
  errorTestOne(data, "FLOW_CLIP_ADAPTER_LEFT2_NOT_UINT16");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(7);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(7);
  errorTestOne(data, "FLOW_CLIP_ADAPTER_RIGHT_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][7][1] = "int32";
  errorTestOne(data, "FLOW_CLIP_ADAPTER_RIGHT_NOT_UINT16");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"].erase(16);
  for (int i = 0; i < data["data"].size(); i++)
   data["data"][i].erase(16);
  errorTestOne(data, "FLOW_CLIP_ADAPTER_RIGHT2_MISSING");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  data["columns"][16][1] = "int32";
  errorTestOne(data, "FLOW_CLIP_ADAPTER_RIGHT2_NOT_UINT16");
}

TEST(ReadsValidator, SequenceNameData) {
  JSON data;
  vector<string> tags, replace;
  string name, seq;
  
  tags.push_back("NAME_INVALID");
  replace.push_back("[!-?A-~]{1,255}");
  replace.push_back("");

  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  name = "";
  name.assign(256, 'a');
  data["data"][0][0] = name;
  replace[1] = "1st";
  warningTestOne(data, tags, replace);
  
  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  data["data"][1][3] = "test\t";
  replace[1] = "2nd";
  warningTestOne(data, tags, replace);
  
  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  replace[1] = "3rd";
  data["data"][2][8] = "test\tt";
  warningTestOne(data, tags, replace);

  tags[0] = "SEQUENCE_INVALID";

  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  seq = data["data"][0][1].get<string>();
  *((char *)seq.data()+4) = '.';
  data["data"][0][1] = seq;
  replace[0] = "[ACGTN]+"; replace[1] = "1st";
  warningTestOne(data, tags, replace);
  
  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  seq = data["data"][1][4].get<string>();
  *((char *)seq.data()) = 'a';
  data["data"][1][4] = seq;
  replace[0] = "[ACGT][0-3.]+", replace[1] = "2nd";
  warningTestOne(data, tags, replace);

  *((char *)seq.data()) = 'A';
  *((char *)seq.data() + 1) = 'A';
  data["data"][1][4] = seq;
  warningTestOne(data, tags, replace);
  
  *((char *)seq.data() + 1) = '4';
  data["data"][1][4] = seq;
  warningTestOne(data, tags, replace);
}

TEST(ReadsValidator, FlowLeftRightData) {
  JSON data;
  vector<string> tags, replace;
  string name, seq;
  
  tags.push_back("SEQUENCE_EMPTY_AFTER_TRIMMING");
  replace.push_back("1st");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");

  data["data"][0][4] = 101;
  warningTestOne(data, tags, replace);

  data["data"][0][4] = 5;
  data["data"][0][6] = 101;
  warningTestOne(data, tags, replace);
  data["data"][0][6] = 0;

  replace[0] = "2nd";
  data["data"][1][13] = 150;
  warningTestOne(data, tags, replace);

  data["data"][1][13] = 5;
  data["data"][1][15] = 150;
  warningTestOne(data, tags, replace);
  data["data"][1][15] = 0;

  replace[0] = "3rd";
  data["data"][2][4] = 300;
  data["data"][2][5] = 350;
  warningTestOne(data, tags, replace);
}

TEST(ReadsValidator, SequenceQualityData) {
  JSON data;
  string qual;
  vector<string> replace;

  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  replace.push_back("5th");
  qual = data["data"][4][2].get<string>();

  data["data"][4][2] = qual + "5";
  errorTestOne(data, "QUALITY_SEQUENCE_NOT_MATCH", replace);

  data["data"][4][2] = qual.substr(0, qual.size()-1);
  errorTestOne(data, "QUALITY_SEQUENCE_NOT_MATCH", replace);

  *((char *)qual.data()+10) = '\t';
  data["data"][4][2] = qual;
  errorTestOne(data, "QUALITY_NOT_PHRED33", replace);

  *((char *)qual.data()+10) = 'A';
  data["data"][4][2] = qual;

  replace[0] = "3rd";
  qual = data["data"][2][5].get<string>();

  data["data"][2][5] = qual + "A";
  errorTestOne(data, "QUALITY2_SEQUENCE2_NOT_MATCH", replace);

  data["data"][2][5] = qual.substr(0, qual.size()-1);
  errorTestOne(data, "QUALITY2_SEQUENCE2_NOT_MATCH", replace);

  *((char *)qual.data()) = '\t';
  data["data"][2][5] = qual;
  errorTestOne(data, "QUALITY2_NOT_PHRED33", replace);

  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  replace[0] = "4th";
  qual = data["data"][3][2].get<string>();

  data["data"][3][2] = qual + ".";
  errorTestOne(data, "QUALITY_SEQUENCE_NOT_MATCH", replace);

  data["data"][3][2] = qual.substr(0, qual.size()-1);
  errorTestOne(data, "QUALITY_SEQUENCE_NOT_MATCH", replace);

  *((char *)qual.data()+7) = '\t';
  data["data"][3][2] = qual;
  errorTestOne(data, "QUALITY_NOT_PHRED33", replace);

  *((char *)qual.data()+7) = '5';
  data["data"][3][2] = qual;

  replace[0] = "2nd";
  qual = data["data"][1][5].get<string>();

  data["data"][1][5] = qual + "!";
  errorTestOne(data, "QUALITY2_SEQUENCE2_NOT_MATCH", replace);

  data["data"][1][5] = qual.substr(0, qual.size()-1);
  errorTestOne(data, "QUALITY2_SEQUENCE2_NOT_MATCH", replace);

  *((char *)qual.data()+11) = '\t';
  data["data"][1][5] = qual;
  errorTestOne(data, "QUALITY2_NOT_PHRED33", replace);

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  replace[0] = "1st";
  qual = data["data"][0][1].get<string>();

  data["data"][0][1] = qual + "t";
  errorTestOne(data, "QUALITY_SEQUENCE_NOT_MATCH", replace);

  data["data"][0][1] = qual.substr(0, qual.size()-1);
  errorTestOne(data, "QUALITY_SEQUENCE_NOT_MATCH", replace);

  *((char *)qual.data()+3) = '\t';
  data["data"][0][1] = qual;
  errorTestOne(data, "QUALITY_NOT_PHRED33", replace);

  *((char *)qual.data()+3) = '(';
  data["data"][0][1] = qual;

  replace[0] = "2nd";
  qual = data["data"][1][10].get<string>();

  data["data"][1][10] = qual + "!";
  errorTestOne(data, "QUALITY2_SEQUENCE2_NOT_MATCH", replace);

  data["data"][1][10] = qual.substr(0, qual.size()-1);
  errorTestOne(data, "QUALITY2_SEQUENCE2_NOT_MATCH", replace);

  *((char *)qual.data()+9) = '\t';
  data["data"][1][10] = qual;
  errorTestOne(data, "QUALITY2_NOT_PHRED33", replace);
}

TEST(ReadsValidator, FlowSeqKeyGramIndices) {
  JSON data;
  string key, seq, gram, indices;
  vector<string>replace; replace.push_back("");

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  replace[0] = "1st";
  key = data["details"]["flow_key"].get<string>();
  data["details"]["flow_key"] = "AAAA";
  errorTestOne(data, "FLOW_KEY_SEQUENCE_NOT_MATCH", replace);
  data["details"]["flow_key"] = key;

  replace[0] = "2nd";
  seq = data["data"][1][0].get<string>();
  data["data"][1][0] = "AAAA" + seq.substr(4, seq.size()-4);
  errorTestOne(data, "FLOW_KEY_SEQUENCE_NOT_MATCH", replace);
  data["data"][1][0] = seq;

  replace[0] = "1st";
  key = data["details"]["flow_key2"].get<string>();
  data["details"]["flow_key2"] = "AAAA";
  errorTestOne(data, "FLOW_KEY2_SEQUENCE2_NOT_MATCH", replace);
  data["details"]["flow_key2"] = key;

  replace[0] = "3rd";
  seq = data["data"][2][9].get<string>();
  data["data"][2][9] = "AAAA" + seq.substr(4, seq.size()-4);
  errorTestOne(data, "FLOW_KEY2_SEQUENCE2_NOT_MATCH", replace);
  data["data"][2][9] = seq;


  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  replace[0] = "1st";
  gram = data["data"][0][2].get<string>();
  data["data"][0][2] = gram + "0";
  errorTestOne(data, "FLOWGRAM_INVALID_LENGTH", replace);

  data["data"][0][2] = gram.substr(0, gram.size()-1);
  errorTestOne(data, "FLOWGRAM_INVALID_LENGTH", replace);

  *((char *)gram.data()+5) = 'G';
  data["data"][0][2] = gram;
  errorTestOne(data, "FLOWGRAM_INVALID_CHARACTER", replace);

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  replace[0] = "2nd";
  gram = data["data"][1][11].get<string>();
  data["data"][1][11] = gram + "0064";
  errorTestOne(data, "FLOWGRAM2_INVALID_LENGTH", replace);

  data["data"][1][11] = gram.substr(0, gram.size()-4);
  errorTestOne(data, "FLOWGRAM2_INVALID_LENGTH", replace);

  *((char *)gram.data()+21) = 's';
  data["data"][1][11] = gram;
  errorTestOne(data, "FLOWGRAM2_INVALID_CHARACTER", replace);


  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  replace[0] = "3rd";
  indices = data["data"][2][3].get<string>();
  data["data"][2][3] = indices + "0";
  errorTestOne(data, "FLOW_INDICES_INVALID_LENGTH", replace);

  data["data"][2][3] = indices.substr(0, indices.size()-1);
  errorTestOne(data, "FLOW_INDICES_INVALID_LENGTH", replace);

  *((char *)indices.data()+5) = 'p';
  data["data"][2][3] = indices;
  errorTestOne(data, "FLOW_INDICES_INVALID_CHARACTER", replace);

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  replace[0] = "4th";
  indices = data["data"][3][12].get<string>();
  data["data"][3][12] = indices + "03";
  errorTestOne(data, "FLOW_INDICES2_INVALID_LENGTH", replace);

  data["data"][3][12] = indices.substr(0, indices.size()-2);
  errorTestOne(data, "FLOW_INDICES2_INVALID_LENGTH", replace);

  *((char *)indices.data()+3) = 's';
  data["data"][3][12] = indices;
  errorTestOne(data, "FLOW_INDICES2_INVALID_CHARACTER", replace);


  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  replace[0] = "1st";
  seq = data["data"][0][0].get<string>();
  *((char *)seq.data() + 4) = 'g';
  errorTestOne(data, "FLOW_INDICES_SEQUENCE_NOT_MATCH", replace);

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  indices = data["data"][0][3].get<string>();
  *((char *)indices.data()+10) = 'f';
  errorTestOne(data, "FLOW_INDICES_SEQUENCE_NOT_MATCH", replace);

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  replace[0] = "2nd";
  seq = data["data"][1][9].get<string>();
  *((char *)seq.data() + 5) = 'A';
  errorTestOne(data, "FLOW_INDICES2_SEQUENCE2_NOT_MATCH", replace);

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  indices = data["data"][1][12].get<string>();
  *((char *)indices.data()+15) = 'F';
  errorTestOne(data, "FLOW_INDICES2_SEQUENCE2_NOT_MATCH", replace);
}

TEST(ReadsValidator, Valid) {
  JSON data, info;
  
  data = readJSON(myPath() + "/letterReads.paired.valid.js");
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "letter");
  ASSERT_TRUE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));
  
  data["columns"].erase(3);
  for (int i = 0; i < data["data"].size(); i++)
    data["data"][i].erase(3);
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "letter");
  ASSERT_TRUE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));

  data["columns"].erase(0);
  for (int i = 0; i < data["data"].size(); i++)
    data["data"][i].erase(0);
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "letter");
  ASSERT_TRUE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));

  data["columns"].erase(3);
  data["columns"].erase(1);
  for (int i = 0; i < data["data"].size(); i++) {
    data["data"][i].erase(3);
    data["data"][i].erase(1);
  }
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "letter");
  ASSERT_TRUE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));
 
  data = readJSON(myPath() + "/letterReads.unpaired.valid.js");
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "letter");
  ASSERT_FALSE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));
  
  data = readJSON(myPath() + "/colorReads.paired.valid.js");
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "color");
  ASSERT_TRUE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));

  data["columns"].erase(3);
  for (int i = 0; i < data["data"].size(); i++)
    data["data"][i].erase(3);
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "color");
  ASSERT_TRUE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));

  data["columns"].erase(0);
  for (int i = 0; i < data["data"].size(); i++)
    data["data"][i].erase(0);
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "color");
  ASSERT_TRUE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));

  data["columns"].erase(3);
  data["columns"].erase(1);
  for (int i = 0; i < data["data"].size(); i++) {
    data["data"][i].erase(3);
    data["data"][i].erase(1);
  }
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "color");
  ASSERT_TRUE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));
 
  data = readJSON(myPath() + "/colorReads.unpaired.valid.js");
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "color");
  ASSERT_FALSE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));

  data = readJSON(myPath() + "/flowReads.paired.valid.js");
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "flow");
  ASSERT_TRUE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));

  data["columns"].erase(17);
  for (int i = 0; i < data["data"].size(); i++)
    data["data"][i].erase(17);
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "flow");
  ASSERT_TRUE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));

  data["columns"].erase(8);
  for (int i = 0; i < data["data"].size(); i++)
    data["data"][i].erase(8);
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "flow");
  ASSERT_TRUE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));
 
  data = readJSON(myPath() + "/flowReads.unpaired.valid.js");
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "flow");
  ASSERT_FALSE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));

  data = readJSON(myPath() + "/flowReads.paired.oneseq.valid.js");
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "flow");
  ASSERT_TRUE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));

  data["columns"].erase(13);
  for (int i = 0; i < data["data"].size(); i++)
    data["data"][i].erase(13);
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "flow");
  ASSERT_TRUE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));

  data["columns"].erase(8);
  for (int i = 0; i < data["data"].size(); i++)
    data["data"][i].erase(8);
  oneTest(data, info, true);
  ASSERT_TRUE(bool(info["valid"]));
  ASSERT_TRUE(info["type"] == "flow");
  ASSERT_TRUE(bool(info["paired"]));
  ASSERT_FALSE(info.has("warning"));
}

int main(int argc, char **argv) {
  loadFromEnvironment();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}   
