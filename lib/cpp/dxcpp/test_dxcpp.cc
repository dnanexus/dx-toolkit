#include <iostream>
#include <fstream>
#include <string>
#include <boost/lexical_cast.hpp>
#include <gtest/gtest.h>
#include "dxjson/dxjson.h"
#include "dxcpp.h"

using namespace std;
using namespace dx;

string proj_id = "project-000000000000000000000001";
string second_proj_id = "project-000000000000000000000002";

// TODO: Finish writing tests for other classes.

void remove_all(const string &proj, const string &folder="/") {
  DXProject dxproject(proj);
  JSON listf = dxproject.listFolder(folder);
  dxproject.removeObjects(listf["objects"]);
  for (int i = 0; i < listf["folders"].size(); i++) {
    string subfolder = listf["folders"][i].get<string>();
    remove_all(proj, subfolder);
    dxproject.removeFolder(subfolder);
  }
}

//////////////
// DXRecord //
//////////////

class DXRecordTest : public testing::Test {
};

// const JSON DXRecordTest::example_JSON =
//   JSON::parse("{\"foo\": \"bar\", \"alpha\": [1, 2, 3]}");
// const JSON DXRecordTest::another_example_JSON =
//   JSON::parse("[\"foo\", \"bar\", {\"alpha\": [1, 2.340, -10]}]");

// TEST_F(DXRecordTest, CreateDestroyJSONTest) {
//   DXRecord first_record = DXRecord::newDXRecord(DXRecordTest::example_JSON);
//   ASSERT_EQ(DXRecordTest::example_JSON,
// 	    first_record.get());
//   string firstID = first_record.getID();

//   DXRecord second_record = DXRecord(first_record.getID());
//   ASSERT_EQ(first_record.getID(), second_record.getID());
//   ASSERT_EQ(first_record.get(), second_record.get());

//   second_record.create(DXRecordTest::example_JSON);
//   ASSERT_NE(first_record.getID(), second_record.getID());
//   ASSERT_EQ(first_record.get(), second_record.get());

//   ASSERT_NO_THROW(first_record.describe());

//   first_record.destroy();
//   ASSERT_NO_THROW(first_record.getID());
//   ASSERT_THROW(first_record.describe(), DXAPIError);
//   second_record.destroy();
//   ASSERT_THROW(second_record.describe(), DXAPIError);
// }

TEST_F(DXRecordTest, DescribeTest) {
  DXRecord dxrecord = DXRecord::newDXRecord();
  JSON desc = dxrecord.describe();
  ASSERT_EQ(desc["project"], proj_id);
  ASSERT_EQ(desc["id"], dxrecord.getID());
  ASSERT_EQ(desc["class"], "record");
  ASSERT_EQ(desc["types"], JSON(JSON_ARRAY));
  ASSERT_EQ(desc["state"], "open");
  ASSERT_FALSE(desc["hidden"].get<bool>());
  ASSERT_EQ(desc["links"], JSON(JSON_ARRAY));
  ASSERT_EQ(desc["name"], dxrecord.getID());
  ASSERT_EQ(desc["folder"], "/");
  ASSERT_EQ(desc["tags"], JSON(JSON_ARRAY));
  ASSERT_TRUE(desc.has("created"));
  ASSERT_TRUE(desc.has("modified"));
  ASSERT_FALSE(desc.has("properties"));

  desc = dxrecord.describe(true);
  ASSERT_EQ(desc["properties"], JSON(JSON_OBJECT));

  JSON settings(JSON_OBJECT);
  JSON types = JSON(JSON_ARRAY);
  types.push_back("mapping");
  types.push_back("foo");
  JSON tags = JSON(JSON_ARRAY);
  tags.push_back("bar");
  tags.push_back("baz");
  JSON properties = JSON(JSON_OBJECT);
  properties["project"] = "cancer";
  JSON details = JSON(JSON_OBJECT);
  details["$dnanexus_link"] = dxrecord.getID();
  JSON links_to_expect = JSON(JSON_ARRAY);
  links_to_expect.push_back(dxrecord.getID());

  settings["types"] = types;
  settings["tags"] = tags;
  settings["properties"] = properties;
  settings["hidden"] = true;
  settings["details"] = details;
  settings["folder"] = "/a";
  settings["parents"] = true;
  settings["name"] = "Name";
  DXRecord second_dxrecord = DXRecord::newDXRecord(settings);
  desc = second_dxrecord.describe(true);
  ASSERT_EQ(desc["project"], proj_id);
  ASSERT_EQ(desc["id"], second_dxrecord.getID());
  ASSERT_EQ(desc["class"], "record");
  ASSERT_EQ(desc["types"], types);
  ASSERT_EQ(desc["state"], "open");
  ASSERT_TRUE(desc["hidden"].get<bool>());
  ASSERT_EQ(desc["links"], links_to_expect);
  ASSERT_EQ(desc["name"], "Name");
  ASSERT_EQ(desc["folder"], "/a");
  ASSERT_EQ(desc["tags"], tags);
  ASSERT_TRUE(desc.has("created"));
  ASSERT_TRUE(desc.has("modified"));
  ASSERT_EQ(desc["properties"], properties);
}

////////////
// DXFile //
////////////

string getBaseName(const string& filename) {
  size_t lastslash = filename.find_last_of("/\\");
  return filename.substr(lastslash+1);
}

string foofilename = "";

class DXFileTest : public testing::Test {
public:
  static const string foostr;
  string tempfilename;

  DXFile dxfile;

protected:
  virtual void SetUp() {
    char name [L_tmpnam];
    tmpnam(name);
    tempfilename = string(name);

    if (foofilename == "") {
      char fooname [L_tmpnam];
      tmpnam(fooname);
      foofilename = string(fooname);
      ofstream foofile(fooname);
      foofile << foostr;
      foofile.close();
    }
  }

  virtual void TearDown() {
    remove(tempfilename.c_str());

    remove_all(proj_id);
  }
};

const string DXFileTest::foostr = "foo\n";

TEST_F(DXFileTest, UploadDownloadFiles) {
  dxfile = DXFile::uploadLocalFile(foofilename);
  dxfile.waitOnClose();
  ASSERT_FALSE(dxfile.is_open());

  EXPECT_EQ(getBaseName(foofilename),
  	    dxfile.describe(true)["properties"]["name"].get<string>());

  DXFile::downloadDXFile(dxfile.getID(), tempfilename);

  char stored[10];
  ifstream downloadedfile(tempfilename.c_str());
  downloadedfile.read(stored, 10);
  ASSERT_EQ(foostr.size(), downloadedfile.gcount());
  ASSERT_EQ(foostr, string(stored, downloadedfile.gcount()));
}

TEST_F(DXFileTest, WriteReadFile) {
  dxfile = DXFile::newDXFile();
  dxfile.write(foostr.data(), foostr.length());
  dxfile.close();

  DXFile same_dxfile = DXFile::openDXFile(dxfile.getID());
  same_dxfile.waitOnClose();

  char stored[10];
  same_dxfile.read(stored, foostr.length());
  ASSERT_EQ(foostr, string(stored, same_dxfile.gcount()));
  EXPECT_TRUE(same_dxfile.eof());

  same_dxfile.seek(1);
  EXPECT_FALSE(same_dxfile.eof());
  same_dxfile.read(stored, foostr.length());
  ASSERT_EQ(foostr.substr(1), string(stored, same_dxfile.gcount()));
}

TEST_F(DXFileTest, StreamingOperators) {
  dxfile = DXFile::newDXFile();
  stringstream samestr;
  dxfile  << "foo" << 1 << " " << 2.5 << endl;
  samestr << "foo" << 1 << " " << 2.5 << endl;
  dxfile  << "bar" << endl;
  samestr << "bar" << endl;
  dxfile.close(true);
  
  char stored[50];
  DXFile::downloadDXFile(dxfile.getID(), tempfilename);
  ifstream downloadedfile(tempfilename.c_str());
  downloadedfile.read(stored, 50);
  ASSERT_EQ(samestr.str(), string(stored, downloadedfile.gcount()));

  // TODO: Test >> if/when implemented
}

//////////////
// DXGTable //
//////////////

class DXGTableTest : public testing::Test {
public:
  DXGTable dxgtable;
  vector<JSON> columns;

protected:
  virtual void SetUp() {
    columns = vector<JSON>();
    columns.push_back(DXGTable::columnDesc("a", "string"));
    columns.push_back(DXGTable::columnDesc("b", "int32"));
  }
  virtual void TearDown() {
    try {
      dxgtable.remove();
    } catch (...) {
    }
  }
};

TEST_F(DXGTableTest, CreateDXGTableTest) {
  dxgtable = DXGTable::newDXGTable(DXGTableTest::columns);
  JSON desc = dxgtable.describe();
  ASSERT_EQ(DXGTableTest::columns.size(), desc["columns"].size());
  for (int i = 0; i < DXGTableTest::columns.size(); i++) {
    ASSERT_EQ(DXGTableTest::columns[i]["name"].get<string>(),
              desc["columns"][i]["name"].get<string>());
    ASSERT_EQ(DXGTableTest::columns[i]["type"].get<string>(),
              desc["columns"][i]["type"].get<string>());
  }
}

TEST_F(DXGTableTest, ExtendDXGTableTest) {
  DXGTable table_to_extend = DXGTable::newDXGTable(DXGTableTest::columns);
  try {
    table_to_extend.addRows(JSON::parse("[[\"Row 1\", 1], [\"Row 2\", 2]]"));
    table_to_extend.close(true);
    EXPECT_EQ("closed", table_to_extend.describe()["state"].get<string>());
    

    vector<JSON> more_cols;
    more_cols.push_back(DXGTable::columnDesc("c", "int32"));
    more_cols.push_back(DXGTable::columnDesc("d", "string"));
    dxgtable = DXGTable::extendDXGTable(table_to_extend.getID(),
                                        more_cols);

    JSON desc = dxgtable.describe();
    ASSERT_EQ(4, desc["columns"].size());
    for (int i = 2; i < 4; i++) {
      ASSERT_EQ(more_cols[i-2]["name"].get<string>(),
                desc["columns"][i]["name"].get<string>());
      ASSERT_EQ(more_cols[i-2]["type"].get<string>(),
                desc["columns"][i]["type"].get<string>());
    }
    dxgtable.addRows(JSON::parse("[[10, \"End row 1\"], [20, \"End row 2\"]]"));

    dxgtable.close(true);
  } catch (int e) {
    try {
      table_to_extend.remove();
    } catch (...) {
    }
    throw e;
  }
}

TEST_F(DXGTableTest, AddRowsTest) {
  dxgtable = DXGTable::newDXGTable(DXGTableTest::columns);
  dxgtable.addRows(JSON(JSON_ARRAY), 9999);

  JSON empty_row = JSON(JSON_ARRAY);
  empty_row.push_back(JSON(JSON_ARRAY));
  ASSERT_THROW(dxgtable.addRows(empty_row, 9997), DXAPIError);

  for (int i = 0; i < 64; i++) {
    string rowstr = "[[\"Row " + boost::lexical_cast<string>(i) +
      "\", " + boost::lexical_cast<string>(i) + "]]";
    dxgtable.addRows(JSON::parse(rowstr), i+1);
  }

  dxgtable.close();

  ASSERT_THROW(dxgtable.close(), DXAPIError);
}

TEST_F(DXGTableTest, AddRowsNoIndexTest) {
  dxgtable = DXGTable::newDXGTable(DXGTableTest::columns);

  for (int i = 0; i < 64; i++) {
    string rowstr = "[[\"Row " + boost::lexical_cast<string>(i) +
      "\", " + boost::lexical_cast<string>(i+1) + "]]";
    dxgtable.addRows(JSON::parse(rowstr));
  }
  dxgtable.flush();
  JSON desc = dxgtable.describe();
  EXPECT_EQ(1, desc["parts"].size());

  dxgtable.close(true);

  desc = dxgtable.describe();
  EXPECT_EQ(64, desc["size"].get<int>());
}

TEST_F(DXGTableTest, InvalidSpecTest) {
  vector<JSON> invalid_spec = columns;
  invalid_spec[1]["type"] = "muffins";
  ASSERT_THROW(DXGTable::newDXGTable(invalid_spec),
	       DXAPIError);
}

TEST_F(DXGTableTest, GetRowsTest) {
  dxgtable = DXGTable::newDXGTable(DXGTableTest::columns);

  for (int i = 0; i < 64; i++) {
    string rowstr = "[[\"Row " + boost::lexical_cast<string>(i) +
      "\", " + boost::lexical_cast<string>(i+1) + "]]";
    dxgtable.addRows(JSON::parse(rowstr), i+1);
  }
  dxgtable.close(true);

  JSON rows = dxgtable.getRows();
  EXPECT_EQ(64, rows["size"].get<int>());
  EXPECT_EQ(JSON_NULL, rows["next"].type());
  EXPECT_EQ(64, rows["data"].size());
}

TEST_F(DXGTableTest, GRITest) {
  JSON rows1 = JSON::parse("[[\"chr2\", 22, 28, \"j\"], [\"chr1\",  0,  3, \"a\"], [\"chr1\",  5,  8, \"b\"]]");
  JSON rows10 = JSON::parse("[[\"chr1\", 25, 30, \"i\"], [\"chr1\",  6, 10, \"c\"], [\"chr1\", 19, 20, \"h\"]]");
  JSON rows100 = JSON::parse("[[\"chr1\",  8,  9, \"d\"], [\"chr1\", 17, 19, \"g\"], [\"chr1\", 15, 23, \"e\"]]");
  JSON rows1000 = JSON::parse("[[\"chr1\", 16, 21, \"f\"]]");
  vector<JSON> columns;
  columns.push_back(JSON::parse("{ \"name\": \"foo\", \"type\": \"string\" }"));
  columns.push_back(JSON::parse("{ \"name\": \"bar\", \"type\": \"int32\" }"));
  columns.push_back(JSON::parse("{ \"name\": \"baz\", \"type\": \"int32\" }"));
  columns.push_back(JSON::parse("{ \"name\": \"quux\", \"type\": \"string\" }"));
  JSON genomic_index = DXGTable::genomicRangeIndex("foo", "bar", "baz");
  ASSERT_EQ(genomic_index, JSON::parse("{\"name\": \"gri\", \"type\": \"genomic\", \"chr\": \"foo\", \"lo\": \"bar\", \"hi\": \"baz\"}"));
  vector<JSON> indices;
  indices.push_back(genomic_index);

  dxgtable = DXGTable::newDXGTable(columns, indices);
  JSON desc = dxgtable.describe();
  ASSERT_EQ(desc["indices"][0], genomic_index);

  dxgtable.addRows(rows1, 1);
  dxgtable.addRows(rows10, 10);
  dxgtable.addRows(rows100, 100);
  dxgtable.addRows(rows1000, 1000);

  dxgtable.close(true);

  desc = dxgtable.describe();
  ASSERT_EQ(desc["size"].get<int>(), 10);

  //Offset + limit queries
  JSON result = dxgtable.getRows(JSON(JSON_NULL), JSON(JSON_NULL), 0, 1);
  ASSERT_EQ(result["data"],
            JSON::parse("[[0, \"chr1\",  0,  3, \"a\"]]"));
  ASSERT_EQ(result["next"].get<int>(), 1);
  ASSERT_EQ(result["size"].get<int>(), 1);

  result = dxgtable.getRows(JSON(JSON_NULL), JSON(JSON_NULL), 4, 3);
  ASSERT_EQ(result["data"],
            JSON::parse("[[4, \"chr1\", 15, 23, \"e\"], [5, \"chr1\", 16, 21, \"f\"], [6, \"chr1\", 17, 19, \"g\"]]"));
  ASSERT_EQ(result["next"].get<int>(), 7);
  ASSERT_EQ(result["size"].get<int>(), 3);

  // Range query
  JSON genomic_query = DXGTable::genomicRangeQuery("chr1", 22, 25);
  result = dxgtable.getRows(genomic_query);
  ASSERT_EQ(result["data"],
            JSON::parse("[[4, \"chr1\", 15, 23, \"e\"]]"));
  ASSERT_EQ(result["next"], JSON(JSON_NULL));
  ASSERT_EQ(result["size"].get<int>(), 1);

  // Range query with nonconsecutive rows in result
  genomic_query = DXGTable::genomicRangeQuery("chr1", 20, 26);
  result = dxgtable.getRows(genomic_query);
  ASSERT_EQ(result["data"],
            JSON::parse("[[4, \"chr1\", 15, 23, \"e\"], [5, \"chr1\", 16, 21, \"f\"], [8, \"chr1\", 25, 30, \"i\"]]"));
  ASSERT_EQ(result["next"], JSON(JSON_NULL));
  ASSERT_EQ(result["size"].get<int>(), 3);

  // TODO: Test with > 1 index
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  loadFromEnvironment();
  int result = RUN_ALL_TESTS();
  remove(foofilename.c_str());
  return result;
}
