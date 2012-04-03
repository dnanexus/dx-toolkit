#include <iostream>
#include <fstream>
#include <string>
#include <boost/lexical_cast.hpp>
#include <gtest/gtest.h>
#include "dxjson/dxjson.h"
#include "dxcpp.h"

using namespace std;
using namespace dx;

// TODO: Finish writing tests for other classes.

//////////////
// DXRecord //
//////////////

class DXRecordTest : public testing::Test {
public:
  static const JSON example_JSON;
  static const JSON another_example_JSON;
  DXRecord dxrecord;
};

const JSON DXRecordTest::example_JSON =
  JSON::parse("{\"foo\": \"bar\", \"alpha\": [1, 2, 3]}");
const JSON DXRecordTest::another_example_JSON =
  JSON::parse("[\"foo\", \"bar\", {\"alpha\": [1, 2.340, -10]}]");

TEST_F(DXRecordTest, CreateDestroyJSONTest) {
  DXRecord first_record = DXRecord::newDXRecord(DXRecordTest::example_JSON);
  ASSERT_EQ(DXRecordTest::example_JSON,
	    first_record.get());
  string firstID = first_record.getID();

  DXRecord second_record = DXRecord(first_record.getID());
  ASSERT_EQ(first_record.getID(), second_record.getID());
  ASSERT_EQ(first_record.get(), second_record.get());

  second_record.create(DXRecordTest::example_JSON);
  ASSERT_NE(first_record.getID(), second_record.getID());
  ASSERT_EQ(first_record.get(), second_record.get());

  ASSERT_NO_THROW(first_record.describe());

  first_record.destroy();
  ASSERT_NO_THROW(first_record.getID());
  ASSERT_THROW(first_record.describe(), DXAPIError);
  second_record.destroy();
  ASSERT_THROW(second_record.describe(), DXAPIError);
}

TEST_F(DXRecordTest, GetSetJSONTest) {
  DXRecord dxrecord = DXRecord::newDXRecord(DXRecordTest::example_JSON);
  ASSERT_EQ(DXRecordTest::example_JSON, dxrecord.get());

  dxrecord.set(DXRecordTest::another_example_JSON);
  ASSERT_NE(DXRecordTest::example_JSON, dxrecord.get());
  ASSERT_EQ(DXRecordTest::another_example_JSON, dxrecord.get());
  dxrecord.destroy();
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

    try {
      dxfile.destroy();
    } catch (...) {
    }
  }
};

const string DXFileTest::foostr = "foo\n";

TEST_F(DXFileTest, UploadDownloadFiles) {
  dxfile = DXFile::uploadLocalFile(foofilename);
  dxfile.waitOnClose();
  ASSERT_FALSE(dxfile.is_open());

  EXPECT_EQ(getBaseName(foofilename),
  	    dxfile.getProperties()["name"].get<string>());

  DXFile::downloadDXFile(dxfile.getID(), tempfilename);

  char stored[10];
  ifstream downloadedfile(tempfilename.c_str());
  downloadedfile.read(stored, 10);
  ASSERT_EQ(foostr.size(), downloadedfile.gcount());
  ASSERT_EQ(foostr, string(stored, downloadedfile.gcount()));
}

TEST_F(DXFileTest, WriteReadFile) {
  // TODO

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

/////////////
// DXTable //
/////////////

class DXTableTest : public testing::Test {
public:
  DXTable dxtable;
  static const JSON columns;

protected:
  virtual void TearDown() {
    try {
      dxtable.destroy();
    } catch (...) {
    }
  }
};

const JSON DXTableTest::columns = JSON::parse("[\"a:string\", \"b:int32\"]");

TEST_F(DXTableTest, ColumnDescTest) {
  JSON columns(JSON_ARRAY);
  columns.push_back(DXTable::columnDesc("a", "string"));
  columns.push_back(DXTable::columnDesc("b", "int32"));
  ASSERT_EQ(DXTableTest::columns, columns);
}

TEST_F(DXTableTest, CreateDXTableTest) {
  dxtable = DXTable::newDXTable(DXTableTest::columns);
  JSON desc = dxtable.describe();
  ASSERT_EQ(DXTableTest::columns, desc["columns"]);
}

TEST_F(DXTableTest, ExtendDXTableTest) {
  DXTable table_to_extend = DXTable::newDXTable(DXTableTest::columns);
  try {
    table_to_extend.addRows(JSON::parse("[[\"Row 1\", 1], [\"Row 2\", 2]]"));
    table_to_extend.close(true);
    EXPECT_EQ("closed", table_to_extend.describe()["state"].get<string>());
    

    JSON more_cols = JSON::parse("[\"c:int32\", \"d:string\"]");
    dxtable = DXTable::extendDXTable(table_to_extend.getID(),
				  more_cols);

    ASSERT_EQ(JSON::parse("[\"a:string\", \"b:int32\", \"c:int32\", \"d:string\"]"),
	      dxtable.describe()["columns"]);

    dxtable.addRows(JSON::parse("[[10, \"End row 1\"], [20, \"End row 2\"]]"));

    dxtable.close(true);
  } catch (int e) {
    try {
      table_to_extend.destroy();
    } catch (...) {
    }
    throw e;
  }
}

TEST_F(DXTableTest, AddRowsTest) {
  dxtable = DXTable::newDXTable(DXTableTest::columns);
  dxtable.addRows(JSON(JSON_ARRAY), 9999);

  JSON empty_row = JSON(JSON_ARRAY);
  empty_row.push_back(JSON(JSON_ARRAY));
  ASSERT_THROW(dxtable.addRows(empty_row, 9997), DXAPIError);

  for (int i = 0; i < 64; i++) {
    string rowstr = "[[\"Row " + boost::lexical_cast<string>(i) +
      "\", " + boost::lexical_cast<string>(i) + "]]";
    dxtable.addRows(JSON::parse(rowstr), i+1);
  }

  dxtable.close();

  ASSERT_THROW(dxtable.close(), DXAPIError);
}

TEST_F(DXTableTest, AddRowsNoIndexTest) {
  dxtable = DXTable::newDXTable(DXTableTest::columns);

  for (int i = 0; i < 64; i++) {
    string rowstr = "[[\"Row " + boost::lexical_cast<string>(i) +
      "\", " + boost::lexical_cast<string>(i+1) + "]]";
    dxtable.addRows(JSON::parse(rowstr));
  }
  dxtable.flush();
  JSON desc = dxtable.describe();
  EXPECT_EQ(1, desc["parts"].size());

  dxtable.close(true);

  desc = dxtable.describe();
  EXPECT_EQ(64, desc["size"].get<int>());
}

TEST_F(DXTableTest, InvalidSpecTest) {
  ASSERT_THROW(DXTable::newDXTable(JSON::parse("[\"a:string\", \"b:muffins\"]")),
	       DXAPIError);
}

TEST_F(DXTableTest, GetRowsTest) {
  dxtable = DXTable::newDXTable(DXTableTest::columns);

  for (int i = 0; i < 64; i++) {
    string rowstr = "[[\"Row " + boost::lexical_cast<string>(i) +
      "\", " + boost::lexical_cast<string>(i+1) + "]]";
    dxtable.addRows(JSON::parse(rowstr), i+1);
  }
  dxtable.close(true);

  JSON rows = dxtable.getRows();
  EXPECT_EQ(64, rows["size"].get<int>());
  EXPECT_EQ(JSON_NULL, rows["next"].type());
  EXPECT_EQ(64, rows["data"].size());
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  loadFromEnvironment();
  int result = RUN_ALL_TESTS();
  remove(foofilename.c_str());
  return result;
}
