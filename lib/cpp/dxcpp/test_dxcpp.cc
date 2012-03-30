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

string getBaseName(const string& filename) {
  size_t lastslash = filename.find_last_of("/\\");
  return filename.substr(lastslash+1);
}

string foofilename = "";

class DISABLED_DXFileTest : public testing::Test {
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

const string DISABLED_DXFileTest::foostr = "foo\n";

TEST_F(DISABLED_DXFileTest, UploadDownloadFiles) {
  dxfile = DXFile::uploadLocalFile(foofilename);
  dxfile.waitOnClose();
  ASSERT_FALSE(dxfile.is_open());

  // TODO: Uncomment/fix for actual JSON interface
  //  EXPECT_EQ(getBaseName(foofilename),
  //	    dxfile.getProperties["name"]);

  DXFile::downloadDXFile(dxfile.getID(), tempfilename);

  string stored;
  ifstream downloadedfile(tempfilename.c_str());
  downloadedfile >> stored;
  ASSERT_EQ(foostr, stored);
}

TEST_F(DISABLED_DXFileTest, UploadString) {
  // TODO
}

TEST_F(DISABLED_DXFileTest, WriteReadFile) {
  // TODO

  dxfile = DXFile::newDXFile();
  dxfile.write(DISABLED_DXFileTest::foostr.data(), DISABLED_DXFileTest::foostr.length());

  DXFile same_dxfile = DXFile::openDXFile(dxfile.getID());
  same_dxfile.waitOnClose();

  char buf[10];
  same_dxfile.read(buf, foostr.length()+1);
  ASSERT_EQ(buf, DISABLED_DXFileTest::foostr.c_str());
  EXPECT_TRUE(same_dxfile.eof());

  same_dxfile.seek(1);
  EXPECT_FALSE(same_dxfile.eof());
  same_dxfile.read(buf, foostr.length());
  ASSERT_EQ(buf, DISABLED_DXFileTest::foostr.substr(1).c_str());
}

TEST_F(DISABLED_DXFileTest, StreamingOperators) {
  // TODO: Test << and >>
}

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
