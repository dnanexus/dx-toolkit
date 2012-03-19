#include <iostream>
#include <fstream>
#include <string>
#include <gtest/gtest.h>
#include "json.h"
#include "dxcpp.h"

using namespace std;

// TODO: Finish writing tests for other classes.

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
  dxfile = uploadLocalFile(foofilename);
  dxfile.waitOnClose();
  ASSERT_FALSE(dxfile.is_open());

  // TODO: Uncomment/fix for actual JSON interface
  //  EXPECT_EQ(getBaseName(foofilename),
  //	    dxfile.getProperties["name"]);

  downloadDXFile(dxfile.getID(), tempfilename);

  string stored;
  ifstream downloadedfile(tempfilename.c_str());
  downloadedfile >> stored;
  ASSERT_EQ(foostr, stored);
}

TEST_F(DXFileTest, UploadString) {
  // TODO
}

TEST_F(DXFileTest, WriteReadFile) {
  // TODO

  dxfile = newDXFile();
  dxfile.write(DXFileTest::foostr.data(), DXFileTest::foostr.length());

  DXFile same_dxfile = openDXFile(dxfile.getID());
  same_dxfile.waitOnClose();

  char buf[10];
  same_dxfile.read(buf, foostr.length()+1);
  ASSERT_EQ(buf, DXFileTest::foostr.c_str());
  EXPECT_TRUE(same_dxfile.eof());

  same_dxfile.seek(1);
  EXPECT_FALSE(same_dxfile.eof());
  same_dxfile.read(buf, foostr.length());
  ASSERT_EQ(buf, DXFileTest::foostr.substr(1).c_str());
}

TEST_F(DXFileTest, StreamingOperators) {
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

const JSON DXTableTest::columns = JSON("[\"a:string\", \"b:int32\"]");

TEST_F(DXTableTest, CreateDXTableTest) {
  dxtable = newDXTable(DXTableTest::columns);
  JSON desc = dxtable.describe();
  ASSERT_EQ(DXTableTest::columns, desc["columns"]);
}

TEST_F(DXTableTest, ExtendDXTableTest) {
  DXTable table_to_extend = newDXTable(DXTableTest::columns);
  try {
    table_to_extend.addRows(JSON("[[\"Row 1\", 1], [\"Row 2\", 2]]"));
    table_to_extend.close(true);
    EXPECT_EQ("closed", table_to_extend.describe()["state"]);
    

    JSON more_cols("[\"c:int32\", \"d:string\"]");
    dxtable = extendDXTable(table_to_extend.getID(),
				  more_cols);

    ASSERT_EQ(JSON("[\"a:string\", \"b:int32\", \"c:int32\", \"d:string\"]"),
	      dxtable.describe()["columns"]);

    dxtable.addRows(JSON("[[10, \"End row 1\"], [20, \"End row 2\"]]"));

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
  dxtable = newDXTable(DXTableTest::columns);
  // TODO: Finish this.
}

TEST_F(DXTableTest, AddRowsNoIndexTest) {
  // TODO: Finish this.
}

TEST_F(DXTableTest, InvalidSpecTest) {
  ASSERT_THROW(newDXTable(JSON("[\"a:string\", \"b:muffins\"]")),
	       DXAPIError);
}

TEST_F(DXTableTest, GetRowsTest) {
  // TODO: Finish this.
}

class DXJSONTest : public testing::Test {
public:

  static const JSON example_json;
  static const JSON another_example_json;
};

const JSON DXJSONTest::example_json = JSON("{\"foo\": \"bar\", \"alpha\": [1, 2, 3]}");
const JSON DXJSONTest::another_example_json = JSON("[\"foo\", \"bar\", {\"alpha\": [1, 2.340, -10]}]");

TEST_F(DXJSONTest, CreateDestroyDXJSONTest) {
  DXJSON first_dxjson = newDXJSON(DXJSONTest::example_json);
  string first_id = first_dxjson.getID();
  ASSERT_EQ(29, first_id.size());
  EXPECT_TRUE(first_id.find("json-") != string::npos);
  ASSERT_EQ(DXJSONTest::example_json, first_dxjson.get());

  DXJSON second_dxjson(first_id);
  ASSERT_EQ(first_dxjson.getID(), second_dxjson.getID());
  ASSERT_EQ(first_dxjson.get(), second_dxjson.get());

  second_dxjson.create(DXJSONTest::example_json);
  ASSERT_NE(first_dxjson.getID(), second_dxjson.getID());
  ASSERT_EQ(first_dxjson.get(), second_dxjson.get());

  ASSERT_NO_THROW(first_dxjson.destroy());
  cout << first_dxjson.getID() << endl;

  second_dxjson.destroy();

  DXJSON third_dxjson(first_id);
  ASSERT_THROW(third_dxjson.destroy(), DXAPIError);
}

TEST_F(DXJSONTest, DescribeTest) {
  DXJSON dxjson = newDXJSON(DXJSONTest::example_json);
  JSON desc = dxjson.describe();
  ASSERT_EQ(dxjson.getID(), desc["id"]);
  ASSERT_EQ("json", desc["class"]);
  // Check desc has key "types", "createdAt"
  dxjson.destroy();
}

TEST_F(DXJSONTest, PropertiesTest) {
  DXJSON dxjson = newDXJSON(DXJSONTest::example_json);
  JSON properties("{\"project\": \"cancer project\", \"foo\": \"bar\"}");
  dxjson.setProperties(properties);
  ASSERT_EQ(properties, dxjson.getProperties()); // TODO: See if equality is being implemented and how.
  ASSERT_EQ("cancer project", dxjson.getProperties()["project"]);
  ASSERT_EQ("bar", dxjson.getProperties()["foo"]);
  ASSERT_EQ("cancer project", dxjson.getProperties()["project"]);
  dxjson.setProperties(JSON("{\"project\": null}"));
  //ASSERT_EQ(JSON_NULL, dxjson.getProperties(JSON("[\"project\"]"))["project"]); TODO: Fix when interface of JSON class is known

  dxjson.destroy();
}

TEST_F(DXJSONTest, TypesTest) {
  DXJSON dxjson = newDXJSON(DXJSONTest::example_json);
  JSON types("[\"foo\", \"othertype\"]");
  dxjson.addTypes(types);
  ASSERT_EQ(types, dxjson.getTypes());

  dxjson.removeTypes(JSON("[\"foo\"]"));
  ASSERT_EQ(JSON("[\"othertype\"]"), dxjson.getTypes());

  dxjson.destroy();
}

TEST_F(DXJSONTest, GetSetTest) {
  DXJSON dxjson = newDXJSON(DXJSONTest::example_json);
  ASSERT_EQ(DXJSONTest::example_json, dxjson.get());

  dxjson.set(DXJSONTest::another_example_json);
  ASSERT_EQ(DXJSONTest::another_example_json, dxjson.get());

  dxjson.destroy();
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  loadFromEnvironment();
  int result = RUN_ALL_TESTS();
  remove(foofilename.c_str());
  return result;
}
