#include <iostream>
#include <fstream>
#include <string>
#include <boost/lexical_cast.hpp>
#include <gtest/gtest.h>
#include "dxjson/dxjson.h"
#include "dxcpp.h"

using namespace std;
using namespace dx;

string proj_id = "";
string second_proj_id = "";

// TODO: Finish writing tests for other classes.

JSON getObjFromListf(JSON &listf) {
  JSON objects(JSON_ARRAY);
  for (int i = 0; i < listf["objects"].size(); i++) {
    objects.push_back(listf["objects"][i]["id"].get<string>());
  }
  return objects;
}

void remove_all(const string &proj, const string &folder="/") {
  DXProject dxproject(proj);
  dxproject.removeFolder(folder, true);
}

////////////
// DXLink //
////////////

TEST(DXLinkTest, CreationTest) {
  string record_id = "record-0000000000000000000000pb";
  JSON link = DXLink(record_id);
  EXPECT_EQ(record_id, link["$dnanexus_link"].get<string>());
  EXPECT_EQ(1, link.size());
}

///////////////
// DXProject //
///////////////

class DXProjectTest : public testing::Test {
  virtual void TearDown() {
    remove_all(proj_id);
    remove_all(second_proj_id);
  }
};

TEST_F(DXProjectTest, UpdateDescribeTest) {
  DXProject dxproject;
  JSON to_update(JSON_OBJECT);
  to_update["name"] = "newprojname";
  to_update["protected"] = true;
  to_update["restricted"] = true;
  to_update["description"] = "new description";
  dxproject.update(to_update);
  JSON desc = dxproject.describe();
  ASSERT_EQ(desc["id"].get<string>(), proj_id);
  ASSERT_EQ(desc["class"].get<string>(), "project");
  ASSERT_EQ(desc["name"].get<string>(), "newprojname");
  ASSERT_EQ(desc["protected"].get<bool>(), true);
  ASSERT_EQ(desc["restricted"].get<bool>(), true);
  ASSERT_EQ(desc["description"].get<string>(), "new description");
  ASSERT_TRUE(desc.has("created"));
  ASSERT_FALSE(desc.has("folders"));
  desc = dxproject.describe(true);
  ASSERT_EQ(desc["folders"].size(), 1);
  ASSERT_EQ(desc["folders"][0].get<string>(), "/");

  to_update["restricted"] = false;
  dxproject.update(to_update);
}

TEST_F(DXProjectTest, NewListRemoveFoldersTest) {
  DXProject dxproject;
  JSON listf = dxproject.listFolder();
  ASSERT_EQ(listf["folders"], JSON(JSON_ARRAY));
  ASSERT_EQ(listf["objects"], JSON(JSON_ARRAY));

  DXRecord dxrecord = DXRecord::newDXRecord();
  dxproject.newFolder("/a/b/c/d", true);
  listf = dxproject.listFolder();
  JSON expected(JSON_ARRAY);
  expected.push_back("/a");
  ASSERT_EQ(listf["folders"], expected);
  expected[0] = dxrecord.getID();
  ASSERT_EQ(listf["objects"].size(), 1);
  ASSERT_EQ(listf["objects"][0]["id"], expected[0]);
  listf = dxproject.listFolder("/a");
  expected[0] = "/a/b";
  ASSERT_EQ(listf["folders"], expected);
  ASSERT_EQ(listf["objects"], JSON(JSON_ARRAY));
  listf = dxproject.listFolder("/a/b");
  expected[0] = "/a/b/c";
  ASSERT_EQ(listf["folders"], expected);
  listf = dxproject.listFolder("/a/b/c");
  expected[0] = "/a/b/c/d";
  ASSERT_EQ(listf["folders"], expected);
  listf = dxproject.listFolder("/a/b/c/d");
  ASSERT_EQ(listf["folders"], JSON(JSON_ARRAY));

  ASSERT_THROW(dxproject.removeFolder("/a"), DXAPIError);
  dxproject.removeFolder("/a/b/c/d");
  dxproject.removeFolder("/a//b////c/");
  dxproject.removeFolder("/a/b");
  dxproject.removeFolder("/a");
  dxrecord.remove();
  listf = dxproject.listFolder();
  ASSERT_EQ(listf["objects"], JSON(JSON_ARRAY));
  ASSERT_EQ(listf["folders"], JSON(JSON_ARRAY));
}

TEST_F(DXProjectTest, MoveTest) {
  DXProject dxproject;
  dxproject.newFolder("/a/b/c/d", true);
  vector<DXRecord> dxrecords;
  JSON options(JSON_OBJECT);
  for (int i = 0; i < 4; i++) {
    options["name"] = "record-" + boost::lexical_cast<string>(i);
    dxrecords.push_back(DXRecord::newDXRecord(options));
  }
  JSON objects_to_move(JSON_ARRAY);
  objects_to_move.push_back(dxrecords[0].getID());
  objects_to_move.push_back(dxrecords[1].getID());
  JSON folders_to_move(JSON_ARRAY);
  folders_to_move.push_back("/a/b/c/d");
  dxproject.move(objects_to_move,
                 folders_to_move,
                 "/a");
  JSON listf = dxproject.listFolder();
  JSON expected(JSON_ARRAY);
  expected.push_back(dxrecords[2].getID());
  expected.push_back(dxrecords[3].getID());
  ASSERT_EQ(listf["objects"].size(), expected.size());
  ASSERT_TRUE(listf["objects"][0]["id"] == expected[0] ||
              listf["objects"][1]["id"] == expected[0]);
  ASSERT_TRUE(listf["objects"][0]["id"] == expected[1] ||
              listf["objects"][1]["id"] == expected[1]);
  expected = JSON(JSON_ARRAY);
  expected.push_back("/a");
  ASSERT_EQ(listf["folders"], expected);

  listf = dxproject.listFolder("/a");
  expected = JSON(JSON_ARRAY);
  expected.push_back(dxrecords[0].getID());
  expected.push_back(dxrecords[1].getID());
  ASSERT_EQ(listf["objects"].size(), expected.size());
  ASSERT_TRUE(listf["objects"][0]["id"] == expected[0] ||
              listf["objects"][1]["id"] == expected[0]);
  ASSERT_TRUE(listf["objects"][0]["id"] == expected[1] ||
              listf["objects"][1]["id"] == expected[1]);
  expected = JSON(JSON_ARRAY);
  expected.push_back("/a/b");
  expected.push_back("/a/d");
  ASSERT_EQ(listf["folders"].size(), expected.size());
  ASSERT_TRUE(listf["folders"][0] == expected[0] ||
              listf["folders"][1] == expected[0]);
  ASSERT_TRUE(listf["folders"][0] == expected[1] ||
              listf["folders"][1] == expected[1]);

  JSON desc = dxrecords[0].describe();
  ASSERT_EQ(desc["folder"].get<string>(), "/a");
}

TEST_F(DXProjectTest, CloneTest) {
  DXProject dxproject;
  dxproject.newFolder("/a/b/c/d", true);
  vector<DXRecord> dxrecords;
  JSON options(JSON_OBJECT);
  for (int i = 0; i < 4; i++) {
    options["name"] = "record-" + boost::lexical_cast<string>(i);
    dxrecords.push_back(DXRecord::newDXRecord(options));
  }
  JSON objects_to_clone(JSON_ARRAY);
  objects_to_clone.push_back(dxrecords[0].getID());
  objects_to_clone.push_back(dxrecords[1].getID());
  JSON folders_to_clone(JSON_ARRAY);
  folders_to_clone.push_back("/a/b/c/d");
  ASSERT_THROW(dxproject.clone(objects_to_clone, folders_to_clone,
                               second_proj_id), DXAPIError);

  dxrecords[0].close();
  dxrecords[1].close();
  dxproject.clone(objects_to_clone, folders_to_clone, second_proj_id);

  DXProject second_proj(second_proj_id);
  JSON listf = second_proj.listFolder();
  JSON expected(JSON_ARRAY);
  expected.push_back(dxrecords[0].getID());
  expected.push_back(dxrecords[1].getID());
  ASSERT_EQ(listf["objects"].size(), expected.size());
  ASSERT_TRUE(listf["objects"][0]["id"] == expected[0] ||
              listf["objects"][1]["id"] == expected[0]);
  ASSERT_TRUE(listf["objects"][0]["id"] == expected[1] ||
              listf["objects"][1]["id"] == expected[1]);
  expected = JSON(JSON_ARRAY);
  expected.push_back("/d");
  ASSERT_EQ(listf["folders"], expected);
}

TEST_F(DXProjectTest, CloneRemoveObjectsTest) {
  DXProject dxproject;
  DXRecord dxrecord = DXRecord::newDXRecord();
  dxrecord.close();

  JSON listf = dxproject.listFolder();
  JSON id(JSON_ARRAY);
  id.push_back(dxrecord.getID());
  ASSERT_EQ(listf["objects"].size(), 1);
  ASSERT_EQ(listf["objects"][0]["id"].get<string>(), dxrecord.getID());

  DXProject second_project(second_proj_id);
  second_project.newFolder("/a");
  dxproject.cloneObjects(id, second_proj_id, "/a");
  listf = second_project.listFolder("/a");
  ASSERT_EQ(listf["objects"].size(), 1);
  ASSERT_EQ(listf["objects"][0]["id"].get<string>(), dxrecord.getID());

  dxproject.removeObjects(id);
  listf = dxproject.listFolder();
  ASSERT_EQ(listf["objects"], JSON(JSON_ARRAY));
  JSON desc = dxrecord.describe();
  ASSERT_EQ(desc["folder"], "/a");
}

//////////////
// DXRecord //
//////////////

class DXRecordTest : public testing::Test {
  virtual void TearDown() {
    remove_all(proj_id);
    remove_all(second_proj_id);
  }
public:
  static const JSON example_JSON;
};

const JSON DXRecordTest::example_JSON =
  JSON::parse("{\"foo\": \"bar\", \"alpha\": [1, 2, 3]}");

TEST_F(DXRecordTest, CreateRemoveTest) {
  JSON options(JSON_OBJECT);
  options["details"] = DXRecordTest::example_JSON;
  DXRecord first_record = DXRecord::newDXRecord(options);
  ASSERT_EQ(DXRecordTest::example_JSON,
	    first_record.getDetails());
  ASSERT_EQ(first_record.getProjectID(), proj_id);
  string firstID = first_record.getID();

  DXRecord second_record(firstID);
  ASSERT_EQ(first_record.getID(), second_record.getID());
  ASSERT_EQ(first_record.getDetails(), second_record.getDetails());
  ASSERT_EQ(second_record.getProjectID(), proj_id);

  options["project"] = second_proj_id;
  second_record.create(options);
  ASSERT_NE(first_record.getID(), second_record.getID());
  ASSERT_EQ(second_record.getProjectID(), second_proj_id);
  ASSERT_EQ(first_record.getDetails(), second_record.getDetails());

  ASSERT_NO_THROW(first_record.describe());

  first_record.remove();
  ASSERT_THROW(first_record.describe(), DXAPIError);
  second_record.remove();
  ASSERT_THROW(second_record.describe(), DXAPIError);

  DXRecord third_record(firstID);
  ASSERT_THROW(third_record.describe(), DXAPIError);
}

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
  ASSERT_EQ(desc["id"].get<string>(), second_dxrecord.getID());
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

TEST_F(DXRecordTest, TypesTest) {
  DXRecord dxrecord = DXRecord::newDXRecord();
  vector<string> types;
  types.push_back("foo");
  types.push_back("othertype");
  dxrecord.addTypes(types);
  ASSERT_EQ(dxrecord.describe()["types"], JSON(types));

  types.pop_back();
  dxrecord.removeTypes(types);
  ASSERT_EQ("othertype", dxrecord.describe()["types"][0].get<string>());
}

TEST_F(DXRecordTest, DetailsTest) {
  JSON details_no_link(JSON_OBJECT);
  details_no_link["foo"] = "bar";

  DXRecord dxrecord = DXRecord::newDXRecord();
  dxrecord.setDetails(details_no_link);
  ASSERT_EQ(dxrecord.getDetails(), details_no_link);
  ASSERT_EQ(dxrecord.describe()["links"], JSON(JSON_ARRAY));

  JSON details_two_links(JSON_ARRAY);
  details_two_links.push_back(JSON(JSON_OBJECT));
  details_two_links[0]["$dnanexus_link"] = dxrecord.getID();
  details_two_links.push_back(JSON(JSON_OBJECT));
  details_two_links[1]["$dnanexus_link"] = dxrecord.getID();

  dxrecord.setDetails(details_two_links);
  ASSERT_EQ(dxrecord.getDetails(), details_two_links);
  JSON links = dxrecord.describe()["links"];
  ASSERT_EQ(links.size(), 1);
  ASSERT_EQ(links[0].get<string>(), dxrecord.getID());
}

TEST_F(DXRecordTest, VisibilityTest) {
  DXRecord dxrecord = DXRecord::newDXRecord();
  dxrecord.hide();
  ASSERT_EQ(dxrecord.describe()["hidden"].get<bool>(), true);

  dxrecord.unhide();
  ASSERT_EQ(dxrecord.describe()["hidden"].get<bool>(), false);
}

TEST_F(DXRecordTest, RenameTest) {
  DXRecord dxrecord = DXRecord::newDXRecord();
  dxrecord.rename("newname");
  ASSERT_EQ(dxrecord.describe()["name"].get<string>(), "newname");

  dxrecord.rename("secondname");
  ASSERT_EQ(dxrecord.describe()["name"].get<string>(), "secondname");
}

TEST_F(DXRecordTest, SetPropertiesTest) {
  DXRecord dxrecord = DXRecord::newDXRecord();
  JSON properties(JSON_OBJECT);
  properties["project"] = "cancer project";
  properties["foo"] = "bar";
  dxrecord.setProperties(properties);
  JSON desc = dxrecord.describe(true);
  ASSERT_EQ(desc["properties"], properties);

  JSON unset_property(JSON_OBJECT);
  unset_property["project"] = JSON(JSON_NULL);
  dxrecord.setProperties(unset_property);
  properties.erase("project");
  ASSERT_EQ(dxrecord.describe(true)["properties"], properties);
}

TEST_F(DXRecordTest, TagsTest) {
  DXRecord dxrecord = DXRecord::newDXRecord();
  vector<string> tags;
  tags.push_back("foo");
  tags.push_back("othertag");
  dxrecord.addTags(tags);
  ASSERT_EQ(dxrecord.describe()["tags"], JSON(tags));

  tags.pop_back();
  dxrecord.removeTags(tags);
  ASSERT_EQ("othertag", dxrecord.describe()["tags"][0].get<string>());
}

TEST_F(DXRecordTest, ListProjectsTest) {
  DXRecord dxrecord = DXRecord::newDXRecord();
  dxrecord.close();
  dxrecord.clone(second_proj_id);
  JSON projects = dxrecord.listProjects();
  ASSERT_TRUE(projects.has(proj_id));
  ASSERT_TRUE(projects.has(second_proj_id));
}

TEST_F(DXRecordTest, CloseTest) {
  DXRecord dxrecord = DXRecord::newDXRecord();
  dxrecord.close();
  ASSERT_THROW(dxrecord.hide(), DXAPIError);
  ASSERT_THROW(dxrecord.setDetails(JSON(JSON_ARRAY)), DXAPIError);

  ASSERT_EQ(dxrecord.getDetails(), JSON(JSON_OBJECT));
  dxrecord.rename("newname");
  ASSERT_EQ(dxrecord.describe()["name"].get<string>(), "newname");

  dxrecord.rename("secondname");
  ASSERT_EQ(dxrecord.describe()["name"].get<string>(), "secondname");
}

TEST_F(DXRecordTest, CloneTest) {
  JSON options(JSON_OBJECT);
  options["name"] = "firstname";
  options["tags"] = JSON(JSON_ARRAY);
  options["tags"].push_back("tag");
  DXRecord dxrecord = DXRecord::newDXRecord(options);

  ASSERT_THROW(dxrecord.clone(second_proj_id), DXAPIError);
  dxrecord.close();

  DXRecord second_dxrecord = dxrecord.clone(second_proj_id);
  second_dxrecord.rename("newname");

  JSON first_desc = dxrecord.describe();
  JSON second_desc = second_dxrecord.describe();

  ASSERT_EQ(first_desc["id"].get<string>(), dxrecord.getID());
  ASSERT_EQ(second_desc["id"].get<string>(), dxrecord.getID());
  ASSERT_EQ(first_desc["project"].get<string>(), proj_id);
  ASSERT_EQ(second_desc["project"].get<string>(), second_proj_id);
  ASSERT_EQ(first_desc["name"].get<string>(), "firstname");
  ASSERT_EQ(second_desc["name"].get<string>(), "newname");
  ASSERT_EQ(first_desc["tags"], second_desc["tags"]);
  ASSERT_EQ(first_desc["created"], second_desc["created"]);
  ASSERT_EQ(first_desc["state"].get<string>(), "closed");
  ASSERT_EQ(second_desc["state"].get<string>(), "closed");
}

TEST_F(DXRecordTest, MoveTest) {
  DXProject dxproject = DXProject();
  dxproject.newFolder("/a/b/c/d", true);
  DXRecord dxrecord = DXRecord::newDXRecord();
  dxrecord.move("/a/b/c");
  JSON listf = dxproject.listFolder();
  ASSERT_EQ(listf["objects"], JSON(JSON_ARRAY));
  listf = dxproject.listFolder("/a/b/c");
  ASSERT_EQ(listf["objects"][0]["id"].get<string>(), dxrecord.getID());
  JSON desc = dxrecord.describe();
  ASSERT_EQ(desc["folder"].get<string>(), "/a/b/c");
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
    remove_all(second_proj_id);
  }
};

const string DXFileTest::foostr = "foo\n";

TEST_F(DXFileTest, SimpleCloneTest) {
  DXFile dxfile = DXFile::newDXFile();
  dxfile.write("foo");
  dxfile.close(true);
  dxfile.clone(second_proj_id);
  JSON projects = dxfile.listProjects();
  ASSERT_TRUE(projects.has(proj_id));
  ASSERT_TRUE(projects.has(second_proj_id));
}

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
    remove_all(proj_id);
    remove_all(second_proj_id);
  }
};

TEST_F(DXGTableTest, SimpleCloneTest) {
  DXGTable dxgtable = DXGTable::newDXGTable(DXGTableTest::columns);
  dxgtable.addRows(JSON::parse("[[\"foo\", 1], [\"foo\", 2]]"));
  dxgtable.close(true);
  dxgtable.clone(second_proj_id);
  JSON projects = dxgtable.listProjects();
  ASSERT_TRUE(projects.has(proj_id));
  ASSERT_TRUE(projects.has(second_proj_id));
}

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
  EXPECT_EQ(64, desc["length"].get<int>());
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
  EXPECT_EQ(64, rows["length"].get<int>());
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
  ASSERT_EQ(desc["length"].get<int>(), 10);

  //Offset + limit queries
  JSON result = dxgtable.getRows(JSON(JSON_NULL), JSON(JSON_NULL), 0, 1);
  ASSERT_EQ(result["data"],
            JSON::parse("[[0, \"chr1\",  0,  3, \"a\"]]"));
  ASSERT_EQ(result["next"].get<int>(), 1);
  ASSERT_EQ(result["length"].get<int>(), 1);

  result = dxgtable.getRows(JSON(JSON_NULL), JSON(JSON_NULL), 4, 3);
  ASSERT_EQ(result["data"],
            JSON::parse("[[4, \"chr1\", 15, 23, \"e\"], [5, \"chr1\", 16, 21, \"f\"], [6, \"chr1\", 17, 19, \"g\"]]"));
  ASSERT_EQ(result["next"].get<int>(), 7);
  ASSERT_EQ(result["length"].get<int>(), 3);

  // Range query
  JSON genomic_query = DXGTable::genomicRangeQuery("chr1", 22, 25);
  result = dxgtable.getRows(genomic_query);
  ASSERT_EQ(result["data"],
            JSON::parse("[[4, \"chr1\", 15, 23, \"e\"]]"));
  ASSERT_EQ(result["next"], JSON(JSON_NULL));
  ASSERT_EQ(result["length"].get<int>(), 1);

  // Range query with nonconsecutive rows in result
  genomic_query = DXGTable::genomicRangeQuery("chr1", 20, 26);
  result = dxgtable.getRows(genomic_query);
  ASSERT_EQ(result["data"],
            JSON::parse("[[4, \"chr1\", 15, 23, \"e\"], [5, \"chr1\", 16, 21, \"f\"], [8, \"chr1\", 25, 30, \"i\"]]"));
  ASSERT_EQ(result["next"], JSON(JSON_NULL));
  ASSERT_EQ(result["length"].get<int>(), 3);

  // TODO: Test with > 1 index
}

///////////
// DXApp //
///////////

// This was used to test using a locally made app object.  Once we
// have a more convenient way of generating an app object, we can put
// in tests.
//
// TEST(DXAppTest, SimpleTest) {
//   DXApp dxapp;
//   dxapp.setID("app-9zF6jpPxK60yb2Vk91600001");
//   JSON desc = dxapp.describe();
//   string name = desc["name"].get<string>();
//   DXApp second_dxapp(name);
//   JSON secondDesc = second_dxapp.describe();
//   ASSERT_EQ(desc, secondDesc);
// }

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  loadFromEnvironment();
  JSON project_hash(JSON_OBJECT);
  project_hash["name"] = "test_project";
  JSON resp = projectNew(project_hash);
  proj_id = resp["id"].get<string>();
  project_hash["name"] = "second_test_project";
  resp = projectNew(project_hash);
  second_proj_id = resp["id"].get<string>();

  setWorkspaceID(proj_id);

  int result = RUN_ALL_TESTS();
  remove(foofilename.c_str());
  projectDestroy(proj_id);
  projectDestroy(second_proj_id);

  return result;
}
