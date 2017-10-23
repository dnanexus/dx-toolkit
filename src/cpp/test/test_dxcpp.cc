// Copyright (C) 2013-2016 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may
//   not use this file except in compliance with the License. You may obtain a
//   copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//   License for the specific language governing permissions and limitations
//   under the License.

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
string third_proj_id = "";

bool DXTEST_FULL = false;
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


// A helper function/global var for
// creating an applet
JSON applet_details(JSON_OBJECT);
void createANewApplet(DXApplet &apl) {
  applet_details.clear();
  applet_details = JSON(JSON_OBJECT);
  applet_details["name"] = "test_applet";
  applet_details["inputSpec"] = JSON(JSON_ARRAY);
  applet_details["inputSpec"].push_back(JSON::parse("{\"name\": \"rowFetchChunk\", \"class\": \"int\"}"));
  applet_details["runSpec"] = JSON(JSON_OBJECT);
  applet_details["outputSpec"] = JSON::parse("[{\"name\":\"message\", \"class\":\"string\"}]");
  applet_details["runSpec"]["code"] = "#!/bin/bash\n\n#main() {\necho '{\"message\": \"hello world!\"}' > job_output.json \n#}";
  applet_details["runSpec"]["interpreter"] = "bash";
  applet_details["runSpec"]["release"] = "14.04";
  applet_details["runSpec"]["distribution"] = "Ubuntu";
  applet_details["dxapi"] = "1.0.0";
  apl.create(applet_details);
}

//////////////////////
// Nonce Generation //
/////////////////////

TEST(NonceGeneration, generateNonces) {
  std::set<std::string> nonces;
  const int numNonces = 100;
  for (int i=0; i < numNonces; ++i) {
    nonces.insert(Nonce::nonce());
  }
  // This ensures that there are no duplicate nonces
  ASSERT_EQ(nonces.size(), numNonces);

  for (std::set<std::string>::iterator it = nonces.begin();
                                       it != nonces.end();
                                       ++it) {
    ASSERT_TRUE(it->size() > 0);
    ASSERT_TRUE(it->size() <= 128);
  }
}

TEST(NonceGeneration, updateInput) {
  JSON inputParams(JSON_OBJECT);
  inputParams["p1"] = "v1";
  inputParams["p2"] = "v2";
  JSON updatedInput = Nonce::updateNonce(inputParams);
  ASSERT_EQ(inputParams["p1"], updatedInput["p1"]);
  ASSERT_EQ(inputParams["p2"], updatedInput["p2"]);
  ASSERT_TRUE(updatedInput.has("nonce"));
  std::string nonce = updatedInput["nonce"].get<string>();
  ASSERT_TRUE(nonce.size() > 0);
  ASSERT_TRUE(nonce.size() <= 128);

  std::string inputNonce = Nonce::nonce();
  inputParams["nonce"] = inputNonce;
  updatedInput = Nonce::updateNonce(inputParams);
  ASSERT_EQ(inputParams["p1"], updatedInput["p1"]);
  ASSERT_EQ(inputParams["p2"], updatedInput["p2"]);
  ASSERT_TRUE(updatedInput.has("nonce"));
  nonce = updatedInput["nonce"].get<string>();
  ASSERT_EQ(inputNonce, nonce);
  ASSERT_TRUE(nonce.size() > 0);
  ASSERT_TRUE(nonce.size() <= 128);
}

/////////////////
// Idempotency //
/////////////////

// Assert that a call to an api method returns the same response with a given input, an Idempotent Operation.
void assertEqualResponse(JSON (*apiMethod)(const dx::JSON &, const bool), const JSON &inputParams, const bool safeToRetry=false) {
  JSON response1 =  (*apiMethod)(inputParams, safeToRetry);
  JSON response2 =  (*apiMethod)(inputParams, safeToRetry);
  ASSERT_EQ(response1, response2);
}

void assertEqualResponse(JSON (*apiMethod)(const string &, const dx::JSON &, const bool),
                         const string& objectId, const JSON &inputParams, const bool safeToRetry=false) {
  JSON response1 =  (*apiMethod)(objectId, inputParams, safeToRetry);
  JSON response2 =  (*apiMethod)(objectId, inputParams, safeToRetry);
  ASSERT_EQ(response1, response2);
}

static const std::string apiNonceError =
  "InvalidInput: 'Nonce was reused for an earlier API request that had a different input', Server returned HTTP code '422'";

// Reusing a nonce with a different input should fail.
void assertNonceReuseError(JSON (*apiMethod)(const dx::JSON &, const bool), const JSON &inputParams, const bool safeToRetry=false){
  try {
    (*apiMethod)(inputParams, safeToRetry);
    ASSERT_THROW((*apiMethod)(inputParams, safeToRetry), DXAPIError);
  } catch(DXAPIError &err) {
    ASSERT_EQ(err.resp_code, 422);
    ASSERT_EQ(std::string(err.what()), apiNonceError);
  }
}

void assertNonceReuseError(JSON (*apiMethod)(const string &, const dx::JSON &, const bool),
                           const string& objectId, const JSON &inputParams, const bool safeToRetry=false) {
  try {
    (*apiMethod)(objectId, inputParams, safeToRetry);
    ASSERT_THROW((*apiMethod)(objectId, inputParams, safeToRetry), DXAPIError);
  } catch(DXAPIError &err) {
    ASSERT_EQ(err.resp_code, 422);
    ASSERT_EQ(std::string(err.what()), apiNonceError);
  }
}

TEST(Idempotency, fileNew) {
  JSON inputParams(JSON_OBJECT);
  inputParams["project"] = config::CURRENT_PROJECT();
  inputParams["name"] = "testfile.txt";
  inputParams["nonce"] = Nonce::nonce();
  assertEqualResponse(fileNew, inputParams);

  inputParams["name"] = "testfile2.txt";
  assertNonceReuseError(fileNew, inputParams);
}

TEST(Idempotency, recordNew) {
  JSON inputParams(JSON_OBJECT);
  inputParams["project"] = config::CURRENT_PROJECT();
  inputParams["name"] = "test_record";
  inputParams["nonce"] = Nonce::nonce();
  assertEqualResponse(recordNew, inputParams);

  inputParams["name"] = "test_record_2";
  assertNonceReuseError(recordNew, inputParams);
}

TEST(Idempotency, appletNew) {
  JSON inputParams(JSON_OBJECT);
  inputParams["name"] = "test_applet";
  inputParams["inputSpec"] = JSON(JSON_ARRAY);
  inputParams["inputSpec"].push_back(JSON::parse("{\"name\": \"rowFetchChunk\", \"class\": \"int\"}"));
  inputParams["runSpec"] = JSON(JSON_OBJECT);
  inputParams["outputSpec"] = JSON::parse("[{\"name\":\"message\", \"class\":\"string\"}]");
  inputParams["runSpec"]["code"] = "#!/bin/bash\n\n#main() {\necho '{\"message\": \"hello world!\"}' > job_output.json \n#}";
  inputParams["runSpec"]["interpreter"] = "bash";
  inputParams["runSpec"]["release"] = "14.04";
  inputParams["runSpec"]["distribution"] = "Ubuntu";
  inputParams["dxapi"] = "1.0.0";
  inputParams["project"] = config::CURRENT_PROJECT();
  inputParams["nonce"] = Nonce::nonce();
  assertEqualResponse(appletNew, inputParams);

  inputParams["name"] = "test_applet2";
  assertNonceReuseError(appletNew, inputParams);
}

TEST(Idempotency, appletRun) {
  DXApplet apl;
  createANewApplet(apl);
  JSON inputParams(JSON_OBJECT);
  inputParams["input"] = JSON::parse("{\"rowFetchChunk\": 100}");
  inputParams["project"] = config::CURRENT_PROJECT();
  inputParams["nonce"] = Nonce::nonce();
  assertEqualResponse(appletRun, apl.getID(), inputParams);

  inputParams["input"] = JSON::parse("{\"rowFetchChunk\": 500}");
  assertNonceReuseError(appletRun, apl.getID(), inputParams);
}

TEST(Idempotency, appCreateAndRun) {
  if (DXTEST_FULL){
    DXApplet apl;
    createANewApplet(apl);
    JSON inputParams(JSON_OBJECT);
    inputParams["applet"] = apl.getID();
    inputParams["version"] = "1";
    inputParams["name"] = "app_name";
    inputParams["nonce"] = Nonce::nonce();
    assertEqualResponse(appNew, inputParams);

    inputParams["name"] = "new_app_name";
    assertNonceReuseError(appNew, inputParams);
  } else {
    cerr << "Skipping appCreateAndRun test because DXTEST_FULL was not set" << endl;
  }
}

TEST(Idempotency, workflowNew) {
  JSON inputParams(JSON_OBJECT);
  inputParams["name"] = "WorkflowTest";
  inputParams["project"] = config::CURRENT_PROJECT();
  inputParams["nonce"] = Nonce::nonce();
  assertEqualResponse(workflowNew, inputParams);

  inputParams["name"] = "New_workflow";
  assertNonceReuseError(workflowNew, inputParams);
}

/////////////////
// Retry logic //
/////////////////

TEST(DXHTTPRequestTest, retryLogicWithRetryAfter) {
  // Do this weird dance here in case there is clock skew between client and
  // server
  int64_t localStartTime = std::time(NULL) * 1000;
  JSON response = DXHTTPRequest(std::string("/system/comeBackLater"), std::string("{}"));
  long currentTime = response["currentTime"].get<long>();
  long waitUntil = currentTime + 8000;
  DXHTTPRequest(std::string("/system/comeBackLater"),
		std::string("{\"waitUntil\": ") + boost::lexical_cast<string>(waitUntil) + std::string("}"));
  long localTimeElapsed = std::time(NULL) * 1000 - localStartTime;
  cerr << "Local time elapsed: " << localTimeElapsed;
  ASSERT_GE(localTimeElapsed, 8000);
  ASSERT_LE(localTimeElapsed, 16000);
}

////////////
// DXLink //
////////////

TEST(DXLinkTest, CreationTest) {
  string record_id = "record-0000000000000000000000pb";
  string proj_id = "project-0000000000000000000000pb";
  JSON link = DXLink(record_id);
  EXPECT_EQ(record_id, link["$dnanexus_link"].get<string>());
  EXPECT_EQ(1, link.size());

  link = DXLink(record_id, proj_id);
  EXPECT_EQ(record_id, link["$dnanexus_link"]["id"].get<string>());
  EXPECT_EQ(proj_id, link["$dnanexus_link"]["project"].get<string>());
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

  DXProject third_proj(third_proj_id);

  dxproject.cloneFolder("/a/b/c/d", third_proj_id, "/");
  expected = JSON(JSON_ARRAY);
  expected.push_back("/d");
  ASSERT_EQ(third_proj.listFolder()["folders"], expected);
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
  ASSERT_EQ(desc["folder"].get<string>(), "/a");
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

  // Check describe call with "details": true
  ASSERT_EQ(DXRecordTest::example_JSON, first_record.describe(false, true)["details"]);

  DXRecord second_record(firstID);
  ASSERT_EQ(first_record.getID(), second_record.getID());
  ASSERT_EQ(first_record.getDetails(), second_record.getDetails());
  ASSERT_EQ(second_record.getProjectID(), proj_id);

  options["project"] = second_proj_id;
  second_record.create(options);
  ASSERT_NE(first_record.getID(), second_record.getID());
  ASSERT_EQ(second_record.getProjectID(), second_proj_id);
  ASSERT_EQ(first_record.getDetails(), second_record.getDetails());
  ASSERT_EQ(first_record.describe(false, true)["details"], second_record.describe(false, true)["details"]);

  ASSERT_NO_THROW(first_record.describe());

  first_record.remove();
  ASSERT_THROW(first_record.describe(), DXAPIError);
  second_record.remove();
  ASSERT_THROW(second_record.describe(), DXAPIError);

  DXRecord third_record(firstID);
  ASSERT_THROW(third_record.describe(), DXAPIError);
}

TEST_F(DXRecordTest, InitializeFromTest) {
  DXRecord dxrecord = DXRecord::newDXRecord();
  DXRecord second_record = DXRecord::newDXRecord(dxrecord);
  JSON desc = second_record.describe();
  ASSERT_EQ(desc["name"], dxrecord.getID());
}

TEST_F(DXRecordTest, DescribeTest) {
  DXRecord dxrecord = DXRecord::newDXRecord();
  JSON desc = dxrecord.describe();
  ASSERT_EQ(desc["project"], proj_id);
  ASSERT_EQ(desc["id"], dxrecord.getID());
  ASSERT_EQ(desc["class"].get<string>(), "record");
  ASSERT_EQ(desc["types"], JSON(JSON_ARRAY));
  ASSERT_EQ(desc["state"].get<string>(), "open");
  ASSERT_FALSE(desc["hidden"].get<bool>());
  ASSERT_EQ(desc["links"], JSON(JSON_ARRAY));
  ASSERT_EQ(desc["name"], dxrecord.getID());
  ASSERT_EQ(desc["folder"].get<string>(), "/");
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
  desc = second_dxrecord.describe(true, true);
  ASSERT_EQ(desc["project"], proj_id);
  ASSERT_EQ(desc["id"].get<string>(), second_dxrecord.getID());
  ASSERT_EQ(desc["class"].get<string>(), "record");
  ASSERT_EQ(desc["types"], types);
  ASSERT_EQ(desc["state"].get<string>(), "open");
  ASSERT_TRUE(desc["hidden"].get<bool>());
  ASSERT_EQ(desc["links"], links_to_expect);
  ASSERT_EQ(desc["name"].get<string>(), "Name");
  ASSERT_EQ(desc["folder"].get<string>(), "/a");
  ASSERT_EQ(desc["tags"], tags);
  ASSERT_TRUE(desc.has("created"));
  ASSERT_TRUE(desc.has("modified"));
  ASSERT_EQ(desc["properties"], properties);
  ASSERT_EQ(desc["properties"], second_dxrecord.getProperties());
  ASSERT_EQ(desc["details"], details);
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

TEST_F(DXRecordTest, SetAndGetPropertiesTest) {
  DXRecord dxrecord = DXRecord::newDXRecord();
  JSON properties(JSON_OBJECT);
  properties["project"] = "cancer project";
  properties["foo"] = "bar";
  dxrecord.setProperties(properties);
  JSON desc = dxrecord.describe(true);
  ASSERT_EQ(desc["properties"], properties);
  ASSERT_EQ(dxrecord.getProperties(), properties);

  JSON unset_property(JSON_OBJECT);
  unset_property["project"] = JSON(JSON_NULL);
  dxrecord.setProperties(unset_property);
  properties.erase("project");
  ASSERT_EQ(dxrecord.getProperties(), properties);
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

TEST(ConstructFromDXLink_Tests, setIDAndConstructor) {
  JSON options(JSON_OBJECT);
  options["name"] = "firstname";
  options["tags"] = JSON(JSON_ARRAY);
  options["tags"].push_back("tag");

  DXRecord dxr = DXRecord::newDXRecord(options);
  DXRecord dxr2(JSON::parse("{\"$dnanexus_link\": \"" + dxr.getID() + + "\"}"));
  ASSERT_EQ(dxr2.getID(), dxr.getID());
  DXRecord dxr3;
  ASSERT_NE(dxr3.getID(), dxr.getID());
  JSON dxlink = JSON::parse("{\"$dnanexus_link\": {\"project\": \"" + proj_id + "\", \"id\": \"" + dxr.getID() + "\"}}");
  dxr3.setIDs(dxlink);
  ASSERT_EQ(dxr3.getID(), dxr.getID());

  JSON invalid_dxlink = JSON::parse("{\"$dnanexus_link\": 12122}");
  ASSERT_THROW(dxr3.setIDs(invalid_dxlink), DXError);
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
    ASSERT_STRNE(NULL, tmpnam(name));
    tempfilename = string(name);

    if (foofilename == "") {
      char fooname [L_tmpnam];
      ASSERT_STRNE(NULL, tmpnam(fooname));
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

TEST_F(DXFileTest, CheckCopyConstructorAndAssignmentOperator) {
  std::vector<DXFile> fv;
  DXFile dxf = DXFile::newDXFile();
  ASSERT_EQ(104857600, dxf.getMaxBufferSize());
  ASSERT_EQ(5, dxf.getNumWriteThreads());

  dxf.setMaxBufferSize(5*1024*1024);
  dxf.setNumWriteThreads(10);
  ASSERT_EQ(dxf.getMaxBufferSize(), 5*1024*1024);
  ASSERT_EQ(dxf.getNumWriteThreads(), 10);

  // Assignment operator test
  DXFile dxcpy = dxf;
  ASSERT_EQ(dxf.getMaxBufferSize(), 5*1024*1024);
  ASSERT_EQ(dxf.getNumWriteThreads(), 10);
  ASSERT_EQ(dxcpy.getMaxBufferSize(), 5*1024*1024);
  ASSERT_EQ(dxcpy.getNumWriteThreads(), 10);
  ASSERT_EQ(dxcpy.getID(), dxf.getID());
  ASSERT_EQ(dxcpy.getProjectID(), dxf.getProjectID());

  // Copy constructor
  fv.push_back(dxf);
  ASSERT_EQ(dxf.getMaxBufferSize(), 5*1024*1024);
  ASSERT_EQ(dxf.getNumWriteThreads(), 10);
  ASSERT_EQ(fv[0].getMaxBufferSize(), dxf.getMaxBufferSize());
  ASSERT_EQ(fv[0].getNumWriteThreads(), dxf.getNumWriteThreads());
  ASSERT_EQ(fv[0].getID(), dxf.getID());
  ASSERT_EQ(fv[0].getProjectID(), dxf.getProjectID());

  // Check that exception is thrown if we try to set buffer size < 5 MB
  ASSERT_THROW(dxf.setMaxBufferSize(5*1024*1024 - 1), DXFileError);
}

TEST_F(DXFileTest, UploadPartMultipleTime) {
  DXFile dxf = DXFile::newDXFile();
  // If we do not specify a part id, it should be overwritten each time
  string s = "blah";
  dxf.uploadPart(s);
  dxf.uploadPart(s);
  dxf.uploadPart(s);
  dxf.close(true);
  ASSERT_EQ(dxf.describe()["size"], 4);

  dxf = DXFile::newDXFile();
  // since each part (except last) must be at least 5 mb
  int sizeFirstPart = 5242880 + 1;
  dxf.uploadPart(string(sizeFirstPart, 'x'), 1);
  dxf.uploadPart(string("foo"), 1000);
  dxf.close(true);
  char data[9] = {0};
  dxf.read(data, 8);
  ASSERT_EQ(string(data), string(8, 'x'));
  ASSERT_EQ(dxf.describe()["size"], sizeFirstPart + 3);
}

TEST_F(DXFileTest, UploadEmptyFile) {
  DXFile dxf = DXFile::newDXFile();
  dxf.close();
  ASSERT_EQ(dxf.describe()["size"], 0);

  char fname[L_tmpnam];
  ASSERT_STRNE(NULL, tmpnam(fname));
  ofstream lf(fname);
  lf.close();
  DXFile dxf2 = DXFile::uploadLocalFile(fname);
  ASSERT_EQ(dxf2.describe()["size"], 0);
}

TEST(DXFileTest_Async, UploadAndDownloadLargeFile_1_SLOW) {
  // Upload a file with "file_size" number of '$' in it
  // and download it, check that it is same.

  char fname[L_tmpnam];
  const int file_size = 25 * 1024 * 1024;
  ASSERT_STRNE(NULL, tmpnam(fname));
  ofstream lf(fname);
  for (int i = 0; i < file_size; ++i)
    lf<<"$";
  lf.close();
  DXFile dxf = DXFile::uploadLocalFile(fname);
  dxf.waitOnClose();

  char fname2[L_tmpnam];
  ASSERT_STRNE(NULL, tmpnam(fname2));
  DXFile::downloadDXFile(dxf.getID(), fname2, 99999);


  // Read the local file contents in a string
  string df_content;
  ifstream fp(fname);
  ASSERT_TRUE(fp.is_open());
  // Reserve memory for string upfront (to avoid having reallocation multiple time)
  fp.seekg(0, ios::end);
  ASSERT_EQ(file_size, fp.tellg());
  fp.seekg(0, ios::beg);
  int count = 0;
  while (fp.good()) {
    char ch = fp.get();
    if (fp.eof())
      break;
    count++;
    ASSERT_EQ('$', ch);
  }
  fp.close();


  ASSERT_EQ(count, file_size);
  remove(fname);
  remove(fname2);

  dxf.flush();
  dxf.remove();
}

TEST(DXFileTest_Async, UploadAndDownloadLargeFile_2_SLOW) {
  const int64_t file_size = 25.211 * 1024 * 1024;

  DXFile dxfile = DXFile::newDXFile();
  dxfile.setNumWriteThreads(1000);
  dxfile.setMaxBufferSize(5 * 1024 * 1024);
  int64_t chunkSize = 5*1024*1024; // minimum chunk size allowed by api
  for (int64_t i = 0; i < file_size; i += chunkSize) {
    string toWrite = string(std::min(chunkSize, (file_size - i)), '#');
    dxfile.write(toWrite);
    if (random() % 2 == 0) {
      // Randomly flush sometime
      dxfile.flush();
    }
  }
  dxfile.close(true);
  ASSERT_EQ(dxfile.is_closed(), true);

  std::string chunk;
  EXPECT_EQ(dxfile.getNextChunk(chunk), false);
  dxfile.startLinearQuery();
  int64_t bytes_read = 0;
  while (dxfile.getNextChunk(chunk)) {
    for (int i = 0; i < chunk.size(); ++i)
      ASSERT_EQ(chunk[i], '#');
    bytes_read += chunk.size();
    if (random() % 10 == 0) {
      // ~1 in 10 time, stop the linear query and restart from current position
      dxfile.stopLinearQuery();
      dxfile.startLinearQuery(bytes_read);
    }
  }
  ASSERT_EQ(dxfile.getNextChunk(chunk), false);
  ASSERT_EQ(bytes_read, file_size);
  dxfile.remove();
}

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
  	    dxfile.describe(true)["name"].get<string>());

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

TEST(DXSystemTest, findDataObjects) {
  // We skip running of findDataObjects test in automated test suits (jenkins)
  // because these tests rely heavily on server & client clock being in total sync
  // (so they are good for running locally only)
  if (DXTEST_FULL) {
    // Note: Due to clock differences on various machine, some of these test might fail.
    //       Be aware of this fact while debugging.
    usleep(1 * 1000000); // Sleep for 1s
    int64_t ts1 = std::time(NULL) * 1000; // in ms => Time of object creation
    usleep(10000); // Sleep for 10ms
    DXRecord dxrecord = DXRecord::newDXRecord();
    JSON q1(JSON_OBJECT);
    q1["created"] = JSON::parse("{\"after\": " + boost::lexical_cast<std::string>(ts1) + "}");
    JSON res = DXSystem::findDataObjects(q1);
  //  std::cout<<endl<<res.toString()<<endl;
    ASSERT_EQ(res["results"].size(), 1);
    ASSERT_EQ(res["next"], JSON(JSON_NULL));

    ASSERT_EQ(res["results"][0], DXSystem::findOneDataObject(q1));

    // Sleep for .5 sec, and then find all objects modified in last .25 second
    // should be zero
    usleep(2 * 1000000); // Sleep for 2sec
    q1 = JSON::parse("{\"modified\": {\"after\": \"-0.25s\"}}");
    res = DXSystem::findDataObjects(q1);
    ASSERT_EQ(res["results"].size(), 0);
    ASSERT_EQ(res["next"], JSON(JSON_NULL));

    // find all objects modified after (ts1 - 1) seconds
    q1["modified"]["after"] = boost::lexical_cast<std::string>(ts1/1000 - 1) + "s";
    res = DXSystem::findDataObjects(q1);
    ASSERT_EQ(res["results"].size(), 1);
    ASSERT_EQ(res["next"], JSON(JSON_NULL));

    // find all objects in open state, and created after (ts1 - 1) seconds
    q1 = JSON::parse("{\"state\": \"open\", \"created\":{\"after\":-" + boost::lexical_cast<std::string>(std::time(NULL)*1000 - ts1 + 1000) + "}}");
    res = DXSystem::findDataObjects(q1);
    ASSERT_EQ(res["results"].size(), 1);

    // Remove test data
    dxrecord.remove();
  } else {
    cerr << "Skipping findDataObjects test because DXTEST_FULL was not set" << endl;
  }
}

TEST(DXSystemTest, findJobs) {
  int64_t ts = std::time(NULL);
  DXApplet apl;
  createANewApplet(apl);
  DXJob job = apl.run(JSON::parse("{\"rowFetchChunk\": 100}"));

  JSON query = JSON::parse("{\"project\": \"" + apl.getProjectID() + "\"}");

  JSON res = DXSystem::findJobs(query);
  ASSERT_TRUE(res["results"].size() > 0);
  ASSERT_EQ(res["results"][0]["id"].get<string>(), job.getID());

  query["created"] = JSON::parse("{\"after\": " + boost::lexical_cast<string>(ts * 1000 - 5 * 60 * 1000) + "}"); // assuming clocks can be 5min out of sync
  JSON res2 = DXSystem::findJobs(query);
  ASSERT_TRUE(res2["results"].size() >= 1);

  query["created"]["after"] = std::time(NULL) * 1000 + 5 * 60 * 1000;
  JSON res3 = DXSystem::findJobs(query);
  ASSERT_EQ(res3["results"].size(), 0);

  apl.remove();
  job.terminate();
}

TEST(DXSystemTest, findProjects) {
  JSON q = JSON::parse("{\"name\": \"dxcpp_test_prj\"}");
  JSON res = DXSystem::findProjects(q);
  int len = res["results"].size();

  std::string id = projectNew(std::string("{\"name\": \"dxcpp_test_prj\"}"))["id"].get<std::string>();

  ASSERT_EQ(DXSystem::findProjects(q)["results"].size(), (len == 1000) ? len : len + 1);
  DXProject dxprj(id);
  dxprj.destroy();
}

TEST(DXSystemTest, findApps) {
  if (DXTEST_FULL) {
    int64_t ts = std::time(NULL);
    DXApplet apl;
    createANewApplet(apl);
    JSON inp(JSON_OBJECT);
    inp["applet"] = apl.getID();
    inp["version"] = "1";
    inp["name"] = apl.getID() + "blah";
    string appid = appNew(inp)["id"].get<string>();
    DXApp app(appid);

    JSON res = DXSystem::findApps(JSON::parse("{\"created\": {\"after\": " + boost::lexical_cast<string>(ts*1000 - 1) + "}, \"describe\": true}"));
    ASSERT_EQ(res["results"].size(), 1);
    ASSERT_EQ(res["results"][0]["describe"]["name"].get<string>(), apl.getID() + "blah");

    JSON res2 = DXSystem::findApps(JSON::parse("{\"modified\": {\"after\": " + boost::lexical_cast<string>(ts*1000 - 1) + "}, \"describe\": true}"));
    ASSERT_EQ(res2["results"].size(), 1);
    ASSERT_EQ(res2["results"][0]["describe"]["name"].get<string>(), apl.getID() + "blah");
    ASSERT_EQ(res["results"][0]["id"], res2["results"][0]["id"]);

    JSON res3 = DXSystem::findApps(JSON::parse("{\"created\": {\"after\": " + boost::lexical_cast<string>(ts*1000 - 1) + "},\"modified\": {\"after\": " + boost::lexical_cast<string>(ts*1000 - 1) + "}}"));
    ASSERT_EQ(res3["results"].size(), 1);
    ASSERT_EQ(res["results"][0]["id"], res2["results"][0]["id"]);

    apl.remove();
  //  app.remove();
  } else {
    cerr << "Skipping findApps test, as DXTEST_FULL is not set" << endl;
  }
}

TEST(DXAppletTest, AllAppletTests) {
  DXApplet apl;
  createANewApplet(apl);
  ASSERT_EQ(apl.get()["inputSpec"], applet_details["inputSpec"]);
  ASSERT_EQ(apl.describe()["name"].get<string>(), "test_applet");

  // Run the applet
  DXJob job = apl.run(JSON::parse("{\"rowFetchChunk\": 100}"), "/");
  ASSERT_EQ(job.describe()["applet"].get<string>(), apl.getID());
  job.terminate();

  // Clone the applet
  DXApplet apl2 = apl.clone(second_proj_id);
  apl.remove();
  ASSERT_EQ(apl2.get()["inputSpec"], applet_details["inputSpec"]);
  apl2.remove();
}

// AllJobTests are slow because they wait for full execution
// of an applet
// For this reason they need executionserver and jobserver running as well
TEST(DXJobTest, AllJobTests_SLOW) {
  DXApplet apl;
  createANewApplet(apl);
  DXJob job = apl.run(JSON::parse("{\"rowFetchChunk\": 100}"));
  ASSERT_EQ(job.describe()["applet"].get<string>(), apl.getID());

  // Check state after 2 minutes
  job.waitOnDone(120);
  string j1_state = job.getState();
  if (j1_state == "failed" || j1_state == "terminated") {
    ASSERT_TRUE(false); // Job has failed - unexpected
  }

  // Check state again after 2 minutes
  job.waitOnDone(120);
  j1_state = job.getState();
  if (j1_state == "failed" || j1_state == "terminated") {
    ASSERT_TRUE(false); // Job has failed - unexpected
  }

  // otherwise give it 6 more minutes to finish
  job.waitOnDone(360);

  // If state is not "done" after even 10min, that means job most probably failed:
  // should not happen
  ASSERT_EQ(job.getState(), "done");

  vector<string> depends;
  depends.push_back(job.getID());
  DXJob job2 = apl.run(JSON::parse("{\"rowFetchChunk\": 100}"), "/", depends, "mem2_hdd2_x1");
  job2.waitOnDone(180);
  string j2_state = job2.getState();
  ASSERT_NE(j2_state, "failed");
  ASSERT_NE(j2_state, "terminated");
  job2.terminate();
  apl.remove();
}

TEST(DXAppTest, AllAppTests) {
  if (DXTEST_FULL) {
    DXApplet apl;
    createANewApplet(apl);
    JSON inp(JSON_OBJECT);
    inp["applet"] = apl.getID();
    inp["version"] = "1";
    inp["name"] = apl.getID() + "blah";
    string appid = appNew(inp)["id"].get<string>();
    DXApp app(appid);

    ASSERT_EQ(app.get()["inputSpec"], applet_details["inputSpec"]);

  //  app.update(JSON::parse("{\"name\": \"\"}"));
    ASSERT_EQ(app.describe()["name"].get<string>(), apl.getID() + "blah");
    ASSERT_EQ(app.describe()["installed"].get<bool>(), true);

    // Test addTags() and removeTags()
    app.addTags(JSON::parse("[\"blah-1\", \"blah-2\"]"));
    JSON desc = app.describe();
    int countTags = 0;
    for (int i = 0; i < desc["aliases"].size(); ++i) {
      if (desc["aliases"][i].get<string>() == "blah-1" || desc["aliases"][i].get<string>() == "blah-2")
        countTags++;
    }
    ASSERT_EQ(countTags, 2);

    app.removeTags(JSON::parse("[\"blah-1\", \"blah-2\"]"));
    countTags = 0;
    desc = app.describe();
    for (int i = 0; i < desc["aliases"].size(); ++i) {
      if (desc["aliases"][i].get<string>() == "blah-1" || desc["aliases"][i].get<string>() == "blah-2")
        countTags++;
    }
    ASSERT_EQ(countTags, 0);

    // Test addCategories() and removeCategories()
    app.addCategories(JSON::parse("[\"blah-1\", \"blah-2\"]"));
    desc = app.describe();
    int countCategories = 0;
    for (int i = 0; i < desc["categories"].size(); ++i) {
      if (desc["categories"][i].get<string>() == "blah-1" || desc["categories"][i].get<string>() == "blah-2")
        countCategories++;
    }
    ASSERT_EQ(countCategories, 2);

    app.removeCategories(JSON::parse("[\"blah-1\", \"blah-2\"]"));
    desc = app.describe();
    countCategories = 0;
    for (int i = 0; i < desc["categories"].size(); ++i) {
      if (desc["categories"][i].get<string>() == "blah-1" || desc["categories"][i].get<string>() == "blah-2")
        countCategories++;
    }
    ASSERT_EQ(countCategories, 0);

    //Test Install and uninstall
    // TODO: We need to create another user to test uninstalltion
    //       since a developer cannot uninstall the app

    // Test publish()
    ASSERT_EQ(app.describe().has("published"), false);
    app.publish();
    ASSERT_EQ(app.describe().has("published"), true);

    // Remove the app
    // app.remove();
    //ASSERT_EQ(app.describe()["deleted"].get<bool>(), true);

    //apl.remove();
  } else {
    cerr << "Skipping DXAppTest test, as DXTEST_FULL is not set" << endl;
  }
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
  {
    DXTEST_FULL = false;
    char *tmp = getenv("DXTEST_FULL");
    if (tmp != NULL) {
      if (string(tmp) != "0" && string(tmp) != "false") {
        cerr << "DXTEST_FULL env variable is set. Will run all tests (including tests which create apps)" << endl;
        DXTEST_FULL = true; // set the global variable
      }
    }
  }
  JSON project_hash(JSON_OBJECT);
  project_hash["name"] = "test_project_dxcpp";
  JSON resp = projectNew(project_hash);
  proj_id = resp["id"].get<string>();
  project_hash["name"] = "second_test_project_dxcpp";
  resp = projectNew(project_hash);
  second_proj_id = resp["id"].get<string>();

  resp = projectNew(project_hash);
  project_hash["name"] = "third_test_project_dxcpp";
  third_proj_id = resp["id"].get<string>();

  config::CURRENT_PROJECT() = proj_id;

  int result = RUN_ALL_TESTS();
  remove(foofilename.c_str());
  projectDestroy(proj_id, string("{\"terminateJobs\": true}"));
  projectDestroy(second_proj_id, string("{\"terminateJobs\": true}"));
  projectDestroy(third_proj_id, string("{\"terminateJobs\": true}"));

  return result;
}
