#!/usr/bin/env v8cgi

// Copyright (C) 2013 DNAnexus, Inc.
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

// This is a sample tests file for JS client-side bindings,
// The initial code (above main() is taken from template for seed_apps used by EM)
require.paths.push(".");

job = {};
job.input = {"fastq": "792064"};
job.output = {};
job.security_context = {auth_token_type: "Bearer", auth_token: "abcdef"};

var dn = require('DNAnexus.js').DNAnexus;
var assert = require("assert");

function main() {
  // NOTE:
  // 1. Order of tests is important in this function, because later tests
  //    assume succesful completion of previous tests
  //    For example: listTables() will assume, crateTable() have been called
  //    and might assert on size of the tables rerturned, etc.
  //
  // 2. These tests are not meant to be exhaustive, and usually test
  //    one or two aspects of return values only. They are more of sanity checks.

  var rv = {}; // Will keep track of values returned by library functions

  // createUser() test
  var name = "test-user-" + Math.floor(Math.random() * Math.pow(2, 32)).toString(16);
  rv["createUser-1"] =  dn.createUser("hello", name);
  assert.notEqual(rv["createUser-1"], null, "No ID field present in createUser() return value");

  // createEmptyObject() tests
  rv["createEmptyObject-1"] = dn.createEmptyObject();
  assert.notEqual(rv["createEmptyObject-1"], null, "No ID field present in createEmptyObject() return value");

  rv["createEmptyObject-2"] = dn.createEmptyObject({blah: "foo"}, null);
  assert.notEqual(rv["createEmptyObject-2"], null, "No ID field present in createEmptyObject() return value");

  // getObjectPropertiesAndRelationships() tests
  rv["getObjectPropertiesAndRelationships-1"] = dn.getObjectPropertiesAndRelationships(rv["createEmptyObject-2"]);
  assert.notEqual(rv["getObjectPropertiesAndRelationships-1"].properties && rv["getObjectPropertiesAndRelationships-1"].relationships, false, "properties and relationships field must be present in getObjectPropertiesAndRelationships() output");
  assert.equal(rv["getObjectPropertiesAndRelationships-1"]["properties"].blah, "foo", "The property blah was set to \"foo\". Must be returned back same");

  rv["createEmptyObject-3"] = dn.createEmptyObject(null, null);
  assert.notEqual(rv["createEmptyObject-3"], null, "No ID field present in createEmptyObject() return value");

  // getObjectProperties() and updateObjectProperties() tests
  dn.updateObjectProperties(rv["createEmptyObject-1"], {xyz: "blah", xyz2: "blah2"});
  rv["getObjectProperties-1"] = dn.getObjectProperties(rv["createEmptyObject-1"]);
  assert.ok(rv["getObjectProperties-1"].xyz == "blah" && rv["getObjectProperties-1"].xyz2 == "blah2", "Properties returned back do not match the properties set");
// assert.ok(false, "HEREEEE");
  // listObjects() test
   // dn.setObjectProperty(rv["createEmptyObject-1"], "blah", "foooooo");
  rv["listObjects-1"] = dn.listObjects();
  assert.ok(rv["listObjects-1"].length >= 3, "Number of objects returned by listObjects() cannot be less than 3 at this point of code (after 3 createEmptyObject() succesful tests)");
  assert.notEqual(rv["listObjects-1"][0].id, null, "Field ID should be present with all objects in the output of listObjects()");

  // createTable() test - 1
  rv["createTable-1"] = dn.createTable(["hahaha:int32"]);
  assert.notEqual(rv["createTable-1"].id, null, "No ID field returned by createTable()-1");

  rv["createTable-forAppendRows"] = dn.createTable(["hahaha:int32"]);
  assert.notEqual(rv["createTable-1"].id, null, "No ID field returned by createTable()-1");

  // appendRowsToTable() test
  rv["appendRowsToTable-1"] = dn.appendRowsToTable(rv["createTable-forAppendRows"].id, {"data": [[2], [3], [5]]});
  assert.equal(rv["appendRowsToTable-1"].id, rv["createTable-forAppendRows"].id, "appendRowsToTable() should have returned back same table ID");

  // closeTable() & getRowsFromTable() & getTableInfo() tests
  rv["getTableInfo-1"] = dn.getTableInfo(rv["createTable-1"].id);
  assert.equal(rv["getTableInfo-1"].status, "OPEN", "Table created using columnDescriptors must be open");
  dn.closeTable(rv["createTable-1"].id);
  dn.closeTable(rv["createTable-forAppendRows"].id);

  rv["getRowsFromTable-1"] = dn.getRowsFromTable(rv["createTable-forAppendRows"].id, {limit: 500});
  assert.ok(rv["getRowsFromTable-1"].limit == 500, "Limit should be same as sent : getRowsFromTable()");
  assert.ok(rv["getRowsFromTable-1"].rows.length == 3, "We appended exactly 3 rows. Should get back same number: getRowsFromTable()");

  rv["getRowsFromTable-2"] = dn.getRowsFromTable(rv["createTable-forAppendRows"].id);
  assert.ok(rv["getRowsFromTable-2"].limit == 1000, "Default limit should be returned as 1000 in getRowsFromTable()");
  assert.ok(rv["getRowsFromTable-2"].rows.length == 3, "We appended exactly 3 rows. Should get back same number: getRowsFromTable()");



  rv["getTableInfo-2"] = dn.getTableInfo(rv["createTable-1"].id);
  assert.equal(rv["getTableInfo-2"].status, "CLOSED", "closeTable() must have closed this table");

  // createTable() test - 2
  rv["createTable-2"] = dn.createTable(["chr:int32", "start:double", "stop:double"]);
  dn.closeTable(rv["createTable-2"].id);

  rv["createTable-3"] = dn.createTable(["/tables/" + rv["createTable-1"].id, "/tables/" + rv["createTable-2"].id]);
  assert.notEqual(rv["createTable-3"].id, null, "No ID field returned by createTable()-2");
  assert.ok(rv["createTable-3"].columns.length == 4, "Columns must be concatenated in createTable-3 call");

  // listTable() tests
  rv["listTables-1"] = dn.listTables();
  assert.ok(rv["listTables-1"].length >= 2, "Number of tables returned by listTables() cannot be less than 2 at this point of code (after 2 successful createTable() tests");
  assert.notEqual(rv["listTables-1"][0].id, null, "ID should be present in listTables() output");


  // deleteTables() tests
  dn.deleteTable(rv["createTable-1"].id);
  var errorCatched = false;
  try {
    var iShouldFail = dn.getTableInfo(rv["createTable-1"].id);
    // Code should not reach this point
    assert.ok(false, "Table was deleted, exception should have been thrown while getTableInfo()");
  } catch (err) {
    // this error should have been thown
    errorCatched = true;
  }
  assert.ok(errorCatched, "Error should have been thrown while trying to get info on a deleted table");

  // Cords_table tests
  rv["createCoordsTable-1"] = dn.createCoordsTable("/tables/" + rv["createTable-2"].id);
  assert.notEqual(rv["createCoordsTable-1"].id, null, "ID field should be present in createCoordsTable() output");
  rv["listCoordsTables-1"] = dn.listCoordsTable();
  assert.ok(rv["listCoordsTables-1"].length >= 1, "At least one coords_table must be present in listCoordsTables() output");

  rv["createTable-forCT"] = dn.createTable(["chr:int32", "start:double", "stop:double", "blah:string"]);
  assert.notEqual(rv["createTable-forCT"].id, null, "No ID field returned by createTable() - for CT");

  rv["appendRowsToTable-forCT"] = dn.appendRowsToTable(rv["createTable-forCT"].id, {"data": [[1, 2, 200, "row1-blah"], [2, 3, 2000, "row2-blah"]]});
  assert.equal(rv["appendRowsToTable-forCT"].id, rv["createTable-forCT"].id, "appendRowsToTable() should have returned back same table ID - for Coords table");
  dn.closeTable(rv["createTable-forCT"].id);

  rv["createCoordsTable-2"] = dn.createCoordsTable("/tables/" + rv["createTable-forCT"].id);
  assert.notEqual(rv["createCoordsTable-2"].id, null, "ID field should be present in createCoordsTable() output -2");

  rv["getCoordsTableInfo-1"]  = dn.getCoordsTableInfo(rv["createCoordsTable-2"].id);
  assert.ok(rv["getCoordsTableInfo-1"].numRows == 2, "Just added two rows to this coords table. should get back same number by getCoordsTableInfo()");

  rv["getRowsFromCoordsTable-1"] = dn.getRowsFromCoordsTable(rv["createCoordsTable-2"].id);
  assert.ok(rv["getRowsFromCoordsTable-1"].rows.length == 2, "Added two reows to this table, call to getRowsFromCoordsTable() with default params should return back 2");

  rv["getRowsFromCoordsTable-2"] = dn.getRowsFromCoordsTable(rv["createCoordsTable-2"].id, {chr: 1, start: 0, stop: 100});
  assert.ok(rv["getRowsFromCoordsTable-2"].rows.length == 1, "Only 1 row with chr=1 exist. Should have got it back. getRowsFromCoordsTable()");

  // deleteCoordsTable() tests
  dn.deleteCoordsTable(rv["createCoordsTable-1"].id);
  errorCatched = false;
  try {
    var iShouldFail = dn.getCoordsTableInfo(rv["createCoordsTable-1"].id);
    // Code should not reach this point
    assert.ok(false, "Coords Table was deleted, exception should have been thrown while getCoordsTableInfo()");
  } catch(err) {
    // this error should have been thown
    errorCatched = true;
  }
  assert.ok(errorCatched, "Error should have been thrown while trying to get info on a deleted coords table");

  // app & job tests
  rv["createApp-1"] = dn.createApp({input_spec: {blah: "table"}, output_spec: {foo: "file"}, code: "function main() { system.stdout('Hello World'); }"});
  assert.notEqual(rv["createApp-1"], null, "A valid App Id must be returned by createApp() ");

  rv["runApp-1"] = dn.runApp(rv["createApp-1"], {input: {blah: rv["createTable-2"].id}});
  assert.notEqual(rv["runApp-1"], null, "A valid Job Id must be returned by runApp() ");

  rv["getApp-1"] = dn.getApp(rv["createApp-1"]);
  assert.ok(Object.keys(rv["getApp-1"]).length >= 1, "getApp() - Some fields must be returned by getApp, since app was created by dame user");

  rv["getJob-1"] = dn.getJob(rv["runApp-1"]);
  assert.ok(rv["getJob-1"].appID === rv["createApp-1"], "getJob() - This job was created using this app, should contain same appID in meta data");

  // Tests for system()
  rv["system-1"] = dn.system("ls");
  assert.equal(rv["system-1"].stdout, undefined, "system() - Default value for \"boolReturnOutput\" should be false - so should not return field: stdout or stderr");
  rv["system-2"] = dn.system("ls -a | grep '^\\.$'", {"capture_stdout": true, "capture_stderr": true});
  assert.ok(rv["system-2"].stderr.length == 0, "Nothing should be printed on stderr for this command");
  assert.ok(rv["system-2"].stdout == '.\n', "Output of this command line should be just '.\n'");

  rv["system-3"] = dn.system("ls -a | grep '^\\.$'", {"capture_stdout": true});
  assert.ok(rv["system-3"].stderr === undefined, "stderr should not be defined");
  assert.ok(rv["system-3"].stdout == '.\n', "Output of this command line should be just '.\n'");


  // Test for search
  var uniqueString = (new Date()).getTime().toString() + "--" + Math.random().toString();
  rv["createEmptyObject-forSearch"] = dn.createEmptyObject({"uniqueString": uniqueString});
  rv["search-1"] = dn.search({properties: {"uniqueString": uniqueString}});
  assert.ok(rv["search-1"].length == 1, "search() - should have returned exactly ine object with property : {uniqueString: " + uniqueString + "}");
  rv["search-2"] = dn.search({});
  assert.ok(rv["search-2"].length >= 1, "search() with empty query should have returned at least one object");
  rv["search-3"] = dn.search({type: ["object"]});
  assert.ok(rv["search-3"].length >= 1, "search() for type = 'object' should have returned at least one object");
  var i;
  for (i = 0; i < rv["search-3"].length; ++i) {
    assert.equal(rv["search-3"][i].type, "object", "search() - searched for type = 'Object', should not get back any other type of object");
  }

  // Upload & filetest
  rv["createUpload-1"] = dn.createUpload();
  assert.ok(rv["createUpload-1"].parts.length ===  1, "createUpload() - called with default num_parts value - should return one URL to put data to");

  rv["createUpload-2"] = dn.createUpload(3);
  assert.ok(rv["createUpload-2"].parts.length ===  3, "createUpload() - called with num_parts=3 - should return 3 URLs to put data to");

  rv["listUploads"] = dn.listUploads();
  assert.ok(rv["listUploads"].length >= 2, "listUploads() - just created two uploads, should have got back at least those");
  rv["getUploadInfo-1"] = dn.getUploadInfo(rv["createUpload-2"].id);
  assert.ok(rv["getUploadInfo-1"].length == 3, "getupload() - should have returned info about all 3 parts");

  rv["createFile-1"] = dn.createFile({id: rv["createUpload-1"].id, content_type: 'text/plain'});
  assert.notEqual(rv["createFile-1"], null, "createFile()-1 - A valid file URL must be returned");

  rv["createFile-2"] = dn.createFile({url: "/uploads/" + rv["createUpload-1"].id});
  assert.notEqual(rv["createFile-2"], null, "createFile()-2 - A valid file URL must be returned");

  rv["listFiles-1"] = dn.listFiles();
  assert.ok(rv["listFiles-1"].length >= 2, "listFiles() - created 2 files, should have got them back from listFiles");

  rv["getFileInfo-1"] = dn.getFileInfo(rv["listFiles-1"][0].id);
  assert.equal(rv["getFileInfo-1"].id, rv["listFiles-1"][0].id, "getFileInfo() - Queried with a particular file ID, should have recieved back information about file with the same ID");

  ///// TODO: Add test for runJob()

  system.stdout("\nAll tests completed succesfully ... Yay!! :)\n");
}

main();
//system.stdout(JSON.stringify(job.output));
