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

var DNAnexus = {};
var File = require('fs').File;
var Directory = require('fs').Directory;
var api = require('api_helper').DNAnexusAPI;

// system and messaging
DNAnexus.system = new (require('process')).Process().exec;
DNAnexus.msg = function (info) {
  var tInfo = (typeof info === 'string') ? info : JSON.stringify(info);
  system.stdout(tInfo + "\n");
};

// FILE I/O
DNAnexus.readFile = function (filename) {
  var f = new File(filename), code;
  f.open('r');
  code = f.read();
  f.close();
  return code;
};

DNAnexus.writeFile = function (filename, code) {
  var f = new File(filename);
  f.open('w');
  f.write(code);
  f.close();
};

DNAnexus.readFileWithEncode = function (filename, encode) {
  return DNAnexus.readFile(filename).toString(encode);
};

// utils
function getResJSON(response) {
  return JSON.parse(response.data.toString('utf-8'));
}

function getIDFromLocation(response) {
  var result = response.header('location').match(/\/(\d+)$/);
  if (result == null) { return null; }
  return result[1];
}

DNAnexus.uploadFile = function (filename, content_type, auth) {
//  var data = DNAnexus.readFile(filename);
//  var data2 = data.toString("LATIN1");
  var upload = getResJSON(api.createUpload(1, auth));
  var cmd = "curl -H 'Content-Type: application/octet-stream' -H 'Authorization:" + auth + "' -X PUT --data-binary @" + filename + " http://localhost:8124" + upload.parts[0] + " " + " -o tempFile 2>temp.err";
  DNAnexus.system(cmd + "\n");
//  api.putUpload(upload.parts[0], data2, auth);
  var response = api.createFile(JSON.stringify({id: upload.id, content_type: content_type}), auth);
  return getIDFromLocation(response);
};

DNAnexus.getFile = function (fileID, localFile, auth) {
  var cmd = "curl -H 'Authorization:" + auth + "' http://localhost:8124/files/" + fileID + " -o " + localFile + " 2>temp.err";
  DNAnexus.system(cmd);
};

DNAnexus.createApp = function (appSpec, auth) {
  return getResJSON(api.createApp(appSpec, auth));
};

function expandInput(input) {
  var key;
  for (key in input) {
    if (input[key].jobID != null) { input[key] = {job: input[key].jobID, field: key}; }
  }
}

DNAnexus.runApp = function (appID, input, auth) {
  expandInput(input);
  return getResJSON(api.runApp(appID, JSON.stringify({input: input}), auth));
};

/*
DNAnexus.runJob = function (job_id, func_name, input, token) {
  expandInput(input);
  headers.Authorization = token;
  return DNAnexus.httpPost("/jobs/" + job_id + "/jobs", {func_name: func_name, input: input});
};
*/

DNAnexus.getJob = function (jobID, auth) {
  return getResJSON(api.getObject("job", jobID, auth));
};

DNAnexus.getApp = function (appID, auth) {
  return getResJSON(api.getObject("app", appID, auth));
};

DNAnexus.updateJob = function (jobID, jobSpec, auth) {
  return getResJSON(api.updateJob(jobID, jobSpec, auth));
};

DNAnexus.createTable = function (tableSpec, auth) {
  return getIDFromLocation(api.createTable(tableSpec, auth));
};

DNAnexus.appendRows = function (tableID, rowsSpec, auth) {
//  DNAnexus.msg(rowsSpec);
  return getResJSON(api.appendRows(tableID, rowsSpec, auth));
};

DNAnexus.closeTable = function (tableID, auth) {
  if (getResJSON(api.closeTable(tableID, auth)).status !== 'CLOSED') { throw new Error("Fail to close table"); }
};

/*
DNAnexus.getAuthToken = function () {
  var ret_val = {};
  ret_val.userId = DNAnexus.httpPost("/users/", {password: "mypassword"}).id;
  ret_val.token = DNAnexus.httpGet("/accounts/GetAccessToken?userId=" + ret_val.userId);
  return ret_val;
};
*/
function Log(filename_, path_) {
  var path = path_, filename = filename_;
  if (path == null) { path = "/tmp/log"; }

  var hostname = require('socket').Socket.getHostName();

  var dr = new Directory(path);
  var stream = null, currentLog = null;

  function currTimeString() {
    return (new Date()).toJSON();
  }

  function logFilename() {
    return path + "/" + filename + "-" + currTimeString().replace(/T(\d+).*/, "-$1");
  }

  function currentTime() {
    return currTimeString().replace(/T(.*)Z/, " $1");
  }

  function checkStream() {
    var logFile = logFilename();

    if (!dr.exists()) { DNAnexus.system("mkdir -p " + path); }

    if (logFile !== currentLog) {
      if (stream != null) { stream.close(); }
      stream = new File(logFile);
      stream.open("a+");
      currentLog = logFile;
    }

    if (!stream.exists()) { stream.open("a+"); }
  }

  this.message = function (id, process, type, message) {
    checkStream();
    var buf = ["[" + currentTime() + "]", hostname, id, process, type, "[msg] " + message];
    stream.write(buf.join("\t") + "\n");
    stream.flush();
  };

  this.start_job = function (id) {
    this.message(id, "jobmanager", "INFO", "Job starts");
  };

  this.end_job = function (id) {
    this.message(id, "jobmanager", "INFO", "Job ends");
  };
}

exports.Log = Log;
exports.DNAnexus = DNAnexus;
