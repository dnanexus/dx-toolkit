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

var DNAnexusAPI = {};
var apiserver = "localhost:8124";
var http = require('http');
var headers = [{"Content-Type": "application/json; charset=utf8", "host": apiserver}, {"host": apiserver}];

// basic http request
function httpReq(req, res) {
  var response, request = new http.ClientRequest(apiserver + req.url);
  var tH = headers[req.headersIndex];

  request.method = req.method;
  if (req.data != null) { request.post = req.data; }
  tH.Authorization = req.auth_token;
  request.header(tH);
  response = request.send(false);

  if (response.status !== res.desired_status) { throw new Error(res.err_msg + ": " + (response.data).toString('utf8')); }
  return response;
}

function formReq(url, method, token, data, headersIndex) {
  if (headersIndex == null) { headersIndex = 0; }
  return {auth_token: token, url: url, method: method, data: data, headersIndex: headersIndex};
}

function formRes(stat, err_msg) {
  return {desired_status: stat, err_msg: err_msg};
}

// DNAnexus API wrapper
DNAnexusAPI.getObject = function (type, objectID, auth_token) {
  return httpReq(formReq("/" + type + "s/" + objectID, "GET", auth_token), formRes(200, "Error when getting " + type + " " + objectID));
};

DNAnexusAPI.setProperty = function (type, objectID, name, value, auth_token) {
  httpReq(formReq("/" + type + "s/" + objectID + "/properties/" + name, 'PUT', auth_token, value), formRes(204, "Error when setting property of " + type + " " + objectID));
};

DNAnexusAPI.getProperty = function (type, objectID, name, auth_token) {
  return httpReq(formReq("/" + type + "s/" + objectID + "/properties/" + name, 'GET', auth_token), formRes(200, "Error when getting property of " + type + " " + objectID));
};

DNAnexusAPI.createUpload = function (num_parts, auth_token) {
  return httpReq(formReq("/uploads", 'POST', auth_token, JSON.stringify({num_parts: num_parts})), formRes(201, "Error when creating upload!"));
};

DNAnexusAPI.putUpload = function (url, data, auth_token) {
  try {
    return httpReq(formReq(url, 'PUT', auth_token, data, 1), formRes(200, "Error when uploading data!"));
  } catch (err) {
    return null;
  }
};

DNAnexusAPI.getFile = function (fileID, auth_token) {
  return httpReq(formReq("/files/" + fileID, 'GET', auth_token, null, 1), formRes(200, "Error when getting file!"));
};

DNAnexusAPI.createFile = function (fileSpec, auth_token) {
  return httpReq(formReq("/files", 'POST', auth_token, fileSpec), formRes(201, "Error when creating file!"));
};

DNAnexusAPI.createApp = function (appSpec, auth_token) {
  return httpReq(formReq("/apps", "POST", auth_token, appSpec), formRes(201, "Error when creating app"));
};

DNAnexusAPI.runApp = function (appID, jobSpec, auth_token) {
  return httpReq(formReq("/apps/" + appID + "/jobs", "POST", auth_token, jobSpec), formRes(201, "Error when creating job"));
};

DNAnexusAPI.runJob = function (jobID, jobSpec, auth_token) {
  return httpReq(formReq("/jobs/" + jobID + "/jobs", "POST", auth_token, jobSpec), formRes(201, "Error when creating job"));
};

DNAnexusAPI.createTable = function (tableSpec, auth_token) {
  return httpReq(formReq("/tables", "POST", auth_token, tableSpec), formRes(200, "Error when creating table"));
};

DNAnexusAPI.appendRows = function (tableID, rowsSpec, auth_token) {
  return httpReq(formReq("/tables/" + tableID + "/rows", "POST", auth_token, rowsSpec), formRes(200, "Error when appending rows"));
};

DNAnexusAPI.closeTable = function (tableID, auth_token) {
  return httpReq(formReq("/tables/" + tableID, "PUT", auth_token, JSON.stringify({status: 'CLOSED'})), formRes(200, "Error when closing table"));
};

DNAnexusAPI.getTableRows = function (auth_token, type, tableID, columns, offset, limit) {
  var columns_str = encodeURIComponent(JSON.generate(columns));
  return httpReq(formReq("/" + type + "/" + tableID + "/rows?columns=" + columns_str + "&offset=" + offset + "&limit=" + limit, 'GET', auth_token), formRes(200, "Error when getting rows"));
};

DNAnexusAPI.createCoordsTable = function (url, auth_token) {
  return httpReq(formReq("/coords_tables", "POST", auth_token, JSON.stringify({sourceTable: url})), formRes(200, "Error when creating coords table"));
};

DNAnexusAPI.updateJob = function (jobID, jobSpec, auth_token) {
  return httpReq(formReq("/jobs/" + jobID, "PUT", auth_token, jobSpec), formRes(200, "Error when updating job " + jobID));
};

DNAnexusAPI.setPassWord = function (password) {
  return httpReq(formReq("/users", "POST", null, JSON.stringify({password: "mypassword"})), formRes(200, "Error when setting password"));
};

DNAnexusAPI.getAuthToken = function(userID) {
  return httpReq(formReq("/accounts/GetAccessToken?userId=" + userID, 'GET'), formRes(200, "Error when getting auth_token"));
};

exports.DNAnexusAPI = DNAnexusAPI;
