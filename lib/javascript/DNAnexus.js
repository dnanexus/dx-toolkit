var underscore = require('underscore')._;
var http = require('http');
var process = require('process');
var fs = require('fs');

var APISERVER_HOST;
var APISERVER_PORT;
var APISERVER;
var SECURITY_CONTEXT;

function DXHTTPRequest(resource, data, method, headers) {
  if (underscore.isUndefined(resource)) {
    throw new Error("resource argument is required");
  }
  if (underscore.isUndefined(SECURITY_CONTEXT)) {
    throw new Error("SECURITY_CONTEXT must be set");
  }
  if (underscore.isUndefined(data)) {
    data = {};
  }
  if (underscore.isUndefined(method)) {
    method = 'POST';
  }
  if (underscore.isUndefined(headers)) {
    headers = {};
  }
  headers['Authorization'] = SECURITY_CONTEXT.auth_token_type + ' ' + SECURITY_CONTEXT.auth_token;
  headers['Content-Type'] = 'application/json';
  
  var request = new http.ClientRequest(APISERVER_HOST + ':' + APISERVER_PORT + resource);
  request.method = method;
  request.header(headers);
  request.post = JSON.stringify(data);
  
  response = request.send(false);
  if (response.status != 200) {
    throw new Error("Code "+response.status+", "+response.data.toString('utf-8')+" TODO: make me a DXError");
  }
  return JSON.parse(response.data.toString('utf-8'));
}

function setAPIServerInfo(host, port, protocol) {
  APISERVER_HOST = host;
  APISERVER_PORT = port;
  APISERVER = protocol + "://" + host + ":" + port;
}

function setSecurityContext(security_context) {
  SECURITY_CONTEXT = JSON.parse(security_context);
}

if (!(underscore.isUndefined(system.env.APISERVER_HOST) || underscore.isUndefined(system.env.APISERVER_PORT))) {
  setAPIServerInfo(system.env.APISERVER_HOST, system.env.APISERVER_PORT, 'http');
} else {
  setAPIServerInfo('localhost', 8124, 'http');
}

if (!(underscore.isUndefined(system.env.SECURITY_CONTEXT))) {
  setSecurityContext(system.env.SECURITY_CONTEXT);
}

exports.DXHTTPRequest = DXHTTPRequest;
exports.setAPIServerInfo = setAPIServerInfo;
exports.setSecurityContext = setSecurityContext;

exports.api = require('DNAnexusAPI');
