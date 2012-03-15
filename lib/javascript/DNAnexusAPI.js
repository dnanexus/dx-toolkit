
var dx = require('DNAnexus');


exports.systemSearch = function(input_params) {
  return dx.DXHTTPRequest('/system/search', input_params);
};


exports.userDescribe = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/describe', input_params);
};


exports.userGetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getProperties', input_params);
};


exports.userSetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setProperties', input_params);
};


exports.userGetPermissions = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getPermissions', input_params);
};


exports.userRevokePermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/revokePermission', input_params);
};


exports.userGrantPermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/grantPermission', input_params);
};


exports.userAddTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTypes', input_params);
};


exports.userRemoveTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTypes', input_params);
};


exports.groupNew = function(input_params) {
  return dx.DXHTTPRequest('/group/new', input_params);
};


exports.groupDescribe = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/describe', input_params);
};


exports.groupDestroy = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/destroy', input_params);
};


exports.groupGetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getProperties', input_params);
};


exports.groupSetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setProperties', input_params);
};


exports.groupGetPermissions = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getPermissions', input_params);
};


exports.groupRevokePermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/revokePermission', input_params);
};


exports.groupGrantPermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/grantPermission', input_params);
};


exports.groupAddMembers = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addMembers', input_params);
};


exports.groupRemoveMembers = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeMembers', input_params);
};


exports.groupAddTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTypes', input_params);
};


exports.groupRemoveTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTypes', input_params);
};


exports.jsonNew = function(input_params) {
  return dx.DXHTTPRequest('/json/new', input_params);
};


exports.jsonDescribe = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/describe', input_params);
};


exports.jsonDestroy = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/destroy', input_params);
};


exports.jsonGetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getProperties', input_params);
};


exports.jsonSetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setProperties', input_params);
};


exports.jsonGetPermissions = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getPermissions', input_params);
};


exports.jsonRevokePermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/revokePermission', input_params);
};


exports.jsonGrantPermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/grantPermission', input_params);
};


exports.jsonAddTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTypes', input_params);
};


exports.jsonRemoveTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTypes', input_params);
};


exports.jsonGet = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/get', input_params);
};


exports.jsonSet = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/set', input_params);
};


exports.collectionNew = function(input_params) {
  return dx.DXHTTPRequest('/collection/new', input_params);
};


exports.collectionDescribe = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/describe', input_params);
};


exports.collectionDestroy = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/destroy', input_params);
};


exports.collectionGetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getProperties', input_params);
};


exports.collectionSetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setProperties', input_params);
};


exports.collectionGetPermissions = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getPermissions', input_params);
};


exports.collectionRevokePermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/revokePermission', input_params);
};


exports.collectionGrantPermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/grantPermission', input_params);
};


exports.collectionAddTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTypes', input_params);
};


exports.collectionRemoveTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTypes', input_params);
};


exports.collectionGet = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/get', input_params);
};


exports.fileNew = function(input_params) {
  return dx.DXHTTPRequest('/file/new', input_params);
};


exports.fileDescribe = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/describe', input_params);
};


exports.fileDestroy = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/destroy', input_params);
};


exports.fileGetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getProperties', input_params);
};


exports.fileSetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setProperties', input_params);
};


exports.fileGetPermissions = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getPermissions', input_params);
};


exports.fileRevokePermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/revokePermission', input_params);
};


exports.fileGrantPermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/grantPermission', input_params);
};


exports.fileAddTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTypes', input_params);
};


exports.fileRemoveTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTypes', input_params);
};


exports.fileUpload = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/upload', input_params);
};


exports.fileClose = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/close', input_params);
};


exports.fileDownload = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/download', input_params);
};


exports.tableNew = function(input_params) {
  return dx.DXHTTPRequest('/table/new', input_params);
};


exports.tableDescribe = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/describe', input_params);
};


exports.tableExtend = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/extend', input_params);
};


exports.tableDestroy = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/destroy', input_params);
};


exports.tableGetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getProperties', input_params);
};


exports.tableSetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setProperties', input_params);
};


exports.tableGetPermissions = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getPermissions', input_params);
};


exports.tableRevokePermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/revokePermission', input_params);
};


exports.tableGrantPermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/grantPermission', input_params);
};


exports.tableAddTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTypes', input_params);
};


exports.tableRemoveTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTypes', input_params);
};


exports.tableAddRows = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addRows', input_params);
};


exports.tableClose = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/close', input_params);
};


exports.tableGet = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/get', input_params);
};


exports.appNew = function(input_params) {
  return dx.DXHTTPRequest('/app/new', input_params);
};


exports.appDescribe = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/describe', input_params);
};


exports.appDestroy = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/destroy', input_params);
};


exports.appGetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getProperties', input_params);
};


exports.appSetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setProperties', input_params);
};


exports.appGetPermissions = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getPermissions', input_params);
};


exports.appRevokePermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/revokePermission', input_params);
};


exports.appGrantPermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/grantPermission', input_params);
};


exports.appAddTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTypes', input_params);
};


exports.appRemoveTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTypes', input_params);
};


exports.appRun = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/run', input_params);
};


exports.jobNew = function(input_params) {
  return dx.DXHTTPRequest('/job/new', input_params);
};


exports.jobDescribe = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/describe', input_params);
};


exports.jobDestroy = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/destroy', input_params);
};


exports.jobGetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getProperties', input_params);
};


exports.jobSetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setProperties', input_params);
};


exports.jobGetPermissions = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getPermissions', input_params);
};


exports.jobRevokePermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/revokePermission', input_params);
};


exports.jobGrantPermission = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/grantPermission', input_params);
};


exports.jobAddTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTypes', input_params);
};


exports.jobRemoveTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTypes', input_params);
};

