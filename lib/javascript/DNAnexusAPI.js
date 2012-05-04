
var dx = require('DNAnexus');


exports.fileNew = function(input_params) {
  return dx.DXHTTPRequest('/file/new', input_params);
};


exports.fileAddTags = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTags', input_params);
};


exports.fileAddTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTypes', input_params);
};


exports.fileClose = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/close', input_params);
};


exports.fileDescribe = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/describe', input_params);
};


exports.fileDownload = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/download', input_params);
};


exports.fileGetDetails = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getDetails', input_params);
};


exports.fileListProjects = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/listProjects', input_params);
};


exports.fileRemoveTags = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTags', input_params);
};


exports.fileRemoveTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTypes', input_params);
};


exports.fileRename = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/rename', input_params);
};


exports.fileSetDetails = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setDetails', input_params);
};


exports.fileSetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setProperties', input_params);
};


exports.fileSetVisibility = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setVisibility', input_params);
};


exports.fileUpload = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/upload', input_params);
};


exports.gtableNew = function(input_params) {
  return dx.DXHTTPRequest('/gtable/new', input_params);
};


exports.gtableAddRows = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addRows', input_params);
};


exports.gtableAddTags = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTags', input_params);
};


exports.gtableAddTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTypes', input_params);
};


exports.gtableClose = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/close', input_params);
};


exports.gtableDescribe = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/describe', input_params);
};


exports.gtableExtend = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/extend', input_params);
};


exports.gtableGet = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/get', input_params);
};


exports.gtableGetDetails = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getDetails', input_params);
};


exports.gtableListProjects = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/listProjects', input_params);
};


exports.gtableRemoveTags = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTags', input_params);
};


exports.gtableRemoveTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTypes', input_params);
};


exports.gtableRename = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/rename', input_params);
};


exports.gtableSetDetails = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setDetails', input_params);
};


exports.gtableSetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setProperties', input_params);
};


exports.gtableSetVisibility = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setVisibility', input_params);
};


exports.jobNew = function(input_params) {
  return dx.DXHTTPRequest('/job/new', input_params);
};


exports.jobDescribe = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/describe', input_params);
};


exports.jobTerminate = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/terminate', input_params);
};


exports.programNew = function(input_params) {
  return dx.DXHTTPRequest('/program/new', input_params);
};


exports.programAddTags = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTags', input_params);
};


exports.programAddTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTypes', input_params);
};


exports.programClose = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/close', input_params);
};


exports.programDescribe = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/describe', input_params);
};


exports.programGet = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/get', input_params);
};


exports.programGetDetails = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getDetails', input_params);
};


exports.programListProjects = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/listProjects', input_params);
};


exports.programRemoveTags = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTags', input_params);
};


exports.programRemoveTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTypes', input_params);
};


exports.programRename = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/rename', input_params);
};


exports.programRun = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/run', input_params);
};


exports.programSetDetails = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setDetails', input_params);
};


exports.programSetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setProperties', input_params);
};


exports.programSetVisibility = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setVisibility', input_params);
};


exports.projectNew = function(input_params) {
  return dx.DXHTTPRequest('/project/new', input_params);
};


exports.projectClone = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/clone', input_params);
};


exports.projectDescribe = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/describe', input_params);
};


exports.projectDestroy = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/destroy', input_params);
};


exports.projectListFolder = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/listFolder', input_params);
};


exports.projectMove = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/move', input_params);
};


exports.projectNewFolder = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/newFolder', input_params);
};


exports.projectRemoveFolder = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeFolder', input_params);
};


exports.projectRemoveObjects = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeObjects', input_params);
};


exports.projectUpdate = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/update', input_params);
};


exports.recordNew = function(input_params) {
  return dx.DXHTTPRequest('/record/new', input_params);
};


exports.recordAddTags = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTags', input_params);
};


exports.recordAddTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTypes', input_params);
};


exports.recordClose = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/close', input_params);
};


exports.recordDescribe = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/describe', input_params);
};


exports.recordGetDetails = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getDetails', input_params);
};


exports.recordListProjects = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/listProjects', input_params);
};


exports.recordRemoveTags = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTags', input_params);
};


exports.recordRemoveTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTypes', input_params);
};


exports.recordRename = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/rename', input_params);
};


exports.recordSetDetails = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setDetails', input_params);
};


exports.recordSetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setProperties', input_params);
};


exports.recordSetVisibility = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setVisibility', input_params);
};


exports.systemGetLog = function(input_params) {
  return dx.DXHTTPRequest('/system/getLog', input_params);
};


exports.systemFindDataObjects = function(input_params) {
  return dx.DXHTTPRequest('/system/findDataObjects', input_params);
};


exports.systemFindJobs = function(input_params) {
  return dx.DXHTTPRequest('/system/findJobs', input_params);
};


exports.systemFindProjects = function(input_params) {
  return dx.DXHTTPRequest('/system/findProjects', input_params);
};


exports.tableNew = function(input_params) {
  return dx.DXHTTPRequest('/table/new', input_params);
};


exports.tableAddColumns = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addColumns', input_params);
};


exports.tableAddIndices = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addIndices', input_params);
};


exports.tableAddRows = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addRows', input_params);
};


exports.tableAddTags = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTags', input_params);
};


exports.tableAddTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/addTypes', input_params);
};


exports.tableClose = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/close', input_params);
};


exports.tableDescribe = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/describe', input_params);
};


exports.tableGet = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/get', input_params);
};


exports.tableGetDetails = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/getDetails', input_params);
};


exports.tableListProjects = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/listProjects', input_params);
};


exports.tableRemoveColumns = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeColumns', input_params);
};


exports.tableRemoveIndices = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeIndices', input_params);
};


exports.tableRemoveRows = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeRows', input_params);
};


exports.tableRemoveTags = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTags', input_params);
};


exports.tableRemoveTypes = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/removeTypes', input_params);
};


exports.tableRename = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/rename', input_params);
};


exports.tableSetDetails = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setDetails', input_params);
};


exports.tableSetProperties = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setProperties', input_params);
};


exports.tableSetVisibility = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/setVisibility', input_params);
};


exports.tableUpdate = function(object_id, input_params) {
  return dx.DXHTTPRequest('/' + object_id + '/update', input_params);
};

