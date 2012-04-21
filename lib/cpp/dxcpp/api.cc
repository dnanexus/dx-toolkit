
#include "api.h"


dx::JSON fileNew(const std::string &input_params) {
  return DXHTTPRequest("/file/new", input_params);
}

dx::JSON fileNew(const dx::JSON &input_params) {
  return fileNew(input_params.toString());
}


dx::JSON fileAddTags(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTags"), input_params);
}

dx::JSON fileAddTags(const std::string &object_id, const dx::JSON &input_params) {
  return fileAddTags(object_id, input_params.toString());
}


dx::JSON fileAddTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTypes"), input_params);
}

dx::JSON fileAddTypes(const std::string &object_id, const dx::JSON &input_params) {
  return fileAddTypes(object_id, input_params.toString());
}


dx::JSON fileClose(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/close"), input_params);
}

dx::JSON fileClose(const std::string &object_id, const dx::JSON &input_params) {
  return fileClose(object_id, input_params.toString());
}


dx::JSON fileDescribe(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/describe"), input_params);
}

dx::JSON fileDescribe(const std::string &object_id, const dx::JSON &input_params) {
  return fileDescribe(object_id, input_params.toString());
}


dx::JSON fileDownload(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/download"), input_params);
}

dx::JSON fileDownload(const std::string &object_id, const dx::JSON &input_params) {
  return fileDownload(object_id, input_params.toString());
}


dx::JSON fileGetDetails(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getDetails"), input_params);
}

dx::JSON fileGetDetails(const std::string &object_id, const dx::JSON &input_params) {
  return fileGetDetails(object_id, input_params.toString());
}


dx::JSON fileListProjects(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/listProjects"), input_params);
}

dx::JSON fileListProjects(const std::string &object_id, const dx::JSON &input_params) {
  return fileListProjects(object_id, input_params.toString());
}


dx::JSON fileRemoveTags(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTags"), input_params);
}

dx::JSON fileRemoveTags(const std::string &object_id, const dx::JSON &input_params) {
  return fileRemoveTags(object_id, input_params.toString());
}


dx::JSON fileRemoveTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTypes"), input_params);
}

dx::JSON fileRemoveTypes(const std::string &object_id, const dx::JSON &input_params) {
  return fileRemoveTypes(object_id, input_params.toString());
}


dx::JSON fileRename(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/rename"), input_params);
}

dx::JSON fileRename(const std::string &object_id, const dx::JSON &input_params) {
  return fileRename(object_id, input_params.toString());
}


dx::JSON fileSetDetails(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setDetails"), input_params);
}

dx::JSON fileSetDetails(const std::string &object_id, const dx::JSON &input_params) {
  return fileSetDetails(object_id, input_params.toString());
}


dx::JSON fileSetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setProperties"), input_params);
}

dx::JSON fileSetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return fileSetProperties(object_id, input_params.toString());
}


dx::JSON fileSetVisibility(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setVisibility"), input_params);
}

dx::JSON fileSetVisibility(const std::string &object_id, const dx::JSON &input_params) {
  return fileSetVisibility(object_id, input_params.toString());
}


dx::JSON fileUpload(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/upload"), input_params);
}

dx::JSON fileUpload(const std::string &object_id, const dx::JSON &input_params) {
  return fileUpload(object_id, input_params.toString());
}


dx::JSON gtableNew(const std::string &input_params) {
  return DXHTTPRequest("/gtable/new", input_params);
}

dx::JSON gtableNew(const dx::JSON &input_params) {
  return gtableNew(input_params.toString());
}


dx::JSON gtableAddRows(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addRows"), input_params);
}

dx::JSON gtableAddRows(const std::string &object_id, const dx::JSON &input_params) {
  return gtableAddRows(object_id, input_params.toString());
}


dx::JSON gtableAddTags(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTags"), input_params);
}

dx::JSON gtableAddTags(const std::string &object_id, const dx::JSON &input_params) {
  return gtableAddTags(object_id, input_params.toString());
}


dx::JSON gtableAddTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTypes"), input_params);
}

dx::JSON gtableAddTypes(const std::string &object_id, const dx::JSON &input_params) {
  return gtableAddTypes(object_id, input_params.toString());
}


dx::JSON gtableClose(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/close"), input_params);
}

dx::JSON gtableClose(const std::string &object_id, const dx::JSON &input_params) {
  return gtableClose(object_id, input_params.toString());
}


dx::JSON gtableDescribe(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/describe"), input_params);
}

dx::JSON gtableDescribe(const std::string &object_id, const dx::JSON &input_params) {
  return gtableDescribe(object_id, input_params.toString());
}


dx::JSON gtableExtend(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/extend"), input_params);
}

dx::JSON gtableExtend(const std::string &object_id, const dx::JSON &input_params) {
  return gtableExtend(object_id, input_params.toString());
}


dx::JSON gtableGet(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/get"), input_params);
}

dx::JSON gtableGet(const std::string &object_id, const dx::JSON &input_params) {
  return gtableGet(object_id, input_params.toString());
}


dx::JSON gtableGetDetails(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getDetails"), input_params);
}

dx::JSON gtableGetDetails(const std::string &object_id, const dx::JSON &input_params) {
  return gtableGetDetails(object_id, input_params.toString());
}


dx::JSON gtableListProjects(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/listProjects"), input_params);
}

dx::JSON gtableListProjects(const std::string &object_id, const dx::JSON &input_params) {
  return gtableListProjects(object_id, input_params.toString());
}


dx::JSON gtableRemoveTags(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTags"), input_params);
}

dx::JSON gtableRemoveTags(const std::string &object_id, const dx::JSON &input_params) {
  return gtableRemoveTags(object_id, input_params.toString());
}


dx::JSON gtableRemoveTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTypes"), input_params);
}

dx::JSON gtableRemoveTypes(const std::string &object_id, const dx::JSON &input_params) {
  return gtableRemoveTypes(object_id, input_params.toString());
}


dx::JSON gtableRename(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/rename"), input_params);
}

dx::JSON gtableRename(const std::string &object_id, const dx::JSON &input_params) {
  return gtableRename(object_id, input_params.toString());
}


dx::JSON gtableSetDetails(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setDetails"), input_params);
}

dx::JSON gtableSetDetails(const std::string &object_id, const dx::JSON &input_params) {
  return gtableSetDetails(object_id, input_params.toString());
}


dx::JSON gtableSetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setProperties"), input_params);
}

dx::JSON gtableSetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return gtableSetProperties(object_id, input_params.toString());
}


dx::JSON gtableSetVisibility(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setVisibility"), input_params);
}

dx::JSON gtableSetVisibility(const std::string &object_id, const dx::JSON &input_params) {
  return gtableSetVisibility(object_id, input_params.toString());
}


dx::JSON jobNew(const std::string &input_params) {
  return DXHTTPRequest("/job/new", input_params);
}

dx::JSON jobNew(const dx::JSON &input_params) {
  return jobNew(input_params.toString());
}


dx::JSON jobDescribe(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/describe"), input_params);
}

dx::JSON jobDescribe(const std::string &object_id, const dx::JSON &input_params) {
  return jobDescribe(object_id, input_params.toString());
}


dx::JSON jobTerminate(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/terminate"), input_params);
}

dx::JSON jobTerminate(const std::string &object_id, const dx::JSON &input_params) {
  return jobTerminate(object_id, input_params.toString());
}


dx::JSON programNew(const std::string &input_params) {
  return DXHTTPRequest("/program/new", input_params);
}

dx::JSON programNew(const dx::JSON &input_params) {
  return programNew(input_params.toString());
}


dx::JSON programAddTags(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTags"), input_params);
}

dx::JSON programAddTags(const std::string &object_id, const dx::JSON &input_params) {
  return programAddTags(object_id, input_params.toString());
}


dx::JSON programAddTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTypes"), input_params);
}

dx::JSON programAddTypes(const std::string &object_id, const dx::JSON &input_params) {
  return programAddTypes(object_id, input_params.toString());
}


dx::JSON programClose(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/close"), input_params);
}

dx::JSON programClose(const std::string &object_id, const dx::JSON &input_params) {
  return programClose(object_id, input_params.toString());
}


dx::JSON programDescribe(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/describe"), input_params);
}

dx::JSON programDescribe(const std::string &object_id, const dx::JSON &input_params) {
  return programDescribe(object_id, input_params.toString());
}


dx::JSON programGet(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/get"), input_params);
}

dx::JSON programGet(const std::string &object_id, const dx::JSON &input_params) {
  return programGet(object_id, input_params.toString());
}


dx::JSON programGetDetails(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getDetails"), input_params);
}

dx::JSON programGetDetails(const std::string &object_id, const dx::JSON &input_params) {
  return programGetDetails(object_id, input_params.toString());
}


dx::JSON programListProjects(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/listProjects"), input_params);
}

dx::JSON programListProjects(const std::string &object_id, const dx::JSON &input_params) {
  return programListProjects(object_id, input_params.toString());
}


dx::JSON programRemoveTags(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTags"), input_params);
}

dx::JSON programRemoveTags(const std::string &object_id, const dx::JSON &input_params) {
  return programRemoveTags(object_id, input_params.toString());
}


dx::JSON programRemoveTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTypes"), input_params);
}

dx::JSON programRemoveTypes(const std::string &object_id, const dx::JSON &input_params) {
  return programRemoveTypes(object_id, input_params.toString());
}


dx::JSON programRename(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/rename"), input_params);
}

dx::JSON programRename(const std::string &object_id, const dx::JSON &input_params) {
  return programRename(object_id, input_params.toString());
}


dx::JSON programRun(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/run"), input_params);
}

dx::JSON programRun(const std::string &object_id, const dx::JSON &input_params) {
  return programRun(object_id, input_params.toString());
}


dx::JSON programSetDetails(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setDetails"), input_params);
}

dx::JSON programSetDetails(const std::string &object_id, const dx::JSON &input_params) {
  return programSetDetails(object_id, input_params.toString());
}


dx::JSON programSetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setProperties"), input_params);
}

dx::JSON programSetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return programSetProperties(object_id, input_params.toString());
}


dx::JSON programSetVisibility(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setVisibility"), input_params);
}

dx::JSON programSetVisibility(const std::string &object_id, const dx::JSON &input_params) {
  return programSetVisibility(object_id, input_params.toString());
}


dx::JSON projectNew(const std::string &input_params) {
  return DXHTTPRequest("/project/new", input_params);
}

dx::JSON projectNew(const dx::JSON &input_params) {
  return projectNew(input_params.toString());
}


dx::JSON projectClone(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/clone"), input_params);
}

dx::JSON projectClone(const std::string &object_id, const dx::JSON &input_params) {
  return projectClone(object_id, input_params.toString());
}


dx::JSON projectDescribe(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/describe"), input_params);
}

dx::JSON projectDescribe(const std::string &object_id, const dx::JSON &input_params) {
  return projectDescribe(object_id, input_params.toString());
}


dx::JSON projectDestroy(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/destroy"), input_params);
}

dx::JSON projectDestroy(const std::string &object_id, const dx::JSON &input_params) {
  return projectDestroy(object_id, input_params.toString());
}


dx::JSON projectListFolder(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/listFolder"), input_params);
}

dx::JSON projectListFolder(const std::string &object_id, const dx::JSON &input_params) {
  return projectListFolder(object_id, input_params.toString());
}


dx::JSON projectMove(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/move"), input_params);
}

dx::JSON projectMove(const std::string &object_id, const dx::JSON &input_params) {
  return projectMove(object_id, input_params.toString());
}


dx::JSON projectNewFolder(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/newFolder"), input_params);
}

dx::JSON projectNewFolder(const std::string &object_id, const dx::JSON &input_params) {
  return projectNewFolder(object_id, input_params.toString());
}


dx::JSON projectRemoveFolder(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeFolder"), input_params);
}

dx::JSON projectRemoveFolder(const std::string &object_id, const dx::JSON &input_params) {
  return projectRemoveFolder(object_id, input_params.toString());
}


dx::JSON projectRemoveObjects(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeObjects"), input_params);
}

dx::JSON projectRemoveObjects(const std::string &object_id, const dx::JSON &input_params) {
  return projectRemoveObjects(object_id, input_params.toString());
}


dx::JSON projectUpdate(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/update"), input_params);
}

dx::JSON projectUpdate(const std::string &object_id, const dx::JSON &input_params) {
  return projectUpdate(object_id, input_params.toString());
}


dx::JSON recordNew(const std::string &input_params) {
  return DXHTTPRequest("/record/new", input_params);
}

dx::JSON recordNew(const dx::JSON &input_params) {
  return recordNew(input_params.toString());
}


dx::JSON recordAddTags(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTags"), input_params);
}

dx::JSON recordAddTags(const std::string &object_id, const dx::JSON &input_params) {
  return recordAddTags(object_id, input_params.toString());
}


dx::JSON recordAddTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTypes"), input_params);
}

dx::JSON recordAddTypes(const std::string &object_id, const dx::JSON &input_params) {
  return recordAddTypes(object_id, input_params.toString());
}


dx::JSON recordClose(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/close"), input_params);
}

dx::JSON recordClose(const std::string &object_id, const dx::JSON &input_params) {
  return recordClose(object_id, input_params.toString());
}


dx::JSON recordDescribe(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/describe"), input_params);
}

dx::JSON recordDescribe(const std::string &object_id, const dx::JSON &input_params) {
  return recordDescribe(object_id, input_params.toString());
}


dx::JSON recordGetDetails(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getDetails"), input_params);
}

dx::JSON recordGetDetails(const std::string &object_id, const dx::JSON &input_params) {
  return recordGetDetails(object_id, input_params.toString());
}


dx::JSON recordListProjects(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/listProjects"), input_params);
}

dx::JSON recordListProjects(const std::string &object_id, const dx::JSON &input_params) {
  return recordListProjects(object_id, input_params.toString());
}


dx::JSON recordRemoveTags(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTags"), input_params);
}

dx::JSON recordRemoveTags(const std::string &object_id, const dx::JSON &input_params) {
  return recordRemoveTags(object_id, input_params.toString());
}


dx::JSON recordRemoveTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTypes"), input_params);
}

dx::JSON recordRemoveTypes(const std::string &object_id, const dx::JSON &input_params) {
  return recordRemoveTypes(object_id, input_params.toString());
}


dx::JSON recordRename(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/rename"), input_params);
}

dx::JSON recordRename(const std::string &object_id, const dx::JSON &input_params) {
  return recordRename(object_id, input_params.toString());
}


dx::JSON recordSetDetails(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setDetails"), input_params);
}

dx::JSON recordSetDetails(const std::string &object_id, const dx::JSON &input_params) {
  return recordSetDetails(object_id, input_params.toString());
}


dx::JSON recordSetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setProperties"), input_params);
}

dx::JSON recordSetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return recordSetProperties(object_id, input_params.toString());
}


dx::JSON recordSetVisibility(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setVisibility"), input_params);
}

dx::JSON recordSetVisibility(const std::string &object_id, const dx::JSON &input_params) {
  return recordSetVisibility(object_id, input_params.toString());
}


dx::JSON systemGetLog(const std::string &input_params) {
  return DXHTTPRequest("/system/getLog", input_params);
}

dx::JSON systemGetLog(const dx::JSON &input_params) {
  return systemGetLog(input_params.toString());
}


dx::JSON systemFindDataObjects(const std::string &input_params) {
  return DXHTTPRequest("/system/findDataObjects", input_params);
}

dx::JSON systemFindDataObjects(const dx::JSON &input_params) {
  return systemFindDataObjects(input_params.toString());
}


dx::JSON systemFindJobs(const std::string &input_params) {
  return DXHTTPRequest("/system/findJobs", input_params);
}

dx::JSON systemFindJobs(const dx::JSON &input_params) {
  return systemFindJobs(input_params.toString());
}


dx::JSON tableNew(const std::string &input_params) {
  return DXHTTPRequest("/table/new", input_params);
}

dx::JSON tableNew(const dx::JSON &input_params) {
  return tableNew(input_params.toString());
}


dx::JSON tableAddColumns(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addColumns"), input_params);
}

dx::JSON tableAddColumns(const std::string &object_id, const dx::JSON &input_params) {
  return tableAddColumns(object_id, input_params.toString());
}


dx::JSON tableAddIndices(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addIndices"), input_params);
}

dx::JSON tableAddIndices(const std::string &object_id, const dx::JSON &input_params) {
  return tableAddIndices(object_id, input_params.toString());
}


dx::JSON tableAddRows(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addRows"), input_params);
}

dx::JSON tableAddRows(const std::string &object_id, const dx::JSON &input_params) {
  return tableAddRows(object_id, input_params.toString());
}


dx::JSON tableAddTags(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTags"), input_params);
}

dx::JSON tableAddTags(const std::string &object_id, const dx::JSON &input_params) {
  return tableAddTags(object_id, input_params.toString());
}


dx::JSON tableAddTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTypes"), input_params);
}

dx::JSON tableAddTypes(const std::string &object_id, const dx::JSON &input_params) {
  return tableAddTypes(object_id, input_params.toString());
}


dx::JSON tableClose(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/close"), input_params);
}

dx::JSON tableClose(const std::string &object_id, const dx::JSON &input_params) {
  return tableClose(object_id, input_params.toString());
}


dx::JSON tableDescribe(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/describe"), input_params);
}

dx::JSON tableDescribe(const std::string &object_id, const dx::JSON &input_params) {
  return tableDescribe(object_id, input_params.toString());
}


dx::JSON tableGet(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/get"), input_params);
}

dx::JSON tableGet(const std::string &object_id, const dx::JSON &input_params) {
  return tableGet(object_id, input_params.toString());
}


dx::JSON tableGetDetails(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getDetails"), input_params);
}

dx::JSON tableGetDetails(const std::string &object_id, const dx::JSON &input_params) {
  return tableGetDetails(object_id, input_params.toString());
}


dx::JSON tableListProjects(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/listProjects"), input_params);
}

dx::JSON tableListProjects(const std::string &object_id, const dx::JSON &input_params) {
  return tableListProjects(object_id, input_params.toString());
}


dx::JSON tableRemoveColumns(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeColumns"), input_params);
}

dx::JSON tableRemoveColumns(const std::string &object_id, const dx::JSON &input_params) {
  return tableRemoveColumns(object_id, input_params.toString());
}


dx::JSON tableRemoveIndices(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeIndices"), input_params);
}

dx::JSON tableRemoveIndices(const std::string &object_id, const dx::JSON &input_params) {
  return tableRemoveIndices(object_id, input_params.toString());
}


dx::JSON tableRemoveRows(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeRows"), input_params);
}

dx::JSON tableRemoveRows(const std::string &object_id, const dx::JSON &input_params) {
  return tableRemoveRows(object_id, input_params.toString());
}


dx::JSON tableRemoveTags(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTags"), input_params);
}

dx::JSON tableRemoveTags(const std::string &object_id, const dx::JSON &input_params) {
  return tableRemoveTags(object_id, input_params.toString());
}


dx::JSON tableRemoveTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTypes"), input_params);
}

dx::JSON tableRemoveTypes(const std::string &object_id, const dx::JSON &input_params) {
  return tableRemoveTypes(object_id, input_params.toString());
}


dx::JSON tableRename(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/rename"), input_params);
}

dx::JSON tableRename(const std::string &object_id, const dx::JSON &input_params) {
  return tableRename(object_id, input_params.toString());
}


dx::JSON tableSetDetails(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setDetails"), input_params);
}

dx::JSON tableSetDetails(const std::string &object_id, const dx::JSON &input_params) {
  return tableSetDetails(object_id, input_params.toString());
}


dx::JSON tableSetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setProperties"), input_params);
}

dx::JSON tableSetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return tableSetProperties(object_id, input_params.toString());
}


dx::JSON tableSetVisibility(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setVisibility"), input_params);
}

dx::JSON tableSetVisibility(const std::string &object_id, const dx::JSON &input_params) {
  return tableSetVisibility(object_id, input_params.toString());
}


dx::JSON tableUpdate(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/update"), input_params);
}

dx::JSON tableUpdate(const std::string &object_id, const dx::JSON &input_params) {
  return tableUpdate(object_id, input_params.toString());
}

