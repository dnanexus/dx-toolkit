#ifndef DXCPP_API_H
#define DXCPP_API_H

#include "dxjson/dxjson.h"


dx::JSON fileNew(const std::string &input_params="{}");
dx::JSON fileNew(const dx::JSON &input_params);


dx::JSON fileAddTags(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileAddTags(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileAddTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileAddTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileClose(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileClose(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileDescribe(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileDescribe(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileDownload(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileDownload(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileGetDetails(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileGetDetails(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileListProjects(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileListProjects(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileRemoveTags(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileRemoveTags(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileRemoveTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileRemoveTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileRename(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileRename(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileSetDetails(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileSetDetails(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileSetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileSetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileSetVisibility(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileSetVisibility(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileUpload(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileUpload(const std::string &object_id, const dx::JSON &input_params);


dx::JSON gtableNew(const std::string &input_params="{}");
dx::JSON gtableNew(const dx::JSON &input_params);


dx::JSON gtableAddRows(const std::string &object_id, const std::string &input_params="{}");
dx::JSON gtableAddRows(const std::string &object_id, const dx::JSON &input_params);


dx::JSON gtableAddTags(const std::string &object_id, const std::string &input_params="{}");
dx::JSON gtableAddTags(const std::string &object_id, const dx::JSON &input_params);


dx::JSON gtableAddTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON gtableAddTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON gtableClose(const std::string &object_id, const std::string &input_params="{}");
dx::JSON gtableClose(const std::string &object_id, const dx::JSON &input_params);


dx::JSON gtableDescribe(const std::string &object_id, const std::string &input_params="{}");
dx::JSON gtableDescribe(const std::string &object_id, const dx::JSON &input_params);


dx::JSON gtableExtend(const std::string &object_id, const std::string &input_params="{}");
dx::JSON gtableExtend(const std::string &object_id, const dx::JSON &input_params);


dx::JSON gtableGet(const std::string &object_id, const std::string &input_params="{}");
dx::JSON gtableGet(const std::string &object_id, const dx::JSON &input_params);


dx::JSON gtableGetDetails(const std::string &object_id, const std::string &input_params="{}");
dx::JSON gtableGetDetails(const std::string &object_id, const dx::JSON &input_params);


dx::JSON gtableListProjects(const std::string &object_id, const std::string &input_params="{}");
dx::JSON gtableListProjects(const std::string &object_id, const dx::JSON &input_params);


dx::JSON gtableRemoveTags(const std::string &object_id, const std::string &input_params="{}");
dx::JSON gtableRemoveTags(const std::string &object_id, const dx::JSON &input_params);


dx::JSON gtableRemoveTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON gtableRemoveTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON gtableRename(const std::string &object_id, const std::string &input_params="{}");
dx::JSON gtableRename(const std::string &object_id, const dx::JSON &input_params);


dx::JSON gtableSetDetails(const std::string &object_id, const std::string &input_params="{}");
dx::JSON gtableSetDetails(const std::string &object_id, const dx::JSON &input_params);


dx::JSON gtableSetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON gtableSetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON gtableSetVisibility(const std::string &object_id, const std::string &input_params="{}");
dx::JSON gtableSetVisibility(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jobNew(const std::string &input_params="{}");
dx::JSON jobNew(const dx::JSON &input_params);


dx::JSON jobDescribe(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jobDescribe(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jobTerminate(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jobTerminate(const std::string &object_id, const dx::JSON &input_params);


dx::JSON programNew(const std::string &input_params="{}");
dx::JSON programNew(const dx::JSON &input_params);


dx::JSON programAddTags(const std::string &object_id, const std::string &input_params="{}");
dx::JSON programAddTags(const std::string &object_id, const dx::JSON &input_params);


dx::JSON programAddTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON programAddTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON programClose(const std::string &object_id, const std::string &input_params="{}");
dx::JSON programClose(const std::string &object_id, const dx::JSON &input_params);


dx::JSON programDescribe(const std::string &object_id, const std::string &input_params="{}");
dx::JSON programDescribe(const std::string &object_id, const dx::JSON &input_params);


dx::JSON programGet(const std::string &object_id, const std::string &input_params="{}");
dx::JSON programGet(const std::string &object_id, const dx::JSON &input_params);


dx::JSON programGetDetails(const std::string &object_id, const std::string &input_params="{}");
dx::JSON programGetDetails(const std::string &object_id, const dx::JSON &input_params);


dx::JSON programListProjects(const std::string &object_id, const std::string &input_params="{}");
dx::JSON programListProjects(const std::string &object_id, const dx::JSON &input_params);


dx::JSON programRemoveTags(const std::string &object_id, const std::string &input_params="{}");
dx::JSON programRemoveTags(const std::string &object_id, const dx::JSON &input_params);


dx::JSON programRemoveTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON programRemoveTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON programRename(const std::string &object_id, const std::string &input_params="{}");
dx::JSON programRename(const std::string &object_id, const dx::JSON &input_params);


dx::JSON programRun(const std::string &object_id, const std::string &input_params="{}");
dx::JSON programRun(const std::string &object_id, const dx::JSON &input_params);


dx::JSON programSetDetails(const std::string &object_id, const std::string &input_params="{}");
dx::JSON programSetDetails(const std::string &object_id, const dx::JSON &input_params);


dx::JSON programSetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON programSetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON programSetVisibility(const std::string &object_id, const std::string &input_params="{}");
dx::JSON programSetVisibility(const std::string &object_id, const dx::JSON &input_params);


dx::JSON projectNew(const std::string &input_params="{}");
dx::JSON projectNew(const dx::JSON &input_params);


dx::JSON projectClone(const std::string &object_id, const std::string &input_params="{}");
dx::JSON projectClone(const std::string &object_id, const dx::JSON &input_params);


dx::JSON projectDescribe(const std::string &object_id, const std::string &input_params="{}");
dx::JSON projectDescribe(const std::string &object_id, const dx::JSON &input_params);


dx::JSON projectDestroy(const std::string &object_id, const std::string &input_params="{}");
dx::JSON projectDestroy(const std::string &object_id, const dx::JSON &input_params);


dx::JSON projectListFolder(const std::string &object_id, const std::string &input_params="{}");
dx::JSON projectListFolder(const std::string &object_id, const dx::JSON &input_params);


dx::JSON projectMove(const std::string &object_id, const std::string &input_params="{}");
dx::JSON projectMove(const std::string &object_id, const dx::JSON &input_params);


dx::JSON projectNewFolder(const std::string &object_id, const std::string &input_params="{}");
dx::JSON projectNewFolder(const std::string &object_id, const dx::JSON &input_params);


dx::JSON projectRemoveFolder(const std::string &object_id, const std::string &input_params="{}");
dx::JSON projectRemoveFolder(const std::string &object_id, const dx::JSON &input_params);


dx::JSON projectRemoveObjects(const std::string &object_id, const std::string &input_params="{}");
dx::JSON projectRemoveObjects(const std::string &object_id, const dx::JSON &input_params);


dx::JSON projectUpdate(const std::string &object_id, const std::string &input_params="{}");
dx::JSON projectUpdate(const std::string &object_id, const dx::JSON &input_params);


dx::JSON recordNew(const std::string &input_params="{}");
dx::JSON recordNew(const dx::JSON &input_params);


dx::JSON recordAddTags(const std::string &object_id, const std::string &input_params="{}");
dx::JSON recordAddTags(const std::string &object_id, const dx::JSON &input_params);


dx::JSON recordAddTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON recordAddTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON recordClose(const std::string &object_id, const std::string &input_params="{}");
dx::JSON recordClose(const std::string &object_id, const dx::JSON &input_params);


dx::JSON recordDescribe(const std::string &object_id, const std::string &input_params="{}");
dx::JSON recordDescribe(const std::string &object_id, const dx::JSON &input_params);


dx::JSON recordGetDetails(const std::string &object_id, const std::string &input_params="{}");
dx::JSON recordGetDetails(const std::string &object_id, const dx::JSON &input_params);


dx::JSON recordListProjects(const std::string &object_id, const std::string &input_params="{}");
dx::JSON recordListProjects(const std::string &object_id, const dx::JSON &input_params);


dx::JSON recordRemoveTags(const std::string &object_id, const std::string &input_params="{}");
dx::JSON recordRemoveTags(const std::string &object_id, const dx::JSON &input_params);


dx::JSON recordRemoveTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON recordRemoveTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON recordRename(const std::string &object_id, const std::string &input_params="{}");
dx::JSON recordRename(const std::string &object_id, const dx::JSON &input_params);


dx::JSON recordSetDetails(const std::string &object_id, const std::string &input_params="{}");
dx::JSON recordSetDetails(const std::string &object_id, const dx::JSON &input_params);


dx::JSON recordSetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON recordSetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON recordSetVisibility(const std::string &object_id, const std::string &input_params="{}");
dx::JSON recordSetVisibility(const std::string &object_id, const dx::JSON &input_params);


dx::JSON systemGetLog(const std::string &input_params="{}");
dx::JSON systemGetLog(const dx::JSON &input_params);


dx::JSON systemFindDataObjects(const std::string &input_params="{}");
dx::JSON systemFindDataObjects(const dx::JSON &input_params);


dx::JSON systemFindJobs(const std::string &input_params="{}");
dx::JSON systemFindJobs(const dx::JSON &input_params);


dx::JSON systemFindProjects(const std::string &input_params="{}");
dx::JSON systemFindProjects(const dx::JSON &input_params);


dx::JSON tableNew(const std::string &input_params="{}");
dx::JSON tableNew(const dx::JSON &input_params);


dx::JSON tableAddColumns(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableAddColumns(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableAddIndices(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableAddIndices(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableAddRows(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableAddRows(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableAddTags(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableAddTags(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableAddTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableAddTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableClose(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableClose(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableDescribe(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableDescribe(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableGet(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableGet(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableGetDetails(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableGetDetails(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableListProjects(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableListProjects(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableRemoveColumns(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableRemoveColumns(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableRemoveIndices(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableRemoveIndices(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableRemoveRows(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableRemoveRows(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableRemoveTags(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableRemoveTags(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableRemoveTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableRemoveTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableRename(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableRename(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableSetDetails(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableSetDetails(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableSetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableSetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableSetVisibility(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableSetVisibility(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableUpdate(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableUpdate(const std::string &object_id, const dx::JSON &input_params);


#include "dxcpp.h"

#endif

