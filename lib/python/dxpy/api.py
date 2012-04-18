
from dxpy import DXHTTPRequest


def systemSearch(input_params={}, **kwargs):
    return DXHTTPRequest('/system/search', input_params, **kwargs)


def projectNew(input_params={}, **kwargs):
    return DXHTTPRequest('/project/new', input_params, **kwargs)


def projectDescribe(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/describe' % object_id, input_params, **kwargs)


def projectDestroy(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/destroy' % object_id, input_params, **kwargs)


def projectUpdate(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/update' % object_id, input_params, **kwargs)


def projectNewFolder(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/newFolder' % object_id, input_params, **kwargs)


def projectListFolder(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/listFolder' % object_id, input_params, **kwargs)


def projectRemoveFolder(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeFolder' % object_id, input_params, **kwargs)


def projectMove(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/move' % object_id, input_params, **kwargs)


def projectRemoveObjects(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeObjects' % object_id, input_params, **kwargs)


def projectClone(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/clone' % object_id, input_params, **kwargs)


def recordNew(input_params={}, **kwargs):
    return DXHTTPRequest('/record/new', input_params, **kwargs)


def recordDescribe(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/describe' % object_id, input_params, **kwargs)


def recordSetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setProperties' % object_id, input_params, **kwargs)


def recordAddTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTypes' % object_id, input_params, **kwargs)


def recordRemoveTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTypes' % object_id, input_params, **kwargs)


def recordAddTags(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTags' % object_id, input_params, **kwargs)


def recordRemoveTags(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTags' % object_id, input_params, **kwargs)


def recordListProjects(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/listProjects' % object_id, input_params, **kwargs)


def recordGetDetails(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getDetails' % object_id, input_params, **kwargs)


def recordSetDetails(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setDetails' % object_id, input_params, **kwargs)


def recordSetVisibility(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setVisibility' % object_id, input_params, **kwargs)


def recordRename(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/rename' % object_id, input_params, **kwargs)


def recordClose(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/close' % object_id, input_params, **kwargs)


def tableNew(input_params={}, **kwargs):
    return DXHTTPRequest('/table/new', input_params, **kwargs)


def tableDescribe(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/describe' % object_id, input_params, **kwargs)


def tableSetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setProperties' % object_id, input_params, **kwargs)


def tableAddTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTypes' % object_id, input_params, **kwargs)


def tableRemoveTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTypes' % object_id, input_params, **kwargs)


def tableAddTags(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTags' % object_id, input_params, **kwargs)


def tableRemoveTags(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTags' % object_id, input_params, **kwargs)


def tableListProjects(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/listProjects' % object_id, input_params, **kwargs)


def tableGetDetails(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getDetails' % object_id, input_params, **kwargs)


def tableSetDetails(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setDetails' % object_id, input_params, **kwargs)


def tableSetVisibility(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setVisibility' % object_id, input_params, **kwargs)


def tableRename(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/rename' % object_id, input_params, **kwargs)


def tableAddRows(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addRows' % object_id, input_params, **kwargs)


def tableRemoveRows(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeRows' % object_id, input_params, **kwargs)


def tableAddColumns(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addColumns' % object_id, input_params, **kwargs)


def tableRemoveColumns(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeColumns' % object_id, input_params, **kwargs)


def tableAddIndices(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addIndices' % object_id, input_params, **kwargs)


def tableRemoveIndices(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeIndices' % object_id, input_params, **kwargs)


def tableGet(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/get' % object_id, input_params, **kwargs)


def tableUpdate(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/update' % object_id, input_params, **kwargs)


def tableClose(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/close' % object_id, input_params, **kwargs)


def fileNew(input_params={}, **kwargs):
    return DXHTTPRequest('/file/new', input_params, **kwargs)


def fileDescribe(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/describe' % object_id, input_params, **kwargs)


def fileSetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setProperties' % object_id, input_params, **kwargs)


def fileAddTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTypes' % object_id, input_params, **kwargs)


def fileRemoveTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTypes' % object_id, input_params, **kwargs)


def fileAddTags(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTags' % object_id, input_params, **kwargs)


def fileRemoveTags(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTags' % object_id, input_params, **kwargs)


def fileListProjects(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/listProjects' % object_id, input_params, **kwargs)


def fileGetDetails(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getDetails' % object_id, input_params, **kwargs)


def fileSetDetails(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setDetails' % object_id, input_params, **kwargs)


def fileSetVisibility(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setVisibility' % object_id, input_params, **kwargs)


def fileRename(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/rename' % object_id, input_params, **kwargs)


def fileUpload(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/upload' % object_id, input_params, **kwargs)


def fileClose(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/close' % object_id, input_params, **kwargs)


def fileDownload(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/download' % object_id, input_params, **kwargs)


def gtableNew(input_params={}, **kwargs):
    return DXHTTPRequest('/gtable/new', input_params, **kwargs)


def gtableDescribe(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/describe' % object_id, input_params, **kwargs)


def gtableExtend(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/extend' % object_id, input_params, **kwargs)


def gtableSetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setProperties' % object_id, input_params, **kwargs)


def gtableAddTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTypes' % object_id, input_params, **kwargs)


def gtableRemoveTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTypes' % object_id, input_params, **kwargs)


def gtableAddTags(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTags' % object_id, input_params, **kwargs)


def gtableRemoveTags(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTags' % object_id, input_params, **kwargs)


def gtableListProjects(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/listProjects' % object_id, input_params, **kwargs)


def gtableGetDetails(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getDetails' % object_id, input_params, **kwargs)


def gtableSetDetails(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setDetails' % object_id, input_params, **kwargs)


def gtableSetVisibility(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setVisibility' % object_id, input_params, **kwargs)


def gtableAddRows(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addRows' % object_id, input_params, **kwargs)


def gtableClose(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/close' % object_id, input_params, **kwargs)


def gtableGet(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/get' % object_id, input_params, **kwargs)


def programNew(input_params={}, **kwargs):
    return DXHTTPRequest('/program/new', input_params, **kwargs)


def programDescribe(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/describe' % object_id, input_params, **kwargs)


def programSetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setProperties' % object_id, input_params, **kwargs)


def programAddTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTypes' % object_id, input_params, **kwargs)


def programRemoveTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTypes' % object_id, input_params, **kwargs)


def programAddTags(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTags' % object_id, input_params, **kwargs)


def programRemoveTags(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTags' % object_id, input_params, **kwargs)


def programListProjects(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/listProjects' % object_id, input_params, **kwargs)


def programGetDetails(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getDetails' % object_id, input_params, **kwargs)


def programSetDetails(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setDetails' % object_id, input_params, **kwargs)


def programSetVisibility(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setVisibility' % object_id, input_params, **kwargs)


def programRename(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/rename' % object_id, input_params, **kwargs)


def programClose(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/close' % object_id, input_params, **kwargs)


def programGet(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/get' % object_id, input_params, **kwargs)


def programRun(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/run' % object_id, input_params, **kwargs)


def jobNew(input_params={}, **kwargs):
    return DXHTTPRequest('/job/new', input_params, **kwargs)


def jobDescribe(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/describe' % object_id, input_params, **kwargs)


def jobTerminate(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/terminate' % object_id, input_params, **kwargs)

