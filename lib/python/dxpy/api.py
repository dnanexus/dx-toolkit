
from dxpy import DXHTTPRequest


def systemSearch(input_params={}, **kwargs):
    return DXHTTPRequest('/system/search', input_params, **kwargs)


def userDescribe(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/describe' % object_id, input_params, **kwargs)


def userGetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getProperties' % object_id, input_params, **kwargs)


def userSetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setProperties' % object_id, input_params, **kwargs)


def userGetPermissions(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getPermissions' % object_id, input_params, **kwargs)


def userRevokePermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/revokePermission' % object_id, input_params, **kwargs)


def userGrantPermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/grantPermission' % object_id, input_params, **kwargs)


def userAddTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTypes' % object_id, input_params, **kwargs)


def userRemoveTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTypes' % object_id, input_params, **kwargs)


def groupNew(input_params={}, **kwargs):
    return DXHTTPRequest('/group/new', input_params, **kwargs)


def groupDescribe(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/describe' % object_id, input_params, **kwargs)


def groupDestroy(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/destroy' % object_id, input_params, **kwargs)


def groupGetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getProperties' % object_id, input_params, **kwargs)


def groupSetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setProperties' % object_id, input_params, **kwargs)


def groupGetPermissions(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getPermissions' % object_id, input_params, **kwargs)


def groupRevokePermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/revokePermission' % object_id, input_params, **kwargs)


def groupGrantPermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/grantPermission' % object_id, input_params, **kwargs)


def groupAddMembers(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addMembers' % object_id, input_params, **kwargs)


def groupRemoveMembers(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeMembers' % object_id, input_params, **kwargs)


def groupAddTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTypes' % object_id, input_params, **kwargs)


def groupRemoveTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTypes' % object_id, input_params, **kwargs)


def jsonNew(input_params={}, **kwargs):
    return DXHTTPRequest('/json/new', input_params, **kwargs)


def jsonDescribe(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/describe' % object_id, input_params, **kwargs)


def jsonDestroy(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/destroy' % object_id, input_params, **kwargs)


def jsonGetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getProperties' % object_id, input_params, **kwargs)


def jsonSetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setProperties' % object_id, input_params, **kwargs)


def jsonGetPermissions(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getPermissions' % object_id, input_params, **kwargs)


def jsonRevokePermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/revokePermission' % object_id, input_params, **kwargs)


def jsonGrantPermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/grantPermission' % object_id, input_params, **kwargs)


def jsonAddTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTypes' % object_id, input_params, **kwargs)


def jsonRemoveTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTypes' % object_id, input_params, **kwargs)


def jsonGet(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/get' % object_id, input_params, **kwargs)


def jsonSet(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/set' % object_id, input_params, **kwargs)


def collectionNew(input_params={}, **kwargs):
    return DXHTTPRequest('/collection/new', input_params, **kwargs)


def collectionDescribe(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/describe' % object_id, input_params, **kwargs)


def collectionDestroy(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/destroy' % object_id, input_params, **kwargs)


def collectionGetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getProperties' % object_id, input_params, **kwargs)


def collectionSetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setProperties' % object_id, input_params, **kwargs)


def collectionGetPermissions(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getPermissions' % object_id, input_params, **kwargs)


def collectionRevokePermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/revokePermission' % object_id, input_params, **kwargs)


def collectionGrantPermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/grantPermission' % object_id, input_params, **kwargs)


def collectionAddTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTypes' % object_id, input_params, **kwargs)


def collectionRemoveTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTypes' % object_id, input_params, **kwargs)


def collectionGet(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/get' % object_id, input_params, **kwargs)


def fileNew(input_params={}, **kwargs):
    return DXHTTPRequest('/file/new', input_params, **kwargs)


def fileDescribe(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/describe' % object_id, input_params, **kwargs)


def fileDestroy(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/destroy' % object_id, input_params, **kwargs)


def fileGetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getProperties' % object_id, input_params, **kwargs)


def fileSetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setProperties' % object_id, input_params, **kwargs)


def fileGetPermissions(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getPermissions' % object_id, input_params, **kwargs)


def fileRevokePermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/revokePermission' % object_id, input_params, **kwargs)


def fileGrantPermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/grantPermission' % object_id, input_params, **kwargs)


def fileAddTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTypes' % object_id, input_params, **kwargs)


def fileRemoveTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTypes' % object_id, input_params, **kwargs)


def fileUpload(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/upload' % object_id, input_params, **kwargs)


def fileClose(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/close' % object_id, input_params, **kwargs)


def fileDownload(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/download' % object_id, input_params, **kwargs)


def tableNew(input_params={}, **kwargs):
    return DXHTTPRequest('/table/new', input_params, **kwargs)


def tableDescribe(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/describe' % object_id, input_params, **kwargs)


def tableExtend(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/extend' % object_id, input_params, **kwargs)


def tableDestroy(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/destroy' % object_id, input_params, **kwargs)


def tableGetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getProperties' % object_id, input_params, **kwargs)


def tableSetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setProperties' % object_id, input_params, **kwargs)


def tableGetPermissions(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getPermissions' % object_id, input_params, **kwargs)


def tableRevokePermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/revokePermission' % object_id, input_params, **kwargs)


def tableGrantPermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/grantPermission' % object_id, input_params, **kwargs)


def tableAddTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTypes' % object_id, input_params, **kwargs)


def tableRemoveTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTypes' % object_id, input_params, **kwargs)


def tableAddRows(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addRows' % object_id, input_params, **kwargs)


def tableClose(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/close' % object_id, input_params, **kwargs)


def tableGet(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/get' % object_id, input_params, **kwargs)


def appNew(input_params={}, **kwargs):
    return DXHTTPRequest('/app/new', input_params, **kwargs)


def appDescribe(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/describe' % object_id, input_params, **kwargs)


def appDestroy(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/destroy' % object_id, input_params, **kwargs)


def appGetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getProperties' % object_id, input_params, **kwargs)


def appSetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setProperties' % object_id, input_params, **kwargs)


def appGetPermissions(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getPermissions' % object_id, input_params, **kwargs)


def appRevokePermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/revokePermission' % object_id, input_params, **kwargs)


def appGrantPermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/grantPermission' % object_id, input_params, **kwargs)


def appAddTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTypes' % object_id, input_params, **kwargs)


def appRemoveTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTypes' % object_id, input_params, **kwargs)


def appRun(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/run' % object_id, input_params, **kwargs)


def jobNew(input_params={}, **kwargs):
    return DXHTTPRequest('/job/new', input_params, **kwargs)


def jobDescribe(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/describe' % object_id, input_params, **kwargs)


def jobDestroy(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/destroy' % object_id, input_params, **kwargs)


def jobGetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getProperties' % object_id, input_params, **kwargs)


def jobSetProperties(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/setProperties' % object_id, input_params, **kwargs)


def jobGetPermissions(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/getPermissions' % object_id, input_params, **kwargs)


def jobRevokePermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/revokePermission' % object_id, input_params, **kwargs)


def jobGrantPermission(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/grantPermission' % object_id, input_params, **kwargs)


def jobAddTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/addTypes' % object_id, input_params, **kwargs)


def jobRemoveTypes(object_id, input_params={}, **kwargs):
    return DXHTTPRequest('/%s/removeTypes' % object_id, input_params, **kwargs)

