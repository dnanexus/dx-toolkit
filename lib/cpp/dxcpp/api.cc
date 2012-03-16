
#include "api.h"


JSON systemSearch(const string &input_params) {
  return DXHTTPRequest("/system/search", input_params);
}

JSON systemSearch(const JSON &input_params) {
  return systemSearch(input_params.to_string());
}


JSON userDescribe(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/describe"), input_params);
}

JSON userDescribe(const string &object_id, const JSON &input_params) {
  return userDescribe(object_id, input_params.to_string());
}


JSON userGetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params);
}

JSON userGetProperties(const string &object_id, const JSON &input_params) {
  return userGetProperties(object_id, input_params.to_string());
}


JSON userSetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params);
}

JSON userSetProperties(const string &object_id, const JSON &input_params) {
  return userSetProperties(object_id, input_params.to_string());
}


JSON userGetPermissions(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params);
}

JSON userGetPermissions(const string &object_id, const JSON &input_params) {
  return userGetPermissions(object_id, input_params.to_string());
}


JSON userRevokePermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params);
}

JSON userRevokePermission(const string &object_id, const JSON &input_params) {
  return userRevokePermission(object_id, input_params.to_string());
}


JSON userGrantPermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params);
}

JSON userGrantPermission(const string &object_id, const JSON &input_params) {
  return userGrantPermission(object_id, input_params.to_string());
}


JSON userAddTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params);
}

JSON userAddTypes(const string &object_id, const JSON &input_params) {
  return userAddTypes(object_id, input_params.to_string());
}


JSON userRemoveTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params);
}

JSON userRemoveTypes(const string &object_id, const JSON &input_params) {
  return userRemoveTypes(object_id, input_params.to_string());
}


JSON groupNew(const string &input_params) {
  return DXHTTPRequest("/group/new", input_params);
}

JSON groupNew(const JSON &input_params) {
  return groupNew(input_params.to_string());
}


JSON groupDescribe(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/describe"), input_params);
}

JSON groupDescribe(const string &object_id, const JSON &input_params) {
  return groupDescribe(object_id, input_params.to_string());
}


JSON groupDestroy(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/destroy"), input_params);
}

JSON groupDestroy(const string &object_id, const JSON &input_params) {
  return groupDestroy(object_id, input_params.to_string());
}


JSON groupGetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params);
}

JSON groupGetProperties(const string &object_id, const JSON &input_params) {
  return groupGetProperties(object_id, input_params.to_string());
}


JSON groupSetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params);
}

JSON groupSetProperties(const string &object_id, const JSON &input_params) {
  return groupSetProperties(object_id, input_params.to_string());
}


JSON groupGetPermissions(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params);
}

JSON groupGetPermissions(const string &object_id, const JSON &input_params) {
  return groupGetPermissions(object_id, input_params.to_string());
}


JSON groupRevokePermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params);
}

JSON groupRevokePermission(const string &object_id, const JSON &input_params) {
  return groupRevokePermission(object_id, input_params.to_string());
}


JSON groupGrantPermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params);
}

JSON groupGrantPermission(const string &object_id, const JSON &input_params) {
  return groupGrantPermission(object_id, input_params.to_string());
}


JSON groupAddMembers(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/addMembers"), input_params);
}

JSON groupAddMembers(const string &object_id, const JSON &input_params) {
  return groupAddMembers(object_id, input_params.to_string());
}


JSON groupRemoveMembers(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/removeMembers"), input_params);
}

JSON groupRemoveMembers(const string &object_id, const JSON &input_params) {
  return groupRemoveMembers(object_id, input_params.to_string());
}


JSON groupAddTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params);
}

JSON groupAddTypes(const string &object_id, const JSON &input_params) {
  return groupAddTypes(object_id, input_params.to_string());
}


JSON groupRemoveTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params);
}

JSON groupRemoveTypes(const string &object_id, const JSON &input_params) {
  return groupRemoveTypes(object_id, input_params.to_string());
}


JSON jsonNew(const string &input_params) {
  return DXHTTPRequest("/json/new", input_params);
}

JSON jsonNew(const JSON &input_params) {
  return jsonNew(input_params.to_string());
}


JSON jsonDescribe(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/describe"), input_params);
}

JSON jsonDescribe(const string &object_id, const JSON &input_params) {
  return jsonDescribe(object_id, input_params.to_string());
}


JSON jsonDestroy(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/destroy"), input_params);
}

JSON jsonDestroy(const string &object_id, const JSON &input_params) {
  return jsonDestroy(object_id, input_params.to_string());
}


JSON jsonGetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params);
}

JSON jsonGetProperties(const string &object_id, const JSON &input_params) {
  return jsonGetProperties(object_id, input_params.to_string());
}


JSON jsonSetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params);
}

JSON jsonSetProperties(const string &object_id, const JSON &input_params) {
  return jsonSetProperties(object_id, input_params.to_string());
}


JSON jsonGetPermissions(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params);
}

JSON jsonGetPermissions(const string &object_id, const JSON &input_params) {
  return jsonGetPermissions(object_id, input_params.to_string());
}


JSON jsonRevokePermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params);
}

JSON jsonRevokePermission(const string &object_id, const JSON &input_params) {
  return jsonRevokePermission(object_id, input_params.to_string());
}


JSON jsonGrantPermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params);
}

JSON jsonGrantPermission(const string &object_id, const JSON &input_params) {
  return jsonGrantPermission(object_id, input_params.to_string());
}


JSON jsonAddTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params);
}

JSON jsonAddTypes(const string &object_id, const JSON &input_params) {
  return jsonAddTypes(object_id, input_params.to_string());
}


JSON jsonRemoveTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params);
}

JSON jsonRemoveTypes(const string &object_id, const JSON &input_params) {
  return jsonRemoveTypes(object_id, input_params.to_string());
}


JSON jsonGet(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/get"), input_params);
}

JSON jsonGet(const string &object_id, const JSON &input_params) {
  return jsonGet(object_id, input_params.to_string());
}


JSON jsonSet(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/set"), input_params);
}

JSON jsonSet(const string &object_id, const JSON &input_params) {
  return jsonSet(object_id, input_params.to_string());
}


JSON collectionNew(const string &input_params) {
  return DXHTTPRequest("/collection/new", input_params);
}

JSON collectionNew(const JSON &input_params) {
  return collectionNew(input_params.to_string());
}


JSON collectionDescribe(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/describe"), input_params);
}

JSON collectionDescribe(const string &object_id, const JSON &input_params) {
  return collectionDescribe(object_id, input_params.to_string());
}


JSON collectionDestroy(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/destroy"), input_params);
}

JSON collectionDestroy(const string &object_id, const JSON &input_params) {
  return collectionDestroy(object_id, input_params.to_string());
}


JSON collectionGetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params);
}

JSON collectionGetProperties(const string &object_id, const JSON &input_params) {
  return collectionGetProperties(object_id, input_params.to_string());
}


JSON collectionSetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params);
}

JSON collectionSetProperties(const string &object_id, const JSON &input_params) {
  return collectionSetProperties(object_id, input_params.to_string());
}


JSON collectionGetPermissions(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params);
}

JSON collectionGetPermissions(const string &object_id, const JSON &input_params) {
  return collectionGetPermissions(object_id, input_params.to_string());
}


JSON collectionRevokePermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params);
}

JSON collectionRevokePermission(const string &object_id, const JSON &input_params) {
  return collectionRevokePermission(object_id, input_params.to_string());
}


JSON collectionGrantPermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params);
}

JSON collectionGrantPermission(const string &object_id, const JSON &input_params) {
  return collectionGrantPermission(object_id, input_params.to_string());
}


JSON collectionAddTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params);
}

JSON collectionAddTypes(const string &object_id, const JSON &input_params) {
  return collectionAddTypes(object_id, input_params.to_string());
}


JSON collectionRemoveTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params);
}

JSON collectionRemoveTypes(const string &object_id, const JSON &input_params) {
  return collectionRemoveTypes(object_id, input_params.to_string());
}


JSON collectionGet(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/get"), input_params);
}

JSON collectionGet(const string &object_id, const JSON &input_params) {
  return collectionGet(object_id, input_params.to_string());
}


JSON fileNew(const string &input_params) {
  return DXHTTPRequest("/file/new", input_params);
}

JSON fileNew(const JSON &input_params) {
  return fileNew(input_params.to_string());
}


JSON fileDescribe(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/describe"), input_params);
}

JSON fileDescribe(const string &object_id, const JSON &input_params) {
  return fileDescribe(object_id, input_params.to_string());
}


JSON fileDestroy(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/destroy"), input_params);
}

JSON fileDestroy(const string &object_id, const JSON &input_params) {
  return fileDestroy(object_id, input_params.to_string());
}


JSON fileGetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params);
}

JSON fileGetProperties(const string &object_id, const JSON &input_params) {
  return fileGetProperties(object_id, input_params.to_string());
}


JSON fileSetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params);
}

JSON fileSetProperties(const string &object_id, const JSON &input_params) {
  return fileSetProperties(object_id, input_params.to_string());
}


JSON fileGetPermissions(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params);
}

JSON fileGetPermissions(const string &object_id, const JSON &input_params) {
  return fileGetPermissions(object_id, input_params.to_string());
}


JSON fileRevokePermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params);
}

JSON fileRevokePermission(const string &object_id, const JSON &input_params) {
  return fileRevokePermission(object_id, input_params.to_string());
}


JSON fileGrantPermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params);
}

JSON fileGrantPermission(const string &object_id, const JSON &input_params) {
  return fileGrantPermission(object_id, input_params.to_string());
}


JSON fileAddTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params);
}

JSON fileAddTypes(const string &object_id, const JSON &input_params) {
  return fileAddTypes(object_id, input_params.to_string());
}


JSON fileRemoveTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params);
}

JSON fileRemoveTypes(const string &object_id, const JSON &input_params) {
  return fileRemoveTypes(object_id, input_params.to_string());
}


JSON fileUpload(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/upload"), input_params);
}

JSON fileUpload(const string &object_id, const JSON &input_params) {
  return fileUpload(object_id, input_params.to_string());
}


JSON fileClose(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/close"), input_params);
}

JSON fileClose(const string &object_id, const JSON &input_params) {
  return fileClose(object_id, input_params.to_string());
}


JSON fileDownload(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/download"), input_params);
}

JSON fileDownload(const string &object_id, const JSON &input_params) {
  return fileDownload(object_id, input_params.to_string());
}


JSON tableNew(const string &input_params) {
  return DXHTTPRequest("/table/new", input_params);
}

JSON tableNew(const JSON &input_params) {
  return tableNew(input_params.to_string());
}


JSON tableDescribe(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/describe"), input_params);
}

JSON tableDescribe(const string &object_id, const JSON &input_params) {
  return tableDescribe(object_id, input_params.to_string());
}


JSON tableExtend(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/extend"), input_params);
}

JSON tableExtend(const string &object_id, const JSON &input_params) {
  return tableExtend(object_id, input_params.to_string());
}


JSON tableDestroy(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/destroy"), input_params);
}

JSON tableDestroy(const string &object_id, const JSON &input_params) {
  return tableDestroy(object_id, input_params.to_string());
}


JSON tableGetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params);
}

JSON tableGetProperties(const string &object_id, const JSON &input_params) {
  return tableGetProperties(object_id, input_params.to_string());
}


JSON tableSetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params);
}

JSON tableSetProperties(const string &object_id, const JSON &input_params) {
  return tableSetProperties(object_id, input_params.to_string());
}


JSON tableGetPermissions(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params);
}

JSON tableGetPermissions(const string &object_id, const JSON &input_params) {
  return tableGetPermissions(object_id, input_params.to_string());
}


JSON tableRevokePermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params);
}

JSON tableRevokePermission(const string &object_id, const JSON &input_params) {
  return tableRevokePermission(object_id, input_params.to_string());
}


JSON tableGrantPermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params);
}

JSON tableGrantPermission(const string &object_id, const JSON &input_params) {
  return tableGrantPermission(object_id, input_params.to_string());
}


JSON tableAddTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params);
}

JSON tableAddTypes(const string &object_id, const JSON &input_params) {
  return tableAddTypes(object_id, input_params.to_string());
}


JSON tableRemoveTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params);
}

JSON tableRemoveTypes(const string &object_id, const JSON &input_params) {
  return tableRemoveTypes(object_id, input_params.to_string());
}


JSON tableAddRows(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/addRows"), input_params);
}

JSON tableAddRows(const string &object_id, const JSON &input_params) {
  return tableAddRows(object_id, input_params.to_string());
}


JSON tableClose(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/close"), input_params);
}

JSON tableClose(const string &object_id, const JSON &input_params) {
  return tableClose(object_id, input_params.to_string());
}


JSON tableGet(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/get"), input_params);
}

JSON tableGet(const string &object_id, const JSON &input_params) {
  return tableGet(object_id, input_params.to_string());
}


JSON appNew(const string &input_params) {
  return DXHTTPRequest("/app/new", input_params);
}

JSON appNew(const JSON &input_params) {
  return appNew(input_params.to_string());
}


JSON appDescribe(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/describe"), input_params);
}

JSON appDescribe(const string &object_id, const JSON &input_params) {
  return appDescribe(object_id, input_params.to_string());
}


JSON appDestroy(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/destroy"), input_params);
}

JSON appDestroy(const string &object_id, const JSON &input_params) {
  return appDestroy(object_id, input_params.to_string());
}


JSON appGetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params);
}

JSON appGetProperties(const string &object_id, const JSON &input_params) {
  return appGetProperties(object_id, input_params.to_string());
}


JSON appSetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params);
}

JSON appSetProperties(const string &object_id, const JSON &input_params) {
  return appSetProperties(object_id, input_params.to_string());
}


JSON appGetPermissions(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params);
}

JSON appGetPermissions(const string &object_id, const JSON &input_params) {
  return appGetPermissions(object_id, input_params.to_string());
}


JSON appRevokePermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params);
}

JSON appRevokePermission(const string &object_id, const JSON &input_params) {
  return appRevokePermission(object_id, input_params.to_string());
}


JSON appGrantPermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params);
}

JSON appGrantPermission(const string &object_id, const JSON &input_params) {
  return appGrantPermission(object_id, input_params.to_string());
}


JSON appAddTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params);
}

JSON appAddTypes(const string &object_id, const JSON &input_params) {
  return appAddTypes(object_id, input_params.to_string());
}


JSON appRemoveTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params);
}

JSON appRemoveTypes(const string &object_id, const JSON &input_params) {
  return appRemoveTypes(object_id, input_params.to_string());
}


JSON appRun(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/run"), input_params);
}

JSON appRun(const string &object_id, const JSON &input_params) {
  return appRun(object_id, input_params.to_string());
}


JSON jobNew(const string &input_params) {
  return DXHTTPRequest("/job/new", input_params);
}

JSON jobNew(const JSON &input_params) {
  return jobNew(input_params.to_string());
}


JSON jobDescribe(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/describe"), input_params);
}

JSON jobDescribe(const string &object_id, const JSON &input_params) {
  return jobDescribe(object_id, input_params.to_string());
}


JSON jobDestroy(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/destroy"), input_params);
}

JSON jobDestroy(const string &object_id, const JSON &input_params) {
  return jobDestroy(object_id, input_params.to_string());
}


JSON jobGetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params);
}

JSON jobGetProperties(const string &object_id, const JSON &input_params) {
  return jobGetProperties(object_id, input_params.to_string());
}


JSON jobSetProperties(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params);
}

JSON jobSetProperties(const string &object_id, const JSON &input_params) {
  return jobSetProperties(object_id, input_params.to_string());
}


JSON jobGetPermissions(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params);
}

JSON jobGetPermissions(const string &object_id, const JSON &input_params) {
  return jobGetPermissions(object_id, input_params.to_string());
}


JSON jobRevokePermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params);
}

JSON jobRevokePermission(const string &object_id, const JSON &input_params) {
  return jobRevokePermission(object_id, input_params.to_string());
}


JSON jobGrantPermission(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params);
}

JSON jobGrantPermission(const string &object_id, const JSON &input_params) {
  return jobGrantPermission(object_id, input_params.to_string());
}


JSON jobAddTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params);
}

JSON jobAddTypes(const string &object_id, const JSON &input_params) {
  return jobAddTypes(object_id, input_params.to_string());
}


JSON jobRemoveTypes(const string &object_id, const string &input_params) {
  return DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params);
}

JSON jobRemoveTypes(const string &object_id, const JSON &input_params) {
  return jobRemoveTypes(object_id, input_params.to_string());
}

