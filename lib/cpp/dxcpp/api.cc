
#include "api.h"


dx::JSON systemSearch(const std::string &input_params) {
  return DXHTTPRequest("/system/search", input_params);
}

dx::JSON systemSearch(const dx::JSON &input_params) {
  return systemSearch(input_params.toString());
}


dx::JSON userDescribe(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/describe"), input_params);
}

dx::JSON userDescribe(const std::string &object_id, const dx::JSON &input_params) {
  return userDescribe(object_id, input_params.toString());
}


dx::JSON userGetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getProperties"), input_params);
}

dx::JSON userGetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return userGetProperties(object_id, input_params.toString());
}


dx::JSON userSetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setProperties"), input_params);
}

dx::JSON userSetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return userSetProperties(object_id, input_params.toString());
}


dx::JSON userGetPermissions(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getPermissions"), input_params);
}

dx::JSON userGetPermissions(const std::string &object_id, const dx::JSON &input_params) {
  return userGetPermissions(object_id, input_params.toString());
}


dx::JSON userRevokePermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/revokePermission"), input_params);
}

dx::JSON userRevokePermission(const std::string &object_id, const dx::JSON &input_params) {
  return userRevokePermission(object_id, input_params.toString());
}


dx::JSON userGrantPermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/grantPermission"), input_params);
}

dx::JSON userGrantPermission(const std::string &object_id, const dx::JSON &input_params) {
  return userGrantPermission(object_id, input_params.toString());
}


dx::JSON userAddTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTypes"), input_params);
}

dx::JSON userAddTypes(const std::string &object_id, const dx::JSON &input_params) {
  return userAddTypes(object_id, input_params.toString());
}


dx::JSON userRemoveTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTypes"), input_params);
}

dx::JSON userRemoveTypes(const std::string &object_id, const dx::JSON &input_params) {
  return userRemoveTypes(object_id, input_params.toString());
}


dx::JSON groupNew(const std::string &input_params) {
  return DXHTTPRequest("/group/new", input_params);
}

dx::JSON groupNew(const dx::JSON &input_params) {
  return groupNew(input_params.toString());
}


dx::JSON groupDescribe(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/describe"), input_params);
}

dx::JSON groupDescribe(const std::string &object_id, const dx::JSON &input_params) {
  return groupDescribe(object_id, input_params.toString());
}


dx::JSON groupDestroy(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/destroy"), input_params);
}

dx::JSON groupDestroy(const std::string &object_id, const dx::JSON &input_params) {
  return groupDestroy(object_id, input_params.toString());
}


dx::JSON groupGetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getProperties"), input_params);
}

dx::JSON groupGetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return groupGetProperties(object_id, input_params.toString());
}


dx::JSON groupSetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setProperties"), input_params);
}

dx::JSON groupSetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return groupSetProperties(object_id, input_params.toString());
}


dx::JSON groupGetPermissions(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getPermissions"), input_params);
}

dx::JSON groupGetPermissions(const std::string &object_id, const dx::JSON &input_params) {
  return groupGetPermissions(object_id, input_params.toString());
}


dx::JSON groupRevokePermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/revokePermission"), input_params);
}

dx::JSON groupRevokePermission(const std::string &object_id, const dx::JSON &input_params) {
  return groupRevokePermission(object_id, input_params.toString());
}


dx::JSON groupGrantPermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/grantPermission"), input_params);
}

dx::JSON groupGrantPermission(const std::string &object_id, const dx::JSON &input_params) {
  return groupGrantPermission(object_id, input_params.toString());
}


dx::JSON groupAddMembers(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addMembers"), input_params);
}

dx::JSON groupAddMembers(const std::string &object_id, const dx::JSON &input_params) {
  return groupAddMembers(object_id, input_params.toString());
}


dx::JSON groupRemoveMembers(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeMembers"), input_params);
}

dx::JSON groupRemoveMembers(const std::string &object_id, const dx::JSON &input_params) {
  return groupRemoveMembers(object_id, input_params.toString());
}


dx::JSON groupAddTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTypes"), input_params);
}

dx::JSON groupAddTypes(const std::string &object_id, const dx::JSON &input_params) {
  return groupAddTypes(object_id, input_params.toString());
}


dx::JSON groupRemoveTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTypes"), input_params);
}

dx::JSON groupRemoveTypes(const std::string &object_id, const dx::JSON &input_params) {
  return groupRemoveTypes(object_id, input_params.toString());
}


dx::JSON jsonNew(const std::string &input_params) {
  return DXHTTPRequest("/json/new", input_params);
}

dx::JSON jsonNew(const dx::JSON &input_params) {
  return jsonNew(input_params.toString());
}


dx::JSON jsonDescribe(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/describe"), input_params);
}

dx::JSON jsonDescribe(const std::string &object_id, const dx::JSON &input_params) {
  return jsonDescribe(object_id, input_params.toString());
}


dx::JSON jsonDestroy(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/destroy"), input_params);
}

dx::JSON jsonDestroy(const std::string &object_id, const dx::JSON &input_params) {
  return jsonDestroy(object_id, input_params.toString());
}


dx::JSON jsonGetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getProperties"), input_params);
}

dx::JSON jsonGetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return jsonGetProperties(object_id, input_params.toString());
}


dx::JSON jsonSetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setProperties"), input_params);
}

dx::JSON jsonSetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return jsonSetProperties(object_id, input_params.toString());
}


dx::JSON jsonGetPermissions(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getPermissions"), input_params);
}

dx::JSON jsonGetPermissions(const std::string &object_id, const dx::JSON &input_params) {
  return jsonGetPermissions(object_id, input_params.toString());
}


dx::JSON jsonRevokePermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/revokePermission"), input_params);
}

dx::JSON jsonRevokePermission(const std::string &object_id, const dx::JSON &input_params) {
  return jsonRevokePermission(object_id, input_params.toString());
}


dx::JSON jsonGrantPermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/grantPermission"), input_params);
}

dx::JSON jsonGrantPermission(const std::string &object_id, const dx::JSON &input_params) {
  return jsonGrantPermission(object_id, input_params.toString());
}


dx::JSON jsonAddTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTypes"), input_params);
}

dx::JSON jsonAddTypes(const std::string &object_id, const dx::JSON &input_params) {
  return jsonAddTypes(object_id, input_params.toString());
}


dx::JSON jsonRemoveTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTypes"), input_params);
}

dx::JSON jsonRemoveTypes(const std::string &object_id, const dx::JSON &input_params) {
  return jsonRemoveTypes(object_id, input_params.toString());
}


dx::JSON jsonGet(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/get"), input_params);
}

dx::JSON jsonGet(const std::string &object_id, const dx::JSON &input_params) {
  return jsonGet(object_id, input_params.toString());
}


dx::JSON jsonSet(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/set"), input_params);
}

dx::JSON jsonSet(const std::string &object_id, const dx::JSON &input_params) {
  return jsonSet(object_id, input_params.toString());
}


dx::JSON collectionNew(const std::string &input_params) {
  return DXHTTPRequest("/collection/new", input_params);
}

dx::JSON collectionNew(const dx::JSON &input_params) {
  return collectionNew(input_params.toString());
}


dx::JSON collectionDescribe(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/describe"), input_params);
}

dx::JSON collectionDescribe(const std::string &object_id, const dx::JSON &input_params) {
  return collectionDescribe(object_id, input_params.toString());
}


dx::JSON collectionDestroy(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/destroy"), input_params);
}

dx::JSON collectionDestroy(const std::string &object_id, const dx::JSON &input_params) {
  return collectionDestroy(object_id, input_params.toString());
}


dx::JSON collectionGetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getProperties"), input_params);
}

dx::JSON collectionGetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return collectionGetProperties(object_id, input_params.toString());
}


dx::JSON collectionSetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setProperties"), input_params);
}

dx::JSON collectionSetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return collectionSetProperties(object_id, input_params.toString());
}


dx::JSON collectionGetPermissions(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getPermissions"), input_params);
}

dx::JSON collectionGetPermissions(const std::string &object_id, const dx::JSON &input_params) {
  return collectionGetPermissions(object_id, input_params.toString());
}


dx::JSON collectionRevokePermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/revokePermission"), input_params);
}

dx::JSON collectionRevokePermission(const std::string &object_id, const dx::JSON &input_params) {
  return collectionRevokePermission(object_id, input_params.toString());
}


dx::JSON collectionGrantPermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/grantPermission"), input_params);
}

dx::JSON collectionGrantPermission(const std::string &object_id, const dx::JSON &input_params) {
  return collectionGrantPermission(object_id, input_params.toString());
}


dx::JSON collectionAddTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTypes"), input_params);
}

dx::JSON collectionAddTypes(const std::string &object_id, const dx::JSON &input_params) {
  return collectionAddTypes(object_id, input_params.toString());
}


dx::JSON collectionRemoveTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTypes"), input_params);
}

dx::JSON collectionRemoveTypes(const std::string &object_id, const dx::JSON &input_params) {
  return collectionRemoveTypes(object_id, input_params.toString());
}


dx::JSON collectionGet(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/get"), input_params);
}

dx::JSON collectionGet(const std::string &object_id, const dx::JSON &input_params) {
  return collectionGet(object_id, input_params.toString());
}


dx::JSON fileNew(const std::string &input_params) {
  return DXHTTPRequest("/file/new", input_params);
}

dx::JSON fileNew(const dx::JSON &input_params) {
  return fileNew(input_params.toString());
}


dx::JSON fileDescribe(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/describe"), input_params);
}

dx::JSON fileDescribe(const std::string &object_id, const dx::JSON &input_params) {
  return fileDescribe(object_id, input_params.toString());
}


dx::JSON fileDestroy(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/destroy"), input_params);
}

dx::JSON fileDestroy(const std::string &object_id, const dx::JSON &input_params) {
  return fileDestroy(object_id, input_params.toString());
}


dx::JSON fileGetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getProperties"), input_params);
}

dx::JSON fileGetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return fileGetProperties(object_id, input_params.toString());
}


dx::JSON fileSetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setProperties"), input_params);
}

dx::JSON fileSetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return fileSetProperties(object_id, input_params.toString());
}


dx::JSON fileGetPermissions(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getPermissions"), input_params);
}

dx::JSON fileGetPermissions(const std::string &object_id, const dx::JSON &input_params) {
  return fileGetPermissions(object_id, input_params.toString());
}


dx::JSON fileRevokePermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/revokePermission"), input_params);
}

dx::JSON fileRevokePermission(const std::string &object_id, const dx::JSON &input_params) {
  return fileRevokePermission(object_id, input_params.toString());
}


dx::JSON fileGrantPermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/grantPermission"), input_params);
}

dx::JSON fileGrantPermission(const std::string &object_id, const dx::JSON &input_params) {
  return fileGrantPermission(object_id, input_params.toString());
}


dx::JSON fileAddTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTypes"), input_params);
}

dx::JSON fileAddTypes(const std::string &object_id, const dx::JSON &input_params) {
  return fileAddTypes(object_id, input_params.toString());
}


dx::JSON fileRemoveTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTypes"), input_params);
}

dx::JSON fileRemoveTypes(const std::string &object_id, const dx::JSON &input_params) {
  return fileRemoveTypes(object_id, input_params.toString());
}


dx::JSON fileUpload(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/upload"), input_params);
}

dx::JSON fileUpload(const std::string &object_id, const dx::JSON &input_params) {
  return fileUpload(object_id, input_params.toString());
}


dx::JSON fileClose(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/close"), input_params);
}

dx::JSON fileClose(const std::string &object_id, const dx::JSON &input_params) {
  return fileClose(object_id, input_params.toString());
}


dx::JSON fileDownload(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/download"), input_params);
}

dx::JSON fileDownload(const std::string &object_id, const dx::JSON &input_params) {
  return fileDownload(object_id, input_params.toString());
}


dx::JSON tableNew(const std::string &input_params) {
  return DXHTTPRequest("/table/new", input_params);
}

dx::JSON tableNew(const dx::JSON &input_params) {
  return tableNew(input_params.toString());
}


dx::JSON tableDescribe(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/describe"), input_params);
}

dx::JSON tableDescribe(const std::string &object_id, const dx::JSON &input_params) {
  return tableDescribe(object_id, input_params.toString());
}


dx::JSON tableExtend(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/extend"), input_params);
}

dx::JSON tableExtend(const std::string &object_id, const dx::JSON &input_params) {
  return tableExtend(object_id, input_params.toString());
}


dx::JSON tableDestroy(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/destroy"), input_params);
}

dx::JSON tableDestroy(const std::string &object_id, const dx::JSON &input_params) {
  return tableDestroy(object_id, input_params.toString());
}


dx::JSON tableGetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getProperties"), input_params);
}

dx::JSON tableGetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return tableGetProperties(object_id, input_params.toString());
}


dx::JSON tableSetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setProperties"), input_params);
}

dx::JSON tableSetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return tableSetProperties(object_id, input_params.toString());
}


dx::JSON tableGetPermissions(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getPermissions"), input_params);
}

dx::JSON tableGetPermissions(const std::string &object_id, const dx::JSON &input_params) {
  return tableGetPermissions(object_id, input_params.toString());
}


dx::JSON tableRevokePermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/revokePermission"), input_params);
}

dx::JSON tableRevokePermission(const std::string &object_id, const dx::JSON &input_params) {
  return tableRevokePermission(object_id, input_params.toString());
}


dx::JSON tableGrantPermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/grantPermission"), input_params);
}

dx::JSON tableGrantPermission(const std::string &object_id, const dx::JSON &input_params) {
  return tableGrantPermission(object_id, input_params.toString());
}


dx::JSON tableAddTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTypes"), input_params);
}

dx::JSON tableAddTypes(const std::string &object_id, const dx::JSON &input_params) {
  return tableAddTypes(object_id, input_params.toString());
}


dx::JSON tableRemoveTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTypes"), input_params);
}

dx::JSON tableRemoveTypes(const std::string &object_id, const dx::JSON &input_params) {
  return tableRemoveTypes(object_id, input_params.toString());
}


dx::JSON tableAddRows(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addRows"), input_params);
}

dx::JSON tableAddRows(const std::string &object_id, const dx::JSON &input_params) {
  return tableAddRows(object_id, input_params.toString());
}


dx::JSON tableClose(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/close"), input_params);
}

dx::JSON tableClose(const std::string &object_id, const dx::JSON &input_params) {
  return tableClose(object_id, input_params.toString());
}


dx::JSON tableGet(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/get"), input_params);
}

dx::JSON tableGet(const std::string &object_id, const dx::JSON &input_params) {
  return tableGet(object_id, input_params.toString());
}


dx::JSON appNew(const std::string &input_params) {
  return DXHTTPRequest("/app/new", input_params);
}

dx::JSON appNew(const dx::JSON &input_params) {
  return appNew(input_params.toString());
}


dx::JSON appDescribe(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/describe"), input_params);
}

dx::JSON appDescribe(const std::string &object_id, const dx::JSON &input_params) {
  return appDescribe(object_id, input_params.toString());
}


dx::JSON appDestroy(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/destroy"), input_params);
}

dx::JSON appDestroy(const std::string &object_id, const dx::JSON &input_params) {
  return appDestroy(object_id, input_params.toString());
}


dx::JSON appGetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getProperties"), input_params);
}

dx::JSON appGetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return appGetProperties(object_id, input_params.toString());
}


dx::JSON appSetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setProperties"), input_params);
}

dx::JSON appSetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return appSetProperties(object_id, input_params.toString());
}


dx::JSON appGetPermissions(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getPermissions"), input_params);
}

dx::JSON appGetPermissions(const std::string &object_id, const dx::JSON &input_params) {
  return appGetPermissions(object_id, input_params.toString());
}


dx::JSON appRevokePermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/revokePermission"), input_params);
}

dx::JSON appRevokePermission(const std::string &object_id, const dx::JSON &input_params) {
  return appRevokePermission(object_id, input_params.toString());
}


dx::JSON appGrantPermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/grantPermission"), input_params);
}

dx::JSON appGrantPermission(const std::string &object_id, const dx::JSON &input_params) {
  return appGrantPermission(object_id, input_params.toString());
}


dx::JSON appAddTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTypes"), input_params);
}

dx::JSON appAddTypes(const std::string &object_id, const dx::JSON &input_params) {
  return appAddTypes(object_id, input_params.toString());
}


dx::JSON appRemoveTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTypes"), input_params);
}

dx::JSON appRemoveTypes(const std::string &object_id, const dx::JSON &input_params) {
  return appRemoveTypes(object_id, input_params.toString());
}


dx::JSON appRun(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/run"), input_params);
}

dx::JSON appRun(const std::string &object_id, const dx::JSON &input_params) {
  return appRun(object_id, input_params.toString());
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


dx::JSON jobDestroy(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/destroy"), input_params);
}

dx::JSON jobDestroy(const std::string &object_id, const dx::JSON &input_params) {
  return jobDestroy(object_id, input_params.toString());
}


dx::JSON jobGetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getProperties"), input_params);
}

dx::JSON jobGetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return jobGetProperties(object_id, input_params.toString());
}


dx::JSON jobSetProperties(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/setProperties"), input_params);
}

dx::JSON jobSetProperties(const std::string &object_id, const dx::JSON &input_params) {
  return jobSetProperties(object_id, input_params.toString());
}


dx::JSON jobGetPermissions(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/getPermissions"), input_params);
}

dx::JSON jobGetPermissions(const std::string &object_id, const dx::JSON &input_params) {
  return jobGetPermissions(object_id, input_params.toString());
}


dx::JSON jobRevokePermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/revokePermission"), input_params);
}

dx::JSON jobRevokePermission(const std::string &object_id, const dx::JSON &input_params) {
  return jobRevokePermission(object_id, input_params.toString());
}


dx::JSON jobGrantPermission(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/grantPermission"), input_params);
}

dx::JSON jobGrantPermission(const std::string &object_id, const dx::JSON &input_params) {
  return jobGrantPermission(object_id, input_params.toString());
}


dx::JSON jobAddTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/addTypes"), input_params);
}

dx::JSON jobAddTypes(const std::string &object_id, const dx::JSON &input_params) {
  return jobAddTypes(object_id, input_params.toString());
}


dx::JSON jobRemoveTypes(const std::string &object_id, const std::string &input_params) {
  return DXHTTPRequest(std::string("/") + object_id + std::string("/removeTypes"), input_params);
}

dx::JSON jobRemoveTypes(const std::string &object_id, const dx::JSON &input_params) {
  return jobRemoveTypes(object_id, input_params.toString());
}

