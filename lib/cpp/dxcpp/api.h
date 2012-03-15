#ifndef DXCPP_API_H
#define DXCPP_API_H

#include "json.h"
#include "dxcpp.h"

namespace dxpy {


  JSON systemSearch(JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest("/system/search", input_params.to_string());
  }


  JSON userDescribe(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/describe"), input_params.to_string());
  }


  JSON userGetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params.to_string());
  }


  JSON userSetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params.to_string());
  }


  JSON userGetPermissions(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params.to_string());
  }


  JSON userRevokePermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params.to_string());
  }


  JSON userGrantPermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params.to_string());
  }


  JSON userAddTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params.to_string());
  }


  JSON userRemoveTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params.to_string());
  }


  JSON groupNew(JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest("/group/new", input_params.to_string());
  }


  JSON groupDescribe(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/describe"), input_params.to_string());
  }


  JSON groupDestroy(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/destroy"), input_params.to_string());
  }


  JSON groupGetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params.to_string());
  }


  JSON groupSetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params.to_string());
  }


  JSON groupGetPermissions(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params.to_string());
  }


  JSON groupRevokePermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params.to_string());
  }


  JSON groupGrantPermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params.to_string());
  }


  JSON groupAddMembers(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/addMembers"), input_params.to_string());
  }


  JSON groupRemoveMembers(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/removeMembers"), input_params.to_string());
  }


  JSON groupAddTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params.to_string());
  }


  JSON groupRemoveTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params.to_string());
  }


  JSON jsonNew(JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest("/json/new", input_params.to_string());
  }


  JSON jsonDescribe(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/describe"), input_params.to_string());
  }


  JSON jsonDestroy(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/destroy"), input_params.to_string());
  }


  JSON jsonGetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params.to_string());
  }


  JSON jsonSetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params.to_string());
  }


  JSON jsonGetPermissions(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params.to_string());
  }


  JSON jsonRevokePermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params.to_string());
  }


  JSON jsonGrantPermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params.to_string());
  }


  JSON jsonAddTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params.to_string());
  }


  JSON jsonRemoveTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params.to_string());
  }


  JSON jsonGet(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/get"), input_params.to_string());
  }


  JSON jsonSet(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/set"), input_params.to_string());
  }


  JSON collectionNew(JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest("/collection/new", input_params.to_string());
  }


  JSON collectionDescribe(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/describe"), input_params.to_string());
  }


  JSON collectionDestroy(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/destroy"), input_params.to_string());
  }


  JSON collectionGetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params.to_string());
  }


  JSON collectionSetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params.to_string());
  }


  JSON collectionGetPermissions(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params.to_string());
  }


  JSON collectionRevokePermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params.to_string());
  }


  JSON collectionGrantPermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params.to_string());
  }


  JSON collectionAddTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params.to_string());
  }


  JSON collectionRemoveTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params.to_string());
  }


  JSON collectionGet(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/get"), input_params.to_string());
  }


  JSON fileNew(JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest("/file/new", input_params.to_string());
  }


  JSON fileDescribe(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/describe"), input_params.to_string());
  }


  JSON fileDestroy(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/destroy"), input_params.to_string());
  }


  JSON fileGetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params.to_string());
  }


  JSON fileSetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params.to_string());
  }


  JSON fileGetPermissions(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params.to_string());
  }


  JSON fileRevokePermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params.to_string());
  }


  JSON fileGrantPermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params.to_string());
  }


  JSON fileAddTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params.to_string());
  }


  JSON fileRemoveTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params.to_string());
  }


  JSON fileUpload(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/upload"), input_params.to_string());
  }


  JSON fileClose(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/close"), input_params.to_string());
  }


  JSON fileDownload(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/download"), input_params.to_string());
  }


  JSON tableNew(JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest("/table/new", input_params.to_string());
  }


  JSON tableDescribe(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/describe"), input_params.to_string());
  }


  JSON tableExtend(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/extend"), input_params.to_string());
  }


  JSON tableDestroy(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/destroy"), input_params.to_string());
  }


  JSON tableGetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params.to_string());
  }


  JSON tableSetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params.to_string());
  }


  JSON tableGetPermissions(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params.to_string());
  }


  JSON tableRevokePermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params.to_string());
  }


  JSON tableGrantPermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params.to_string());
  }


  JSON tableAddTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params.to_string());
  }


  JSON tableRemoveTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params.to_string());
  }


  JSON tableAddRows(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/addRows"), input_params.to_string());
  }


  JSON tableClose(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/close"), input_params.to_string());
  }


  JSON tableGet(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/get"), input_params.to_string());
  }


  JSON appNew(JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest("/app/new", input_params.to_string());
  }


  JSON appDescribe(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/describe"), input_params.to_string());
  }


  JSON appDestroy(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/destroy"), input_params.to_string());
  }


  JSON appGetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params.to_string());
  }


  JSON appSetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params.to_string());
  }


  JSON appGetPermissions(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params.to_string());
  }


  JSON appRevokePermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params.to_string());
  }


  JSON appGrantPermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params.to_string());
  }


  JSON appAddTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params.to_string());
  }


  JSON appRemoveTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params.to_string());
  }


  JSON appRun(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/run"), input_params.to_string());
  }


  JSON jobNew(JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest("/job/new", input_params.to_string());
  }


  JSON jobDescribe(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/describe"), input_params.to_string());
  }


  JSON jobDestroy(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/destroy"), input_params.to_string());
  }


  JSON jobGetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getProperties"), input_params.to_string());
  }


  JSON jobSetProperties(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/setProperties"), input_params.to_string());
  }


  JSON jobGetPermissions(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/getPermissions"), input_params.to_string());
  }


  JSON jobRevokePermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/revokePermission"), input_params.to_string());
  }


  JSON jobGrantPermission(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/grantPermission"), input_params.to_string());
  }


  JSON jobAddTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/addTypes"), input_params.to_string());
  }


  JSON jobRemoveTypes(string object_id, JSON input_params=JSON("{}")) {
    return dxpy::DXHTTPRequest(string("/") + object_id + string("/removeTypes"), input_params.to_string());
  }


}

#endif

