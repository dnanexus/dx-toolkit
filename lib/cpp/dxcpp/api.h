#ifndef DXCPP_API_H
#define DXCPP_API_H

#include "dxjson/dxjson.h"


dx::JSON systemSearch(const std::string &input_params="{}");
dx::JSON systemSearch(const dx::JSON &input_params);


dx::JSON userDescribe(const std::string &object_id, const std::string &input_params="{}");
dx::JSON userDescribe(const std::string &object_id, const dx::JSON &input_params);


dx::JSON userGetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON userGetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON userSetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON userSetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON userGetPermissions(const std::string &object_id, const std::string &input_params="{}");
dx::JSON userGetPermissions(const std::string &object_id, const dx::JSON &input_params);


dx::JSON userRevokePermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON userRevokePermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON userGrantPermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON userGrantPermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON userAddTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON userAddTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON userRemoveTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON userRemoveTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON groupNew(const std::string &input_params="{}");
dx::JSON groupNew(const dx::JSON &input_params);


dx::JSON groupDescribe(const std::string &object_id, const std::string &input_params="{}");
dx::JSON groupDescribe(const std::string &object_id, const dx::JSON &input_params);


dx::JSON groupDestroy(const std::string &object_id, const std::string &input_params="{}");
dx::JSON groupDestroy(const std::string &object_id, const dx::JSON &input_params);


dx::JSON groupGetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON groupGetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON groupSetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON groupSetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON groupGetPermissions(const std::string &object_id, const std::string &input_params="{}");
dx::JSON groupGetPermissions(const std::string &object_id, const dx::JSON &input_params);


dx::JSON groupRevokePermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON groupRevokePermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON groupGrantPermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON groupGrantPermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON groupAddMembers(const std::string &object_id, const std::string &input_params="{}");
dx::JSON groupAddMembers(const std::string &object_id, const dx::JSON &input_params);


dx::JSON groupRemoveMembers(const std::string &object_id, const std::string &input_params="{}");
dx::JSON groupRemoveMembers(const std::string &object_id, const dx::JSON &input_params);


dx::JSON groupAddTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON groupAddTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON groupRemoveTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON groupRemoveTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jsonNew(const std::string &input_params="{}");
dx::JSON jsonNew(const dx::JSON &input_params);


dx::JSON jsonDescribe(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jsonDescribe(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jsonDestroy(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jsonDestroy(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jsonGetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jsonGetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jsonSetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jsonSetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jsonGetPermissions(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jsonGetPermissions(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jsonRevokePermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jsonRevokePermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jsonGrantPermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jsonGrantPermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jsonAddTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jsonAddTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jsonRemoveTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jsonRemoveTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jsonGet(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jsonGet(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jsonSet(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jsonSet(const std::string &object_id, const dx::JSON &input_params);


dx::JSON collectionNew(const std::string &input_params="{}");
dx::JSON collectionNew(const dx::JSON &input_params);


dx::JSON collectionDescribe(const std::string &object_id, const std::string &input_params="{}");
dx::JSON collectionDescribe(const std::string &object_id, const dx::JSON &input_params);


dx::JSON collectionDestroy(const std::string &object_id, const std::string &input_params="{}");
dx::JSON collectionDestroy(const std::string &object_id, const dx::JSON &input_params);


dx::JSON collectionGetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON collectionGetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON collectionSetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON collectionSetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON collectionGetPermissions(const std::string &object_id, const std::string &input_params="{}");
dx::JSON collectionGetPermissions(const std::string &object_id, const dx::JSON &input_params);


dx::JSON collectionRevokePermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON collectionRevokePermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON collectionGrantPermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON collectionGrantPermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON collectionAddTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON collectionAddTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON collectionRemoveTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON collectionRemoveTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON collectionGet(const std::string &object_id, const std::string &input_params="{}");
dx::JSON collectionGet(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileNew(const std::string &input_params="{}");
dx::JSON fileNew(const dx::JSON &input_params);


dx::JSON fileDescribe(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileDescribe(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileDestroy(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileDestroy(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileGetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileGetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileSetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileSetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileGetPermissions(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileGetPermissions(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileRevokePermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileRevokePermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileGrantPermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileGrantPermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileAddTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileAddTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileRemoveTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileRemoveTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileUpload(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileUpload(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileClose(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileClose(const std::string &object_id, const dx::JSON &input_params);


dx::JSON fileDownload(const std::string &object_id, const std::string &input_params="{}");
dx::JSON fileDownload(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableNew(const std::string &input_params="{}");
dx::JSON tableNew(const dx::JSON &input_params);


dx::JSON tableDescribe(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableDescribe(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableExtend(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableExtend(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableDestroy(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableDestroy(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableGetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableGetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableSetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableSetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableGetPermissions(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableGetPermissions(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableRevokePermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableRevokePermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableGrantPermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableGrantPermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableAddTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableAddTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableRemoveTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableRemoveTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableAddRows(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableAddRows(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableClose(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableClose(const std::string &object_id, const dx::JSON &input_params);


dx::JSON tableGet(const std::string &object_id, const std::string &input_params="{}");
dx::JSON tableGet(const std::string &object_id, const dx::JSON &input_params);


dx::JSON appNew(const std::string &input_params="{}");
dx::JSON appNew(const dx::JSON &input_params);


dx::JSON appDescribe(const std::string &object_id, const std::string &input_params="{}");
dx::JSON appDescribe(const std::string &object_id, const dx::JSON &input_params);


dx::JSON appDestroy(const std::string &object_id, const std::string &input_params="{}");
dx::JSON appDestroy(const std::string &object_id, const dx::JSON &input_params);


dx::JSON appGetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON appGetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON appSetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON appSetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON appGetPermissions(const std::string &object_id, const std::string &input_params="{}");
dx::JSON appGetPermissions(const std::string &object_id, const dx::JSON &input_params);


dx::JSON appRevokePermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON appRevokePermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON appGrantPermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON appGrantPermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON appAddTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON appAddTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON appRemoveTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON appRemoveTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON appRun(const std::string &object_id, const std::string &input_params="{}");
dx::JSON appRun(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jobNew(const std::string &input_params="{}");
dx::JSON jobNew(const dx::JSON &input_params);


dx::JSON jobDescribe(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jobDescribe(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jobDestroy(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jobDestroy(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jobGetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jobGetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jobSetProperties(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jobSetProperties(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jobGetPermissions(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jobGetPermissions(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jobRevokePermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jobRevokePermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jobGrantPermission(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jobGrantPermission(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jobAddTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jobAddTypes(const std::string &object_id, const dx::JSON &input_params);


dx::JSON jobRemoveTypes(const std::string &object_id, const std::string &input_params="{}");
dx::JSON jobRemoveTypes(const std::string &object_id, const dx::JSON &input_params);


#include "dxcpp.h"

#endif

