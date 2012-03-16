#ifndef DXCPP_API_H
#define DXCPP_API_H

#include "json.h"


JSON systemSearch(const string &input_params=string("{}"));
JSON systemSearch(const JSON &input_params);


JSON userDescribe(const string &object_id, const string &input_params=string("{}"));
JSON userDescribe(const string &object_id, const JSON &input_params);


JSON userGetProperties(const string &object_id, const string &input_params=string("{}"));
JSON userGetProperties(const string &object_id, const JSON &input_params);


JSON userSetProperties(const string &object_id, const string &input_params=string("{}"));
JSON userSetProperties(const string &object_id, const JSON &input_params);


JSON userGetPermissions(const string &object_id, const string &input_params=string("{}"));
JSON userGetPermissions(const string &object_id, const JSON &input_params);


JSON userRevokePermission(const string &object_id, const string &input_params=string("{}"));
JSON userRevokePermission(const string &object_id, const JSON &input_params);


JSON userGrantPermission(const string &object_id, const string &input_params=string("{}"));
JSON userGrantPermission(const string &object_id, const JSON &input_params);


JSON userAddTypes(const string &object_id, const string &input_params=string("{}"));
JSON userAddTypes(const string &object_id, const JSON &input_params);


JSON userRemoveTypes(const string &object_id, const string &input_params=string("{}"));
JSON userRemoveTypes(const string &object_id, const JSON &input_params);


JSON groupNew(const string &input_params=string("{}"));
JSON groupNew(const JSON &input_params);


JSON groupDescribe(const string &object_id, const string &input_params=string("{}"));
JSON groupDescribe(const string &object_id, const JSON &input_params);


JSON groupDestroy(const string &object_id, const string &input_params=string("{}"));
JSON groupDestroy(const string &object_id, const JSON &input_params);


JSON groupGetProperties(const string &object_id, const string &input_params=string("{}"));
JSON groupGetProperties(const string &object_id, const JSON &input_params);


JSON groupSetProperties(const string &object_id, const string &input_params=string("{}"));
JSON groupSetProperties(const string &object_id, const JSON &input_params);


JSON groupGetPermissions(const string &object_id, const string &input_params=string("{}"));
JSON groupGetPermissions(const string &object_id, const JSON &input_params);


JSON groupRevokePermission(const string &object_id, const string &input_params=string("{}"));
JSON groupRevokePermission(const string &object_id, const JSON &input_params);


JSON groupGrantPermission(const string &object_id, const string &input_params=string("{}"));
JSON groupGrantPermission(const string &object_id, const JSON &input_params);


JSON groupAddMembers(const string &object_id, const string &input_params=string("{}"));
JSON groupAddMembers(const string &object_id, const JSON &input_params);


JSON groupRemoveMembers(const string &object_id, const string &input_params=string("{}"));
JSON groupRemoveMembers(const string &object_id, const JSON &input_params);


JSON groupAddTypes(const string &object_id, const string &input_params=string("{}"));
JSON groupAddTypes(const string &object_id, const JSON &input_params);


JSON groupRemoveTypes(const string &object_id, const string &input_params=string("{}"));
JSON groupRemoveTypes(const string &object_id, const JSON &input_params);


JSON jsonNew(const string &input_params=string("{}"));
JSON jsonNew(const JSON &input_params);


JSON jsonDescribe(const string &object_id, const string &input_params=string("{}"));
JSON jsonDescribe(const string &object_id, const JSON &input_params);


JSON jsonDestroy(const string &object_id, const string &input_params=string("{}"));
JSON jsonDestroy(const string &object_id, const JSON &input_params);


JSON jsonGetProperties(const string &object_id, const string &input_params=string("{}"));
JSON jsonGetProperties(const string &object_id, const JSON &input_params);


JSON jsonSetProperties(const string &object_id, const string &input_params=string("{}"));
JSON jsonSetProperties(const string &object_id, const JSON &input_params);


JSON jsonGetPermissions(const string &object_id, const string &input_params=string("{}"));
JSON jsonGetPermissions(const string &object_id, const JSON &input_params);


JSON jsonRevokePermission(const string &object_id, const string &input_params=string("{}"));
JSON jsonRevokePermission(const string &object_id, const JSON &input_params);


JSON jsonGrantPermission(const string &object_id, const string &input_params=string("{}"));
JSON jsonGrantPermission(const string &object_id, const JSON &input_params);


JSON jsonAddTypes(const string &object_id, const string &input_params=string("{}"));
JSON jsonAddTypes(const string &object_id, const JSON &input_params);


JSON jsonRemoveTypes(const string &object_id, const string &input_params=string("{}"));
JSON jsonRemoveTypes(const string &object_id, const JSON &input_params);


JSON jsonGet(const string &object_id, const string &input_params=string("{}"));
JSON jsonGet(const string &object_id, const JSON &input_params);


JSON jsonSet(const string &object_id, const string &input_params=string("{}"));
JSON jsonSet(const string &object_id, const JSON &input_params);


JSON collectionNew(const string &input_params=string("{}"));
JSON collectionNew(const JSON &input_params);


JSON collectionDescribe(const string &object_id, const string &input_params=string("{}"));
JSON collectionDescribe(const string &object_id, const JSON &input_params);


JSON collectionDestroy(const string &object_id, const string &input_params=string("{}"));
JSON collectionDestroy(const string &object_id, const JSON &input_params);


JSON collectionGetProperties(const string &object_id, const string &input_params=string("{}"));
JSON collectionGetProperties(const string &object_id, const JSON &input_params);


JSON collectionSetProperties(const string &object_id, const string &input_params=string("{}"));
JSON collectionSetProperties(const string &object_id, const JSON &input_params);


JSON collectionGetPermissions(const string &object_id, const string &input_params=string("{}"));
JSON collectionGetPermissions(const string &object_id, const JSON &input_params);


JSON collectionRevokePermission(const string &object_id, const string &input_params=string("{}"));
JSON collectionRevokePermission(const string &object_id, const JSON &input_params);


JSON collectionGrantPermission(const string &object_id, const string &input_params=string("{}"));
JSON collectionGrantPermission(const string &object_id, const JSON &input_params);


JSON collectionAddTypes(const string &object_id, const string &input_params=string("{}"));
JSON collectionAddTypes(const string &object_id, const JSON &input_params);


JSON collectionRemoveTypes(const string &object_id, const string &input_params=string("{}"));
JSON collectionRemoveTypes(const string &object_id, const JSON &input_params);


JSON collectionGet(const string &object_id, const string &input_params=string("{}"));
JSON collectionGet(const string &object_id, const JSON &input_params);


JSON fileNew(const string &input_params=string("{}"));
JSON fileNew(const JSON &input_params);


JSON fileDescribe(const string &object_id, const string &input_params=string("{}"));
JSON fileDescribe(const string &object_id, const JSON &input_params);


JSON fileDestroy(const string &object_id, const string &input_params=string("{}"));
JSON fileDestroy(const string &object_id, const JSON &input_params);


JSON fileGetProperties(const string &object_id, const string &input_params=string("{}"));
JSON fileGetProperties(const string &object_id, const JSON &input_params);


JSON fileSetProperties(const string &object_id, const string &input_params=string("{}"));
JSON fileSetProperties(const string &object_id, const JSON &input_params);


JSON fileGetPermissions(const string &object_id, const string &input_params=string("{}"));
JSON fileGetPermissions(const string &object_id, const JSON &input_params);


JSON fileRevokePermission(const string &object_id, const string &input_params=string("{}"));
JSON fileRevokePermission(const string &object_id, const JSON &input_params);


JSON fileGrantPermission(const string &object_id, const string &input_params=string("{}"));
JSON fileGrantPermission(const string &object_id, const JSON &input_params);


JSON fileAddTypes(const string &object_id, const string &input_params=string("{}"));
JSON fileAddTypes(const string &object_id, const JSON &input_params);


JSON fileRemoveTypes(const string &object_id, const string &input_params=string("{}"));
JSON fileRemoveTypes(const string &object_id, const JSON &input_params);


JSON fileUpload(const string &object_id, const string &input_params=string("{}"));
JSON fileUpload(const string &object_id, const JSON &input_params);


JSON fileClose(const string &object_id, const string &input_params=string("{}"));
JSON fileClose(const string &object_id, const JSON &input_params);


JSON fileDownload(const string &object_id, const string &input_params=string("{}"));
JSON fileDownload(const string &object_id, const JSON &input_params);


JSON tableNew(const string &input_params=string("{}"));
JSON tableNew(const JSON &input_params);


JSON tableDescribe(const string &object_id, const string &input_params=string("{}"));
JSON tableDescribe(const string &object_id, const JSON &input_params);


JSON tableExtend(const string &object_id, const string &input_params=string("{}"));
JSON tableExtend(const string &object_id, const JSON &input_params);


JSON tableDestroy(const string &object_id, const string &input_params=string("{}"));
JSON tableDestroy(const string &object_id, const JSON &input_params);


JSON tableGetProperties(const string &object_id, const string &input_params=string("{}"));
JSON tableGetProperties(const string &object_id, const JSON &input_params);


JSON tableSetProperties(const string &object_id, const string &input_params=string("{}"));
JSON tableSetProperties(const string &object_id, const JSON &input_params);


JSON tableGetPermissions(const string &object_id, const string &input_params=string("{}"));
JSON tableGetPermissions(const string &object_id, const JSON &input_params);


JSON tableRevokePermission(const string &object_id, const string &input_params=string("{}"));
JSON tableRevokePermission(const string &object_id, const JSON &input_params);


JSON tableGrantPermission(const string &object_id, const string &input_params=string("{}"));
JSON tableGrantPermission(const string &object_id, const JSON &input_params);


JSON tableAddTypes(const string &object_id, const string &input_params=string("{}"));
JSON tableAddTypes(const string &object_id, const JSON &input_params);


JSON tableRemoveTypes(const string &object_id, const string &input_params=string("{}"));
JSON tableRemoveTypes(const string &object_id, const JSON &input_params);


JSON tableAddRows(const string &object_id, const string &input_params=string("{}"));
JSON tableAddRows(const string &object_id, const JSON &input_params);


JSON tableClose(const string &object_id, const string &input_params=string("{}"));
JSON tableClose(const string &object_id, const JSON &input_params);


JSON tableGet(const string &object_id, const string &input_params=string("{}"));
JSON tableGet(const string &object_id, const JSON &input_params);


JSON appNew(const string &input_params=string("{}"));
JSON appNew(const JSON &input_params);


JSON appDescribe(const string &object_id, const string &input_params=string("{}"));
JSON appDescribe(const string &object_id, const JSON &input_params);


JSON appDestroy(const string &object_id, const string &input_params=string("{}"));
JSON appDestroy(const string &object_id, const JSON &input_params);


JSON appGetProperties(const string &object_id, const string &input_params=string("{}"));
JSON appGetProperties(const string &object_id, const JSON &input_params);


JSON appSetProperties(const string &object_id, const string &input_params=string("{}"));
JSON appSetProperties(const string &object_id, const JSON &input_params);


JSON appGetPermissions(const string &object_id, const string &input_params=string("{}"));
JSON appGetPermissions(const string &object_id, const JSON &input_params);


JSON appRevokePermission(const string &object_id, const string &input_params=string("{}"));
JSON appRevokePermission(const string &object_id, const JSON &input_params);


JSON appGrantPermission(const string &object_id, const string &input_params=string("{}"));
JSON appGrantPermission(const string &object_id, const JSON &input_params);


JSON appAddTypes(const string &object_id, const string &input_params=string("{}"));
JSON appAddTypes(const string &object_id, const JSON &input_params);


JSON appRemoveTypes(const string &object_id, const string &input_params=string("{}"));
JSON appRemoveTypes(const string &object_id, const JSON &input_params);


JSON appRun(const string &object_id, const string &input_params=string("{}"));
JSON appRun(const string &object_id, const JSON &input_params);


JSON jobNew(const string &input_params=string("{}"));
JSON jobNew(const JSON &input_params);


JSON jobDescribe(const string &object_id, const string &input_params=string("{}"));
JSON jobDescribe(const string &object_id, const JSON &input_params);


JSON jobDestroy(const string &object_id, const string &input_params=string("{}"));
JSON jobDestroy(const string &object_id, const JSON &input_params);


JSON jobGetProperties(const string &object_id, const string &input_params=string("{}"));
JSON jobGetProperties(const string &object_id, const JSON &input_params);


JSON jobSetProperties(const string &object_id, const string &input_params=string("{}"));
JSON jobSetProperties(const string &object_id, const JSON &input_params);


JSON jobGetPermissions(const string &object_id, const string &input_params=string("{}"));
JSON jobGetPermissions(const string &object_id, const JSON &input_params);


JSON jobRevokePermission(const string &object_id, const string &input_params=string("{}"));
JSON jobRevokePermission(const string &object_id, const JSON &input_params);


JSON jobGrantPermission(const string &object_id, const string &input_params=string("{}"));
JSON jobGrantPermission(const string &object_id, const JSON &input_params);


JSON jobAddTypes(const string &object_id, const string &input_params=string("{}"));
JSON jobAddTypes(const string &object_id, const JSON &input_params);


JSON jobRemoveTypes(const string &object_id, const string &input_params=string("{}"));
JSON jobRemoveTypes(const string &object_id, const JSON &input_params);


#include "dxcpp.h"

#endif

