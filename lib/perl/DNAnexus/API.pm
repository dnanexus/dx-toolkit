
package DNAnexus::API;

use strict;
use Exporter;
use DNAnexus qw(DXHTTPRequest);


sub systemSearch(;$%) {
    my ($input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/system/search', $input_params, %kwargs);
}


sub userDescribe($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/describe', $input_params, %kwargs);
}


sub userGetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getProperties', $input_params, %kwargs);
}


sub userSetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/setProperties', $input_params, %kwargs);
}


sub userGetPermissions($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getPermissions', $input_params, %kwargs);
}


sub userRevokePermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/revokePermission', $input_params, %kwargs);
}


sub userGrantPermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/grantPermission', $input_params, %kwargs);
}


sub userAddTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/addTypes', $input_params, %kwargs);
}


sub userRemoveTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/removeTypes', $input_params, %kwargs);
}


sub groupNew(;$%) {
    my ($input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/group/new', $input_params, %kwargs);
}


sub groupDescribe($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/describe', $input_params, %kwargs);
}


sub groupDestroy($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/destroy', $input_params, %kwargs);
}


sub groupGetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getProperties', $input_params, %kwargs);
}


sub groupSetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/setProperties', $input_params, %kwargs);
}


sub groupGetPermissions($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getPermissions', $input_params, %kwargs);
}


sub groupRevokePermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/revokePermission', $input_params, %kwargs);
}


sub groupGrantPermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/grantPermission', $input_params, %kwargs);
}


sub groupAddMembers($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/addMembers', $input_params, %kwargs);
}


sub groupRemoveMembers($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/removeMembers', $input_params, %kwargs);
}


sub groupAddTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/addTypes', $input_params, %kwargs);
}


sub groupRemoveTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/removeTypes', $input_params, %kwargs);
}


sub jsonNew(;$%) {
    my ($input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/json/new', $input_params, %kwargs);
}


sub jsonDescribe($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/describe', $input_params, %kwargs);
}


sub jsonDestroy($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/destroy', $input_params, %kwargs);
}


sub jsonGetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getProperties', $input_params, %kwargs);
}


sub jsonSetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/setProperties', $input_params, %kwargs);
}


sub jsonGetPermissions($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getPermissions', $input_params, %kwargs);
}


sub jsonRevokePermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/revokePermission', $input_params, %kwargs);
}


sub jsonGrantPermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/grantPermission', $input_params, %kwargs);
}


sub jsonAddTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/addTypes', $input_params, %kwargs);
}


sub jsonRemoveTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/removeTypes', $input_params, %kwargs);
}


sub jsonGet($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/get', $input_params, %kwargs);
}


sub jsonSet($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/set', $input_params, %kwargs);
}


sub collectionNew(;$%) {
    my ($input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/collection/new', $input_params, %kwargs);
}


sub collectionDescribe($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/describe', $input_params, %kwargs);
}


sub collectionDestroy($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/destroy', $input_params, %kwargs);
}


sub collectionGetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getProperties', $input_params, %kwargs);
}


sub collectionSetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/setProperties', $input_params, %kwargs);
}


sub collectionGetPermissions($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getPermissions', $input_params, %kwargs);
}


sub collectionRevokePermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/revokePermission', $input_params, %kwargs);
}


sub collectionGrantPermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/grantPermission', $input_params, %kwargs);
}


sub collectionAddTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/addTypes', $input_params, %kwargs);
}


sub collectionRemoveTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/removeTypes', $input_params, %kwargs);
}


sub collectionGet($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/get', $input_params, %kwargs);
}


sub fileNew(;$%) {
    my ($input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/file/new', $input_params, %kwargs);
}


sub fileDescribe($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/describe', $input_params, %kwargs);
}


sub fileDestroy($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/destroy', $input_params, %kwargs);
}


sub fileGetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getProperties', $input_params, %kwargs);
}


sub fileSetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/setProperties', $input_params, %kwargs);
}


sub fileGetPermissions($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getPermissions', $input_params, %kwargs);
}


sub fileRevokePermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/revokePermission', $input_params, %kwargs);
}


sub fileGrantPermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/grantPermission', $input_params, %kwargs);
}


sub fileAddTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/addTypes', $input_params, %kwargs);
}


sub fileRemoveTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/removeTypes', $input_params, %kwargs);
}


sub fileUpload($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/upload', $input_params, %kwargs);
}


sub fileClose($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/close', $input_params, %kwargs);
}


sub fileDownload($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/download', $input_params, %kwargs);
}


sub tableNew(;$%) {
    my ($input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/table/new', $input_params, %kwargs);
}


sub tableDescribe($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/describe', $input_params, %kwargs);
}


sub tableExtend($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/extend', $input_params, %kwargs);
}


sub tableDestroy($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/destroy', $input_params, %kwargs);
}


sub tableGetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getProperties', $input_params, %kwargs);
}


sub tableSetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/setProperties', $input_params, %kwargs);
}


sub tableGetPermissions($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getPermissions', $input_params, %kwargs);
}


sub tableRevokePermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/revokePermission', $input_params, %kwargs);
}


sub tableGrantPermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/grantPermission', $input_params, %kwargs);
}


sub tableAddTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/addTypes', $input_params, %kwargs);
}


sub tableRemoveTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/removeTypes', $input_params, %kwargs);
}


sub tableAddRows($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/addRows', $input_params, %kwargs);
}


sub tableClose($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/close', $input_params, %kwargs);
}


sub tableGet($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/get', $input_params, %kwargs);
}


sub appNew(;$%) {
    my ($input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/app/new', $input_params, %kwargs);
}


sub appDescribe($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/describe', $input_params, %kwargs);
}


sub appDestroy($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/destroy', $input_params, %kwargs);
}


sub appGetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getProperties', $input_params, %kwargs);
}


sub appSetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/setProperties', $input_params, %kwargs);
}


sub appGetPermissions($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getPermissions', $input_params, %kwargs);
}


sub appRevokePermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/revokePermission', $input_params, %kwargs);
}


sub appGrantPermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/grantPermission', $input_params, %kwargs);
}


sub appAddTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/addTypes', $input_params, %kwargs);
}


sub appRemoveTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/removeTypes', $input_params, %kwargs);
}


sub appRun($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/run', $input_params, %kwargs);
}


sub jobNew(;$%) {
    my ($input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/job/new', $input_params, %kwargs);
}


sub jobDescribe($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/describe', $input_params, %kwargs);
}


sub jobDestroy($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/destroy', $input_params, %kwargs);
}


sub jobGetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getProperties', $input_params, %kwargs);
}


sub jobSetProperties($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/setProperties', $input_params, %kwargs);
}


sub jobGetPermissions($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/getPermissions', $input_params, %kwargs);
}


sub jobRevokePermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/revokePermission', $input_params, %kwargs);
}


sub jobGrantPermission($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/grantPermission', $input_params, %kwargs);
}


sub jobAddTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/addTypes', $input_params, %kwargs);
}


sub jobRemoveTypes($;$%) {
    my ($object_id, $input_params, %kwargs) = @_;
    %kwargs = () unless %kwargs;
    return DXHTTPRequest('/'.$object_id.'/removeTypes', $input_params, %kwargs);
}


our @ISA = "Exporter";
our @EXPORT_OK = qw(systemSearch userDescribe userGetProperties userSetProperties userGetPermissions userRevokePermission userGrantPermission userAddTypes userRemoveTypes groupNew groupDescribe groupDestroy groupGetProperties groupSetProperties groupGetPermissions groupRevokePermission groupGrantPermission groupAddMembers groupRemoveMembers groupAddTypes groupRemoveTypes jsonNew jsonDescribe jsonDestroy jsonGetProperties jsonSetProperties jsonGetPermissions jsonRevokePermission jsonGrantPermission jsonAddTypes jsonRemoveTypes jsonGet jsonSet collectionNew collectionDescribe collectionDestroy collectionGetProperties collectionSetProperties collectionGetPermissions collectionRevokePermission collectionGrantPermission collectionAddTypes collectionRemoveTypes collectionGet fileNew fileDescribe fileDestroy fileGetProperties fileSetProperties fileGetPermissions fileRevokePermission fileGrantPermission fileAddTypes fileRemoveTypes fileUpload fileClose fileDownload tableNew tableDescribe tableExtend tableDestroy tableGetProperties tableSetProperties tableGetPermissions tableRevokePermission tableGrantPermission tableAddTypes tableRemoveTypes tableAddRows tableClose tableGet appNew appDescribe appDestroy appGetProperties appSetProperties appGetPermissions appRevokePermission appGrantPermission appAddTypes appRemoveTypes appRun jobNew jobDescribe jobDestroy jobGetProperties jobSetProperties jobGetPermissions jobRevokePermission jobGrantPermission jobAddTypes jobRemoveTypes);

