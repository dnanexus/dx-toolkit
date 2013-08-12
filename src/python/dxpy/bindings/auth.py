from dxpy import get_auth_server_name, DXHTTPRequest

def user_info(authserver_host=None, authserver_port=None):
    """
    Returns the result of the user_info call against the specified auth server.
    """
    authserver = get_auth_server_name(authserver_host, authserver_port)
    return DXHTTPRequest(authserver + "/user_info", {}, prepend_srv=False)
