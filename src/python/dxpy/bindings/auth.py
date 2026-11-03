# Copyright (C) 2013-2016 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

from __future__ import print_function, unicode_literals, division, absolute_import

import dxpy
from dxpy import get_auth_server_name, DXHTTPRequest

from dxpy.api import system_get_user_identity

def is_read_only_session():
    """
    Returns True if the active security context was recorded by ``dx login``
    as read-only.
    """
    return bool((dxpy.SECURITY_CONTEXT or {}).get("read_only"))

def user_info(authserver_host=None, authserver_port=None):
    """Returns the result of the user_info call against the specified auth
    server.

    .. deprecated:: 0.108.0
       Use :func:`whoami` instead where possible.

    """
    authserver = get_auth_server_name(authserver_host, authserver_port)
    return DXHTTPRequest(authserver + "/system/getUserInfo", {}, prepend_srv=False)

def whoami(allow_read_only=None):
    """
    Returns the user ID of the currently requesting user.

    :param allow_read_only: Value of ``allowReadOnly``, without which a
        read-only token is refused. Defaults to whether the active security
        context is read-only.
    :type allow_read_only: boolean or None
    """
    if allow_read_only is None:
        allow_read_only = is_read_only_session()
    return system_get_user_identity({"allowReadOnly": bool(allow_read_only)})['id']
