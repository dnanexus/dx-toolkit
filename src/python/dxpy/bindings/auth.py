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

from dxpy import get_auth_server_name, DXHTTPRequest

from dxpy.api import system_whoami

def user_info(authserver_host=None, authserver_port=None):
    """Returns the result of the user_info call against the specified auth
    server.

    .. deprecated:: 0.108.0
       Use :func:`whoami` instead where possible.

    """
    authserver = get_auth_server_name(authserver_host, authserver_port)
    return DXHTTPRequest(authserver + "/system/getUserInfo", {}, prepend_srv=False)

def whoami():
    """
    Returns the user ID of the currently requesting user.
    """
    return system_whoami()['id']
