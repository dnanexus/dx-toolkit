# Copyright (C) 2026 DNAnexus, Inc.
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

'''
Local HTTP listener used by the SSO Loopback flow.

This module owns only the callback transport and browser response lifecycle.
SSO protocol requests and flow orchestration remain in sso_login.py.
'''

from __future__ import print_function, unicode_literals, division, absolute_import


class LoopbackListener:
    '''
    Receives the browser callback for one SSO Loopback login attempt.

    *normalize_code* checks a received one-time code before the callback is
    accepted. Construction reserves the local port before redirect_url is sent
    to the auth server, so that port remains available for this listener.
    '''

    def __init__(self, normalize_code):
        raise NotImplementedError("Loopback listener binding is not implemented")

    @property
    def redirect_url(self):
        '''Returns the canonical loopback callback URL for the bound listener.'''
        raise NotImplementedError("Loopback redirect URL is not implemented")

    def set_expected_state(self, state):
        '''Sets the SSO session state that a callback must contain.'''
        raise NotImplementedError("Loopback callback state validation is not implemented")

    def wait_for_callback(self, timeout):
        '''
        Waits up to *timeout* seconds for a valid callback.

        Returns (code, error), with exactly one value set. A code is normalized
        before it is returned; an error is the known failure sent by the auth
        server.
        '''
        raise NotImplementedError("Loopback callback waiting is not implemented")

    def report_finalize_result(self, error=None):
        '''
        Reports the final login outcome to the browser callback response.

        A missing *error* indicates that finalize succeeded.
        '''
        raise NotImplementedError("Loopback finalize result reporting is not implemented")

    def close(self):
        '''Stops the listener and releases its bound socket.'''
        raise NotImplementedError("Loopback listener shutdown is not implemented")
