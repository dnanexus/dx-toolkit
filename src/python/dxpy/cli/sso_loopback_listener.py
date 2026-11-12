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

import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


class _LoopbackRequestHandler(BaseHTTPRequestHandler):
    pass


class _IPv6HTTPServer(HTTPServer):
    address_family = socket.AF_INET6


class LoopbackListener:
    '''
    Receives the browser callback for one SSO Loopback login attempt.

    *normalize_code* checks a received one-time code before the callback is
    accepted. Construction reserves the local port before redirect_url is sent
    to the auth server, so that port remains available for this listener.
    '''

    def __init__(self, normalize_code):
        self._normalize_code = normalize_code
        try:
            self._server = HTTPServer(("127.0.0.1", 0), _LoopbackRequestHandler)
            authority = "127.0.0.1"
        except OSError:
            self._server = _IPv6HTTPServer(("::1", 0), _LoopbackRequestHandler)
            authority = "[::1]"

        port = self._server.server_address[1]
        self._redirect_url = "http://{}:{}/callback".format(authority, port)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.25},
            name="dx-sso-loopback-listener",
            daemon=True,
        )
        try:
            self._thread.start()
        except BaseException:
            self._thread = None
            server = self._server
            self._server = None
            server.server_close()
            raise

    @property
    def redirect_url(self):
        '''Returns the canonical loopback callback URL for the bound listener.'''
        return self._redirect_url

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
        server = self._server
        if server is None:
            return

        thread = self._thread
        if thread is threading.current_thread():
            raise RuntimeError("Loopback listener cannot be closed from its server thread")

        self._server = None
        self._thread = None
        try:
            server.shutdown()
        finally:
            try:
                server.server_close()
            finally:
                thread.join()
