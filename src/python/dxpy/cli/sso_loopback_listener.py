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

import hmac
import queue
import socket
import threading
import time
from html import escape
from http.server import BaseHTTPRequestHandler, HTTPServer
from http import HTTPStatus
from urllib.parse import parse_qs, urlsplit


class _CallbackRequestError(Exception):
    def __init__(self, status):
        super().__init__()
        self.status = status


class _CallbackAlreadyReceived(Exception):
    pass


class _CallbackExchange:
    def __init__(self, normalize_code):
        self.normalize_code = normalize_code
        self.expected_state = None
        self.authority = None
        self._accepted = False
        self._finalize_result_set = False
        self._callback_result_queue = queue.Queue()
        self._finalize_result_queue = queue.Queue(maxsize=1)

    def set_authority(self, host, port):
        self.authority = "{}:{}".format(host, port)

    def set_expected_state(self, state):
        self.expected_state = state

    def parse_request(self, target, host_headers):
        if target.scheme or target.netloc or target.fragment or host_headers != [self.authority]:
            raise _CallbackRequestError(HTTPStatus.BAD_REQUEST)

        try:
            query = parse_qs(target.query, keep_blank_values=True, strict_parsing=True)
        except ValueError:
            raise _CallbackRequestError(HTTPStatus.BAD_REQUEST)

        if set(query) not in ({"code", "state"}, {"error", "state"}):
            raise _CallbackRequestError(HTTPStatus.BAD_REQUEST)
        # reject if appears more than once
        if any(len(values) != 1 for values in query.values()):
            raise _CallbackRequestError(HTTPStatus.BAD_REQUEST)

        state = query["state"][0]
        if self.expected_state is None or not hmac.compare_digest(
                state.encode("utf-8"), self.expected_state.encode("utf-8")):
            raise _CallbackRequestError(HTTPStatus.BAD_REQUEST)

        code = query.get("code", [None])[0]
        error = query.get("error", [None])[0]
        if code is not None:
            try:
                code = self.normalize_code(code)
            except ValueError:
                raise _CallbackRequestError(HTTPStatus.BAD_REQUEST)

        return code, error

    def accept_callback(self, target, host_headers):
        result = self.parse_request(target, host_headers)
        # HTTPServer runs handlers sequentially on one server thread. Switching
        # to a threaded server would require locking this check-and-set.
        if self._accepted:
            raise _CallbackAlreadyReceived()
        self._accepted = True
        self._callback_result_queue.put(result)
        return self._finalize_result_queue.get()

    def set_finalize_result(self, error):
        if self._finalize_result_set:
            return False
        self._finalize_result_queue.put(error)
        self._finalize_result_set = True
        return True

    def wait_for_callback(self, timeout):
        deadline = time.monotonic() + timeout
        remaining = timeout
        # On Windows with Python < 3.14, Ctrl-C cannot interrupt Queue.get()
        # while it waits on its lock. Poll briefly so Ctrl-C is handled within
        # 0.5 seconds, while the deadline preserves the requested total timeout.
        while True:
            try:
                return self._callback_result_queue.get(timeout=min(remaining, 0.5))
            except queue.Empty:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError() from None


class _LoopbackRequestHandler(BaseHTTPRequestHandler):
    # Browsers may speculatively open a connection without sending a request.
    timeout = 5

    def do_GET(self):
        target = urlsplit(self.path)
        if target.path == "/callback":
            self._handle_callback(target)
        else:
            self._send_browser_response(
                HTTPStatus.NOT_FOUND,
                "The requested sign-in callback was not found. Return to your terminal.",
            )

    def do_POST(self):
        self._send_browser_response(
            HTTPStatus.METHOD_NOT_ALLOWED,
            "This sign-in callback only accepts GET requests. Return to your terminal.",
            headers=(("Allow", "GET"),),
        )

    def _handle_callback(self, target):
        try:
            error = self.server.callback_exchange.accept_callback(
                target,
                self.headers.get_all("Host", []),
            )
        except _CallbackRequestError as error:
            self._send_browser_response(
                error.status,
                "The sign-in callback was invalid. Return to your terminal.",
            )
            return
        except _CallbackAlreadyReceived:
            self._send_browser_response(
                HTTPStatus.CONFLICT,
                "This sign-in response was already received. Return to your terminal.",
            )
            return

        if error is None:
            message = (
                "Login completed successfully. "
                "You may now close this tab and return to your terminal."
            )
        else:
            message = "Login failed: {}. Return to your terminal.".format(error.rstrip("."))
        self._send_browser_response(HTTPStatus.OK, message)

    # This temporary local response may later redirect the browser to a
    # polished frontend confirmation page.
    def _send_browser_response(self, status, message, headers=()):
        body = (
            "<!doctype html>\n"
            "<html lang=\"en\">\n"
            "<head><meta charset=\"utf-8\"><title>DNAnexus login</title></head>\n"
            "<body><p>{}</p></body>\n"
            "</html>\n"
        ).format(escape(message)).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Security-Policy", "default-src 'none'")
        self.send_header("Referrer-Policy", "no-referrer")
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format, *_args):
        # Callback URLs contain one-time codes and session state, so never log requests.
        pass


class _LoopbackHTTPServer(HTTPServer):
    # SO_REUSEADDR allows competing processes to share a listening port on Windows.
    allow_reuse_address = False

    def __init__(self, address, callback_exchange):
        self.callback_exchange = callback_exchange
        super().__init__(address, _LoopbackRequestHandler)


class _IPv6HTTPServer(_LoopbackHTTPServer):
    address_family = socket.AF_INET6


class LoopbackListener:
    '''
    Receives the browser callback for one SSO Loopback login attempt.

    *normalize_code* checks a received one-time code before the callback is
    accepted. Construction reserves the local port before redirect_url is sent
    to the auth server, so that port remains available for this listener.
    '''

    def __init__(self, normalize_code):
        self._exchange = _CallbackExchange(normalize_code)
        try:
            self._server = _LoopbackHTTPServer(("127.0.0.1", 0), self._exchange)
            host = "127.0.0.1"
        except OSError:
            self._server = _IPv6HTTPServer(("::1", 0), self._exchange)
            host = "[::1]"

        port = self._server.server_address[1]
        self._exchange.set_authority(host, port)
        self._redirect_url = "http://{}/callback".format(self._exchange.authority)
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
        self._exchange.set_expected_state(state)

    def wait_for_callback(self, timeout):
        '''
        Waits up to *timeout* seconds for a valid callback.

        Returns (code, error), with exactly one value set. A code is normalized
        before it is returned; an error is the known failure sent by the auth
        server.
        '''
        return self._exchange.wait_for_callback(timeout)

    def set_finalize_result(self, error=None):
        '''
        Releases the accepted browser callback with the final login result.

        *error* is None for success or a safe message to show for failure.
        The result may be set only once.
        '''
        if not self._exchange.set_finalize_result(error):
            raise RuntimeError("Finalize result has already been set")

    def close(self):
        '''Stops the listener and releases its bound socket.'''
        server = self._server
        if server is None:
            return

        thread = self._thread
        if thread is threading.current_thread():
            raise RuntimeError("Loopback listener cannot be closed from its server thread")

        self._exchange.set_finalize_result("Login could not be completed")
        self._server = None
        self._thread = None
        try:
            server.shutdown()
        finally:
            try:
                server.server_close()
            finally:
                thread.join()
