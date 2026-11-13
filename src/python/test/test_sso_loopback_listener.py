#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
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

from __future__ import print_function, unicode_literals, division, absolute_import

import io
import queue
import socket
import threading
import unittest
from http.client import HTTPConnection
from unittest.mock import Mock, call, patch
from urllib.parse import urlsplit

from dxpy.cli import sso_loopback_listener


class TestCallbackExchange(unittest.TestCase):
    def test_wait_for_callback_polls_until_the_deadline(self):
        exchange = sso_loopback_listener._CallbackExchange(lambda code: code)
        result = ("ABCD2345", None)
        exchange._callback_result_queue = Mock()
        exchange._callback_result_queue.get.side_effect = [
            queue.Empty,
            queue.Empty,
            result,
        ]

        with patch.object(
                sso_loopback_listener.time,
                "monotonic",
                side_effect=[100.0, 100.5, 101.0]):
            self.assertEqual(exchange.wait_for_callback(1.2), result)

        timeouts = [
            item.kwargs["timeout"]
            for item in exchange._callback_result_queue.get.call_args_list
        ]
        self.assertEqual(timeouts[:2], [0.5, 0.5])
        self.assertAlmostEqual(timeouts[2], 0.2)

    def test_wait_for_callback_times_out_at_the_deadline(self):
        exchange = sso_loopback_listener._CallbackExchange(lambda code: code)
        exchange._callback_result_queue = Mock()
        exchange._callback_result_queue.get.side_effect = queue.Empty

        with patch.object(
                sso_loopback_listener.time,
                "monotonic",
                side_effect=[100.0, 100.5, 101.0]):
            with self.assertRaises(TimeoutError):
                exchange.wait_for_callback(1.0)

        self.assertEqual(
            exchange._callback_result_queue.get.call_args_list,
            [call(timeout=0.5), call(timeout=0.5)],
        )


class TestLoopbackListenerBinding(unittest.TestCase):
    def test_request_connections_have_a_timeout(self):
        request = Mock()
        with patch.object(sso_loopback_listener._LoopbackRequestHandler, "handle"), \
                patch.object(sso_loopback_listener._LoopbackRequestHandler, "finish"):
            sso_loopback_listener._LoopbackRequestHandler(
                request,
                ("127.0.0.1", 54321),
                Mock(),
            )

        request.settimeout.assert_called_once_with(5)

    def test_binds_ipv4_on_an_ephemeral_port(self):
        listener = sso_loopback_listener.LoopbackListener(lambda code: code)
        self.addCleanup(listener.close)

        host, port = listener._server.server_address[:2]
        self.assertEqual(host, "127.0.0.1")
        self.assertGreater(port, 0)
        self.assertEqual(listener.redirect_url, "http://127.0.0.1:{}/callback".format(port))
        self.assertIs(listener._server.callback_exchange, listener._exchange)
        self.assertFalse(hasattr(listener._server, "listener"))
        self.assertFalse(listener._server.allow_reuse_address)

    def test_ipv4_bind_failure_falls_back_to_ipv6(self):
        attempts = []
        ipv6_server = Mock()
        ipv6_server.server_address = ("::1", 54321, 0, 0)

        def bind_ipv4(address, context):
            attempts.append(("ipv4", address, context))
            raise OSError("IPv4 unavailable")

        def bind_ipv6(address, context):
            attempts.append(("ipv6", address, context))
            return ipv6_server

        with patch.object(sso_loopback_listener, "_LoopbackHTTPServer", side_effect=bind_ipv4), \
                patch.object(sso_loopback_listener, "_IPv6HTTPServer", side_effect=bind_ipv6):
            listener = sso_loopback_listener.LoopbackListener(lambda code: code)

        self.assertEqual(
            attempts,
            [
                ("ipv4", ("127.0.0.1", 0), listener._exchange),
                ("ipv6", ("::1", 0), listener._exchange),
            ],
        )
        self.assertEqual(listener.redirect_url, "http://[::1]:54321/callback")
        listener.close()
        ipv6_server.shutdown.assert_called_once_with()
        ipv6_server.server_close.assert_called_once_with()

    def test_accepts_valid_ipv6_callback(self):
        if not socket.has_ipv6:
            self.skipTest("IPv6 is unavailable")
        try:
            with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as probe:
                probe.bind(("::1", 0))
        except OSError as error:
            self.skipTest("IPv6 loopback is unavailable: {}".format(error))

        with patch.object(
                sso_loopback_listener,
                "_LoopbackHTTPServer",
                side_effect=OSError("IPv4 unavailable")):
            listener = sso_loopback_listener.LoopbackListener(
                lambda code: code.replace("-", "").upper())
        self.addCleanup(listener.close)
        listener.set_expected_state("sess123")

        redirect = urlsplit(listener.redirect_url)
        self.assertEqual(redirect.hostname, "::1")
        connection = HTTPConnection(redirect.hostname, redirect.port, timeout=1)
        try:
            connection.request(
                "GET",
                "/callback?code=abcd-2345&state=sess123",
                headers={"Host": redirect.netloc},
            )
            response = connection.getresponse()
            response.read()
        finally:
            connection.close()

        self.assertEqual(response.status, 200)
        self.assertEqual(listener.wait_for_callback(1), ("ABCD2345", None))

    def test_both_bind_failures_raise_the_ipv6_error(self):
        attempts = []
        ipv6_error = OSError("IPv6 unavailable")

        def fail_ipv4(address, _context):
            attempts.append(("ipv4", address))
            raise OSError("IPv4 unavailable")

        def fail_ipv6(address, _context):
            attempts.append(("ipv6", address))
            raise ipv6_error

        with patch.object(sso_loopback_listener, "_LoopbackHTTPServer", side_effect=fail_ipv4), \
                patch.object(sso_loopback_listener, "_IPv6HTTPServer", side_effect=fail_ipv6):
            with self.assertRaises(OSError) as cm:
                sso_loopback_listener.LoopbackListener(lambda code: code)

        self.assertIs(cm.exception, ipv6_error)
        self.assertEqual(attempts, [("ipv4", ("127.0.0.1", 0)), ("ipv6", ("::1", 0))])

    def test_runs_in_a_daemon_thread_and_closes_once(self):
        server = Mock()
        server.server_address = ("127.0.0.1", 54321)
        serving = threading.Event()
        stopped = threading.Event()

        def serve_forever(poll_interval):
            serving.set()
            stopped.wait(1)

        server.serve_forever.side_effect = serve_forever
        server.shutdown.side_effect = stopped.set
        with patch.object(sso_loopback_listener, "_LoopbackHTTPServer", return_value=server):
            listener = sso_loopback_listener.LoopbackListener(lambda code: code)

        self.assertTrue(serving.wait(1))
        thread = listener._thread
        self.assertEqual(thread.name, "dx-sso-loopback-listener")
        self.assertTrue(thread.daemon)
        self.assertTrue(thread.is_alive())

        listener.close()
        listener.close()

        self.assertFalse(thread.is_alive())
        self.assertEqual(
            server.method_calls,
            [
                call.serve_forever(poll_interval=0.25),
                call.shutdown(),
                call.server_close(),
            ],
        )

    def test_close_releases_server_when_shutdown_fails(self):
        server = Mock()
        server.server_address = ("127.0.0.1", 54321)
        server.shutdown.side_effect = RuntimeError("shutdown failed")
        with patch.object(sso_loopback_listener, "_LoopbackHTTPServer", return_value=server):
            listener = sso_loopback_listener.LoopbackListener(lambda code: code)

        with self.assertRaisesRegex(RuntimeError, "shutdown failed"):
            listener.close()

        server.server_close.assert_called_once_with()
        self.assertIsNone(listener._server)
        self.assertIsNone(listener._thread)

    def test_close_rejects_the_server_thread(self):
        server = Mock()
        server.server_address = ("127.0.0.1", 54321)
        with patch.object(sso_loopback_listener, "_LoopbackHTTPServer", return_value=server):
            listener = sso_loopback_listener.LoopbackListener(lambda code: code)

        with patch.object(sso_loopback_listener.threading, "current_thread",
                          return_value=listener._thread):
            with self.assertRaisesRegex(RuntimeError, "cannot be closed"):
                listener.close()

        server.shutdown.assert_not_called()
        server.server_close.assert_not_called()
        listener.close()


class TestLoopbackListenerValidation(unittest.TestCase):
    def setUp(self):
        self.normalizer = Mock(side_effect=lambda code: code.replace("-", "").upper())
        self.listener = sso_loopback_listener.LoopbackListener(self.normalizer)
        self.addCleanup(self.listener.close)
        redirect = urlsplit(self.listener.redirect_url)
        self.host = redirect.hostname
        self.port = redirect.port
        self.authority = redirect.netloc

    def request_response(self, target, host=None, method="GET"):
        connection = HTTPConnection(self.host, self.port, timeout=1)
        try:
            connection.request(method, target, headers={"Host": host or self.authority})
            response = connection.getresponse()
            response.read()
            return response.status, dict(response.getheaders())
        finally:
            connection.close()

    def request(self, target, host=None, method="GET"):
        status, _headers = self.request_response(target, host=host, method=method)
        return status

    def assert_page_response(self, status, headers, expected_status):
        self.assertEqual(status, expected_status)
        self.assertEqual(headers["Content-Type"], "text/html; charset=utf-8")
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertEqual(headers["Content-Security-Policy"], "default-src 'none'")
        self.assertEqual(headers["Referrer-Policy"], "no-referrer")
        self.assertGreater(int(headers["Content-Length"]), 0)

    def test_valid_code_is_normalized_and_queued(self):
        self.listener.set_expected_state("sess123")

        self.assertEqual(
            self.request("/callback?code=abcd-2345&state=sess123"),
            200,
        )
        self.assertEqual(self.listener.wait_for_callback(1), ("ABCD2345", None))
        self.normalizer.assert_called_once_with("abcd-2345")

    def test_valid_error_is_queued_without_normalization(self):
        self.listener.set_expected_state("sess123")

        self.assertEqual(
            self.request("/callback?error=Access+denied&state=sess123"),
            200,
        )
        self.assertEqual(self.listener.wait_for_callback(1), (None, "Access denied"))
        self.normalizer.assert_not_called()

    # To make sure that we dont leak one time code to the terminal
    def test_callback_request_is_not_logged_to_stderr(self):
        self.listener.set_expected_state("sess123")
        captured = io.StringIO()

        with patch("sys.stderr", new=captured):
            self.assertEqual(
                self.request("/callback?code=abcd-2345&state=sess123"),
                200,
            )

        self.assertEqual(captured.getvalue(), "")
        self.assertEqual(self.listener.wait_for_callback(1), ("ABCD2345", None))

    def test_invalid_code_is_rejected_without_queueing(self):
        self.listener.set_expected_state("sess123")
        self.normalizer.side_effect = ValueError("invalid code")

        self.assertEqual(
            self.request("/callback?code=invalid&state=sess123"),
            400,
        )
        with self.assertRaises(TimeoutError):
            self.listener.wait_for_callback(0)

    def test_wrong_path_returns_not_found_and_listener_stays_active(self):
        self.listener.set_expected_state("sess123")

        self.assertEqual(self.request("/wrong?code=ABCD2345&state=sess123"), 404)
        self.assertEqual(
            self.request("/callback?code=ABCD2345&state=sess123"),
            200,
        )
        self.assertEqual(self.listener.wait_for_callback(1), ("ABCD2345", None))

    def test_invalid_request_does_not_claim_callback(self):
        self.listener.set_expected_state("sess123")

        self.assertEqual(
            self.request("/callback?code=ABCD2345&state=sess123", host="localhost:1234"),
            400,
        )
        self.assertEqual(
            self.request("/callback?code=ABCD2345&state=sess123"),
            200,
        )
        self.assertEqual(self.listener.wait_for_callback(1), ("ABCD2345", None))

    def test_only_first_valid_callback_is_accepted(self):
        self.listener.set_expected_state("sess123")

        self.assertEqual(
            self.request("/callback?code=ABCD2345&state=sess123"),
            200,
        )
        self.assertEqual(self.listener.wait_for_callback(1), ("ABCD2345", None))

        self.assertEqual(
            self.request("/callback?error=Access+denied&state=sess123"),
            409,
        )
        with self.assertRaises(TimeoutError):
            self.listener.wait_for_callback(0)

    def test_all_responses_are_pages_with_security_headers(self):
        self.listener.set_expected_state("sess123")

        rejected_requests = [
            ("GET", "/wrong", 404),
            ("GET", "/callback?state=sess123", 400),
            ("POST", "/callback?code=ABCD2345&state=sess123", 405),
        ]
        for method, target, expected_status in rejected_requests:
            with self.subTest(method=method, target=target):
                status, headers = self.request_response(target, method=method)
                self.assert_page_response(status, headers, expected_status)
                if method == "POST":
                    self.assertEqual(headers["Allow"], "GET")

        status, headers = self.request_response(
            "/callback?code=ABCD2345&state=sess123",
        )
        self.assert_page_response(status, headers, 200)
        self.assertEqual(self.listener.wait_for_callback(1), ("ABCD2345", None))

        status, headers = self.request_response(
            "/callback?error=Access+denied&state=sess123",
        )
        self.assert_page_response(status, headers, 409)

    def test_state_must_be_set_and_match(self):
        target = "/callback?code=ABCD2345&state=sess123"

        self.assertEqual(self.request(target), 400)
        self.listener.set_expected_state("different")
        self.assertEqual(self.request(target), 400)
        self.listener.set_expected_state("sess123")
        self.assertEqual(self.request(target), 200)
        self.assertEqual(self.listener.wait_for_callback(1), ("ABCD2345", None))

    def test_query_requires_one_state_and_exactly_one_result(self):
        self.listener.set_expected_state("sess123")
        invalid_targets = [
            "/callback",
            "/callback?state=sess123",
            "/callback?code=ABCD2345",
            "/callback?code=ABCD2345&error=Access+denied&state=sess123",
            "/callback?code=ABCD2345&state=sess123&state=other",
            "/callback?code=ABCD2345&code=WXYZ6789&state=sess123",
            "/callback?error=Access+denied&error=Try+again&state=sess123",
            "/callback?code=ABCD2345&state=sess123&extra=value",
            "/callback?code=ABCD2345&state=sess123&",
            "/callback?code=ABCD2345&state=%C3%A9",
            "http://{}/callback?code=ABCD2345&state=sess123".format(self.authority),
        ]

        for target in invalid_targets:
            with self.subTest(target=target):
                self.assertEqual(self.request(target), 400)


if __name__ == "__main__":
    unittest.main()
