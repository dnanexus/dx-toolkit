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

import threading
import unittest
from unittest.mock import ANY, Mock, call, patch

import urllib3
from parameterized import parameterized

import dxpy
from dxpy.cli import sso_login
from dxpy.exceptions import DXAPIError, HTTPErrorWithContent
from dxpy_testutil import s256

AUTHSERVER = "https://auth.dnanexus.com"
REDIRECT_URL = "http://127.0.0.1:53682/callback"
INVALID_CODE_MESSAGE = "Invalid one-time code. Enter the 8-character code shown in the browser."
TOKEN = {"access_token": "token", "token_type": "bearer", "expires_in": 64800,
         "token_signature": "signature", "user_id": "user-alice"}
EXPIRES = 1800000000000


def api_error(name, message="message", code=400):
    """The exception DXHTTPRequest raises for a JSON error response."""
    error_class = getattr(dxpy.exceptions, name, DXAPIError)
    return error_class({"error": {"type": name, "message": message}}, code)


def http_error(status):
    """The exception DXHTTPRequest raises for a non-JSON error response."""
    return HTTPErrorWithContent("{} [Time=0 RequestID=1]".format(status), "<html></html>")


class TestMakePkcePair(unittest.TestCase):
    """Tests for make_pkce_pair()."""

    def test_rfc7636_appendix_b(self):
        verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
        with patch("secrets.token_urlsafe", return_value=verifier) as token_urlsafe:
            self.assertEqual(sso_login.make_pkce_pair(), (verifier, "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"))
        token_urlsafe.assert_called_once_with(32)

    def test_pair_meets_server_rules(self):
        verifier, challenge = sso_login.make_pkce_pair()
        self.assertRegex(verifier, r"^[A-Za-z0-9._~-]{43,128}$")
        self.assertRegex(challenge, r"^[A-Za-z0-9_-]{43}$")
        self.assertEqual(challenge, s256(verifier))

    def test_new_pair_each_time(self):
        self.assertNotEqual(sso_login.make_pkce_pair(), sso_login.make_pkce_pair())


class TestNormalizeOneTimeCode(unittest.TestCase):
    """Tests for normalize_one_time_code()."""

    @parameterized.expand([
        ("ABCD2345",),
        ("abcd2345",),
        ("abcd-2345",),
        (" ABCD 2345\n",),
        ("AB-CD-23-45",),
        ("ab\tcd 23-45-",),
    ])
    def test_valid(self, text):
        self.assertEqual(sso_login.normalize_one_time_code(text), "ABCD2345")

    @parameterized.expand([
        ("",),
        ("   ",),
        ("-",),
        ("ABCD234",),
        ("ABCD23456",),
        # 0, O, 1 and I are not in the alphabet
        ("ABCD2340",),
        ("abcdo345",),
        ("ABCD2341",),
        ("abcdi345",),
        ("ABCD_2345",),
        ("ABCD.2345",),
        ("ÄBCD2345",),
    ])
    def test_invalid(self, text):
        with self.assertRaises(ValueError) as cm:
            sso_login.normalize_one_time_code(text)
        self.assertEqual(str(cm.exception), INVALID_CODE_MESSAGE)


class TestGetSsoIdArgs(unittest.TestCase):
    """Tests for get_sso_id_args()."""

    @parameterized.expand([
        ("alice", {"id": "user-alice"}),
        ("user-alice", {"id": "user-alice"}),
        ("alice@example.com", {"email": "alice@example.com"}),
        ("user-alice@example.com", {"email": "user-alice@example.com"}),
    ])
    def test_args(self, username, sso_id_args):
        self.assertEqual(sso_login.get_sso_id_args(username), sso_id_args)


class TestAuthorizeUrl(unittest.TestCase):
    """Tests for authorize_url()."""

    @parameterized.expand([
        ("https://auth.dnanexus.com", "https://auth.dnanexus.com"),
        # Default ports are dropped
        ("https://stagingauth.dnanexus.com:443", "https://stagingauth.dnanexus.com"),
        ("http://localhost:80", "http://localhost"),
        # Other ports are kept
        ("https://auth.cn.dnanexus.com:8001", "https://auth.cn.dnanexus.com:8001"),
        ("http://127.0.0.1:443", "http://127.0.0.1:443"),
        ("https://127.0.0.1:80", "https://127.0.0.1:80"),
        ("http://127.0.0.1:5000", "http://127.0.0.1:5000"),
    ])
    def test_url(self, authserver, url):
        self.assertEqual(sso_login.authorize_url(authserver, "sess123"), url + "/sso/authorize/sess123")


class TestLookupIdp(unittest.TestCase):
    """Tests for lookup_idp() with a mocked DXHTTPRequest."""

    def test_sso_user(self):
        with patch("dxpy.DXHTTPRequest", return_value={"SSOId": "0oa123"}) as request:
            self.assertEqual(sso_login.lookup_idp("alice"), "0oa123")
        request.assert_called_once_with(
            "/user/getSSOId",
            {"id": "user-alice"},
            auth=None,
            max_retries=0,
            timeout=10,
        )

    def test_body_from_username(self):
        with patch("dxpy.DXHTTPRequest", return_value={"SSOId": ""}) as request:
            sso_login.lookup_idp("alice@example.com")
        self.assertEqual(request.call_args[0][1], {"email": "alice@example.com"})

    @parameterized.expand([
        ({"SSOId": ""},),
        ({"SSOId": None},),
        ({},),
        ([],),
        ("text",),
    ])
    def test_not_sso(self, response):
        with patch("dxpy.DXHTTPRequest", return_value=response):
            self.assertIsNone(sso_login.lookup_idp("alice"))

    @parameterized.expand([
        (api_error("ResourceNotFound", code=404),),
        (api_error("InvalidInput", code=422),),
        (api_error("RateLimitConditional", code=429),),
        (api_error("InternalError", code=500),),
        (http_error("404 NOT FOUND"),),
        (http_error("502 Bad Gateway"),),
        (urllib3.exceptions.ProtocolError("Connection aborted."),),
        (ConnectionRefusedError(),),
    ])
    def test_error_means_not_sso(self, error):
        with patch("dxpy.DXHTTPRequest", side_effect=error) as request:
            self.assertIsNone(sso_login.lookup_idp("alice"))
        request.assert_called_once()

    def test_ctrl_c_passes_through(self):
        with patch("dxpy.DXHTTPRequest", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                sso_login.lookup_idp("alice")


class TestCanOpenBrowser(unittest.TestCase):
    """Tests for can_open_browser()."""

    def test_remote_session_has_no_browser(self):
        for variable in ("SSH_CONNECTION", "SSH_TTY"):
            with self.subTest(variable=variable), \
                    patch.dict(sso_login.os.environ, {variable: "set"}, clear=True), \
                    patch.object(sso_login.sys, "platform", "darwin"), \
                    patch.object(sso_login.webbrowser, "get") as browser_get:
                self.assertFalse(sso_login.can_open_browser())
                browser_get.assert_not_called()

    def test_headless_linux_has_no_browser(self):
        with patch.dict(sso_login.os.environ, {"TERM": "xterm"}, clear=True), \
                patch.object(sso_login.sys, "platform", "linux"), \
                patch.object(sso_login.webbrowser, "get") as browser_get:
            self.assertFalse(sso_login.can_open_browser())
            browser_get.assert_not_called()

    @parameterized.expand([
        ("display", {"DISPLAY": ":0"}),
        ("wayland", {"WAYLAND_DISPLAY": "wayland-0"}),
    ])
    def test_linux_graphical_browser(self, _name, environment):
        controller = object()
        with patch.dict(sso_login.os.environ, environment, clear=True), \
                patch.object(sso_login.sys, "platform", "linux"), \
                patch.object(sso_login.webbrowser, "get", return_value=controller) as browser_get:
            self.assertTrue(sso_login.can_open_browser())
            browser_get.assert_called_once_with()

    def test_generic_system_browser_is_accepted(self):
        controller = object()
        with patch.dict(sso_login.os.environ, {}, clear=True), \
                patch.object(sso_login.sys, "platform", "darwin"), \
                patch.object(sso_login.webbrowser, "get", return_value=controller):
            self.assertTrue(sso_login.can_open_browser())

    def test_browser_command_is_accepted(self):
        # A BROWSER command such as wslview uses the same class as text-mode browsers
        controller = sso_login.webbrowser.GenericBrowser("wslview")
        with patch.dict(sso_login.os.environ, {"DISPLAY": ":0"}, clear=True), \
                patch.object(sso_login.sys, "platform", "linux"), \
                patch.object(sso_login.webbrowser, "get", return_value=controller):
            self.assertTrue(sso_login.can_open_browser())

    @parameterized.expand([
        ("www-browser",),
        ("links",),
        ("elinks",),
        ("lynx",),
        ("w3m",),
        ("/usr/bin/w3m",),
    ])
    def test_text_mode_browser_is_rejected(self, command):
        controller = sso_login.webbrowser.GenericBrowser(command)
        with patch.dict(sso_login.os.environ, {"DISPLAY": ":0"}, clear=True), \
                patch.object(sso_login.sys, "platform", "linux"), \
                patch.object(sso_login.webbrowser, "get", return_value=controller):
            self.assertFalse(sso_login.can_open_browser())

    def test_no_registered_browser(self):
        error = sso_login.webbrowser.Error("could not locate runnable browser")
        with patch.dict(sso_login.os.environ, {}, clear=True), \
                patch.object(sso_login.sys, "platform", "darwin"), \
                patch.object(sso_login.webbrowser, "get", side_effect=error):
            self.assertFalse(sso_login.can_open_browser())


class TestLaunchBrowser(unittest.TestCase):
    """Tests for launching the selected browser without blocking the CLI."""

    def test_blocking_browser_runs_in_a_daemon_thread(self):
        started = threading.Event()
        release = threading.Event()
        browser = Mock()

        def open_browser(_url, new):
            started.set()
            release.wait(1)

        browser.open.side_effect = open_browser
        with patch.object(sso_login.webbrowser, "get", return_value=browser):
            thread = sso_login._launch_browser(AUTHSERVER)

        self.addCleanup(thread.join, 1)
        self.addCleanup(release.set)
        self.assertTrue(started.wait(1))
        self.assertTrue(thread.daemon)
        self.assertEqual(thread.name, "dx-sso-browser-launcher")
        self.assertTrue(thread.is_alive())

        release.set()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        browser.open.assert_called_once_with(AUTHSERVER, new=2)

    def test_browser_open_error_is_ignored(self):
        browser = Mock()
        browser.open.side_effect = OSError("could not open browser")
        with patch.object(sso_login.webbrowser, "get", return_value=browser):
            thread = sso_login._launch_browser(AUTHSERVER)

        thread.join(1)
        self.assertFalse(thread.is_alive())

    def test_missing_browser_returns_none(self):
        error = sso_login.webbrowser.Error("could not locate runnable browser")
        with patch.object(sso_login.webbrowser, "get", side_effect=error):
            self.assertIsNone(sso_login._launch_browser(AUTHSERVER))

    def test_thread_start_error_returns_none(self):
        thread = Mock()
        thread.start.side_effect = RuntimeError("can't start new thread")
        with patch.object(sso_login.webbrowser, "get", return_value=Mock()), \
                patch.object(sso_login.threading, "Thread", return_value=thread):
            self.assertIsNone(sso_login._launch_browser(AUTHSERVER))


class TestInitializeCode(unittest.TestCase):
    """Tests for initialize_code() with a mocked DXHTTPRequest."""

    def test_request(self):
        response = {"sessionId": "sess123", "expiresIn": 300}
        with patch("dxpy.DXHTTPRequest", return_value=response) as request:
            self.assertIs(sso_login.initialize_code(AUTHSERVER, "0oa123", "challenge"), response)
        request.assert_called_once_with(AUTHSERVER + "/sso/initialize",
                                        {"type": "code", "codeChallenge": "challenge", "idpId": "0oa123"},
                                        prepend_srv=False, auth=None, max_retries=0)

    def test_no_sso_routes(self):
        with patch("dxpy.DXHTTPRequest", side_effect=http_error("404 NOT FOUND")):
            with self.assertRaises(sso_login.SSOUnavailable) as cm:
                sso_login.initialize_code(AUTHSERVER, "0oa123", "challenge")
        self.assertEqual(str(cm.exception), 'Logging in with single sign-on is not supported. Use the Profile page '
                                            'on the DNAnexus website to create an API token, then run "dx login '
                                            '--token" to log in with it.')

    @parameterized.expand([
        (api_error("ResourceNotFound", code=404),),
        (api_error("RateLimitConditional", code=429),),
        (http_error("400 BAD REQUEST"),),
        (http_error("502 Bad Gateway"),),
    ])
    def test_other_errors_pass_through(self, error):
        with patch("dxpy.DXHTTPRequest", side_effect=error) as request:
            with self.assertRaises(type(error)) as cm:
                sso_login.initialize_code(AUTHSERVER, "0oa123", "challenge")
        self.assertIs(cm.exception, error)
        request.assert_called_once()


class TestInitializeLoopback(unittest.TestCase):
    """Tests for initialize_loopback() with a mocked DXHTTPRequest."""

    def test_request(self):
        response = {"sessionId": "sess123", "expiresIn": 300}
        with patch("dxpy.DXHTTPRequest", return_value=response) as request:
            self.assertIs(sso_login.initialize_loopback(AUTHSERVER, "0oa123", "challenge", REDIRECT_URL),
                          response)
        request.assert_called_once_with(AUTHSERVER + "/sso/initialize",
                                        {"type": "loopback", "codeChallenge": "challenge", "idpId": "0oa123",
                                         "redirectUrl": REDIRECT_URL},
                                        prepend_srv=False, auth=None, max_retries=0)


class TestFinalize(unittest.TestCase):
    """Tests for finalize() with a mocked DXHTTPRequest."""

    def test_request(self):
        with patch("dxpy.DXHTTPRequest", return_value=TOKEN) as request:
            self.assertIs(sso_login.finalize(AUTHSERVER, "sess123", "verifier", "ABCD2345", EXPIRES), TOKEN)
        request.assert_called_once_with(AUTHSERVER + "/sso/finalize",
                                        {"sessionId": "sess123", "cliCodeVerifier": "verifier",
                                         "oneTimeCode": "ABCD2345", "expires": EXPIRES},
                                        prepend_srv=False, auth=None, max_retries=0)

    def test_code_rejected(self):
        error = api_error("SSOLoginFailedError", "SSO login could not be completed")
        with patch("dxpy.DXHTTPRequest", side_effect=error):
            with self.assertRaises(sso_login.SSOCodeRejected) as cm:
                sso_login.finalize(AUTHSERVER, "sess123", "verifier", "ABCD2345", EXPIRES)
        self.assertEqual(str(cm.exception), "SSO login could not be completed")

    @parameterized.expand([
        ("SSOSessionExpiredError", "The SSO login session expired. Run dx login again."),
        ("SSOOneTimeCodeExpiredError", "The SSO one-time code expired. Run dx login again."),
        ("SSONetworkChangedError", "Your network address changed during login. Run dx login again."),
    ])
    def test_login_ended(self, name, message):
        with patch("dxpy.DXHTTPRequest", side_effect=api_error(name, message)):
            with self.assertRaises(sso_login.SSOLoginEnded) as cm:
                sso_login.finalize(AUTHSERVER, "sess123", "verifier", "ABCD2345", EXPIRES)
        self.assertEqual(str(cm.exception), message)

    @parameterized.expand([
        (api_error("AccountLockoutError"),),
        (api_error("RateLimitConditional", code=429),),
        (api_error("InvalidInput", code=422),),
        (api_error("InternalError", code=500),),
        (http_error("502 Bad Gateway"),),
    ])
    def test_other_errors_pass_through(self, error):
        with patch("dxpy.DXHTTPRequest", side_effect=error) as request:
            with self.assertRaises(type(error)) as cm:
                sso_login.finalize(AUTHSERVER, "sess123", "verifier", "ABCD2345", EXPIRES)
        self.assertIs(cm.exception, error)
        request.assert_called_once()


class TestRunCodeFlow(unittest.TestCase):
    """Tests for run_code_flow() with mocked initialize_code() and finalize()."""

    INSTRUCTIONS = ["To log in with single sign-on, open the following URL in a browser:",
                    AUTHSERVER + "/sso/authorize/sess123",
                    "Then enter the one-time code shown in the browser."]

    def _run_flow(self, entries, finalize_results=(TOKEN,), initialize_error=None):
        """Runs run_code_flow(). Each entry is what the user types at one prompt, or an exception
        the prompt raises. finalize() returns or raises each of finalize_results in turn. The
        prompts, the output lines and both mocks are kept on self for the test to check.
        """
        self.prompts = []
        self.output = []
        entries = iter(entries)

        def prompt(text):
            self.prompts.append(text)
            entry = next(entries)
            if isinstance(entry, BaseException):
                raise entry
            return entry

        with patch.object(sso_login, "initialize_code", return_value={"sessionId": "sess123", "expiresIn": 300},
                          side_effect=initialize_error) as initialize, \
                patch.object(sso_login, "finalize", side_effect=list(finalize_results)) as finalize:
            self.initialize, self.finalize = initialize, finalize
            return sso_login.run_code_flow(AUTHSERVER, "0oa123", EXPIRES, prompt, self.output.append)

    def _rejected(self):
        return sso_login.SSOCodeRejected("SSO login could not be completed")

    def test_right_code(self):
        self.assertIs(self._run_flow(["abcd-2345"]), TOKEN)
        self.assertEqual(self.output, self.INSTRUCTIONS)
        self.assertEqual(self.prompts, ["One-time code: "])
        self.initialize.assert_called_once()
        authserver, idp_id, challenge = self.initialize.call_args[0]
        self.assertEqual((authserver, idp_id), (AUTHSERVER, "0oa123"))
        self.finalize.assert_called_once()
        authserver, session_id, verifier, code, expires = self.finalize.call_args[0]
        self.assertEqual((authserver, session_id, code, expires), (AUTHSERVER, "sess123", "ABCD2345", EXPIRES))
        self.assertEqual(s256(verifier), challenge)

    def test_invalid_code_asks_again(self):
        self.assertIs(self._run_flow(["abcd", "", "abcd-2345"]), TOKEN)
        self.assertEqual(self.output[3:], [INVALID_CODE_MESSAGE, INVALID_CODE_MESSAGE])
        self.assertEqual(len(self.prompts), 3)
        self.assertEqual(self.finalize.call_args_list, [call(AUTHSERVER, "sess123", ANY, "ABCD2345", EXPIRES)])

    def test_rejected_code_then_right_code(self):
        self.assertIs(self._run_flow(["ABCD2345", "ABCD2346"], [self._rejected(), TOKEN]), TOKEN)
        self.assertEqual(self.output[3:], ["Incorrect one-time code"])
        self.assertEqual([c[0][3] for c in self.finalize.call_args_list], ["ABCD2345", "ABCD2346"])

    def test_three_rejected_codes(self):
        with self.assertRaises(sso_login.SSOLoginEnded) as cm:
            self._run_flow(["ABCD2345", "ABCD2346", "ABCD2347"],
                           [self._rejected(), self._rejected(), self._rejected()])
        self.assertEqual(str(cm.exception), 'Incorrect one-time code. Run "dx login" again.')
        self.assertEqual(self.output[3:], ["Incorrect one-time code", "Incorrect one-time code"])
        self.assertEqual(self.finalize.call_count, 3)
        self.assertEqual(len(self.prompts), 3)

    def test_invalid_codes_do_not_use_an_attempt(self):
        self.assertIs(self._run_flow(["ABCD2345", "x", "ABCD2346", "", "ABCD2347"],
                                     [self._rejected(), self._rejected(), TOKEN]), TOKEN)
        self.assertEqual(self.finalize.call_count, 3)

    @parameterized.expand([
        ("The SSO login session expired. Run dx login again.",),
        ("The SSO one-time code expired. Run dx login again.",),
        ("Your network address changed during login. Run dx login again.",),
    ])
    def test_login_ended_passes_through(self, message):
        error = sso_login.SSOLoginEnded(message)
        with self.assertRaises(sso_login.SSOLoginEnded) as cm:
            self._run_flow(["ABCD2345"], [error])
        self.assertIs(cm.exception, error)
        self.finalize.assert_called_once()
        self.assertEqual(len(self.prompts), 1)

    @parameterized.expand([
        (KeyboardInterrupt(),),
        (EOFError(),),
    ])
    def test_ctrl_c_and_end_of_input_pass_through(self, error):
        with self.assertRaises(type(error)):
            self._run_flow([error])
        self.finalize.assert_not_called()

    def test_sso_unavailable(self):
        with self.assertRaises(sso_login.SSOUnavailable):
            self._run_flow([], initialize_error=sso_login.SSOUnavailable("not supported"))
        self.assertEqual(self.output, [])
        self.assertEqual(self.prompts, [])

    def test_other_errors_not_retried(self):
        error = api_error("InternalError", code=500)
        with self.assertRaises(DXAPIError) as cm:
            self._run_flow(["ABCD2345", "ABCD2345"], [error])
        self.assertIs(cm.exception, error)
        self.finalize.assert_called_once()
        self.assertEqual(len(self.prompts), 1)

    def test_typed_code_never_shown(self):
        self._run_flow(["qrst-9870", "qrst-uvwx", "wxyz-2345"], [self._rejected(), TOKEN])
        for line in self.output:
            self.assertNotRegex(line.upper(), "QRST|UVWX|WXYZ")


class TestRunLoopbackFlow(unittest.TestCase):
    """Tests for run_loopback_flow() with a listener test double."""

    INSTRUCTIONS = ["Opening your browser to sign in to DNAnexus.",
                    "If it doesn't open automatically, visit this URL in a browser on this computer:",
                    "",
                    "  " + AUTHSERVER + "/sso/authorize/sess123",
                    "",
                    "Waiting for authorization. Press Ctrl-C to cancel."]

    def _run_flow(self, callback=("ABCD2345", None), initialize_error=None, finalize_error=None,
                  wait_error=None, expires_in=300):
        self.output = []
        self.events = []
        self.listener = Mock(spec=sso_login.LoopbackListener)
        self.listener.redirect_url = REDIRECT_URL
        self.listener.set_finalize_result.side_effect = (
            lambda error=None: self.events.append(("finalize_result", error))
        )
        self.listener.close.side_effect = lambda: self.events.append(("close",))
        if wait_error is None:
            self.listener.wait_for_callback.return_value = callback
        else:
            self.listener.wait_for_callback.side_effect = wait_error

        def finish_login(*_args):
            self.events.append(("finalize",))
            if finalize_error is not None:
                raise finalize_error
            return TOKEN

        with patch.object(sso_login, "initialize_loopback",
                          return_value={"sessionId": "sess123", "expiresIn": expires_in},
                          side_effect=initialize_error) as initialize, \
                patch.object(sso_login, "finalize", side_effect=finish_login) as finalize, \
                patch.object(sso_login, "_launch_browser", return_value=None) as launch_browser:
            self.initialize = initialize
            self.finalize = finalize
            self.launch_browser = launch_browser
            return sso_login.run_loopback_flow(AUTHSERVER, "0oa123", EXPIRES, self.listener,
                                               self.output.append)

    def test_success(self):
        self.assertIs(self._run_flow(), TOKEN)
        self.assertEqual(self.output, self.INSTRUCTIONS)
        self.initialize.assert_called_once()
        authserver, idp_id, challenge, redirect_url = self.initialize.call_args[0]
        self.assertEqual((authserver, idp_id, redirect_url), (AUTHSERVER, "0oa123", REDIRECT_URL))
        self.listener.set_expected_state.assert_called_once_with("sess123")
        self.launch_browser.assert_called_once_with(AUTHSERVER + "/sso/authorize/sess123")
        self.listener.wait_for_callback.assert_called_once_with(300)
        self.finalize.assert_called_once()
        authserver, session_id, verifier, code, expires = self.finalize.call_args[0]
        self.assertEqual((authserver, session_id, code, expires),
                         (AUTHSERVER, "sess123", "ABCD2345", EXPIRES))
        self.assertEqual(s256(verifier), challenge)
        self.listener.set_finalize_result.assert_called_once_with()
        self.listener.close.assert_called_once_with()
        self.assertEqual(
            self.events,
            [("finalize",), ("finalize_result", None), ("close",)],
        )

    def test_browser_launcher_returning_none_still_prints_link(self):
        self.assertIs(self._run_flow(), TOKEN)
        self.assertEqual(self.output, self.INSTRUCTIONS)
        self.listener.wait_for_callback.assert_called_once_with(300)
        self.listener.close.assert_called_once_with()

    def test_received_authserver_failure(self):
        with self.assertRaises(sso_login.SSOLoginEnded) as cm:
            self._run_flow(callback=(None, "Access denied"))
        self.assertEqual(str(cm.exception), "Access denied")
        self.finalize.assert_not_called()
        self.listener.set_finalize_result.assert_called_once_with("Access denied")
        self.listener.close.assert_called_once_with()
        self.assertEqual(
            self.events,
            [("finalize_result", "Access denied"), ("close",)],
        )

    def test_initialize_error_passes_through(self):
        error = api_error("InvalidInput", code=422)
        with self.assertRaises(DXAPIError) as cm:
            self._run_flow(initialize_error=error)
        self.assertIs(cm.exception, error)
        self.listener.set_expected_state.assert_not_called()
        self.launch_browser.assert_not_called()
        self.listener.wait_for_callback.assert_not_called()
        self.finalize.assert_not_called()
        self.listener.set_finalize_result.assert_not_called()
        self.listener.close.assert_called_once_with()
        self.assertEqual(self.events, [("close",)])

    def test_rejected_code_ends_login(self):
        error = sso_login.SSOCodeRejected("SSO login could not be completed")
        with self.assertRaises(sso_login.SSOLoginEnded) as cm:
            self._run_flow(finalize_error=error)
        message = 'Login could not be completed. Run "dx login" again.'
        self.assertEqual(str(cm.exception), message)
        self.listener.set_finalize_result.assert_called_once_with(message)
        self.listener.close.assert_called_once_with()
        self.assertEqual(
            self.events,
            [("finalize",), ("finalize_result", message), ("close",)],
        )

    def test_finalize_login_ended_passes_through(self):
        error = sso_login.SSOLoginEnded("The SSO login session expired. Run dx login again.")
        with self.assertRaises(sso_login.SSOLoginEnded) as cm:
            self._run_flow(finalize_error=error)
        self.assertIs(cm.exception, error)
        self.listener.set_finalize_result.assert_called_once_with(str(error))
        self.listener.close.assert_called_once_with()
        self.assertEqual(
            self.events,
            [("finalize",), ("finalize_result", str(error)), ("close",)],
        )

    def test_other_finalize_error_passes_through(self):
        error = api_error("InternalError", code=500)
        with self.assertRaises(DXAPIError) as cm:
            self._run_flow(finalize_error=error)
        self.assertIs(cm.exception, error)
        self.listener.set_finalize_result.assert_called_once_with(
            sso_login.LOOPBACK_BROWSER_FAILURE,
        )
        self.listener.close.assert_called_once_with()
        self.assertEqual(
            self.events,
            [
                ("finalize",),
                ("finalize_result", sso_login.LOOPBACK_BROWSER_FAILURE),
                ("close",),
            ],
        )

    def test_ctrl_c_during_finalize_passes_through(self):
        with self.assertRaises(KeyboardInterrupt):
            self._run_flow(finalize_error=KeyboardInterrupt())
        self.listener.set_finalize_result.assert_called_once_with(
            sso_login.LOOPBACK_BROWSER_FAILURE,
        )
        self.listener.close.assert_called_once_with()
        self.assertEqual(
            self.events,
            [
                ("finalize",),
                ("finalize_result", sso_login.LOOPBACK_BROWSER_FAILURE),
                ("close",),
            ],
        )

    def test_timeout_ends_login(self):
        with self.assertRaises(sso_login.SSOLoginEnded) as cm:
            self._run_flow(wait_error=TimeoutError(), expires_in=90)
        self.assertEqual(str(cm.exception), "Sign-in did not finish within 1 minute, 30 seconds.")
        self.listener.wait_for_callback.assert_called_once_with(90)
        self.finalize.assert_not_called()
        self.listener.set_finalize_result.assert_not_called()
        self.listener.close.assert_called_once_with()
        self.assertEqual(self.events, [("close",)])

    def test_ctrl_c_passes_through(self):
        with self.assertRaises(KeyboardInterrupt):
            self._run_flow(wait_error=KeyboardInterrupt())
        self.finalize.assert_not_called()
        self.listener.set_finalize_result.assert_not_called()
        self.listener.close.assert_called_once_with()
        self.assertEqual(self.events, [("close",)])


class TestRunSsoFlow(unittest.TestCase):
    """Tests for run_sso_flow()."""

    def test_no_gui_uses_code_flow_without_listener(self):
        with patch.object(sso_login, "can_open_browser", return_value=False), \
                patch.object(sso_login, "LoopbackListener") as listener_class, \
                patch.object(sso_login, "run_code_flow", return_value=TOKEN) as run_code_flow:
            result = sso_login.run_sso_flow(AUTHSERVER, "0oa123", EXPIRES, "prompt", "output")
        self.assertIs(result, TOKEN)
        listener_class.assert_not_called()
        run_code_flow.assert_called_once_with(AUTHSERVER, "0oa123", EXPIRES, "prompt", "output")

    def test_bind_failure_uses_code_flow(self):
        output = Mock()
        with patch.object(sso_login, "can_open_browser", return_value=True), \
                patch.object(sso_login, "LoopbackListener", side_effect=OSError("address unavailable")) \
                as listener_class, \
                patch.object(sso_login, "run_code_flow", return_value=TOKEN) as run_code_flow, \
                patch.object(sso_login, "run_loopback_flow") as run_loopback_flow:
            result = sso_login.run_sso_flow(AUTHSERVER, "0oa123", EXPIRES, "prompt", output)
        self.assertIs(result, TOKEN)
        listener_class.assert_called_once_with(sso_login.normalize_one_time_code)
        output.assert_called_once_with("Could not start a local browser callback; using code-based sign-in.")
        run_code_flow.assert_called_once_with(AUTHSERVER, "0oa123", EXPIRES, "prompt", output)
        run_loopback_flow.assert_not_called()

    def test_bound_listener_is_passed_to_loopback_flow(self):
        listener = Mock(spec=sso_login.LoopbackListener)
        with patch.object(sso_login, "can_open_browser", return_value=True), \
                patch.object(sso_login, "LoopbackListener", return_value=listener) as listener_class, \
                patch.object(sso_login, "run_code_flow") as run_code_flow, \
                patch.object(sso_login, "run_loopback_flow", return_value=TOKEN) as run_loopback_flow:
            result = sso_login.run_sso_flow(AUTHSERVER, "0oa123", EXPIRES, "prompt", "output")
        self.assertIs(result, TOKEN)
        listener_class.assert_called_once_with(sso_login.normalize_one_time_code)
        run_loopback_flow.assert_called_once_with(AUTHSERVER, "0oa123", EXPIRES, listener, "output")
        run_code_flow.assert_not_called()

    def test_loopback_errors_do_not_fall_back(self):
        errors = [
            api_error("InvalidInput", code=422),
            sso_login.SSOLoginEnded("Sign-in did not finish within 5 minutes."),
            KeyboardInterrupt(),
        ]
        for error in errors:
            with self.subTest(error=type(error).__name__):
                listener = Mock(spec=sso_login.LoopbackListener)
                with patch.object(sso_login, "can_open_browser", return_value=True), \
                        patch.object(sso_login, "LoopbackListener", return_value=listener), \
                        patch.object(sso_login, "run_code_flow") as run_code_flow, \
                        patch.object(sso_login, "run_loopback_flow", side_effect=error):
                    with self.assertRaises(type(error)) as cm:
                        sso_login.run_sso_flow(AUTHSERVER, "0oa123", EXPIRES, "prompt", "output")
                self.assertIs(cm.exception, error)
                run_code_flow.assert_not_called()


if __name__ == "__main__":
    unittest.main()
