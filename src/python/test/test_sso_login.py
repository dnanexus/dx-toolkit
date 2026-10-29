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

import base64
import hashlib
import unittest
from unittest.mock import ANY, call, patch

import urllib3
from parameterized import parameterized

import dxpy
from dxpy.cli import sso_login
from dxpy.exceptions import DXAPIError, HTTPErrorWithContent

AUTHSERVER = "https://auth.dnanexus.com"
INVALID_CODE_MESSAGE = "Invalid one-time code. Enter the 8-character code shown in the browser."
TOKEN = {"access_token": "token", "token_type": "bearer", "expires_in": 64800,
         "token_signature": "signature", "user_id": "user-alice"}


def api_error(name, message="message", code=400):
    """The exception DXHTTPRequest raises for a JSON error response."""
    error_class = getattr(dxpy.exceptions, name, DXAPIError)
    return error_class({"error": {"type": name, "message": message}}, code)


def http_error(status):
    """The exception DXHTTPRequest raises for a non-JSON error response."""
    return HTTPErrorWithContent("{} [Time=0 RequestID=1]".format(status), "<html></html>")


def s256(verifier):
    return base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest()).rstrip(b"=").decode("ascii")


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
        request.assert_called_once_with("/user/getSSOId", {"id": "user-alice"}, auth=None, max_retries=0)

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


class TestInitialize(unittest.TestCase):
    """Tests for initialize() with a mocked DXHTTPRequest."""

    def test_request(self):
        response = {"sessionId": "sess123", "expiresIn": 300}
        with patch("dxpy.DXHTTPRequest", return_value=response) as request:
            self.assertIs(sso_login.initialize(AUTHSERVER, "0oa123", "challenge"), response)
        request.assert_called_once_with(AUTHSERVER + "/sso/initialize",
                                        {"type": "code", "codeChallenge": "challenge", "idpId": "0oa123"},
                                        prepend_srv=False, auth=None, max_retries=0)

    def test_no_sso_routes(self):
        with patch("dxpy.DXHTTPRequest", side_effect=http_error("404 NOT FOUND")):
            with self.assertRaises(sso_login.SSOUnavailable) as cm:
                sso_login.initialize(AUTHSERVER, "0oa123", "challenge")
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
                sso_login.initialize(AUTHSERVER, "0oa123", "challenge")
        self.assertIs(cm.exception, error)
        request.assert_called_once()


class TestFinalize(unittest.TestCase):
    """Tests for finalize() with a mocked DXHTTPRequest."""

    def test_request(self):
        with patch("dxpy.DXHTTPRequest", return_value=TOKEN) as request:
            self.assertIs(sso_login.finalize(AUTHSERVER, "sess123", "verifier", "ABCD2345"), TOKEN)
        # Exactly these three fields: no "expires", the auth server sets the token lifetime
        request.assert_called_once_with(AUTHSERVER + "/sso/finalize",
                                        {"sessionId": "sess123", "cliCodeVerifier": "verifier",
                                         "oneTimeCode": "ABCD2345"},
                                        prepend_srv=False, auth=None, max_retries=0)

    def test_code_rejected(self):
        error = api_error("SSOLoginFailedError", "SSO login could not be completed")
        with patch("dxpy.DXHTTPRequest", side_effect=error):
            with self.assertRaises(sso_login.SSOCodeRejected) as cm:
                sso_login.finalize(AUTHSERVER, "sess123", "verifier", "ABCD2345")
        self.assertEqual(str(cm.exception), "SSO login could not be completed")

    @parameterized.expand([
        ("SSOSessionExpiredError", "The SSO login session expired. Run dx login again."),
        ("SSOOneTimeCodeExpiredError", "The SSO one-time code expired. Run dx login again."),
        ("SSONetworkChangedError", "Your network address changed during login. Run dx login again."),
    ])
    def test_login_ended(self, name, message):
        with patch("dxpy.DXHTTPRequest", side_effect=api_error(name, message)):
            with self.assertRaises(sso_login.SSOLoginEnded) as cm:
                sso_login.finalize(AUTHSERVER, "sess123", "verifier", "ABCD2345")
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
                sso_login.finalize(AUTHSERVER, "sess123", "verifier", "ABCD2345")
        self.assertIs(cm.exception, error)
        request.assert_called_once()


class TestRunCodeFlow(unittest.TestCase):
    """Tests for run_code_flow() with mocked initialize() and finalize()."""

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

        with patch.object(sso_login, "initialize", return_value={"sessionId": "sess123", "expiresIn": 300},
                          side_effect=initialize_error) as initialize, \
                patch.object(sso_login, "finalize", side_effect=list(finalize_results)) as finalize:
            self.initialize, self.finalize = initialize, finalize
            return sso_login.run_code_flow(AUTHSERVER, "0oa123", prompt, self.output.append)

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
        authserver, session_id, verifier, code = self.finalize.call_args[0]
        self.assertEqual((authserver, session_id, code), (AUTHSERVER, "sess123", "ABCD2345"))
        self.assertEqual(s256(verifier), challenge)

    def test_invalid_code_asks_again(self):
        self.assertIs(self._run_flow(["abcd", "", "abcd-2345"]), TOKEN)
        self.assertEqual(self.output[3:], [INVALID_CODE_MESSAGE, INVALID_CODE_MESSAGE])
        self.assertEqual(len(self.prompts), 3)
        self.assertEqual(self.finalize.call_args_list, [call(AUTHSERVER, "sess123", ANY, "ABCD2345")])

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


if __name__ == "__main__":
    unittest.main()
