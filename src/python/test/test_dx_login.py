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
import json
import os
import re
import secrets
import shlex
import shutil
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import ANY
from urllib.parse import urlencode, urlsplit

import pexpect

from dxpy.exceptions import EXPECTED_ERR_EXIT_STATUS
from dxpy_testutil import override_environment, s256

spawn_extra_args = {"encoding": "utf-8"}

PASSWORD = "password"
OTP = "123456"
SSO_ID = "0oa123"
ONE_TIME_CODE = "WXYZ2345"
PASSWORD_TOKEN = {"access_token": "password-token", "token_type": "Bearer"}
SSO_TOKEN = {"access_token": "sso-token", "token_type": "bearer", "expires_in": 64800,
             "token_signature": "signature", "user_id": "user-alice"}
NOT_FOUND_PAGE = "<html><title>404 Not Found</title></html>"
FAKE_BROWSER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fake_browser.py")


class FakeServer(HTTPServer):
    """
    A fake API server and auth server on one local port, for the requests that
    "dx login" makes. Each request is kept in *requests* as a (path, body)
    pair. A path in *errors* gets that (status, content) response instead.
    """

    def __init__(self):
        HTTPServer.__init__(self, ("127.0.0.1", 0), FakeServerHandler)
        self.requests = []
        self.errors = {}
        self.sso_id = ""
        self.otp_required = False
        self.expires_in = SSO_TOKEN["expires_in"]
        self.session_expires_in = 300
        self.authorization_error = None
        self.sessions = {}

    def approve(self, session_id):
        """Signs in to the SSO session, as the browser does, and returns the one-time code."""
        self.sessions[session_id]["code"] = ONE_TIME_CODE
        return ONE_TIME_CODE

    def respond(self, path, body):
        if path in self.errors:
            return self.errors[path]
        if path == "/user/getSSOId":
            return 200, {"SSOId": self.sso_id}
        if path == "/system/newAuthToken":
            if body["password"] != PASSWORD:
                return 401, {"error": {"type": "UsernameOrPasswordError",
                                       "message": "Incorrect username and/or password"}}
            if self.otp_required and body["otp"] is None:
                return 401, {"error": {"type": "OTPRequiredError", "message": "Verification code required"}}
            return 200, PASSWORD_TOKEN
        if path == "/sso/initialize":
            session_id = secrets.token_hex(12)
            self.sessions[session_id] = {
                "challenge": body["codeChallenge"],
                "code": None,
                "type": body["type"],
                "redirect_url": body.get("redirectUrl"),
            }
            return 200, {"sessionId": session_id, "expiresIn": self.session_expires_in}
        if path == "/sso/finalize":
            session = self.sessions.get(body["sessionId"])
            # Like the auth server, one error for a wrong session, code or verifier
            if (session is None or body["oneTimeCode"] != session["code"]
                    or s256(body["cliCodeVerifier"]) != session["challenge"]):
                return 400, {"error": {"type": "SSOLoginFailedError",
                                       "message": "SSO login could not be completed"}}
            return 200, dict(SSO_TOKEN, expires_in=self.expires_in)
        return 404, NOT_FOUND_PAGE

    def authorize(self, session_id):
        """Returns the Loopback callback URL produced by browser authorization."""
        session = self.sessions.get(session_id)
        if session is None or session["type"] != "loopback" or session["redirect_url"] is None:
            return None

        if self.authorization_error is None:
            query = [("code", self.approve(session_id)), ("state", session_id)]
        else:
            query = [("error", self.authorization_error), ("state", session_id)]
        return session["redirect_url"] + "?" + urlencode(query)


class FakeServerHandler(BaseHTTPRequestHandler):
    """Answers dxpy API requests and redirects browser authorization."""

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.requests.append((self.path, body))
        status, content = self.server.respond(self.path, body)
        self._send_content(status, content)

    def do_GET(self):
        self.server.requests.append((self.path, None))
        match = re.fullmatch(r"/sso/authorize/(\w+)", urlsplit(self.path).path)
        location = self.server.authorize(match.group(1)) if match is not None else None
        if location is None:
            self._send_content(404, NOT_FOUND_PAGE)
            return

        self.send_response(302)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _send_content(self, status, content):
        if isinstance(content, dict):
            content_type, content = "application/json", json.dumps(content)
        else:
            content_type = "text/html"
        data = content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format, *args):
        pass


@unittest.skipIf(sys.platform.startswith("win"), "pexpect is not supported")
class TestDXLogin(unittest.TestCase):
    """Tests for "dx login" against a fake API server and auth server, with no platform."""

    def setUp(self):
        self.fake = FakeServer()
        self.addCleanup(self.fake.server_close)
        self.authserver = "http://127.0.0.1:{}".format(self.fake.server_port)
        self.home = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.home)
        self.fake_browser = "{} {} %s".format(
            shlex.quote(sys.executable),
            shlex.quote(FAKE_BROWSER),
        )
        self.idle_browser = os.path.join(self.home, "idle-browser")
        with open(self.idle_browser, "w") as fd:
            fd.write("#!/bin/sh\nexit 0\n")
        os.chmod(self.idle_browser, 0o755)
        self.idle_browser_command = shlex.quote(self.idle_browser) + " %s"

    def _spawn(self, opts="", gui=False, browser=None):
        """Starts "dx login" against the fake server, with a new config directory.
        Everything it prints is kept in dx_login.logfile_read.
        """
        port = str(self.fake.server_port)
        browser = self.fake_browser if browser is None else browser
        env = override_environment(HOME=self.home, DX_USER_CONF_DIR=None, DX_USERNAME=None,
                                   DX_SECURITY_CONTEXT=None, DX_APISERVER_HOST="127.0.0.1",
                                   DX_APISERVER_PORT=port, DX_APISERVER_PROTOCOL="http",
                                   DX_AUTHSERVER_HOST="http://127.0.0.1", DX_AUTHSERVER_PORT=port,
                                   SSH_CONNECTION=None if gui else "test", SSH_TTY=None,
                                   DISPLAY=":99" if gui else None, WAYLAND_DISPLAY=None,
                                   BROWSER=browser)
        dx_login = pexpect.spawn("dx login --noprojects" + opts, env=env, **spawn_extra_args)
        self.addCleanup(dx_login.close)
        # Serve only after the fork, because forking a process that has threads can deadlock
        # the child. The server is already listening, so the child's requests wait until then.
        thread = threading.Thread(target=self.fake.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(self.fake.shutdown)
        dx_login.logfile_read = io.StringIO()
        dx_login.setwinsize(20, 90)
        return dx_login

    def _start_code_sso(self, opts=""):
        """Starts "dx login" for an SSO user, and signs in to the printed link as
        the browser does. Returns dx_login at the code prompt, and the code.
        """
        self.fake.sso_id = SSO_ID
        dx_login = self._spawn(opts)
        dx_login.expect("Username: ")
        dx_login.sendline("alice")
        dx_login.expect(re.escape(self.authserver + "/sso/authorize/") + r"(\w+)")
        code = self.fake.approve(dx_login.match.group(1))
        dx_login.expect("One-time code: ")
        return dx_login, code

    def _start_loopback_sso(self, browser=None):
        """Starts "dx login" for an SSO user with a detected graphical browser."""
        self.fake.sso_id = SSO_ID
        dx_login = self._spawn(gui=True, browser=browser)
        dx_login.expect("Username: ")
        dx_login.sendline("alice")
        return dx_login

    def _exit_status(self, dx_login):
        dx_login.expect(pexpect.EOF)
        dx_login.close()
        return dx_login.exitstatus

    def _saved(self, name):
        """Returns a variable from the environment.json that "dx login" wrote."""
        with open(os.path.join(self.home, ".dnanexus_config", "environment.json")) as fd:
            return json.load(fd).get(name)

    def _paths(self):
        return [path for path, body in self.fake.requests]

    def test_password_login(self):
        dx_login = self._spawn()
        dx_login.expect("Username: ")
        dx_login.sendline("alice")
        dx_login.expect("Password: ")
        dx_login.sendline(PASSWORD)
        self.assertEqual(self._exit_status(dx_login), 0)
        # The lookup says "not SSO", so there is no /sso/ request
        self.assertEqual(self._paths(), ["/user/getSSOId", "/system/newAuthToken"])
        self.assertEqual(self.fake.requests[1][1],
                         {"username": "alice", "password": PASSWORD, "otp": None, "expires": ANY})
        self.assertEqual(json.loads(self._saved("DX_SECURITY_CONTEXT")),
                         {"auth_token": "password-token", "auth_token_type": "Bearer"})
        self.assertEqual(self._saved("DX_USERNAME"), "alice")

    def test_password_wrong_then_right(self):
        dx_login = self._spawn()
        dx_login.expect("Username: ")
        dx_login.sendline("alice")
        dx_login.expect("Password: ")
        dx_login.sendline("wrong password")
        dx_login.expect("Incorrect username and/or password")
        dx_login.expect_exact("Username [alice]: ")
        dx_login.sendline()
        dx_login.expect("Password: ")
        dx_login.sendline(PASSWORD)
        self.assertEqual(self._exit_status(dx_login), 0)
        # Each typed username is looked up
        self.assertEqual(self._paths(), ["/user/getSSOId", "/system/newAuthToken"] * 2)

    def test_password_otp(self):
        self.fake.otp_required = True
        dx_login = self._spawn()
        dx_login.expect("Username: ")
        dx_login.sendline("alice")
        dx_login.expect("Password: ")
        dx_login.sendline(PASSWORD)
        dx_login.expect("Verification code: ")
        dx_login.sendline(OTP)
        self.assertEqual(self._exit_status(dx_login), 0)
        # The username and password are reused: not asked for, and not looked up, again
        output = dx_login.logfile_read.getvalue()
        self.assertEqual((output.count("Username"), output.count("Password")), (1, 1))
        self.assertEqual(self._paths(), ["/user/getSSOId", "/system/newAuthToken", "/system/newAuthToken"])
        self.assertEqual(self.fake.requests[2][1],
                         {"username": "alice", "password": PASSWORD, "otp": OTP, "expires": ANY})

    def test_sso_loopback_login(self):
        dx_login = self._start_loopback_sso()
        dx_login.expect("Waiting for authorization")
        self.assertEqual(self._exit_status(dx_login), 0)
        self.assertEqual(
            self._paths(),
            ["/user/getSSOId", "/sso/initialize", ANY, "/sso/finalize"],
        )
        initialize = self.fake.requests[1][1]
        self.assertEqual(initialize["type"], "loopback")
        self.assertEqual(initialize["idpId"], SSO_ID)
        redirect = urlsplit(initialize["redirectUrl"])
        self.assertEqual((redirect.scheme, redirect.path), ("http", "/callback"))
        self.assertIn(redirect.hostname, ("127.0.0.1", "::1"))
        self.assertGreater(redirect.port, 0)
        self.assertRegex(self.fake.requests[2][0], r"^/sso/authorize/\w+$")
        self.assertEqual(json.loads(self._saved("DX_SECURITY_CONTEXT")),
                         {"auth_token": "sso-token", "auth_token_type": "bearer"})
        self.assertEqual(self._saved("DX_USERNAME"), "alice")
        output = dx_login.logfile_read.getvalue()
        self.assertNotIn("Password", output)
        self.assertNotIn("One-time code", output)
        self.assertNotIn(ONE_TIME_CODE, output)

    def test_sso_loopback_authserver_failure(self):
        self.fake.authorization_error = "Access denied"
        dx_login = self._start_loopback_sso()
        dx_login.expect("SSOLoginEnded: Access denied")
        self.assertEqual(self._exit_status(dx_login), EXPECTED_ERR_EXIT_STATUS)
        self.assertEqual(
            self._paths(),
            ["/user/getSSOId", "/sso/initialize", ANY],
        )
        self.assertRegex(self.fake.requests[2][0], r"^/sso/authorize/\w+$")
        self.assertNotIn("/sso/finalize", self._paths())
        self.assertNotIn("One-time code", dx_login.logfile_read.getvalue())

    def test_sso_loopback_initialize_error_does_not_fall_back(self):
        self.fake.errors["/sso/initialize"] = (
            422,
            {"error": {"type": "InvalidInput", "message": "Invalid redirectUrl"}},
        )
        dx_login = self._start_loopback_sso()
        dx_login.expect("Invalid redirectUrl")
        self.assertEqual(self._exit_status(dx_login), EXPECTED_ERR_EXIT_STATUS)
        self.assertEqual(self._paths(), ["/user/getSSOId", "/sso/initialize"])
        initialize = self.fake.requests[1][1]
        self.assertEqual(initialize["type"], "loopback")
        self.assertIn("redirectUrl", initialize)
        self.assertNotIn("One-time code", dx_login.logfile_read.getvalue())

    def test_sso_loopback_timeout(self):
        self.fake.session_expires_in = 1
        dx_login = self._start_loopback_sso(browser=self.idle_browser_command)
        dx_login.expect("Sign-in did not finish within 1 second.")
        self.assertEqual(self._exit_status(dx_login), EXPECTED_ERR_EXIT_STATUS)
        self.assertEqual(self._paths(), ["/user/getSSOId", "/sso/initialize"])
        self.assertNotIn("/sso/finalize", self._paths())
        self.assertNotIn("One-time code", dx_login.logfile_read.getvalue())

    def test_sso_code_flow_without_browser(self):
        dx_login, code = self._start_code_sso()
        # Typed as the browser shows it
        typed = code[:4] + "-" + code[4:]
        dx_login.sendline(typed)
        self.assertEqual(self._exit_status(dx_login), 0)
        self.assertEqual(self._paths(), ["/user/getSSOId", "/sso/initialize", "/sso/finalize"])
        initialize = self.fake.requests[1][1]
        self.assertEqual(initialize["idpId"], SSO_ID)
        self.assertEqual(initialize["type"], "code")
        self.assertNotIn("redirectUrl", initialize)
        self.assertEqual(json.loads(self._saved("DX_SECURITY_CONTEXT")),
                         {"auth_token": "sso-token", "auth_token_type": "bearer"})
        self.assertEqual(self._saved("DX_USERNAME"), "alice")
        output = dx_login.logfile_read.getvalue()
        self.assertNotIn("Password", output)
        self.assertNotIn(typed, output)
        self.assertNotIn(code, output)

    def test_sso_wrong_code_then_right(self):
        dx_login, code = self._start_code_sso()
        dx_login.sendline("QRST6789")
        dx_login.expect("Incorrect one-time code")
        dx_login.expect("One-time code: ")
        dx_login.sendline(code)
        self.assertEqual(self._exit_status(dx_login), 0)
        self.assertEqual(self._paths(), ["/user/getSSOId", "/sso/initialize", "/sso/finalize", "/sso/finalize"])
        output = dx_login.logfile_read.getvalue()
        self.assertNotIn("QRST6789", output)
        self.assertNotIn(code, output)

    def test_sso_not_supported(self):
        # An auth server with no /sso/ routes
        self.fake.errors["/sso/initialize"] = (404, NOT_FOUND_PAGE)
        self.fake.sso_id = SSO_ID
        dx_login = self._spawn()
        dx_login.expect("Username: ")
        dx_login.sendline("alice")
        dx_login.expect_exact("dxpy.cli.sso_login.SSOUnavailable: Logging in with single sign-on is not supported.")
        self.assertEqual(self._exit_status(dx_login), EXPECTED_ERR_EXIT_STATUS)
        self.assertEqual(self._paths(), ["/user/getSSOId", "/sso/initialize"])
        # Shown once, not again as "Login error: ..."
        self.assertNotIn("Login error", dx_login.logfile_read.getvalue())

    def test_sso_finalize_not_retried(self):
        self.fake.errors["/sso/finalize"] = (500, {"error": {"type": "InternalError", "message": "Internal error"}})
        dx_login, code = self._start_code_sso()
        dx_login.sendline(code)
        dx_login.expect("Login error: ")
        self.assertEqual(self._exit_status(dx_login), EXPECTED_ERR_EXIT_STATUS)
        self.assertEqual(self._paths().count("/sso/finalize"), 1)

    def test_sso_timeout(self):
        # A security policy can give the token a shorter lifetime than requested
        self.fake.expires_in = 3600
        start = int(time.time() * 1000)
        dx_login, code = self._start_code_sso(" --timeout 2h")
        dx_login.sendline(code)
        self.assertEqual(self._exit_status(dx_login), 0)
        end = int(time.time() * 1000)
        # The requested expiration is sent, and the closing message shows the lifetime of the token
        two_hours = 2 * 60 * 60 * 1000
        self.assertTrue(start + two_hours <= self.fake.requests[2][1]["expires"] <= end + two_hours)
        self.assertRegex(dx_login.logfile_read.getvalue(), r"will\s+expire\s+in\s+1:00:00\.")


if __name__ == "__main__":
    unittest.main()
