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
This submodule contains the single sign-on (SSO) login used by "dx login".

When a local graphical browser is available, the Loopback flow opens it and
receives the authorization callback on a local listener. Otherwise, the Code
flow prints a link and asks the user to enter the one-time code shown by the
browser.
'''

from __future__ import print_function, unicode_literals, division, absolute_import

import base64
import hashlib
import os
import secrets
import sys
import webbrowser
from urllib.parse import urlsplit

import dxpy
from ..exceptions import DXAPIError, DXCLIError, HTTPErrorWithContent
from ..utils.pretty_print import format_timedelta
from .sso_loopback_listener import LoopbackListener

# One-time codes have no 0, O, 1 or I, so that they are easy to read and type
ONE_TIME_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
ONE_TIME_CODE_LENGTH = 8

CODE_ATTEMPTS = 3
DEFAULT_PORTS = {"http": 80, "https": 443}

# Finalize errors after which the session cannot be used again
FINALIZE_ENDED_ERRORS = ("SSOSessionExpiredError", "SSOOneTimeCodeExpiredError", "SSONetworkChangedError")


class SSOUnavailable(DXCLIError):
    '''
    SSO login cannot start, for example because the auth server does not
    support it or the user's identity provider is not set up for it. The
    message tells the user what to do instead.
    '''


class SSOCodeRejected(DXCLIError):
    '''
    The auth server did not accept the one-time code. The SSO session is still
    usable, so the user may type the code again.
    '''


class SSOLoginEnded(DXCLIError):
    '''
    SSO login started but cannot finish, for example because the session or
    the one-time code expired. The message tells the user to run "dx login"
    again.
    '''


def make_pkce_pair():
    """
    Returns a new PKCE (RFC 7636) verifier and its S256 challenge, for one
    login. The verifier must stay in memory: never print, log or save it.
    """
    verifier = secrets.token_urlsafe(32)
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return verifier, challenge


def normalize_one_time_code(text):
    """
    Removes whitespace and "-" from a typed one-time code and upper-cases it.
    Raises ValueError if it is not a valid code; the message is meant for the
    user and never repeats what was typed.
    """
    code = "".join(text.split()).replace("-", "").upper()
    if len(code) != ONE_TIME_CODE_LENGTH or not set(code) <= set(ONE_TIME_CODE_ALPHABET):
        raise ValueError("Invalid one-time code. Enter the {}-character code shown in the browser."
                         .format(ONE_TIME_CODE_LENGTH))
    return code


def get_sso_id_args(username):
    # /user/getSSOId takes either "email" or a user "id", not a raw username.
    # Usernames cannot contain "@", so input with "@" is an email.
    if "@" in username:
        return {"email": username}
    if username.startswith("user-"):
        return {"id": username}
    return {"id": "user-" + username}


def authorize_url(authserver, session_id):
    """
    Returns the sign-in link for the session, without the port when it is the
    default for the scheme (e.g. ":443" for https).
    """
    url = urlsplit(authserver)
    if url.port is not None and url.port == DEFAULT_PORTS.get(url.scheme):
        url = url._replace(netloc=url.netloc.rsplit(":", 1)[0])
    return url.geturl() + "/sso/authorize/" + session_id


def lookup_idp(username):
    """
    Returns the identity provider ID if *username* signs in with SSO, or None.
    Never retried, and never raises: any error means "not SSO", so the user
    gets the password prompt as before.
    """
    try:
        response = dxpy.DXHTTPRequest("/user/getSSOId", get_sso_id_args(username), auth=None, max_retries=0)
        return response.get("SSOId") or None
    except Exception:
        return None


def can_open_browser():
    """
    Returns whether a local graphical browser can be launched.

    Remote sessions and headless Linux environments use Code flow instead.
    """
    if os.environ.get("SSH_CONNECTION") or os.environ.get("SSH_TTY"):
        return False
    if sys.platform.startswith("linux") and not (
            os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")):
        return False

    try:
        webbrowser.get()
    except webbrowser.Error:
        return False

    return True


def _initialize(authserver, body):
    """
    Starts an SSO session with *body* and returns the response. Never retried.
    Raises SSOUnavailable if the auth server does not support SSO login.
    """
    try:
        return dxpy.DXHTTPRequest(authserver + "/sso/initialize", body,
                                  prepend_srv=False, auth=None, max_retries=0)
    except HTTPErrorWithContent as e:
        # A non-JSON 404 means the auth server has no /sso routes. DXHTTPRequest
        # starts the message of a non-JSON error with the HTTP status.
        if str(e).startswith("404 "):
            raise SSOUnavailable('Logging in with single sign-on is not supported. Use the Profile page on the '
                                 'DNAnexus website to create an API token, then run "dx login --token" to log '
                                 'in with it.')
        raise


def initialize_code(authserver, idp_id, challenge):
    """
    Starts an SSO code-flow session and returns the response ("sessionId",
    "expiresIn"). Never retried. Raises SSOUnavailable if the auth server does
    not support SSO login.
    """
    return _initialize(authserver, {"type": "code", "codeChallenge": challenge, "idpId": idp_id})


def initialize_loopback(authserver, idp_id, challenge, redirect_url):
    """
    Starts an SSO Loopback session and returns the response ("sessionId",
    "expiresIn"). Never retried. Raises SSOUnavailable if the auth server does
    not support SSO login.
    """
    return _initialize(authserver, {"type": "loopback", "codeChallenge": challenge, "idpId": idp_id,
                                    "redirectUrl": redirect_url})


def finalize(authserver, session_id, verifier, code, expires):
    """
    Exchanges a one-time code for an API token that expires at *expires*
    (milliseconds since the epoch). Never retried, because the auth server
    uses up the code. Raises SSOCodeRejected if the code was not accepted, or
    SSOLoginEnded if the session or the code expired or the network address
    changed. Other errors are raised unchanged.
    """
    try:
        return dxpy.DXHTTPRequest(authserver + "/sso/finalize",
                                  {"sessionId": session_id, "cliCodeVerifier": verifier, "oneTimeCode": code,
                                   "expires": expires},
                                  prepend_srv=False, auth=None, max_retries=0)
    except DXAPIError as e:
        if e.name == "SSOLoginFailedError":
            raise SSOCodeRejected(e.msg)
        if e.name in FINALIZE_ENDED_ERRORS:
            raise SSOLoginEnded(e.msg)
        raise


def run_code_flow(authserver, idp_id, expires, prompt, output):
    """
    Prints the sign-in link, reads the one-time code with *prompt* (e.g.
    getpass.getpass), and returns the response from finalize() for a token
    that expires at *expires*. A code that is not valid is asked for again;
    the login ends after 3 rejected codes.
    *output* shows a line to the user (e.g. print). Never opens a browser.
    Raises SSOUnavailable or SSOLoginEnded if the login cannot complete.
    """
    verifier, challenge = make_pkce_pair()
    session_id = initialize_code(authserver, idp_id, challenge)["sessionId"]
    attempt = 1

    output("To log in with single sign-on, open the following URL in a browser:")
    output(authorize_url(authserver, session_id))
    output("Then enter the one-time code shown in the browser.")

    while True:
        try:
            code = normalize_one_time_code(prompt("One-time code: "))
        except ValueError as e:
            output(str(e))
            continue
        try:
            return finalize(authserver, session_id, verifier, code, expires)
        except SSOCodeRejected:
            if attempt == CODE_ATTEMPTS:
                raise SSOLoginEnded('Incorrect one-time code. Run "dx login" again.')
            output("Incorrect one-time code")
            attempt += 1


def run_loopback_flow(authserver, idp_id, expires, listener: LoopbackListener, output):
    """
    Runs one browser-based SSO login using an already-bound listener.

    Waits for the callback, exchanges its code for a token, and returns the
    finalize response. Always closes the listener.
    """
    try:
        verifier, challenge = make_pkce_pair()
        initialized = initialize_loopback(authserver, idp_id, challenge, listener.redirect_url)
        session_id = initialized["sessionId"]
        listener.set_expected_state(session_id)
        url = authorize_url(authserver, session_id)

        output("Opening your browser to sign in to DNAnexus.")
        try:
            webbrowser.open(url, new=2)
        except (OSError, webbrowser.Error):
            pass
        output("If it doesn't open automatically, visit this URL in a browser on this computer:")
        output("")
        output("  " + url)
        output("")
        output("Waiting for authorization. Press Ctrl-C to cancel.")

        timeout = initialized["expiresIn"]
        try:
            code, error = listener.wait_for_callback(timeout)
        except TimeoutError:
            duration = format_timedelta(timeout, in_seconds=True, auto_singulars=True)
            raise SSOLoginEnded("Sign-in did not finish within {}.".format(duration))

        if error is not None:
            raise SSOLoginEnded(error)

        try:
            result = finalize(authserver, session_id, verifier, code, expires)
        except SSOCodeRejected:
            message = 'Login could not be completed. Run "dx login" again.'
            listener.report_finalize_result(error=message)
            raise SSOLoginEnded(message)
        except Exception as e:
            listener.report_finalize_result(error=str(e))
            raise

        listener.report_finalize_result()
        return result
    finally:
        listener.close()


def run_sso_flow(authserver, idp_id, expires, prompt, output):
    """
    Runs the SSO login flow and returns the response containing the API token.
    """
    if not can_open_browser():
        return run_code_flow(authserver, idp_id, expires, prompt, output)

    try:
        listener = LoopbackListener(normalize_one_time_code)
    except OSError:
        output("Could not start a local browser callback; using code-based sign-in.")
        return run_code_flow(authserver, idp_id, expires, prompt, output)

    return run_loopback_flow(authserver, idp_id, expires, listener, output)
