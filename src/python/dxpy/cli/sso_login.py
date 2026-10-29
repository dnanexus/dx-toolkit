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

In the code flow, "dx login" asks the API server whether the user signs in
with SSO. If so, it starts an SSO session on the auth server and prints a link.
The user opens the link in a browser on any computer, signs in with their
identity provider, and types the one-time code shown in the browser back into
the terminal. The code is then exchanged for an API token.

The code flow never opens a browser and never polls for the code: the user
typing the code is what ties the browser sign-in to this terminal.
'''

from __future__ import print_function, unicode_literals, division, absolute_import

import base64
import hashlib
import secrets
from urllib.parse import urlsplit

import dxpy
from ..exceptions import DXAPIError, DXCLIError, HTTPErrorWithContent

# One-time codes have no 0, O, 1 or I, so that they are easy to read and type
ONE_TIME_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
ONE_TIME_CODE_LENGTH = 8

_CODE_ATTEMPTS = 3
_DEFAULT_PORTS = {"http": 80, "https": 443}

# Finalize errors after which the session cannot be used again
_FINALIZE_ENDED_ERRORS = ("SSOSessionExpiredError", "SSOOneTimeCodeExpiredError", "SSONetworkChangedError")


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


def sso_lookup_body(username):
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
    if url.port is not None and url.port == _DEFAULT_PORTS.get(url.scheme):
        url = url._replace(netloc=url.netloc.rsplit(":", 1)[0])
    return url.geturl() + "/sso/authorize/" + session_id


def lookup_idp(username):
    """
    Returns the identity provider ID if *username* signs in with SSO, or None.
    Never retried, and never raises: any error means "not SSO", so the user
    gets the password prompt as before.
    """
    try:
        response = dxpy.DXHTTPRequest("/user/getSSOId", sso_lookup_body(username), auth=None, max_retries=0)
        return response.get("SSOId") or None
    except Exception:
        return None


def initialize(authserver, idp_id, challenge):
    """
    Starts an SSO code-flow session and returns the response ("sessionId",
    "expiresIn"). Never retried. Raises SSOUnavailable if the auth server does
    not support SSO login.
    """
    try:
        return dxpy.DXHTTPRequest(authserver + "/sso/initialize",
                                  {"type": "code", "codeChallenge": challenge, "idpId": idp_id},
                                  prepend_srv=False, auth=None, max_retries=0)
    except HTTPErrorWithContent as e:
        # A non-JSON 404 means the auth server has no /sso routes. DXHTTPRequest
        # starts the message of a non-JSON error with the HTTP status.
        if str(e).startswith("404 "):
            raise SSOUnavailable('Logging in with single sign-on is not supported. Use the Profile page on the '
                                 'DNAnexus website to create an API token, then run "dx login --token" to log '
                                 'in with it.')
        raise


def finalize(authserver, session_id, verifier, code):
    """
    Exchanges a one-time code for an API token. Never retried, because the
    auth server uses up the code. Raises SSOCodeRejected if the code was not
    accepted, or SSOLoginEnded if the session or the code expired or the
    network address changed. Other errors are raised unchanged.
    """
    try:
        return dxpy.DXHTTPRequest(authserver + "/sso/finalize",
                                  {"sessionId": session_id, "cliCodeVerifier": verifier, "oneTimeCode": code},
                                  prepend_srv=False, auth=None, max_retries=0)
    except DXAPIError as e:
        if e.name == "SSOLoginFailedError":
            raise SSOCodeRejected(e.msg)
        if e.name in _FINALIZE_ENDED_ERRORS:
            raise SSOLoginEnded(e.msg)
        raise


def run_code_flow(authserver, idp_id, prompt, output):
    """
    Prints the sign-in link, reads the one-time code with *prompt* (e.g.
    getpass.getpass), and returns the response from finalize(). A code that is
    not valid is asked for again; the login ends after 3 rejected codes.
    *output* shows a line to the user (e.g. print). Never opens a browser.
    Raises SSOUnavailable or SSOLoginEnded if the login cannot complete.
    """
    verifier, challenge = make_pkce_pair()
    session_id = initialize(authserver, idp_id, challenge)["sessionId"]
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
            return finalize(authserver, session_id, verifier, code)
        except SSOCodeRejected:
            if attempt == _CODE_ATTEMPTS:
                raise SSOLoginEnded('Incorrect one-time code. Run "dx login" again.')
            output("Incorrect one-time code")
            attempt += 1
