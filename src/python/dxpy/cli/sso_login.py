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

from ..exceptions import DXCLIError


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
    Makes a new PKCE (RFC 7636) verifier and its S256 challenge. Call it once
    per login. The verifier must stay in memory: never print, log or save it.

    :returns: (verifier, challenge). The verifier is 43 characters from
        "A-Z a-z 0-9 - _". The challenge is base64url(SHA-256(verifier))
        without "=" padding, also 43 characters.
    :rtype: tuple of (str, str)
    """
    raise NotImplementedError()


def normalize_one_time_code(text):
    """
    Cleans up a one-time code as typed by the user: removes spaces and "-",
    and upper-cases it, so "abcd-2345" and "ABCD 2345" are both accepted.

    :param text: the code as typed
    :type text: str
    :returns: the 8-character code, using only the characters
        "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    :rtype: str
    :raises ValueError: if the cleaned code has the wrong length or a character
        outside that alphabet. The message is meant for the user and never
        repeats what was typed.
    """
    raise NotImplementedError()


def sso_lookup_body(username):
    """
    Builds the /user/getSSOId request body for what the user typed at the
    "Username" prompt.

    :param username: an email address, a user ID ("user-alice"), or a
        username ("alice")
    :type username: str
    :returns: {"email": username} if it contains "@", {"id": username} if it
        starts with "user-", or {"id": "user-" + username} otherwise
    :rtype: dict
    """
    raise NotImplementedError()


def authorize_url(authserver, session_id):
    """
    Builds the link the user opens in a browser to sign in.

    :param authserver: the auth server URL, as returned by
        :func:`dxpy.get_auth_server_name`
    :type authserver: str
    :param session_id: the session ID from :func:`initialize`. It is treated as
        an opaque string.
    :type session_id: str
    :returns: "<authserver>/sso/authorize/<session_id>", without the port when
        it is the default for the scheme (e.g. ":443" for https)
    :rtype: str
    """
    raise NotImplementedError()


def lookup_idp(username):
    """
    Asks the API server whether *username* signs in with SSO. No token is
    sent, and the call is never retried.

    The lookup only picks the login flow, so it never fails: any error is
    treated as "not SSO", and the user gets the password prompt as before.

    :param username: what the user typed at the "Username" prompt
    :type username: str
    :returns: the user's identity provider ID, or None if the user does not
        sign in with SSO or the lookup failed
    :rtype: str or None
    """
    return None


def initialize(authserver, idp_id, challenge):
    """
    Starts an SSO code-flow session on the auth server. The call is never
    retried.

    :param authserver: the auth server URL
    :type authserver: str
    :param idp_id: the identity provider ID from :func:`lookup_idp`
    :type idp_id: str
    :param challenge: the PKCE challenge from :func:`make_pkce_pair`
    :type challenge: str
    :returns: the response, {"sessionId": str, "expiresIn": int}, where
        expiresIn is the session lifetime in seconds
    :rtype: dict
    :raises SSOUnavailable: if the auth server has no SSO support, does not
        know the identity provider, is rate limiting, or cannot be reached
    """
    raise NotImplementedError()


def finalize(authserver, session_id, verifier, code):
    """
    Exchanges a one-time code for an API token. The call is never retried,
    because the auth server uses up the code on success. No token lifetime is
    sent: the auth server decides it.

    :param authserver: the auth server URL
    :type authserver: str
    :param session_id: the session ID from :func:`initialize`
    :type session_id: str
    :param verifier: the PKCE verifier from :func:`make_pkce_pair`
    :type verifier: str
    :param code: the one-time code from :func:`normalize_one_time_code`
    :type code: str
    :returns: the response, in the same shape as /system/newAuthToken:
        {"access_token", "token_type", "expires_in", "token_signature",
        "user_id"}, where expires_in is the token lifetime in seconds
    :rtype: dict
    :raises SSOCodeRejected: if the code was not accepted. The session can
        still be used.
    :raises SSOLoginEnded: if the session or the code expired, the network
        address changed, the account is locked, the auth server is rate
        limiting, or it cannot be reached
    :raises DXAPIError: for any other error from the auth server
    """
    raise NotImplementedError()


def run_code_flow(authserver, idp_id, prompt, output):
    """
    Runs the whole SSO code flow for one login: starts a session, shows the
    sign-in link, reads the one-time code (up to 3 attempts), and exchanges it
    for an API token. It never opens a browser.

    :param authserver: the auth server URL
    :type authserver: str
    :param idp_id: the identity provider ID from :func:`lookup_idp`
    :type idp_id: str
    :param prompt: reads the code without showing it, e.g.
        :func:`getpass.getpass`. Called with the prompt text, returns what
        was typed.
    :type prompt: callable
    :param output: shows a line to the user, e.g. :func:`print`
    :type output: callable
    :returns: the response from :func:`finalize`, unchanged
    :rtype: dict
    :raises SSOUnavailable: if stdin is not a terminal, or from
        :func:`initialize`
    :raises SSOLoginEnded: from :func:`finalize`, or after 3 rejected codes
    :raises KeyboardInterrupt, EOFError: if the user ends input at the prompt
    """
    raise NotImplementedError()
