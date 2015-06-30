# Copyright (C) 2013-2014 DNAnexus, Inc.
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
When this package is imported, configuration values will be loaded from
the following sources in order of decreasing priority:

1. Environment variables
2. Values stored in ``~/.dnanexus_config/environment``
3. Values stored in ``/opt/dnanexus/environment``
4. Hardcoded defaults

The bindings are configured by the following environment variables:

.. envvar:: DX_SECURITY_CONTEXT

   A JSON hash containing your auth token, typically of the form
   ``{"auth_token_type": "Bearer", "auth_token": "YOUR_TOKEN"}``.

.. envvar:: DX_APISERVER_PROTOCOL

   Either ``http`` or ``https`` (usually ``https``).

.. envvar:: DX_APISERVER_HOST

   Hostname of the DNAnexus API server.

.. envvar:: DX_APISERVER_PORT

   Port of the DNAnexus API server.

.. envvar:: DX_JOB_ID

   Should only be present if run in an Execution Environment; indicates
   the ID of the currently running job.

.. envvar:: DX_WORKSPACE_ID

   Should only be present if run in an Execution Environment; indicates
   the running job's temporary workspace ID.

.. envvar:: DX_PROJECT_CONTEXT_ID

   Indicates either the project context of a running job, or the default
   project to use for a user accessing the platform from the outside.

The following fields can be used to read the current configuration
values:

.. py:data:: APISERVER_PROTOCOL

   Protocol being used to access the DNAnexus API server. Either
   ``http`` or ``https`` (usually ``https``).

.. py:data:: APISERVER_HOST

   Hostname of the DNAnexus API server.

.. py:data:: APISERVER_PORT

   Port of the DNAnexus API server.

.. py:data:: JOB_ID

   Indicates the ID of the currently running job, or None if we are not
   in an Execution Environment.

.. py:data:: WORKSPACE_ID

   Indicates the temporary workspace ID of the currently running job, or
   the current project if we are not in an Execution Environment.

.. py:data:: PROJECT_CONTEXT_ID

   Indicates either the project context of a running job, if there is
   one, or the default project that is being used, for users accessing
   the platform from the outside.

.. py:data:: USER_AGENT

   The user agent string that dxpy will send to the server with each request.

The :func:`dxpy.DXHTTPRequest` function uses the ``DX_SECURITY_CONTEXT``
and ``DX_APISERVER_*`` variables to select an API server and provide
appropriate authentication headers to it. (Note: all methods in the
:mod:`dxpy.api` module, and by extension any of the bindings methods
that make API calls, use this function.)

All object handler methods that require a project or data container ID
use by default the ``DX_WORKSPACE_ID`` (if running inside an Execution
Environment) or ``DX_PROJECT_CONTEXT_ID`` (otherwise).

The following functions can be used to override any of the settings
obtained from the environment for the duration of the session:

* :func:`dxpy.set_security_context`: to specify an authentication token
* :func:`dxpy.set_api_server_info`: to specify the API server (host, port, or protocol)
* :func:`dxpy.set_workspace_id`: to specify the default data container

To pass API server requests through an HTTP(S) proxy, set the following
environment variables:

.. envvar:: HTTP_PROXY

   HTTP proxy, in the form 'protocol://hostname:port' (e.g. 'http://10.10.1.10:3128')

.. envvar:: HTTPS_PROXY

   HTTPS proxy, in the form 'protocol://hostname:port'

'''

from __future__ import (print_function, unicode_literals)

import os, sys, json, time, logging, platform, collections, ssl, traceback
import errno
import requests
import socket

from requests.exceptions import ConnectionError, HTTPError, Timeout
from requests.auth import AuthBase
from .compat import USING_PYTHON2, expanduser

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
logging.getLogger('dxpy.packages.requests.packages.urllib3.connectionpool').setLevel(logging.ERROR)

from . import exceptions
from .toolkit_version import version as TOOLKIT_VERSION
__version__ = TOOLKIT_VERSION

snappy_available = True
if sys.version_info < (3, 0):
    try:
        import snappy
    except ImportError:
        snappy_available = False
else:
    snappy_available = False

API_VERSION = '1.0.0'
AUTH_HELPER, SECURITY_CONTEXT = None, None
JOB_ID, WORKSPACE_ID, PROJECT_CONTEXT_ID = None, None, None

DEFAULT_APISERVER_PROTOCOL = 'https'
DEFAULT_APISERVER_HOST = 'api.dnanexus.com'
DEFAULT_APISERVER_PORT = '443'

APISERVER_PROTOCOL = DEFAULT_APISERVER_PROTOCOL
APISERVER_HOST = DEFAULT_APISERVER_HOST
APISERVER_PORT = DEFAULT_APISERVER_PORT

SESSION_HANDLERS = collections.defaultdict(requests.session)

DEFAULT_RETRIES = 6
DEFAULT_TIMEOUT = 600
DEFAULT_RETRY_AFTER_503_INTERVAL = 60

_DEBUG = 0  # debug verbosity level
_UPGRADE_NOTIFY = True

USER_AGENT = "{name}/{version} ({platform})".format(name=__name__,
                                                    version=TOOLKIT_VERSION,
                                                    platform=platform.platform())

_expected_exceptions = exceptions.network_exceptions + (exceptions.DXAPIError, )

def _process_method_url_headers(method, url, headers):
    if callable(url):
        _url, _headers = url()
        _headers.update(headers)
    else:
        _url, _headers = url, headers
    # When *data* is bytes but *headers* contains Unicode text, httplib tries to concatenate them and decode
    # *data*, which should not be done. Also, per HTTP/1.1 headers must be encoded with MIME, but we'll
    # disregard that here, and just encode them with the Python default (ascii) and fail for any non-ascii
    # content. See http://tools.ietf.org/html/rfc3987 for a discussion of encoding URLs.
    # TODO: ascertain whether this is a problem in Python 3/make test
    if USING_PYTHON2:
        return method.encode(), _url.encode('utf-8'), {k.encode(): v.encode() for k, v in _headers.items()}
    else:
        return method, _url, _headers


# When any of the following errors are indicated, we are sure that the
# server never received our request and therefore the request can be
# retried (even if the request is not idempotent).
_RETRYABLE_SOCKET_ERRORS = {
    errno.ENETDOWN,     # The network was down
    errno.ENETUNREACH,  # The subnet containing the remote host was unreachable
    errno.ECONNREFUSED  # A remote host refused to allow the network connection
}


def _is_retryable_exception(e):
    """Returns True if the exception is always safe to retry.

    This is True if the client was never able to establish a connection
    to the server (for example, name resolution failed or the connection
    could otherwise not be initialized).

    Conservatively, if we can't tell whether a network connection could
    have been established, we return False.

    """
    try:
        if isinstance(e, ConnectionError):
            # Unfortunately requests doesn't seem to provide a sensible
            # API to retrieve the cause
            cause = e.args[0].args[1]
            if isinstance(cause, (socket.gaierror, socket.herror)):
                return True
            if isinstance(cause, socket.error) and cause.errno in _RETRYABLE_SOCKET_ERRORS:
                return True
        return False
    except (AttributeError, TypeError, IndexError):
        return False


def _extract_msg_from_last_exception():
    ''' Extract a useful error message from the last thrown exception '''
    last_exc_type, last_error, last_traceback = sys.exc_info()
    if isinstance(last_error, exceptions.DXAPIError):
        # Using the same code path as below would not
        # produce a useful message when the error contains a
        # 'details' hash (which would have a last line of
        # '}')
        return last_error.error_message()
    else:
        return traceback.format_exc().splitlines()[-1].strip()


def _extract_retry_after_timeout(response):
    '''Returns the time in seconds that the server is asking us to
    wait. The information is deduced from the server http response.'''
    try:
        seconds_to_wait = int(response.headers.get('retry-after', DEFAULT_RETRY_AFTER_503_INTERVAL))
    except ValueError:
        # retry-after could be formatted as absolute time
        # instead of seconds to wait. We don't know how to
        # parse that, but the apiserver doesn't generate
        # such responses anyway.
        seconds_to_wait = DEFAULT_RETRY_AFTER_503_INTERVAL
    return max(1, seconds_to_wait)


def DXHTTPRequest(resource, data, method='POST', headers=None, auth=True,
                  timeout=DEFAULT_TIMEOUT,
                  use_compression=None, jsonify_data=True, want_full_response=False,
                  decode_response_body=True, prepend_srv=True, session_handler=None,
                  max_retries=DEFAULT_RETRIES, always_retry=False, **kwargs):
    '''
    :param resource: API server route, e.g. "/record/new". If *prepend_srv* is False, a fully qualified URL is expected. If this argument is a callable, it will be called just before each request attempt, and expected to return a tuple (URL, headers). Headers returned by the callback are updated with *headers* (including headers set by this method).
    :type resource: string
    :param data: Content of the request body
    :type data: list or dict, if *jsonify_data* is True; or string or file-like object, otherwise
    :param headers: Names and values of HTTP headers to submit with the request (in addition to those needed for authentication, compression, or other options specified with the call).
    :type headers: dict
    :param auth: Overrides the *auth* value to pass through to :meth:`requests.request`. By default a token is obtained from the ``DX_SECURITY_CONTEXT``.
    :type auth: tuple, object, True (default), or None
    :param timeout: HTTP request timeout, in seconds
    :type timeout: float
    :param config: *config* value to pass through to :meth:`requests.request`
    :type config: dict
    :param use_compression: "snappy" to use Snappy compression, or None
    :type use_compression: string or None
    :param jsonify_data: If True, *data* is converted from a Python list or dict to a JSON string
    :type jsonify_data: boolean
    :param want_full_response: If True, the full :class:`requests.Response` object is returned (otherwise, only the content of the response body is returned)
    :type want_full_response: boolean
    :param decode_response_body: If True (and *want_full_response* is False), the response body is decoded and, if it is a JSON string, deserialized. Otherwise, the response body is uncompressed if transport compression is on, and returned raw.
    :type decode_response_body: boolean
    :param prepend_srv: If True, prepends the API server location to the URL
    :type prepend_srv: boolean
    :param max_retries: Maximum number of retries to perform for a request. A "failed" request is retried if any of the following is true:

                        - A response is received from the server, and the content length received does not match the "Content-Length" header.
                        - A response is received from the server, and the response has an HTTP status code in 5xx range.
                        - A response is received from the server, the "Content-Length" header is not set, and the response JSON cannot be parsed.
                        - No response is received from the server, and either *always_retry* is True or the request *method* is "GET".

    :type max_retries: int
    :param always_retry: If True, indicates that it is safe to retry a request on failure

                        - Note: It is not guaranteed that the request will *always* be retried on failure; rather, this is an indication to the function that it would be safe to do so.

    :type always_retry: boolean
    :returns: Response from API server in the format indicated by *want_full_response* and *decode_response_body*.
    :raises: :exc:`exceptions.DXAPIError` or a subclass if the server returned a non-200 status code; :exc:`requests.exceptions.HTTPError` if an invalid response was received from the server; or :exc:`requests.exceptions.ConnectionError` if a connection cannot be established.

    Wrapper around :meth:`requests.request()` that makes an HTTP
    request, inserting authentication headers and (by default)
    converting *data* to JSON.

    .. note:: Bindings methods that make API calls make the underlying
       HTTP request(s) using :func:`DXHTTPRequest`, and most of them
       will pass any unrecognized keyword arguments you have supplied
       through to :func:`DXHTTPRequest`.

    '''
    if session_handler is None:
        session_handler = SESSION_HANDLERS[os.getpid()]
    if headers is None:
        headers = {}

    global _UPGRADE_NOTIFY

    url = APISERVER + resource if prepend_srv else resource
    method = method.upper() # Convert method name to uppercase, to ease string comparisons later
    if _DEBUG >= 2:
        print(method, url, "=>\n" + json.dumps(data, indent=2), file=sys.stderr)
    elif _DEBUG > 0:
        from repr import Repr
        print(method, url, "=>", Repr().repr(data), file=sys.stderr)

    if auth is True:
        auth = AUTH_HELPER

    if 'verify' not in kwargs and 'DX_CA_CERT' in os.environ:
        kwargs['verify'] = os.environ['DX_CA_CERT']
        if os.environ['DX_CA_CERT'] == 'NOVERIFY':
            kwargs['verify'] = False
            from requests.packages import urllib3
            urllib3.disable_warnings()

    if jsonify_data:
        data = json.dumps(data)
        if 'Content-Type' not in headers and method == 'POST':
            headers['Content-Type'] = 'application/json'

    headers['DNAnexus-API'] = API_VERSION
    headers['User-Agent'] = USER_AGENT

    if use_compression == 'snappy':
        if not snappy_available:
            raise exceptions.DXError("Snappy compression requested, but the snappy module is unavailable")
        headers['accept-encoding'] = 'snappy'

    # If the input is a buffer, its data gets consumed by
    # requests.request (moving the read position). Record the initial
    # buffer position so that we can return to it if the request fails
    # and needs to be retried.
    rewind_input_buffer_offset = None
    if hasattr(data, 'seek') and hasattr(data, 'tell'):
        rewind_input_buffer_offset = data.tell()

    try_index = 0
    while True:
        success, streaming_response_truncated = True, False
        response = None
        try:
            _method, _url, _headers = _process_method_url_headers(method, url, headers)
            response = session_handler.request(_method, _url, headers=_headers, data=data,
                                               timeout=timeout, auth=auth, **kwargs)

            if _UPGRADE_NOTIFY and response.headers.get('x-upgrade-info', '').startswith('A recommended update is available') and not os.environ.has_key('_ARGCOMPLETE'):
                logger.info(response.headers['x-upgrade-info'])
                try:
                    with file(_UPGRADE_NOTIFY, 'a'):
                        os.utime(_UPGRADE_NOTIFY, None)
                except:
                    pass
                _UPGRADE_NOTIFY = False

            # If an HTTP code that is not in the 200 series is received and the content is JSON, parse it and throw the
            # appropriate error.  Otherwise, raise the usual exception.
            if response.status_code // 100 != 2:
                # response.headers key lookup is case-insensitive
                if response.headers.get('content-type', '').startswith('application/json'):
                    content = json.loads(response.content.decode('utf-8'))
                    error_class = getattr(exceptions, content["error"]["type"], exceptions.DXAPIError)
                    raise error_class(content, response.status_code)
                response.raise_for_status()

            if want_full_response:
                return response
            else:
                if 'content-length' in response.headers:
                    if int(response.headers['content-length']) != len(response.content):
                        range_str = (' (%s)' % (headers['Range'],)) if 'Range' in headers else ''
                        raise exceptions.ContentLengthError(
                            "Received response with content-length header set to %s but content length is %d%s" %
                            (response.headers['content-length'], len(response.content), range_str)
                        )

                if use_compression and response.headers.get('content-encoding', '') == 'snappy':
                    # TODO: check if snappy raises any exceptions on truncated response content
                    content = snappy.uncompress(response.content)
                else:
                    content = response.content

                if decode_response_body:
                    content = content.decode('utf-8')
                    if response.headers.get('content-type', '').startswith('application/json'):
                        try:
                            content = json.loads(content)
                            if _DEBUG >= 2:
                                t = int(response.elapsed.total_seconds()*1000)
                                print(method, url, "<=", response.status_code, "(%dms)"%t, "\n" + json.dumps(content, indent=2), file=sys.stderr)
                            elif _DEBUG > 0:
                                t = int(response.elapsed.total_seconds()*1000)
                                print(method, url, "<=", response.status_code, "(%dms)"%t, Repr().repr(content), file=sys.stderr)
                            return content
                        except ValueError:
                            # If a streaming API call (no content-length
                            # set) encounters an error it may just halt the
                            # response because it has no other way to
                            # indicate an error. Under these circumstances
                            # the client sees unparseable JSON, and we
                            # should be able to recover.
                            streaming_response_truncated = 'content-length' not in response.headers
                            raise HTTPError("Invalid JSON received from server")
                return content
            raise AssertionError('Should never reach this line: expected a result to have been returned by now')
        except Exception as e:
            success = False
            exception_msg = _extract_msg_from_last_exception()
            if isinstance(e, _expected_exceptions):
                if response is not None and response.status_code == 503:
                    seconds_to_wait = _extract_retry_after_timeout(response)
                    logger.warn("%s %s: %s. Waiting %d seconds due to server unavailability...",
                                method, url, exception_msg, seconds_to_wait)
                    time.sleep(seconds_to_wait)
                    # Note, we escape the "except" block here without
                    # incrementing try_index because 503 responses with
                    # Retry-After should not count against the number of
                    # permitted retries.
                    continue

                # Total number of allowed tries is the initial try + up to
                # (max_retries) subsequent retries.
                total_allowed_tries = max_retries + 1
                ok_to_retry = False
                # Because try_index is not incremented until we escape this
                # iteration of the loop, try_index is equal to the number of
                # tries that have failed so far, minus one. Test whether we
                # have exhausted all retries.
                if try_index + 1 < total_allowed_tries:
                    if response is None or isinstance(e, exceptions.ContentLengthError) or \
                       streaming_response_truncated:
                        ok_to_retry = always_retry or (method == 'GET') or _is_retryable_exception(e)
                    else:
                        ok_to_retry = 500 <= response.status_code < 600

                if ok_to_retry:
                    if rewind_input_buffer_offset is not None:
                        data.seek(rewind_input_buffer_offset)
                    delay = min(2 ** try_index, DEFAULT_TIMEOUT)
                    logger.warn("%s %s: %s. Waiting %d seconds before retry %d of %d...",
                                method, url, exception_msg, delay, try_index + 1, max_retries)
                    time.sleep(delay)
                    try_index += 1
                    continue

            # All retries have been exhausted OR the error is deemed not
            # retryable. Print the latest error and propagate it back to the caller.
            if not isinstance(e, exceptions.DXAPIError):
                logger.error("%s %s: %s", method, url, exception_msg)
            raise
        finally:
            if success and try_index > 0:
                logger.info("%s %s: Recovered after %d retries", method, url, try_index)

        raise AssertionError('Should never reach this line: should have attempted a retry or reraised by now')
    raise AssertionError('Should never reach this line: should never break out of loop')


class DXHTTPOAuth2(AuthBase):
    def __init__(self, security_context):
        self.security_context = security_context

    def __call__(self, r):
        if self.security_context["auth_token_type"].lower() == 'bearer':
            auth_header = self.security_context["auth_token_type"] + " " + self.security_context["auth_token"]
            r.headers[b'Authorization'] = auth_header.encode()
        else:
            raise NotImplementedError("Token types other than bearer are not yet supported")
        return r

def set_api_server_info(host=None, port=None, protocol=None):
    '''
    :param host: API server hostname
    :type host: string
    :param port: API server port. If not specified, *port* is guessed based on *protocol*.
    :type port: string
    :param protocol: Either "http" or "https"
    :type protocol: string

    Overrides the current settings for which API server to communicate
    with. Any parameters that are not explicitly specified are not
    overridden.
    '''
    global APISERVER_PROTOCOL, APISERVER_HOST, APISERVER_PORT, APISERVER
    if host is not None:
        APISERVER_HOST = host
    if port is not None:
        APISERVER_PORT = port
    if protocol is not None:
        APISERVER_PROTOCOL = protocol
    if port is None or port == '':
        APISERVER = APISERVER_PROTOCOL + "://" + APISERVER_HOST
    else:
        APISERVER = APISERVER_PROTOCOL + "://" + APISERVER_HOST + ":" + str(APISERVER_PORT)

def set_security_context(security_context):
    '''
    :param security_context: Authentication hash, usually with keys ``auth_token_type`` set to ``Bearer`` and ``auth_token`` set to the authentication token.
    :type security_context: dict

    Sets the security context to use the provided token.
    '''
    global SECURITY_CONTEXT, AUTH_HELPER
    SECURITY_CONTEXT = security_context
    AUTH_HELPER = DXHTTPOAuth2(security_context)

def set_job_id(dxid):
    """
    :param dxid: ID of a job
    :type dxid: string

    Sets the ID of the running job.

    .. warning:: This function is only really useful if you are
       developing code that will run in and interact with the Execution
       Environment, but wish to test it outside of an actual Execution
       Environment.

    """
    global JOB_ID
    JOB_ID = dxid

def set_workspace_id(dxid):
    """
    :param dxid: ID of a project or workspace
    :type dxid: string

    Sets the default data container for object creation and modification
    to the specified project or workspace.

    """

    global WORKSPACE_ID
    WORKSPACE_ID = dxid

def set_project_context(dxid):
    """
    :param dxid: Project ID
    :type dxid: string

    Sets the project context for a running job.

    .. warning:: This function is only really useful if you are
       developing code that will run in and interact with the Execution
       Environment but wish to test it outside of an actual Execution
       Environment.

       It does not change the default data container in which new
       objects are created or name resolution is performed. If you want
       to do that, use :func:`set_workspace_id` instead.

    """

    global PROJECT_CONTEXT_ID
    PROJECT_CONTEXT_ID = dxid

def get_auth_server_name(host_override=None, port_override=None):
    """
    Chooses the auth server name from the currently configured API server name.

    Raises DXError if the auth server name cannot be guessed and the overrides
    are not provided (or improperly provided).
    """
    if host_override is not None or port_override is not None:
        if host_override is None or port_override is None:
            raise exceptions.DXError("Both host and port must be specified if either is specified")
        return 'http://' + host_override + ':' + str(port_override)
    elif APISERVER_HOST == 'stagingapi.dnanexus.com':
        return 'https://stagingauth.dnanexus.com'
    elif APISERVER_HOST == 'api.dnanexus.com':
        return 'https://auth.dnanexus.com'
    else:
        err_msg = "Could not determine which auth server is associated with {apiserver}."
        raise exceptions.DXError(err_msg.format(apiserver=APISERVER_HOST))

from .utils.config import DXConfig as _DXConfig
config = _DXConfig()

from .bindings import *
from .dxlog import DXLogHandler
from .utils.exec_utils import run, entry_point
