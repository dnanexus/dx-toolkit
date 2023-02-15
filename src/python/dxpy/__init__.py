# Copyright (C) 2013-2016 DNAnexus, Inc.
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

from __future__ import print_function, unicode_literals, division, absolute_import

import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

import os, sys, json, time, platform, ssl, traceback
import errno
import math
import mmap
import requests
import socket
import threading
import subprocess

from collections import namedtuple

from . import exceptions
from .compat import USING_PYTHON2, BadStatusLine, StringIO, bytes, Repr
from .utils.printing import BOLD, BLUE, YELLOW, GREEN, RED, WHITE

from random import randint
from requests.auth import AuthBase
from requests.packages import urllib3
from threading import Lock
from . import ssh_tunnel_app_support

try:
    # python-3
    from urllib.parse import urlsplit
except ImportError:
    # python-2
    from urlparse import urlsplit

sequence_number_mutex = threading.Lock()
counter = 0

def _get_sequence_number():
    global counter
    with sequence_number_mutex:
        retval = counter
        counter += 1
        return retval


def configure_urllib3():
    # Disable verbose urllib3 warnings and log messages
    urllib3.disable_warnings(category=urllib3.exceptions.InsecurePlatformWarning)
    logging.getLogger('dxpy.packages.requests.packages.urllib3.connectionpool').setLevel(logging.ERROR)

configure_urllib3()

from .toolkit_version import version as TOOLKIT_VERSION
__version__ = TOOLKIT_VERSION

API_VERSION = '1.0.0'
AUTH_HELPER, SECURITY_CONTEXT = None, None
JOB_ID, WATCH_PORT, WORKSPACE_ID, PROJECT_CONTEXT_ID = None, None, None, None

DEFAULT_APISERVER_PROTOCOL = 'https'
DEFAULT_APISERVER_HOST = 'api.dnanexus.com'
DEFAULT_APISERVER_PORT = '443'

APISERVER_PROTOCOL = DEFAULT_APISERVER_PROTOCOL
APISERVER_HOST = DEFAULT_APISERVER_HOST
APISERVER_PORT = DEFAULT_APISERVER_PORT

DEFAULT_RETRIES = 6
DEFAULT_TIMEOUT = 600

_DEBUG = 0  # debug verbosity level
_UPGRADE_NOTIFY = True

INCOMPLETE_READS_NUM_SUBCHUNKS = 8

USER_AGENT = "{name}/{version} ({platform})".format(name=__name__,
                                                    version=TOOLKIT_VERSION,
                                                    platform=platform.platform())
_default_certs = requests.certs.where()
_default_headers = requests.utils.default_headers()
_default_timeout = urllib3.util.timeout.Timeout(connect=DEFAULT_TIMEOUT, read=DEFAULT_TIMEOUT)
_RequestForAuth = namedtuple('_RequestForAuth', 'method url headers')
_expected_exceptions = (exceptions.network_exceptions, exceptions.DXAPIError, BadStatusLine, exceptions.BadJSONInReply,
                        exceptions.UrllibInternalError)

# Multiple threads can ask for the pool, so we need to protect
# access and make it thread safe.
_pool_mutex = Lock()
_pool_manager = None

def _get_proxy_info(url):
    proxy_info = {}

    url_info = urlsplit(url)
    # If the url contains a username, need to separate the username/password
    # from the url
    if url_info.username:
        # Strip the username/password out of the url
        url = url_info.netloc[url_info.netloc.find('@')+1:]
        # Now get the username and possibly password
        proxy_info['proxy_url'] = '{0}://{1}'.format(url_info.scheme, url)
        if url_info.password:
            proxy_auth = '{0}:{1}'.format(url_info.username, url_info.password)
        else:
            proxy_auth = url_info.username
        proxy_info['proxy_headers'] = urllib3.make_headers(proxy_basic_auth=proxy_auth)
    else:
        # No username was given, so just take the url as is.
        proxy_info['proxy_url'] = url

    return proxy_info

def _get_env_var_proxy(print_proxy=False):
  proxy_tuple = ('http_proxy', 'HTTP_PROXY', 'https_proxy', 'HTTPS_PROXY')
  proxy = None
  for env_proxy in proxy_tuple:
    if env_proxy in os.environ:
      proxy = os.environ[env_proxy]
  if print_proxy:
    print('Using env variable %s=%s as proxy' % (env_proxy,proxy),
          file=sys.stderr)
  return proxy

def _get_pool_manager(verify, cert_file, key_file, ssl_context=None):
    global _pool_manager
    default_pool_args = dict(maxsize=32,
                             cert_reqs=ssl.CERT_REQUIRED,
                             headers=_default_headers,
                             timeout=_default_timeout)
    # Don't use the default CA bundle if the user has set the env variable
    # DX_USE_OS_CA_BUNDLE. Enabling that var will make us attempt to load
    # the default CA certs provided by the OS; see DEVEX-875.
    if 'DX_USE_OS_CA_BUNDLE' not in os.environ:
        default_pool_args.update(ca_certs=_default_certs)

    if cert_file is None and verify is None and 'DX_CA_CERT' not in os.environ:
        with _pool_mutex:
            if _pool_manager is None:
                if _get_env_var_proxy():
                    proxy_params = _get_proxy_info(_get_env_var_proxy(print_proxy=True))
                    default_pool_args.update(proxy_params)
                    _pool_manager = urllib3.ProxyManager(**default_pool_args)
                else:
                    _pool_manager = urllib3.PoolManager(**default_pool_args)
            return _pool_manager
    else:
        # This is the uncommon case, normally, we want to cache the pool
        # manager.
        pool_args = dict(default_pool_args,
                         cert_file=cert_file,
                         key_file=key_file,
                         ssl_context=ssl_context,
                         ca_certs=verify or os.environ.get('DX_CA_CERT') or requests.certs.where())
        if verify is False or os.environ.get('DX_CA_CERT') == 'NOVERIFY':
            pool_args.update(cert_reqs=ssl.CERT_NONE, ca_certs=None)
            urllib3.disable_warnings()
        if _get_env_var_proxy():
            proxy_params = _get_proxy_info(_get_env_var_proxy(print_proxy=True))
            pool_args.update(proxy_params)
            return urllib3.ProxyManager(**pool_args)
        else:
            return urllib3.PoolManager(**pool_args)


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
    if isinstance(e, urllib3.exceptions.ProtocolError):
        e = e.args[1]
    if isinstance(e, (socket.gaierror, socket.herror)):
        return True
    if isinstance(e, socket.error) and e.errno in _RETRYABLE_SOCKET_ERRORS:
        return True
    if isinstance(e, urllib3.exceptions.NewConnectionError):
        return True
    if isinstance(e, requests.exceptions.SSLError):
        return True
    if isinstance(e, urllib3.exceptions.SSLError):
        return True
    if isinstance(e, ssl.SSLError):
        return True
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


def _calculate_retry_delay(response, num_attempts):
    '''
    Returns the time in seconds that we should wait.

    :param num_attempts: number of attempts that have been made to the
        resource, including the most recent failed one
    :type num_attempts: int
    '''
    if response is not None and response.status == 503 and 'retry-after' in response.headers:
        try:
            return int(response.headers['retry-after'])
        except ValueError:
            # In RFC 2616, retry-after can be formatted as absolute time
            # instead of seconds to wait. We don't bother to parse that,
            # but the apiserver doesn't generate such responses anyway.
            pass
    if num_attempts <= 1:
        return 1
    num_attempts = min(num_attempts, 7)
    return randint(2 ** (num_attempts - 2), 2 ** (num_attempts - 1))


# Truncate the message, if the error injection flag is on, and other
# conditions hold. This causes a BadRequest 400 HTTP code, which is
# subsequentally retried.
#
# Note: the minimal upload size for S3 is 5MB. In theory, you are
# supposed to get an "EntityTooSmall" error from S3, which has a 400
# code. However, I have not observed such responses in practice.
# http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
def _maybe_truncate_request(url, data):
    MIN_UPLOAD_LEN = 16 * 1024
    if _INJECT_ERROR:
        if (randint(0, 9) == 0) and "upload" in url and len(data) > MIN_UPLOAD_LEN:
            logger.info("truncating upload data to length=%d", MIN_UPLOAD_LEN)
            return data[0:MIN_UPLOAD_LEN]
    return data


def _raise_error_for_testing(try_index=None, method='GET'):
    if _INJECT_ERROR and method == 'GET' and randint(0, 9) == 0:
        error_thrown = randint(0, 1)
        if error_thrown == 0 and try_index is None:
            raise exceptions.DXIncompleteReadsError()

        # Raise exception to test urllib3 error in downloads
        elif error_thrown == 1 and try_index is not None and try_index < 3:
            raise exceptions.UrllibInternalError()


def _debug_print_request(debug_level, seq_num, time_started, method, url, headers, jsonify_data, data):
    if debug_level >= 2:
        if not jsonify_data:
            if len(data) == 0:
                formatted_data = '""'
            else:
                formatted_data = "<file data of length " + str(len(data)) + ">"
        else:
            try:
                if _DEBUG >= 3:
                    formatted_data = json.dumps(data, indent=2)
                else:
                    formatted_data = json.dumps(data)
            except (UnicodeDecodeError, TypeError):
                formatted_data = "<binary data>"

        printable_headers = ''
        if 'Range' in headers:
            printable_headers = " " + json.dumps({"Range": headers["Range"]})
        print("%s [%f] %s %s%s => %s\n" % (YELLOW(BOLD(">%d" % seq_num)),
                                           time_started,
                                           BLUE(method),
                                           url,
                                           printable_headers,
                                           formatted_data),
              file=sys.stderr,
              end="")
    elif debug_level > 0:
        print("%s [%f] %s %s => %s\n" % (YELLOW(BOLD(">%d" % seq_num)),
                                         time_started,
                                         BLUE(method),
                                         url,
                                         Repr().repr(data)),
              file=sys.stderr,
              end="")


def _debug_print_response(debug_level, seq_num, time_started, req_id, response_status, response_was_json, method,
                          url, content):
    if debug_level > 0:
        if response_was_json:
            if debug_level >= 3:
                content_to_print = "\n  " + json.dumps(content, indent=2).replace("\n", "\n  ")
            elif debug_level == 2:
                content_to_print = json.dumps(content)
            else:
                content_to_print = Repr().repr(content)
        else:
            content_to_print = "(%d bytes)" % len(content) if len(content) > 0 else ''

        t = int((time.time() - time_started) * 1000)
        code_format = GREEN if (200 <= response_status < 300) else RED
        print("  " + YELLOW(BOLD("<%d" % seq_num)),
              "[%f]" % time_started,
              BLUE(method),
              req_id,
              url,
              "<=",
              code_format(str(response_status)),
              WHITE(BOLD("(%dms)" % t)),
              content_to_print,
              file=sys.stderr)


def _test_tls_version():
    tls12_check_script = os.path.join(os.getenv("DNANEXUS_HOME"), "build", "tls12check.py")
    if not os.path.exists(tls12_check_script):
        return

    try:
        subprocess.check_output(['python', tls12_check_script])
    except subprocess.CalledProcessError as e:
        if e.returncode == 1:
            print (e.output)
            raise exceptions.InvalidTLSProtocol


def DXHTTPRequest(resource, data, method='POST', headers=None, auth=True,
                  timeout=DEFAULT_TIMEOUT,
                  use_compression=None, jsonify_data=True, want_full_response=False,
                  decode_response_body=True, prepend_srv=True, session_handler=None,
                  max_retries=DEFAULT_RETRIES, always_retry=False,
                  **kwargs):
    '''
    :param resource: API server route, e.g. "/record/new". If *prepend_srv* is False, a fully qualified URL is expected. If this argument is a callable, it will be called just before each request attempt, and expected to return a tuple (URL, headers). Headers returned by the callback are updated with *headers* (including headers set by this method).
    :type resource: string
    :param data: Content of the request body
    :type data: list or dict, if *jsonify_data* is True; or string or file-like object, otherwise
    :param headers: Names and values of HTTP headers to submit with the request (in addition to those needed for authentication, compression, or other options specified with the call).
    :type headers: dict
    :param auth:
        Controls the ``Authentication`` header or other means of authentication supplied with the request. If ``True``
        (default), a token is obtained from the ``DX_SECURITY_CONTEXT``. If the value evaluates to false, no action is
        taken to prepare authentication for the request. Otherwise, the value is assumed to be callable, and called with
        three arguments (method, url, headers) and expected to prepare the authentication headers by reference.
    :type auth: tuple, object, True (default), or None
    :param timeout: HTTP request timeout, in seconds
    :type timeout: float
    :param config: *config* value to pass through to :meth:`requests.request`
    :type config: dict
    :param use_compression: Deprecated
    :type use_compression: string or None
    :param jsonify_data: If True, *data* is converted from a Python list or dict to a JSON string
    :type jsonify_data: boolean
    :param want_full_response: If True, the full :class:`requests.Response` object is returned (otherwise, only the content of the response body is returned)
    :type want_full_response: boolean
    :param decode_response_body: If True (and *want_full_response* is False), the response body is decoded and, if it is a JSON string, deserialized. Otherwise, the response body is uncompressed if transport compression is on, and returned raw.
    :type decode_response_body: boolean
    :param prepend_srv: If True, prepends the API server location to the URL
    :type prepend_srv: boolean
    :param session_handler: Deprecated.
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
    if headers is None:
        headers = {}

    global _UPGRADE_NOTIFY

    seq_num = _get_sequence_number()

    url = APISERVER + resource if prepend_srv else resource
    method = method.upper()  # Convert method name to uppercase, to ease string comparisons later

    if auth is True:
        auth = AUTH_HELPER

    if auth:
        auth(_RequestForAuth(method, url, headers))

    pool_args = {arg: kwargs.pop(arg, None) for arg in ("verify", "cert_file", "key_file", "ssl_context")}
    test_retry = kwargs.pop("_test_retry_http_request", False)

    # data is a sequence/buffer or a dict
    # serialized_data is a sequence/buffer

    if jsonify_data:
        serialized_data = json.dumps(data)
        if 'Content-Type' not in headers and method == 'POST':
            headers['Content-Type'] = 'application/json'
    else:
        serialized_data = data

    # If the input is a buffer, its data gets consumed by
    # requests.request (moving the read position). Record the initial
    # buffer position so that we can return to it if the request fails
    # and needs to be retried.
    rewind_input_buffer_offset = None
    if hasattr(data, 'seek') and hasattr(data, 'tell'):
        rewind_input_buffer_offset = data.tell()

    # Maintain two separate counters for the number of tries...

    try_index = 0  # excluding 503 errors. The number of tries as given here
                   # cannot exceed (max_retries + 1).
    try_index_including_503 = 0  # including 503 errors. This number is used to
                                 # do exponential backoff.

    retried_responses = []
    _url = None
    while True:
        success, time_started = True, None
        response = None
        req_id = None
        try:
            time_started = time.time()
            _method, _url, _headers = _process_method_url_headers(method, url, headers)

            _debug_print_request(_DEBUG, seq_num, time_started, _method, _url, _headers, jsonify_data, data)

            body = _maybe_truncate_request(_url, serialized_data)

            # throws BadStatusLine if the server returns nothing
            try:
                pool_manager = _get_pool_manager(**pool_args)

                _headers['User-Agent'] = USER_AGENT
                _headers['DNAnexus-API'] = API_VERSION

                # Converted Unicode headers to ASCII and throw an error if not possible
                def ensure_ascii(i):
                    if not isinstance(i, bytes):
                        i = i.encode('ascii')
                    return i

                _headers = {ensure_ascii(k): ensure_ascii(v) for k, v in _headers.items()}
                if USING_PYTHON2:
                    encoded_url = _url
                else:
                    # This is needed for python 3 urllib
                    _headers.pop(b'host', None)
                    _headers.pop(b'content-length', None)
                    _headers.pop(b'Content-Length', None)

                    # The libraries downstream (http client) require elimination of non-ascii
                    # chars from URL.
                    # We check if the URL contains non-ascii characters to see if we need to
                    # quote it. It is important not to always quote the path (here: parts[2])
                    # since it might contain elements (e.g. HMAC for api proxy) containing
                    # special characters that should not be quoted.
                    try:
                        ensure_ascii(_url)
                        encoded_url = _url
                    except UnicodeEncodeError:
                        import urllib.parse
                        parts = list(urllib.parse.urlparse(_url))
                        parts[2] = urllib.parse.quote(parts[2])
                        encoded_url = urllib.parse.urlunparse(parts)

                response = pool_manager.request(_method, encoded_url, headers=_headers, body=body,
                                                timeout=timeout, retries=False, **kwargs)

            except urllib3.exceptions.ClosedPoolError:
                # If another thread closed the pool before the request was
                # started, will throw ClosedPoolError
                raise exceptions.UrllibInternalError("ClosedPoolError")

            _raise_error_for_testing(try_index, method)
            req_id = response.headers.get("x-request-id", "unavailable")

            if (_UPGRADE_NOTIFY
               and response.headers.get('x-upgrade-info', '').startswith('A recommended update is available')
               and '_ARGCOMPLETE' not in os.environ):
                logger.info(response.headers['x-upgrade-info'])
                try:
                    with file(_UPGRADE_NOTIFY, 'a'):
                        os.utime(_UPGRADE_NOTIFY, None)
                except:
                    pass
                _UPGRADE_NOTIFY = False

            # If an HTTP code that is not in the 200 series is received and the content is JSON, parse it and throw the
            # appropriate error.  Otherwise, raise the usual exception.
            if response.status // 100 != 2:
                # response.headers key lookup is case-insensitive
                if response.headers.get('content-type', '').startswith('application/json'):
                    try:
                        content = response.data.decode('utf-8')
                    except AttributeError:
                        raise exceptions.UrllibInternalError("Content is none", response.status)
                    try:
                        content = json.loads(content)
                    except ValueError:
                        # The JSON is not parsable, but we should be able to retry.
                        raise exceptions.BadJSONInReply("Invalid JSON received from server", response.status)
                    try:
                        error_class = getattr(exceptions, content["error"]["type"], exceptions.DXAPIError)
                    except (KeyError, AttributeError, TypeError):
                        raise exceptions.HTTPError(response.status, content)
                    raise error_class(content, response.status, time_started, req_id)
                else:
                    try:
                        content = response.data.decode('utf-8')
                    except AttributeError:
                        raise exceptions.UrllibInternalError("Content is none", response.status)
                    raise exceptions.HTTPError("{} {} [Time={} RequestID={}]\n{}".format(response.status,
                                                                                         response.reason,
                                                                                         time_started,
                                                                                         req_id,
                                                                                         content))

            if want_full_response:
                return response
            else:
                if 'content-length' in response.headers:
                    if int(response.headers['content-length']) != len(response.data):
                        range_str = (' (%s)' % (headers['Range'],)) if 'Range' in headers else ''
                        raise exceptions.ContentLengthError(
                            "Received response with content-length header set to %s but content length is %d%s. " +
                            "[Time=%f RequestID=%s]" %
                            (response.headers['content-length'], len(response.data), range_str, time_started, req_id)
                        )

                content = response.data

                response_was_json = False

                if decode_response_body:
                    content = content.decode('utf-8')
                    if response.headers.get('content-type', '').startswith('application/json'):
                        try:
                            content = json.loads(content)
                        except ValueError:
                            # The JSON is not parsable, but we should be able to retry.
                            raise exceptions.BadJSONInReply("Invalid JSON received from server", response.status)
                        else:
                            response_was_json = True

                req_id = response.headers.get('x-request-id') or "--"

                _debug_print_response(_DEBUG, seq_num, time_started, req_id, response.status, response_was_json,
                                      _method, _url, content)

                if test_retry:
                    retried_responses.append(content)
                    if len(retried_responses) == 1:
                        continue
                    else:
                        _set_retry_response(retried_responses[0])
                        return retried_responses[1]

                return content
            raise AssertionError('Should never reach this line: expected a result to have been returned by now')
        except Exception as e:
            # Avoid reusing connections in the pool, since they may be
            # in an inconsistent state (observed as "ResponseNotReady"
            # errors).
            _get_pool_manager(**pool_args).clear()
            success = False
            exception_msg = _extract_msg_from_last_exception()
            if isinstance(e, _expected_exceptions):
                # Total number of allowed tries is the initial try PLUS
                # up to (max_retries) subsequent retries.
                total_allowed_tries = max_retries + 1
                ok_to_retry = False
                is_retryable = always_retry or (method == 'GET') or _is_retryable_exception(e)
                # Because try_index is not incremented until we escape
                # this iteration of the loop, try_index is equal to the
                # number of tries that have failed so far, minus one.
                if try_index + 1 < total_allowed_tries:
                    # BadStatusLine ---  server did not return anything
                    # BadJSONInReply --- server returned JSON that didn't parse properly
                    if (response is None
                       or isinstance(e, (exceptions.ContentLengthError, BadStatusLine, exceptions.BadJSONInReply,
                                         urllib3.exceptions.ProtocolError, exceptions.UrllibInternalError))):
                        ok_to_retry = is_retryable
                    else:
                        ok_to_retry = 500 <= response.status < 600

                    # The server has closed the connection prematurely
                    if (response is not None
                       and response.status == 400 and is_retryable and method == 'PUT'
                       and isinstance(e, requests.exceptions.HTTPError)):
                        if '<Code>RequestTimeout</Code>' in exception_msg:
                            logger.info("Retrying 400 HTTP error, due to slow data transfer. " +
                                        "Request Time=%f Request ID=%s", time_started, req_id)
                        else:
                            logger.info("400 HTTP error, of unknown origin, exception_msg=[%s]. " +
                                        "Request Time=%f Request ID=%s", exception_msg, time_started, req_id)
                        ok_to_retry = True

                    # Unprocessable entity, request has semantical errors
                    if response is not None and response.status == 422:
                        ok_to_retry = False

                if ok_to_retry:
                    if rewind_input_buffer_offset is not None:
                        data.seek(rewind_input_buffer_offset)

                    delay = _calculate_retry_delay(response, try_index_including_503 + 1)

                    range_str = (' (range=%s)' % (headers['Range'],)) if 'Range' in headers else ''
                    if response is not None and response.status == 503:
                        waiting_msg = 'Waiting %d seconds before retry...' % (delay,)
                    else:
                        waiting_msg = 'Waiting %d seconds before retry %d of %d...' % (
                            delay, try_index + 1, max_retries)

                    logger.warning("[%s] %s %s: %s. %s %s",
                                   time.ctime(), method, _url, exception_msg, waiting_msg, range_str)
                    time.sleep(delay)
                    try_index_including_503 += 1
                    if response is None or response.status != 503:
                        try_index += 1
                    continue

            # All retries have been exhausted OR the error is deemed not
            # retryable. Print the latest error and propagate it back to the caller.
            if not isinstance(e, exceptions.DXAPIError):
                logger.error("[%s] %s %s: %s.", time.ctime(), method, _url, exception_msg)

            if isinstance(e, urllib3.exceptions.ProtocolError) and \
                'Connection reset by peer' in exception_msg:
                # If the protocol error is 'connection reset by peer', most likely it is an
                # error in the ssl handshake due to unsupported TLS protocol.
                _test_tls_version()

            # Retries have been exhausted, and we are unable to get a full
            # buffer from the data source. Raise a special exception.
            if isinstance(e, urllib3.exceptions.ProtocolError) and \
               'Connection broken: IncompleteRead' in exception_msg:
                raise exceptions.DXIncompleteReadsError(exception_msg)
            raise
        finally:
            if success and try_index > 0:
                logger.info("[%s] %s %s: Recovered after %d retries", time.ctime(), method, _url, try_index)

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


'''
This function is used for reading a part of an S3 object. It returns a string containing the data. If there is an
error, and exception is thrown.

There is special handling if a DXIncompleteReadsError is thrown, for which urllib3 gets only part of the requested
range from the chunk of data. The range is split into smaller chunks, and each sub-chunk is tried in a DXHTTPRequest.
The smaller chunks are then concatenated to form the original range of data. If a DXIncompleteReadsError is thrown
(after retrying the sub-chunk 6 times) while reading a sub-chunk, then we fail.
'''


def _dxhttp_read_range(url, headers, start_pos, end_pos, timeout, sub_range=True):
    if sub_range:
        headers['Range'] = "bytes=" + str(start_pos) + "-" + str(end_pos)
    try:
        data = DXHTTPRequest(url, '', method='GET', headers=headers, auth=None, jsonify_data=False, prepend_srv=False,
                             always_retry=True, timeout=timeout, decode_response_body=False)
        _raise_error_for_testing()
        return data

    # When chunk fails to be read, it gets broken into sub-chunks
    except exceptions.DXIncompleteReadsError:
        chunk_buffer = StringIO()
        subchunk_len = int(math.ceil((end_pos - start_pos + 1)/INCOMPLETE_READS_NUM_SUBCHUNKS))
        subchunk_start_pos = start_pos

        while subchunk_start_pos <= end_pos:
            subchunk_end_pos = min(subchunk_start_pos + subchunk_len - 1, end_pos)
            headers['Range'] = "bytes=" + str(subchunk_start_pos) + "-" + str(subchunk_end_pos)
            subchunk_start_pos += subchunk_len
            data = DXHTTPRequest(url, '', method='GET', headers=headers, auth=None, jsonify_data=False,
                                 prepend_srv=False, always_retry=True, timeout=timeout,
                                 decode_response_body=False)

            # Concatenate sub-chunks
            chunk_buffer.write(data)

        concat_chunks = chunk_buffer.getvalue()
        chunk_buffer.close()
        return concat_chunks


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

def set_watch_port(port=None):
    """
    :param port: port to use for streaming job logs
    :type port: string

    Sets the port to use for streaming job logs via `dx watch` inside the
    Execution Environment

    .. warning:: This function is only really useful if you are
       developing code that will run in and interact with the Execution
       Environment.

    """
    global WATCH_PORT
    WATCH_PORT = port

def get_auth_server_name(host_override=None, port_override=None, protocol='https'):
    """
    Chooses the auth server name from the currently configured API server name.

    Raises DXError if the auth server name cannot be guessed and the overrides
    are not provided (or improperly provided).
    """
    if host_override is not None or port_override is not None:
        if host_override is None or port_override is None:
            raise exceptions.DXError("Both host and port must be specified if either is specified")
        return protocol + '://' + host_override + ':' + str(port_override)
    elif APISERVER_HOST == 'stagingapi.dnanexus.com':
        return 'https://stagingauth.dnanexus.com'
    elif APISERVER_HOST == 'api.dnanexus.com':
        return 'https://auth.dnanexus.com'
    elif APISERVER_HOST == 'stagingapi.cn.dnanexus.com':
        return 'https://stagingauth.cn.dnanexus.com:7001'
    elif APISERVER_HOST == 'api.cn.dnanexus.com':
        return 'https://auth.cn.dnanexus.com:8001'
    elif APISERVER_HOST == "localhost" or APISERVER_HOST == "127.0.0.1":
        if "DX_AUTHSERVER_HOST" not in os.environ or "DX_AUTHSERVER_PORT" not in os.environ:
            err_msg = "Must set authserver env vars (DX_AUTHSERVER_HOST, DX_AUTHSERVER_PORT) if apiserver is {apiserver}."
            raise exceptions.DXError(err_msg.format(apiserver=APISERVER_HOST))
        else:
            return os.environ["DX_AUTHSERVER_HOST"] + ":" + os.environ["DX_AUTHSERVER_PORT"]
    else:
        err_msg = "Could not determine which auth server is associated with {apiserver}."
        raise exceptions.DXError(err_msg.format(apiserver=APISERVER_HOST))


'''This field is used for testing a retry of an Http request. The caller can pass
an argument "_test_retry_http_request"=1 to DXHTTPREQUEST to simulate a request that
required a retry. The first response will be returned and the second response can be
retrieved by calling _get_retry_response
'''

_retry_response = None


def _set_retry_response(response):
    global _retry_response
    _retry_response = response


def _get_retry_response():
    return _retry_response

def append_underlying_workflow_describe(globalworkflow_desc):
    """
    Adds the "workflowDescribe" field to the config for each region of
    the global workflow. The value is the description of an underlying
    workflow in that region.
    """
    if not globalworkflow_desc or \
            globalworkflow_desc['class'] != 'globalworkflow' or \
            not 'regionalOptions' in globalworkflow_desc:
        return globalworkflow_desc

    for region, config in globalworkflow_desc['regionalOptions'].items():
        workflow_id = config['workflow']
        workflow_desc = dxpy.api.workflow_describe(workflow_id, input_params={"project": config["resources"]})
        globalworkflow_desc['regionalOptions'][region]['workflowDescribe'] = workflow_desc
    return globalworkflow_desc


from .utils.config import DXConfig as _DXConfig
config = _DXConfig()

from .bindings import *
from .dxlog import DXLogHandler
from .utils.exec_utils import run, entry_point
