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

   HTTP proxy, in the form 'hostname:port' (e.g. '10.10.1.10:3128')

.. envvar:: HTTPS_PROXY

   HTTPS proxy, in the form 'hostname:port'

'''

# Try to reset the encoding to utf-8.
# (The alternative is to encode every single input and output as utf-8, which is unmaintainable.)
# TODO: consider using codecs.wrap/sys.stdout = codecs.getwriter('utf8')(sys.stdout) or PYTHONIOENCODING instead of this
try:
    import sys, locale
    reload(sys).setdefaultencoding(locale.getdefaultlocale()[1])
except:
    pass

import os, json, requests, time
from requests.exceptions import ConnectionError, HTTPError
from requests.auth import AuthBase
import httplib
from dxpy.exceptions import *

snappy_available = True
try:
    import snappy
except ImportError:
    snappy_available = False

API_VERSION = '1.0.0'
AUTH_HELPER = None
JOB_ID, WORKSPACE_ID, PROJECT_CONTEXT_ID = None, None, None

APISERVER_PROTOCOL = 'https'
APISERVER_HOST = 'preprodapi.dnanexus.com'
APISERVER_PORT = '443'

DEFAULT_RETRIES = 5

http_server_errors = set([requests.codes.server_error,
                          requests.codes.bad_gateway,
                          requests.codes.service_unavailable,
                          requests.codes.gateway_timeout])

def DXHTTPRequest(resource, data, method='POST', headers={}, auth=None, timeout=3600, config=None,
                  use_compression=None, jsonify_data=True, want_full_response=False,
                  prepend_srv=True,
                  max_retries=DEFAULT_RETRIES, always_retry=False,
                  **kwargs):
    '''
    :param resource: API server route, e.g. "/record/new"
    :type resource: string
    :param data: Content of the request body
    :type data: list or dict, if *jsonify_data* is True; or string, otherwise
    :param headers: Names and values of HTTP headers to submit with the request (in addition to those needed for authentication, compression, or other options specified with the call).
    :type headers: dict
    :param auth: Overrides the *auth* value to pass through to :meth:`requests.request`. By default a token is obtained from the ``DX_SECURITY_CONTEXT``.
    :type auth: tuple, object, or None
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
    :param prepend_srv: If True, prepends the API server location to the URL
    :type prepend_srv: boolean
    :param max_retries: Number of retries to perform for requests that are safe to retry. Safe requests are GET requests and requests that produced a network error or HTTP/1.1 server error (500, 502, 503, 504).
    :type max_retries: int
    :param always_retry: If True, attempts retries even for requests that are not considered safe to retry. As an exception, if the HTTP response has code 422, retries are never attempted (it is likely that the request is invalid and cannot be completed successfully with retries).
    :type always_retry: boolean
    :returns: Response from API server in the format indicated by *want_full_response*. Note: if *want_full_response* is set to False and the header "content-type" is found in the response with value "application/json", the body of the response will **always** be converted from JSON to a Python list or dict before it is returned.
    :raises: :exc:`requests.exceptions.HTTPError` if the response code was not 200 (OK), :exc:`ValueError` if the response from the API server cannot be decoded

    Wrapper around :meth:`requests.request()` that makes an HTTP
    request, inserting authentication headers and (by default)
    converting *data* to JSON.

    .. note:: Bindings methods that make API calls make the underlying
       HTTP request(s) using :func:`DXHTTPRequest`, and most of them
       will pass any unrecognized keyword arguments you have supplied
       through to :func:`DXHTTPRequest`.

    '''
    url = APISERVER + resource if prepend_srv else resource

    if auth is None:
        auth = AUTH_HELPER
    if config is None:
        config = {}
    # This will make the total number of retries MAX_RETRIES^2 for some errors. TODO: check how to better integrate with requests retry logic.
    # config.setdefault('max_retries', MAX_RETRIES)
    if 'Content-Type' not in headers and method == 'POST':
        headers['Content-Type'] = 'application/json'
    if jsonify_data:
        data = json.dumps(data)

    headers['DNAnexus-API'] = API_VERSION

    if use_compression == 'snappy':
        if not snappy_available:
            raise DXError("Snappy compression requested, but the snappy module is unavailable")
        headers['accept-encoding'] = 'snappy'

    response, last_error = None, None
    for retry in range(max_retries + 1):
        try:
            response = requests.request(method, url, data=data, headers=headers, timeout=timeout,
                                        auth=auth, config=config, **kwargs)

            # If HTTP code that is not 200 (OK) is received and the content is
            # JSON, parse it and throw the appropriate error.  Otherwise,
            # raise the usual exception.
            if response.status_code != requests.codes.ok:
                # response.headers key lookup is case-insensitive
                if response.headers.get('content-type', '').startswith('application/json'):
                    content = json.loads(response.content)
                    raise DXAPIError(content,
                                     response.status_code)
                response.raise_for_status()

            if want_full_response:
                return response
            else:
                if 'content-length' in response.headers:
                    if int(response.headers['content-length']) != len(response.content):
                        raise HTTPError("Received response with content-length header set to %s but content length is %d"
                            % (response.headers['content-length'], len(response.content)))

                if use_compression and response.headers.get('content-encoding', '') == 'snappy':
                    # TODO: check if snappy raises any exceptions on truncated response content
                    decoded_content = snappy.uncompress(response.content)
                else:
                    decoded_content = response.content

                if response.headers.get('content-type', '').startswith('application/json'):
                    try:
                        return json.loads(decoded_content)
                    except ValueError:
                        raise HTTPError("Invalid JSON received from server")
                return decoded_content
        except (DXAPIError, ConnectionError, HTTPError, httplib.HTTPException) as e:
            last_error = e
            
            # TODO: support HTTP/1.1 503 Retry-After
            # TODO: if the socket was dropped mid-request, ConnectionError is raised, but non-idempotent requests are unsafe to retry
            # Distinguish between connection initiation errors and dropped socket errors
            if retry < max_retries:
                ok_to_retry = False
                if isinstance(e, ConnectionError):
                    ok_to_retry = True
                elif response is not None:
                    if response.status_code != 422:
                        if always_retry or method == 'GET' or response.status_code in http_server_errors:
                            ok_to_retry = True

                if ok_to_retry:
                    delay = 2 ** (retry+1)
                    logging.warn("%s %s: %s. Waiting %d seconds before retry %d of %d..." % (method, url, str(e), delay, retry+1, max_retries))
                    time.sleep(delay)
                    continue
            break
        if last_error is None:
            last_error = DXError("Internal error in DXHTTPRequest")
    raise last_error

class DXHTTPOAuth2(AuthBase):
    def __init__(self, security_context):
        self.security_context = security_context

    def __call__(self, r):
        if self.security_context["auth_token_type"].lower() == 'bearer':
            r.headers['Authorization'] = \
                self.security_context["auth_token_type"] + " " + \
                self.security_context["auth_token"]
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

from dxpy.utils.env import get_env

def _initialize(suppress_warning=False):
    '''
    :param suppress_warning: Whether to suppress the warning message for any mismatch found in the environment variables and the dx configuration file
    :type suppress_warning: boolean
    '''
    env_vars = get_env(suppress_warning)
    for var in env_vars:
        if env_vars[var] is not None:
            os.environ[var] = env_vars[var]

    set_api_server_info(host=os.environ.get("DX_APISERVER_HOST", None),
                        port=os.environ.get("DX_APISERVER_PORT", None),
                        protocol=os.environ.get("DX_APISERVER_PROTOCOL", None))

    if "DX_SECURITY_CONTEXT" in os.environ:
        set_security_context(json.loads(os.environ['DX_SECURITY_CONTEXT']))

    if "DX_JOB_ID" in os.environ:
        set_job_id(os.environ["DX_JOB_ID"])
        if "DX_WORKSPACE_ID" in os.environ:
            set_workspace_id(os.environ["DX_WORKSPACE_ID"])
        if "DX_PROJECT_CONTEXT_ID" in os.environ:
            set_project_context(os.environ["DX_PROJECT_CONTEXT_ID"])
    else:
        if "DX_PROJECT_CONTEXT_ID" in os.environ:
            set_workspace_id(os.environ["DX_PROJECT_CONTEXT_ID"])

_initialize()

from dxpy.bindings import *
from dxpy.dxlog import *
from dxpy.utils.exec_utils import run, entry_point


# This should be in exec_utils but fails because of circular imports
# TODO: fix the imports
current_job, current_applet, current_app = None, None, None
if JOB_ID is not None:
    current_job = DXJob(JOB_ID)
    job_desc = current_job.describe()
    if 'applet' in job_desc:
        current_applet = DXApplet(job_desc['applet'])
    else:
        current_app = DXApp(job_desc['app'])
