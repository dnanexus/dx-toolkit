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
   None if we are not in an Execution Environment.

.. py:data:: PROJECT_CONTEXT_ID

   Indicates either the project context of a running job, if there is
   one, or the default project that is being used, for users accessing
   the platform from the outside.

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

# Note: The default I/O stream encoding in Python 2.7 (as configured on ubuntu) is ascii, not UTF-8 or the system locale
# encoding. The lines below attempt to reset it here to avoid having to set it for every I/O operation explicitly.
# However, this method doesn't work with pypy, so instead we set the environment variable PYTHONIOENCODING=UTF-8 in
# dx-toolkit environment initialization (dx-toolkit/environment).
# One other alternative is to use codecs.wrap or sys.stdout = codecs.getwriter('utf8')(sys.stdout).
# try:
#     import sys, locale
#     reload(sys).setdefaultencoding(locale.getdefaultlocale()[1])
# except:
#     pass

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

def DXHTTPRequest(resource, data, method='POST', headers={}, auth=True, timeout=600, config=None,
                  use_compression=None, jsonify_data=True, want_full_response=False,
                  prepend_srv=True,
                  max_retries=DEFAULT_RETRIES, always_retry=False, retry_on_error_reponse_cb=None,
                  **kwargs):
    '''
    :param resource: API server route, e.g. "/record/new"
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
    :param prepend_srv: If True, prepends the API server location to the URL
    :type prepend_srv: boolean
    :param max_retries: Maximum number of retries to perform for a request. A "failed" request is retried if either of the following is true:
                        
                        - *always_retry* is True
                        - method.upper() == 'GET'
                        - Server responded with HTTP status code in 5xx range (only applicable if response is received from server)

    :type max_retries: int
    :param always_retry: If True, always attempt retry for failed requests.
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
    method = method.upper() # Convert method string to upper case, makes our life easier for comparing string later (POST, GET, etc)
    if _DEBUG:
        from repr import Repr
        print >>sys.stderr, method, url, "=>", Repr().repr(data)

    if auth is True:
        auth = AUTH_HELPER
    if config is None:
        config = {}
    # This will make the total number of retries MAX_RETRIES^2 for some errors. TODO: check how to better integrate with requests retry logic.
    # config.setdefault('max_retries', MAX_RETRIES)
    if 'Content-Type' not in headers and method == 'POST':
        headers['Content-Type'] = 'application/json'
    if jsonify_data:
        data = json.dumps(data)

    # If the input is a buffer, its data gets consumed by
    # requests.request (moving the read position). Record the initial
    # buffer position so that we can return to it if the request fails
    # and needs to be retried.
    rewind_input_buffer_offset = None
    if hasattr(data, 'seek') and hasattr(data, 'tell'):
        rewind_input_buffer_offset = data.tell()

    headers['DNAnexus-API'] = API_VERSION

    if use_compression == 'snappy':
        if not snappy_available:
            raise DXError("Snappy compression requested, but the snappy module is unavailable")
        headers['accept-encoding'] = 'snappy'

    if 'verify' not in kwargs and 'DX_CA_CERT' in os.environ:
        kwargs['verify'] = os.environ['DX_CA_CERT']
        if os.environ['DX_CA_CERT'] == 'NOVERIFY':
            kwargs['verify'] = False

    response, last_error = None, None
    for retry in range(max_retries + 1):
        try:
            response = requests.request(method, url, data=data, headers=headers, timeout=timeout,
                                        auth=auth, config=config, **kwargs)

            if _DEBUG:
                print >>sys.stderr, method, url, "<=", response.status_code, Repr().repr(response.content)

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
            # TODO: if the socket was dropped mid-request, ConnectionError or httplib.IncompleteRead is raised,
            # but non-idempotent requests can be unsafe to retry
            # Distinguish between connection initiation errors and dropped socket errors
            if retry < max_retries:
                # If an error occurs, we retry if *either* of the following is true:
                # 1) always_retry is True , 2) it was a GET request, 3) server responded with 5xx HTTP status code
                ok_to_retry = always_retry or (method == 'GET')

                if response is not None:
                    ok_to_retry = ok_to_retry or (response.status_code >= 500 and response.status_code < 600) 
                    
                if ok_to_retry:
                    if rewind_input_buffer_offset is not None:
                        data.seek(rewind_input_buffer_offset)
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
    global _DEBUG
    _DEBUG = False
    if '_DX_DEBUG' in os.environ:
        _DEBUG = True

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

from dxpy.toolkit_version import version as TOOLKIT_VERSION


# This should be in exec_utils but fails because of circular imports
# TODO: fix the imports
current_job, current_applet, current_app = None, None, None
if JOB_ID is not None:
    current_job = DXJob(JOB_ID)
    try:
        job_desc = current_job.describe()
    except DXAPIError as e:
        if e.name == 'ResourceNotFound':
            print "Job ID %r was not found. Unset the DX_JOB_ID environment variable OR set it to be the ID of a valid job." % (JOB_ID,)
            sys.exit(1)
        else:
            raise
    if 'applet' in job_desc:
        current_applet = DXApplet(job_desc['applet'])
    else:
        current_app = DXApp(job_desc['app'])
