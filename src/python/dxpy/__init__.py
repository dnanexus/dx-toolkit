'''
When importing this package, configuration values will be loaded from the following sources in order of decreasing priority:

1. Environment variables
2. Values stored in ~/.dnanexus_config/environment
3. Values stored in /opt/dnanexus/environment
4. Hardcoded defaults

The relevant environment variables are the following:

* DX_SECURITY_CONTEXT: stores a JSON containing your auth token
* DX_APISERVER_PROTOCOL: either "http" or "https" (usually "https")
* DX_APISERVER_HOST: hostname of the DNAnexus API server
* DX_APISERVER_PORT: port of the DNAnexus API server
* DX_JOB_ID: should only be present if run in an Execution Environment
* DX_WORKSPACE_ID: should only be present if run in an Execution Environment; indicates the running job's temporary workspace ID
* DX_PROJECT_CONTEXT_ID: indicates either the project context of a running job, or the default project to use for a user accessing the platfrom

If the security context and API server variables are available, then
upon importing the module, any method which relies on the
:func:`dxpy.DXHTTPRequest` function will set the appropriate
authentication headers for making API calls to the API server.  (Note:
All methods in the :mod:`dxpy.api` use this function.)  In addition,
it will set the default workspace according to DX_WORKSPACE_ID (if
running inside an Execution Environment) or DX_PROJECT_CONTEXT_ID
(otherwise).  This workspace will be used by default for any object
handler methods that require a project ID.

To override any of the settings from the environment for a particular
session, the following functions can be used:

* :func:`dxpy.set_security_context`: for using a different authentication token
* :func:`dxpy.set_api_server_info`: for using a different API server
* :func:`dxpy.set_workspace_id`: for overriding the default data container to use

If an HTTP/HTTPS proxy is to be used, set the environment variables
beforehand as applicable while using the format 'hostname:port'
(e.g. '10.10.1.10:3128'):

* **HTTP_PROXY**: 'hostname:port' for the HTTP proxy
* **HTTPS_PROXY**: 'hostname:port' for the HTTPS proxy

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
    :param data: Contents for the request body
    :param jsonify_data: Indicates whether *data* should be converted from a Python list or dict to a JSON string
    :type jsonify_data: boolean
    :param want_full_response: Indicates whether the function should return the full :class:`requests.Response` object or just the content of the response
    :type want_full_response: boolean
    :param max_retries: Number of retries to perform for requests which are safe to retry. Safe requests are GET requests or requests which produced a network error or HTTP/1.1 server error (500, 502, 503, 504).
    :type max_retries: int
    :param always_retry: Indicates whether to attempt retries even for requests which are not considered safe to retry.  Exception: if the HTTP response has code 422, no retries will be attempted (request is invalid and cannot be fulfilled with retries).
    :type always_retry: boolean
    :returns: Response from API server in the requested format.  Note: if *want_full_response* is set to False and the header "content-type" is found in the response with value "application/json", the contents of the response will **always** be converted from JSON to Python before it is returned, and it will therefore be of type list or dict.
    :raises: :exc:`requests.exceptions.HTTPError` if response code was not 200 (OK), :exc:`ValueError` if the response from the API server cannot be decoded

    Wrapper around requests.request(). Inserts authentication and
    converts *data* to JSON.
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
    :param port: API server port
    :type port: string
    :param protocol: either "http" or "https" for SSL
    :type protocol: string

    Overrides the current settings for which API server to communicate
    with.
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
    :param security_context: Authentication hash, usually with keys "auth_token_type" set to "bearer" and "auth_token" set to the authentication token.
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

    """
    global JOB_ID
    JOB_ID = dxid

def set_workspace_id(dxid):
    """
    :param dxid: ID of a project or workspace
    :type dxid: string

    Sets the default project or workspace for object creation and
    modification to *dxid*.
    """

    global WORKSPACE_ID
    WORKSPACE_ID = dxid

def set_project_context(dxid):
    """
    :param dxid: Project ID
    :type dxid: string

    Sets the project context for a running job.  This does not change
    the default data container in which new objects are created or
    name resolution is attempted.

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
