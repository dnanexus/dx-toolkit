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
Exceptions for the :mod:`dxpy` package.
'''

from __future__ import print_function, unicode_literals, division, absolute_import

import sys
import json
import traceback
import errno
import socket
import requests
from requests.exceptions import HTTPError

import dxpy
from .compat import USING_PYTHON2
import urllib3
import ssl

EXPECTED_ERR_EXIT_STATUS = 3

class DXError(Exception):
    '''Base class for exceptions in this package.'''

class DXAPIError(DXError):
    '''
    Exception for when the API server responds with a code that is not 200 (OK). See
    https://documentation.dnanexus.com/developer/api/protocols#errors for complete documentation of API errors,
    including those reflected by subclasses of this class.
    '''
    def __init__(self, content, code, timestamp="", req_id=""):
        self.name = content["error"]["type"]
        self.msg = content["error"]["message"]
        if "details" in content["error"]:
            self.details = content["error"]["details"]
        else:
            self.details = None
        self.code = code
        self.timestamp = timestamp
        self.req_id = req_id

    def error_message(self):
        "Returns a one-line description of the error."
        output = self.msg + ", code " + str(self.code)
        output += ". Request Time={}, Request ID={}".format(self.timestamp, self.req_id)
        if self.name != self.__class__.__name__:
            output = self.name + ": " + output
        return output

    def __str__(self):
        output = self.error_message()
        if self.details:
            output += "\nDetails: " + json.dumps(self.details, indent=4)
        return output

class MalformedJSON(DXAPIError):
    ''' Raised when the input could not be parsed as JSON. '''

class InvalidAuthentication(DXAPIError):
    ''' Raised when the provided OAuth2 token is invalid. '''

class PermissionDenied(DXAPIError):
    ''' Raised when the supplied credentials have insufficient permissions to perform this action. '''

class SpendingLimitExceeded(DXAPIError):
    ''' Raised when the spending limit has been reached for the account that would be billed for this action. '''

class ResourceNotFound(DXAPIError):
    ''' Raised when a specified entity or resource could not be found. '''

class InvalidInput(DXAPIError):
    ''' Raised when the input is syntactically correct (JSON), but semantically incorrect (for example, a JSON array
    is provided where a hash was required; or a required parameter was missing, etc.). '''

class InvalidState(DXAPIError):
    ''' Raised when the operation is not allowed at this object state. '''

class InvalidType(DXAPIError):
    ''' Raised when an object specified in the request is of invalid type. '''

class RateLimitConditional(DXAPIError):
    ''' Raised when the rate of invalid requests is too high. '''

class InternalError(DXAPIError):
    ''' Raised when the server encountered an internal error. '''

class ServiceUnavailable(DXAPIError):
    ''' Raised when an API service was temporarily unavailable. '''

class DXFileError(DXError):
    '''Exception for :class:`dxpy.bindings.dxfile.DXFile`.'''

class DXIncompleteReadsError(DXError):
    '''Exception for :class:`dxpy.bindings.dxfile.DXFile` when returned read data is shorter than requested'''

class DXPartLengthMismatchError(DXFileError):
    '''Exception raised by :class:`dxpy.bindings.dxfile.DXFile` on part length mismatch.'''

class DXChecksumMismatchError(DXFileError):
    '''Exception raised by :class:`dxpy.bindings.dxfile.DXFile` on checksum mismatch.'''

class DXSearchError(DXError):
    '''Exception for :mod:`dxpy.bindings.search` methods.'''

class DXAppletError(DXError):
    '''Exception for :class:`dxpy.bindings.dxapplet.DXApplet`.'''

class DXJobFailureError(DXError):
    '''Exception produced by :class:`dxpy.bindings.dxjob.DXJob` when a job fails.'''

class ProgramError(DXError):
    '''Deprecated. Use :class:`AppError` instead.'''

class AppError(ProgramError):
    '''
    Base class for fatal exceptions to be raised while using :mod:`dxpy` inside
    DNAnexus execution containers.

    This exception is thrown for user errors, and the error message is
    presented to the user. Throwing this exception will cause the Python
    execution template to write exception information into the file
    *job_error.json* in the current working directory, allowing reporting of
    the error state through the DNAnexus API.
    '''

class AppInternalError(DXError):
    '''
    Base class for fatal exceptions to be raised while using :mod:`dxpy` inside
    DNAnexus execution containers.

    This exception is intended for internal App errors, whose message goes to
    the App developer. Throwing this exception will cause the Python execution
    template to write exception information into the file ``job_error.json`` in
    the current working directory, allowing reporting of the error state
    through the DNAnexus API.
    '''

class DXCLIError(DXError):
    '''
    Exception class for generic errors in the command-line client
    '''

class ContentLengthError(HTTPError):
    '''
    Raised when actual content length received from the server does not
    match the "Content-Length" header
    '''

class HTTPErrorWithContent(HTTPError):
    '''
    Specific variant of HTTPError with response content.

    This class was created to avoid appending content directly to error message
    which makes difficult to format log strings.
    '''

    def __init__(self, value, content):
        super(HTTPError, self).__init__(value)
        self.content = content

class BadJSONInReply(ValueError):
    '''
    Raised when the server returned invalid JSON in the response body. Possible reasons
    for this are the network connection breaking, or overload on the server.
    '''

class InvalidTLSProtocol(DXAPIError):
    '''
    Raised when the connection to the server was reset due to an ssl protocol not supported.
    Only connections with TLS v1.2 will be accepted.
    '''
    def __init__(self):
        pass

    def error_message(self):
        output = "Please refer to our blog post at https://blog.dnanexus.com/2017-09-23-upgrading-tls/ regarding upgrading to TLS 1.2."
        return output

    def __str__(self):
        return self.error_message()

class UrllibInternalError(AttributeError):
    '''
    Exception class for AttributeError from urllib3
    '''


def format_exception(e):
    """Returns a string containing the type and text of the exception.

    """
    from .utils.printing import fill
    return '\n'.join(fill(line) for line in traceback.format_exception_only(type(e), e))


def exit_with_exc_info(code=1, message='', print_tb=False, exception=None):
    '''Exits the program, printing information about the last exception (if
    any) and an optional error message.  Uses *exception* instead if provided.

    :param code: Exit code.
    :type code: integer (valid exit code, 0-255)
    :param message: Message to be printed after the exception information.
    :type message: string
    :param print_tb: If set to True, prints the exception traceback; otherwise, suppresses it.
    :type print_tb: boolean
    :type exception: an exception to use in place of the last exception raised
    '''
    exc_type, exc_value = (exception.__class__, exception) \
                          if exception is not None else sys.exc_info()[:2]

    if exc_type is not None:
        if print_tb:
            traceback.print_exc()
        elif isinstance(exc_value, KeyboardInterrupt):
            sys.stderr.write('^C\n')
        else:
            for line in traceback.format_exception_only(exc_type, exc_value):
                sys.stderr.write(line)

    sys.stderr.write(message)
    if message != '' and not message.endswith('\n'):
        sys.stderr.write('\n')
    sys.exit(code)

network_exceptions = (requests.packages.urllib3.exceptions.ProtocolError,
                      requests.packages.urllib3.exceptions.NewConnectionError,
                      requests.packages.urllib3.exceptions.DecodeError,
                      requests.packages.urllib3.exceptions.ConnectTimeoutError,
                      requests.packages.urllib3.exceptions.ReadTimeoutError,
                      requests.packages.urllib3.connectionpool.HTTPException,
                      urllib3.exceptions.SSLError,
                      ssl.SSLError,
                      HTTPError,
                      socket.error)
if not USING_PYTHON2:
    network_exceptions += (ConnectionResetError,)


try:
    json_exceptions = (json.decoder.JSONDecodeError,)
except:
    json_exceptions = (ValueError,)

default_expected_exceptions = network_exceptions + json_exceptions + (DXAPIError,
                                                    DXCLIError,
                                                    KeyboardInterrupt)

def err_exit(message='', code=None, expected_exceptions=default_expected_exceptions, arg_parser=None,
             ignore_sigpipe=True, exception=None):
    '''Exits the program, printing information about the last exception (if
    any) and an optional error message.  Uses *exception* instead if provided.

    Uses **expected_exceptions** to set the error code decide whether to
    suppress the error traceback.

    :param message: Message to be printed after the exception information.
    :type message: string
    :param code: Exit code.
    :type code: integer (valid exit code, 0-255)
    :param expected_exceptions: Exceptions for which to exit with error code 3 (expected error condition) and suppress the stack trace (unless the _DX_DEBUG environment variable is set).
    :type expected_exceptions: iterable
    :param arg_parser: argparse.ArgumentParser object used in the program (optional)
    :param ignore_sigpipe: Whether to exit silently with code 3 when IOError with code EPIPE is raised. Default true.
    :type ignore_sigpipe: boolean
    :param exception: an exception to use in place of the last exception raised
    '''
    if arg_parser is not None:
        message = arg_parser.prog + ": " + message

    exc = exception if exception is not None else sys.exc_info()[1]
    if isinstance(exc, SystemExit):
        raise exc
    elif isinstance(exc, expected_exceptions):
        exit_with_exc_info(EXPECTED_ERR_EXIT_STATUS, message, print_tb=dxpy._DEBUG > 0, exception=exception)
    elif ignore_sigpipe and isinstance(exc, IOError) and getattr(exc, 'errno', None) == errno.EPIPE:
        if dxpy._DEBUG > 0:
            print("Broken pipe", file=sys.stderr)
        sys.exit(3)
    else:
        if code is None:
            code = 1
        exit_with_exc_info(code, message, print_tb=True, exception=exception)
