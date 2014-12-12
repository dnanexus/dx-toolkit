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
Exceptions for the :mod:`dxpy` package.
'''

from __future__ import (print_function, unicode_literals)

import os, sys, json, traceback, errno
from .packages import requests

import dxpy

EXPECTED_ERR_EXIT_STATUS = 3

class DXError(Exception):
    '''Base class for exceptions in this package.'''
    pass

class DXAPIError(DXError):
    '''
    Exception for when the API server responds with a code that is not 200 (OK). See
    https://wiki.dnanexus.com/API-Specification-v1.0.0/Protocols#Errors for complete documentation of API errors,
    including those reflected by subclasses of this class.
    '''
    def __init__(self, content, code):
        self.name = content["error"]["type"]
        self.msg = content["error"]["message"]
        if "details" in content["error"]:
            self.details = content["error"]["details"]
        else:
            self.details = None
        self.code = code

    def __str__(self):
        output = self.msg + ", code " + str(self.code)
        if self.name != self.__class__.__name__:
            output = self.name + ": " + output
        if self.details:
            output += "\nDetails: " + json.dumps(self.details, indent=4)
        return output

class MalformedJSON(DXAPIError):
    ''' Raised when the input could not be parsed as JSON. '''
    pass

class InvalidAuthentication(DXAPIError):
    ''' Raised when the provided OAuth2 token is invalid. '''
    pass

class PermissionDenied(DXAPIError):
    ''' Raised when the supplied credentials have insufficient permissions to perform this action. '''
    pass

class SpendingLimitExceeded(DXAPIError):
    ''' Raised when the spending limit has been reached for the account that would be billed for this action. '''
    pass

class ResourceNotFound(DXAPIError):
    ''' Raised when a specified entity or resource could not be found. '''
    pass

class InvalidInput(DXAPIError):
    ''' Raised when the input is syntactically correct (JSON), but semantically incorrect (for example, a JSON array
    is provided where a hash was required; or a required parameter was missing, etc.). '''
    pass

class InvalidState(DXAPIError):
    ''' Raised when the operation is not allowed at this object state. '''
    pass

class InvalidType(DXAPIError):
    ''' Raised when an object specified in the request is of invalid type. '''
    pass

class RateLimitConditional(DXAPIError):
    ''' Raised when the rate of invalid requests is too high. '''
    pass

class InternalError(DXAPIError):
    ''' Raised when the server encountered an internal error. '''
    pass

class ServiceUnavailable(DXAPIError):
    ''' Raised when an API service was temporarily unavailable. '''
    pass

class DXFileError(DXError):
    '''Exception for :class:`dxpy.bindings.dxfile.DXFile`.'''
    pass

class DXGTableError(DXError):
    '''Exception for :class:`dxpy.bindings.dxgtable.DXGTable`.'''
    pass

class DXSearchError(DXError):
    '''Exception for :mod:`dxpy.bindings.search` methods.'''
    pass

class DXAppletError(DXError):
    '''Exception for :class:`dxpy.bindings.dxapplet.DXApplet`.'''
    pass

class DXJobFailureError(DXError):
    '''Exception produced by :class:`dxpy.bindings.dxjob.DXJob` when a job fails.'''
    pass

class ProgramError(DXError):
    '''Deprecated. Use :class:`AppError` instead.'''
    pass

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
    pass

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
    pass

class DXCLIError(DXError):
    '''
    Exception class for generic errors in the command-line client
    '''
    pass

class ContentLengthError(requests.HTTPError):
    '''Will be raised when actual content length received from server does not match the "Content-Length" header'''
    pass


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

network_exceptions = (requests.ConnectionError,
                      requests.exceptions.ChunkedEncodingError,
                      requests.exceptions.ContentDecodingError,
                      requests.HTTPError,
                      requests.Timeout,
                      requests.packages.urllib3.connectionpool.HTTPException)

default_expected_exceptions = network_exceptions + (DXAPIError,
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
        raise
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
