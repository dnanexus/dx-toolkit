# Copyright (C) 2013 DNAnexus, Inc.
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

import json

class DXError(Exception):
    '''Base class for exceptions in this package.'''
    pass

class DXAPIError(DXError):
    '''
    Exception for when the API server responds with a code that is
    not 200 (OK).

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
        output = self.name + ": " + self.msg + ", code " + str(self.code)
        if self.details is not None:
            output += "\nDetails: " + json.dumps(self.details)
        return output

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
