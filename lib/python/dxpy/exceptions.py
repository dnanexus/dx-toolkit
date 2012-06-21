'''
Exceptions for the :mod:`dxpy` package.
'''

import json

class DXError(Exception):
    '''Base class for exceptions in this package'''
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
    '''Exception for :class:`dxpy.bindings.DXFile`'''
    pass

class DXGTableError(DXError):
    '''Exception for :class:`dxpy.bindings.DXGTable`'''
    pass

class DXProgramError(DXError):
    '''Exception for :class:`dxpy.bindings.DXProgram`'''
    pass

class DXJobFailureError(DXError):
    '''Exception produced by :class:`dxpy.bindings.DXJob` when a job fails'''
    pass

class ProgramError(DXError):
    '''Base class for fatal exceptions to be raised while using dxpy inside DNAnexus execution containers.
    Throwing this exception will cause the Python execution template to write exception information into the file
    *job_error.json* in the current working directory, allowing reporting of the error state through the DNAnexus
    API.'''
    pass
