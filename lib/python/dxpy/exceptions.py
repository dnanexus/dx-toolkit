'''
Exceptions for the :mod:`dxpy` package.
'''

class DXError(Exception):
    '''Base class for exceptions in this package'''
    pass

class DXAPIError(DXError):
    '''
    Exception for when the API server responds with a code that is
    not 200.

    '''
    def __init__(self, name, msg, code):
        self.name = name
        self.msg = msg
        self.code = code

    def __str__(self):
        return self.name + ": " + self.msg + ", code " + str(self.code)

class DXFileError(DXError):
    '''Exception for :class:`dxpy.bindings.DXFile`'''
    pass

class DXTableError(DXError):
    '''Exception for :class:`dxpy.bindings.DXTable`'''
    pass

class DXAppError(DXError):
    '''Exception for :class:`dxpy.bindings.DXApp`'''
    pass

class DXJobFailureError(DXError):
    '''Exception produced by :class:`dxpy.bindings.DXJob` when a job fails'''
    pass
