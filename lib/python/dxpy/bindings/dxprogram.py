"""
TODO: Write something here.
"""

from dxpy.bindings import *

#############
# DXProgram #
#############

def new_dxprogram(code_file=None, code_string=None):
    '''
    :param codefile: filename containing code to be run
    :type codefile: string
    :param codestring: code to be run
    :type codestring: string
    :rtype: :class:`dxpy.bindings.DXProgram`

    Creates a new program with the given code.  See
    :meth:`dxpy.bindings.DXProgram.new` for behavior.

    Note that this function is shorthand for::

        dxprogram = DXProgram()
        dxprogram.new(code_file)

    '''

    dxprogram = DXProgram()
    dxprogram.new(code_file, code_string)
    return dxprogram

class DXProgram(DXDataObject):
    '''
    Remote program object handler

    .. automethod:: _new
    '''

    _class = "program"

    _describe = staticmethod(dxpy.api.programDescribe)
    _add_types = staticmethod(dxpy.api.programAddTypes)
    _remove_types = staticmethod(dxpy.api.programRemoveTypes)
    _get_details = staticmethod(dxpy.api.programGetDetails)
    _set_details = staticmethod(dxpy.api.programSetDetails)
    _set_visibility = staticmethod(dxpy.api.programSetVisibility)
    _rename = staticmethod(dxpy.api.programRename)
    _set_properties = staticmethod(dxpy.api.programSetProperties)
    _add_tags = staticmethod(dxpy.api.programAddTags)
    _remove_tags = staticmethod(dxpy.api.programRemoveTags)
    _close = staticmethod(dxpy.api.programClose)
    _list_projects = staticmethod(dxpy.api.programListProjects)

    def _new(self, code_file=None, code_string=None):
        '''
        :param codefile: filename containing code to be run
        :type codefile: string
        :param codestring: code to be run
        :type codestring: string

        Creates a program with the code provided.  Exactly one argument
        between codefile and codestring should be given.  The program is
        not run until :meth:`dxpy.bindings.DXProgram.run` is called.

        '''

        if code_file is not None:
            if code_string is not None:
                raise DXProgramError("Expecting 1 argument for code and got"+
                                 " both code_file and code_string")
            with open(code_file, 'r') as codefd:
                code_string = codefd.read()
        elif code_string is None:
            raise DXProgramError("Expecting 1 argument for code and got"+
                             " neither code_file nor code_string")

        resp = dxpy.api.programNew({"code": code_string})
        self.set_id(resp["id"])

    def run(self, program_input):
        '''
        :param program_input: Hash of the program's input arguments
        :type program_input: dict
        :returns: Object handler of the created job now running the program
        :rtype: :class:`dxpy.bindings.DXJob`

        Creates a new job to execute the function "main" of this program
        with the given input *program_input*.

        '''

        return DXJob(dxpy.api.programRun(self._dxid, {"input": program_input})["id"])
