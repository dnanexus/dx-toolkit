"""
TODO: Write something here.
"""

from dxpy.bindings import *

#############
# DXProgram #
#############

def make_run_spec(interpreter,
                  codefile=None, codelocalfile=None, codestring=None,
                  bundled_depends=None, exec_depends=None):
    '''
    :param interpreter: name of the interpreter to use (e.g. "v8cgi", "python2.7")
    :type interpreter: string
    :param codefile: file object ID containing code to be run
    :type codefile: string
    :param codelocalfile: local filename of the file containing code to be run
    :type codelocalfile: string
    :param codestring: code to be run
    :type codestring: string
    :param bundled_depends: list of assets the program requires
    :type bundled_depends: list of dicts
    :param exec_depends: list of package names and versions required
    :type exec_depends: list of dicts

    TODO: Flesh out stuff.  Would it be better if run_spec were a
    class/struct?

    '''
    pass

def new_dxprogram(run_spec, input_spec=None, output_spec=None, **kwargs):
    '''

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

    def _new(self, dx_hash, **kwargs):
        '''
        :param dx_hash: Standard hash populated in :func:`dxpy.bindings.DXDataObject.new()`
        :type dx_hash: dict
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

    def get(self):
        """
        Returns the contents of the program.
        """
        return dxpy.api.programGet(self._dxid)

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
