"""
TODO: Write something here.
"""

from dxpy.bindings import *

#########
# DXApp #
#########

def new_dxapp(code_file=None, code_string=None):
    '''
    :param codefile: filename containing code to be run
    :type codefile: string
    :param codestring: code to be run
    :type codestring: string
    :rtype: :class:`dxpy.bindings.DXApp`

    Creates a new app with the given code.  See
    :meth:`dxpy.bindings.DXApp.new` for behavior.

    Note that this function is shorthand for::

        dxapp = DXApp()
        dxapp.new(code_file)

    '''

    dxapp = DXApp()
    dxapp.new(code_file, code_string)
    return dxapp

class DXApp(DXClass):
    '''Remote app object handler'''

    _class = "app"

    _describe = staticmethod(dxpy.api.appDescribe)
    _get_properties = staticmethod(dxpy.api.appGetProperties)
    _set_properties = staticmethod(dxpy.api.appSetProperties)
    _add_types = staticmethod(dxpy.api.appAddTypes)
    _remove_types = staticmethod(dxpy.api.appRemoveTypes)
    _destroy = staticmethod(dxpy.api.appDestroy)

    def new(self, code_file=None, code_string=None):
        '''
        :param codefile: filename containing code to be run
        :type codefile: string
        :param codestring: code to be run
        :type codestring: string

        Creates an app with the code provided.  Exactly one argument
        between codefile and codestring should be given.  The app is
        not run until :meth:`dxpy.bindings.DXApp.run` is called.

        '''

        if code_file is not None:
            if code_string is not None:
                raise DXAppError("Expecting 1 argument for code and got"+
                                 " both code_file and code_string")
            with open(code_file, 'r') as codefd:
                code_string = codefd.read()
        elif code_string is None:
            raise DXAppError("Expecting 1 argument for code and got"+
                             " neither code_file nor code_string")

        resp = dxpy.api.appNew({"code": code_string})
        self.set_id(resp["id"])

    def run(self, app_input):
        '''
        :param app_input: Hash of the app's input arguments
        :type app_input: dict
        :returns: Object handler of the created job now running the app
        :rtype: :class:`dxpy.bindings.DXJob`

        Creates a new job to execute the function "main" of this app
        with the given input *app_input*.

        '''

        return DXJob(dxpy.api.appRun(self._dxid, {"input": app_input})["id"])

#########
# DXJob #
#########

def new_dxjob(fn_input, fn_name):
    '''
    :param fn_input: Function input
    :type fn_input: dict
    :param fn_name: Name of the function to be called
    :type fn_name: string
    :rtype: :class:`dxpy.bindings.DXJob`

    Creates and enqueues a new job that will execute a particular
    function (from the same app as the one the current job is
    running).  Returns the DXJob handle for the job.

    Note that this function is shorthand for::

        dxjob = DXJob()
        dxjob.new(fn_input, fn_name)

    .. note:: This method is intended for calls made from within already-executing jobs or apps.  If it is called from outside of an Execution Environment, an exception will be thrown.

    '''
    dxjob = DXJob()
    dxjob.new(fn_input, fn_name)
    return dxjob

class DXJob(DXClass):
    '''Remote job object handler'''

    _class = "job"

    _describe = staticmethod(dxpy.api.jobDescribe)
    _get_properties = staticmethod(dxpy.api.jobGetProperties)
    _set_properties = staticmethod(dxpy.api.jobSetProperties)
    _add_types = staticmethod(dxpy.api.jobAddTypes)
    _remove_types = staticmethod(dxpy.api.jobRemoveTypes)
    _destroy = staticmethod(dxpy.api.jobDestroy)

    def new(self, fn_input, fn_name):
        '''
        :param fn_input: Function input
        :type fn_input: dict
        :param fn_name: Name of the function to be called
        :type fn_name: string

        Creates and enqueues a new job that will execute a particular
        function (from the same app as the one the current job is
        running).

        .. note:: This method is intended for calls made from within already-executing jobs or apps.  If it is called from outside of an Execution Environment, an exception will be thrown.

        '''

        req_input = {}
        req_input["input"] = fn_input
        req_input["function"] = fn_name
        resp = dxpy.api.jobNew(req_input)
        self.set_id(resp["id"])

    def wait_on_done(self, interval=2, timeout=sys.maxint):
        '''
        :param interval: Number of seconds between queries to the job's state
        :type interval: integer
        :param timeout: Max amount of time to wait until the job is done running
        :type timeout: integer
        :raises: :exc:`dxpy.exceptions.DXError` if the timeout is reached before the job has finished running

        Wait until the job has finished running.
        '''

        elapsed = 0
        while True:
            state = self._get_state()
            if state == "done":
                break
            if state == "failed":
                raise DXJobFailureError("Job has failed.")

            if elapsed >= timeout or elapsed < 0:
                raise DXJobFailureError("Reached timeout while waiting for the remote object to close")

            time.sleep(interval)
            elapsed += interval
