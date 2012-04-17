"""
TODO: Write something here.
"""

from dxpy.bindings import *

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

class DXJob(DXDataObject):
    '''Remote job object handler'''

    _class = "job"

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

    def set_id(self, dxid):
        '''
        :param dxid: Object ID
        :type dxid: string

        Discards the currently stored ID and associates the handler
        with *dxid*.
        '''
        self._dxid = dxid

    def get_id(self):
        '''
        :returns: Job ID of associated job
        :rtype: string

        Returns the job ID that the handler is currently associated
        with.

        '''

        return self._dxid

    def describe(self):
        """
        :returns: Description of the job
        :rtype: dict

        Returns a hash which will include key-value pairs that include
        information on its input and outputs, state, etc.  See the API
        documentation for the full list.

        """
        return dxpy.api.jobDescribe(self._dxid)

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

    def terminate(self):
        '''
        Terminate the associated job.
        '''
        dxpy.api.jobTerminate(self._dxid)
