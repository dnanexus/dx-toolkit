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

"""
DXJob Handler
+++++++++++++

Jobs are DNAnexus entities that capture an instantiation of a running
app or applet. They can be created from either
:func:`dxpy.bindings.dxapplet.DXApplet.run` or
:func:`dxpy.bindings.dxapp.DXApp.run` if running an applet or app, or
via :func:`new_dxjob` or :func:`DXJob.new` in the case of an existing
job creating a subjob.

"""

from dxpy.bindings import *
from dxpy.utils.local_exec_utils import queue_entry_point

#########
# DXJob #
#########

def new_dxjob(fn_input, fn_name, name=None, instance_type=None, depends_on=None, details=None,
              **kwargs):
    '''
    :param fn_input: Function input
    :type fn_input: dict
    :param fn_name: Name of the function to be called
    :type fn_name: string
    :param name: Name for the new job (default is "<parent job name>:<fn_name>")
    :type name: string
    :param instance_type: Instance type on which the job will be run, or a dict mapping function names to instance type requests
    :type instance_type: string or dict
    :param depends_on: List of data objects or jobs to wait that need to enter the "closed" or "done" states, respectively, before the new job will be run; each element in the list can either be a dxpy handler or a string ID
    :type depends_on: list
    :param details: Details to set for the job
    :type details: dict or list
    :rtype: :class:`~dxpy.bindings.dxjob.DXJob`

    Creates and enqueues a new job that will execute a particular
    function (from the same app or applet as the one the current job is
    running). Returns the :class:`~dxpy.bindings.dxjob.DXJob` handle for
    the job.

    Note that this function is shorthand for::

        dxjob = DXJob()
        dxjob.new(fn_input, fn_name, name, instance_type)

    .. note:: This method is intended for calls made from within
       already-executing jobs or apps. If it is called from outside of
       an Execution Environment, an exception will be thrown. To create
       new jobs from outside the Execution Environment, use
       :func:`dxpy.bindings.dxapplet.DXApplet.run` or
       :func:`dxpy.bindings.dxapp.DXApp.run`.

    .. note:: If the environment variable ``DX_JOB_ID`` is not set, this method assmes that it is running within the debug harness, executes the job in place, and provides a debug job handler object that does not have a corresponding remote API job object.

    '''
    dxjob = DXJob()
    dxjob.new(fn_input, fn_name, name, instance_type, depends_on, details, **kwargs)
    return dxjob

class DXJob(DXObject):
    '''
    Remote job object handler.
    '''

    _class = "job"

    def __init__(self, dxid=None):
        self._test_harness_result = None
        DXObject.__init__(self, dxid=dxid)

    def new(self, fn_input, fn_name, name=None, instance_type=None, depends_on=None, details=None,
            **kwargs):
        '''
        :param fn_input: Function input
        :type fn_input: dict
        :param fn_name: Name of the function to be called
        :type fn_name: string
        :param name: Name for the new job (default is "<parent job name>:<fn_name>")
        :type name: string
        :param instance_type: Instance type on which the job will be run, or a dict mapping function names to instance type requests
        :type instance_type: string or dict
        :param depends_on: List of data objects or jobs to wait that need to enter the "closed" or "done" states, respectively, before the new job will be run; each element in the list can either be a dxpy handler or a string ID
        :type depends_on: list
        :param details: Details to set for the job
        :type details: dict or list

        Creates and enqueues a new job that will execute a particular
        function (from the same app or applet as the one the current job
        is running).

        .. note:: This method is intended for calls made from within
           already-executing jobs or apps. If it is called from outside
           of an Execution Environment, an exception will be thrown. To
           create new jobs from outside the Execution Environment, use
           :func:`dxpy.bindings.dxapplet.DXApplet.run` or
           :func:`dxpy.bindings.dxapp.DXApp.run`.

        '''
        final_depends_on = []
        if depends_on is not None:
            if isinstance(depends_on, list):
                for item in depends_on:
                    if isinstance(item, DXJob) or isinstance(item, DXDataObject):
                        if item.get_id() is None:
                            raise DXError('A dxpy handler given in depends_on does not have an ID set')
                        final_depends_on.append(item.get_id())
                    elif isinstance(item, basestring):
                        final_depends_on.append(item)
                    else:
                        raise DXError('Expected elements of depends_on to only be either instances of DXJob or DXDataObject, or strings')
            else:
                raise DXError('Expected depends_on field to be a list')

        if 'DX_JOB_ID' in os.environ:
            req_input = {}
            req_input["input"] = fn_input
            req_input["function"] = fn_name
            if name is not None:
                req_input["name"] = name
            if instance_type is not None:
                if isinstance(instance_type, basestring):
                    req_input["systemRequirements"] = {fn_name: {"instanceType": instance_type}}
                elif isinstance(instance_type, dict):
                    req_input["systemRequirements"] = {stage: {"instanceType": stage_inst} for stage, stage_inst in instance_type.iteritems()}
                else:
                    raise DXError('Expected instance_type field to be either a string or a dict')
            if depends_on is not None:
                req_input["dependsOn"] = final_depends_on
            if details is not None:
                req_input["details"] = details
            resp = dxpy.api.job_new(req_input, **kwargs)
            self.set_id(resp["id"])
        else:
            self.set_id(queue_entry_point(function=fn_name, input_hash=fn_input,
                                          depends_on=final_depends_on,
                                          name=name))

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

    def describe(self, fields=None, io=None, **kwargs):
        """
        :param fields: Hash where the keys are field names that should be returned, and values should be set to True (default is that all fields are returned)
        :type fields: dict
        :param io: Include input and output fields in description; cannot be provided with *fields*; default is True if *fields* is not provided (deprecated)
        :type io: bool
        :returns: Description of the job
        :rtype: dict

        Returns a hash with key-value pairs containing information about
        the job, including its state and (optionally) its inputs and
        outputs, as described in the API documentation for the
        `/job-xxxx/describe
        <https://wiki.dnanexus.com/API-Specification-v1.0.0/Applets-and-Entry-Points#API-method:-/job-xxxx/describe>`_
        method.

        """
        if fields is not None and io is not None:
            raise DXError('DXJob.describe: cannot provide non-None values for both fields and io')
        describe_input = {}
        if fields is not None:
            describe_input['fields'] = fields
        if io is not None:
            describe_input['io'] = io
        self._desc = dxpy.api.job_describe(self._dxid, describe_input, **kwargs)
        return self._desc

    def wait_on_done(self, interval=2, timeout=sys.maxint, **kwargs):
        '''
        :param interval: Number of seconds between queries to the job's state
        :type interval: integer
        :param timeout: Maximum amount of time to wait, in seconds, until the job is done running
        :type timeout: integer
        :raises: :exc:`~dxpy.exceptions.DXError` if the timeout is reached before the job has finished running, or :exc:`dxpy.exceptions.DXJobFailureError` if the job fails

        Waits until the job has finished running.
        '''

        elapsed = 0
        while True:
            state = self._get_state(**kwargs)
            if state == "done":
                break
            if state == "failed":
                desc = self.describe(**kwargs)
                err_msg = "Job has failed because of {failureReason}: {failureMessage}".format(**desc)
                if desc.get("failureFrom") != None and desc["failureFrom"]["id"] != desc["id"]:
                    err_msg += " (failure from {id})".format(id=desc['failureFrom']['id'])
                raise DXJobFailureError(err_msg)
            if state == "terminated":
                raise DXJobFailureError("Job was terminated.")

            if elapsed >= timeout or elapsed < 0:
                raise DXJobFailureError("Reached timeout while waiting for the job to finish")

            time.sleep(interval)
            elapsed += interval

    def terminate(self, **kwargs):
        '''
        Terminates the associated job.
        '''
        dxpy.api.job_terminate(self._dxid, **kwargs)

    def get_output_ref(self, field):
        '''
        :param field: Output field name of this job
        :type field: string

        Returns a dict containing a valid job-based object reference
        to refer to an output of this job.  This can be used directly
        in place of a DNAnexus link when used as a job output value.
        For example, after creating a subjob, the following app
        snippet uses a reference to the new job's output as part of
        its own output::

            mysubjob = dxpy.new_dxjob({}, "my_function")
            return {"myfileoutput": mysubjob.get_output_ref("output_field_name")}
        '''

        return {"$dnanexus_link": {"job": self._dxid, "field": field}}

    def _get_state(self, **kwargs):
        '''
        :returns: State of the remote object
        :rtype: string

        Queries the API server for the job's state.

        Note that this function is shorthand for:

            dxjob.describe(io=False, **kwargs)["state"]

        '''

        return self.describe(io=False, **kwargs)["state"]
