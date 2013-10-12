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
DXApplet Handler
++++++++++++++++

Applets are data objects that store application logic, including
specifications for executing it, and (optionally) input and output
signatures. They can be run by calling the :func:`DXApplet.run` method.

"""

import dxpy
from dxpy.bindings import *
from dxpy.utils import merge

class DXExecutable:
    '''Methods in :class:`!DXExecutable` are used by both
    :class:`~dxpy.bindings.dxapp.DXApp` and
    :class:`~dxpy.bindings.dxapplet.DXApplet`.
    '''
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("This class is a mix-in. Use DXApp or DXApplet instead.")

    def run(self, executable_input, project=None, folder="/", name=None, instance_type=None,
            depends_on=None, details=None, delay_workspace_destruction=None,
            extra_args=None, **kwargs):
        '''
        :param executable_input: Hash of the executable's input arguments
        :type executable_input: dict
        :param project: Project ID of the project context
        :type project: string
        :param folder: Folder in which executable's outputs will be placed in *project*
        :type folder: string
        :param name: Name for the new job (default is "<name of the executable>")
        :type name: string
        :param instance_type: Instance type on which the jobs will be run, or a dict mapping function names to instance type requests
        :type instance_type: string or dict
        :param depends_on: List of data objects or jobs to wait that need to enter the "closed" or "done" states, respectively, before the new job will be run; each element in the list can either be a dxpy handler or a string ID
        :type depends_on: list
        :param details: Details to set for the job
        :type details: dict or list
        :param delay_workspace_destruction: Whether to keep the job's temporary workspace around for debugging purposes for 3 days after it succeeds or fails
        :type delay_workspace_destruction: boolean
        :param extra_args: If provided, a hash of options that will be merged into the underlying JSON given for the API call
        :type extra_args: dict
        :returns: Object handler of the newly created job
        :rtype: :class:`~dxpy.bindings.dxjob.DXJob`

        Creates a new job that executes the function "main" of this executable with
        the given input *executable_input*.

        '''
        if project is None:
            project = dxpy.WORKSPACE_ID

        run_input = {"input": executable_input,
                     "folder": folder}
        if name is not None:
            run_input["name"] = name
        if instance_type is not None:
            if isinstance(instance_type, basestring):
                run_input["systemRequirements"] = {"*": {"instanceType": instance_type}}
            elif isinstance(instance_type, dict):
                run_input["systemRequirements"] = {stage: {"instanceType": stage_inst} for stage, stage_inst in instance_type.iteritems()}
            else:
                raise DXError('Expected instance_type field to be either a string or a dict')

        if depends_on is not None:
            run_input["dependsOn"] = []
            if isinstance(depends_on, list):
                for item in depends_on:
                    if isinstance(item, DXJob) or isinstance(item, DXDataObject):
                        if item.get_id() is None:
                            raise DXError('A dxpy handler given in depends_on does not have an ID set')
                        run_input["dependsOn"].append(item.get_id())
                    elif isinstance(item, basestring):
                        run_input['dependsOn'].append(item)
                    else:
                        raise DXError('Expected elements of depends_on to only be either instances of DXJob or DXDataObject, or strings')
            else:
                raise DXError('Expected depends_on field to be a list')

        if details is not None:
            run_input["details"] = details

        if delay_workspace_destruction is not None:
            run_input["delayWorkspaceDestruction"] = delay_workspace_destruction

        if dxpy.JOB_ID is None:
            run_input["project"] = project

        if extra_args is not None:
            merge(run_input, extra_args)

        if isinstance(self, DXApplet):
            return DXJob(dxpy.api.applet_run(self._dxid, run_input, **kwargs)["id"])
        elif isinstance(self, dxpy.bindings.DXAnalysisWorkflow):
            return DXAnalysis(dxpy.api.workflow_run(self._dxid, run_input, **kwargs)["id"])
        elif self._dxid is not None:
            return DXJob(dxpy.api.app_run(self._dxid, input_params=run_input, **kwargs)["id"])
        else:
            return DXJob(dxpy.api.app_run('app-' + self._name, alias=self._alias,
                                          input_params=run_input,
                                          **kwargs)["id"])


############
# DXApplet #
############

def _makeNonexistentAPIWrapper(method):
    def nonexistentAPIWrapper(object_id, input_params=None, always_retry=None, **kwargs):
        raise DXError("Wrapper for " + method + " does not exist")
    return nonexistentAPIWrapper

class DXApplet(DXDataObject, DXExecutable):
    '''
    Remote applet object handler.

    .. py:attribute:: runSpec

       The applet's run specification (a dict indicating, among other things, how the code of the
       applet is to be interpreted). See `the API docs for Run Specification
       <https://wiki.dnanexus.com/API-Specification-v1.0.0/IO-and-Run-Specifications#Run-Specification>`_
       for more information.

    .. py:attribute:: dxapi

       String containing the version of the DNAnexus API that the applet should run against.

    .. py:attribute:: access

       The applet's access requirements hash (a dict indicating any nonstandard permissions, such
       as requiring access to the internet, that are needed by the applet). See `the API docs for
       Access Requirements
       <https://wiki.dnanexus.com/API-Specification-v1.0.0/IO-and-Run-Specifications#Access-Requirements>`_
       for more information.

    .. py:attribute:: title

       String containing the (human-readable) title of the app

    .. py:attribute:: summary

       String containing a short, one-line summary of the applet's purpose

    .. py:attribute:: description

       String of free-form text (`Markdown <http://daringfireball.net/projects/markdown/>`_ syntax
       is supported) containing a description of the applet. The description is presented to users
       to help them understand the purpose of the app and how to invoke it.

    .. py:attribute:: developerNotes

       String of free-form text (`Markdown <http://daringfireball.net/projects/markdown/>`_ syntax
       is supported) containing information about the internals or implementation details of the
       applet, suitable for developers or advanced users.

    .. automethod:: _new
    '''

    _class = "applet"

    _describe = staticmethod(dxpy.api.applet_describe)
    _add_types = staticmethod(_makeNonexistentAPIWrapper("/applet-xxxx/addTypes"))
    _remove_types = staticmethod(_makeNonexistentAPIWrapper("/applet-xxxx/removeTypes"))
    _get_details = staticmethod(dxpy.api.applet_get_details)
    _set_details = staticmethod(_makeNonexistentAPIWrapper("/applet-xxxx/setDetails"))
    _set_visibility = staticmethod(_makeNonexistentAPIWrapper("/applet-xxxx/setVisibility"))
    _rename = staticmethod(dxpy.api.applet_rename)
    _set_properties = staticmethod(dxpy.api.applet_set_properties)
    _add_tags = staticmethod(dxpy.api.applet_add_tags)
    _remove_tags = staticmethod(dxpy.api.applet_remove_tags)
    _close = staticmethod(_makeNonexistentAPIWrapper("/applet-xxxx/close"))
    _list_projects = staticmethod(dxpy.api.applet_list_projects)

    def _new(self, dx_hash, **kwargs):
        '''
        :param dx_hash: Standard hash populated in :func:`dxpy.bindings.DXDataObject.new()` containing attributes common to all data object classes.
        :type dx_hash: dict
        :param runSpec: Run specification
        :type runSpec: dict
        :param dxapi: API version string
        :type dxapi: string
        :param inputSpec: Input specification (optional)
        :type inputSpec: dict
        :param outputSpec: Output specification (optional)
        :type outputSpec: dict
        :param access: Access specification (optional)
        :type access: dict
        :param title: Title string (optional)
        :type title: string
        :param summary: Summary string (optional)
        :type summary: string
        :param description: Description string (optional)
        :type description: string

        .. note:: It is highly recommended that the higher-level module
           :mod:`dxpy.app_builder` or (preferably) its frontend `dx build
           <https://wiki.dnanexus.com/Command-Line-Client/Index-of-dx-Commands#build>`_
           be used instead for applet creation.

        Creates an applet with the given parameters. See the API
        documentation for the `/applet/new
        <https://wiki.dnanexus.com/API-Specification-v1.0.0/Applets#API-method%3A-%2Fapplet%2Fnew>`_
        method for more info. The applet is not run until :meth:`run()`
        is called.

        '''
        for field in 'runSpec', 'dxapi':
            if field not in kwargs:
                raise DXError("%s: Keyword argument %s is required" % (self.__class__.__name__, field))
            dx_hash[field] = kwargs[field]
            del kwargs[field]
        for field in 'inputSpec', 'outputSpec', 'access', 'title', 'summary', 'description':
            if field in kwargs:
                dx_hash[field] = kwargs[field]
                del kwargs[field]

        resp = dxpy.api.applet_new(dx_hash, **kwargs)
        self.set_ids(resp["id"], dx_hash["project"])

    def get(self, **kwargs):
        """
        :returns: Full specification of the remote applet object
        :rtype: dict

        Returns the contents of the applet. The result includes the
        key-value pairs as specified in the API documentation for the
        `/applet-xxxx/get
        <https://wiki.dnanexus.com/API-Specification-v1.0.0/Applets#API-method%3A-%2Fapplet-xxxx%2Fget>`_
        method.
        """
        return dxpy.api.applet_get(self._dxid, **kwargs)

    def run(self, applet_input, *args, **kwargs):
        """
        Creates a new job that executes the function "main" of this applet with
        the given input *applet_input*.

        See :meth:`dxpy.bindings.dxapplet.DXExecutable.run` for the available
        args.
        """
        # Rename applet_input arg to preserve API compatibility when calling
        # DXApplet.run(applet_input=...)
        return super(DXApplet, self).run(applet_input, *args, **kwargs)
