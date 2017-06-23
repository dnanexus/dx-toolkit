# Copyright (C) 2013-2016 DNAnexus, Inc.
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

from __future__ import print_function, unicode_literals, division, absolute_import

import dxpy
from . import DXDataObject, DXJob
from ..utils import merge, instance_type_to_sys_reqs
from ..exceptions import DXError
from ..compat import basestring

class DXExecutable:
    '''Methods in :class:`!DXExecutable` are used by
    :class:`~dxpy.bindings.dxapp.DXApp`,
    :class:`~dxpy.bindings.dxapplet.DXApplet`, and
    :class:`~dxpy.bindings.dxworkflow.DXWorkflow`
    '''
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("This class is a mix-in. Use DXApp or DXApplet instead.")

    @staticmethod
    def _get_run_input_common_fields(executable_input, **kwargs):
        '''
        Takes the same arguments as the run method. Creates an input hash for the /executable-xxxx/run method,
        translating ONLY the fields that can be handled uniformly across all executables: project, folder, name, tags,
        properties, details, depends_on, allow_ssh, debug, delay_workspace_destruction, and extra_args.
        '''
        project = kwargs.get('project') or dxpy.WORKSPACE_ID

        run_input = {"input": executable_input}
        for arg in ['folder', 'name', 'tags', 'properties', 'details']:
            if kwargs.get(arg) is not None:
                run_input[arg] = kwargs[arg]

        if kwargs.get('instance_type') is not None:
            run_input["systemRequirements"] = instance_type_to_sys_reqs(kwargs['instance_type'])

        if kwargs.get('depends_on') is not None:
            run_input["dependsOn"] = []
            if isinstance(kwargs['depends_on'], list):
                for item in kwargs['depends_on']:
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

        if kwargs.get('delay_workspace_destruction') is not None:
            run_input["delayWorkspaceDestruction"] = kwargs['delay_workspace_destruction']

        if kwargs.get('allow_ssh') is not None:
            run_input["allowSSH"] = kwargs['allow_ssh']

        if kwargs.get('debug') is not None:
            run_input["debug"] = kwargs['debug']

        if kwargs.get('priority') is not None:
            run_input["priority"] = kwargs['priority']

        if dxpy.JOB_ID is None:
            run_input["project"] = project

        if kwargs.get('extra_args') is not None:
            merge(run_input, kwargs['extra_args'])

        return run_input

    @staticmethod
    def _get_run_input_fields_for_applet(executable_input, **kwargs):
        '''
        Takes the same arguments as the run method. Creates an input
        hash for the /applet-xxxx/run method.
        '''
        # Although it says "for_applet", this is factored out of
        # DXApplet because apps currently use the same mechanism
        for unsupported_arg in ['stage_instance_types', 'stage_folders', 'rerun_stages']:
            if kwargs.get(unsupported_arg):
                raise DXError(unsupported_arg + ' is not supported for applets (only workflows)')
        return DXExecutable._get_run_input_common_fields(executable_input, **kwargs)

    def _run_impl(self, run_input, **kwargs):
        """
        Runs the executable with the specified input and returns a
        handler for the resulting execution object
        (:class:`~dxpy.bindings.dxjob.DXJob` or
        :class:`~dxpy.bindings.dxanalysis.DXAnalysis`).

        Any kwargs are passed on to :func:`~dxpy.DXHTTPRequest`.
        """
        raise NotImplementedError('_run_impl is not implemented')

    def _get_run_input(self, executable_input, **kwargs):
        """
        Takes the same arguments as the run method. Creates an input
        hash for the /executable-xxxx/run method.
        """
        raise NotImplementedError('_get_run_input is not implemented')

    def _get_required_keys(self):
        """
        Abstract method used in executable_unbuilder.dump_executable
        """
        raise NotImplementedError('_get_required_keys is not implemented')

    def _get_optional_keys(self):
        """
        Abstract method used in executable_unbuilder.dump_executable
        """
        raise NotImplementedError('_get_optional_keys is not implemented')

    def _get_describe_output_keys(self):
        """
        Abstract method used in executable_unbuilder.dump_executable
        """
        raise NotImplementedError('_get_describe_output_keys is not implemented')

    def _get_cleanup_keys(self):
        """
        Abstract method used in executable_unbuilder.dump_executable
        """
        raise NotImplementedError('_get_cleanup_keys is not implemented')

    def run(self, executable_input, project=None, folder=None, name=None, tags=None, properties=None, details=None,
            instance_type=None, stage_instance_types=None, stage_folders=None, rerun_stages=None,
            depends_on=None, allow_ssh=None, debug=None, delay_workspace_destruction=None, priority=None,
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
        :param tags: Tags to associate with the job
        :type tags: list of strings
        :param properties: Properties to associate with the job
        :type properties: dict with string values
        :param details: Details to set for the job
        :type details: dict or list
        :param instance_type: Instance type on which the jobs will be run, or a dict mapping function names to instance type requests
        :type instance_type: string or dict
        :param depends_on: List of data objects or jobs to wait that need to enter the "closed" or "done" states, respectively, before the new job will be run; each element in the list can either be a dxpy handler or a string ID
        :type depends_on: list
        :param allow_ssh: List of hostname or IP masks to allow SSH connections from
        :type allow_ssh: list
        :param debug: Configuration options for job debugging
        :type debug: dict
        :param delay_workspace_destruction: Whether to keep the job's temporary workspace around for debugging purposes for 3 days after it succeeds or fails
        :type delay_workspace_destruction: boolean
        :param priority: Priority level to request for all jobs created in the execution tree, either "normal" or "high"
        :type priority: string
        :param extra_args: If provided, a hash of options that will be merged into the underlying JSON given for the API call
        :type extra_args: dict
        :returns: Object handler of the newly created job
        :rtype: :class:`~dxpy.bindings.dxjob.DXJob`

        Creates a new job that executes the function "main" of this executable with
        the given input *executable_input*.

        '''
        # stage_instance_types, stage_folders, and rerun_stages are
        # only supported for workflows, but we include them
        # here. Applet-based executables should detect when they
        # receive a truthy workflow-specific value and raise an error.
        run_input = self._get_run_input(executable_input,
                                        project=project,
                                        folder=folder,
                                        name=name,
                                        tags=tags,
                                        properties=properties,
                                        details=details,
                                        instance_type=instance_type,
                                        stage_instance_types=stage_instance_types,
                                        stage_folders=stage_folders,
                                        rerun_stages=rerun_stages,
                                        depends_on=depends_on,
                                        allow_ssh=allow_ssh,
                                        debug=debug,
                                        delay_workspace_destruction=delay_workspace_destruction,
                                        priority=priority,
                                        extra_args=extra_args)

        return self._run_impl(run_input, **kwargs)


############
# DXApplet #
############
_applet_required_keys = ['name', 'title', 'summary', 'types', 'tags',
                         'properties', 'dxapi', 'inputSpec', 'outputSpec',
                         'runSpec', 'access', 'details']
_applet_optional_keys = []
_applet_describe_output_keys = ['properties', 'details']
_applet_cleanup_keys = ['name', 'title', 'summary', 'types', 'tags',
                        'properties', 'runSpec', 'access', 'details']

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
        <https://wiki.dnanexus.com/API-Specification-v1.0.0/Applets-and-Entry-Points#API-method:-/applet/new>`_
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
        <https://wiki.dnanexus.com/API-Specification-v1.0.0/Applets-and-Entry-Points#API-method:-/applet-xxxx/get>`_
        method.
        """
        return dxpy.api.applet_get(self._dxid, **kwargs)

    def _run_impl(self, run_input, **kwargs):
        return DXJob(dxpy.api.applet_run(self._dxid, run_input, **kwargs)["id"])

    def _get_run_input(self, executable_input, **kwargs):
        return DXExecutable._get_run_input_fields_for_applet(executable_input, **kwargs)

    def _get_required_keys(self):
        return _applet_required_keys

    def _get_optional_keys(self):
        return _applet_optional_keys

    def _get_describe_output_keys(self):
        return _applet_describe_output_keys

    def _get_cleanup_keys(self):
        return _applet_cleanup_keys

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
