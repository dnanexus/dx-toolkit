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

############
# DXApplet #
############

class DXApplet(DXDataObject):
    '''
    Remote applet object handler.

    .. automethod:: _new
    '''

    _class = "applet"

    _describe = staticmethod(dxpy.api.appletDescribe)
    _add_types = staticmethod(dxpy.api.appletAddTypes)
    _remove_types = staticmethod(dxpy.api.appletRemoveTypes)
    _get_details = staticmethod(dxpy.api.appletGetDetails)
    _set_details = staticmethod(dxpy.api.appletSetDetails)
    _set_visibility = staticmethod(dxpy.api.appletSetVisibility)
    _rename = staticmethod(dxpy.api.appletRename)
    _set_properties = staticmethod(dxpy.api.appletSetProperties)
    _add_tags = staticmethod(dxpy.api.appletAddTags)
    _remove_tags = staticmethod(dxpy.api.appletRemoveTags)
    _close = staticmethod(dxpy.api.appletClose)
    _list_projects = staticmethod(dxpy.api.appletListProjects)

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
           :mod:`dxpy.app_builder` or (preferably) its frontend
           `dx-build-applet <http://wiki.dnanexus.com/DxBuildApplet>`_ be used
           instead for applet creation.

        Creates an applet with the given parameters. See the API
        documentation for the `/applet/new
        <http://wiki.dnanexus.com/API-Specification-v1.0.0/Applets#API-method%3A-%2Fapplet%2Fnew>`_
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

        resp = dxpy.api.appletNew(dx_hash, **kwargs)
        self.set_ids(resp["id"], dx_hash["project"])

    def get(self, **kwargs):
        """
        :returns: Full specification of the remote applet object
        :rtype: dict

        Returns the contents of the applet. The result includes the
        key-value pairs as specified in the API documentation for the
        `/applet-xxxx/get
        <http://wiki.dnanexus.com/API-Specification-v1.0.0/Applets#API-method%3A-%2Fapplet-xxxx%2Fget>`_
        method.
        """
        return dxpy.api.appletGet(self._dxid, **kwargs)

    def run(self, applet_input, project=None, folder="/", name=None, instance_type=None, depends_on=None, **kwargs):
        '''
        :param applet_input: Hash of the applet's input arguments
        :type applet_input: dict
        :param project: Project ID of the project context
        :type project: string
        :param folder: Folder in which applet's outputs will be placed in *project*
        :type folder: string
        :param name: Name for the new job (default is "<name of the applet>")
        :type name: string
        :param instance_type: Instance type on which the job with entry point "main" will be run, or a dict mapping function names to instance type requests
        :type instance_type: string or dict
        :param depends_on: List of data objects or jobs to wait that need to enter the "closed" or "done" states, respectively, before the new job will be run; each element in the list can either be a dxpy handler or a string ID
        :type depends_on: list
        :returns: Object handler of the newly created job
        :rtype: :class:`~dxpy.bindings.dxjob.DXJob`

        Creates a new job that executes the function "main" of this applet with
        the given input *applet_input*.

        '''
        if project is None:
            project = dxpy.WORKSPACE_ID

        run_input = {"input": applet_input,
                     "folder": folder}
        if name is not None:
            run_input["name"] = name
        if instance_type is not None:
            if isinstance(instance_type, basestring):
                run_input["systemRequirements"] = {"main": {"instanceType": instance_type}}
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

        if dxpy.JOB_ID is None:
            run_input["project"] = project

        return DXJob(dxpy.api.appletRun(self._dxid, run_input,
                                        **kwargs)["id"])
