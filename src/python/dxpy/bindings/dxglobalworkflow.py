# Copyright (C) 2013-2018 DNAnexus, Inc.
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
DXGlobalWorkflow Handler
++++++++++++++++++++++++

"""

from __future__ import print_function, unicode_literals, division, absolute_import

import dxpy
from . import DXObject, DXAnalysis, DXExecutable, verify_string_dxid
from ..exceptions import DXError
from ..compat import basestring

####################
# DXGlobalWorkflow #
####################


class DXGlobalWorkflow(DXObject, DXExecutable):
    '''
    Remote global workflow object handler.
    '''

    _class = "globalworkflow"

    def __init__(self, dxid=None, name=None, alias=None):
        DXObject.__init__(self)
        if dxid is not None or name is not None:
            self.set_id(dxid=dxid, name=name, alias=alias)

        # caches for underlying workflow instances and descriptions per region
        # (they are immutable for a given global workflow)
        self._workflows_by_region = {}
        self._workflow_desc_by_region = {}

    def set_id(self, dxid=None, name=None, alias=None):
        '''
        :param dxid: Global workflow ID
        :type dxid: string
        :param name: Global workflow name
        :type name: string
        :param alias: Global workflow version or tag
        :type alias: string
        :raises: :exc:`~dxpy.exceptions.DXError` if *dxid* and some other input are both given or if neither *dxid* nor *name* are given

        Discards the currently stored ID and associates the handler
        with the requested parameters.  Note that if *dxid* is given,
        the other fields should not be given, and if *name* is given,
        *alias* has default value "default".

        '''
        self._dxid = None
        self._name = None
        self._alias = None
        if dxid is not None:
            if name is not None or alias is not None:
                raise DXError("Did not expect name or alias to be given if dxid is given")
            verify_string_dxid(dxid, self._class)
            self._dxid = dxid
        elif name is not None:
            self._name = name
            if not isinstance(name, basestring):
                raise DXError("Global workflow name needs to be a string: %r" % (name,))
            if alias is not None:
                if not isinstance(alias, basestring):
                    raise DXError("Global workflow alias needs to be a string: %r" % (alias,))
                self._alias = alias
            else:
                self._alias = 'default'

    def get_id(self):
        '''
        :returns: Object ID of associated global workflow
        :rtype: string

        Returns the object ID of the global workflow that the handler is currently
        associated with.
        '''
        if self._dxid is not None:
            return self._dxid
        else:
            return 'globalworkflow-' + self._name + '/' + self._alias

    def new(self, **kwargs):
        '''

        .. note:: It is highly recommended that `dx build --create-global-workflow
           <https://wiki.dnanexus.com/Command-Line-Client/Index-of-dx-Commands#build>`_
           be used instead for global workflow creation.

        Creates a workflow in the global space with the given parameters by using
        the specified workflow(s) in regionalOptions as a base.

        The workflow is available only to its developers until
        :meth:`publish()` is called, and is not run until :meth:`run()`
        is called.

        '''
        dx_hash = {}

        for field in ['version', 'name', 'regionalOptions']:
            if field not in kwargs:
                raise DXError("%s: Keyword argument %s is required" % (self.__class__.__name__, field))
            dx_hash[field] = kwargs[field]
            del kwargs[field]

        if "bill_to" in kwargs:
            dx_hash['billTo'] = kwargs['bill_to']
            del kwargs["bill_to"]

        resp = dxpy.api.global_workflow_new(dx_hash, **kwargs)
        self.set_id(dxid=resp["id"])

    def describe(self, fields=None, **kwargs):
        '''
        :param fields: Hash where the keys are field names that should be returned, and values should be set to True (default is that all fields are returned)
        :type fields: dict
        :returns: Description of the remote global workflow object
        :rtype: dict

        Returns a dict with a description of the workflow.

        '''
        describe_input = {}
        if fields:
            describe_input['fields'] = fields

        if self._dxid is not None:
            self._desc = dxpy.api.global_workflow_describe(self._dxid, input_params=describe_input, **kwargs)
        else:
            self._desc = dxpy.api.global_workflow_describe('globalworkflow-' + self._name, alias=self._alias,
                                                           input_params=describe_input, **kwargs)

        return self._desc

    def publish(self, **kwargs):
        """
        Publishes the global workflow, so all users can find it and use it on the platform.

        The current user must be a developer of the workflow.
        """
        if self._dxid is not None:
            return dxpy.api.global_workflow_publish(self._dxid, **kwargs)
        else:
            return dxpy.api.global_workflow_publish('globalworkflow-' + self._name, alias=self._alias, **kwargs)

    def describe_underlying_workflow(self, region, describe_output=None):
        """
        :param region: region name
        :type region: string
        :param describe_output: description of a global workflow
        :type describe_output: dict
        :returns: object description of a workflow
        :rtype: : dict

        Returns an object description of an underlying workflow from a given region.
        """
        assert(describe_output is None or describe_output.get('class', '') == 'globalworkflow')

        if region is None:
            raise DXError(
                'DXGlobalWorkflow: region must be provided to get an underlying workflow')

        # Perhaps we have cached it already
        if region in self._workflow_desc_by_region:
            return self._workflow_desc_by_region[region]

        if not describe_output:
            describe_output = self.describe()

        if region not in describe_output['regionalOptions'].keys():
            raise DXError('DXGlobalWorkflow: the global workflow {} is not enabled in region {}'.format(
                self.get_id(), region))

        underlying_workflow_id = describe_output['regionalOptions'][region]['workflow']
        dxworkflow = dxpy.DXWorkflow(underlying_workflow_id)
        dxworkflow_desc = dxworkflow.describe()
        self._workflow_desc_by_region = dxworkflow_desc
        return dxworkflow_desc

    def get_underlying_workflow(self, region, describe_output=None):
        """
        :param region: region name
        :type region: string
        :param describe_output: description of a global workflow
        :type describe_output: dict
        :returns: object handler of a workflow
        :rtype: :class:`~dxpy.bindings.dxworkflow.DXWorkflow`

        Returns an object handler of an underlying workflow from a given region.
        """
        assert(describe_output is None or describe_output.get('class') == 'globalworkflow')

        if region is None:
            raise DXError(
                'DXGlobalWorkflow: region must be provided to get an underlying workflow')

        # Perhaps we have cached it already
        if region in self._workflows_by_region:
            return self._workflows_by_region[region]

        if not describe_output:
            describe_output = self.describe()

        if region not in describe_output['regionalOptions'].keys():
            raise DXError('DXGlobalWorkflow: the global workflow {} is not enabled in region {}'.format(
                self.get_id(), region))

        underlying_workflow_id = describe_output['regionalOptions'][region]['workflow']
        self._workflow_desc_by_region = dxpy.DXWorkflow(underlying_workflow_id)
        return dxpy.DXWorkflow(underlying_workflow_id)

    def append_underlying_workflow_desc(self, describe_output, region):
        """
        :param region: region name
        :type region: string
        :param describe_output: description of a global workflow
        :type describe_output: dict
        :returns: object description of the global workflow
        :rtype: : dict

        Appends stages, inputs, outputs and other workflow-specific metadata to a global workflow describe output.

        Note: global workflow description does not contain functional metadata (stages, IO), since this data
        is region-specific (due to applets and bound inputs) and so reside only in region-specific underlying
        workflows. We add them to global_workflow_desc so that it can be used for a workflow or a global workflow
        """
        assert(describe_output is None or describe_output.get('class') == 'globalworkflow')

        underlying_workflow_desc = self.describe_underlying_workflow(region,
                                                                     describe_output=describe_output)
        for field in ['inputs', 'outputs', 'inputSpec', 'outputSpec', 'stages']:
            describe_output[field] = underlying_workflow_desc[field]
        return describe_output

    def _get_input_name(self, input_str, region=None, describe_output=None):
        dxworkflow = self.get_underlying_workflow(region, describe_output=describe_output)
        return dxworkflow._get_input_name(input_str)

    def _get_run_input(self, workflow_input, project=None, **kwargs):
        """
        Checks the region in which the global workflow is run
        and returns the input associated with the underlying workflow
        from that region.
        """
        region = dxpy.api.project_describe(project,
                                           input_params={"fields": {"region": True}})["region"]
        dxworkflow = self.get_underlying_workflow(region)
        return dxworkflow._get_run_input(workflow_input, **kwargs)

    def _run_impl(self, run_input, **kwargs):
        if self._dxid is not None:
            return DXAnalysis(dxpy.api.global_workflow_run(self._dxid, input_params=run_input, **kwargs)["id"])
        else:
            return DXAnalysis(dxpy.api.global_workflow_run('globalworkflow-' + self._name, alias=self._alias,
                                                           input_params=run_input,
                                                           **kwargs)["id"])

    def run(self, workflow_input, *args, **kwargs):
        '''
        :param workflow_input: Dictionary of the workflow's input arguments; see below for more details
        :type workflow_input: dict
        :param instance_type: Instance type on which all stages' jobs will be run, or a dict mapping function names to instance types. These may be overridden on a per-stage basis if stage_instance_types is specified.
        :type instance_type: string or dict
        :param stage_instance_types: A dict mapping stage IDs, names, or indices to either a string (representing an instance type to be used for all functions in that stage), or a dict mapping function names to instance types.
        :type stage_instance_types: dict
        :param stage_folders: A dict mapping stage IDs, names, indices, and/or the string "*" to folder values to be used for the stages' output folders (use "*" as the default for all unnamed stages)
        :type stage_folders: dict
        :param rerun_stages: A list of stage IDs, names, indices, and/or the string "*" to indicate which stages should be run even if there are cached executions available
        :type rerun_stages: list of strings
        :returns: Object handler of the newly created analysis
        :rtype: :class:`~dxpy.bindings.dxanalysis.DXAnalysis`

        Run the workflow. See :meth:`dxpy.bindings.dxapplet.DXExecutable.run` for additional args.

        When providing input for the workflow, keys should be of one of the following forms:

        * "N.name" where *N* is the stage number, and *name* is the
          name of the input, e.g. "0.reads" if the first stage takes
          in an input called "reads"

        * "stagename.name" where *stagename* is the stage name, and
          *name* is the name of the input within the stage

        * "stageID.name" where *stageID* is the stage ID, and *name*
          is the name of the input within the stage

        * "name" where *name* is the name of a workflow level input
          (defined in inputs) or the name that has been
          exported for the workflow (this name will appear as a key
          in the "inputSpec" of this workflow's description if it has
          been exported for this purpose)

        '''
        return super(DXGlobalWorkflow, self).run(workflow_input, *args, **kwargs)
