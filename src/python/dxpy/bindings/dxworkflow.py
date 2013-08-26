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
DXWorkflow Handlers
+++++++++++++++++++

Workflows contain metadata for a set of jobs to be run together.
There are currently two types of workflows with two respective handler
classes that are supported:

* :class:`DXRecordWorkflow` for records created via the website of
  type "pipeline" which contain metadata to run a series of jobs.
  They can be run by calling the :func:`DXRecordWorkflow.run` method.
  Inputs that have not been bound yet in the workflow need to be
  provided using input field names of the form "N.name", where "name"
  is the name of an input to the Nth stage (starting from 0).  Inputs
  already bound in the workflow can be overridden by setting them here
  as well.

* :class:`DXAnalysisWorkflow` for new workflows that have class
  "workflow".  These workflows create an analysis object when run and
  package the resulting jobs together.

The function :func:`DXWorkflow` can be used to return the appropriate
class automatically.

"""

import json
import copy
from collections import OrderedDict
from argparse import Namespace

import dxpy
from dxpy.bindings import DXDataObject, DXExecutable, get_handler
from dxpy.exceptions import DXError
from dxpy.cli.exec_io import ExecutableInputs, stage_to_job_refs

####################
# DXRecordWorkflow #
####################

class DXRecordWorkflow(DXDataObject, DXExecutable):
    '''
    Remote workflow object handler.  This class is used for the record
    data objects with type "pipeline".
    '''

    _class = "record"

    _describe = staticmethod(dxpy.api.record_describe)
    _add_types = staticmethod(dxpy.api.record_add_types)
    _remove_types = staticmethod(dxpy.api.record_remove_types)
    _get_details = staticmethod(dxpy.api.record_get_details)
    _set_details = staticmethod(dxpy.api.record_set_details)
    _set_visibility = staticmethod(dxpy.api.record_set_visibility)
    _rename = staticmethod(dxpy.api.record_rename)
    _set_properties = staticmethod(dxpy.api.record_set_properties)
    _add_tags = staticmethod(dxpy.api.record_add_tags)
    _remove_tags = staticmethod(dxpy.api.record_remove_tags)
    _close = staticmethod(dxpy.api.record_close)
    _list_projects = staticmethod(dxpy.api.record_list_projects)

    def run(self, workflow_input, project=None, folder="/", name=None, **kwargs):
        '''
        :param workflow_input: Hash of the workflow's input arguments, with keys equal to "N.name" where N is the stage number and name is the name of the input, e.g. "0.reads" if the first stage takes in an input called "reads"
        :type workflow_input: dict
        :param project: Project ID in which to run the jobs (project context)
        :type project: string
        :param folder: Folder in which the workflow's outputs will be placed in *project*
        :type folder: string
        :param name: String to append to the default job name for each job (default is the workflow's name)
        :type name: string
        :returns: list of job IDs in order of the stages

        Run each stage in the associated workflow
        '''

        workflow_name = self.describe()['name']
        workflow_spec = self.get_details()
        workflow_details = copy.deepcopy(workflow_spec)
        if workflow_spec.get('version') not in range(2, 6):
            raise DXError("Unrecognized workflow version {v} in {w}\n".format(v=workflow_spec.get('version', '<none>'), w=self))

        launched_jobs = OrderedDict()
        for stage in workflow_spec['stages']:
            launched_jobs[stage['id']] = None

        for k in range(len(workflow_spec['stages'])):
            workflow_spec['stages'][k].setdefault('key', str(k))
            for i in workflow_spec['stages'][k].get('inputs', {}).keys():
                if workflow_spec['stages'][k]['inputs'][i] == "":
                    del workflow_spec['stages'][k]['inputs'][i]

        for k, stage in enumerate(workflow_spec['stages']):
            inputs_from_stage = {k: stage_to_job_refs(v, launched_jobs) for k, v in stage['inputs'].iteritems() if v is not None}

            exec_id = stage['app']['id'] if 'id' in stage['app'] else stage['app']
            if isinstance(exec_id, dict) and '$dnanexus_link' in exec_id:
                exec_id = exec_id['$dnanexus_link']
            if exec_id.startswith('app-'):
                from dxpy.utils.resolver import get_app_from_path
                exec_id = get_app_from_path(exec_id)['id']

            executable = get_handler(exec_id)
            executable_desc = executable.describe()

            if exec_id.startswith('app-'):
                workflow_details['stages'][k]['app'] = {
                    "$dnanexus_link": 'app-' + executable_desc['name'] + '/' + executable_desc['version']
                }

            job_name = executable_desc.get('title', '')
            if job_name == '':
                job_name = executable_desc['name']
            job_name += ' - ' + (name if name is not None else workflow_name)

            exec_inputs = ExecutableInputs(executable, input_name_prefix=str(stage['key'])+".")
            exec_inputs.update(inputs_from_stage, strip_prefix=False)
            fake_args = Namespace()
            fake_args.filename = None
            fake_args.input = None
            fake_args.input_spec = None
            fake_args.input_json = json.dumps(workflow_input)
            exec_inputs.update_from_args(fake_args)
            input_json = exec_inputs.inputs

            launched_jobs[stage['id']] = executable.run(input_json, project=project, folder=folder,
                                                        name=job_name,
                                                        **kwargs)

        # Update workflow with updated executable IDs
        self.set_details(workflow_details)

        return launched_jobs.values()

######################
# DXAnalysisWorkflow #
######################

class DXAnalysisWorkflow(DXDataObject, DXExecutable):
    '''
    Remote workflow object handler.  This class is used for the
    workflow class data objects which produce an analysis when run.
    '''

    _class = "workflow"

    _describe = staticmethod(dxpy.api.workflow_describe)
    _add_types = staticmethod(dxpy.api.workflow_add_types)
    _remove_types = staticmethod(dxpy.api.workflow_remove_types)
    _get_details = staticmethod(dxpy.api.workflow_get_details)
    _set_details = staticmethod(dxpy.api.workflow_set_details)
    _set_visibility = staticmethod(dxpy.api.workflow_set_visibility)
    _rename = staticmethod(dxpy.api.workflow_rename)
    _set_properties = staticmethod(dxpy.api.workflow_set_properties)
    _add_tags = staticmethod(dxpy.api.workflow_add_tags)
    _remove_tags = staticmethod(dxpy.api.workflow_remove_tags)
    _close = staticmethod(dxpy.api.workflow_close)
    _list_projects = staticmethod(dxpy.api.workflow_list_projects)

    def _get_input_name(self, input_str):
        if '.' in input_str and not input_str.startswith('stage-'):
            stages = self.stages
            stage_index = int(input_str[:input_str.find('.')])
            return stages[stage_index]['id'] + input_str[input_str.find('.'):]
        else:
            return input_str

    def _get_effective_input(self, workflow_input):
        effective_input = {}
        for key in workflow_input:
            effective_input[self._get_input_name(key)] = workflow_input[key]
        return effective_input

    def run(self, workflow_input, *args, **kwargs):
        '''
        :param workflow_input: Hash of the workflow's input arguments; see below for more details
        :type workflow_input: dict
        :returns: Object handler of the newly created analysis
        :rtype: :class:`~dxpy.bindings.dxanalysis.DXAnalysis`

        Run the associated workflow.

        When providing input for the workflow, keys should be of one of the following three forms:

        * "N.name" where *N* is the stage number, and *name* is the
          name of the input, e.g. "0.reads" if the first stage takes
          in an input called "reads"

        * "stageID.name" where *stageID* is the stage ID, and *name*
          is the name of the input

        * "name" where *name* is the name of an input that has been
          exported for the workflow (this name will appear as a key in
          the "inputSpec" of this workflow's description if it has
          been exported for this purpose)

        See :meth:`dxpy.bindings.dxapplet.DXExecutable.run` for the available args.
        '''

        effective_input = self._get_effective_input(workflow_input)
        return super(DXAnalysisWorkflow, self).run(effective_input, *args, **kwargs)

##############
# DXWorkflow #
##############

def DXWorkflow(dxid, project=None):
    '''
    Returns the appropriate remote workflow object handler.
    '''
    if dxid is None:
        # We don't know which subclass to return
        raise DXError('DXWorkflow requires an ID to return the appropriate handler')
    if dxid.startswith('record-'):
        return DXRecordWorkflow(dxid, project)
    elif dxid.startswith('workflow-'):
        return DXAnalysisWorkflow(dxid, project)
    else:
        raise DXError('DXWorkflow requires a record or workflow ID')
