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
DXWorkflow Handler
++++++++++++++++++

Workflows are records created via the website which contain metadata
to run a series of jobs.  They can be run by calling the
:func:`DXWorkflow.run` method.  Inputs that have not been bound yet in
the workflow need to be provided using input field names of the form
"N.name", where "name" is the name of an input to the Nth stage
(starting from 0).  Inputs already bound in the workflow can be
overridden by setting them here as well.

"""

import json
import copy
from collections import OrderedDict
from argparse import Namespace

import dxpy
from dxpy.bindings import DXDataObject, get_handler
from dxpy.exceptions import DXError
from dxpy.cli.exec_io import ExecutableInputs, stage_to_job_refs

##############
# DXWorkflow #
##############

class DXWorkflow(DXDataObject):
    '''
    Remote workflow object handler.
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
