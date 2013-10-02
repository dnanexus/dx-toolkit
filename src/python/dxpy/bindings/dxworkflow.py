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
import re

import dxpy
from dxpy.bindings import DXDataObject, DXExecutable, DXAnalysis, get_handler
from dxpy.exceptions import DXError, DXAPIError
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

def new_dxworkflow(title=None, summary=None, description=None, init_from=None, **kwargs):
    '''
    :param title: Workflow title (optional)
    :type title: string
    :param summary: Workflow summary (optional)
    :type summary: string
    :param description: Workflow description (optional)
    :type description: string
    :param init_from: Another analysis workflow object handler or and analysis (string or handler) from which to initialize the metadata (optional)
    :type init_from: :class:`~dxpy.bindings.dxworkflow.DXAnalysisWorkflow`, :class:`~dxpy.bindings.dxanalysis.DXAnalysis`, or string (for analysis IDs only)
    :rtype: :class:`DXAnalysisWorkflow`

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`, except `details`.

    Creates a new remote workflow object with project set to *project*
    and returns the appropriate handler.

    Example:

        r = dxpy.new_dxworkflow(title="My Workflow", description="This workflow contains...")

    Note that this function is shorthand for::

        dxworkflow = DXAnalysisWorkflow()
        dxworkflow.new(**kwargs)
    '''
    dxworkflow = DXAnalysisWorkflow()
    dxworkflow.new(title=title, summary=summary, description=description, init_from=init_from, **kwargs)
    return dxworkflow

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

    def _new(self, dx_hash, **kwargs):
        """
        :param dx_hash: Standard hash populated in :func:`dxpy.bindings.DXDataObject.new()` containing attributes common to all data object classes.
        :type dx_hash: dict
        :param title: Workflow title (optional)
        :type title: string
        :param summary: Workflow summary (optional)
        :type summary: string
        :param description: Workflow description (optional)
        :type description: string
        :param init_from: Another analysis workflow object handler or and analysis (string or handler) from which to initialize the metadata (optional)
        :type init_from: :class:`~dxpy.bindings.dxworkflow.DXAnalysisWorkflow`, :class:`~dxpy.bindings.dxanalysis.DXAnalysis`, or string (for analysis IDs only)

        Create a new remote workflow object.
        """

        if "init_from" in kwargs:
            if kwargs["init_from"] is not None:
                if not (isinstance(kwargs["init_from"], (DXAnalysisWorkflow, DXAnalysis)) or \
                        (isinstance(kwargs["init_from"], basestring) and \
                         re.compile('^analysis-[0-9A-Za-z]{24}$').match(kwargs["init_from"]))):
                    raise DXError("Expected init_from to be an instance of DXAnalysisWorkflow or DXAnalysis, or to be a string analysis ID.")
                if isinstance(kwargs["init_from"], basestring):
                    dx_hash["initializeFrom"] = {"id": kwargs["init_from"]}
                else:
                    dx_hash["initializeFrom"] = {"id": kwargs["init_from"].get_id(),
                                                 "project": kwargs["init_from"].get_proj_id()}
            del kwargs["init_from"]

        if "title" in kwargs:
            if kwargs["title"] is not None:
                dx_hash["title"] = kwargs["title"]
            del kwargs["title"]

        if "summary" in kwargs:
            if kwargs["summary"] is not None:
                dx_hash["summary"] = kwargs["summary"]
            del kwargs["summary"]

        if "description" in kwargs:
            if kwargs["description"] is not None:
                dx_hash["description"] = kwargs["description"]
            del kwargs["description"]

        resp = dxpy.api.workflow_new(dx_hash, **kwargs)
        self.set_ids(resp["id"], dx_hash["project"])

    def _add_edit_version_to_request(self, request_hash, edit_version=None):
        if edit_version is None:
            request_hash["editVersion"] = self.editVersion
        else:
            request_hash["editVersion"] = edit_version

    def _get_stage_id(self, stage):
        '''
        :param stage: Either a number (for the nth stage, starting from 0), or a stage ID
        :type stage: int or string
        :returns: The stage ID (this is a no-op if it was already a string)
        :raises: :class:`~dxpy.exceptions.DXError` if *stage* could not be parsed or resolved to a stage ID
        '''
        stage_id = None
        if isinstance(stage, basestring):
            stage_id = stage
        else:
            try:
                stage_index = int(stage)
            except:
                raise DXError('DXAnalysisWorkflow: "stage" was neither a string stage ID nor an integer index')
            if stage_index < 0 or stage_index >= len(self.stages):
                raise DXError('DXAnalysisWorkflow: the workflow contains ' + str(len(self.stages)) + ' stage(s), and the provided value for "stage" is out of range')
            stage_id = self.stages[stage_index].get("id")

        if re.compile('^stage-[0-9A-Za-z]{24}$').match(stage_id) is None:
            raise DXError('DXAnalysisWorkflow: "stage" did not resolve to a properly formed stage ID')

        return stage_id

    def add_stage(self, executable, name=None, folder=None, stage_input=None, edit_version=None, **kwargs):
        '''
        :param executable: string or a handler for an app or applet
        :type executable: string, DXApplet, or DXApp
        :param name: name for the stage (optional)
        :type name: string
        :param stage_input: input fields to bind as default inputs for the executable (optional)
        :type stage_input: dict
        :param edit_version: if provided, the edit version of the workflow that should be modified; if not provided, the current edit version will be used (optional)
        :type edit_version: int
        :returns: ID of the added stage
        :rtype: string
        :raises: :class:`~dxpy.exceptions.DXError` if *executable* is not an expected type :class:`~dxpy.exceptions.DXAPIError` for errors thrown from the API call

        Adds the specified executable as a new stage in the workflow.
        '''
        if isinstance(executable, basestring):
            exec_id = executable
        elif isinstance(executable, DXExecutable):
            exec_id = executable.get_id()
        else:
            raise DXError("dxpy.DXWorkflow.add_stage: executable must be a string or an instance of DXApplet or DXApp")
        add_stage_input = {"executable": exec_id}
        if name is not None:
            add_stage_input["name"] = name
        if folder is not None:
            add_stage_input["folder"] = folder
        if stage_input is not None:
            add_stage_input["input"] = stage_input
        self._add_edit_version_to_request(add_stage_input, edit_version)
        try:
            result = dxpy.api.workflow_add_stage(self._dxid, add_stage_input, **kwargs)
        finally:
            self.describe() # update cached describe
        return result['stage']

    def get_stage(self, stage, **kwargs):
        '''
        :param stage: Either a number (for the nth stage, starting from 0), or a stage ID to describe
        :type stage: int or string
        :returns: Hash of stage descriptor in workflow
        '''
        stage_id = self._get_stage_id(stage)
        try:
            return next(stage for stage in self.stages if stage['id'] == stage_id)
        except StopIteration:
            raise DXError('The stage ID ' + stage_id + ' could not be found')

    def remove_stage(self, stage, edit_version=None, **kwargs):
        '''
        :param stage: Either a number (for the nth stage, starting from 0), or a stage ID to remove
        :type stage: int or string
        :param edit_version: if provided, the edit version of the workflow that should be modified; if not provided, the current edit version will be used (optional)
        :type edit_version: int
        :returns: Stage ID that was removed
        :rtype: string

        Removes the specified stage from the workflow
        '''
        stage_id = self._get_stage_id(stage)
        remove_stage_input = {"stage": stage_id}
        self._add_edit_version_to_request(remove_stage_input, edit_version)
        try:
            dxpy.api.workflow_remove_stage(self._dxid, remove_stage_input, **kwargs)
        finally:
            self.describe() # update cached describe
        return stage_id

    def move_stage(self, stage, new_index, edit_version=None, **kwargs):
        '''
        :param stage: Either a number (for the nth stage, starting from 0), or a stage ID to remove
        :type stage: int or string
        :param new_index: The new position in the order of stages that the specified stage should have (where 0 indicates the first stage)
        :type new_index: int
        :param edit_version: if provided, the edit version of the workflow that should be modified; if not provided, the current edit version will be used (optional)
        :type edit_version: int

        Removes the specified stage from the workflow
        '''
        stage_id = self._get_stage_id(stage)
        move_stage_input = {"stage": stage_id,
                            "newIndex": new_index}
        self._add_edit_version_to_request(move_stage_input, edit_version)
        try:
            dxpy.api.workflow_move_stage(self._dxid, move_stage_input, **kwargs)
        finally:
            self.describe() # update cached describe

    def update(self, title=None, unset_title=False, summary=None, description=None, stages=None,
               edit_version=None, **kwargs):
        '''
        :param title: workflow title to set; cannot be provided with *unset_title* set to True
        :type title: string
        :param unset_title: whether to unset the title; cannot be provided with string value for *title*
        :type unset_title: boolean
        :param summary: workflow summary to set
        :type summary: string
        :param description: workflow description to set
        :type description: string
        :param stages: updates to the stages to make; see API documentation for /workflow-xxxx/update for syntax of this field; use :meth:`update_stage()` to update a single stage
        :type stages: dict
        :param edit_version: if provided, the edit version of the workflow that should be modified; if not provided, the current edit version will be used (optional)
        :type edit_version: int

        Make general metadata updates to the workflow
        '''
        update_input = {}
        if title is not None and unset_title:
            raise DXError('dxpy.DXWorkflow.update: cannot provide both "title" and set "unset_title"')
        if title is not None:
            update_input["title"] = title
        if unset_title:
            update_input["title"] = None
        if summary is not None:
            update_input["summary"] = summary
        if description is not None:
            update_input["description"] = description
        if stages is not None:
            update_input["stages"] = stages

        # only perform update if there are changes to make
        if update_input:
            self._add_edit_version_to_request(update_input, edit_version)
            try:
                dxpy.api.workflow_update(self._dxid, update_input, **kwargs)
            finally:
                self.describe() # update cached describe

    def update_stage(self, stage, executable=None, force=False,
                     name=None, unset_name=False, folder=None, stage_input=None,
                     edit_version=None, **kwargs):
        '''
        :param stage: Either a number (for the nth stage, starting from 0), or a stage ID to remove
        :type stage: int or string
        :param executable: string or a handler for an app or applet
        :type executable: string, DXApplet, or DXApp
        :param force: whether to use *executable* even if it is incompatible with the previous executable's spec
        :type force: boolean
        :param name: name for the stage; cannot be provided with *unset_name* set to True
        :type name: string
        :param unset_name: whether to unset the stage name; cannot be provided with string value for *name*
        :type unset_name: boolean
        :param stage_input: input fields to bind as default inputs for the executable (optional)
        :type stage_input: dict
        :param edit_version: if provided, the edit version of the workflow that should be modified; if not provided, the current edit version will be used (optional)
        :type edit_version: int

        Removes the specified stage from the workflow
        '''
        stage_id = self._get_stage_id(stage)

        if name is not None and unset_name:
            raise DXError('dxpy.DXWorkflow.update_stage: cannot provide both "name" and set "unset_name"')

        if executable is not None:
            if isinstance(executable, basestring):
                exec_id = executable
            elif isinstance(executable, DXExecutable):
                exec_id = executable.get_id()
            else:
                raise DXError("dxpy.DXWorkflow.update_stage: executable (if provided) must be a string or an instance of DXApplet or DXApp")
            update_stage_exec_input = {"stage": stage_id,
                                       "executable": exec_id,
                                       "force": force}
            self._add_edit_version_to_request(update_stage_exec_input, edit_version)
            try:
                dxpy.api.workflow_update_stage_executable(self._dxid, update_stage_exec_input, **kwargs)
            finally:
                self.describe() # update cached describe

        # Construct hash and update the workflow's stage if necessary
        update_stage_input = {}
        if name is not None:
            update_stage_input["name"] = name
        if unset_name:
            update_stage_input["name"] = None
        if folder:
            update_stage_input["folder"] = folder
        if stage_input:
            update_stage_input["input"] = stage_input
        if update_stage_input:
            update_input = {"stages": {stage_id: update_stage_input}}
            self._add_edit_version_to_request(update_input, edit_version)
            try:
                dxpy.api.workflow_update(self._dxid, update_input, **kwargs)
            finally:
                self.describe() # update cached describe

    def _get_input_name(self, input_str):
        if '.' in input_str and not input_str.startswith('stage-'):
            stage_index = int(input_str[:input_str.find('.')])
            return self.stages[stage_index]['id'] + input_str[input_str.find('.'):]
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
