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
DXWorkflow Handler
++++++++++++++++++

Workflows are data objects which contain metadata for a set of jobs to
be run together.  They can be run by calling the
:func:`DXWorkflow.run` method.

"""

from __future__ import print_function, unicode_literals, division, absolute_import

import re
import dxpy
from ..system_requirements import SystemRequirementsDict
from ..bindings import DXDataObject, DXExecutable, DXAnalysis
from ..exceptions import DXError
from ..compat import basestring

##############
# DXWorkflow #
##############

def new_dxworkflow(title=None, summary=None, description=None, output_folder=None, init_from=None, **kwargs):
    '''
    :param title: Workflow title (optional)
    :type title: string
    :param summary: Workflow summary (optional)
    :type summary: string
    :param description: Workflow description (optional)
    :type description: string
    :param output_folder: Default output folder of the workflow (optional)
    :type output_folder: string
    :param init_from: Another analysis workflow object handler or and analysis (string or handler) from which to initialize the metadata (optional)
    :type init_from: :class:`~dxpy.bindings.dxworkflow.DXWorkflow`, :class:`~dxpy.bindings.dxanalysis.DXAnalysis`, or string (for analysis IDs only)
    :rtype: :class:`DXWorkflow`

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`, except `details`.

    Creates a new remote workflow object with project set to *project*
    and returns the appropriate handler.

    Example:

        r = dxpy.new_dxworkflow(title="My Workflow", description="This workflow contains...")

    Note that this function is shorthand for::

        dxworkflow = DXWorkflow()
        dxworkflow.new(**kwargs)
    '''
    dxworkflow = DXWorkflow()
    dxworkflow.new(title=title, summary=summary, description=description, output_folder=output_folder, init_from=init_from, **kwargs)
    return dxworkflow

class DXWorkflow(DXDataObject, DXExecutable):
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
        :param output_folder: Default output folder of the workflow (optional)
        :type output_folder: string
        :param stages: Stages of the workflow (optional)
        :type stages: array of dictionaries
        :param workflow_inputs: Workflow-level input specification (optional)
        :type workflow_inputs: array of dictionaries
        :param workflow_outputs: Workflow-level output specification (optional)
        :type workflow_outputs: array of dictionaries
        :param init_from: Another analysis workflow object handler or and analysis (string or handler) from which to initialize the metadata (optional)
        :type init_from: :class:`~dxpy.bindings.dxworkflow.DXWorkflow`, :class:`~dxpy.bindings.dxanalysis.DXAnalysis`, or string (for analysis IDs only)

        Create a new remote workflow object.
        """

        def _set_dx_hash(kwargs, dxhash, key, new_key=None):
            new_key = key if new_key is None else new_key
            if key in kwargs:
                if kwargs[key] is not None:
                    dxhash[new_key] = kwargs[key]
                del kwargs[key]

        if "init_from" in kwargs:
            if kwargs["init_from"] is not None:
                if not (isinstance(kwargs["init_from"], (DXWorkflow, DXAnalysis)) or \
                        (isinstance(kwargs["init_from"], basestring) and \
                         re.compile('^analysis-[0-9A-Za-z]{24}$').match(kwargs["init_from"]))):
                    raise DXError("Expected init_from to be an instance of DXWorkflow or DXAnalysis, or to be a string analysis ID.")
                if isinstance(kwargs["init_from"], basestring):
                    dx_hash["initializeFrom"] = {"id": kwargs["init_from"]}
                else:
                    dx_hash["initializeFrom"] = {"id": kwargs["init_from"].get_id()}
                    if isinstance(kwargs["init_from"], DXWorkflow):
                        dx_hash["initializeFrom"]["project"] = kwargs["init_from"].get_proj_id()
            del kwargs["init_from"]

        _set_dx_hash(kwargs, dx_hash, "title")
        _set_dx_hash(kwargs, dx_hash, "summary")
        _set_dx_hash(kwargs, dx_hash, "description")
        _set_dx_hash(kwargs, dx_hash, "output_folder", "outputFolder")
        _set_dx_hash(kwargs, dx_hash, "stages")
        _set_dx_hash(kwargs, dx_hash, "workflow_inputs", "inputs")
        _set_dx_hash(kwargs, dx_hash, "workflow_outputs", "outputs")

        resp = dxpy.api.workflow_new(dx_hash, **kwargs)
        self.set_ids(resp["id"], dx_hash["project"])

    def _add_edit_version_to_request(self, request_hash, edit_version=None):
        if edit_version is None:
            request_hash["editVersion"] = self.editVersion
        else:
            request_hash["editVersion"] = edit_version

    def _get_stage_id(self, stage):
        '''
        :param stage: A stage ID, name, or index (stage index is the number n for the nth stage, starting from 0; can be provided as an int or a string)
        :type stage: int or string
        :returns: The stage ID (this is a no-op if it was already a stage ID)
        :raises: :class:`~dxpy.exceptions.DXError` if *stage* could not be parsed, resolved to a stage ID, or it could not be found in the workflow
        '''
        # first, if it is a string, see if it is an integer
        if isinstance(stage, basestring):
            try:
                stage = int(stage)
            except:
                # we'll try parsing it as a string later
                pass

        if not isinstance(stage, basestring):
            # Try to parse as stage index; ensure that if it's not a
            # string that it is an integer at this point.
            try:
                stage_index = int(stage)
            except:
                raise DXError('DXWorkflow: the given stage identifier was neither a string stage ID nor an integer index')
            if stage_index < 0 or stage_index >= len(self.stages):
                raise DXError('DXWorkflow: the workflow contains ' + str(len(self.stages)) + \
                              ' stage(s), and the numerical value of the given stage identifier is out of range')
            return self.stages[stage_index].get("id")

        if re.compile('^([a-zA-Z_]|stage-)[0-9a-zA-Z_]*$').match(stage) is not None:
            # Check if there exists a stage with this stage id
            stage_id_exists = any([stg['id'] for stg in self.stages if stg.get('id') == stage])
            if stage_id_exists:
                return stage

        # A stage with the provided ID can't be found in the workflow, so look for it as a name
        stage_ids_matching_name = [stg['id'] for stg in self.stages if stg.get('name') == stage]
        if len(stage_ids_matching_name) == 0:
            raise DXError('DXWorkflow: the given stage identifier ' + stage + ' could not be found as a stage ID nor as a stage name')
        elif len(stage_ids_matching_name) > 1:
            raise DXError('DXWorkflow: more than one workflow stage was found to have the name "' + stage + '"')
        else:
            return stage_ids_matching_name[0]

    def add_stage(self, executable, stage_id=None, name=None, folder=None, stage_input=None, instance_type=None,
                  edit_version=None, **kwargs):
        '''
        :param executable: string or a handler for an app or applet
        :type executable: string, DXApplet, or DXApp
        :param stage_id: id for the stage (optional)
        :type stage_id: string
        :param name: name for the stage (optional)
        :type name: string
        :param folder: default output folder for the stage; either a relative or absolute path (optional)
        :type folder: string
        :param stage_input: input fields to bind as default inputs for the executable (optional)
        :type stage_input: dict
        :param instance_type: Default instance type on which all jobs will be run for this stage, or a dict mapping function names to instance type requests
        :type instance_type: string or dict
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
        if stage_id is not None:
            add_stage_input["id"] = stage_id
        if name is not None:
            add_stage_input["name"] = name
        if folder is not None:
            add_stage_input["folder"] = folder
        if stage_input is not None:
            add_stage_input["input"] = stage_input
        if instance_type is not None:
            add_stage_input["systemRequirements"] = SystemRequirementsDict.from_instance_type(instance_type).as_dict()
        self._add_edit_version_to_request(add_stage_input, edit_version)
        try:
            result = dxpy.api.workflow_add_stage(self._dxid, add_stage_input, **kwargs)
        finally:
            self.describe() # update cached describe
        return result['stage']

    def get_stage(self, stage, **kwargs):
        '''
        :param stage: A number for the stage index (for the nth stage, starting from 0), or a string of the stage index, name, or ID
        :type stage: int or string
        :returns: Hash of stage descriptor in workflow
        '''
        stage_id = self._get_stage_id(stage)
        result = next((stage for stage in self.stages if stage['id'] == stage_id), None)
        if result is None:
            raise DXError('The stage ID ' + stage_id + ' could not be found')
        return result

    def remove_stage(self, stage, edit_version=None, **kwargs):
        '''
        :param stage: A number for the stage index (for the nth stage, starting from 0), or a string of the stage index, name, or ID
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
        :param stage: A number for the stage index (for the nth stage, starting from 0), or a string of the stage index, name, or ID
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

    def update(self, title=None, unset_title=False, summary=None, description=None,
               output_folder=None, unset_output_folder=False,
               workflow_inputs=None, unset_workflow_inputs=False,
               workflow_outputs=None, unset_workflow_outputs=False,
               stages=None, edit_version=None, **kwargs):
        '''
        :param title: workflow title to set; cannot be provided with *unset_title* set to True
        :type title: string
        :param unset_title: whether to unset the title; cannot be provided with string value for *title*
        :type unset_title: boolean
        :param summary: workflow summary to set
        :type summary: string
        :param description: workflow description to set
        :type description: string
        :param output_folder: new default output folder for the workflow
        :type output_folder: string
        :param unset_folder: whether to unset the default output folder; cannot be True with string value for *output_folder*
        :type unset_folder: boolean
        :param stages: updates to the stages to make; see API documentation for /workflow-xxxx/update for syntax of this field; use :meth:`update_stage()` to update a single stage
        :type stages: dict
        :param workflow_inputs: updates to the workflow input to make; see API documentation for /workflow-xxxx/update for syntax of this field
        :type workflow_inputs: dict
        :param workflow_outputs: updates to the workflow output to make; see API documentation for /workflow-xxxx/update for syntax of this field
        :type workflow_outputs: dict
        :param edit_version: if provided, the edit version of the workflow that should be modified; if not provided, the current edit version will be used (optional)
        :type edit_version: int

        Make general metadata updates to the workflow
        '''
        update_input = {}
        if title is not None and unset_title:
            raise DXError('dxpy.DXWorkflow.update: cannot provide both "title" and set "unset_title"')
        if output_folder is not None and unset_output_folder:
            raise DXError('dxpy.DXWorkflow.update: cannot provide both "output_folder" and set "unset_output_folder"')
        if workflow_inputs is not None and unset_workflow_inputs:
            raise DXError('dxpy.DXWorkflow.update: cannot provide both "workflow_inputs" and set "unset_workflow_inputs"')
        if workflow_outputs is not None and unset_workflow_outputs:
            raise DXError('dxpy.DXWorkflow.update: cannot provide both "workflow_outputs" and set "unset_workflow_outputs"')

        if title is not None:
            update_input["title"] = title
        elif unset_title:
            update_input["title"] = None
        if summary is not None:
            update_input["summary"] = summary
        if description is not None:
            update_input["description"] = description
        if output_folder is not None:
            update_input["outputFolder"] = output_folder
        elif unset_output_folder:
            update_input["outputFolder"] = None
        if stages is not None:
            update_input["stages"] = stages
        if workflow_inputs is not None:
            update_input["inputs"] = workflow_inputs
        elif unset_workflow_inputs:
            update_input["inputs"] = None
        if workflow_outputs is not None:
            update_input["outputs"] = workflow_outputs
        elif unset_workflow_outputs:
            update_input["outputs"] = None

        # only perform update if there are changes to make
        if update_input:
            self._add_edit_version_to_request(update_input, edit_version)
            try:
                dxpy.api.workflow_update(self._dxid, update_input, **kwargs)
            finally:
                self.describe() # update cached describe

    def update_stage(self, stage, executable=None, force=False,
                     name=None, unset_name=False, folder=None, unset_folder=False, stage_input=None,
                     instance_type=None, edit_version=None, **kwargs):
        '''
        :param stage: A number for the stage index (for the nth stage, starting from 0), or a string stage index, name, or ID
        :type stage: int or string
        :param executable: string or a handler for an app or applet
        :type executable: string, DXApplet, or DXApp
        :param force: whether to use *executable* even if it is incompatible with the previous executable's spec
        :type force: boolean
        :param name: new name for the stage; cannot be provided with *unset_name* set to True
        :type name: string
        :param unset_name: whether to unset the stage name; cannot be True with string value for *name*
        :type unset_name: boolean
        :param folder: new default output folder for the stage; either a relative or absolute path (optional)
        :type folder: string
        :param unset_folder: whether to unset the stage folder; cannot be True with string value for *folder*
        :type unset_folder: boolean
        :param stage_input: input fields to bind as default inputs for the executable (optional)
        :type stage_input: dict
        :param instance_type: Default instance type on which all jobs will be run for this stage, or a dict mapping function names to instance type requests
        :type instance_type: string or dict
        :param edit_version: if provided, the edit version of the workflow that should be modified; if not provided, the current edit version will be used (optional)
        :type edit_version: int

        Removes the specified stage from the workflow
        '''
        stage_id = self._get_stage_id(stage)

        if name is not None and unset_name:
            raise DXError('dxpy.DXWorkflow.update_stage: cannot provide both "name" and set "unset_name"')
        if folder is not None and unset_folder:
            raise DXError('dxpy.DXWorkflow.update_stage: cannot provide both "folder" and set "unset_folder"')

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
        elif unset_name:
            update_stage_input["name"] = None
        if folder:
            update_stage_input["folder"] = folder
        elif unset_folder:
            update_stage_input["folder"] = None
        if stage_input:
            update_stage_input["input"] = stage_input
        if instance_type is not None:
            update_stage_input["systemRequirements"] = SystemRequirementsDict.from_instance_type(instance_type).as_dict()
        if update_stage_input:
            update_input = {"stages": {stage_id: update_stage_input}}
            self._add_edit_version_to_request(update_input, edit_version)
            try:
                dxpy.api.workflow_update(self._dxid, update_input, **kwargs)
            finally:
                self.describe() # update cached describe

    def is_locked(self):
        return self._desc.get('inputs') is not None and self._desc.get('state') == 'closed'

    def _get_input_name(self, input_str, region=None, describe_output=None):
        '''
        :param input_str: A string of one of the forms: "<exported input field name>", "<explicit workflow input field name>", "<stage ID>.<input field name>", "<stage index>.<input field name>", "<stage name>.<input field name>"
        :type input_str: string
        :returns: If the given form was one of those which uses the stage index or stage name, it is translated to the stage ID for use in the API call (stage name takes precedence)
        '''
        if '.' in input_str:
            stage_identifier, input_name = input_str.split('.', 1)
            # Try to parse as a stage ID or name
            return self._get_stage_id(stage_identifier) + '.' + input_name

        return input_str

    def _get_effective_input(self, workflow_input):
        effective_input = {}
        for key in workflow_input:
            input_name = self._get_input_name(key)
            if input_name in effective_input:
                raise DXError('DXWorkflow: the input for ' + input_name + ' was provided more than once')
            effective_input[input_name] = workflow_input[key]
        return effective_input

    def _get_run_input(self, workflow_input, **kwargs):
        effective_workflow_input = self._get_effective_input(workflow_input)

        run_input = DXExecutable._get_run_input_common_fields(effective_workflow_input, **kwargs)

        if kwargs.get('stage_instance_types') is not None:
            run_input['stageSystemRequirements'] = {}
            for stage, value in kwargs['stage_instance_types'].items():
                if stage != '*':
                    stage = self._get_stage_id(stage)
                run_input['stageSystemRequirements'][stage] = SystemRequirementsDict.from_instance_type(value).as_dict()

        if kwargs.get('stage_folders') is not None:
            run_input['stageFolders'] = {}
            for stage, value in kwargs['stage_folders'].items():
                if stage != '*':
                    stage = self._get_stage_id(stage)
                run_input['stageFolders'][stage] = value

        if kwargs.get('rerun_stages') is not None:
            run_input['rerunStages'] = [
                _stage if _stage == '*' else self._get_stage_id(_stage)
                for _stage in kwargs['rerun_stages']
            ]

        if kwargs.get('ignore_reuse', False):
            run_input['ignoreReuse'] = ['*']

        if kwargs.get('ignore_reuse_stages') is not None:
            run_input['ignoreReuse'] = [
                _stage if _stage == '*' else self._get_stage_id(_stage)
                for _stage in kwargs['ignore_reuse_stages']
            ]

        return run_input

    def _run_impl(self, run_input, **kwargs):
        return DXAnalysis(dxpy.api.workflow_run(self._dxid, run_input, **kwargs)["id"])

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
        :param ignore_reuse_stages: Stages of a workflow (IDs, names, or indices) or "*" for which job reuse should be disabled
        :type ignore_reuse_stages: list
        :returns: Object handler of the newly created analysis
        :rtype: :class:`~dxpy.bindings.dxanalysis.DXAnalysis`

        Run the associated workflow. See :meth:`dxpy.bindings.dxapplet.DXExecutable.run` for additional args.

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
        return super(DXWorkflow, self).run(workflow_input, *args, **kwargs)
