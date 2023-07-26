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

'''
This submodule contains workflow-based commands for the dx
command-line client.
'''

from __future__ import print_function, unicode_literals, division, absolute_import

import dxpy
import dxpy.utils.printing as printing
from .parsers import (process_dataobject_args, process_single_dataobject_output_args,
                      process_instance_type_arg)
from ..utils.describe import io_val_to_str
from ..utils.resolver import (resolve_existing_path, resolve_path, is_analysis_id)
from ..exceptions import (err_exit, DXCLIError, InvalidState)
from . import (try_call, try_call_err_exit)

def new_workflow(args):
    try_call(process_dataobject_args, args)
    try_call(process_single_dataobject_output_args, args)
    init_from = None
    if args.init is not None:
        if is_analysis_id(args.init):
            init_from = args.init
        else:
            init_project, _init_folder, init_result = try_call(resolve_existing_path,
                                                               args.init,
                                                               expected='entity')
            init_from = dxpy.get_handler(init_result['id'], project=init_project)
    if args.output is None:
        project = dxpy.WORKSPACE_ID
        folder = dxpy.config.get("DX_CLI_WD", "/")
        name = None
    else:
        project, folder, name = try_call(dxpy.utils.resolver.resolve_path, args.output)
    if args.output_folder is not None:
        try:
            # Try to resolve to a path in the project
            _ignore, args.output_folder, _ignore = resolve_path(args.output_folder, expected='folder')
        except:
            # But if not, just use the value directly
            pass

    try:
        dxworkflow = dxpy.new_dxworkflow(title=args.title, summary=args.summary,
                                         description=args.description,
                                         output_folder=args.output_folder,
                                         project=project, name=name,
                                         tags=args.tags, types=args.types,
                                         hidden=args.hidden, properties=args.properties,
                                         details=args.details, folder=folder,
                                         parents=args.parents, init_from=init_from)
        if args.brief:
            print(dxworkflow.get_id())
        else:
            dxpy.utils.describe.print_desc(dxworkflow.describe(incl_properties=True, incl_details=True),
                                           args.verbose)
    except:
        err_exit()

def get_workflow_id_and_project(path):
    '''
    :param path: a path or ID to a workflow object
    :type path: string
    :returns: tuple of (workflow ID, project ID)

    Returns the workflow and project IDs from the given path if
    available; otherwise, exits with an appropriate error message.
    '''
    project, _folderpath, entity_result = try_call(resolve_existing_path, path, expected='entity')
    try:
        if entity_result is None or not entity_result['id'].startswith('workflow-'):
            raise DXCLIError('Could not resolve "' + path + '" to a workflow object')
    except:
        err_exit()
    return entity_result['id'], project

def add_stage(args):
    # get workflow
    workflow_id, project = get_workflow_id_and_project(args.workflow)

    # get executable
    exec_handler = try_call(dxpy.utils.resolver.get_exec_handler,
                            args.executable,
                            args.alias)
    exec_inputs = dxpy.cli.exec_io.ExecutableInputs(exec_handler)
    try_call(exec_inputs.update_from_args, args, require_all_inputs=False)

    # get folder path
    folderpath = None
    if args.output_folder is not None:
        try:
            _ignore, folderpath, _none = resolve_path(args.output_folder, expected='folder')
        except:
            folderpath = args.output_folder
    elif args.relative_output_folder is not None:
        folderpath = args.relative_output_folder

    # process instance type
    try_call(process_instance_type_arg, args)

    dxworkflow = dxpy.DXWorkflow(workflow_id, project=project)
    stage_id = try_call(dxworkflow.add_stage,
                        exec_handler,
                        name=args.name,
                        stage_id=args.stage_id,
                        folder=folderpath,
                        stage_input=exec_inputs.inputs,
                        instance_type=args.instance_type)
    if args.brief:
        print(stage_id)
    else:
        dxpy.utils.describe.print_desc(dxworkflow.describe())

def list_stages(args):
    # get workflow
    workflow_id, project = get_workflow_id_and_project(args.workflow)

    dxworkflow = dxpy.DXWorkflow(workflow_id, project=project)
    desc = dxworkflow.describe()
    print((printing.BOLD() + printing.GREEN() + '{name}' + printing.ENDC() + ' ({id})').format(**desc))
    print()
    print('Title: ' + desc['title'])
    print('Output Folder: ' + (desc.get('outputFolder') if desc.get('outputFolder') is not None else '-'))
    if len(desc['stages']) == 0:
        print()
        print(' No stages; add stages with the command "dx add stage"')
    for i, stage in enumerate(desc['stages']):
        stage['i'] = i
        print()
        if stage['name'] is None:
            stage['name'] = '<no name>'
        print((printing.UNDERLINE() + 'Stage {i}' + printing.ENDC() + ': {name} ({id})').format(**stage))
        print('Executable      {executable}'.format(**stage) + \
            (" (" + printing.RED() + "inaccessible" + printing.ENDC() + ")" \
             if stage.get('accessible') is False else ""))
        if stage['folder'] is not None and stage['folder'].startswith('/'):
            stage_output_folder = stage['folder']
        else:
            stage_output_folder = '<workflow output folder>/' + (stage['folder'] if stage['folder'] is not None else "")
        print('Output Folder   {folder}'.format(folder=stage_output_folder))
        if "input" in stage and stage["input"]:
            print('Bound input     ' + \
                ('\n' + ' '*16).join([
                    '{key}={value}'.format(key=key, value=io_val_to_str(stage["input"][key])) for
                    key in stage['input']
                ]))

def remove_stage(args):
    # get workflow
    workflow_id, project = get_workflow_id_and_project(args.workflow)

    try:
        args.stage = int(args.stage)
    except:
        pass
    dxworkflow = dxpy.DXWorkflow(workflow_id, project=project)
    stage_id = try_call(dxworkflow.remove_stage, args.stage)
    if args.brief:
        print(stage_id)
    else:
        print("Removed stage " + stage_id)

def update_workflow(args):
    # get workflow
    workflow_id, project = get_workflow_id_and_project(args.workflow)

    if not any([args.title, args.no_title, args.summary, args.description, args.output_folder,
                args.no_output_folder]):
        print('No updates requested; none made')
        return

    if args.output_folder is not None:
        try:
            # Try to resolve to an existing path in the project
            _ignore, args.output_folder, _ignore = resolve_path(args.output_folder, expected='folder')
        except:
            # But if not, just use the value directly
            pass

    dxworkflow = dxpy.DXWorkflow(workflow_id, project=project)
    try_call(dxworkflow.update,
             title=args.title,
             unset_title=args.no_title,
             summary=args.summary,
             description=args.description,
             output_folder=args.output_folder,
             unset_output_folder=args.no_output_folder)

def update_stage(args):
    # get workflow
    workflow_id, project = get_workflow_id_and_project(args.workflow)
    dxworkflow = dxpy.DXWorkflow(workflow_id, project=project)

    # process instance type
    try_call(process_instance_type_arg, args)

    initial_edit_version = dxworkflow.editVersion

    try:
        args.stage = int(args.stage)
    except:
        pass

    if not any([args.executable, args.name, args.no_name, args.output_folder,
                args.relative_output_folder, args.input, args.input_json, args.filename,
                args.instance_type]):
        print('No updates requested; none made')
        return

    new_exec_handler = None
    if args.executable is not None:
        # get executable
        new_exec_handler = try_call(dxpy.utils.resolver.get_exec_handler,
                                    args.executable,
                                    args.alias)
        exec_inputs = dxpy.cli.exec_io.ExecutableInputs(new_exec_handler)
        try_call(exec_inputs.update_from_args, args, require_all_inputs=False)
        stage_input = exec_inputs.inputs
    elif args.input or args.input_json or args.filename:
        # input is updated, so look up the existing one
        existing_exec_handler = dxpy.utils.resolver.get_exec_handler(dxworkflow.get_stage(args.stage)['executable'])
        exec_inputs = dxpy.cli.exec_io.ExecutableInputs(existing_exec_handler)
        try_call(exec_inputs.update_from_args, args, require_all_inputs=False)
        stage_input = exec_inputs.inputs
    else:
        stage_input = None

    # get folder path
    folderpath = None
    if args.output_folder is not None:
        try:
            _ignore, folderpath, _none = resolve_path(args.output_folder, expected='folder')
        except:
            folderpath = args.output_folder
    elif args.relative_output_folder is not None:
        folderpath = args.relative_output_folder

    try:
        dxworkflow.update_stage(args.stage,
                                executable=new_exec_handler,
                                force=args.force,
                                name=args.name,
                                unset_name=args.no_name,
                                folder=folderpath,
                                stage_input=stage_input,
                                instance_type=args.instance_type,
                                edit_version=initial_edit_version)
    except InvalidState as e:
        if "compatible" in str(e):
            err_msg = 'The requested executable could not be verified as a compatible replacement'
            if 'incompatibilities' in e.details and e.details['incompatibilities']:
                err_msg += ' for the following reasons:\n'
                err_msg += '\n'.join([printing.fill(incompat,
                                                    initial_indent='- ',
                                                    subsequent_indent='  ')
                                      for incompat in e.details['incompatibilities']])
            else:
                err_msg += '.'
            err_msg += '\nRerun with --force to replace the executable anyway'
            err_exit(expected_exceptions=DXCLIError, exception=DXCLIError(err_msg))
        else:
            try_call_err_exit()
    except:
        try_call_err_exit()
