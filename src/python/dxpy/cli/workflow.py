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

'''
This submodule contains workflow-based commands for the dx
command-line client.
'''

import os

import dxpy
from .parsers import (process_dataobject_args, process_single_dataobject_output_args)
from ..utils.resolver import (resolve_existing_path, resolve_path)
from ..exceptions import (err_exit, DXError)
from . import try_call

def new_workflow(args):
    try_call(process_dataobject_args, args)
    try_call(process_single_dataobject_output_args, args)
    init_from = None
    if args.init is not None:
        try:
            init_project, init_folder, init_result = try_call(resolve_existing_path,
                                                              args.init,
                                                              expected='entity')
            init_from = dxpy.get_handler(init_result['id'], project=init_project)
        except:
            init_from = args.init
    if args.output is None:
        project = dxpy.WORKSPACE_ID
        folder = os.environ.get('DX_CLI_WD', '/')
        name = None
    else:
        project, folder, name = dxpy.utils.resolver.resolve_path(args.output)
    try:
        dxworkflow = dxpy.new_dxworkflow(title=args.title, summary=args.summary,
                                         description=args.description,
                                         project=project, name=name,
                                         tags=args.tags, types=args.types,
                                         hidden=args.hidden, properties=args.properties,
                                         details=args.details,
                                         folder=folder,
                                         parents=args.parents, init_from=init_from)
        if args.brief:
            print dxworkflow.get_id()
        else:
            dxpy.utils.describe.print_desc(dxworkflow.describe(incl_properties=True, incl_details=True),
                                           args.verbose)
    except:
        err_exit()

# Workflow-related functions

def add_stage(args):
    # get workflow
    project, folderpath, entity_result = try_call(resolve_existing_path, args.workflow, expected='entity')
    if entity_result is None or not entity_result['id'].startswith('workflow-'):
        err_exit(DXError('Could not resolve \"' + args.workflow + '\" to a workflow object'))

    # get executable
    exec_handler = dxpy.utils.resolver.get_exec_handler(args.executable, args.alias)
    exec_inputs = dxpy.cli.exec_io.ExecutableInputs(exec_handler)
    try_call(exec_inputs.update_from_args, args, require_all_inputs=False)

    # get folder path
    if args.folder is not None:
        ignore, folderpath, none = try_call(resolve_path, args.folder, expected='folder')
    else:
        folderpath = None

    dxworkflow = dxpy.DXWorkflow(entity_result['id'], project=project)
    stage_id = dxworkflow.add_stage(exec_handler, name=args.name, folder=folderpath,
                                    stage_input=exec_inputs.inputs)
    if args.brief:
        print stage_id
    else:
        dxpy.utils.describe.print_desc(dxworkflow.describe())

def remove_stage(args):
    # get workflow
    project, folderpath, entity_result = try_call(resolve_existing_path, args.workflow, expected='entity')
    if entity_result is None or not entity_result['id'].startswith('workflow-'):
        err_exit(DXError('Could not resolve \"' + args.workflow + '\" to a workflow object'))

    try:
        args.stage = int(args.stage)
    except:
        pass
    dxworkflow = dxpy.DXWorkflow(entity_result['id'], project=project)
    stage_id = try_call(dxworkflow.remove_stage, args.stage)
    if args.brief:
        print stage_id
    else:
        print "Removed stage " + stage_id
