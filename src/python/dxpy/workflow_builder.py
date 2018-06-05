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
Workflow Builder Library
+++++++++++++++++++

Contains utility methods useful for deploying workflows onto the platform.
"""

from __future__ import print_function, unicode_literals, division, absolute_import
import os
import sys
import json
import copy

import dxpy
from .cli import INTERACTIVE_CLI
from .utils.printing import fill
from .compat import input
from .utils import json_load_raise_on_duplicates
from .exceptions import err_exit
from . import logger

UPDATABLE_GLOBALWF_FIELDS = {'title', 'summary', 'description', 'developerNotes', 'details'}
GLOBALWF_SUPPORTED_KEYS = {"name", "version", "title", "summary", "description",
                           "developerNotes", "regionalOptions", "categories", "billTo",
                           "dxapi", "details"}
SUPPORTED_KEYS = GLOBALWF_SUPPORTED_KEYS.union({"project", "folder", "outputFolder", "stages",
                                                "inputs", "outputs"})

class WorkflowBuilderException(Exception):
    """
    This exception is raised by the methods in this module when workflow
    building fails.
    """
    pass


def _parse_executable_spec(src_dir, json_file_name, parser):
    """
    Returns the parsed contents of a json specification.
    Raises WorkflowBuilderException (exit code 3) if this cannot be done.
    """
    if not os.path.isdir(src_dir):
        parser.error("{} is not a directory".format(src_dir))

    if not os.path.exists(os.path.join(src_dir, json_file_name)):
        raise WorkflowBuilderException(
            "Directory {} does not contain dxworkflow.json: not a valid DNAnexus workflow source directory"
            .format(src_dir))

    with open(os.path.join(src_dir, json_file_name)) as desc:
        try:
            return json_load_raise_on_duplicates(desc)
        except Exception as e:
            raise WorkflowBuilderException("Could not parse {} file as JSON: {}".format(json_file_name, e.message))


def _get_destination_project(json_spec, args, build_project_id=None):
    """
    Returns destination project in which the workflow should be created.
    In can be set in multiple ways whose order of precedence is:
    1. --destination, -d option supplied with `dx build`,
    2. 'project' specified in the json file,
    3. project set in the dxpy.WORKSPACE_ID environment variable.
    """
    if build_project_id:
        return build_project_id
    if 'project' in json_spec:
        return json_spec['project']
    if dxpy.WORKSPACE_ID:
        return dxpy.WORKSPACE_ID
    error_msg = "Can't create a workflow without specifying a destination project; "
    error_msg += "please use the -d/--destination flag to explicitly specify a project"
    raise WorkflowBuilderException(error_msg)


def _get_destination_folder(json_spec, folder_name=None):
    """
    Returns destination project in which the workflow should be created.
    It can be set in the json specification or by --destination option supplied
    with `dx build`.
    The order of precedence is:
    1. --destination, -d option,
    2. 'folder' specified in the json file.
    """
    dest_folder = folder_name or json_spec.get('folder') or '/'
    if not dest_folder.endswith('/'):
        dest_folder = dest_folder + '/'
    return dest_folder


def _get_workflow_name(json_spec, workflow_name=None):
    """
    Returns the name of the workflow to be created. It can be set in the json
    specification or by --destination option supplied with `dx build`.
    The order of precedence is:
    1. --destination, -d option,
    2. 'name' specified in the json file.
    If not provided, returns empty string.
    """
    return workflow_name or json_spec.get('name')


def _get_unsupported_keys(keys, supported_keys):
    return [key for key in keys if key not in supported_keys]


def _set_categories_on_workflow(global_workflow_id, categories_to_set):
    """
    Note: Categories are set on the workflow series level,
    i.e. the same set applies to all versions.
    """
    assert(isinstance(categories_to_set, list))

    existing_categories = dxpy.api.global_workflow_list_categories(global_workflow_id)['categories']
    categories_to_add = set(categories_to_set).difference(set(existing_categories))
    categories_to_remove = set(existing_categories).difference(set(categories_to_set))
    if categories_to_add:
        dxpy.api.global_workflow_add_categories(global_workflow_id,
                                                input_params={'categories': list(categories_to_add)})
    if categories_to_remove:
        dxpy.api.global_workflow_remove_categories(global_workflow_id,
                                                   input_params={'categories': list(categories_to_remove)})


def _version_exists(json_spec, name=None, version=None):
    """
    Returns True if a global workflow with the given name and version
    already exists in the platform and the user has developer rights
    to the workflow. "name" and "version" can be passed if we already
    made a "describe" API call on the global workflow and so know the
    requested name and version already exists.
    """
    requested_name = json_spec['name']
    requested_version = json_spec['version']

    if requested_name == name and requested_version == version:
        return True
    else:
        try:
            desc_output = dxpy.api.global_workflow_describe('globalworkflow-' + json_spec['name'],
                                                            alias=json_spec['version'],
                                                            input_params={"fields": {"name": True,
                                                                                     "version": True}})
            return desc_output['name'] == json_spec['name'] and desc_output['version'] == json_spec['version']
        except dxpy.exceptions.DXAPIError:
            return False
        except:
            raise


def _get_validated_stage(stage, stage_index):
    # required keys
    if 'executable' not in stage:
        raise WorkflowBuilderException(
            "executable is not specified for stage with index {}".format(stage_index))

    # print ignored keys if present in json_spec
    supported_keys = {"id", "input", "executable", "name", "folder",
                      "executionPolicy", "systemRequirements"}
    unsupported_keys = _get_unsupported_keys(stage.keys(), supported_keys)
    if len(unsupported_keys) > 0:
        print("Warning: the following stage fields are not supported and will be ignored: {}"
              .format(", ".join(unsupported_keys)))

    return stage


def _get_validated_stages(stages):
    """
    Validates stages of the workflow as a list of dictionaries.
    """
    if not isinstance(stages, list):
        raise WorkflowBuilderException("Stages must be specified as a list of dictionaries")
    validated_stages = []
    for index, stage in enumerate(stages):
        validated_stages.append(_get_validated_stage(stage, index))
    return validated_stages


def _validate_json_for_regular_workflow(json_spec, args):
    """
    Validates fields used only for building a regular, project-based workflow.
    """
    validated = {}
    override_project_id, override_folder, override_workflow_name = \
        dxpy.executable_builder.get_parsed_destination(args.destination)
    validated['project'] = _get_destination_project(json_spec, args, override_project_id)
    validated['folder'] = _get_destination_folder(json_spec, override_folder)

    workflow_name = _get_workflow_name(json_spec, override_workflow_name)
    if not workflow_name:
        print('Warning: workflow name is not specified')
    else:
        validated['name'] = workflow_name
    return validated


def _validate_json_for_global_workflow(json_spec, args):
    """
    Validates fields used for building a global workflow.
    Since building a global workflow is done after all the underlying workflows
    are built, which may be time-consuming, we validate as much as possible here.
    """
    # TODO: verify the billTo can build the workflow
    # TODO: if the global workflow build fails add an option to interactively change billto
    # TODO: (or other simple fields) instead of failing altogether
    # TODO: get a confirmation before building a workflow that may be costly
    if 'name' not in json_spec:
        raise WorkflowBuilderException(
            "dxworkflow.json contains no 'name' field, but it is required to build a global workflow")
    if not dxpy.executable_builder.GLOBAL_EXEC_NAME_RE.match(json_spec['name']):
        raise WorkflowBuilderException(
            "The name of your workflow must match /^[a-zA-Z0-9._-]+$/")
    if json_spec['name'] != json_spec['name'].lower():
        logger.warn('workflow name "{}" should be all lowercase'.format(json_spec['name']))

    if 'version' not in json_spec:
        raise WorkflowBuilderException(
            "dxworkflow.json contains no 'version' field, but it is required to build a global workflow")
    if not dxpy.executable_builder.GLOBAL_EXEC_VERSION_RE.match(json_spec['version']):
        logger.warn('"version" {} should be semver compliant (e.g. of the form X.Y.Z)'.format(json_spec['version']))

    if 'details' in json_spec:
        if not isinstance(json_spec['details'], dict):
            raise WorkflowBuilderException(
                'The field "details" must be a dictionary')

    if args.bill_to:
        json_spec["billTo"] = args.bill_to


def _get_validated_json(json_spec, args):
    """
    Validates dxworkflow.json and returns the json that can be sent with the
    /workflow/new API or /globalworkflow/new request.
    """
    if not json_spec:
        return
    if not args:
        return

    validated_spec = copy.deepcopy(json_spec)

    # print ignored keys if present in json_spec
    unsupported_keys = _get_unsupported_keys(validated_spec.keys(), SUPPORTED_KEYS)
    if len(unsupported_keys) > 0:
        logger.warn(
            "Warning: the following root level fields are not supported and will be ignored: {}"
                .format(", ".join(unsupported_keys)))


    if 'stages' in validated_spec:
        validated_spec['stages'] = _get_validated_stages(validated_spec['stages'])

    if 'name' in validated_spec:
        if args.src_dir != validated_spec['name']:
            logger.warn(
                'workflow name "%s" does not match containing directory "%s"' % (validated_spec['name'], args.src_dir))

    validated_documentation_fields = _get_validated_json_for_build_or_update(validated_spec, args)
    validated_spec.update(validated_documentation_fields)

    # Project-based workflow specific validation
    if args.mode == 'workflow':
        validated = _validate_json_for_regular_workflow(json_spec, args)
        validated_spec.update(validated)

    # Global workflow specific validation
    if args.mode == 'globalworkflow':
        _validate_json_for_global_workflow(validated_spec, args)

    return validated_spec


def _get_validated_json_for_build_or_update(json_spec, args):
    """
    Validates those fields that can be used when either building 
    a new version (of a local, project-based workflow) or updating
    an existing version (of a global workflow).
    """
    validated = copy.deepcopy(json_spec)

    dxpy.executable_builder.inline_documentation_files(validated, args.src_dir)

    if 'title' not in json_spec:
        logger.warn("dxworkflow.json is missing a title, please add one in the 'title' field")

    if 'summary' not in json_spec:
        logger.warn("dxworkflow.json is missing a summary, please add one in the 'summary' field")
    else:
        if json_spec['summary'].endswith('.'):
            logger.warn("summary {} should be a short phrase not ending in a period".format(json_spec['summary'],))

    return validated


def _build_regular_workflow(json_spec):
    """
    Precondition: json_spec must be validated
    """
    workflow_id = dxpy.api.workflow_new(json_spec)["id"]
    dxpy.api.workflow_close(workflow_id)
    return workflow_id


def _build_underlying_workflows(json_spec, args):
    """
    Creates a workflow in a temporary project for each enabled region.
    Returns a tuple of dictionaries: workflow IDs by region and project IDs by region.
    The caller is responsible for destroying the projects if this method returns properly.
    """
    # TODO: Initially a global workflow can be enabled in one region only, the region of the
    # underlying workflow (the region of the current project). It will be expanded in
    # the future to build in all regions.

    projects_by_region, workflows_by_region = {}, {}  # IDs by region

    # Create a temp project
    try:
        project_input = {"name": "Temporary build project for dx build global workflow"}
        if args.bill_to:
            project_input["billTo"] = args.bill_to
        working_project = dxpy.api.project_new(project_input)["id"]
    except:
        err_exit()
    region = dxpy.api.project_describe(working_project,
                                       input_params={"fields": {"region": True}})["region"]
    projects_by_region[region] = working_project
    logger.debug("Created temporary project {} to build in".format(working_project))

    # Create a project-based workflow in each temporary project
    try:
        for region, project in projects_by_region.items():
            json_spec['project'] = project
            workflow_id = _build_regular_workflow(json_spec)
            logger.debug("Created workflow " + workflow_id + " successfully")
            workflows_by_region[region] = workflow_id
    except:
        # Clean up
        if projects_by_region:
            dxpy.executable_builder.delete_temporary_projects(projects_by_region.values())
        raise

    return workflows_by_region, projects_by_region


def _build_global_workflow(json_spec, args):
    """
    Creates a workflow in a temporary project for each enabled region
    and builds a global workflow on the platform based on these workflows.
    """

    workflows_by_region, projects_by_region = {}, {}  # IDs by region
    try:
        # prepare "regionalOptions" field for the globalworkflow/new input
        workflows_by_region, projects_by_region = \
            _build_underlying_workflows(json_spec, args)
        regional_options = {}
        for region, workflow_id in workflows_by_region.items():
            regional_options[region] = {'workflow': workflow_id}
        json_spec.update({'regionalOptions': regional_options})

        # leave only fields that are actually used to build the workflow
        gwf_provided_keys = GLOBALWF_SUPPORTED_KEYS.intersection(set(json_spec.keys()))
        gwf_final_json = dict((k, v) for k, v in json_spec.items() if k in gwf_provided_keys)

        # we don't want to print the whole documentation to the screen so we'll remove these fields
        print_spec = copy.deepcopy(gwf_final_json)
        if "description" in gwf_final_json:
            del print_spec["description"]
        if "developerNotes" in gwf_final_json:
            del print_spec["developerNotes"]
        logger.info("Will create global workflow with spec: {}".format(json.dumps(print_spec)))

        # Create a new global workflow version on the platform
        global_workflow_id = dxpy.api.global_workflow_new(gwf_final_json)["id"]

        logger.info("Uploaded global workflow {n}/{v} successfully".format(n=gwf_final_json["name"],
                                                                           v=gwf_final_json["version"]))
        logger.info("You can publish this workflow with:")
        logger.info("  dx publish {n}/{v}".format(n=gwf_final_json["name"],
                                                  v=gwf_final_json["version"]))
    finally:
        # Clean up
        if projects_by_region:
            dxpy.executable_builder.delete_temporary_projects(projects_by_region.values())

    # Set any additional fields on the created workflow
    try:
        _set_categories_on_workflow(global_workflow_id, gwf_final_json.get("categories", []))
    except:
        logger.warn(
            "The workflow {n}/{v} was created but setting categories failed".format(n=gwf_final_json['name'],
                                                                                    v=gwf_final_json['version']))
        raise

    return global_workflow_id


def _update_global_workflow(json_spec, args, global_workflow_id):

    def skip_update():
        skip_update = False
        if non_empty_fields:
            update_message = "The global workflow {}/{} exists so we will update the following fields for this version: {}.".format(
                json_spec["name"], json_spec["version"], ", ".join(non_empty_fields))

            if args.confirm:
                if INTERACTIVE_CLI:
                    try:
                        print('***')
                        print(fill('INFO: ' + update_message))
                        print('***')
                        value = input('Confirm making these updates [y/N]: ')
                    except KeyboardInterrupt:
                        value = 'n'
                    if not value.lower().startswith('y'):
                        skip_update = True
                else:
                    # Default to NOT updating if operating without a TTY.
                    logger.warn(
                        'skipping requested change to update a global workflow version. Rerun "dx build" interactively or pass --yes to confirm this change.')
                    skip_update = True
            else:
                logger.info(update_message)
        else:
            skip_update = True
            logger.info("Nothing to update")
        return skip_update

    update_spec = dict((k, v) for k, v in json_spec.items() if k in UPDATABLE_GLOBALWF_FIELDS)
    validated_spec = _get_validated_json_for_build_or_update(update_spec, args)
    non_empty_fields = dict((k, v) for k, v in validated_spec.items() if v)

    if not skip_update():
        global_workflow_id = dxpy.api.global_workflow_update('globalworkflow-' + json_spec['name'],
                                                             alias=json_spec['version'],
                                                             input_params=non_empty_fields)['id']
    else:
        logger.info("Skipping making updates")
    return global_workflow_id


def _build_or_update_workflow(json_spec, args):
    """
    Creates or updates a workflow on the platform.
    Returns the workflow ID, or None if the workflow cannot be created.
    """
    try:
        if args.mode == 'workflow':
            json_spec = _get_validated_json(json_spec, args)
            workflow_id = _build_regular_workflow(json_spec)
        elif args.mode == 'globalworkflow':
            # Verify if the global workflow already exists and if the user has developer rights to it
            # If the global workflow name doesn't exist, the user is free to build it
            # If the name does exist two things can be done:
            # * either update the requested version, if this version already exists
            # * or create the version if it doesn't exist
            existing_workflow = dxpy.executable_builder.verify_developer_rights('globalworkflow-' + json_spec['name'])
            if existing_workflow and _version_exists(json_spec,
                                                     existing_workflow.name,
                                                     existing_workflow.version):
                workflow_id = _update_global_workflow(json_spec, args, existing_workflow.id)
            else:
                json_spec = _get_validated_json(json_spec, args)
                workflow_id = _build_global_workflow(json_spec, args)
        else:
            raise WorkflowBuilderException("Unrecognized workflow type: {}".format(args.mode))
    except dxpy.exceptions.DXAPIError as e:
        raise e
    return workflow_id


def _print_output(workflow_id, args):
    if args.json and args.mode == 'workflow':
        output = dxpy.api.workflow_describe(workflow_id)
    elif args.json and args.mode == 'globalworkflow':
        output = dxpy.api.global_workflow_describe(workflow_id)
    else:
        output = {'id': workflow_id}
    if output is not None:
        print(json.dumps(output))


def build(args, parser):
    """
    Validates workflow source directory and creates a new (global) workflow based on it.
    Raises: WorkflowBuilderException if the workflow cannot be created.
    """

    if args is None:
        raise Exception("arguments not provided")

    try:
        json_spec = _parse_executable_spec(args.src_dir, "dxworkflow.json", parser)
        workflow_id = _build_or_update_workflow(json_spec, args)
        _print_output(workflow_id, args)
    except WorkflowBuilderException as e:
        print("Error: %s" % (e.message,), file=sys.stderr)
        sys.exit(3)
