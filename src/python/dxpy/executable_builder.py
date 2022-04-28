#!/usr/bin/env python
#
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
Executable Builder
+++++++++++++++++++

Contains utility methods useful for deploying executables (apps, applets, workflows)
onto the platform.

'''

from __future__ import print_function, unicode_literals, division, absolute_import
import os
import re
import collections

from .utils.resolver import resolve_path, is_container_id
from .cli import try_call
import dxpy

GLOBAL_EXEC_NAME_RE = re.compile("^[a-zA-Z0-9._\-]+$")
GLOBAL_EXEC_VERSION_RE = re.compile("^([1-9][0-9]*|0)\.([1-9][0-9]*|0)\.([1-9][0-9]*|0)(-[-0-9A-Za-z]+(\.[-0-9A-Za-z]+)*)?(\+[-0-9A-Za-z]+(\.[-0-9A-Za-z]+)*)?$")

def get_parsed_destination(dest_str):
    """
    Parses dest_str, which is (roughly) of the form
    PROJECT:/FOLDER/NAME, and returns a tuple (project, folder, name)
    """
    # Interpret strings of form "project-XXXX" (no colon) as project. If
    # we pass these through to resolve_path they would get interpreted
    # as folder names...
    if is_container_id(dest_str):
        return (dest_str, None, None)

    # ...otherwise, defer to resolver.resolve_path. This handles the
    # following forms:
    #
    # /FOLDER/
    # /ENTITYNAME
    # /FOLDER/ENTITYNAME
    # [PROJECT]:
    # [PROJECT]:/FOLDER/
    # [PROJECT]:/ENTITYNAME
    # [PROJECT]:/FOLDER/ENTITYNAME
    return try_call(resolve_path, dest_str)


def inline_documentation_files(json_spec, src_dir):
    """
    Modifies the provided json_spec dict (which may be an app, applet,
    workflow spec) to inline the contents of the readme file into
    "description" and the developer readme into "developerNotes".
    """
    # Inline description from a readme file
    if 'description' not in json_spec:
        readme_filename = None
        for filename in 'README.md', 'Readme.md', 'readme.md':
            if os.path.exists(os.path.join(src_dir, filename)):
                readme_filename = filename
                break
        if readme_filename is not None:
            with open(os.path.join(src_dir, readme_filename)) as fh:
                json_spec['description'] = fh.read()

    # Inline developerNotes from Readme.developer.md
    if 'developerNotes' not in json_spec:
        for filename in 'README.developer.md', 'Readme.developer.md', 'readme.developer.md':
            if os.path.exists(os.path.join(src_dir, filename)):
                with open(os.path.join(src_dir, filename)) as fh:
                    json_spec['developerNotes'] = fh.read()
                break


def delete_temporary_projects(projects):
    """
    Destroys all projects from the list.
    """
    for project in projects:
        try:
            dxpy.api.project_destroy(project)
        except Exception:
            pass

def get_valid_bill_to(bill_to, executable_builder_exception):
    """
    Check if the requesting user can perform billable activities on behalf of the billTo
    If not specified, default to the billTo of the requesting user
    otherwise it must be either the ID of the requesting user, 
    or an org of which the requesting user is a member with 'allowBillableActivities' permission
    """
    user_id = dxpy.whoami()
    if not bill_to:
        return dxpy.api.user_describe(user_id)['billTo']

    exception_msg = None
    if bill_to.startswith('user-') and bill_to != user_id:
        exception_msg = 'Cannot request another user to be the "billTo"'
    elif bill_to.startswith('org-'):
        try:
            member_access = dxpy.api.org_describe(bill_to)
            if not member_access['allowBillableActivities']:
                exception_msg='You are not a member in {} with allowBillableActivities permission. Please check the billing policy of the org.'.format(bill_to)
        except:
            exception_msg='Cannot retrieve billing information for {}. Please check your access level and the billing policy of the org.'.format(bill_to)
    else:
        exception_msg='The field "billTo" must be a valid ID of a user/org.'
    
    if exception_msg:
        raise executable_builder_exception(exception_msg)
    
    return bill_to

def verify_developer_rights(prefixed_name):
    """
    Checks if the current user is a developer of the app or global workflow
    with the given name. If the app/global workflow exists and the user has
    developer rights to it, the function returns a named tuple representing
    the executable that was queried.
    """
    assert(prefixed_name.startswith('app-') or prefixed_name.startswith('globalworkflow-'))

    if prefixed_name.partition('-')[0] == 'app':
        exception_type = dxpy.app_builder.AppBuilderException
        describe_method = dxpy.api.app_describe
        exception_msg = \
            'An app with the given name already exists and you are not a developer of that app'
    else:
        exception_type = dxpy.workflow_builder.WorkflowBuilderException
        describe_method = dxpy.api.global_workflow_describe
        exception_msg = \
            'A global workflow with the given name already exists and you are not a developer of that workflow'
    
    name_already_exists = True
    is_developer = False
    version = None
    executable_id = None
    FoundExecutable = collections.namedtuple('FoundExecutable', ['name', 'version', 'id'])
    try:
        describe_output = describe_method(prefixed_name,
                                          input_params={"fields": {"isDeveloperFor": True,
                                                                   "version": True,
                                                                   "id": True}})
        is_developer = describe_output['isDeveloperFor']
        version = describe_output['version']
        executable_id = describe_output['id']
    except dxpy.exceptions.DXAPIError as e:
        if e.name == 'ResourceNotFound':
            name_already_exists = False
        elif e.name == 'PermissionDenied':
            raise exception_type(exception_msg)
        else:
            raise e

    if not name_already_exists:
        # This app/workflow doesn't exist yet so its creation will succeed
        # (or at least, not fail on the basis of the ACL).
        return FoundExecutable(name=None, version=None, id=None)

    name_without_prefix = prefixed_name.partition('-')[2]
    if not is_developer:
        raise exception_type('You are not a developer for {n}'.format(n=name_without_prefix))

    return FoundExecutable(name=name_without_prefix, version=version, id=executable_id)


def assert_consistent_regions(from_spec, from_command_line, executable_builder_exception):
    """
    Verifies the regions passed with --region CLI argument and the ones
    specified in regionalOptions are the same (if both CLI and spec were used)
    """
    if from_spec is None or from_command_line is None:
        return
    if set(from_spec) != set(from_command_line):
        raise executable_builder_exception("--region and the 'regionalOptions' key in the JSON file do not agree")


def assert_consistent_reg_options(exec_type, json_spec, executable_builder_exception):
    """
    Validates the "regionalOptions" field and verifies all the regions used
    in "regionalOptions" have the same options.
    """
    reg_options_spec = json_spec.get('regionalOptions')
    json_fn = 'dxapp.json' if exec_type == 'app' else 'dxworkflow.json'

    if not isinstance(reg_options_spec, dict):
        raise executable_builder_exception("The field 'regionalOptions' in  must be a mapping")
    if not reg_options_spec:
        raise executable_builder_exception(
            "The field 'regionalOptions' in " + json_fn + " must be a non-empty mapping")
    regional_options_list = list(reg_options_spec.items())
    for region, opts_for_region in regional_options_list:
        if not isinstance(opts_for_region, dict):
            raise executable_builder_exception("The field 'regionalOptions['" + region +
                            "']' in " + json_fn + " must be a mapping")
        if set(opts_for_region.keys()) != set(regional_options_list[0][1].keys()):
            if set(opts_for_region.keys()) - set(regional_options_list[0][1].keys()):
                with_key, without_key = region, regional_options_list[0][0]
                key_name = next(iter(set(opts_for_region.keys()) - set(regional_options_list[0][1].keys())))
            else:
                with_key, without_key = regional_options_list[0][0], region
                key_name = next(iter(set(regional_options_list[0][1].keys()) - set(opts_for_region.keys())))
            raise executable_builder_exception(
                "All regions in regionalOptions must specify the same options; " +
                "{} was given for {} but not for {}" .format (key_name, with_key, without_key)
            )

        if exec_type == 'app':
            for key in opts_for_region:
                if key in json_spec.get('runSpec', {}):
                    raise executable_builder_exception(
                    key + " cannot be given in both runSpec and in regional options for " + region)

def get_permitted_regions(bill_to, executable_builder_exception):
    """
    Validates requested bill_to and returns the set of its permitted regions.
    """
    billable_regions = set()
    try:
        billable_regions= set(dxpy.DXHTTPRequest('/' + bill_to + '/describe', {}).get("permittedRegions"))
    except:
        raise executable_builder_exception("Failed to get permitted regions of {}".format(bill_to))
    return billable_regions

def get_enabled_regions(exec_type, json_spec, from_command_line, executable_builder_exception):
    """
    Return a list of regions in which the global executable (app or global workflow)
    will be enabled, based on the "regionalOption" in their JSON specification
    and/or --region CLI argument used with "dx build".

    :param exec_type: 'app' or 'globalworkflow'
    :type json_spec: str.
    :param json_spec: The contents of dxapp.json or dxworkflow.json
    :type json_spec: dict or None.
    :param from_command_line: The regional options specified on the command-line via --region.
    :type from_command_line: list or None.
    :param executable_builder_exception: Exception that will be thrown.
    :type executable_builder_exception: AppBuilderException or WorkflowBuilderException.
    """

    from_spec = json_spec.get('regionalOptions')

    if from_spec is not None:
        assert_consistent_reg_options(exec_type, json_spec, executable_builder_exception)

    assert_consistent_regions(from_spec, from_command_line, executable_builder_exception)

    enabled_regions = None
    if from_spec is not None:
        enabled_regions = from_spec.keys()
    elif from_command_line is not None:
        enabled_regions = from_command_line

    return enabled_regions
