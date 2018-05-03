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
