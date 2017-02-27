# Copyright (C) 2014-2016 DNAnexus, Inc.
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
This module handles download commands for the dx command-line client.
'''
from __future__ import print_function, unicode_literals, division, absolute_import

import os
import sys
import collections
import dxpy
from ..utils.resolver import (resolve_existing_path, get_first_pos_of_char, is_project_explicit,
                              object_exists_in_project, is_jbor_str)
from ..exceptions import err_exit
from . import try_call
from dxpy.utils.printing import (fill)
from dxpy.utils import pathmatch


def download_one_file(project, file_desc, dest_filename, args):
    if not args.overwrite:
        if os.path.exists(dest_filename):
            err_exit(fill('Error: path "' + dest_filename + '" already exists but -f/--overwrite was not set'))

    if file_desc['class'] != 'file':
        print("Skipping non-file data object {name} ({id})".format(**file_desc), file=sys.stderr)
        return

    if file_desc['state'] != 'closed':
        print("Skipping file {name} ({id}) because it is not closed".format(**file_desc), file=sys.stderr)
        return

    try:
        show_progress = args.show_progress
    except AttributeError:
        show_progress = False

    try:
        dxpy.download_dxfile(file_desc['id'], dest_filename, show_progress=show_progress, project=project)
    except:
        err_exit()


def _ensure_local_dir(d):
    if not os.path.isdir(d):
        if os.path.exists(d):
            err_exit(fill('Error: path "' + d + '" already exists and is not a directory'))
        os.makedirs(d)


def _is_glob(path):
    return get_first_pos_of_char('*', path) > -1 or get_first_pos_of_char('?', path) > -1


def _rel2abs(path, project):
    if path.startswith('/') or dxpy.WORKSPACE_ID != project:
        abs_path, strip_prefix = path, os.path.dirname(path.rstrip('/'))
    else:
        wd = dxpy.config.get('DX_CLI_WD', u'/')
        abs_path, strip_prefix = os.path.join(wd, path), os.path.dirname(os.path.join(wd, path).rstrip('/'))
    if len(abs_path) > 1:
        abs_path = abs_path.rstrip('/')
    return abs_path, strip_prefix


def _download_files(files, destdir, args, dest_filename=None):
    for project in files:
        for f in files[project]:
            file_desc = f['describe']
            dest = dest_filename or os.path.join(destdir, file_desc['name'].replace('/', '%2F'))
            download_one_file(project, file_desc, dest, args)


def _download_folders(folders, destdir, args):
    try:
        show_progress = args.show_progress
    except AttributeError:
        show_progress = False
    for project in folders:
        for folder, strip_prefix in folders[project]:
            if not args.recursive:
                err_exit('Error: "' + folder + '" is a folder but the -r/--recursive option was not given')
            assert(folder.startswith(strip_prefix))
            folder_destdir = os.path.join(destdir, folder[len(strip_prefix):].lstrip('/'))
            try:
                dxpy.download_folder(project, folder_destdir, folder=folder, overwrite=args.overwrite,
                                     show_progress=show_progress)
            except:
                err_exit()


# Main entry point.
def download(args):
    folders_to_get, files_to_get, count = collections.defaultdict(list), collections.defaultdict(list), 0
    foldernames, filenames = [], []
    for path in args.paths:
        # Attempt to resolve name. If --all is given or the path looks like a glob, download all matches.
        # Otherwise, the resolver will display a picker (or error out if there is no tty to display to).
        resolver_kwargs = {'allow_empty_string': False}
        if args.all or _is_glob(path):
            resolver_kwargs.update({'allow_mult': True, 'all_mult': True})

        project, folderpath, matching_files = try_call(resolve_existing_path, path, **resolver_kwargs)
        if matching_files is None:
            matching_files = []
        elif not isinstance(matching_files, list):
            matching_files = [matching_files]

        # TODO: this could also be returned as metadata by resolve_path since
        # resolve_path knows these things in some circumstances
        path_has_explicit_proj = is_project_explicit(path) or is_jbor_str(path)

        if is_jbor_str(path):
            assert len(matching_files) == 1
            project = matching_files[0]["describe"]["project"]

        matching_folders = []
        # project may be none if path is an ID and there is no project context
        if project is not None:
            colon_pos = get_first_pos_of_char(":", path)
            if colon_pos >= 0:
                path = path[colon_pos + 1:]
            abs_path, strip_prefix = _rel2abs(path, project)
            parent_folder = os.path.dirname(abs_path)
            folder_listing = dxpy.list_subfolders(project, parent_folder, recurse=False)
            matching_folders = pathmatch.filter(folder_listing, abs_path)
            if '/' in matching_folders and len(matching_folders) > 1:
                # The list of subfolders is {'/', '/A', '/B'}.
                # Remove '/', otherwise we will download everything twice.
                matching_folders.remove('/')

        if len(matching_files) == 0 and len(matching_folders) == 0:
            err_exit(fill('Error: {path} is neither a file nor a folder name'.format(path=path)))

        # If the user did not explicitly provide the project, don't pass any
        # project parameter to the API call but continue with the download.
        if not path_has_explicit_proj:
            project = dxpy.DXFile.NO_PROJECT_HINT

        # If the user explicitly provided the project and it doesn't contain
        # the files, don't allow the download.
        #
        # If length of matching_files is 0 then we're only downloading folders
        # so skip this logic since the files will be verified in the API call.
        if len(matching_files) > 0 and path_has_explicit_proj and not \
                any(object_exists_in_project(f['describe']['id'], project) for f in matching_files):
            err_exit(fill('Error: specified project does not contain specified file object'))

        files_to_get[project].extend(matching_files)
        folders_to_get[project].extend(((f, strip_prefix) for f in matching_folders))
        count += len(matching_files) + len(matching_folders)

        filenames.extend(f["describe"]["name"] for f in matching_files)
        foldernames.extend(f[len(strip_prefix):].lstrip('/') for f in matching_folders)

    if len(filenames) > 0 and len(foldernames) > 0:
        name_conflicts = set(filenames) & set(foldernames)
        if len(name_conflicts) > 0:
            msg = "Error: The following paths are both file and folder names, and " \
                  "cannot be downloaded to the same destination: "
            msg += ", ".join(sorted(name_conflicts))
            err_exit(fill(msg))

    if args.output is None:
        destdir, dest_filename = os.getcwd(), None
    elif count > 1:
        if not os.path.exists(args.output):
            err_exit(fill("Error: When downloading multiple objects, --output must be an existing directory"))
        destdir, dest_filename = args.output, None
    elif os.path.isdir(args.output):
        destdir, dest_filename = args.output, None
    elif args.output.endswith('/'):
        err_exit(fill("Error: {path} could not be found".format(path=args.output)))
    else:
        destdir, dest_filename = os.getcwd(), args.output

    _download_folders(folders_to_get, destdir, args)
    _download_files(files_to_get, destdir, args, dest_filename=dest_filename)
