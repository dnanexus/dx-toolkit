# Copyright (C) 2014-2015 DNAnexus, Inc.
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
from __future__ import (print_function, unicode_literals)
import pprint

import os
import sys
import collections
import dxpy
import dxpy.utils.printing as printing
from ..utils.resolver import (resolve_existing_path, resolve_path,
                              get_last_pos_of_char, get_first_pos_of_char)
from ..exceptions import (err_exit, DXCLIError, InvalidState)
from . import (try_call, try_call_err_exit)
from dxpy.utils.printing import (fill)
from dxpy.utils import pathmatch
from ..utils.env import get_env_var


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


def _list_subfolders(project, path, cached_folder_lists, recurse=True):
    if project not in cached_folder_lists:
        cached_folder_lists[project] = dxpy.get_handler(project).describe(
            input_params={'folders': True}
        )['folders']
    # TODO: support shell-style path globbing (i.e. /a*/c matches /ab/c but not /a/b/c)
    # return pathmatch.filter(cached_folder_lists[project], os.path.join(path, '*'))
    if recurse:
        return (f for f in cached_folder_lists[project] if f.startswith(path))
    else:
        return (f for f in cached_folder_lists[project] if f.startswith(path) and '/' not in f[len(path)+1:])


def _download_one_folder(project, folder, strip_prefix, destdir, cached_folder_lists, args):
    assert(folder.startswith(strip_prefix))
    if not args.recursive:
        err_exit('Error: "' + folder + '" is a folder but the -r/--recursive option was not given')

    for subfolder in _list_subfolders(project, folder, cached_folder_lists, recurse=True):
        _ensure_local_dir(os.path.join(destdir, subfolder[len(strip_prefix):].lstrip('/')))

    # TODO: control visibility=hidden
    for f in dxpy.search.find_data_objects(classname='file', state='closed', project=project, folder=folder,
                                           recurse=True, describe=True):
        file_desc = f['describe']
        dest_filename = os.path.join(destdir, file_desc['folder'][len(strip_prefix):].lstrip('/'), file_desc['name'])
        download_one_file(project, file_desc, dest_filename, args)


def _is_glob(path):
    return get_first_pos_of_char('*', path) > -1 or get_first_pos_of_char('?', path) > -1


def _rel2abs(path, project):
    if path.startswith('/') or dxpy.WORKSPACE_ID != project:
        abs_path, strip_prefix = path, os.path.dirname(path.rstrip('/'))
    else:
        wd = get_env_var('DX_CLI_WD', u'/')
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


def _download_folders(folders, destdir, cached_folder_lists, args):
    for project in folders:
        for folder, strip_prefix in folders[project]:
            _download_one_folder(project, folder, strip_prefix, destdir, cached_folder_lists, args)


# Main entry point.
def download(args):
    # Get space for caching subfolders
    cached_folder_lists = {}

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

        matching_folders = []
        if project is not None:
            # project may be none if path is an ID and there is no project context
            colon_pos = get_first_pos_of_char(":", path)
            if colon_pos >= 0:
                path = path[colon_pos + 1:]
            abs_path, strip_prefix = _rel2abs(path, project)
            parent_folder = os.path.dirname(abs_path)
            folder_listing = _list_subfolders(project, parent_folder, cached_folder_lists, recurse=False)
            matching_folders = pathmatch.filter(folder_listing, abs_path)
            if '/' in matching_folders and len(matching_folders) > 1:
                # The list of subfolders is {'/', '/A', '/B'}.
                # Remove '/', otherwise we will download everything twice.
                matching_folders.remove('/')

        if len(matching_files) == 0 and len(matching_folders) == 0:
            err_exit(fill('Error: {path} is neither a file nor a folder name'.format(path=path)))
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

    _download_folders(folders_to_get, destdir, cached_folder_lists, args)
    _download_files(files_to_get, destdir, args, dest_filename=dest_filename)
