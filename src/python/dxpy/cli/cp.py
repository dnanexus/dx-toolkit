# Copyright (C) 2014 DNAnexus, Inc.
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
This submodule handles copy commands for the dx
command-line client.

'dx cp' works only between DIFFERENT projects. It will exit fatally otherwise.
'''

from __future__ import (print_function, unicode_literals)

import dxpy
import requests
import dxpy.utils.printing as printing
from ..utils.resolver import (resolve_existing_path, resolve_path, is_analysis_id, is_hashid,
                              get_last_pos_of_char, get_first_pos_of_char)
from ..exceptions import (err_exit, DXCLIError, InvalidState)
from . import (try_call, try_call_err_exit)
from dxpy.utils.printing import (fill)


def cp_to_noexistent_destination(args, dest_path, dx_dest, dest_proj):
    ''' Copy the source to a destination that does not currently
    exist. This involves creating the target file/folder.
    '''
    # Destination folder path is new => renaming
    if len(args.sources) != 1:
        # Can't copy and rename more than one object
        raise DXCLIError('The destination folder does not exist')
    last_slash_pos = get_last_pos_of_char('/', dest_path)
    if last_slash_pos == 0:
        dest_folder = '/'
    else:
        dest_folder = dest_path[:last_slash_pos]
    dest_name = dest_path[last_slash_pos + 1:].replace('\/', '/')
    try:
        dx_dest.list_folder(folder=dest_folder, only='folders')
    except dxpy.DXAPIError as details:
        if details.code == requests.codes['not_found']:
            raise DXCLIError('The destination folder does not exist')
        else:
            raise
    except:
        err_exit()

    # Clone and rename either the data object or the folder.
    # src_result is None if it could not be resolved to an object.
    src_proj, src_path, src_results = try_call(resolve_existing_path,
                                               args.sources[0],
                                               allow_mult=True, all_mult=args.all)

    if src_proj == dest_proj:
        if is_hashid(args.sources[0]):
            # This is the only case in which the source project is
            # purely assumed, so give a better error message.
            raise DXCLIError(fill('Error: You must specify a source project for ' + args.sources[0]))
        else:
            raise DXCLIError(fill('A source path and the destination path resolved to the ' +
                                'same project or container.  Please specify different source ' +
                                'and destination containers, e.g.') +
                             '\n  dx cp source-project:source-id-or-path dest-project:dest-path')

    if src_results is None:
        try:
            contents = dxpy.api.project_list_folder(src_proj,
                                                    {"folder": src_path, "includeHidden": True})
            dxpy.api.project_new_folder(dest_proj, {"folder": dest_path})
            exists = dxpy.api.project_clone(src_proj,
                                            {"folders": contents['folders'],
                                             "objects": [result['id'] for result in contents['objects']],
                                             "project": dest_proj,
                                             "destination": dest_path})['exists']
            if len(exists) > 0:
                print(fill('The following objects already existed in the destination ' +
                           'container and were not copied:') + '\n ' + '\n '.join(exists))
                return
        except:
            err_exit()
    else:
        try:
            exists = dxpy.api.project_clone(src_proj,
                                            {"objects": [result['id'] for result in src_results],
                                             "project": dest_proj,
                                             "destination": dest_folder})['exists']
            if len(exists) > 0:
                print(fill('The following objects already existed in the destination ' +
                           'container and were not copied:') + '\n ' + '\n '.join(exists))
            for result in src_results:
                if result['id'] not in exists:
                    dxpy.DXHTTPRequest('/' + result['id'] + '/rename',
                                       {"project": dest_proj,
                                        "name": dest_name})
            return
        except:
            err_exit()


def cp(args):
    dest_proj, dest_path, _none = try_call(resolve_path,
                                           args.destination, 'folder')
    if dest_path is None:
        raise DXCLIError('Cannot copy to a hash ID')
    dx_dest = dxpy.get_handler(dest_proj)
    try:
        # check if the destination exists
        dx_dest.list_folder(folder=dest_path, only='folders')
    except:
        cp_to_noexistent_destination(args, dest_path, dx_dest, dest_proj)
        return

    # The destination exists, we need to copy all of the sources to it.
    if len(args.sources) == 0:
        raise DXCLIError('No sources provided to copy to another project')
    src_objects = []
    src_folders = []
    for source in args.sources:
        src_proj, src_folderpath, src_results = try_call(resolve_existing_path,
                                                         source,
                                                         allow_mult=True, all_mult=args.all)
        if src_proj == dest_proj:
            if is_hashid(source):
                # This is the only case in which the source project is
                # purely assumed, so give a better error message.
                raise DXCLIError(fill('Error: You must specify a source project for ' + source))
            else:
                raise DXCLIError(fill('Error: A source path and the destination path resolved ' +
                                    'to the same project or container. Please specify ' +
                                    'different source and destination containers, e.g.') +
                                 '\n  dx cp source-project:source-id-or-path dest-project:dest-path')

        if src_proj is None:
            raise DXCLIError(fill('Error: A source project must be specified or a current ' +
                                  'project set in order to clone objects between projects'))

        if src_results is None:
            src_folders.append(src_folderpath)
        else:
            src_objects += [result['id'] for result in src_results]
    try:
        exists = dxpy.DXHTTPRequest('/' + src_proj + '/clone',
                                    {"objects": src_objects,
                                     "folders": src_folders,
                                     "project": dest_proj,
                                     "destination": dest_path})['exists']
        if len(exists) > 0:
            print(fill('The following objects already existed in the destination container ' +
                       'and were left alone:') + '\n ' + '\n '.join(exists))
    except:
        err_exit()
