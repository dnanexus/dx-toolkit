# Copyright (C) 2014-2020 DNAnexus, Inc.
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

from __future__ import print_function
import subprocess
import json
import os
from dxpy.utils import file_load_utils
from dxpy.exceptions import err_exit

def _build_mount_manifest(to_mount):
    files_list = []
    for file_rec in to_mount:
        file_entry = {}
        file_handler = file_rec['handler']
        file_entry['proj_id'] = file_handler.get_proj_id()
        file_entry['file_id'] = file_rec['src_file_id']
        file_name = file_rec['trg_fname']
        file_entry['parent'] = "/" + os.path.dirname(file_name)
        files_list.append(file_entry)
    files_manifest = {'Files': files_list}
    print("File mount manifest:")
    print(files_manifest)
    return files_manifest

def _which(program):
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    for path in os.environ["PATH"].split(os.pathsep):
        exe_file = os.path.join(path, program)
        if is_exe(exe_file):
            return exe_file
    return None

def _gen_helper_dict(filtered_inputs):
    '''
    Create a dict of values for the mounted files. This is similar to the variables created
    when running a bash app.
    '''

    file_key_descs, _ignore = file_load_utils.analyze_bash_vars(
        file_load_utils.get_input_json_file(), None)

    flattened_dict = {}

    def add_if_no_collision(key, value, dict_):
        if key not in dict_:
            dict_[key] = value

    for input_ in filtered_inputs:
        if input_ not in file_key_descs:
            continue
        input_var_dict = file_key_descs[input_]
        add_if_no_collision(input_ + '_path', input_var_dict["path"], flattened_dict)
        add_if_no_collision(input_ + '_name', input_var_dict["basename"], flattened_dict)
        add_if_no_collision(input_ + '_prefix', input_var_dict["prefix"], flattened_dict)

    return flattened_dict


def mount_all_inputs(exclude=None, verbose=False):
    '''
    :param exclude: List of input variables that should not be mounted.
    :type exclude: Array of strings
    :returns: dict of lists of strings where each key is the input variable
                and each list element is the full path to the file that has
                been mounted.
    :param verbose: Start dxfuse with '-verbose 2' logging
    :type verbose: boolean


    This function mounts all files that were supplied as inputs to the app.
    By convention, if an input parameter "FOO" has value

        {"$dnanexus_link": "file-xxxx"}

    and filename INPUT.TXT, then the linked file will be mounted into the
    path:

        $HOME/in/FOO/INPUT.TXT

    If an input is an array of files, then all files will be placed into
    numbered subdirectories under a parent directory named for the
    input. For example, if the input key is FOO, and the inputs are {A, B,
    C}.vcf then, the directory structure will be:

        $HOME/in/FOO/0/A.vcf
                     1/B.vcf
                     2/C.vcf

    Zero padding is used to ensure argument order. For example, if there are
    12 input files {A, B, C, D, E, F, G, H, I, J, K, L}.txt, the directory
    structure will be:

        $HOME/in/FOO/00/A.vcf
                     ...
                     11/L.vcf

    This allows using shell globbing (FOO/*/*.vcf) to get all the files in the input
    order and prevents issues with files which have the same filename.'''

    print("Mounting inputs...")

    home_dir = os.environ["HOME"]
    mount_dir = os.path.join(home_dir, "in")
    mount_manifest_file = os.path.join(home_dir, "mount-manifest.json")
    dxfuse_cmd = _which("dxfuse")
    if dxfuse_cmd is None:
        err_exit("dxfuse is not installed on this system")

    subprocess.check_output(["mkdir", mount_dir])

    try:
        job_input_file = file_load_utils.get_input_json_file()
        dirs, inputs, rest = file_load_utils.get_job_input_filenames(job_input_file)
    except IOError:
        msg = 'Error: Could not find the input json file: {0}.\n'.format(job_input_file)
        msg += '       This function should only be called from within a running job.'
        print(msg)
        raise

    # Remove excluded inputs
    if exclude:
        inputs = file_load_utils.filter_dict(inputs, exclude)

    # Convert to a flat list of elements to mount
    to_mount = []
    for ival_list in inputs.values():
        to_mount.extend(ival_list)

    files_manifest = _build_mount_manifest(to_mount)
    with open(mount_manifest_file, 'w') as mfile:
        json.dump(files_manifest, mfile)

    dxfuse_version = subprocess.check_output([dxfuse_cmd, "-version"])
    print("Using dxfuse version " + str(dxfuse_version))

    uid = str(int(subprocess.check_output(["id", "-u"])))
    gid = str(int(subprocess.check_output(["id", "-g"])))
    cmd = [dxfuse_cmd, "-uid", uid, "-gid", gid, mount_dir, mount_manifest_file]
    if verbose:
        cmd[1:1] = ["-verbose", "2"]
    print(subprocess.check_output(cmd))

    print("Done mounting inputs.")

    subprocess.call(["find", mount_dir, "-name", "*"])

    helper_vars = _gen_helper_dict(inputs)
    return helper_vars
