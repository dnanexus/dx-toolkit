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

from __future__ import print_function
import concurrent.futures
import os
import sys
import multiprocessing
import psutil

import dxpy
from dxpy.utils import file_load_utils

def _create_dirs(idir, dirs):
    '''
    Create a set of directories, so we could store the input files.
    For example, seq1 could be stored under:
        /in/seq1/NC_001122.fasta

    TODO: this call could fail, we need to report a reasonable error code

    Note that we create a directory for every file array, even if
    it has zero inputs.
    '''
    # create the <idir> itself
    file_load_utils.ensure_dir(idir)
    # create each subdir
    for d in dirs:
        file_load_utils.ensure_dir(os.path.join(idir, d))

def _download_one_file(file_rec, idir):
    src_file = file_rec['src_file_id']
    trg_file = os.path.join(idir, file_rec['trg_fname'])
    print("downloading file: " + src_file + " to filesystem: " + trg_file)
    sys.stdout.flush()
    dxpy.download_dxfile(src_file, trg_file)
    return file_rec

# Download the files sequentially
#   to_download: list of tuples describing files to download
def _sequential_file_download(to_download, idir):
    for file_rec in to_download:
        _download_one_file(file_rec, idir)

# Download files in parallel
#   to_download: list of tuples describing files to download
def _parallel_file_download(to_download, idir, max_num_parallel_downloads):
    try:
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_num_parallel_downloads) as executor:
            future_files = {executor.submit(_download_one_file, file_rec, idir): file_rec
                            for file_rec in to_download}
            for future in concurrent.futures.as_completed(future_files):
                file_rec = future_files[future]
                try:
                    future.result()
                except Exception:
                    sys.stderr.write('%r -> %s generated an exception' %
                                     (file_rec['src_file_id'], file_rec['trg_fname']))
                    raise
    except KeyboardInterrupt:
        # Call os._exit() in case of KeyboardInterrupt. Otherwise, the atexit registered handler in
        # concurrent.futures.thread will run, and issue blocking join() on all worker threads,
        # requiring us to listen to events in worker threads in order to enable timely exit in
        # response to Ctrl-C.
        print("", file=sys.stderr)
        os._exit(os.EX_IOERR)

def _gen_helper_dict(filtered_inputs):
    '''
    Create a dict of values for the downloaded files. This is similar to the variables created
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


def _get_num_parallel_threads(max_threads, num_cores, mem_available_mb):
    '''
    Ensure at least ~1.2 GB memory per thread, see PTFM-18767
    '''
    return min(max_threads, num_cores, max(int(mem_available_mb/1200), 1))


def download_all_inputs(exclude=None, parallel=False, max_threads=8):
    '''
    :param exclude: List of input variables that should not be downloaded.
    :type exclude: Array of strings
    :param parallel: Should we download multiple files in parallel? (default: False)
    :type filename: boolean
    :param max_threads: If parallel is True, how many threads should be used
        to download files? (default: 8)
    :type append: int
    :returns: dict of lists of strings where each key is the input variable
                and each list element is the full path to the file that has
                been downloaded.

    This function downloads all files that were supplied as inputs to the app.
    By convention, if an input parameter "FOO" has value

        {"$dnanexus_link": "file-xxxx"}

    and filename INPUT.TXT, then the linked file will be downloaded into the
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

    # Input directory, where all inputs are downloaded
    idir = file_load_utils.get_input_dir()
    try:
        job_input_file = file_load_utils.get_input_json_file()
        dirs, inputs, rest = file_load_utils.get_job_input_filenames(job_input_file)
    except IOError:
        msg = 'Error: Could not find the input json file: {0}.\n'.format(job_input_file)
        msg += '       This function should only be called from within a running job.'
        print(msg)
        raise

    # Exclude directories
    # dirs contain all folders (e.g. $HOME/in/FOO) and their sub folders (e.g. $HOME/in/FOO/1, $HOME/in/FOO/2, etc.)
    # If the main folder is excluded, its sub-folder would also be excluded from dirs_to_create
    dirs_to_create = []
    for d in dirs:
        keep = True
        if (exclude is not None) and (d is not None):
            if (d.split('/')[0] in exclude):
                keep = False
        if keep:
            dirs_to_create.append(d)

    # Create the directory structure, in preparation for download.
    # Allows performing the download in parallel.
    _create_dirs(idir, dirs_to_create)

    # Remove excluded inputs
    if exclude:
        inputs = file_load_utils.filter_dict(inputs, exclude)

    # Convert to a flat list of elements to download
    to_download = []
    for ival_list in inputs.values():
        to_download.extend(ival_list)

    # Download the files
    if parallel:
        total_mem = psutil.virtual_memory().total >> 20  # Total RAM in MB
        num_cores = multiprocessing.cpu_count()
        max_num_parallel_downloads = _get_num_parallel_threads(max_threads, num_cores, total_mem)
        sys.stderr.write("Downloading files using {} threads".format(max_num_parallel_downloads))
        _parallel_file_download(to_download, idir, max_num_parallel_downloads)
    else:
        _sequential_file_download(to_download, idir)

    helper_vars = _gen_helper_dict(inputs)
    return helper_vars
