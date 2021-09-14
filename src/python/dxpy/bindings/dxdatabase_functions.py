# Copyright (C) 2013-2019 DNAnexus, Inc.
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
Helper Functions
****************

The following helper functions are useful shortcuts for interacting with Database objects.

'''

from __future__ import print_function, unicode_literals, division, absolute_import

import os, sys, math, mmap, stat
import hashlib
import traceback
import warnings
from collections import defaultdict
import multiprocessing

import dxpy
from .. import logger
from . import dxfile, DXFile
from . import dxdatabase, DXDatabase
from .dxfile import FILE_REQUEST_TIMEOUT
from ..compat import open, USING_PYTHON2, md5_hasher
from ..exceptions import DXFileError, DXChecksumMismatchError, DXIncompleteReadsError, err_exit
from ..utils import response_iterator
import subprocess

def download_dxdatabasefile(dxid, dst_filename, src_filename, file_status, chunksize=dxfile.DEFAULT_BUFFER_SIZE, append=False, show_progress=False,
                            project=None, describe_output=None, **kwargs):
    '''
    :param dxid: DNAnexus database ID or DXDatabase (database handler) object
    :type dxid: string or DXDatabase
    :param dst_filename: Local filename
    :type dst_filename: string
    :param src_filename: Name of database file or folder being downloaded
    :type src_filename: string
    :param file_status: Metadata for the source file being downloaded
    :type file_status: dict
    :param append: If True, appends to the local file (default is to truncate local file if it exists)
    :type append: boolean
    :param project: project to use as context for this download (may affect
            which billing account is billed for this download). If None or
            DXFile.NO_PROJECT_HINT, no project hint is supplied to the API server.
    :type project: str or None
    :param describe_output: (experimental) output of the database-xxxx/describe API call,
            if available. It will make it possible to skip another describe API call.
            It should contain the default fields of the describe API call output and
            the "parts" field, not included in the output by default.
    :type describe_output: dict or None

    Downloads the remote file referenced by *src_filename* from database referenced
    by *dxid* and saves it to *dst_filename*.

    Example::

        download_dxdatabasefile("database-xxxx", "localfilename", "tablename/data.parquet)

    '''
    # retry the inner loop while there are retriable errors

    part_retry_counter = defaultdict(lambda: 3)
    success = False
    while not success:
        success = _download_dxdatabasefile(dxid,
                                           dst_filename,
                                           src_filename,
                                           file_status,
                                           part_retry_counter,
                                           chunksize=chunksize,
                                           append=append,
                                           show_progress=show_progress,
                                           project=project,
                                           describe_output=describe_output,
                                           **kwargs)


# Check if a program (wget, curl, etc.) is on the path, and
# can be called.
def _which(program):
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    for path in os.environ["PATH"].split(os.pathsep):
        exe_file = os.path.join(path, program)
        if is_exe(exe_file):
            return exe_file
    return None

# Caluclate the md5 checkum for [filename], and raise
# an exception if the checksum is wrong.
def _verify(filename, md5digest):
    md5sum_exe = _which("md5sum")
    if md5sum_exe is None:
        err_exit("md5sum is not installed on this system")
    cmd = [md5sum_exe, "-b", filename]
    try:
        print("Calculating checksum")
        cmd_out = subprocess.check_output(cmd)
    except subprocess.CalledProcessError:
        err_exit("Failed to run md5sum: " + str(cmd))

    line = cmd_out.strip().split()
    if len(line) != 2:
        err_exit("md5sum returned weird results: " + str(line))
    actual_md5 = line[0]
    md5digest = md5digest.encode("ascii")

    # python-3 : both digests have to be in bytes
    if actual_md5 != md5digest:
        err_exit("Checksum doesn't match " + str(actual_md5) + "  expected:" + str(md5digest))
    print("Checksum correct")

def do_debug(msg):
    logger.debug(msg)

def _download_dxdatabasefile(dxid, filename, src_filename, file_status, part_retry_counter,
                     chunksize=dxfile.DEFAULT_BUFFER_SIZE, append=False, show_progress=False,
                     project=None, describe_output=None, **kwargs):
    '''
    Core of download logic. Download the specified file/folder *src_filename*
    associated with database-id *dxid* and store it in
    a local file *filename*.

    The return value is as follows:
    - True means the download was successfully completed
    - False means the download was stopped because of a retryable error
    - Exception raised for other errors
    '''
    def print_progress(bytes_downloaded, file_size, action="Downloaded"):
        num_ticks = 60

        effective_file_size = file_size or 1
        if bytes_downloaded > effective_file_size:
            effective_file_size = bytes_downloaded

        ticks = int(round((bytes_downloaded / float(effective_file_size)) * num_ticks))
        percent = int(math.floor((bytes_downloaded / float(effective_file_size)) * 100))

        fmt = "[{done}{pending}] {action} {done_bytes:,}{remaining} bytes ({percent}%) {name}"
        # Erase the line and return the cursor to the start of the line.
        # The following VT100 escape sequence will erase the current line.
        sys.stderr.write("\33[2K")
        sys.stderr.write(fmt.format(action=action,
                                    done=("=" * (ticks - 1) + ">") if ticks > 0 else "",
                                    pending=" " * (num_ticks - ticks),
                                    done_bytes=bytes_downloaded,
                                    remaining=" of {size:,}".format(size=file_size) if file_size else "",
                                    percent=percent,
                                    name=filename))
        sys.stderr.flush()
        sys.stderr.write("\r")
        sys.stderr.flush()

    _bytes = 0

    if isinstance(dxid, DXDatabase):
        dxdatabase = dxid
    else:
        dxdatabase = DXDatabase(dxid, project=(project if project != DXFile.NO_PROJECT_HINT else None))

    do_debug("dxdatabase_functions.py _download_dxdatabasefile - dxdatabase: {}".format(dxdatabase)) 

    if describe_output and describe_output.get("parts") is not None:
        dxdatabase_desc = describe_output
    else:
        dxdatabase_desc = dxdatabase.describe(fields={"parts"}, default_fields=True, **kwargs)

    # For database files, the whole parquet file is one 'part'.
    # Consider a multi part download approach in the future.
    parts = {u'1': {u'state': u'complete', u'size': file_status["size"]}}
    parts_to_get = sorted(parts, key=int)
    file_size = file_status["size"]

    offset = 0
    for part_id in parts_to_get:
        parts[part_id]["start"] = offset
        offset += parts[part_id]["size"]

    do_debug("dxdatabase_functions.py _download_dxdatabasefile - parts {}".format(parts))

    # Create proper destination path, including any subdirectories needed within path.
    ensure_local_dir(filename);
    dest_path = os.path.join(filename, src_filename)
    dest_dir_idx = dest_path.rfind("/");
    if dest_dir_idx != -1:
        dest_dir = dest_path[:dest_dir_idx]
        ensure_local_dir(dest_dir)      

    do_debug("dxdatabase_functions.py _download_dxdatabasefile - dest_path {}".format(dest_path)) 

    if append:
        fh = open(dest_path, "ab")
    else:
        try:
            fh = open(dest_path, "rb+")
        except IOError:
            fh = open(dest_path, "wb")

    if show_progress:
        print_progress(0, None)

    def get_chunk(part_id_to_get, start, end):
        do_debug("dxdatabase_functions.py get_chunk - start {}, end {}, part id {}".format(start, end, part_id_to_get))
        url, headers = dxdatabase.get_download_url(src_filename=src_filename, project=project, **kwargs)
        # No sub ranges for database file downloads
        sub_range = False
        data_url = dxpy._dxhttp_read_range(url, headers, start, end, FILE_REQUEST_TIMEOUT, sub_range)
        do_debug("dxdatabase_functions.py get_chunk - data_url = {}".format(data_url))
        # 'data_url' is the s3 URL, so read again, just like in DNAxFileSystem
        data = dxpy._dxhttp_read_range(data_url, headers, start, end, FILE_REQUEST_TIMEOUT, sub_range)
        return part_id_to_get, data

    def chunk_requests():
        for part_id_to_chunk in parts_to_get:
            part_info = parts[part_id_to_chunk]
            for chunk_start in range(part_info["start"], part_info["start"] + part_info["size"], chunksize):
                chunk_end = min(chunk_start + chunksize, part_info["start"] + part_info["size"]) - 1
                yield get_chunk, [part_id_to_chunk, chunk_start, chunk_end], {}

    def verify_part(_part_id, got_bytes, hasher):
        if got_bytes is not None and got_bytes != parts[_part_id]["size"]:
            msg = "Unexpected part data size in {} part {} (expected {}, got {})"
            msg = msg.format(dxdatabase.get_id(), _part_id, parts[_part_id]["size"], got_bytes)
            raise DXPartLengthMismatchError(msg)
        if hasher is not None and "md5" not in parts[_part_id]:
            warnings.warn("Download of file {} is not being checked for integrity".format(dxdatabase.get_id()))
        elif hasher is not None and hasher.hexdigest() != parts[_part_id]["md5"]:
            msg = "Checksum mismatch in {} part {} (expected {}, got {})"
            msg = msg.format(dxdatabase.get_id(), _part_id, parts[_part_id]["md5"], hasher.hexdigest())
            raise DXChecksumMismatchError(msg)

    with fh:

        try:
            # Main loop. In parallel: download chunks, verify them, and write them to disk.
            get_first_chunk_sequentially = (file_size > 128 * 1024 and dxpy.JOB_ID)
            cur_part, got_bytes, hasher = None, None, None
            for chunk_part, chunk_data in response_iterator(chunk_requests(),
                                                            dxdatabase._http_threadpool,
                                                            do_first_task_sequentially=get_first_chunk_sequentially):
                if chunk_part != cur_part:
                    # TODO: remove permanently if we don't find use for this
                    # verify_part(cur_part, got_bytes, hasher)

                    cur_part, got_bytes, hasher = chunk_part, 0, md5_hasher()
                got_bytes += len(chunk_data)
                hasher.update(chunk_data)
                fh.write(chunk_data)
                if show_progress:
                    _bytes += len(chunk_data)
                    print_progress(_bytes, file_size)
            # TODO: same as above
            # verify_part(cur_part, got_bytes, hasher)
            if show_progress:
                print_progress(_bytes, file_size, action="Completed")
        except DXFileError:
            print(traceback.format_exc(), file=sys.stderr)
            part_retry_counter[cur_part] -= 1
            if part_retry_counter[cur_part] > 0:
                print("Retrying {} ({} tries remain for part {})".format(dxdatabase.get_id(), part_retry_counter[cur_part], cur_part),
                      file=sys.stderr)
                return False
            raise

        if show_progress:
            sys.stderr.write("\n")

        return True

def ensure_local_dir(d):
    if not os.path.isdir(d):
        if os.path.exists(d):
            raise DXFileError("Destination location '{}' already exists and is not a directory".format(d))
        logger.debug("Creating destination directory: '%s'", d)
        os.makedirs(d)
