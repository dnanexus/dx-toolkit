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
Helper Functions
****************

The following helper functions are useful shortcuts for interacting with File objects.

'''

from __future__ import print_function, unicode_literals, division, absolute_import

import os, sys, math, mmap, stat
import hashlib
import traceback
import warnings
from collections import defaultdict
import multiprocessing
from random import randint
from time import sleep

import dxpy
from .. import logger
from . import dxfile, DXFile
from .dxfile import FILE_REQUEST_TIMEOUT
from ..exceptions import DXError, DXFileError, DXPartLengthMismatchError, DXChecksumMismatchError, DXIncompleteReadsError, err_exit
from ..compat import open, md5_hasher, USING_PYTHON2
from ..utils import response_iterator
import subprocess

def open_dxfile(dxid, project=None, mode=None, read_buffer_size=dxfile.DEFAULT_BUFFER_SIZE):
    '''
    :param dxid: file ID
    :type dxid: string
    :rtype: :class:`~dxpy.bindings.dxfile.DXFile`

    Given the object ID of an uploaded file, returns a remote file
    handler that is a Python file-like object.

    Example::

      with open_dxfile("file-xxxx") as fd:
          for line in fd:
              ...

    Note that this is shorthand for::

      DXFile(dxid)

    '''
    return DXFile(dxid, project=project, mode=mode, read_buffer_size=read_buffer_size)


def new_dxfile(mode=None, write_buffer_size=dxfile.DEFAULT_BUFFER_SIZE, expected_file_size=None, file_is_mmapd=False,
               **kwargs):
    '''
    :param mode: One of "w" or "a" for write and append modes, respectively
    :type mode: string
    :rtype: :class:`~dxpy.bindings.dxfile.DXFile`

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`.

    Creates a new remote file object that is ready to be written to;
    returns a :class:`~dxpy.bindings.dxfile.DXFile` object that is a
    writable file-like object.

    Example::

        with new_dxfile(media_type="application/json") as fd:
            fd.write("foo\\n")

    Note that this is shorthand for::

        dxFile = DXFile()
        dxFile.new(**kwargs)

    '''
    dx_file = DXFile(mode=mode, write_buffer_size=write_buffer_size, expected_file_size=expected_file_size,
                     file_is_mmapd=file_is_mmapd)
    dx_file.new(**kwargs)
    return dx_file


def download_dxfile(dxid, filename, chunksize=dxfile.DEFAULT_BUFFER_SIZE, append=False, show_progress=False,
                    project=None, describe_output=None, symlink_max_tries=15, **kwargs):
    '''
    :param dxid: DNAnexus file ID or DXFile (file handler) object
    :type dxid: string or DXFile
    :param filename: Local filename
    :type filename: string
    :param append: If True, appends to the local file (default is to truncate local file if it exists)
    :type append: boolean
    :param project: project to use as context for this download (may affect
            which billing account is billed for this download). If None or
            DXFile.NO_PROJECT_HINT, no project hint is supplied to the API server.
    :type project: str or None
    :param describe_output: (experimental) output of the file-xxxx/describe API call,
            if available. It will make it possible to skip another describe API call.
            It should contain the default fields of the describe API call output and
            the "parts" field, not included in the output by default.
    :type describe_output: dict or None
    :param symlink_max_tries: Maximum amount of tries when downloading a symlink with aria2c.
    :type symlink_max_tries: int or None

    Downloads the remote file referenced by *dxid* and saves it to *filename*.

    Example::

        download_dxfile("file-xxxx", "localfilename.fastq")

    '''
    # retry the inner loop while there are retriable errors
    part_retry_counter = defaultdict(lambda: 3)
    success = False
    while not success:
        success = _download_dxfile(dxid,
                                   filename,
                                   part_retry_counter,
                                   chunksize=chunksize,
                                   append=append,
                                   show_progress=show_progress,
                                   project=project,
                                   describe_output=describe_output,
                                   symlink_max_tries=symlink_max_tries,
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


# [dxid] is a symbolic link. Create a preauthenticated URL,
# and download it
def _download_symbolic_link(dxid, md5digest, project, dest_filename, symlink_max_tries=15):
    if symlink_max_tries < 1:
        raise dxpy.exceptions.DXError("symlink_max_tries argument has to be positive integer")

    # Check if aria2 present, if not, error.
    aria2c_exe = _which("aria2c")

    if aria2c_exe is None:
        err_exit("aria2c must be installed on this system to download this data. " + \
                 "Please see the documentation at https://aria2.github.io/.")
        return

    dxfile = dxpy.DXFile(dxid)
    url, _headers = dxfile.get_download_url(preauthenticated=True,
                                            duration=6*3600,
                                            project=project)

    # aria2c does not allow more than 16 connections per server
    max_connections = min(16, multiprocessing.cpu_count())
    cmd = [
        "aria2c",
        "-c",                        # continue downloading a partially downloaded file
        "-s", str(max_connections),  # number of concurrent downloads (split file)
        "-x", str(max_connections),  # maximum number of connections to one server for  each  download
        "--retry-wait=10"            # time to wait before retrying
    ]
    cmd.extend(["-m", str(symlink_max_tries)])

    # Split path properly for aria2c
    # If '-d' arg not provided, aria2c uses current working directory
    cwd = os.getcwd()
    directory, filename = os.path.split(dest_filename)
    directory = cwd if directory in ["", cwd] else directory
    cmd += ["-o", filename, "-d", os.path.abspath(directory), url]

    try:
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        err_exit("Failed to call download: {cmd}\n{msg}\n".format(cmd=str(cmd), msg=e))

    if md5digest is not None:
        _verify(dest_filename, md5digest)

def _download_dxfile(dxid, filename, part_retry_counter,
                     chunksize=dxfile.DEFAULT_BUFFER_SIZE, append=False, show_progress=False,
                     project=None, describe_output=None, symlink_max_tries=15, **kwargs):
    '''
    Core of download logic. Download file-id *dxid* and store it in
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

    if isinstance(dxid, DXFile):
        dxfile = dxid
    else:
        dxfile = DXFile(dxid, mode="r", project=(project if project != DXFile.NO_PROJECT_HINT else None))

    if describe_output and describe_output.get("parts") is not None:
        dxfile_desc = describe_output
    else:
        dxfile_desc = dxfile.describe(fields={"parts"}, default_fields=True, **kwargs)

    # handling of symlinked files.
    if 'drive' in dxfile_desc:
        if 'md5' in dxfile_desc:
            md5 = dxfile_desc['md5']
        else:
            md5 = None
        _download_symbolic_link(dxid, md5, project, filename, symlink_max_tries=symlink_max_tries)
        return True

    parts = dxfile_desc["parts"]
    parts_to_get = sorted(parts, key=int)
    file_size = dxfile_desc.get("size")

    offset = 0
    for part_id in parts_to_get:
        parts[part_id]["start"] = offset
        offset += parts[part_id]["size"]

    if append:
        fh = open(filename, "ab")
    else:
        try:
            fh = open(filename, "rb+")
        except IOError:
            fh = open(filename, "wb")

    if show_progress:
        print_progress(0, None)

    def get_chunk(part_id_to_get, start, end):
        url, headers = dxfile.get_download_url(project=project, **kwargs)
        # If we're fetching the whole object in one shot, avoid setting the Range header to take advantage of gzip
        # transfer compression
        sub_range = False
        if len(parts) > 1 or (start > 0) or (end - start + 1 < parts[part_id_to_get]["size"]):
            sub_range = True
        data = dxpy._dxhttp_read_range(url, headers, start, end, FILE_REQUEST_TIMEOUT, sub_range)
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
            msg = msg.format(dxfile.get_id(), _part_id, parts[_part_id]["size"], got_bytes)
            raise DXPartLengthMismatchError(msg)
        if hasher is not None and "md5" not in parts[_part_id]:
            warnings.warn("Download of file {} is not being checked for integrity".format(dxfile.get_id()))
        elif hasher is not None and hasher.hexdigest() != parts[_part_id]["md5"]:
            msg = "Checksum mismatch in {} part {} (expected {}, got {})"
            msg = msg.format(dxfile.get_id(), _part_id, parts[_part_id]["md5"], hasher.hexdigest())
            raise DXChecksumMismatchError(msg)

    with fh:
        last_verified_pos = 0

        if fh.mode == "rb+":
            # We already downloaded the beginning of the file, verify that the
            # chunk checksums match the metadata.
            last_verified_part, max_verify_chunk_size = None, 1024*1024
            try:
                for part_id in parts_to_get:
                    part_info = parts[part_id]
                    if "md5" not in part_info:
                        raise DXFileError("File {} does not contain part md5 checksums".format(dxfile.get_id()))
                    bytes_to_read = part_info["size"]
                    hasher = md5_hasher()
                    while bytes_to_read > 0:
                        chunk = fh.read(min(max_verify_chunk_size, bytes_to_read))
                        if len(chunk) < min(max_verify_chunk_size, bytes_to_read):
                            raise DXFileError("Local data for part {} is truncated".format(part_id))
                        hasher.update(chunk)
                        bytes_to_read -= max_verify_chunk_size
                    if hasher.hexdigest() != part_info["md5"]:
                        raise DXFileError("Checksum mismatch when verifying downloaded part {}".format(part_id))
                    else:
                        last_verified_part = part_id
                        last_verified_pos = fh.tell()
                        if show_progress:
                            _bytes += part_info["size"]
                            print_progress(_bytes, file_size, action="Verified")
            except (IOError, DXFileError) as e:
                logger.debug(e)
            fh.seek(last_verified_pos)
            fh.truncate()
            if last_verified_part is not None:
                del parts_to_get[:parts_to_get.index(last_verified_part)+1]
            if show_progress and len(parts_to_get) < len(parts):
                print_progress(last_verified_pos, file_size, action="Resuming at")
            logger.debug("Verified %s/%d downloaded parts", last_verified_part, len(parts_to_get))

        try:
            # Main loop. In parallel: download chunks, verify them, and write them to disk.
            get_first_chunk_sequentially = (file_size > 128 * 1024 and last_verified_pos == 0 and dxpy.JOB_ID)
            cur_part, got_bytes, hasher = None, None, None
            for chunk_part, chunk_data in response_iterator(chunk_requests(),
                                                            dxfile._http_threadpool,
                                                            do_first_task_sequentially=get_first_chunk_sequentially):
                if chunk_part != cur_part:
                    verify_part(cur_part, got_bytes, hasher)
                    cur_part, got_bytes, hasher = chunk_part, 0, md5_hasher()
                got_bytes += len(chunk_data)
                hasher.update(chunk_data)
                fh.write(chunk_data)
                if show_progress:
                    _bytes += len(chunk_data)
                    print_progress(_bytes, file_size)
            verify_part(cur_part, got_bytes, hasher)
            if show_progress:
                print_progress(_bytes, file_size, action="Completed")
        except DXFileError:
            print(traceback.format_exc(), file=sys.stderr)
            part_retry_counter[cur_part] -= 1
            if part_retry_counter[cur_part] > 0:
                print("Retrying {} ({} tries remain for part {})".format(dxfile.get_id(), part_retry_counter[cur_part], cur_part),
                      file=sys.stderr)
                return False
            raise

        if show_progress:
            sys.stderr.write("\n")

        return True

def upload_local_file(filename=None, file=None, media_type=None, keep_open=False,
                      wait_on_close=False, use_existing_dxfile=None, show_progress=False,
                      write_buffer_size=None, multithread=True, **kwargs):
    '''
    :param filename: Local filename
    :type filename: string
    :param file: File-like object
    :type file: File-like object
    :param media_type: Internet Media Type
    :type media_type: string
    :param keep_open: If False, closes the file after uploading
    :type keep_open: boolean
    :param write_buffer_size: Buffer size to use for upload
    :type write_buffer_size: int
    :param wait_on_close: If True, waits for the file to close
    :type wait_on_close: boolean
    :param use_existing_dxfile: Instead of creating a new file object, upload to the specified file
    :type use_existing_dxfile: :class:`~dxpy.bindings.dxfile.DXFile`
    :param multithread: If True, sends multiple write requests asynchronously
    :type multithread: boolean
    :returns: Remote file handler
    :rtype: :class:`~dxpy.bindings.dxfile.DXFile`

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`.

    Exactly one of *filename* or *file* is required.

    Uploads *filename* or reads from *file* into a new file object (with
    media type *media_type* if given) and returns the associated remote
    file handler. The "name" property of the newly created remote file
    is set to the basename of *filename* or to *file.name* (if it
    exists).

    Examples::

      # Upload from a path
      dxpy.upload_local_file("/home/ubuntu/reads.fastq.gz")
      # Upload from a file-like object
      with open("reads.fastq") as fh:
          dxpy.upload_local_file(file=fh)

    '''
    fd = file if filename is None else open(filename, 'rb')
    try:
        file_size = os.fstat(fd.fileno()).st_size
    except:
        file_size = 0

    def can_be_mmapd(fd):
        if not hasattr(fd, "fileno"):
            return False
        mode = os.fstat(fd.fileno()).st_mode
        return not (stat.S_ISCHR(mode) or stat.S_ISFIFO(mode))

    def read(num_bytes):
        """
        Returns a string or mmap'd data containing the next num_bytes of
        the file, or up to the end if there are fewer than num_bytes
        left.
        """
        # If file cannot be mmap'd (e.g. is stdin, or a fifo), fall back
        # to doing an actual read from the file.
        if not can_be_mmapd(fd):
            return fd.read(num_bytes)

        bytes_available = max(file_size - offset, 0)
        if bytes_available == 0:
            return b""

        return mmap.mmap(fd.fileno(), min(num_bytes, bytes_available), offset=offset, access=mmap.ACCESS_READ)

    def report_progress(handler, num_bytes):
        handler._num_bytes_transmitted += num_bytes
        if file_size > 0:
            ticks = int(round((handler._num_bytes_transmitted / float(file_size)) * num_ticks))
            percent = int(round((handler._num_bytes_transmitted / float(file_size)) * 100))

            fmt = "[{done}{pending}] Uploaded {done_bytes:,} of {total:,} bytes ({percent}%) {name}"
            sys.stderr.write(fmt.format(done='=' * (ticks - 1) + '>' if ticks > 0 else '',
                                        pending=' ' * (num_ticks - ticks),
                                        done_bytes=handler._num_bytes_transmitted,
                                        total=file_size,
                                        percent=percent,
                                        name=filename if filename is not None else ''))
            sys.stderr.flush()
            sys.stderr.write("\r")
            sys.stderr.flush()

    def get_new_handler(filename):
        # Set a reasonable name for the file if none has been set
        # already
        creation_kwargs = kwargs.copy()
        if 'name' not in kwargs:
            if filename is not None:
                creation_kwargs['name'] = os.path.basename(filename)
            else:
                # Try to get filename from file-like object
                try:
                    local_file_name = file.name
                except AttributeError:
                    pass
                else:
                    creation_kwargs['name'] = os.path.basename(local_file_name)

        # Use 'a' mode because we will be responsible for closing the file
        # ourselves later (if requested).
        return new_dxfile(mode='a', media_type=media_type, write_buffer_size=write_buffer_size,
                             expected_file_size=file_size, file_is_mmapd=file_is_mmapd, **creation_kwargs)

    retries = 0
    max_retries = 2
    file_is_mmapd = hasattr(fd, "fileno")

    if write_buffer_size is None:
        write_buffer_size=dxfile.DEFAULT_BUFFER_SIZE

    # APPS-650 file upload would occasionally fail due to some parts not being uploaded correctly. This will try to re-upload in case this happens.
    while retries <= max_retries:
        retries += 1

        if use_existing_dxfile:
            handler = use_existing_dxfile
        else:
            handler = get_new_handler(filename)

        # For subsequent API calls, don't supply the dataobject metadata
        # parameters that are only needed at creation time.
        _, remaining_kwargs = dxpy.DXDataObject._get_creation_params(kwargs)

        num_ticks = 60
        offset = 0

        handler._ensure_write_bufsize(**remaining_kwargs)

        handler._num_bytes_transmitted = 0

        if show_progress:
            report_progress(handler, 0)

        while True:
            buf = read(handler._write_bufsize)
            offset += len(buf)

            if len(buf) == 0:
                break

            handler.write(buf,
                          report_progress_fn=report_progress if show_progress else None,
                          multithread=multithread,
                          **remaining_kwargs)

        handler.flush(report_progress_fn=report_progress if show_progress else None, **remaining_kwargs)

        if show_progress:
            sys.stderr.write("\n")
            sys.stderr.flush()
        try:
            handler.wait_until_parts_uploaded()
        except DXError:
            if show_progress:
                logger.warning("File {} was not uploaded correctly!".format(filename))
            if retries > max_retries:
                raise
            if show_progress:
                logger.warning("Retrying...({}/{})".format(retries, max_retries))
            continue
        if filename is not None:
            fd.close()
        break

    if not keep_open:
        handler.close(block=wait_on_close, report_progress_fn=report_progress if show_progress else None, **remaining_kwargs)

    return handler

def upload_string(to_upload, media_type=None, keep_open=False, wait_on_close=False, **kwargs):
    """
    :param to_upload: String to upload into a file
    :type to_upload: string
    :param media_type: Internet Media Type
    :type media_type: string
    :param keep_open: If False, closes the file after uploading
    :type keep_open: boolean
    :param wait_on_close: If True, waits for the file to close
    :type wait_on_close: boolean
    :returns: Remote file handler
    :rtype: :class:`~dxpy.bindings.dxfile.DXFile`

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`.

    Uploads the data in the string *to_upload* into a new file object
    (with media type *media_type* if given) and returns the associated
    remote file handler.

    """

    # Use 'a' mode because we will be responsible for closing the file
    # ourselves later (if requested).
    handler = new_dxfile(media_type=media_type, mode='a', **kwargs)

    # For subsequent API calls, don't supply the dataobject metadata
    # parameters that are only needed at creation time.
    _, remaining_kwargs = dxpy.DXDataObject._get_creation_params(kwargs)
    handler.write(to_upload, **remaining_kwargs)

    if not keep_open:
        handler.close(block=wait_on_close, **remaining_kwargs)

    return handler

def list_subfolders(project, path, recurse=True):
    '''
    :param project: Project ID to use as context for the listing
    :type project: string
    :param path: Subtree root path
    :type path: string
    :param recurse: Return a complete subfolders tree
    :type recurse: boolean

    Returns a list of subfolders for the remote *path* (included to the result) of the *project*.

    Example::

        list_subfolders("project-xxxx", folder="/input")

    '''
    project_folders = dxpy.get_handler(project).describe(input_params={'folders': True})['folders']
    # If path is '/', return all folders
    # If path is '/foo', return '/foo' and all subfolders of '/foo/'
    if recurse:
        return (f for f in project_folders if path == '/' or (f == path or f.startswith(path + '/')))
    else:
        return (f for f in project_folders if f.startswith(path) and '/' not in f[len(path)+1:])

def download_folder(project, destdir, folder="/", overwrite=False, chunksize=dxfile.DEFAULT_BUFFER_SIZE,
                    show_progress=False, **kwargs):
    '''
    :param project: Project ID to use as context for this download.
    :type project: string
    :param destdir: Local destination location
    :type destdir: string
    :param folder: Path to the remote folder to download
    :type folder: string
    :param overwrite: Overwrite existing files
    :type overwrite: boolean

    Downloads the contents of the remote *folder* of the *project* into the local directory specified by *destdir*.

    Example::

        download_folder("project-xxxx", "/home/jsmith/input", folder="/input")

    '''

    def ensure_local_dir(d):
        if not os.path.isdir(d):
            if os.path.exists(d):
                raise DXFileError("Destination location '{}' already exists and is not a directory".format(d))
            logger.debug("Creating destination directory: '%s'", d)
            os.makedirs(d)

    def compose_local_dir(d, remote_folder, remote_subfolder):
        suffix = remote_subfolder[1:] if remote_folder == "/" else remote_subfolder[len(remote_folder) + 1:]
        if os.sep != '/':
            suffix = suffix.replace('/', os.sep)
        return os.path.join(d, suffix) if suffix != "" else d

    normalized_folder = folder.strip()
    if normalized_folder != "/" and normalized_folder.endswith("/"):
        normalized_folder = normalized_folder[:-1]
    if normalized_folder == "":
        raise DXFileError("Invalid remote folder name: '{}'".format(folder))
    normalized_dest_dir = os.path.normpath(destdir).strip()
    if normalized_dest_dir == "":
        raise DXFileError("Invalid destination directory name: '{}'".format(destdir))
    # Creating target directory tree
    remote_folders = list(list_subfolders(project, normalized_folder, recurse=True))
    if len(remote_folders) <= 0:
        raise DXFileError("Remote folder '{}' not found".format(normalized_folder))
    remote_folders.sort()
    for remote_subfolder in remote_folders:
        ensure_local_dir(compose_local_dir(normalized_dest_dir, normalized_folder, remote_subfolder))

    # Downloading files
    describe_input = dict(fields=dict(folder=True,
                                      name=True,
                                      id=True,
                                      parts=True,
                                      size=True,
                                      drive=True,
                                      md5=True))

    # A generator that returns the files one by one. We don't want to materialize it, because
    # there could be many files here.
    files_gen = dxpy.search.find_data_objects(classname='file', state='closed', project=project,
                                              folder=normalized_folder, recurse=True, describe=describe_input)
    if files_gen is None:
        # In python 3, the generator can be None, and iterating on it
        # will cause an error.
        return

    # Now it is safe, in both python 2 and 3, to iterate on the generator
    for remote_file in files_gen:
        local_filename = os.path.join(compose_local_dir(normalized_dest_dir,
                                                        normalized_folder,
                                                        remote_file['describe']['folder']),
                                      remote_file['describe']['name'])
        if os.path.exists(local_filename) and not overwrite:
            raise DXFileError(
                "Destination file '{}' already exists but no overwrite option is provided".format(local_filename)
            )
        logger.debug("Downloading '%s/%s' remote file to '%s' location",
                     ("" if remote_file['describe']['folder'] == "/" else remote_file['describe']['folder']),
                     remote_file['describe']['name'],
                     local_filename)
        download_dxfile(remote_file['describe']['id'],
                        local_filename,
                        chunksize=chunksize,
                        project=project,
                        show_progress=show_progress,
                        describe_output=remote_file['describe'],
                        **kwargs)
