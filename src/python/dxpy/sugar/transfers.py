"""
This module contains utilities for file upload and download. File downloads can be
done in blocking or non-blocking mode.

This module also provides the Uploader and Downloader classes for managing multiple
concurrent up/downloads. Individual files, lists of files, and dicts of files can
be queued for up/download with different settings. Each enqueue event is associated
with a name. Calling the `wait()` method blocks until all up/downloads have completed
and returns a dict mapping names to either local paths (for Downloader) or dxlinks
(for Uploader).

```
with Downloader() as downloader:
  downloader.enqueue_file("reference_tgz", "file-XXX")
  downloader.enqueue_list("fastqs", ["file-YYY", "file-ZZZ"], skip_decompress=True)
  local_files = downloader.wait()

subprocess.call(
    "bwa mem {reference_tgz} {fastqs}".format(**local_files),
    output_filename="output.bam"
)

with Uploader() as uploader:
  uploader.enqueue_file("output_bam", "output.bam", skip_compress=True)
  uploader.enqueue_file("output_bai", "output.bam.bai", skip_compress=True)
  return uploader.wait()
```

TODO: switch to using dxpy vs calls to dx whenever possible
"""

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
from __future__ import print_function, unicode_literals, division, absolute_import
import concurrent.futures
import contextlib
import copy
import logging
import multiprocessing
import os
import re
import sys
import tempfile

import psutil

import dxpy
from dxpy.compat import basestring, makedirs
from dxpy.utils.resolver import data_obj_pattern
from dxpy.sugar import processing as proc


LOG = logging.getLogger()
SPECIAL_RE = re.compile(r"[^\w.]")
MAX_READ_SIZE = (1024 * 1024 * 1024 * 2) - 1
"""Maximum number of bytes that can be read from a file at once. The limit here is
due to a known bug in some versions of python on macOS:
https://bugs.python.org/issue24658.
"""


def upload_file(filename, skip_compress=False, **kwargs):
    """
    Upload a file, optionally compressing it.

    Args:
        filename (str): The file to upload.
        skip_compress (bool): Whether to skip gzip compression.
        kwargs: Additional kwargs to the upload function.

    Returns:
        A dxlink or DXFile.
    """
    if skip_compress:
        return simple_upload_file(filename, **kwargs)
    else:
        return compress_and_upload_file(filename, **kwargs)


def simple_upload_file(
    local_path,
    name=None,
    folder="/",
    return_handler=False,
    project=None,
    wait_on_close=False,
    max_part_size=None,
    max_parallel=1
):
    """
    Upload a file and return a link.

    Args:
        local_path (str): Local filename.
        name (str): Optional, remote filename to upload file to.
        folder (str): Optional, remote folder to upload file to.
        return_handler (bool): Whether to return a DXFile handler.
        project (str): The project ID to upload to, if not the currently selected
            project.
        wait_on_close (bool): Whether to block until the file has closed.
        max_part_size (int): Optional, maximum file part size, defaults to project
            value. Maybe limited by the amount of available memory.
        max_parallel (int): Max number of parallel download threads; this is used
            to limit memory usage, and thus may cause max_part_size to be reduced.

    Returns:
        DNAnexus link or DXFile pointing to the uploaded file (depending on
        the value of `return_handler`).
    """
    kwargs = {
        "wait_on_close": wait_on_close,
        "write_buffer_size": _get_max_part_size(
            max_part_size, max_parallel, project
        )
    }

    if name:
        kwargs["name"] = name
    if not folder:
        folder = "/"
    elif not folder.startswith("/"):
        folder = "/{}".format(folder)
    kwargs["folder"] = folder
    if project:
        kwargs["project"] = project
    LOG.info("Uploading file %s to %s", local_path, name or local_path)
    handler = dxpy.upload_local_file(local_path, **kwargs)
    if return_handler:
        return handler
    else:
        return _file_handler_as_link(handler)


def compress_and_upload_file(
    local_path,
    name=None,
    folder="/",
    compression_level=1,
    return_handler=False,
    project=None,
    wait_on_close=False,
    compression_type="gz",
    max_part_size=None,
    max_parallel=1
):
    """
    Gzip and upload a local file.

    Shorthand for running 'gzip' and 'dx upload' using a subprocess on a given
    local file.

    Args:
        local_path (str): Local filename.
        name (str): Optional, remote filename to upload file to.
        folder (str): Optional, remote folder to upload file to.
        compression_level (int): Level of compression between 1 and 9 to compress
            file to. Specify 1 for gzip --fast and 9 for gzip --best. If not
            specified, --fast is assumed.
        return_handler (bool): Whether to return a DXFile handler.
        project (str): The project ID to upload to, if not the currently selected
            project.
        wait_on_close (bool): Whether to block until the file has closed.
        compression_type (str): Compression method; one of 'gz', 'bz2'.
        max_part_size (int): Optional, maximum file part size, defaults to project
            value. Maybe limited by the amount of available memory.
        max_parallel (int): Max number of parallel download threads; this is used
            to limit memory usage, and thus may cause max_part_size to be reduced.

    Returns:
        DNAnexus link or DXFile pointing to the uploaded file (depending on
        the value of `return_handler`).

    Raises:
        ValueError: if compression_level not between 1 and 9
        CalledProcessError: propogated from run_pipe if called command fails
    """
    ext = ".{}".format(compression_type)
    if name is None:
        name = os.path.basename(local_path)
    if not name.endswith(ext):
        name += ext

    if not folder:
        folder = "/"
    elif not folder.startswith("/"):
        folder = "/{}".format(folder)

    remote_path = "{}/{}".format(folder, name)
    if project:
        remote_path = "{}:{}".format(project, remote_path)

    # check that compression level is valid
    if compression_level < 1 or compression_level > 9:
        raise ValueError("Compression level must be between 1 and 9")

    max_part_size = _get_max_part_size(max_part_size, max_parallel, project)

    if local_path.endswith(ext):
        cmd = [
            "dx", "upload", "--brief", "--buffer-size", str(max_part_size),
            "--path", name, local_path
        ]
        file_id = proc.run_cmd(cmd).output
    else:
        exe = "bzip2" if compression_type == "bz2" else "gzip"
        zip_cmd = [exe, "-{0}".format(compression_level), "-c", local_path]
        upload_cmd = [
            "dx", "upload", "--brief", "--buffer-size", str(max_part_size),
            "--path", remote_path
        ]
        if wait_on_close:
            upload_cmd.append("--wait")
        upload_cmd.append("-")
        file_id = proc.chain_cmds([zip_cmd, upload_cmd], shell=True).output

    return _wrap_file_id(file_id, return_handler)


def tar_and_upload_files(
    local_paths,
    prefix=None,
    folder="/",
    compression_level=1,
    chdir=None,
    return_handler=False,
    project=None,
    wait_on_close=False,
    method="gz",
    max_part_size=None,
    max_parallel=1
):
    """
    Archive and upload one or more files.

    Shorthand for running 'tar', 'gzip', and 'dx upload' using subprocess on a
    list of local files/directories.

    Args:
        local_paths (str or list of str): = Local filenames or directories.
        prefix (str) = Name to give to output tar archive. Must be provided
            unless `filenames` is of length 1, in which case the prefix will be
            the same as that of the single filename.
        folder (str): Optional, remote folder to upload file to.
        compression_level (int) = Level of compression between 1 and 9 to
            compress tar to. Specify 1 for gzip --fast and 9 for gzip --best.
            If not specified, --fast is assumed. If None or 0, no compression is
            performed, i.e. the output is a .tar file.
        chdir (str): Change to this directory before tarring files (tar -C option).
        return_handler (bool): Whether to return a DXFile handler.
        project (str): The project ID to upload to, if not the currently selected
            project.
        wait_on_close (bool): Whether to block until the file has closed.
        method (str): Compression method; one of 'gz', 'bz2'.
        max_part_size (int): Optional, maximum file part size, defaults to project
            value. Maybe limited by the amount of available memory.
        max_parallel (int): Max number of parallel download threads; this is used
            to limit memory usage, and thus may cause max_part_size to be reduced.

    Returns:
        DNAnexus link or DXFile pointing to the uploaded tar archive (depending on
        the value of `return_handler`).

    Raises:
        ValueError: if compression_level not between 1 and 9
        CalledProcessError: propogated from run_pipe if called command fails
    """
    if isinstance(local_paths, basestring):
        local_paths = [local_paths]

    if prefix is None:
        if len(local_paths) == 1:
            prefix = os.path.basename(local_paths[0])
        else:
            raise ValueError("'prefix' must be specified with multiple filenames")

    if not folder:
        folder = "/"
    elif not folder.startswith("/"):
        folder = "/{}".format(folder)

    if compression_level == 0:
        compression_level = None
    elif compression_level is not None and (
        compression_level < 1 or compression_level > 9
    ):
        raise ValueError("Compression level must be between 1 and 9")

    if compression_level:
        zip_ext = ".{}".format(method)
        ext = "tar" + zip_ext
    else:
        ext = "tar"

    remote_path = "{}/{}.{}".format(folder, prefix, ext)
    if project:
        remote_path = "{}:{}".format(project, remote_path)

    max_part_size = _get_max_part_size(max_part_size, max_parallel, project)

    with _tmpfile() as names_file:
        with open(names_file, "wt") as out:
            out.write("\n".join(local_paths))

        tar_cmd = ["tar"]
        if chdir:
            tar_cmd.extend(["-C", chdir])
        tar_cmd.extend(["cvf", "-", "--files-from", names_file])
        cmds = [tar_cmd]

        if compression_level:
            exe = "bzip2" if method == "bz2" else "gzip"
            cmds.append([exe, "-{}".format(compression_level)])

        upload_cmd = [
            "dx", "upload", "--brief", "--buffer-size", max_part_size,
            "--path", remote_path
        ]
        if wait_on_close:
            upload_cmd.append("--wait")
        upload_cmd.append("-")
        cmds.append(upload_cmd)

        file_id = proc.chain_cmds(cmds, shell=True).output

    return _wrap_file_id(file_id, return_handler)


def _get_max_part_size(max_part_size=None, max_threads=1, project_id=None):
    if not project_id:
        project_id = dxpy.PROJECT_CONTEXT_ID

    # Determine the absolute maximum value we can use
    # TODO: cache value by project ID
    project = dxpy.DXProject(project_id)
    desc = project.describe(input_params={"fields": {"fileUploadParameters": True}})
    abs_max_part_size = min(
        desc["fileUploadParameters"]["maximumPartSize"],
        MAX_READ_SIZE
    )

    # Set to min of desired value and abs max value
    if not max_part_size or max_part_size < 0:
        max_part_size = abs_max_part_size
    else:
        max_part_size = min(max_part_size, abs_max_part_size)

    # Further limit max part size by available memory
    available_mem = psutil.virtual_memory().available
    available_mem_per_thread = available_mem // max_threads
    max_part_size = min(max_part_size, available_mem_per_thread)

    # Use the dxpy default part size (100 MB) as the minimum
    max_part_size = max(max_part_size, dxpy.DEFAULT_BUFFER_SIZE)

    return max_part_size


def _wrap_file_id(file_id, return_handler, project=None):
    """
    Given a file ID, create either a link or a `dxpy.DXFile` object, depending
    on the value of `return_handler`.

    Args:
        file_id (str): The file ID to wrap.
        return_handler (bool): Whether to return a `dxpy.DXFile` object.
        project (str): The project ID (defaults to currently selected project).

    Returns:
        A `dxpy.DXFile` object, depending on the value of `return_handler`.
    """
    try:
        dxpy.verify_string_dxid(file_id, "file")
    except:
        # dxpy prints warnings (e.g. about readline support) even when '--brief'
        # is used, so we may need to parse out the file ID from stdout.
        match = data_obj_pattern.search(file_id)
        if match:
            file_id = match.group()
        else:
            raise ValueError("Invalid file ID: {}".format(file_id))

    handler = dxpy.DXFile(file_id, project=project)

    if return_handler:
        return handler
    else:
        return _file_handler_as_link(handler)


def _file_handler_as_link(dxfile):
    file_id = dxfile.get_id()
    project = dxfile.describe()["project"]
    if project is None or not project.startswith("project-"):
        return dxpy.dxlink(file_id)
    else:
        return dxpy.dxlink(file_id, project_id=project)


def download_file(
    remote_file,
    skip_decompress=False,
    skip_unpack=False,
    remote_filename=None,
    local_filename=None,
    output_dir=None,
    project=None,
    block=True,
):
    """
    Download and unzip a gzip file.

    Shorthand for running dx download on a given input_file dx file link.
    Additionally use subprocess to decompress and/or untar the file
    automatically based on the name suffix of the file provided.

    Args:
        remote_file (dict or str): DNAnexus link or file-id of file to download
        remote_filename (str): Name to use for the input filename, if different than
            the name of the input_file. If not provided, platform filename is used.
        skip_decompress (bool): Whether to skip decompressing files of type *.gz
        skip_unpack (bool): Whether to skip unpacking archive files (.tar.*)
        local_filename (str): Local file where the data is to be saved.
        output_dir (str): Download file to a specific directory (default is the current
            directory).
        project (str): The ID of the project that contains the file, if it is not the
            currently selected project and is not specified in the remote file
            object/link.
        block (bool): Wait for the download to complete before returning.

    Notes:
        Supported filetypes: *.tar.gz, *.tgz, *.tar, *.tar.bz2, *.tbz2, *.gz, *.bz2.

        The arg skip_unpack only impacts tar files, and the arg skip_decompress only
        impacts non-tar files of type *.gz.

    Returns:
        str: filename or local named pipe which file was downloaded to. If `block`
        is False, also returns the Popen for the process running in the background.
    """
    remote_file = _as_dxfile(remote_file, project)

    if remote_filename is None:
        remote_filename = remote_file.describe()["name"]

    if output_dir:
        makedirs(output_dir, exist_ok=True)
    else:
        output_dir = os.getcwd()

    is_tar = (
        ".tar" in remote_filename
        or remote_filename.endswith(".tgz")
        or remote_filename.endswith(".tbz2")
    )
    unpack = is_tar and not skip_unpack
    unzip = (
        not skip_decompress
        and not is_tar
        and (remote_filename.endswith(".gz") or remote_filename.endswith(".bz2"))
    )

    if not any((unpack, unzip)):
        return simple_download_file(remote_file, local_filename, output_dir, block)

    dl_func = download_and_unpack_archive if unpack else download_and_decompress_file
    return dl_func(remote_file, remote_filename, local_filename, output_dir, block)


def simple_download_file(
    dx_file_or_link,
    local_path=None,
    output_dir=None,
    project=None,
    block=True,
    **kwargs
):
    """
    Download a file.

    Args:
        dx_file_or_link (dxpy.DXFile or dxlink): The file to download.
        local_path (str): The local_filename, or None to use the input filename.
        output_dir (str): The output directory, or None to use the current directory.
        project (str): The ID of the project that contains the file, if it is not the
            currently selected project and is not specified in the remote file
            object/link.
        block (bool): Wait for the download to complete before returning. Ignored if
            create_named_pipe=True.
        kwargs: Additional arguments passed to `dxpy.sugar.processing.run_cmd()`.

    Returns:
        The output filename, if `block is True`, otherwise a
        :class:`dxpy.sugar.processing.Processes` object.
    """
    dxfile = _as_dxfile(dx_file_or_link, project)

    if local_path is None:
        local_path = SPECIAL_RE.sub("", dxfile.name)
    if output_dir:
        local_path = os.path.join(output_dir, local_path)

    LOG.info("Downloading file %s to %s", dxfile.get_id(), local_path)

    if block:
        dxpy.download_dxfile(dxfile, local_path)
        LOG.info(
            "Completed downloading file %s to %s", dxfile.get_id(), local_path
        )
        return local_path
    else:
        cmd = ["dx", "download", dxfile.get_id(), "-o", local_path]
        return proc.run_cmd(cmd, block=block, **kwargs)


def download_and_unpack_archive(
    dx_file_or_link,
    input_filename=None,
    local_filename=None,
    output_dir=None,
    project=None,
    block=True,
):
    """
    Download and unpack a tar file, which may optionally be gzip-compressed.

    Args:
        dx_file_or_link (dxpy.DXFile): DNAnexus link or file-id of file to download
        input_filename (str): Name to use for the input filename, if different than
            the name of the input_file. If not provided, platform filename is used.
        local_filename (str): Local filename/dirname. If not None, this file/directory
            must exist after unpacking the archive or an error is raised.
        output_dir (str): Download file to a specific directory (default is the current
            directory).
        project (str): The ID of the project that contains the file, if it is not the
            currently selected project and is not specified in the remote file
            object/link.
        block (bool): Wait for the download to complete before returning.

    Returns:
        The list of unpacked filenames, if `block is True`, otherwise
        a :class:`dxpy.sugar.processing.Processes` object.

        If `local_filename` is provided and `block is False`, the list of unpacked
        filenames has a single element, which is the `local_filename`, converted to
        an absolute path if necessary.
    """
    dxfile = _as_dxfile(dx_file_or_link, project)

    if input_filename is None:
        input_filename = dxfile.describe()["name"]
    if not output_dir:
        output_dir = os.getcwd()
    elif not os.path.isabs(output_dir):
        output_dir = os.path.abspath(output_dir)

    tar_cmd = ["tar", "--no-same-owner", "-C", output_dir]
    if input_filename.endswith(".tar.gz"):
        tar_cmd.append("-z")
        ext_len = 7
    elif input_filename.endswith(".tgz"):
        tar_cmd.append("-z")
        ext_len = 4
    elif input_filename.endswith(".tar.bz2"):
        tar_cmd.append("-j")
        ext_len = 8
    elif input_filename.endswith(".tbz2"):
        tar_cmd.append("-j")
        ext_len = 5
    elif input_filename.endswith(".tar"):
        ext_len = 4
    else:
        raise ValueError("Unsupported file type: {}".format(input_filename))
    tar_cmd.extend(["-x", "-v", "-f", "-"])

    if local_filename:
        suffix = os.path.basename(local_filename)
    else:
        suffix = SPECIAL_RE.sub("", input_filename[:-ext_len])
    file_list_filename = "tar_output_{}".format(suffix)

    cmds = [["dx", "download", dxfile.get_id(), "-o", "-"], tar_cmd]

    LOG.info(
        "Downloading file %s using command %s and saving command stdout to "
        "intermediate file %s", dxfile.get_id(), cmds, file_list_filename
    )

    result = proc.chain_cmds(cmds, stdout=file_list_filename, block=block)

    if not block:
        return result

    LOG.info("Completed downloading file %s", dxfile.get_id())

    if local_filename:
        # If a local_filename was provided, make sure it exists
        if not os.path.isabs(local_filename):
            local_filename = os.path.join(output_dir, local_filename)
        if not os.path.exists(local_filename):
            raise ValueError(
                "Expected file {} does not exist after untarring file {}",
                local_filename, dxfile.get_id()
            )
        return [local_filename]
    else:
        # Otherwise return the list of files that were unpacked from the tar file
        with open(file_list_filename, "rt") as inp:
            return [os.path.join(output_dir, filename.rstrip()) for filename in inp]


def download_and_decompress_file(
    dx_file_or_link,
    input_filename=None,
    local_filename=None,
    output_dir=None,
    project=None,
    block=True,
):
    """
    Download and decompress a gzipped file.

    Args:
        dx_file_or_link (dxpy.DXFile or dxlink): DNAnexus link or file-id of file to
            download.
        input_filename (str): Name to use for the input filename, if different than
            the name of the input_file. If not provided, platform filename is used.
        local_filename (str): Local filename/dirname. If not None, this file/directory
            must exist after unpacking the archive or an error is raised.
        output_dir (str): Download file to a specific directory (default is the current
            directory).
        project (str): The ID of the project that contains the file, if it is not the
            currently selected project and is not specified in the remote file
            object/link.
        block (bool): Wait for the download to complete before returning.

    Returns:
        The path of the decompressed file, if `block is True`, otherwise
        a :class:`dxpy.sugar.processing.Processes` object.
    """
    dxfile = _as_dxfile(dx_file_or_link, project)

    if input_filename is None:
        input_filename = dxfile.describe()["name"]

    if input_filename.endswith(".gz"):
        exe = "gunzip"
        ext_len = 3
    elif input_filename.endswith(".bz2"):
        exe = "bunzip2"
        ext_len = 4
    else:
        raise ValueError(
            "Unsupported compression method for file {}".format(input_filename)
        )

    if local_filename is None:
        local_filename = input_filename[:-ext_len]
    if output_dir:
        local_filename = os.path.join(output_dir, local_filename)

    cmds = [
        ["dx", "download", dxfile.get_id(), "-o", "-"],
        [exe],
    ]

    LOG.info(
        "Downloading file %s to %s using command %s",
        dxfile.get_id(),
        local_filename,
        cmds,
    )

    result = proc.chain_cmds(cmds, stdout=local_filename, block=block)

    if block:
        LOG.info(
            "Completed downloading file %s to %s", dxfile.get_id(), local_filename
        )
        return local_filename
    else:
        return result


def _as_dxfile(fileobj, project=None):
    """
    Convert a link to a `dxpy.DXFile` object.

    Args:
        fileobj (dict or `dxpy.DXFile): Link to convert; if already a `dxpy.DXFile`
            it is returned without modification.
        project (str): The ID of the project containing the file.

    Returns:
        A `dxpy.DXFile` object.
    """
    if isinstance(fileobj, dxpy.DXFile):
        return fileobj
    else:
        return dxpy.get_handler(fileobj, project)


class DataTransferExecutor(concurrent.futures.ThreadPoolExecutor):
    """
    Abstract subclass of :class:`concurrent.futures.ThreadPoolExecutor` that manages
    processing files in a multithreaded manner.

    Args:
        io_function (Callable): Function to call to perform the data transefer.
        max_parallel (int): Maximum number of parallel threads.
        default_kwargs: Keyword arguments to pass to every enqueue call.
    """

    def __init__(self, io_function, max_parallel=None, **default_kwargs):
        super(DataTransferExecutor, self).__init__(
            # TODO: A question for discussion: often times clients just make API calls
            #  and use bandwidth. A lot of the work performed is Network I/O bound. In
            #  practice, people use a heuristic of 2 * num_cores when dealing with
            #  network I/O bound task. For this scenario, uploading/downloading data,
            #  what are good heuristics for ThreadPoolExecutor worker count?
            max_workers=min(multiprocessing.cpu_count(), max_parallel)
        )
        self._io_function = io_function
        self._default_kwargs = default_kwargs
        self._queue = None

    def enqueue_file(self, param_name, filespec, **kwargs):
        """
        Add a file to the queue associated with `name`.

        Args:
            param_name (str): Name associated with file.
            filespec (str): File to upload/download.
            **kwargs: Additional kwargs to pass to `self._io_function`.
        """
        self._enqueue(param_name, [filespec], _is_list=False, **kwargs)

    def enqueue_list(self, param_name, files, **kwargs):
        """
        Add a list of files to the queue associated with `name`.

        Args:
            param_name (str): Name associated with the file list.
            files (list): List of files.
            **kwargs: Additional kwargs to pass to `self._io_function`.
        """
        self._enqueue(param_name, files, _is_list=True, **kwargs)

    def enqueue_dict(self, files, **kwargs):
        """
        Add a dict of files to the queue.

        Args:
            files (dict): Dict with file values.
            **kwargs: Additional kwargs to pass to `self._io_function`.
        """
        for param_name, _files in files.items():
            is_list = type(_files) in {tuple, list}
            if not is_list:
                _files = [_files]
            self._enqueue(param_name, _files, is_list, **kwargs)

    def _enqueue(self, param_name, files, _is_list, **kwargs):
        if self._queue is None:
            self._queue = {}
        if param_name in self._queue:
            futures = self._queue[param_name][0]
            new_futures, _ = self._submit(
                files, _is_list, _start_index=len(futures), **kwargs
            )
            if len(new_futures) != len(files):
                raise RuntimeError(
                    "Number of futures returned differs from number of files "
                    "submitted; {} != {}".format(len(new_futures), len(files))
                )
            futures.update(new_futures)
            self._queue[param_name] = (futures, True)
        else:
            self._queue[param_name] = self._submit(files, _is_list, **kwargs)

    def _submit(self, files, _is_list, _start_index=0, **kwargs):
        """
        Submit one or more files to be uploaded/downloaded.

        Args:
            files (list): List of files to download.
            _is_list (bool): Whether `files` represents a list of files.
            _start_index int): The starting index, if `files` is being appended
                to an existing list of files.
            **kwargs: Additional kwargs to pass to `download_file`.

        Returns:
            A tuple:
            * dict with Future keys and values of tuple (file index, file).
            * boolean, whether the result should be considered a list
        """
        return (
            {
                self.submit(
                    self._io_function, _file, **self._get_submit_kwargs(kwargs)
                ): (i, _file)
                for i, _file in enumerate(files, _start_index)
            },
            _is_list,
        )

    def _get_submit_kwargs(self, kwargs):
        """
        Merge default and call-specific kwargs.

        Args:
            kwargs (dict): Call-specific kwargs.

        Returns:
            dict that is the merger between default and call-specific kwargs.
        """
        submit_kwargs = copy.copy(self._default_kwargs)
        submit_kwargs.update(kwargs)
        return submit_kwargs

    def __call__(self, *files, **kwargs):
        """Convenience method to enqueue files and wait for results.

        Args:
            files (list or dict): Files to process. Can be a sequence of files, or
                dict with file values.
            kwargs: Keyword arguments to pass to the worker function.

        Returns:
            A list or dict of results, depending on the input type.
        """
        try:
            if len(files) == 1 and isinstance(files[0], dict):
                self.enqueue_dict(files[0], **kwargs)
                return self.wait()
            else:
                self.enqueue_list("default", files, **kwargs)
                return self.wait()["default"]
        except KeyboardInterrupt:
            # Call os._exit() in case of KeyboardInterrupt. Otherwise, the atexit
            # registered handler in concurrent.futures.thread will run, and issue
            # blocking join() on all worker threads, requiring us to listen to
            # events in worker threads in order to enable timely exit in response
            # to Ctrl-C.
            LOG.info("Interrupted by user")
            os._exit(os.EX_IOERR)

    def wait(self):
        """
        Wait for all downloads to complete.

        Returns:
            A dict mapping name to files.
        """
        _queue = self._queue
        results = {}
        if _queue is None:
            return results
        else:
            self._queue = None

        for name, (futures, is_list) in _queue.items():
            if not futures:
                raise RuntimeError("No futures for {}".format(name))

            future_results = [None] * len(futures)

            for future in concurrent.futures.as_completed(futures, timeout=sys.maxint):
                i, _file = futures[future]
                try:
                    future_results[i] = future.result()
                except Exception:
                    LOG.exception("%s generated an exception", _file)
                    raise

            results[name] = future_results if is_list else future_results[0]

        return results


class Uploader(DataTransferExecutor):
    """
    Upload manager.

    Args:
        kwargs: Keyword args to pass to DataTransferExecutor.__init__.
            max_num_parallel_transfers
    """

    def __init__(self, **kwargs):
        super(Uploader, self).__init__(upload_file, **kwargs)

    def _submit(
        self,
        files,
        _is_list,
        _start_index=0,
        skip_compress=False,
        archive=False,
        **kwargs
    ):
        if "max_parallel" not in kwargs:
            kwargs["max_parallel"] = self._max_workers
        if archive:
            compression_level = None if skip_compress else 1
            return (
                {
                    self.submit(
                        tar_and_upload_files,
                        files,
                        compression_level=compression_level,
                        **self._get_submit_kwargs(kwargs)
                    ): (0, ",".join(files))
                },
                False,
            )
        else:
            return super(Uploader, self)._submit(
                files,
                _is_list,
                _start_index=_start_index,
                skip_compress=skip_compress,
                **kwargs
            )


class Downloader(DataTransferExecutor):
    """
    Download manager.

    Args:
        kwargs: Keyword args to pass to DataTransferExecutor.__init__.
            max_num_parallel_transfers
    """

    def __init__(self, **kwargs):
        super(Downloader, self).__init__(download_file, **kwargs)


@contextlib.contextmanager
def _tmpfile(*args, **kwargs):
    """
    Create a temporary file, yield it, and delete it before returning.

    Yields:
        A path to a temporary file.

    Notes:
        This method is needed distinct from :class:`tempfile.TemporaryFile` in the
        case where python needs to write to the file and then a subprocess needs
        to read from the file. For now, keep this private to transfers module rather
        than expose it via the context module.
    """
    path = tempfile.mkstemp(*args, **kwargs)[1]
    try:
        yield path
    finally:
        if os.path.exists(path):
            os.remove(path)
