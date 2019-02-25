"""
This module contains utilities for file upload and download.

This module has a set of functions for file uploads and file downloads. File
downloads can be done in blocking or non-blocking mode. If non-blocking,
a delegator.Command object is returned, which can be used for additional functionality
such as piping the file contents to another command, e.g.

```
from dxpy.sugar import xfer
cmd1 = xfer.simple_download_file("file-XXX", block=False)
cmd2 = delegator.run(["bwa", "mem], block=False)
xfer.chain(cmd1, cmd2, block=True)
```

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

subprocess.call("bwa mem {reference_tgz} {fastqs} > output.bam".format(**local_files))

with Uploader() as uploader:
  uploader.enqueue_file("output_bam", "output.bam", skip_compress=True)
  uploader.enqueue_file("output_bai", "output.bam.bai", skip_compress=True)
  return uploader.wait()
```
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
import concurrent.futures
import os
import re
import sys

import delegator
import dxpy
from dxpy.sugar import info


SPECIAL_RE = re.compile(r"[^\w.]")


def upload_file(filename, skip_compress=False, **kwargs):
    """
    Upload a file, optionally compressing it.

    Args:
        filename: The file to upload.
        skip_compress: Whether to skip gzip compression.
        kwargs: Additional kwargs to the upload function.

    Returns:
        A dxlink or DXFile.
    """
    if skip_compress:
        return simple_upload_file(filename, **kwargs)
    else:
        return compress_and_upload_file(filename, **kwargs)


def simple_upload_file(filename, remote_filename=None, return_handler=False, **kwargs):
    """
    Upload a file and return a link.

    Args:
        filename: Local filename.
        remote_filename (str): Optional, output filename to upload fn to.
        return_handler: Whether to return a DXFile handler.
        **kwargs: Additional arguments to dxpy.upload_local_file

    Returns:
        A dxlink
    """
    if remote_filename:
        folder, name = os.path.split(remote_filename)
        kwargs["name"] = name
        if folder:
            kwargs["folder"] = folder
    info("Uploading file {} to {}", filename, remote_filename or filename)
    file_id = dxpy.upload_local_file(filename, **kwargs)
    dxlink = dxpy.dxlink(file_id)
    if return_handler:
        return dxpy.DXFile(dxlink)
    else:
        return dxlink


def compress_and_upload_file(
    filename, remote_filename=None, compression_level=1, return_handler=False
):
    """
    Gzip and upload a local file.

    Shorthand for running 'gzip' and 'dx upload' using a subprocess on a given
    local file.

    Args:
        filename (str): Local filename.
        remote_filename (str): Optional, output filename to upload fn to.
        compression_level (int): Level of compression between 1 and 9 to compress
            file to. Specify 1 for gzip --fast and 9 for gzip --best. If not
            specified, --fast is assumed.
        return_handler: Whether to return a DXFile handler.

    Returns:
        dict: DNAnexus link pointing to the uploaded file

    Raises:
        ValueError: if compression_level not between 1 and 9
        CalledProcessError: propogated from run_pipe if called command fails
    """
    if remote_filename is None:
        remote_filename = os.path.basename(filename)

    if not remote_filename.endswith(".gz"):
        remote_filename += ".gz"

    # check that compression level is valid
    if compression_level < 1 or compression_level > 9:
        raise ValueError("Compression level must be between 1 and 9")

    if filename.endswith(".gz"):
        cmd = ["dx", "upload", "--brief", "--path", remote_filename, filename]
        file_id = delegator.run(cmd).out
    else:
        gzip_cmd = ["gzip", "-{0}".format(compression_level), "-c", filename]
        upload_cmd = ["dx", "upload", "--brief", "--path", remote_filename, "-"]
        file_id = delegator.chain(gzip_cmd, upload_cmd).out

    dxlink = dxpy.dxlink(file_id)
    if return_handler:
        return dxpy.DXFile(dxlink)
    else:
        return dxlink


def archive_and_upload_files(
    filenames, prefix=None, compression_level=1, chdir=None, return_handler=False
):
    """
    Archive and upload one or more files.

    Shorthand for running 'tar', 'gzip', and 'dx upload' using subprocess on a
    list of local files/directories.

    Args:
        filenames (str or list of str): = Local filenames or directories.
        prefix (str) = name to give to output tar archive.
        compression_level (int) = Level of compression between 1 and 9 to
            compress tar to. Specify 1 for gzip --fast and 9 for gzip --best.
            If not specified, --fast is assumed. If None, no compression is
            performed, i.e. the output is a .tar file.
        chdir: If a path, change to this directory; if None and filenames is
            a directory, will cd to that directory; if True and filenames is
            a directory, same behavior as None, otherwise an error is raised;
            if False, no cd. (tar -C option).
        return_handler: Whether to return a DXFile handler.

    Returns:
        dict: DNAnexus link or DXFile pointing to the uploaded tar archive

    Raises:
        ValueError: if compression_level not between 1 and 9
        CalledProcessError: propogated from run_pipe if called command fails
    """
    is_dir = False
    if isinstance(filenames, basestring):
        is_dir = os.path.isdir(filenames)
        filenames = [filenames]
    if prefix is None:
        if len(filenames) == 1:
            prefix = os.path.basename(filenames[0])
        else:
            raise ValueError("'prefix' must be specified with multiple filenames")

    if compression_level is not None and (
        compression_level < 1 or compression_level > 9
    ):
        raise ValueError("Compression level must be between 1 and 9")

    with temp() as names_file:
        with open(names_file, "wt") as out:
            out.writelines(filenames)

        ext = "tar.gz" if compression_level else "tar"
        outfile = "{}.{}".format(prefix, ext)

        tar_cmd = ["tar"]
        if chdir is not False and not isinstance(chdir, str):
            if is_dir:
                chdir = filenames[0]
            elif chdir is True:
                raise ValueError(
                    "chdir is True but {} is not a directory".format(filenames[0])
                )
            else:
                chdir = None
        if chdir:
            tar_cmd.extend(["-C", chdir])
        tar_cmd.extend(["-cvf", "-", "--files-from", names_file])
        cmds = [tar_cmd]
        if compression_level:
            cmds.append(["gzip", "-{0}".format(compression_level)])
        cmds.append(["dx", "upload", "--brief", "--destination", outfile, "-"])
        file_id = delegator.chain(*cmds).out
        dxlink = dxpy.dxlink(file_id)

    if return_handler:
        return dxpy.DXFile(dxlink)
    else:
        return dxlink


def download_file(
    remote_file, skip_decompress=False, skip_unpack=False, remote_filename=None,
    local_filename=None, output_dir=None, block=True
):
    """
    Download an unzip a gzip file.

    Shorthand for running dx download on a given input_file dx file link.
    Additionally usse subprocess to decompress and/or untar the file
    automatically based on the name suffix of the file provided.

    Args:
        remote_file (dict or str): DNAnexus link or file-id of file to download
        remote_filename (str): Name to use for the input filename, if different than
            the name of the input_file. If not provided, platform filename is used.
        skip_decompress (bool): Whether to skip decompressing files of type *.gz
        skip_unpack (bool): Whether to skip unpacking archive files (.tar.*)
        local_filename: Local file where the data is to be saved.
        output_dir (str): Download file to a specific directory (default is the current
            directory).
        block: Wait for the download to complete before returning.

    Notes:
        Supported filetypes: *.tar.gz, *.tgz, *.tar, *.tar.bz2, *.gz.

        The arg skip_unpack only impacts tar files, and the arg skip_decompress only
        impacts non-tar files of type *.gz.

        List input for additional_pipe is highly recommended over string.

        If `create_named_pipe` is set to True, `block` should be set to False unless
        this function is run in a separate thread/process.

    Returns:
        str: filename or local named pipe which file was downloaded to. If `block`
        is False, also returns the Popen for the process running in the background.
    """
    if not isinstance(remote_file, dxpy.DXFile):
        remote_file = dxpy.DXFile(remote_file)
    if remote_filename is None:
        remote_filename = remote_file.name

    if output_dir:
        mkdir_p(output_dir)
    else:
        output_dir = os.getcwd()

    is_tar = ".tar" in remote_filename or remote_filename.endswith(".tgz")
    unpack = is_tar and not skip_unpack
    unzip = not skip_decompress and not is_tar and remote_filename.endswith(".gz")

    if not any((unpack, unzip)):
        return simple_download_file(remote_file, local_filename, output_dir, block)

    dl_func = download_and_unpack_archive if unpack else download_and_decompress_file
    return dl_func(
        remote_file,
        remote_filename,
        local_filename,
        output_dir,
        block
    )


def simple_download_file(
    remote_file, local_filename=None, output_dir=None, block=True, **kwargs
):
    """
    Download a file.

    Args:
        remote_file (dxpy.DXFile): The file object.
        local_filename: The local_filename, or None to use the input filename.
        output_dir: The output directory, or None to use the current directory.
        block: Wait for the download to complete before returning. Ignored if
            create_named_pipe=True.

    Returns:
        The output filename. Also returns the running download process if
        `block=False`.
    """
    if local_filename is None:
        local_filename = SPECIAL_RE.sub("", remote_file.name)
    if output_dir:
        local_filename = os.path.join(output_dir, local_filename)

    info("Downloading file {} to {}", remote_file.get_id(), local_filename)

    cmd = ["dx", "download", remote_file.get_id(), "-o", local_filename]

    if block:
        delegator.run(cmd, block=True, **kwargs)
        info(
            "Completed downloading file {} to {}", remote_file.get_id(), local_filename
        )
        return local_filename
    else:
        proc = delegator.run(cmd, block=False, **kwargs)
        return local_filename, proc


def download_and_unpack_archive(
    remote_file, input_filename=None, local_filename=None, output_dir=None, block=True
):
    """
    Download and unpack a tar file, which may optionally be gzip-compressed.

    Args:
        remote_file (dxpy.DXFile): DNAnexus link or file-id of file to download
        input_filename (str): Name to use for the input filename, if different than
            the name of the input_file. If not provided, platform filename is used.
        local_filename: Local filename/dirname. If not None, this file/directory must
            exist after unpacking the archive or an error is raised.
        output_dir (str): Download file to a specific directory (default is the current
            directory).
        block: Wait for the download to complete before returning.

    Returns:
        The path of the unpacked file/directory. Also returns the running process if
        `block=False`.
    """
    if input_filename is None:
        input_filename = remote_file.name

    tar_cmd = ["tar", "--no-same-owner", "-C", output_dir]
    if input_filename.endswith(".tar.gz") or input_filename.endswith(".tgz"):
        tar_cmd.append("-z")
        endlen = 7
    elif input_filename.endswith(".tar.bz2"):
        tar_cmd.append("-j")
        endlen = 8
    elif input_filename.endswith(".tar"):
        endlen = 4
    else:
        raise ValueError("Unsupported file type: {}".format(input_filename))
    tar_cmd.extend(["-xvf", "-"])

    if local_filename:
        return_filename = local_filename
    else:
        return_filename = input_filename[:-endlen]
    return_filename = SPECIAL_RE.sub("", return_filename)
    local_filename = "tar_output_{0}".format(return_filename)
    if output_dir:
        return_filename = os.path.join(output_dir, return_filename)

    tar_cmd.extend([">", local_filename])

    cmds = [["dx", "download", remote_file.get_id(), "-o", "-"], tar_cmd]

    info("Downloading file {} to {} using command {}".format(
        remote_file.get_id(), return_filename, cmds
    ))
    info("Saving command stdout to intermediate file {}".format(local_filename))

    proc = delegator.chain(*cmds)

    if block:
        proc.block()
        return return_filename
    else:
        return return_filename, proc


def download_and_decompress_file(
    remote_file, input_filename=None, local_filename=None, output_dir=None, block=True
):
    """
    Download and decompress a gzipped file.

    Args:
        remote_file (dxpy.DXFile): DNAnexus link or file-id of file to download
        input_filename (str): Name to use for the input filename, if different than
            the name of the input_file. If not provided, platform filename is used.
        local_filename: Local filename/dirname. If not None, this file/directory must
            exist after unpacking the archive or an error is raised.
        output_dir (str): Download file to a specific directory (default is the current
            directory).
        block: Wait for the download to complete before returning.

    Returns:
        The path of the decompressed file. Also returns the running download process if
        `use_fifo=True`.
    """
    if input_filename is None:
        input_filename = remote_file.name

    if local_filename is None:
        local_filename = input_filename[:-3]
    if output_dir:
        local_filename = os.path.join(output_dir, local_filename)

    cmds = [
        ["dx", "download", remote_file.get_id(), "-o", "-"],
        ["gunzip", ">", local_filename]
    ]

    info(
        "Downloading file {} to {} using command {}",
        remote_file.get_id(), local_filename, cmds
    )

    proc = delegator.chain(*cmds)

    if block:
        proc.block()
        return local_filename
    else:
        return local_filename, proc


class DataTransferExecutor(concurrent.futures.ThreadPoolExecutor):
    """
    Abstract subclass of :class:`concurrent.futures.ThreadPoolExecutor` that manages
    processing files in a multithreaded manner.

    Args:
        io_function: Function to call to perform the data transefer.
        max_num_parallel_transfers: Maximum number of parallel threads.
    """
    def __init__(self, io_function, max_num_parallel_transfers=None):
        super(DataTransferExecutor, self).__init__(
            max_workers=num_cores(max_num_parallel_transfers)
        )
        self._io_function = io_function
        self._queue = None

    def enqueue_file(self, param_name, filespec, **kwargs):
        """
        Add a file to the queue associated with `name`.

        Args:
            param_name: Name associated with file.
            filespec: File to upload/download.
            **kwargs: Additional kwargs to pass to `self._io_function`.
        """
        self._enqueue(param_name, [filespec], _is_list=False, **kwargs)

    def enqueue_list(self, param_name, files, **kwargs):
        """
        Add a list of files to the queue associated with `name`.

        Args:
            param_name: Name associated with the file list.
            files: List of files.
            **kwargs: Additional kwargs to pass to `self._io_function`.
        """
        self._enqueue(param_name, files, _is_list=True, **kwargs)

    def enqueue_dict(self, files, **kwargs):
        """
        Add a dict of files to the queue.

        Args:
            files: Dict with file values.
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
            new_futures = self._submit(files, _start_index=len(futures), **kwargs)
            if len(new_futures) != len(files):
                raise RuntimeError(
                    "Number of futures returned differs from number of files "
                    "submitted; {} != {}".format(len(new_futures), len(files))
                )
            futures.update(new_futures)
            self._queue[param_name] = (futures, True)
        else:
            self._queue[param_name] = (self._submit(files, **kwargs), _is_list)

    def _submit(self, files, _start_index=0, **kwargs):
        """
        Submit one or more files to be uploaded/downloaded.

        Args:
            files: List of files to download.
            **kwargs: Additional kwargs to pass to `download_file`.

        Returns:
            A dict with Future keys and values of tuple (file index, file).
        """
        return {
            self.submit(self._io_function, _file, **kwargs): (i, _file)
            for i, _file in enumerate(files, _start_index)
        }

    def __call__(self, *files, **kwargs):
        """Convenience method to enqueue files and wait for results.

        Args:
            files: Files to process. Can be a sequence of files, or
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
            info("Interrupted by user")
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

            for future in concurrent.futures.as_completed(
                futures, timeout=sys.maxint
            ):
                i, _file = futures[future]
                try:
                    future_results[i] = future.result()
                except Exception:
                    info("{} generated an exception", _file)
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
        self, files, _start_index=0, skip_compress=False, archive=False, **kwargs
    ):
        if archive:
            compression_level = None if skip_compress else 1
            return {
                self.submit(
                    archive_and_upload_files, files,
                    compression_level=compression_level,
                    **kwargs
                ): (0, ",".join(files))
            }
        else:
            return super(Uploader, self)._submit(
                files, _start_index=_start_index, skip_compress=skip_compress,
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


# Helpers - these belong in other modules

import contextlib
import errno
import multiprocessing
import tempfile


def num_cores(max_cores=None):
    """
    Gets the number of cores available, up to `max_cores`.

    Args:
        max_cores: The max number of cores to use.

    Returns:
        The minimum of max_cores and the number of available CPU cores.
    """
    max_available = multiprocessing.cpu_count()
    if max_cores:
        return min(max_cores, max_available)
    else:
        return max_available


@contextlib.contextmanager
def temp(*args, **kwargs):
    """Context manager that yields a temp file name and deletes the file
    before exiting.

    Args:
        *args: positional arguments passed to mkstemp
        **kwargs: keyword arguments passed to mkstemp

    Examples:
        >>> with temp() as fn:
        >>>     with open(fn, "wt") as out:
        >>>         out.write("foo")
    """
    _, fname = tempfile.mkstemp(*args, **kwargs)
    try:
        yield fname
    finally:
        if os.path.exists(fname):
            os.remove(fname)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def chain(cmd1, cmd2, block=False):
    """
    Chain commands together.

    Args:
        cmd1: A delegator.Command - the left side of the pipe
        cmd2: A delegator.Command - the right side of the pipe
        block: Whether to wait for the last command to complete
    """
    cmd2.send(cmd1.out, end="")
    cmd2.subprocess.sendeof()
    if block:
        cmd2.block()
