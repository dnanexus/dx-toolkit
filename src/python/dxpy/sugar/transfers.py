"""
This module contains utilities for file upload and download. File downloads can be done in blocking 
or non-blocking mode.

This module also provides the Uploader and Downloader classes for managing multiple concurrent up/
downloads. Individual files, lists of files, and dicts of files can be queued for up/download with 
different settings. Each enqueue event is associated with a name. Calling the `wait()` method 
blocks until all up/downloads have completed and returns a dict mapping names to either local paths 
(for Downloader) or dxlinks (for Uploader).

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

# Copyright (C) 2013-2021 DNAnexus, Inc.
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
import copy
import multiprocessing
import os
from pathlib import Path
import psutil
import re
from typing import Dict, Generic, Iterable, List, Optional, Tuple, TypeVar, Union

import dxpy
from dxpy.utils.resolver import data_obj_pattern
from dxpy.sugar import context, get_log, processing


LOG = get_log(__name__)
SPECIAL_RE = re.compile(r"[^\w\-.]")
"""Used based on recommendation here: https://superuser.com/a/748264/70028."""
MAX_READ_SIZE = (1024 * 1024 * 1024 * 2) - 1
"""Maximum number of bytes that can be read from a file at once. The limit here is due to a known 
bug in some versions of python on macOS: https://bugs.python.org/issue24658.
"""
NUM_CPU = multiprocessing.cpu_count()
MAX_PARALLEL = min(NUM_CPU, 8)  # this is the limit imposed by dxpy


def upload_file(
    path: Path, skip_compress: bool = False, **kwargs
) -> Union[dict, dxpy.DXFile]:
    """
    Uploads a file, optionally compressing it.

    Args:
        path: The file to upload.
        skip_compress: Whether to skip gzip compression.
        kwargs: Additional kwargs to the upload function.

    Returns:
        A dxlink or DXFile.
    """
    if skip_compress:
        return simple_upload_file(path, **kwargs)
    else:
        return compress_and_upload_file(path, **kwargs)


def simple_upload_file(
    local_path: Path,
    name: Optional[str] = None,
    folder: str = "/",
    return_handler: bool = False,
    project: Optional[str] = None,
    wait_on_close: bool = False,
    max_part_size: Optional[int] = None,
) -> Union[dict, dxpy.DXFile]:
    """
    Uploads a file and return a link.

    Args:
        local_path: Local filename.
        name: Optional, remote filename to upload file to.
        folder: Optional, remote folder to upload file to.
        return_handler: Whether to return a DXFile handler.
        project: The project ID to upload to, if not the currently selected project.
        wait_on_close: Whether to block until the file has closed.
        max_part_size: Optional, maximum file part size, defaults to project value. Maybe limited
            by the amount of available memory.

    Returns:
        DNAnexus link or DXFile pointing to the uploaded file (depending on
        the value of `return_handler`).
    """
    kwargs = {
        "wait_on_close": wait_on_close,
        "write_buffer_size": _get_max_part_size(max_part_size, MAX_PARALLEL, project),
    }

    if name:
        kwargs["name"] = name
    if not folder:
        folder = "/"
    elif not folder.startswith("/"):
        folder = f"/{folder}"
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
    local_path: Path,
    name: Optional[str] = None,
    folder: str = "/",
    compression_level: int = 1,
    return_handler: bool = False,
    project: Optional[str] = None,
    wait_on_close: bool = False,
    compression_type: str = "gz",
    max_part_size: Optional[int] = None,
) -> Union[dict, dxpy.DXFile]:
    """
    Gzip and upload a local file.

    Shorthand for running 'gzip' and 'dx upload' using a subprocess on a given local file.

    Args:
        local_path: Local filename.
        name: Optional, remote filename to upload file to.
        folder: Optional, remote folder to upload file to.
        compression_level: Level of compression between 1 and 9 to compress file to. Specify 1 for
            gzip --fast and 9 for gzip --best. If not specified, --fast is assumed.
        return_handler: Whether to return a DXFile handler.
        project: The project ID to upload to, if not the currently selected project.
        wait_on_close: Whether to block until the file has closed.
        compression_type: Compression method; one of 'gz', 'bz2'.
        max_part_size: Optional, maximum file part size, defaults to project value. Maybe limited
            by the amount of available memory.

    Returns:
        DNAnexus link or DXFile pointing to the uploaded file (depending on the value of
        `return_handler`).

    Raises:
        ValueError: if compression_level not between 1 and 9
        CalledProcessError: propogated from `run` if called command fails
    """
    ext = f".{compression_type}"
    if name is None:
        name = local_path.name
    if not name.endswith(ext):
        name = f"{name}{ext}"

    if not folder:
        folder = "/"
    else:
        if not folder.startswith("/"):
            folder = f"/{folder}"
        if not folder.endswith("/"):
            folder = f"{folder}/"

    remote_path = f"{folder}{name}"
    if project:
        remote_path = f"{project}:{remote_path}"

    if compression_level < 1 or compression_level > 9:
        raise ValueError("Compression level must be between 1 and 9")

    max_part_size = _get_max_part_size(max_part_size, MAX_PARALLEL, project)

    if local_path.name.endswith(ext):
        cmd = f"dx upload --brief --buffer-size {max_part_size} --path {name} {local_path}"
        file_id = processing.sub(cmd)
    else:
        exe = "bzip2" if compression_type == "bz2" else "gzip"
        zip_cmd = f"{exe} -{compression_level} -c {local_path}"
        upload_cmd = (
            f"dx upload --brief --buffer-size {max_part_size} --path {remote_path} "
            f"{'--wait' if wait_on_close else ''} -"
        )
        file_id = processing.sub([zip_cmd, upload_cmd])

    return _wrap_file_id(file_id, return_handler)


def tar_and_upload_files(
    local_paths: Union[Path, Iterable[Path]],
    prefix: Optional[str] = None,
    folder: str = "/",
    compression_level: Optional[int] = 1,
    chdir: Optional[Path] = None,
    return_handler: bool = False,
    project: Optional[str] = None,
    wait_on_close: bool = False,
    method: str = "gz",
    max_part_size: Optional[int] = None,
) -> Union[dict, dxpy.DXFile]:
    """
    Archive and upload one or more files.

    Shorthand for running 'tar', 'gzip', and 'dx upload' using subprocess on a list of local files/directories.

    Args:
        local_paths: = Local filenames or directories.
        prefix = Name to give to output tar archive. Must be provided unless `filenames` is of
            length 1, in which case the prefix will be the same as that of the single filename.
        folder: Optional, remote folder to upload file to.
        compression_level: Level of compression between 1 and 9 to compress tar to. Specify 1 for
            gzip --fast and 9 for gzip --best. If not specified, --fast is assumed. If None or 0,
            no compression is performed, i.e. the output is a .tar file.
        chdir: Change to this directory before tarring files (tar -C option).
        return_handler: Whether to return a DXFile handler.
        project The project ID to upload to, if not the currently selected project.
        wait_on_close: Whether to block until the file has closed.
        method: Compression method; one of 'gz', 'bz2'.
        max_part_size: Optional, maximum file part size, defaults to project value. Maybe limited
            by the amount of available memory.

    Returns:
        DNAnexus link or DXFile pointing to the uploaded tar archive (depending on the value of
        `return_handler`).

    Raises:
        ValueError: if compression_level not between 1 and 9
        CalledProcessError: propogated from run_pipe if called command fails
    """
    if isinstance(local_paths, Path):
        local_paths = [local_paths]

    if prefix is None:
        if len(local_paths) == 1:
            prefix = local_paths[0].name
        else:
            raise ValueError("'prefix' must be specified with multiple filenames")

    if not folder:
        folder = "/"
    else:
        if not folder.startswith("/"):
            folder = f"/{folder}"
        if not folder.endswith("/"):
            folder = f"{folder}/"

    if compression_level == 0:
        compression_level = None
    elif compression_level is not None and (
        compression_level < 1 or compression_level > 9
    ):
        raise ValueError("Compression level must be between 1 and 9")

    if compression_level:
        ext = f"tar.{method}"
    else:
        ext = "tar"

    remote_path = f"{folder}{prefix}.{ext}"
    if project:
        remote_path = f"{project}:{remote_path}"

    max_part_size = _get_max_part_size(max_part_size, MAX_PARALLEL, project)

    with context.tmpfile() as names_file:
        with open(names_file, "wt") as out:
            out.write("\n".join(str(p) for p in local_paths))

        tar_cmd = (
            f"tar {f'-C {chdir}' if chdir else ''} cvf - --files-from {names_file}"
        )
        cmds = [tar_cmd]

        if compression_level:
            exe = "bzip2" if method == "bz2" else "gzip"
            cmds.append(f"{exe} -{compression_level}")

        cmds.append(
            f"dx upload --brief --buffer-size {max_part_size} --path {remote_path} "
            f"{'--wait' if wait_on_close else ''} -"
        )

        file_id = processing.sub(cmds)

    return _wrap_file_id(file_id, return_handler)


def _get_max_part_size(
    max_part_size: int = None, max_threads: int = 1, project_id: Optional[str] = None
) -> int:
    # Determine the absolute maximum value we can use
    # TODO: cache value by project ID
    container = dxpy.get_handler(project_id or dxpy.WORKSPACE_ID)
    desc = container.describe(input_params={"fields": {"fileUploadParameters": True}})
    abs_max_part_size = min(
        desc["fileUploadParameters"]["maximumPartSize"], MAX_READ_SIZE
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
    return max(max_part_size, dxpy.DEFAULT_BUFFER_SIZE)


def _wrap_file_id(
    file_id: str, return_handler: bool, project: Optional[str] = None
) -> Union[dict, dxpy.DXFile]:
    """
    Given a file ID, create either a link or a `dxpy.DXFile` object, depending on the value of
    `return_handler`.

    Args:
        file_id: The file ID to wrap.
        return_handler: Whether to return a `dxpy.DXFile` object.
        project: The project ID (defaults to currently selected project).

    Returns:
        A `dxpy.DXFile` object, depending on the value of `return_handler`.
    """
    try:
        dxpy.verify_string_dxid(file_id, "file")
    except:
        # dxpy prints warnings (e.g. about readline support) even when '--brief' is used, so we may
        # need to parse out the file ID from stdout.
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


def _file_handler_as_link(dxfile: dxpy.DXFile) -> dict:
    file_id = dxfile.get_id()
    project = dxfile.describe()["project"]
    if project is None or not project.startswith("project-"):
        return dxpy.dxlink(file_id)
    else:
        return dxpy.dxlink(file_id, project_id=project)


def download_file(
    remote_file: Union[str, dict],
    skip_decompress: bool = False,
    skip_unpack: bool = False,
    remote_filename: Optional[str] = None,
    local_filename: Optional[str] = None,
    output_dir: Optional[Path] = None,
    project: Optional[str] = None,
    block: bool = True,
    list_contents: bool = True,
) -> Union[Path, List[Path], Tuple[Path, processing.Processes]]:
    """
    Downloads a file.

    Shorthand for running dx download on a given input_file dx file link. Additionally use
    subprocess to decompress and/or untar the file automatically based on the name suffix of the
    file provided.

    Args:
        remote_file: DNAnexus link or file-id of file to download.
        remote_filename: Name to use for the input filename, if different than the name of the
            input_file. If not provided, platform filename is used.
        skip_decompress: Whether to skip decompressing files of type *.gz.
        skip_unpack: Whether to skip unpacking archive files (.tar.*).
        local_filename: Local file where the data is to be saved.
        output_dir: Download file to a specific directory (default is the current directory).
        project: The ID of the project that contains the file, if it is not the currently selected
            project and is not specified in the remote file object/link.
        block: Wait for the download to complete before returning.
        list_contents: If the file is an archive, whether to return a list of files unpacked from the archive.

    Notes:
        Supported filetypes: *.tar.gz, *.tgz, *.tar, *.tar.bz2, *.tbz2, *.gz, *.bz2.

        The arg skip_unpack only impacts tar files, and the arg skip_decompress only
        impacts non-tar files of type *.gz.

    Returns:
        Path: filename or local named pipe which file was downloaded to. If `block`
        is False, also returns the Popen for the process running in the background.
    """
    remote_file = _as_dxfile(remote_file, project)

    if remote_filename is None:
        remote_filename = remote_file.describe()["name"]

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    else:
        output_dir = Path.cwd()

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

    if unpack:
        return download_and_unpack_archive(
            remote_file,
            remote_filename,
            local_filename,
            output_dir,
            block,
            list_contents,
        )
    elif unzip:
        return download_and_decompress_file(
            remote_file, remote_filename, local_filename, output_dir, block
        )
    else:
        return simple_download_file(remote_file, local_filename, output_dir, block)


def simple_download_file(
    dx_file_or_link: Union[dxpy.DXFile, dict],
    local_path: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    project: Optional[str] = None,
    block: bool = True,
    **kwargs,
) -> Union[Path, Tuple[Path, processing.Processes]]:
    """
    Downloads a file.

    Args:
        dx_file_or_link: The file to download.
        local_path: The local_filename, or None to use the input filename.
        output_dir: The output directory, or None to use the current directory.
        project: The ID of the project that contains the file, if it is not the currently selected
            project and is not specified in the remote file object/link.
        block: Wait for the download to complete before returning. Ignored if
            create_named_pipe=True.
        kwargs: Additional arguments passed to `dxpy.sugar.processing.processing.run()`.

    Returns:
        The output filename, if `block is True`, otherwise a :class:`dxpy.sugar.processing.Processes` object.
    """
    dxfile = _as_dxfile(dx_file_or_link, project)

    if local_path is None:
        local_path = Path(SPECIAL_RE.sub("", dxfile.name))
    if output_dir:
        if local_path.is_absolute():
            raise ValueError(
                f"cannot provide 'output_dir' with absolute path {local_path}"
            )
        local_path = output_dir / local_path

    LOG.info("Downloading file %s to %s", dxfile.get_id(), local_path)

    if block:
        dxpy.download_dxfile(dxfile, local_path)
        LOG.info("Completed downloading file %s to %s", dxfile.get_id(), local_path)
        return local_path
    else:
        cmd = f"dx download {dxfile.get_id()} -o {local_path}"
        return local_path, processing.run(cmd, block=block, **kwargs)


def download_and_unpack_archive(
    dx_file_or_link: Union[dict, dxpy.DXFile],
    input_filename: Optional[str] = None,
    local_path: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    project: Optional[str] = None,
    block: bool = True,
    list_contents: bool = True,
) -> Union[Path, List[Path], Tuple[Path, processing.Processes]]:
    """
    Downloads and unpacks a tar file, which may optionally be gzip-compressed.

    Args:
        dx_file_or_link: DNAnexus link or file-id of file to download.
        input_filename: Name to use for the input filename, if different than the name of the
            input_file. If not provided, platform filename is used.
        local_path: Local filename/dirname. If not None, this file/directory must exist after
            unpacking the archive or an error is raised.
        output_dir: Download file to a specific directory (default is the current directory).
        project: The ID of the project that contains the file, if it is not the currently selected
            project and is not specified in the remote file object/link.
        block: Wait for the download to complete before returning.
        list_contents: Return contents of the archive (True) or its path (False)

    Returns:
        The return value depends on the values of `block` and `list_contents`.

        * `block=True, list_contents=True`: a list of `Path`s of the unpacked files.
        * `block=True, list_contents=False`: a `Path` to the output directory.
        * `block=False`: a tuple with the first element being a `Path` to a file that will contain a list of the unpacked files after the process completes and a :class:`dxpy.sugar.processing.Processes` object that can be polled for the status of the process. `list_contents` is ignored.
    """
    dxfile = _as_dxfile(dx_file_or_link, project)

    if input_filename is None:
        input_filename = dxfile.describe()["name"]
    if not output_dir:
        output_dir = Path.cwd()
    else:
        output_dir = output_dir.absolute()

    tar_opts = []
    if input_filename.endswith(".tar.gz"):
        tar_opts.append("-z")
        ext_len = 7
    elif input_filename.endswith(".tgz"):
        tar_opts.append("-z")
        ext_len = 4
    elif input_filename.endswith(".tar.bz2"):
        tar_opts.append("-j")
        ext_len = 8
    elif input_filename.endswith(".tbz2"):
        tar_opts.append("-j")
        ext_len = 5
    elif input_filename.endswith(".tar"):
        ext_len = 4
    else:
        raise ValueError(f"Unsupported file type: {input_filename}")

    cmds = [
        f"dx download {dxfile.get_id()} -o -",
        f"tar --no-same-owner -C {output_dir} {' '.join(tar_opts)} -xvf -",
    ]

    if local_path:
        suffix = local_path.name
    else:
        suffix = SPECIAL_RE.sub("", input_filename[:-ext_len])
    file_list_path = Path(f"tar_output_{suffix}")

    LOG.info(
        "Downloading file %s using command %s and saving command stdout to intermediate file %s",
        dxfile.get_id(),
        cmds,
        file_list_path,
    )

    result = processing.run(cmds, stdout=file_list_path, block=block)

    if not block:
        return file_list_path, result

    LOG.info("Completed downloading file %s", dxfile.get_id())

    if list_contents:
        with open(file_list_path, "rt") as inp:
            return [output_dir / filename.rstrip() for filename in inp]
    elif local_path:
        # If a local_filename was provided, make sure it exists
        if not local_path.is_absolute():
            local_path = output_dir / local_path
        if not local_path.exists():
            raise ValueError(
                f"Expected file {local_path} does not exist after untarring file {dxfile.get_id()}"
            )
        return local_path
    else:
        return output_dir / suffix


def download_and_decompress_file(
    dx_file_or_link: Union[dict, dxpy.DXFile],
    input_filename: Optional[str] = None,
    local_path: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    project: Optional[str] = None,
    block: bool = True,
) -> Union[Path, Tuple[Path, processing.Processes]]:
    """
    Download and decompress a gzipped file.

    Args:
        dx_file_or_link : DNAnexus link or file-id of file to download.
        input_filename: Name to use for the input filename, if different than the name of the
            input_file. If not provided, platform filename is used.
        local_path: Local file/dir path. If not `None`, this file/directory must exist after
            unpacking the archive or an error is raised.
        output_dir: Download file to a specific directory (default is the current directory).
        project: The ID of the project that contains the file, if it is not the currently selected
            project and is not specified in the remote file object/link.
        block: Wait for the download to complete before returning.

    Returns:
        The path of the decompressed file, if `block is True`, otherwise a :class:`dxpy.sugar.
        processing.Processes` object.
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
        raise ValueError(f"Unsupported compression method for file {input_filename}")

    if local_path is None:
        local_path = Path(input_filename[:-ext_len])
    if output_dir:
        if local_path.is_absolute():
            raise ValueError(
                f"cannot provide 'output_dir' with absolute path {local_path}"
            )
        local_path = output_dir / local_path

    cmds = [f"dx cat {dxfile.get_id()}", exe]

    LOG.info(
        "Downloading file %s to %s using command %s",
        dxfile.get_id(),
        local_path,
        cmds,
    )

    result = processing.run(cmds, stdout=local_path, block=block)

    if block:
        LOG.info("Completed downloading file %s to %s", dxfile.get_id(), local_path)
        return local_path
    else:
        return local_path, result


def _as_dxfile(
    fileobj: Union[dict, dxpy.DXFile], project: Optional[str] = None
) -> dxpy.DXFile:
    """
    Converts a link to a `dxpy.DXFile` object.

    Args:
        fileobj: Link to convert; if already a `dxpy.DXFile` it is returned without modification.
        project: The ID of the project containing the file.

    Returns:
        A `dxpy.DXFile` object.
    """
    if isinstance(fileobj, dxpy.DXFile):
        return fileobj
    else:
        return dxpy.get_handler(fileobj, project)


F = TypeVar("F")
R = TypeVar("R")


class DataTransferExecutor(concurrent.futures.ThreadPoolExecutor, Generic[F, R]):
    """
    Abstract subclass of :class:`concurrent.futures.ThreadPoolExecutor` that manages processing
    files in a multithreaded manner.

    Args:
        io_function: Function to call to perform the data transfer.
        max_parallel: Maximum number of parallel threads.
        default_kwargs: Keyword arguments to pass to every enqueue call.
    """

    def __init__(
        self, io_function, max_parallel: Optional[int] = None, **default_kwargs
    ):
        super(DataTransferExecutor, self).__init__(max_parallel)
        self._io_function = io_function
        self._default_kwargs = default_kwargs
        self._queue = None

    def enqueue_file(self, param_name: str, file: F, **kwargs):
        """
        Add a file to the queue associated with `name`.

        Args:
            param_name: Name associated with file.
            file: File to upload/download.
            **kwargs: Additional kwargs to pass to `self._io_function`.
        """
        self._enqueue(param_name, [file], _is_list=False, **kwargs)

    def enqueue_list(self, param_name: str, files: Iterable[F], **kwargs):
        """
        Add a list of files to the queue associated with `name`.

        Args:
            param_name: Name associated with the file list.
            files: List of files.
            **kwargs: Additional kwargs to pass to `self._io_function`.
        """
        self._enqueue(param_name, files, _is_list=True, **kwargs)

    def enqueue_dict(self, files: Dict[str, Union[F, Iterable[F]]], **kwargs):
        """
        Add a dict of files to the queue.

        Args:
            files: Dict with file values.
            **kwargs: Additional kwargs to pass to `self._io_function`.
        """
        for param_name, param_files in files.items():
            is_list = isinstance(param_files, (list, tuple, set))
            if not is_list:
                param_files = [param_files]
            self._enqueue(param_name, param_files, is_list, **kwargs)

    def _enqueue(self, param_name: str, files: Iterable[F], _is_list: bool, **kwargs):
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

    def _submit(
        self, files: Iterable[F], _is_list: bool, _start_index: int = 0, **kwargs
    ) -> Tuple[dict, bool]:
        """
        Submit one or more files to be uploaded/downloaded.

        Args:
            files: Iterable of files to upload/download.
            _is_list: Whether `files` represents a list of files.
            _start_index: The starting index, if `files` is being appended to an existing list of
                files.
            **kwargs: Additional kwargs to pass to `download_file`.

        Returns:
            A tuple:
            * dict with Future keys and values of tuple (file index, file).
            * boolean, whether the result should be considered a list
        """
        submit_kwargs = self._get_submit_kwargs(kwargs)
        return (
            {
                self.submit(self._io_function, _file, **submit_kwargs): (i, _file)
                for i, _file in enumerate(files, _start_index)
            },
            _is_list,
        )

    def _get_submit_kwargs(self, kwargs: dict) -> dict:
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

    def __call__(self, *files: F, **kwargs) -> Union[List[R], Dict[str, R]]:
        """Convenience method to enqueue files and wait for results.

        Args:
            files: Files to process. Can be a sequence of files, or dict with file values.
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
            # Call os._exit() in case of KeyboardInterrupt. Otherwise, the atexit registered
            # handler in concurrent.futures.thread will run, and issue blocking join() on all
            # worker threads, requiring us to listen to events in worker threads in order to enable
            # timely exit in response to Ctrl-C.
            LOG.info("Interrupted by user")
            os._exit(os.EX_IOERR)

    def wait(self) -> Dict[str, Union[R, List[R]]]:
        """
        Waits for all downloads to complete.

        Returns:
            A dict mapping name to files.
        """
        _queue = self._queue
        if _queue is None:
            return {}
        else:
            self._queue = None

        def _get_result(name, futures, is_list) -> Union[R, List[R]]:
            if futures is None:
                raise RuntimeError(f"No futures for {name}")
            if len(futures) == 0:
                return [] if is_list else None

            future_results = [None] * len(futures)

            for future in concurrent.futures.as_completed(futures):
                i, _file = futures[future]
                try:
                    future_results[i] = future.result()
                except Exception:
                    LOG.exception("%s generated an exception", _file)
                    raise

            return future_results if is_list else future_results[0]

        return dict(
            (name, _get_result(name, futures, is_list))
            for name, (futures, is_list) in _queue.items()
        )


class Uploader(DataTransferExecutor[Path, dxpy.DXFile]):
    """
    Upload manager.

    Args:
        kwargs: Keyword args to pass to `DataTransferExecutor.__init__`
    """

    def __init__(self, **kwargs):
        super(Uploader, self).__init__(upload_file, **kwargs)

    def _submit(
        self,
        files: Iterable[Path],
        _is_list: bool,
        _start_index: int = 0,
        **kwargs,
    ):
        submit_kwargs = self._get_submit_kwargs(kwargs)
        if submit_kwargs.pop("archive", False):
            compression_level = None if submit_kwargs.get("skip_compress") else 1
            return (
                {
                    self.submit(
                        tar_and_upload_files,
                        files,
                        compression_level=compression_level,
                        **submit_kwargs,
                    ): (0, ",".join(str(f) for f in files))
                },
                False,
            )
        else:
            return super(Uploader, self)._submit(
                files,
                _is_list,
                _start_index=_start_index,
                **kwargs,
            )


class Downloader(DataTransferExecutor[Union[dict, dxpy.DXFile], Path]):
    """
    Download manager.

    Args:
        kwargs: Keyword args to pass to DataTransferExecutor.__init__.
            max_num_parallel_transfers
    """

    def __init__(self, **kwargs):
        super(Downloader, self).__init__(download_file, **kwargs)
