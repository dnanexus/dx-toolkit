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

import dxpy
from .. import logger
from . import dxfile, DXFile
from .dxfile import FILE_REQUEST_TIMEOUT
from ..compat import open
from ..exceptions import DXFileError, DXPartLengthMismatchError, DXChecksumMismatchError, DXIncompleteReadsError
from ..utils import response_iterator

def open_dxfile(dxid, project=None, read_buffer_size=dxfile.DEFAULT_BUFFER_SIZE):
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
    return DXFile(dxid, project=project, read_buffer_size=read_buffer_size)


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
                    project=None, **kwargs):
    '''
    :param dxid: DNAnexus file ID or DXFile (file handler) object
    :type dxid: string or DXFile
    :param filename: Local filename
    :type filename: string
    :param append: If True, appends to the local file (default is to truncate local file if it exists)
    :type append: boolean
    :param project: project to use as context for this download (may affect
            which billing account is billed for this download). If None, no
            project hint is supplied to the API server.
    :type project: str or None


    Downloads the remote file referenced by *dxid* and saves it to *filename*.

    Example::

        download_dxfile("file-xxxx", "localfilename.fastq")

    '''
    # retry the inner loop while there are retriable errors
    def download_with_chunksize(csize):
        part_retry_counter = defaultdict(lambda: 3)
        success = False
        while not success:
            success = _download_dxfile(dxid, filename, part_retry_counter,
                                       chunksize=csize, append=append,
                                       show_progress=show_progress, project=project, **kwargs)
    try:
        download_with_chunksize(chunksize)
    except DXIncompleteReadsError:
        # We were unable to read with large buffers, try again with a small buffer.
        # This can happen if the data source is slow in sending data.
        logger.info("Data source is slow, reducing maximal buffer size to %d, and trying again",
                    dxfile.MIN_BUFFER_SIZE)
        download_with_chunksize(dxfile.MIN_BUFFER_SIZE)


def _download_dxfile(dxid, filename, part_retry_counter,
                     chunksize=dxfile.DEFAULT_BUFFER_SIZE, append=False, show_progress=False,
                     project=None, **kwargs):
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
        percent = int(round((bytes_downloaded / float(effective_file_size)) * 100))

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
        dxfile = DXFile(dxid, mode="r")

    dxfile_desc = dxfile.describe(fields={"parts"}, default_fields=True, **kwargs)
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
        if len(parts) > 1 or end - start + 1 < parts[part_id_to_get]["size"]:
            headers["Range"] = "bytes={}-{}".format(start, end)
        data = dxpy.DXHTTPRequest(url, b"", method="GET", headers=headers, auth=None, jsonify_data=False,
                                  prepend_srv=False, always_retry=True, timeout=FILE_REQUEST_TIMEOUT,
                                  decode_response_body=False)
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
        if fh.mode == "rb+":
            # We already downloaded the beginning of the file, verify that the
            # chunk checksums match the metadata.
            last_verified_part, last_verified_pos, max_verify_chunk_size = None, 0, 1024*1024
            try:
                for part_id in parts_to_get:
                    part_info = parts[part_id]
                    if "md5" not in part_info:
                        raise DXFileError("File {} does not contain part md5 checksums".format(dxfile.get_id()))
                    bytes_to_read = part_info["size"]
                    hasher = hashlib.md5()
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
            cur_part, got_bytes, hasher = None, None, None
            dxfile._ensure_http_threadpool()
            for chunk_part, chunk_data in response_iterator(chunk_requests(), dxfile._http_threadpool):
                if chunk_part != cur_part:
                    verify_part(cur_part, got_bytes, hasher)
                    cur_part, got_bytes, hasher = chunk_part, 0, hashlib.md5()
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
                      wait_on_close=False, use_existing_dxfile=None, show_progress=False, **kwargs):
    '''
    :param filename: Local filename
    :type filename: string
    :param file: File-like object
    :type file: File-like object
    :param media_type: Internet Media Type
    :type media_type: string
    :param keep_open: If False, closes the file after uploading
    :type keep_open: boolean
    :param wait_on_close: If True, waits for the file to close
    :type wait_on_close: boolean
    :param use_existing_dxfile: Instead of creating a new file object, upload to the specified file
    :type use_existing_dxfile: :class:`~dxpy.bindings.dxfile.DXFile`
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

    file_is_mmapd = hasattr(fd, "fileno")

    if use_existing_dxfile:
        handler = use_existing_dxfile
    else:
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
        handler = new_dxfile(mode='a', media_type=media_type, write_buffer_size=dxfile.DEFAULT_BUFFER_SIZE,
                             expected_file_size=file_size, file_is_mmapd=file_is_mmapd, **creation_kwargs)

    # For subsequent API calls, don't supply the dataobject metadata
    # parameters that are only needed at creation time.
    _, remaining_kwargs = dxpy.DXDataObject._get_creation_params(kwargs)

    num_ticks = 60
    offset = 0

    handler._ensure_write_bufsize(**remaining_kwargs)

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
            return fd.read(handler._write_bufsize)

        bytes_available = max(file_size - offset, 0)
        if bytes_available == 0:
            return b""

        return mmap.mmap(fd.fileno(), min(handler._write_bufsize, bytes_available), offset=offset, access=mmap.ACCESS_READ)

    handler._num_bytes_transmitted = 0

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

    if show_progress:
        report_progress(handler, 0)

    while True:
        buf = read(handler._write_bufsize)
        offset += len(buf)

        if len(buf) == 0:
            break

        handler.write(buf, report_progress_fn=report_progress if show_progress else None, **remaining_kwargs)

    if filename is not None:
        fd.close()

    handler.flush(report_progress_fn=report_progress if show_progress else None, **remaining_kwargs)

    if show_progress:
        sys.stderr.write("\n")
        sys.stderr.flush()

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
