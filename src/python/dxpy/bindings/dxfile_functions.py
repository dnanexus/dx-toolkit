# Copyright (C) 2013-2015 DNAnexus, Inc.
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

from __future__ import (print_function, unicode_literals)

import os, sys, math, mmap, stat

import dxpy
from . import dxfile, DXFile

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

def new_dxfile(mode=None, write_buffer_size=dxfile.DEFAULT_BUFFER_SIZE, **kwargs):
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
    dx_file = DXFile(mode=mode, write_buffer_size=write_buffer_size)
    dx_file.new(**kwargs)
    return dx_file

def download_dxfile(dxid, filename, chunksize=dxfile.DEFAULT_BUFFER_SIZE, append=False, show_progress=False,
                    project=None, **kwargs):
    '''
    :param dxid: Remote file ID
    :type dxid: string
    :param filename: Local filename
    :type filename: string
    :param append: If True, appends to the local file (default is to truncate local file if it exists)
    :type append: boolean

    Downloads the remote file with object ID *dxid* and saves it to
    *filename*.

    Example::

        download_dxfile("file-xxxx", "localfilename.fastq")

    '''

    def print_progress(bytes_downloaded, file_size):
        num_ticks = 60

        effective_file_size = file_size or 1
        if bytes_downloaded > effective_file_size:
            effective_file_size = bytes_downloaded

        ticks = int(round((bytes_downloaded / float(effective_file_size)) * num_ticks))
        percent = int(round((bytes_downloaded / float(effective_file_size)) * 100))

        fmt = "[{done}{pending}] Downloaded {done_bytes:,}{remaining} bytes ({percent}%) {name}"
        sys.stderr.write(fmt.format(done=('=' * (ticks - 1) + '>') if ticks > 0 else '',
                                    pending=' ' * (num_ticks - ticks),
                                    done_bytes=bytes_downloaded,
                                    remaining=' of {size:,}'.format(size=file_size) if file_size else "",
                                    percent=percent,
                                    name=filename))
        sys.stderr.flush()
        sys.stderr.write("\r")
        sys.stderr.flush()

    file_size = None
    _bytes = 0

    mode = 'ab' if append else 'wb'
    with DXFile(dxid, mode='r', project=project, read_buffer_size=chunksize) as dxfile, open(filename, mode) as fd:
        if show_progress:
            print_progress(0, None)
        while True:
            file_content = dxfile.read(chunksize, **kwargs)
            if file_size is None:
                file_size = dxfile._file_length

            if show_progress:
                _bytes += len(file_content)
                print_progress(_bytes, file_size)

            if len(file_content) == 0:
                if show_progress:
                    sys.stderr.write("\n")
                break

            fd.write(file_content)

def _get_buffer_size_for_file(file_size, file_is_mmapd=False):
    """Returns an upload buffer size that is appropriate to use for a file
    of size file_size. If file_is_mmapd is True, the size is further
    constrained to be suitable for passing to mmap.

    """
    # Raise buffer size (for files exceeding DEFAULT_BUFFER_SIZE * 10k
    # bytes) in order to prevent us from exceeding 10k parts limit.
    min_buffer_size = int(math.ceil(float(file_size) / 10000))
    buffer_size = max(dxfile.DEFAULT_BUFFER_SIZE, min_buffer_size)
    if file_size >= 0 and file_is_mmapd:
        # For mmap'd uploads the buffer size additionally must be a
        # multiple of the ALLOCATIONGRANULARITY.
        buffer_size = int(math.ceil(float(buffer_size) / mmap.ALLOCATIONGRANULARITY)) * mmap.ALLOCATIONGRANULARITY
    if buffer_size * 10000 < file_size:
        raise AssertionError('part size is not large enough to complete upload')
    if file_is_mmapd and buffer_size % mmap.ALLOCATIONGRANULARITY != 0:
        raise AssertionError('part size will not be accepted by mmap')
    return buffer_size

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
    buffer_size = _get_buffer_size_for_file(file_size, file_is_mmapd=hasattr(fd, "fileno"))

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
        handler = new_dxfile(mode='a', media_type=media_type, write_buffer_size=buffer_size, **creation_kwargs)

    # For subsequent API calls, don't supply the dataobject metadata
    # parameters that are only needed at creation time.
    _, remaining_kwargs = dxpy.DXDataObject._get_creation_params(kwargs)

    num_ticks = 60
    offset = 0

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
