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
DXFile Handler
**************

This remote file handler is a Python file-like object.
'''

from __future__ import print_function, unicode_literals, division, absolute_import

import os, sys, logging, traceback, hashlib, copy, time
import math
import mmap
from threading import Lock
from multiprocessing import cpu_count

import dxpy
from . import DXDataObject
from ..exceptions import DXFileError, DXIncompleteReadsError
from ..utils import warn
from ..utils.resolver import object_exists_in_project
from ..compat import BytesIO, basestring


DXFILE_HTTP_THREADS = min(cpu_count(), 8)
MIN_BUFFER_SIZE = 1024*1024
DEFAULT_BUFFER_SIZE = 1024*1024*16
if dxpy.JOB_ID:
    # Increase HTTP request buffer size when we are running within the
    # platform.
    DEFAULT_BUFFER_SIZE = 1024*1024*96

MD5_READ_CHUNK_SIZE = 1024*1024*4
FILE_REQUEST_TIMEOUT = 60


def _validate_headers(headers):
    for key, value in headers.items():
        if not isinstance(key, basestring):
            raise ValueError("Expected key %r of headers to be a string" % (key,))
        if not isinstance(value, basestring):
            raise ValueError("Expected value %r of headers (associated with key %r) to be a string"
                             % (value, key))
    return headers


def _readable_part_size(num_bytes):
    "Returns the file size in readable form."
    B = num_bytes
    KB = float(1024)
    MB = float(KB * 1024)
    GB = float(MB * 1024)
    TB = float(GB * 1024)

    if B < KB:
        return '{0} {1}'.format(B, 'bytes' if B != 1 else 'byte')
    elif KB <= B < MB:
        return '{0:.2f} KiB'.format(B/KB)
    elif MB <= B < GB:
        return '{0:.2f} MiB'.format(B/MB)
    elif GB <= B < TB:
        return '{0:.2f} GiB'.format(B/GB)
    elif TB <= B:
        return '{0:.2f} TiB'.format(B/TB)


def _get_write_buf_size(buffer_size_hint, file_upload_params, expected_file_size, file_is_mmapd=False):
    max_num_parts = file_upload_params['maximumNumParts']
    min_part_size = file_upload_params['minimumPartSize']
    max_part_size = file_upload_params['maximumPartSize']
    max_file_size = file_upload_params['maximumFileSize']

    if expected_file_size is not None and expected_file_size > max_file_size:
        raise DXFileError("Size of file exceeds maximum of {}".format(_readable_part_size(max_file_size)))

    min_buffer_size = min_part_size
    if expected_file_size is not None:
        # Raise buffer size (for files exceeding DEFAULT_BUFFER_SIZE
        # * the maximium parts allowed bytes) in order to prevent us
        # from exceeding the configured parts limit.
        min_buffer_size = max(min_buffer_size, int(math.ceil(float(expected_file_size) / max_num_parts)))
    max_buffer_size = max_part_size

    assert min_buffer_size <= max_buffer_size

    if file_is_mmapd:
        # If file is mmapd, force the eventual result to be a
        # multiple of the allocation granularity by rounding all of
        # buffer_size, min_buffer_size, and max_buffer_size to a
        # nearby multiple of the allocation granularity (below, the
        # final buffer size will be one of these).
        if min_buffer_size % mmap.ALLOCATIONGRANULARITY != 0:
            min_buffer_size += mmap.ALLOCATIONGRANULARITY - min_buffer_size % mmap.ALLOCATIONGRANULARITY
        if max_buffer_size % mmap.ALLOCATIONGRANULARITY != 0:
            max_buffer_size -= max_buffer_size % mmap.ALLOCATIONGRANULARITY
        buffer_size_hint = buffer_size_hint - buffer_size_hint % mmap.ALLOCATIONGRANULARITY
    else:
        buffer_size_hint = buffer_size_hint

    # Use the user-specified hint if it is a permissible size
    # (satisfies API and large enough to upload file of advertised
    # size). Otherwise, select the closest size that is permissible.
    buffer_size = buffer_size_hint
    buffer_size = max(buffer_size, min_buffer_size)
    buffer_size = min(buffer_size, max_buffer_size)

    if expected_file_size is not None and (buffer_size * max_num_parts < expected_file_size):
        raise AssertionError("part size would be too small to upload the requested number of bytes")

    if file_is_mmapd and buffer_size % mmap.ALLOCATIONGRANULARITY != 0:
        raise AssertionError('part size will not be accepted by mmap')

    return buffer_size


class DXFile(DXDataObject):
    '''Remote file object handler.

    :param dxid: Object ID
    :type dxid: string
    :param project: Project ID
    :type project: string
    :param mode: One of "r", "w", or "a" for read, write, and append modes, respectively
    :type mode: string

    .. note:: The attribute values below are current as of the last time
              :meth:`~dxpy.bindings.DXDataObject.describe` was run.
              (Access to any of the below attributes causes
              :meth:`~dxpy.bindings.DXDataObject.describe` to be called
              if it has never been called before.)

    .. py:attribute:: media

       String containing the Internet Media Type (also known as MIME type
       or Content-type) of the file.

    .. automethod:: _new

    '''

    _class = "file"

    _describe = staticmethod(dxpy.api.file_describe)
    _add_types = staticmethod(dxpy.api.file_add_types)
    _remove_types = staticmethod(dxpy.api.file_remove_types)
    _get_details = staticmethod(dxpy.api.file_get_details)
    _set_details = staticmethod(dxpy.api.file_set_details)
    _set_visibility = staticmethod(dxpy.api.file_set_visibility)
    _rename = staticmethod(dxpy.api.file_rename)
    _set_properties = staticmethod(dxpy.api.file_set_properties)
    _add_tags = staticmethod(dxpy.api.file_add_tags)
    _remove_tags = staticmethod(dxpy.api.file_remove_tags)
    _close = staticmethod(dxpy.api.file_close)
    _list_projects = staticmethod(dxpy.api.file_list_projects)

    _http_threadpool_size = DXFILE_HTTP_THREADS
    _http_threadpool = dxpy.utils.get_futures_threadpool(max_workers=_http_threadpool_size)

    NO_PROJECT_HINT = 'NO_PROJECT_HINT'

    @classmethod
    def set_http_threadpool_size(cls, num_threads):
        '''

        .. deprecated:: 0.191.0

        '''
        print('set_http_threadpool_size is deprecated')

    def __init__(self, dxid=None, project=None, mode=None, read_buffer_size=DEFAULT_BUFFER_SIZE,
                 write_buffer_size=DEFAULT_BUFFER_SIZE, expected_file_size=None, file_is_mmapd=False):
        """
        :param dxid: Object ID
        :type dxid: string
        :param project: Project ID
        :type project: string
        :param mode: One of "r", "w", or "a" for read, write, and append
            modes, respectively
        :type mode: string
        :param read_buffer_size: size of read buffer in bytes
        :type read_buffer_size: int
        :param write_buffer_size: hint for size of write buffer in
            bytes. A lower or higher value may be used depending on
            region-specific parameters and on the expected file size.
        :type write_buffer_size: int
        :param expected_file_size: size of data that will be written, if
            known
        :type expected_file_size: int
        :param file_is_mmapd: True if input file is mmap'd (if so, the
            write buffer size will be constrained to be a multiple of
            the allocation granularity)
        :type file_is_mmapd: bool
        """
        DXDataObject.__init__(self, dxid=dxid, project=project)
        if mode is None:
            self._close_on_exit = True
        else:
            if mode not in ['r', 'w', 'a']:
                raise ValueError("mode must be one of 'r', 'w', or 'a'")
            self._close_on_exit = (mode == 'w')
        self._read_buf = BytesIO()
        self._write_buf = BytesIO()

        self._read_bufsize = read_buffer_size

        # Computed lazily later since this depends on the project, and
        # we want to allow the project to be set as late as possible.
        # Call _ensure_write_bufsize to ensure that this is set before
        # trying to read it.
        self._write_bufsize = None

        self._write_buffer_size_hint = write_buffer_size
        self._expected_file_size = expected_file_size
        self._file_is_mmapd = file_is_mmapd

        # These are cached once for all download threads. This saves calls to the apiserver.
        self._download_url, self._download_url_headers, self._download_url_expires = None, None, None

        # This lock protects accesses to the above three variables, ensuring that they would
        # be checked and changed atomically. This protects against thread race conditions.
        self._url_download_mutex = Lock()

        self._request_iterator, self._response_iterator = None, None
        self._http_threadpool_futures = set()

        # Initialize state
        self._pos = 0
        self._file_length = None
        self._cur_part = 1
        self._num_uploaded_parts = 0

    def _new(self, dx_hash, media_type=None, **kwargs):
        """
        :param dx_hash: Standard hash populated in :func:`dxpy.bindings.DXDataObject.new()` containing attributes common to all data object classes.
        :type dx_hash: dict
        :param media_type: Internet Media Type
        :type media_type: string

        Creates a new remote file with media type *media_type*, if given.

        """

        if media_type is not None:
            dx_hash["media"] = media_type

        resp = dxpy.api.file_new(dx_hash, **kwargs)
        self.set_ids(resp["id"], dx_hash["project"])

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.flush()
        if self._close_on_exit and self._get_state() == "open":
            self.close()

    def __del__(self):
        '''
        Exceptions raised here in the destructor are IGNORED by Python! We will try and flush data
        here just as a safety measure, but you should not rely on this to flush your data! We will
        be really grumpy and complain if we detect unflushed data here.

        Use a context manager or flush the object explicitly to avoid this.

        In addition, when this is triggered by interpreter shutdown, the thread pool is not
        available, and we will wait for the request queue forever. In this case, we must revert to
        synchronous, in-thread flushing. We don't know how to detect this condition, so we'll use
        that for all destructor events.

        Neither this nor context managers are compatible with kwargs pass-through (so e.g. no
        custom auth).
        '''
        if not hasattr(self, '_write_buf'):
            # This occurs when there is an exception initializing the
            # DXFile object
            return

        if self._write_buf.tell() > 0 or len(self._http_threadpool_futures) > 0:
            warn("=== WARNING! ===")
            warn("There is still unflushed data in the destructor of a DXFile object!")
            warn("We will attempt to flush it now, but if an error were to occur, we could not report it back to you.")
            warn("Your program could fail to flush the data but appear to succeed.")
            warn("Instead, please call flush() or close(), or use the context managed version (e.g., with open_dxfile(ID, mode='w') as f:)")
        try:
            self.flush(multithread=False)
        except Exception as e:
            warn("=== Exception occurred while flushing accumulated file data for %r" % (self._dxid,))
            traceback.print_exception(*sys.exc_info())
            raise

    def __iter__(self):
        _buffer = self.read(self._read_bufsize)
        done = False
        while not done:
            if b"\n" in _buffer:
                lines = _buffer.splitlines()
                for i in range(len(lines) - 1):
                    yield lines[i]
                _buffer = lines[len(lines) - 1]
            else:
                more = self.read(self._read_bufsize)
                if more == b"":
                    done = True
                else:
                    _buffer = _buffer + more
        if _buffer:
            yield _buffer

    next = next
    __next__ = next
            
    def set_ids(self, dxid, project=None):
        '''
        :param dxid: Object ID
        :type dxid: string
        :param project: Project ID
        :type project: string

        Discards the currently stored ID and associates the handler with
        *dxid*. As a side effect, it also flushes the buffer for the
        previous file object if the buffer is nonempty.
        '''
        if self._dxid is not None:
            self.flush()

        DXDataObject.set_ids(self, dxid, project)

        # Reset state
        self._pos = 0
        self._file_length = None
        self._cur_part = 1
        self._num_uploaded_parts = 0

    def seek(self, offset, from_what=os.SEEK_SET):
        '''
        :param offset: Position in the file to seek to
        :type offset: integer

        Seeks to *offset* bytes from the beginning of the file.  This is a no-op if the file is open for writing.

        The position is computed from adding *offset* to a reference point; the reference point is selected by the
        *from_what* argument. A *from_what* value of 0 measures from the beginning of the file, 1 uses the current file
        position, and 2 uses the end of the file as the reference point. *from_what* can be omitted and defaults to 0,
        using the beginning of the file as the reference point.
        '''
        if from_what == os.SEEK_SET:
            reference_pos = 0
        elif from_what == os.SEEK_CUR:
            reference_pos = self._pos
        elif from_what == os.SEEK_END:
            if self._file_length == None:
                desc = self.describe()
                self._file_length = int(desc["size"])
            reference_pos = self._file_length
        else:
            raise DXFileError("Invalid value supplied for from_what")

        orig_pos = self._pos
        self._pos = reference_pos + offset

        in_buf = False
        orig_buf_pos = self._read_buf.tell()
        if offset < orig_pos:
            if orig_buf_pos > orig_pos - offset:
                # offset is less than original position but within the buffer
                in_buf = True
        else:
            buf_len = dxpy.utils.string_buffer_length(self._read_buf)
            if buf_len - orig_buf_pos > offset - orig_pos:
                # offset is greater than original position but within the buffer
                in_buf = True

        if in_buf:
            # offset is within the buffer (at least one byte following
            # the offset can be read directly out of the buffer)
            self._read_buf.seek(orig_buf_pos - orig_pos + offset)
        elif offset == orig_pos:
            # This seek is a no-op (the cursor is just past the end of
            # the read buffer and coincides with the desired seek
            # position). We don't have the data ready, but the request
            # for the data starting here is already in flight.
            #
            # Detecting this case helps to optimize for sequential read
            # access patterns.
            pass
        else:
            # offset is outside the buffer-- reset buffer and queues.
            # This is the failsafe behavior
            self._read_buf = BytesIO()
            # TODO: if the offset is within the next response(s), don't throw out the queues
            self._request_iterator, self._response_iterator = None, None

    def tell(self):
        '''
        Returns the current position of the file read cursor.

        Warning: Because of buffering semantics, this value will **not** be accurate when using the line iterator form
        (`for line in file`).
        '''
        return self._pos

    def flush(self, multithread=True, **kwargs):
        '''
        Flushes the internal write buffer.
        '''
        if self._write_buf.tell() > 0:
            data = self._write_buf.getvalue()
            self._write_buf = BytesIO()

            if multithread:
                self._async_upload_part_request(data, index=self._cur_part, **kwargs)
            else:
                self.upload_part(data, self._cur_part, **kwargs)

            self._cur_part += 1

        if len(self._http_threadpool_futures) > 0:
            dxpy.utils.wait_for_all_futures(self._http_threadpool_futures)
            try:
                for future in self._http_threadpool_futures:
                    if future.exception() != None:
                        raise future.exception()
            finally:
                self._http_threadpool_futures = set()

    def _async_upload_part_request(self, *args, **kwargs):
        while len(self._http_threadpool_futures) >= self._http_threadpool_size:
            future = dxpy.utils.wait_for_a_future(self._http_threadpool_futures)
            if future.exception() != None:
                raise future.exception()
            self._http_threadpool_futures.remove(future)

        future = self._http_threadpool.submit(self.upload_part, *args, **kwargs)
        self._http_threadpool_futures.add(future)

    def _ensure_write_bufsize(self, **kwargs):
        if self._write_bufsize is not None:
            return
        file_upload_params = dxpy.api.project_describe(
            self.get_proj_id(),
            {'fields': {'fileUploadParameters': True}},
            **kwargs
        )['fileUploadParameters']
        self._empty_last_part_allowed = file_upload_params['emptyLastPartAllowed']
        self._write_bufsize = _get_write_buf_size(self._write_buffer_size_hint,
                                                  file_upload_params,
                                                  self._expected_file_size,
                                                  self._file_is_mmapd)

    def write(self, data, multithread=True, **kwargs):
        '''
        :param data: Data to be written
        :type data: str or mmap object

        Writes the data *data* to the file.

        .. note::

            Writing to remote files is append-only. Using :meth:`seek`
            does not affect where the next :meth:`write` will occur.

        '''
        self._ensure_write_bufsize(**kwargs)

        def write_request(data_for_write_req):
            if multithread:
                self._async_upload_part_request(data_for_write_req, index=self._cur_part, **kwargs)
            else:
                self.upload_part(data_for_write_req, self._cur_part, **kwargs)
            self._cur_part += 1

        if self._write_buf.tell() == 0 and self._write_bufsize == len(data):
            # In the special case of a write that is the same size as
            # our write buffer size, and no unflushed data in the
            # buffer, just directly dispatch the write and bypass the
            # write buffer.
            #
            # This saves a buffer copy, which is especially helpful if
            # 'data' is actually mmap'd from a file.
            #
            # TODO: an additional optimization could be made to allow
            # the last request from an mmap'd upload to take this path
            # too (in general it won't because it's not of length
            # _write_bufsize). This is probably inconsequential though.
            write_request(data)
            return

        remaining_space = self._write_bufsize - self._write_buf.tell()

        if len(data) <= remaining_space:
            self._write_buf.write(data)
        else:
            self._write_buf.write(data[:remaining_space])

            temp_data = self._write_buf.getvalue()
            self._write_buf = BytesIO()
            write_request(temp_data)

            # TODO: check if repeat string splitting is bad for
            # performance when len(data) >> _write_bufsize
            self.write(data[remaining_space:], **kwargs)

    def closed(self, **kwargs):
        '''
        :returns: Whether the remote file is closed
        :rtype: boolean

        Returns :const:`True` if the remote file is closed and
        :const:`False` otherwise. Note that if it is not closed, it can
        be in either the "open" or "closing" states.
        '''

        return self.describe(fields={'state'}, **kwargs)["state"] == "closed"

    def close(self, block=False, **kwargs):
        '''
        :param block: If True, this function blocks until the remote file has closed.
        :type block: boolean

        Attempts to close the file.

        .. note:: The remote file cannot be closed until all parts have
           been fully uploaded. An exception will be thrown if this is
           not the case.
        '''
        self.flush(**kwargs)

        # Also populates emptyLastPartAllowed
        self._ensure_write_bufsize(**kwargs)

        if self._num_uploaded_parts == 0 and self._empty_last_part_allowed:
            # We haven't uploaded any parts in this session.
            # In case no parts have been uploaded at all and region
            # settings allow last empty part upload, try to upload
            # an empty part (otherwise files with 0 parts cannot be closed).
            try:
                self.upload_part('', 1, **kwargs)
            except dxpy.exceptions.InvalidState:
                pass

        if 'report_progress_fn' in kwargs:
            del kwargs['report_progress_fn']

        dxpy.api.file_close(self._dxid, **kwargs)

        if block:
            self._wait_on_close(**kwargs)

    def wait_on_close(self, timeout=3600*24*7, **kwargs):
        '''
        :param timeout: Maximum amount of time to wait (in seconds) until the file is closed.
        :type timeout: integer
        :raises: :exc:`dxpy.exceptions.DXFileError` if the timeout is reached before the remote file has been closed

        Waits until the remote file is closed.
        '''
        self._wait_on_close(timeout, **kwargs)

    def upload_part(self, data, index=None, display_progress=False, report_progress_fn=None, **kwargs):
        """
        :param data: Data to be uploaded in this part
        :type data: str or mmap object
        :param index: Index of part to be uploaded; must be in [1, 10000]
        :type index: integer
        :param display_progress: Whether to print "." to stderr when done
        :type display_progress: boolean
        :param report_progress_fn: Optional: a function to call that takes in two arguments (self, # bytes transmitted)
        :type report_progress_fn: function or None
        :raises: :exc:`dxpy.exceptions.DXFileError` if *index* is given and is not in the correct range, :exc:`requests.exceptions.HTTPError` if upload fails

        Uploads the data in *data* as part number *index* for the
        associated file. If no value for *index* is given, *index*
        defaults to 1. This probably only makes sense if this is the
        only part to be uploaded.
        """
        req_input = {}
        if index is not None:
            req_input["index"] = int(index)

        md5 = hashlib.md5()
        if hasattr(data, 'seek') and hasattr(data, 'tell'):
            # data is a buffer; record initial position (so we can rewind back)
            rewind_input_buffer_offset = data.tell()
            while True:
                bytes_read = data.read(MD5_READ_CHUNK_SIZE)
                if bytes_read:
                    md5.update(bytes_read)
                else:
                    break
            # rewind the buffer to original position
            data.seek(rewind_input_buffer_offset)
        else:
            md5.update(data)

        req_input["md5"] = md5.hexdigest()
        req_input["size"] = len(data)

        def get_upload_url_and_headers():
            # This function is called from within a retry loop, so to avoid amplifying the number of retries
            # geometrically, we decrease the allowed number of retries for the nested API call every time.
            if 'max_retries' not in kwargs:
                kwargs['max_retries'] = dxpy.DEFAULT_RETRIES
            elif kwargs['max_retries'] > 0:
                kwargs['max_retries'] -= 1

            if "timeout" not in kwargs:
                kwargs["timeout"] = FILE_REQUEST_TIMEOUT

            resp = dxpy.api.file_upload(self._dxid, req_input, **kwargs)
            url = resp["url"]
            return url, _validate_headers(resp.get("headers", {}))

        # The file upload API requires us to get a pre-authenticated upload URL (and headers for it) every time we
        # attempt an upload. Because DXHTTPRequest will retry requests under retryable conditions, we give it a callback
        # to ask us for a new upload URL every time it attempts a request (instead of giving them directly).
        dxpy.DXHTTPRequest(get_upload_url_and_headers,
                           data,
                           jsonify_data=False,
                           prepend_srv=False,
                           always_retry=True,
                           timeout=FILE_REQUEST_TIMEOUT,
                           auth=None,
                           method='PUT')

        self._num_uploaded_parts += 1

        if display_progress:
            warn(".")

        if report_progress_fn is not None:
            report_progress_fn(self, len(data))

    def get_download_url(self, duration=None, preauthenticated=False, filename=None, project=None, **kwargs):
        """
        :param duration: number of seconds for which the generated URL will be
            valid, should only be specified when preauthenticated is True
        :type duration: int
        :param preauthenticated: if True, generates a 'preauthenticated'
            download URL, which embeds authentication info in the URL and does
            not require additional headers
        :type preauthenticated: bool
        :param filename: desired filename of the downloaded file
        :type filename: str
        :param project: ID of a project containing the file (the download URL
            will be associated with this project, and this may affect which
            billing account is billed for this download).
            If no project is specified, an attempt will be made to verify if the file is
            in the project from the DXFile handler (as specified by the user or
            the current project stored in dxpy.WORKSPACE_ID). Otherwise, no hint is supplied.
            This fall back behavior does not happen inside a job environment.
            A non preauthenticated URL is only valid as long as the user has
            access to that project and the project contains that file.
        :type project: str
        :returns: download URL and dict containing HTTP headers to be supplied
            with the request
        :rtype: tuple (str, dict)
        :raises: :exc:`~dxpy.exceptions.ResourceNotFound` if a project context was
            given and the file was not found in that project context.
        :raises: :exc:`~dxpy.exceptions.ResourceNotFound` if no project context was
            given and the file was not found in any projects.

        Obtains a URL that can be used to directly download the associated
        file.

        """
        args = {"preauthenticated": preauthenticated}

        if duration is not None:
            args["duration"] = duration
        if filename is not None:
            args["filename"] = filename

        # If project=None, we fall back to the project attached to this handler
        # (if any). If this is supplied, it's treated as a hint: if it's a
        # project in which this file exists, it's passed on to the
        # apiserver. Otherwise, NO hint is supplied. In principle supplying a
        # project in the handler that doesn't contain this file ought to be an
        # error, but it's this way for backwards compatibility. We don't know
        # who might be doing downloads and creating handlers without being
        # careful that the project encoded in the handler contains the file
        # being downloaded. They may now rely on such behavior.
        if project is None and 'DX_JOB_ID' not in os.environ:
            project_from_handler = self.get_proj_id()
            if project_from_handler and object_exists_in_project(self.get_id(), project_from_handler):
                project = project_from_handler

        if project is not None and project is not DXFile.NO_PROJECT_HINT:
            args["project"] = project

        # Test hook to write 'project' argument passed to API call to a
        # local file
        if '_DX_DUMP_BILLED_PROJECT' in os.environ:
            with open(os.environ['_DX_DUMP_BILLED_PROJECT'], "w") as fd:
                if project is not None and project != DXFile.NO_PROJECT_HINT:
                    fd.write(project)

        with self._url_download_mutex:
            if self._download_url is None or self._download_url_expires < time.time():
                # The idea here is to cache a download URL for the entire file, that will
                # be good for a few minutes. This avoids each thread having to ask the
                # server for a URL, increasing server load.
                #
                # To avoid thread race conditions, this check/update procedure is protected
                # with a lock.

                # logging.debug("Download URL unset or expired, requesting a new one")
                if "timeout" not in kwargs:
                    kwargs["timeout"] = FILE_REQUEST_TIMEOUT
                resp = dxpy.api.file_download(self._dxid, args, **kwargs)
                self._download_url = resp["url"]
                self._download_url_headers = _validate_headers(resp.get("headers", {}))
                if preauthenticated:
                    self._download_url_expires = resp["expires"]/1000 - 60  # Try to account for drift
                else:
                    self._download_url_expires = 32503680000  # doesn't expire (year 3000)

            # Make a copy, ensuring each thread has its own mutable
            # version of the headers.  Note: python strings are
            # immutable, so we can safely give a reference to the
            # download url.
            retval_download_url = self._download_url
            retval_download_url_headers = copy.copy(self._download_url_headers)

        return retval_download_url, retval_download_url_headers

    def _generate_read_requests(self, start_pos=0, end_pos=None, project=None,
                                limit_chunk_size=None, **kwargs):
        # project=None means no hint is to be supplied to the apiserver. It is
        # an error to supply a project that does not contain this file.
        if limit_chunk_size is None:
            limit_chunk_size = self._read_bufsize

        if self._file_length == None:
            desc = self.describe(**kwargs)
            self._file_length = int(desc["size"])

        if end_pos == None:
            end_pos = self._file_length
        if end_pos > self._file_length:
            raise DXFileError("Invalid end_pos")

        def chunk_ranges(start_pos, end_pos, init_chunk_size=1024*64, ramp=2, num_requests_between_ramp=4):
            cur_chunk_start = start_pos
            cur_chunk_size = min(init_chunk_size, limit_chunk_size)
            i = 0
            while cur_chunk_start < end_pos:
                cur_chunk_end = min(cur_chunk_start + cur_chunk_size - 1, end_pos)
                yield cur_chunk_start, cur_chunk_end
                cur_chunk_start += cur_chunk_size
                if cur_chunk_size < limit_chunk_size and i % num_requests_between_ramp == (num_requests_between_ramp - 1):
                    cur_chunk_size = min(cur_chunk_size * ramp, limit_chunk_size)
                i += 1

        for chunk_start_pos, chunk_end_pos in chunk_ranges(start_pos, end_pos):
            url, headers = self.get_download_url(project=project, **kwargs)
            # It is possible for chunk_end_pos to be outside of the range of the file
            yield dxpy._dxhttp_read_range, [url, headers, chunk_start_pos, min(chunk_end_pos, self._file_length - 1),
                                            FILE_REQUEST_TIMEOUT], {}

    def _next_response_content(self, get_first_chunk_sequentially=False):
        if self._response_iterator is None:
            self._response_iterator = dxpy.utils.response_iterator(
                self._request_iterator,
                self._http_threadpool,
                do_first_task_sequentially=get_first_chunk_sequentially
            )
        try:
            return next(self._response_iterator)
        except:
            # If an exception is raised, the iterator is unusable for
            # retrieving any more items. Destroy it so we'll reinitialize it
            # next time.
            self._response_iterator = None
            self._request_iterator = None
            raise

    def read(self, length=None, use_compression=None, project=None, **kwargs):
        '''
        :param length: Maximum number of bytes to be read
        :type length: integer
        :param project: project to use as context for this download (may affect
            which billing account is billed for this download). If specified,
            must be a project in which this file exists. If not specified, the
            project ID specified in the handler is used for the download, IF it
            contains this file. If set to DXFile.NO_PROJECT_HINT, no project ID
            is supplied for the download, even if the handler specifies a
            project ID.
        :type project: str or None
        :rtype: string
        :raises: :exc:`~dxpy.exceptions.ResourceNotFound` if *project* is supplied
           and it does not contain this file

        Returns the next *length* bytes, or all the bytes until the end of file
        (if no *length* is given or there are fewer than *length* bytes left in
        the file).

        .. note:: After the first call to read(), the project arg and
           passthrough kwargs are not respected while using the same response
           iterator (i.e. until next seek).

        '''
        if self._file_length == None:
            desc = self.describe(**kwargs)
            if desc["state"] != "closed":
                raise DXFileError("Cannot read from file until it is in the closed state")
            self._file_length = int(desc["size"])

        # If running on a worker, wait for the first file download chunk
        # to come back before issuing any more requests. This ensures
        # that all subsequent requests can take advantage of caching,
        # rather than having all of the first DXFILE_HTTP_THREADS
        # requests simultaneously hit a cold cache. Enforce a minimum
        # size for this heuristic so we don't incur the overhead for
        # tiny files (which wouldn't contribute as much to the load
        # anyway).
        get_first_chunk_sequentially = (self._file_length > 128 * 1024 and self._pos == 0 and dxpy.JOB_ID)

        if self._pos == self._file_length:
            return b""

        if length == None or length > self._file_length - self._pos:
            length = self._file_length - self._pos

        buf = self._read_buf
        buf_remaining_bytes = dxpy.utils.string_buffer_length(buf) - buf.tell()
        if length <= buf_remaining_bytes:
            self._pos += length
            return buf.read(length)
        else:
            orig_buf_pos = buf.tell()
            orig_file_pos = self._pos
            buf.seek(0, os.SEEK_END)
            self._pos += buf_remaining_bytes
            while self._pos < orig_file_pos + length:
                remaining_len = orig_file_pos + length - self._pos

                if self._response_iterator is None:
                    self._request_iterator = self._generate_read_requests(
                        start_pos=self._pos, project=project, **kwargs)

                content = self._next_response_content(get_first_chunk_sequentially=get_first_chunk_sequentially)

                if len(content) < remaining_len:
                    buf.write(content)
                    self._pos += len(content)
                else: # response goes beyond requested length
                    buf.write(content[:remaining_len])
                    self._pos += remaining_len
                    self._read_buf = BytesIO()
                    self._read_buf.write(content[remaining_len:])
                    self._read_buf.seek(0)
            buf.seek(orig_buf_pos)
            return buf.read()

        # Debug fallback
        # import urllib2
        # req = urllib2.Request(url, headers=headers)
        # response = urllib2.urlopen(req)
        # return response.read()
