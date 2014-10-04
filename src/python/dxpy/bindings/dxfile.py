# Copyright (C) 2013-2014 DNAnexus, Inc.
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

from __future__ import (print_function, unicode_literals)

import os, sys, logging, traceback, hashlib, copy, time
import concurrent.futures

import dxpy
from . import DXDataObject
from ..exceptions import DXFileError
from ..utils import warn
from ..compat import BytesIO

if dxpy.snappy_available:
    import snappy

# TODO: adaptive buffer size
DXFILE_HTTP_THREADS = 8
DEFAULT_BUFFER_SIZE = 1024*1024*16
if dxpy.JOB_ID:
    # Increase HTTP request buffer size when we are running within the
    # platform.
    DEFAULT_BUFFER_SIZE = 1024*1024*96

MD5_READ_CHUNK_SIZE = 1024*1024*4

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

    _http_threadpool = None
    _http_threadpool_size = DXFILE_HTTP_THREADS

    @classmethod
    def set_http_threadpool_size(cls, num_threads):
        cls._http_threadpool_size = num_threads

    @classmethod
    def _ensure_http_threadpool(cls):
        if cls._http_threadpool is None:
            cls._http_threadpool = dxpy.utils.get_futures_threadpool(max_workers=cls._http_threadpool_size)

    def __init__(self, dxid=None, project=None, mode=None,
                 read_buffer_size=DEFAULT_BUFFER_SIZE, write_buffer_size=DEFAULT_BUFFER_SIZE):
        DXDataObject.__init__(self, dxid=dxid, project=project)
        if mode is None:
            self._close_on_exit = True
        else:
            if mode not in ['r', 'w', 'a']:
                raise ValueError("mode must be one of 'r', 'w', or 'a'")
            self._close_on_exit = (mode == 'w')

        self._read_buf = BytesIO()
        self._write_buf = BytesIO()

        if write_buffer_size < 5*1024*1024:
            raise DXFileError("Write buffer size must be at least 5 MB")

        self._read_bufsize = read_buffer_size
        self._write_bufsize = write_buffer_size

        self._download_url, self._download_url_headers, self._download_url_expires = None, None, None
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
        self._ensure_http_threadpool()

        while len(self._http_threadpool_futures) >= self._http_threadpool_size:
            future = dxpy.utils.wait_for_a_future(self._http_threadpool_futures)
            if future.exception() != None:
                raise future.exception()
            self._http_threadpool_futures.remove(future)

        future = self._http_threadpool.submit(self.upload_part, *args, **kwargs)
        self._http_threadpool_futures.add(future)

    def write(self, data, multithread=True, **kwargs):
        '''
        :param data: Data to be written
        :type data: str or mmap object

        Writes the data *data* to the file.

        .. note::

            Writing to remote files is append-only. Using :meth:`seek`
            does not affect where the next :meth:`write` will occur.

        '''

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

        return self.describe(**kwargs)["state"] == "closed"

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

        if self._num_uploaded_parts == 0:
            # We haven't uploaded any parts in this session. In case no parts have been uploaded at all, try to upload
            # an empty part (files with 0 parts cannot be closed).
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

        def get_upload_url_and_headers():
            # This function is called from within a retry loop, so to avoid amplifying the number of retries
            # geometrically, we decrease the allowed number of retries for the nested API call every time.
            if 'max_retries' not in kwargs:
                kwargs['max_retries'] = dxpy.DEFAULT_RETRIES
            elif kwargs['max_retries'] > 0:
                kwargs['max_retries'] -= 1

            resp = dxpy.api.file_upload(self._dxid, req_input, **kwargs)
            url = resp["url"]
            headers = resp.get("headers", {})
            headers['Content-Length'] = str(len(data))
            headers['Content-MD5'] = md5.hexdigest()
            return url, headers

        # The file upload API requires us to get a pre-authenticated upload URL (and headers for it) every time we
        # attempt an upload. Because DXHTTPRequest will retry requests under retryable conditions, we give it a callback
        # to ask us for a new upload URL every time it attempts a request (instead of giving them directly).
        dxpy.DXHTTPRequest(get_upload_url_and_headers, data, jsonify_data=False, prepend_srv=False, always_retry=True,
                           auth=None)

        self._num_uploaded_parts += 1

        if display_progress:
            warn(".")

        if report_progress_fn is not None:
            report_progress_fn(self, len(data))

    def get_download_url(self, duration=24*3600, preauthenticated=False, filename=None, project=None, **kwargs):
        """
        :param duration: number of seconds for which the generated URL will be valid
        :type duration: int
        :param preauthenticated: if True, generates a 'preauthenticated' download URL, which embeds authentication info in the URL and does not require additional headers
        :type preauthenticated: bool
        :param filename: desired filename of the downloaded file
        :type filename: str
        :param project: ID of a project containing the file (the download URL should be associated with this project)
        :type project: str
        :returns: download URL and dict containing HTTP headers to be supplied with the request
        :rtype: tuple (str, dict)

        Obtains a URL that can be used to directly download the
        associated file.
        """
        args = {"duration": duration, "preauthenticated": preauthenticated}
        if filename is not None:
            args["filename"] = filename
        if project is not None:
            args["project"] = project
        if self._download_url is None or self._download_url_expires > time.time():
            # logging.debug("Download URL unset or expired, requesting a new one")
            resp = dxpy.api.file_download(self._dxid, args, **kwargs)
            self._download_url = resp["url"]
            self._download_url_headers = resp.get("headers", {})
            self._download_url_expires = time.time() + duration - 60 # Try to account for drift
        return self._download_url, self._download_url_headers

    def _generate_read_requests(self, start_pos=0, end_pos=None, **kwargs):
        url, headers = self.get_download_url(**kwargs)

        if self._file_length == None:
            desc = self.describe(**kwargs)
            self._file_length = int(desc["size"])

        if end_pos == None:
            end_pos = self._file_length
        if end_pos > self._file_length:
            raise DXFileError("Invalid end_pos")

        def chunk_ranges(start_pos, end_pos, init_chunk_size=1024*64, limit_chunk_size=self._read_bufsize, ramp=2, num_requests_between_ramp=4):
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
            headers = copy.copy(headers)
            headers['Range'] = "bytes=" + str(chunk_start_pos) + "-" + str(chunk_end_pos)
            yield dxpy.DXHTTPRequest, [url, ''], {'method': 'GET',
                                                  'headers': headers,
                                                  'auth': None,
                                                  'jsonify_data': False,
                                                  'prepend_srv': False,
                                                  'always_retry': True,
                                                  'decode_response_body': False}

    def _next_response_content(self):
        self._ensure_http_threadpool()

        if self._response_iterator is None:
            self._response_iterator = dxpy.utils.response_iterator(
                self._request_iterator,
                self._http_threadpool,
                max_active_tasks=self._http_threadpool_size,
                queue_id=id(self)
            )
        return next(self._response_iterator)

    def read(self, length=None, use_compression=None, **kwargs):
        '''
        :param size: Maximum number of bytes to be read
        :type size: integer
        :rtype: string

        Returns the next *size* bytes, or all the bytes until the end of
        file (if no *size* is given or there are fewer than *size* bytes
        left in the file).

        .. note:: After the first call to read(), passthrough kwargs are
           not respected while using the same response iterator (i.e.
           until next seek).

        '''
        if self._response_iterator == None:
            self._request_iterator = self._generate_read_requests(start_pos=self._pos, **kwargs)

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
        if self._file_length > 128 * 1024 and self._pos == 0 and dxpy.JOB_ID:
            get_first_chunk_sequentially = True
        else:
            get_first_chunk_sequentially = False

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

                if get_first_chunk_sequentially:
                    # Make the first chunk request without using the
                    # usual thread pool and block until it completes. On
                    # the second chunk, we'll call
                    # _next_response_content in the alternative block
                    # below. This starts the threadpool going for the
                    # second and all subsequent chunks.
                    callable_, args, kwargs = next(self._request_iterator)
                    content = callable_(*args, **kwargs)
                    get_first_chunk_sequentially = False
                else:
                    content = self._next_response_content()

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
