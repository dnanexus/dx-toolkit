'''
DXFile Handler
**************

This remote file handler is a file-like object.
'''

import os, logging
import cStringIO as StringIO
import concurrent.futures
from dxpy.bindings import *
import dxpy.utils

if dxpy.snappy_available:
    import snappy

# TODO: adaptive buffer size
DEFAULT_BUFFER_SIZE = 1024*1024*32

class DXFile(DXDataObject):
    '''
    :param dxid: Object ID
    :type dxid: string
    :param project: Project ID
    :type project: string
    :param keep_open: Indicates whether the remote file should be kept open when exiting the context manager or when the destructor is called on the file handler
    :type keep_open: boolean

    Remote file object handler

    .. automethod:: _new

    '''

    _class = "file"

    _describe = staticmethod(dxpy.api.fileDescribe)
    _add_types = staticmethod(dxpy.api.fileAddTypes)
    _remove_types = staticmethod(dxpy.api.fileRemoveTypes)
    _get_details = staticmethod(dxpy.api.fileGetDetails)
    _set_details = staticmethod(dxpy.api.fileSetDetails)
    _set_visibility = staticmethod(dxpy.api.fileSetVisibility)
    _rename = staticmethod(dxpy.api.fileRename)
    _set_properties = staticmethod(dxpy.api.fileSetProperties)
    _add_tags = staticmethod(dxpy.api.fileAddTags)
    _remove_tags = staticmethod(dxpy.api.fileRemoveTags)
    _close = staticmethod(dxpy.api.fileClose)
    _list_projects = staticmethod(dxpy.api.fileListProjects)

    _http_threadpool = None
    _http_threadpool_size = NUM_HTTP_THREADS

    @classmethod
    def set_http_threadpool_size(cls, num_threads):
        cls._http_threadpool_size = num_threads

    def __init__(self, dxid=None, project=None, keep_open=False, buffer_size=DEFAULT_BUFFER_SIZE):
        self._keep_open = keep_open
        self._read_buf = StringIO.StringIO()
        self._write_buf = StringIO.StringIO()
        self._keep_open = keep_open

        if buffer_size < 5*1024*1024:
            raise DXFileError("Buffer size must be at least 5 MB")

        self._bufsize = buffer_size

        self._download_url, self._download_url_expires = None, None
        self._request_iterator, self._response_iterator = None, None
        self._http_threadpool_futures = set()

        if dxid is not None:
            self.set_ids(dxid, project)

    def _new(self, dx_hash, media_type=None, **kwargs):
        """
        :param dx_hash: Standard hash populated in :func:`dxpy.bindings.DXDataObject.new()`
        :type dx_hash: dict
        :param media_type: Internet Media Type (optional)
        :type media_type: string

        Creates a new remote file with media type *media_type*, if given.

        """

        if media_type is not None:
            dx_hash["media"] = media_type

        resp = dxpy.api.fileNew(dx_hash, **kwargs)
        self.set_ids(resp["id"], dx_hash["project"])

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.flush()
        if (not self._keep_open) and self._get_state() == "open":
            self.close()

    def __del__(self):
        '''
        When this is triggered by interpreter shutdown, the thread pool is not available,
        and we will wait for the request queue forever. In this case, we must revert to synchronous, in-thread flushing.
        I don't know how to detect this condition, so I'll use that for all destructor events.
        Use a context manager or flush the object explicitly to avoid this.

        Also, neither this nor context managers are compatible with kwargs pass-through (so e.g. no custom auth).
        '''
        self.flush(multithread=False)

    def __iter__(self):
        buffer = self.read(self._bufsize)
        done = False
        while not done:
            if "\n" in buffer:
                lines = buffer.splitlines()
                for i in range(len(lines) - 1):
                    yield lines[i]
                buffer = lines[len(lines) - 1]
            else:
                more = self.read(self._bufsize)
                if more == "":
                    done = True
                else:
                    buffer = buffer + more
        if buffer:
            yield buffer

    def set_ids(self, dxid, project=None):
        '''
        :param dxid: Object ID
        :type dxid: string
        :param project: Project ID
        :type project: string

        Discards the currently stored ID and associates the handler
        with *dxid*.  As a side effect, it also flushes the buffer for
        the previous file object if the buffer is nonempty.
        '''
        self.flush()

        DXDataObject.set_ids(self, dxid, project)

        # Reset state
        self._pos = 0
        self._file_length = None
        self._cur_part = 1

    def seek(self, offset):
        '''
        :param offset: Position in the file to seek to
        :type offset: integer

        Seeks to *offset* bytes from the beginning of the file.  This
        is a no-op if the file is open for writing.

        '''
        self._pos = offset
        self._write_buf = StringIO.StringIO()
        self._request_iterator, self._response_iterator = None, None

    def flush(self, multithread=True, **kwargs):
        '''
        Flushes the internal write buffer
        '''
        if self._write_buf.tell() > 0:
            data = self._write_buf.getvalue()
            self._write_buf = StringIO.StringIO()

            if multithread:
                self._async_upload_part_request(data, index=self._cur_part, **kwargs)
            else:
                self.upload_part(data, self._cur_part, **kwargs)

            self._cur_part += 1

        if len(self._http_threadpool_futures) > 0:
            concurrent.futures.wait(self._http_threadpool_futures)
            for future in self._http_threadpool_futures:
                if future.exception() != None:
                    raise future.exception()
            self._http_threadpool_futures = set()

    def _async_upload_part_request(self, *args, **kwargs):
        if self._http_threadpool == None:
            DXFile._http_threadpool = concurrent.futures.ThreadPoolExecutor(max_workers=self._http_threadpool_size)

        while len(self._http_threadpool_futures) >= self._http_threadpool_size:
            future = concurrent.futures.as_completed(self._http_threadpool_futures).next()
            if future.exception() != None:
                raise future.exception()
            self._http_threadpool_futures.remove(future)

        future = self._http_threadpool.submit(self.upload_part, *args, **kwargs)
        self._http_threadpool_futures.add(future)

    def write(self, string, multithread=True, **kwargs):
        '''
        :param str: String to be written
        :type str: string

        Writes the string *string* to the file.

        .. note::

            Writing to remote files is append-only.  Using :meth:`seek` will not affect where the next :meth:`write` will occur.

        '''
        remaining_space = self._bufsize - self._write_buf.tell()
        if len(string) <= remaining_space:
            self._write_buf.write(string)
        else:
            self._write_buf.write(string[:remaining_space])

            data = self._write_buf.getvalue()
            self._write_buf = StringIO.StringIO()

            if multithread:
                self._async_upload_part_request(data, index=self._cur_part, **kwargs)
            else:
                self.upload_part(data, self._cur_part, **kwargs)

            self._cur_part += 1

            # TODO: check if repeat string splitting is bad for performance when len(string) >> _bufsize
            self.write(string[remaining_space:], **kwargs)

    def closed(self, **kwargs):
        '''
        :returns: Whether the remote file is closed
        :rtype: boolean

        Returns :const:`True` if the remote file is closed and
        :const:`False` otherwise.  Note that if it is not closed, it
        can be in either the "open" and "closing" states.
        '''

        return self.describe(**kwargs)["state"] == "closed"

    def close(self, block=False, **kwargs):
        '''
        :param block: Indicates whether this function should block until the remote file has closed or not.
        :type block: boolean

        Attempts to close the file.  Note that the remote file cannot
        be closed until all parts have been fully uploaded, and an
        exception will be thrown in this case.
        '''
        self.flush(**kwargs)

        try:
            dxpy.api.fileClose(self._dxid, **kwargs)
        except DXAPIError as e:
            if e.name == 'InvalidState' and e.msg == 'File needs to contain at least one part to be closed.':
                # File is empty
                self._cur_part += 1
                self.upload_part('', 1, **kwargs)
                dxpy.api.fileClose(self._dxid, **kwargs)
            else:
                raise

        if block:
            self._wait_on_close(**kwargs)

    def wait_on_close(self, timeout=sys.maxint, **kwargs):
        '''
        :param timeout: Max amount of time to wait (in seconds) until the file is closed.
        :type timeout: integer
        :raises: :exc:`dxpy.exceptions.DXFileError` if the timeout is reached before the remote file has been closed

        Wait until the remote file is closed.
        '''
        self._wait_on_close(timeout, **kwargs)

    def upload_part(self, data, index=None, display_progress=False, **kwargs):
        """
        :param data: Data to be uploaded in this part
        :type data: string
        :param index: Index of part to be uploaded; must be in [1, 10000]
        :type index: integer
        :raises: :exc:`dxpy.exceptions.DXFileError` if *index* is given and is in the wrong range, :exc:`requests.exceptions.HTTPError` if upload fails

        Requests a URL for uploading a part, and uploads the data in
        *data* as part number *index* for the associated file.  If no
        value for *index* is given, it is assumed that this is the
        only part to be uploaded.

        """

        req_input = {}
        if index is not None:
            req_input["index"] = int(index)

        resp = dxpy.api.fileUpload(self._dxid, req_input, **kwargs)
        url = resp["url"]
        headers = {}
        headers['Content-Length'] = str(len(data))
        headers['Content-Type'] = 'application/octet-stream'

        DXHTTPRequest(url, data, headers=headers, jsonify_data=False, prepend_srv=False, always_retry=True)

        if display_progress:
            print >> sys.stderr, "."

    def get_download_url(self, duration=24*3600, **kwargs):
        if self._download_url is None or self._download_url_expires > time.time():
            # logging.debug("Download URL unset or expired, requesting a new one")
            resp = dxpy.api.fileDownload(self._dxid, {"duration": duration}, **kwargs)
            self._download_url = resp["url"]
            self._download_url_expires = time.time() + duration - 60 # Try to account for drift
        return self._download_url

    def _generate_read_requests(self, start_pos=0, end_pos=None, **kwargs):
        url = self.get_download_url(**kwargs)

        if self._file_length == None:
            desc = self.describe(**kwargs)
            self._file_length = int(desc["size"])

        if end_pos == None:
            end_pos = self._file_length
        if end_pos > self._file_length:
            raise DXFileError("Invalid end_pos")

        for chunk_start_pos in xrange(start_pos, end_pos, self._bufsize):
            chunk_end_pos = min(chunk_start_pos + self._bufsize - 1, end_pos)
            headers = {'Range': "bytes=" + str(chunk_start_pos) + "-" + str(chunk_end_pos)}
            yield DXHTTPRequest, [url, ''], {'method': 'GET',
                                             'headers': headers,
                                             'jsonify_data': False,
                                             'prepend_srv': False,
                                             'prefetch': True}

    def _next_response_content(self):
        if self._http_threadpool is None:
            DXFile._http_threadpool = concurrent.futures.ThreadPoolExecutor(max_workers=self._http_threadpool_size)

        if self._response_iterator is None:
            self._response_iterator = dxpy.utils.response_iterator(self._request_iterator, self._http_threadpool,
                                                                   max_active_tasks=self._http_threadpool_size)
        return self._response_iterator.next()

    def read(self, length=None, use_compression=None, **kwargs):
        '''
        :param size: Maximum number of bytes to be read
        :type size: integer
        :rtype: string

        Returns the next *size* bytes or until the end of file if no *size* is given or there are fewer than *size*
        bytes left in the file.

        .. note::

            After the first call to read(), passthrough kwargs are not respected while using the same response iterator (i.e. until next seek).
        '''
        if self._response_iterator == None:
            self._request_iterator = self._generate_read_requests(start_pos=self._pos, **kwargs)

        if self._file_length == None:
            desc = self.describe(**kwargs)
            if desc["state"] != "closed":
                raise DXFileError("Cannot read from file until it is in the closed state")
            self._file_length = int(desc["size"])

        if self._pos == self._file_length:
            return ""

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
            while self._pos < orig_file_pos + length:
                remaining_len = orig_file_pos + length - self._pos
                content = self._next_response_content()

                if len(content) < remaining_len:
                    buf.write(content)
                    self._pos += len(content)
                else: # response goes beyond requested length
                    buf.write(content[:remaining_len])
                    self._pos += remaining_len
                    self._read_buf = StringIO.StringIO()
                    self._read_buf.write(content[remaining_len:])
                    self._read_buf.seek(0)
            buf.seek(orig_buf_pos)
            return buf.read()

        # Debug fallback
        # import urllib2
        # req = urllib2.Request(url, headers=headers)
        # response = urllib2.urlopen(req)
        # return response.read()
