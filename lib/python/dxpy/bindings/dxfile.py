'''
DXFile Handler
**************

This remote file handler is a file-like object.
'''

import os, logging
import cStringIO as StringIO
import concurrent.futures
from dxpy.bindings import *

if dxpy.snappy_available:
    import snappy

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

    def __init__(self, dxid=None, project=None, keep_open=False,
                 buffer_size=1024*1024*128, request_size=1024*1024*32,
                 num_http_threads=8):
        '''
        Note: Each upload part must be at least 5 MB (per S3 backend requirements). This is enforced by the API server
        on file close. This means file close will fail if any of the following is true about the tunable args above:
        - buffer_size < 5 MB
        - request_size < 5 MB
        - request_size % buffer_size != 0
        '''
        self._keep_open = keep_open
        self._read_buf = StringIO.StringIO()
        self._write_buf = StringIO.StringIO()
        if dxid is not None:
            self.set_ids(dxid, project)
        self._keep_open = keep_open
        # Default maximum buffer size is 128MB
        self._bufsize = buffer_size
        self._request_size = request_size

        self._download_url, self._download_url_expires = None, None
        self._request_iterator, self._response_iterator, self._http_threadpool = None, None, None
        self._http_threadpool_size = num_http_threads

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
        if (not self._keep_open) and self._get_state() == "open":
            self.close()
        if self._write_buf.tell() > 0:
            self.flush()

    def __del__(self):
        if self._write_buf.tell() > 0:
            self.flush()

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
        if self._write_buf.tell() > 0:
            self.flush()

        DXDataObject.set_ids(self, dxid, project)

        # Reset state
        self._pos = 0
        self._file_length = None
        self._cur_part = 1

    def slow_read(self, size=None, use_compression=None, **kwargs):
        '''
        :param size: Maximum number of bytes to be read
        :type size: integer
        :rtype: string

        Returns the next *size* bytes or until the end of file if no
        *size* is given or there are fewer than *size* bytes left in
        the file.

        '''

        url = self.get_download_url(**kwargs)
        headers = {}

        if self._file_length is None:
            desc = self.describe(**kwargs)
            self._file_length = int(desc["size"])

        if self._pos == self._file_length:
            return ""

        if self._pos > 0 or size is not None:
            endbyte = self._file_length - 1
            if size is not None:
                endbyte = min(self._pos + size - 1, endbyte)

            headers["Range"] = "bytes=" + str(self._pos) + "-" + str(endbyte)
            self._pos = endbyte + 1
        else:
            self._pos = self._file_length

        if use_compression == 'snappy':
            if not dxpy.snappy_available:
                raise DXError("Snappy compression requested, but the snappy module is unavailable")
            headers['accept-encoding'] = 'snappy'

        # HACK. TODO: integrate with overall dxpy retry policy
        FILE_DOWNLOAD_RETRIES = 5
        for i in range(FILE_DOWNLOAD_RETRIES):
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            if 'content-length' in response.headers:
                if int(response.headers['content-length']) != len(response.content):
                    if i == FILE_DOWNLOAD_RETRIES-1:
                        raise HTTPError("Received response with content-length header set to %s but content length is %d"
                                        % (response.headers['content-length'], len(response.content)))
                    else:
                        logging.error("Received response with content-length header set to %s but content length is %d"
                                      % (response.headers['content-length'], len(response.content)))
                        continue

            break

        if use_compression and response.headers.get('content-encoding', '') == 'snappy':
            return snappy.uncompress(response.content)
        else:
            return response.content

        # Debug fallback
        # import urllib2
        # req = urllib2.Request(url, headers=headers)
        # response = urllib2.urlopen(req)
        # return response.read()

    def seek(self, offset):
        '''
        :param offset: Position in the file to seek to
        :type offset: integer

        Seeks to *offset* bytes from the beginning of the file.  This
        is a no-op if the file is open for writing.

        '''
        self._pos = offset
        self._write_buf = StringIO.StringIO()
        self._request_iterator, self._response_iterator, self._http_threadpool = None, None, None

    def _generate_upload_part_args(self, data):
        for chunk_start_pos in xrange(0, len(data), self._request_size):
            chunk_end_pos = min(chunk_start_pos + self._request_size - 1, len(data))
            yield (data[chunk_start_pos:chunk_end_pos+1], self._cur_part)
            self._cur_part += 1

    def flush(self, multithread=True, **kwargs):
        '''
        Flushes the internal write buffer
        '''
        data = self._write_buf.getvalue()
        self._write_buf = StringIO.StringIO()

        if len(data) <= self._request_size or not multithread:
            self.upload_part(data, self._cur_part, **kwargs)
            self._cur_part += 1
        else: # multithreaded upload
            if self._http_threadpool == None:
                self._http_threadpool = concurrent.futures.ThreadPoolExecutor(max_workers=self._http_threadpool_size)

            def do_upload_part(args):
                data, part = args
                self.upload_part(data, index=part, **kwargs)

            for result in self._http_threadpool.map(do_upload_part, self._generate_upload_part_args(data)):
                pass

    def write(self, string, **kwargs):
        '''
        :param str: String to be written
        :type str: string

        Writes the string *string* to the file.

        .. note::

            Writing to remote files is append-only.  Using :meth:`seek` will not affect where the next :meth:`write` will occur.

        '''

        remaining_space = self._bufsize - self._write_buf.tell()
        self._write_buf.write(string[:remaining_space])
        if self._write_buf.tell() == self._bufsize:
            self.flush(**kwargs)
            self.write(string[remaining_space:])

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

        if self._write_buf.tell() > 0:
            self.flush(**kwargs)

        dxpy.api.fileClose(self._dxid, **kwargs)

        if block:
            self._wait_on_close()

    def wait_on_close(self, timeout=sys.maxint, **kwargs):
        '''
        :param timeout: Max amount of time to wait (in seconds) until the file is closed.
        :type timeout: integer
        :raises: :exc:`dxpy.exceptions.DXError` if the timeout is reached before the remote file has been closed

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

        resp = requests.post(url, data=data, headers=headers)
        resp.raise_for_status()

        # TODO: Consider retrying depending on the status

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
        headers = {}

        if self._file_length == None:
            desc = self.describe(**kwargs)
            self._file_length = int(desc["size"])

        if end_pos == None:
            end_pos = self._file_length
        if end_pos > self._file_length:
            raise DXError("Invalid end_pos")

        for chunk_start_pos in xrange(start_pos, end_pos, self._request_size):
            chunk_end_pos = min(chunk_start_pos + self._request_size - 1, end_pos)
            headers["Range"] = "bytes=" + str(chunk_start_pos) + "-" + str(chunk_end_pos)
            request = requests.get(url, headers=headers, return_response=False, prefetch=True)
            yield request

    def _next_response(self):
        '''
        Dequeues and returns the next response from the response queue.
        Expects self._request_iterator, self._response_iterator to be set.
        '''
        while True:
            try:
                return self._response_iterator.next()
            except (StopIteration, AttributeError):
                batch_size = self._bufsize / self._request_size
                request_batch = []
                try:
                    for i in xrange(batch_size):
                        request_batch.append(self._request_iterator.next())
                except StopIteration:
                    if len(request_batch) == 0:
                        raise
                self._start_response_iterator(request_batch)

    def _start_response_iterator(self, request_batch):
        if self._http_threadpool == None:
            self._http_threadpool = concurrent.futures.ThreadPoolExecutor(max_workers=self._http_threadpool_size)

        def send(r):
            r.send(prefetch=True)
            return r.response

        self._response_iterator = self._http_threadpool.map(send, request_batch)

    def read(self, length=None, use_compression=None, **kwargs):
        # Note passthrough kwargs are not respected while using the same response iterator (i.e. until next seek).
        if self._response_iterator == None:
            self._request_iterator = self._generate_read_requests(start_pos=self._pos, **kwargs)

        if self._file_length == None:
            desc = self.describe(**kwargs)
            self._file_length = int(desc["size"])

        if self._pos == self._file_length:
            return ""

        if length == None or length > self._file_length - self._pos:
            length = self._file_length - self._pos

#        print "FL %d, pos %d, length %d" % (self._file_length, self._pos, length)

        buf = self._read_buf
        buf_remaining_bytes = _string_buffer_length(buf) - buf.tell()
#        print "buf length", _string_buffer_length(buf), "pos", buf.tell(), "rem bytes", buf_remaining_bytes
        if length <= buf_remaining_bytes:
            self._pos += length
            return buf.read(length)
        else:
            orig_buf_pos = buf.tell()
            orig_file_pos = self._pos
            buf.seek(0, os.SEEK_END)
            while self._pos < orig_file_pos + length:
                remaining_len = orig_file_pos + length - self._pos
                response = self._next_response()
                response.raise_for_status()

                if use_compression and response.headers.get('content-encoding', '') == 'snappy':
                    content = snappy.uncompress(response.content)
                else:
                    content = response.content

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


def _string_buffer_length(buf):
    old_pos = buf.tell()
    buf.seek(0, os.SEEK_END)
    buf_len = buf.tell()
    buf.seek(old_pos)
    return buf_len
