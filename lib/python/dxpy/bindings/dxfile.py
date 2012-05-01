'''
DXFile Handler
**************

This remote file handler is a file-like object.
'''

import cStringIO as StringIO
from dxpy.bindings import *

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
                 buffer_size=1024*1024*100):
        self._keep_open = keep_open
        self._write_buf = StringIO.StringIO()
        if dxid is not None:
            self.set_ids(dxid, project)
        self._keep_open = keep_open
        # Default maximum buffer size is 100MB
        self._bufsize = buffer_size

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

    def read(self, size=None):
        '''
        :param size: Maximum number of bytes to be read
        :type size: integer
        :rtype: string

        Returns the next *size* bytes or until the end of file if no
        *size* is given or there are fewer than *size* bytes left in
        the file.

        '''

        resp = dxpy.api.fileDownload(self._dxid)
        url = resp["url"]
        headers = {}

        if self._file_length is None:
            desc = self.describe()
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

        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return resp.content

    def seek(self, offset):
        '''
        :param offset: Position in the file to seek to
        :type offset: integer

        Seeks to *offset* bytes from the beginning of the file.  This
        is a no-op if the file is open for writing.

        '''
        self._pos = offset

    def flush(self):
        '''
        Flushes the internal buffer
        '''
        self.upload_part(self._write_buf.getvalue(), self._cur_part)
        self._write_buf = StringIO.StringIO()
        self._cur_part += 1

    def write(self, string):
        '''
        :param str: String to be written
        :type str: string

        Writes the string *string* to the file.

        .. note::

            Writing to remote files is append-only.  Using :meth:`seek` will not affect where the next :meth:`write` will occur.

        '''

        remaining_space = (self._bufsize - self._write_buf.tell())
        self._write_buf.write(string[:remaining_space])
        if self._write_buf.tell() == self._bufsize:
            self.flush()
            self.write(string[remaining_space:])

    def closed(self):
        '''
        :returns: Whether the remote file is closed
        :rtype: boolean

        Returns :const:`True` if the remote file is closed and
        :const:`False` otherwise.  Note that if it is not closed, it
        can be in either the "open" and "closing" states.
        '''

        return self.describe()["state"] == "closed"

    def close(self, block=False, **kwargs):
        '''
        :param block: Indicates whether this function should block until the remote file has closed or not.
        :type block: boolean

        Attempts to close the file.  Note that the remote file cannot
        be closed until all parts have been fully uploaded, and an
        exception will be thrown in this case.
        '''

        if self._write_buf.tell() > 0:
            self.flush()

        dxpy.api.fileClose(self._dxid)

        if block:
            self._wait_on_close()

    def wait_on_close(self, timeout=sys.maxint):
        '''
        :param timeout: Max amount of time to wait (in seconds) until the file is closed.
        :type timeout: integer
        :raises: :exc:`dxpy.exceptions.DXError` if the timeout is reached before the remote file has been closed

        Wait until the remote file is closed.
        '''
        self._wait_on_close(timeout)

    def upload_part(self, data, index=None):
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

        resp = dxpy.api.fileUpload(self._dxid, req_input)
        url = resp["url"]
        headers = {}
        headers['Content-Length'] = str(len(data))

        resp = requests.post(url, data=data, headers=headers)
        resp.raise_for_status()

        # TODO: Consider retrying depending on the status
