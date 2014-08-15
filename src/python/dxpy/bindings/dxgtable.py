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

"""
DXGTable Handler
****************
"""

from __future__ import (print_function, unicode_literals)

import os, sys, json, traceback
import concurrent.futures

import dxpy
from . import DXDataObject
from ..exceptions import DXError
from ..compat import StringIO
from ..utils import warn

DXGTABLE_HTTP_THREADS = 4

# Number of rows to request at a time when reading.
#
# TODO: adaptive buffer size. Start with small requests to improve interactivity and make
# progressively larger requests?
DEFAULT_TABLE_READ_ROW_BUFFER_SIZE = 40000

# Writing uses two buffers: one that contains the actual rows (list of Python lists) and the
# stringified data to send to the server (kept in a StringIO object). The row data is stringified
# when we have accumulated a fixed number of rows. The stringified data is sent to the server once
# its size exceeds a certain number of bytes.
#
# The row buffer should be large enough that we don't suffer a huge amount of overhead in
# stringifying, but the larger the row buffer, the more we could exceed the max byte size of the
# stringified buffer.
DEFAULT_TABLE_WRITE_ROW_BUFFER_SIZE = 10000
DEFAULT_TABLE_WRITE_REQUEST_SIZE = 1024*1024*64 # bytes

# Use this value for creating 'null' values in gtables.  Will be interpreted as null downstream.
# Available in apps as dxpy.NULL
NULL = - (1 << 31)

class DXGTable(DXDataObject):
    '''
    Remote GTable object handler.

    .. py:attribute:: size

       The size, in bytes of the data stored in the GTable.

    .. py:attribute:: columns

       List of dicts representing the columns of the GTable. Each dict has entries "name" and
       "type" indicating the name and data type of the the column, respectively.

    .. py:attribute:: indices

       List of dicts representing the indices of the GTable (available only if one or more indices
       exist). See `the API docs for GenomicTables
       <https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#Indexing>`_ for more
       information about the format of these dicts.

    .. automethod:: _new
    '''

    _class = "gtable"

    _describe = staticmethod(dxpy.api.gtable_describe)
    _add_types = staticmethod(dxpy.api.gtable_add_types)
    _remove_types = staticmethod(dxpy.api.gtable_remove_types)
    _get_details = staticmethod(dxpy.api.gtable_get_details)
    _set_details = staticmethod(dxpy.api.gtable_set_details)
    _set_visibility = staticmethod(dxpy.api.gtable_set_visibility)
    _rename = staticmethod(dxpy.api.gtable_rename)
    _set_properties = staticmethod(dxpy.api.gtable_set_properties)
    _add_tags = staticmethod(dxpy.api.gtable_add_tags)
    _remove_tags = staticmethod(dxpy.api.gtable_remove_tags)
    _close = staticmethod(dxpy.api.gtable_close)
    _list_projects = staticmethod(dxpy.api.gtable_list_projects)

    _http_threadpool = None
    _http_threadpool_size = DXGTABLE_HTTP_THREADS

    @classmethod
    def set_http_threadpool_size(cls, num_threads):
        cls._http_threadpool_size = num_threads

    @classmethod
    def _ensure_http_threadpool(cls):
        if cls._http_threadpool is None:
            cls._http_threadpool = dxpy.utils.get_futures_threadpool(max_workers=cls._http_threadpool_size)

    def __init__(self, dxid=None, project=None, mode=None, request_size=DEFAULT_TABLE_WRITE_REQUEST_SIZE):
        DXDataObject.__init__(self, dxid=dxid, project=project)
        if mode is None:
            self._close_on_exit = True
        else:
            if mode not in ['r', 'w', 'a']:
                raise ValueError("mode must be one of 'r', 'w', or 'a'")
            self._close_on_exit = (mode == 'w')

        self._write_request_size = request_size
        self._row_buf = []
        self._read_row_buffer_size = DEFAULT_TABLE_READ_ROW_BUFFER_SIZE
        self._write_row_buffer_size = DEFAULT_TABLE_WRITE_ROW_BUFFER_SIZE
        self._string_row_buf = None
        self._http_threadpool_futures = set()
        self._columns, self._col_names = None, None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.flush()
        if self._close_on_exit:
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
        if len(self._row_buf) > 0 or (self._string_row_buf != None and self._string_row_buf.tell() > len('{"data": [')) or len(self._http_threadpool_futures) > 0:
            warn("=== WARNING! ===")
            warn("There is still unflushed data in the destructor of a DXGTable object!")
            warn("We will attempt to flush it now, but if an error were to occur, we could not report it back to you.")
            warn("Your program could fail to flush the data but appear to succeed.")
            warn("Instead, please call flush() or close(), or use the context managed version (e.g., with open_dxgtable(ID, mode='w') as gtable:)")
        try:
            self.flush(multithread=False)
        except Exception as e:
            warn("=== Exception occurred while flushing accumulated row data for %r" % (self._dxid,))
            traceback.print_exception(*sys.exc_info())
            raise

    def _check_row_is_valid(self, row):
        # TODO: if the user is using initFrom, we don't know what the schema looks like
        # (self._columns is None). In that case we can't do any local checks of the data's
        # validity.
        if self._columns is None:
            return
        if len(row) != len(self._columns):
            raise ValueError("Row has wrong number of columns (expected %d, got %d)" % (len(self._columns), len(row)))
        for index, (value, column) in enumerate(zip(row, self._columns)):
            if column['type'] == 'string':
                if not isinstance(value, basestring):
                    raise ValueError("Expected value in column %d to be a string, got %r instead" % (index, value))
            elif column['type'] == 'boolean':
                if value != True and value != False:
                    raise ValueError("Expected value in column %d to be a boolean, got %r instead" % (index, value))
            elif column['type'] == 'float' or column['type'] == 'double':
                if type(value) is not int and type(value) is not float:
                    raise ValueError("Expected value in column %d to be a number (int or float), got %r instead" % (index, value))
            elif column['type'].startswith('int') or column['type'].startswith('uint'):
                if type(value) is not int:
                    raise ValueError("Expected value in column %d to be an int, got %r instead" % (index, value))

    def _new(self, dx_hash, **kwargs):
        '''
        :param dx_hash: Standard hash populated in :func:`dxpy.bindings.DXDataObject.new()` containing attributes common to all data object classes.
        :type dx_hash: dict
        :param columns: An ordered list containing column descriptors. See :meth:`make_column_desc`.
        :type columns: list of column descriptors
        :param indices: An ordered list containing index descriptors. See :meth:`genomic_range_index()` and :meth:`lexicographic_index()`. If not provided, no indices are created.
        :type indices: list of index descriptors
        :param init_from: GTable from which to initialize the metadata including column and index specs
        :type init_from: :class:`DXGTable`

        Creates a new GTable with the columns described in *columns* and
        the indices described in *indices*.

        '''

        if "columns" in kwargs:
            if kwargs["columns"] is not None:
                dx_hash["columns"] = kwargs["columns"]
            del kwargs["columns"]
        else:
            if "init_from" not in kwargs:
                raise DXError("Column specs need to be specified if init_from is not used")

        if "indices" in kwargs:
            if kwargs["indices"] is not None:
                dx_hash["indices"] = kwargs["indices"]
            del kwargs["indices"]

        if "init_from" in kwargs:
            if kwargs["init_from"] is not None:
                if not isinstance(kwargs["init_from"], DXGTable):
                    raise DXError("Expected instance of DXGTable to init_from")
                dx_hash["initializeFrom"] = \
                    {"id": kwargs["init_from"].get_id(),
                     "project": kwargs["init_from"].get_proj_id()}
            del kwargs["init_from"]

        resp = dxpy.api.gtable_new(dx_hash, **kwargs)
        self.set_ids(resp["id"], dx_hash["project"])
        if "columns" in dx_hash:
            self._columns = dx_hash["columns"]

    def set_ids(self, dxid, project=None):
        '''
        :param dxid: Object ID
        :type dxid: string
        :param project: Project ID
        :type project: string

        Discards the currently stored ID and associates the handler with
        *dxid*. As a side effect, it also flushes the buffer for the
        previous GTable object if the buffer is nonempty.
        '''
        if self._dxid is not None:
            self.flush()

        DXDataObject.set_ids(self, dxid, project)

    def get_rows(self, query=None, columns=None, starting=None, limit=None, **kwargs):
        '''
        :param query: Query with which to filter the rows. See :meth:`genomic_range_query()` and :meth:`lexicographic_query()`.
        :type query: dict
        :param columns: List of column names to be included in the output. If not specified, each result contains the row ID followed by all column values. You can explicitly obtain the row ID by requesting the column ``__id__``.
        :type columns: list of strings
        :param starting: An optional row offset indicating the minimum row ID to return
        :type starting: integer
        :param limit: Maximum number of rows to be returned
        :type limit: integer
        :returns: A hash with the key-value pairs "length": the number of rows returned, "next": a value to use as "starting" in a subsequent query to get the next chunk of rows, and "data": a list of rows satisfying the query.

        Queries the GTable for rows matching the given query (or all
        rows, if no query is provided). If *columns* is set, the order
        of elements in the returned rows follows the ordering in
        *columns*. The *starting* and *limit* options restrict the row
        range of rows to return.

        Each row is returned as a list containing the row id followed by
        the values for each of the requested columns.

        .. note:: The
           :meth:`~dxpy.bindings.dxgtable.DXGTable.iterate_rows` and
           :meth:`~dxpy.bindings.dxgtable.DXGTable.iterate_query_rows`
           methods provide faster generator-based access to GTable rows.
           They transparently issue parallel background requests to
           obtain the data.

        Example::

            dxgtable = open_dxgtable("gtable-xxxx")
            query = genomic_range_query(chr="chromosome18", lo=30, hi=2049)
            for row in dxgtable.get_rows(query=query, limit=500):
                rowid = row[0]
                first_col_data = row[1]

        '''
        get_rows_params = {}
        if query is not None:
            get_rows_params['query'] = query
        if columns is not None:
            get_rows_params["columns"] = columns
        if starting is not None:
            get_rows_params["starting"] = starting
        if limit is not None:
            get_rows_params["limit"] = limit

        return dxpy.api.gtable_get(self._dxid, get_rows_params, always_retry=True, **kwargs)

    def get_columns(self, **kwargs):
        '''
        :returns: A list of column descriptors
        :rtype: list of dicts

        Returns a list of the descriptors for each of the GTable's
        columns.
        '''
        if self._columns is None:
            self._columns = self.describe(**kwargs).get("columns")
        return self._columns

    def get_col_names(self, **kwargs):
        '''
        :returns: A list of column names
        :rtype: list of strings

        Returns a list of the GTable's column names.
        '''
        if self._col_names is None:
            self._col_names = [col["name"] for col in self.get_columns(**kwargs)]
        return self._col_names

    def iterate_rows(self, start=0, end=None, columns=None, want_dict=False, **kwargs):
        """
        :param start: The row ID of the first row to return
        :type start: integer
        :param end: Return all rows before this row (return all rows until the end if None)
        :type end: integer or None
        :param columns: List of column names to be included in the output. If not specified, each result contains the row ID followed by all column values. You can explicitly obtain the row ID by requesting the column ``__id__``.
        :type columns: list of strings
        :param want_dict: If True, return a mapping of column names to values, instead of an array of values
        :type want_dict: boolean
        :rtype: generator

        Returns a generator that yields rows with IDs in the interval
        [*start*, *end*).

        """
        if want_dict:
            if columns is None:
                col_names = ['__id__'] + self.get_col_names(**kwargs)
            else:
                col_names = columns

        DXGTable._ensure_http_threadpool()

        request_iterator = self._generate_read_requests(start_row=start, end_row=end, columns=columns, **kwargs)

        for response in dxpy.utils.response_iterator(request_iterator, self._http_threadpool, max_active_tasks=self._http_threadpool_size):
            if want_dict:
                for row in response['data']:
                    yield dict(zip(col_names, row))
            else:
                for row in response['data']:
                    yield row

    def iterate_query_rows(self, query=None, columns=None, limit=None, want_dict=False, **kwargs):
        """
        :param query: Query with which to filter the rows. See :meth:`genomic_range_query()` and :meth:`lexicographic_query()`.
        :type query: dict
        :param columns: List of column names to be included in the output. If not specified, each result contains the row ID followed by all column values. You can explicitly obtain the row ID by requesting the column ``__id__``.
        :type columns: list of strings
        :param limit: Maximum number of rows to return (default is to return all matching rows)
        :type limit: int
        :param want_dict: If True, return a mapping of column names to values, instead of an array of values
        :type want_dict: boolean
        :rtype: generator

        Returns a generator that yields the rows of the table that match
        the given query parameters. If *query* is not given, all rows
        are returned in order of the row ID.

        Example::

            dxgtable = open_dxgtable(dxid)
            for row in dxgtable.iterate_query_rows(genomic_range_query(chr, lo, hi), [colname1, colname2]):
                print row

        """
        if want_dict:
            if columns is None:
                col_names = ['__id__'] + self.get_col_names(**kwargs)
            else:
                col_names = columns
        cursor = 0
        returned = 0
        while cursor is not None:
            if limit is not None and returned == limit:
                return
            resp = self.get_rows(query=query, columns=columns,
                                 starting=cursor,
                                 limit=(self._read_row_buffer_size if limit is None else min(limit - returned, self._read_row_buffer_size)),
                                 **kwargs)
            _buffer = resp['data']
            cursor = resp['next']
            if len(_buffer) < 1: break
            if want_dict:
                for row in _buffer:
                    returned += 1
                    yield dict(zip(col_names, row))
            else:
                for row in _buffer:
                    returned += 1
                    yield row

    def __iter__(self):
        return self.iterate_rows()

    # TODO: make this consume recarrays
    def add_rows(self, data, part=None, validate=True, **kwargs):
        '''
        :param data: List of rows to be added
        :type data: List of lists, list of mappings from column names to values (TODO), or mapping from column names to lists of values (TODO)
        :param part: The part ID to label the rows in data. Optional; it will be selected automatically if not given.
        :type part: integer
        :raises: :exc:`~dxpy.exceptions.DXGTableError`

        Adds the rows listed in data to the current GTable. If *part* is
        not given (recommended), rows may be queued up for addition
        internally and will be flushed to the remote server
        periodically.

        Example::

            with new_dxgtable([dxpy.DXGTable.make_column_desc("a", "string"),
                               dxpy.DXGTable.make_column_desc("b", "int32")], mode='w') as dxgtable:
                dxgtable.add_rows([["foo", 23], ["bar", 7]])

        '''

        if validate:
            for row in data:
                self._check_row_is_valid(row)
        if part is None:
            for row in data:
                self._row_buf.append(row)
                if len(self._row_buf) >= self._write_row_buffer_size:
                    self._flush_row_buf_to_string_buf()
                    if self._string_row_buf.tell() > self._write_request_size:
                        self._finalize_string_row_buf()
                        request_data = self._string_row_buf.getvalue()
                        self._string_row_buf = None
                        self._async_add_rows_request(self._dxid, request_data, jsonify_data=False, **kwargs)
                        del request_data
        else:
            dxpy.api.gtable_add_rows(self._dxid, {"data": data, "part": part}, **kwargs)

    def add_row(self, row, **kwargs):
        '''
        :param row: Row to be added
        :type data: List or mapping from column names to values (TODO)
        :raises: :exc:`~dxpy.exceptions.DXGTableError`

        Adds a single row to the current GTable. Rows may be queued up for addition internally
        and will be flushed to the remote server periodically.

        Example::

            with new_dxgtable([dxpy.DXGTable.make_column_desc("a", "string"),
                               dxpy.DXGTable.make_column_desc("b", "int32")], mode='w') as dxgtable:
                for i in range(1000):
                    dxgtable.add_row(["foo", i])
        '''
        self.add_rows([row], **kwargs)

    def get_unused_part_id(self, **kwargs):
        '''
        :returns: An unused part id
        :rtype: integer

        Obtains an unused part ID for use with
        :meth:`~dxpy.bindings.dxgtable.DXGTable.add_rows()`. Each call
        to this method on the same GTable (even from different clients)
        returns a different part ID.

        '''
        return dxpy.api.gtable_next_part(self._dxid, **kwargs)['part']

    def _flush_row_buf_to_string_buf(self):
        if self._string_row_buf == None:
            self._string_row_buf = StringIO()
            self._string_row_buf.write('{"data": [')

        if len(self._row_buf) > 0:
            self._string_row_buf.write(json.dumps(self._row_buf)[1:])
            self._string_row_buf.seek(-1, os.SEEK_END) # chop off trailing "]"
            self._string_row_buf.write(", ")
            self._row_buf = []

    def _finalize_string_row_buf(self, part_id=None):
        if part_id == None:
            part_id = self.get_unused_part_id()

        # Temporary debug
        self._string_row_buf.seek(-2, os.SEEK_END) # chop off trailing ", "
        tail = self._string_row_buf.read()
        if tail != ', ':
            self._string_row_buf.seek(-100, os.SEEK_END)
            tail = self._string_row_buf.read()
            raise Exception("Unexpected buffer state: _finalize_string_row_buf called twice. Buffer tail: "+tail)
        
        self._string_row_buf.seek(-2, os.SEEK_END) # chop off trailing ", "
        self._string_row_buf.write('], "part": %s}' % str(part_id))

    def flush(self, multithread=True, **kwargs):
        '''
        Sends any rows in the internal buffer to the API server. If the buffer is empty, does nothing.
        '''
        if len(self._row_buf) > 0:
            self._flush_row_buf_to_string_buf()
        if self._string_row_buf != None and self._string_row_buf.tell() > len('{"data": ['):
            self._finalize_string_row_buf()
            request_data = self._string_row_buf.getvalue()
            self._string_row_buf = None
            if multithread:
                self._async_add_rows_request(self._dxid, request_data, jsonify_data=False, **kwargs)
            else:
                dxpy.api.gtable_add_rows(self._dxid, request_data, jsonify_data=False, **kwargs)

        if len(self._http_threadpool_futures) > 0:
            dxpy.utils.wait_for_all_futures(self._http_threadpool_futures)
            try:
                for future in self._http_threadpool_futures:
                    if future.exception() != None:
                        raise future.exception()
            finally:
                self._http_threadpool_futures = set()

    def close(self, block=False, **kwargs):
        '''
        :param block: If True, blocks until the remote GTable has closed
        :type block: boolean

        Closes the GTable.

        '''
        self.flush(**kwargs)

        dxpy.api.gtable_close(self._dxid, **kwargs)

        if block:
            self._wait_on_close(**kwargs)

    def wait_on_close(self, timeout=3600*24*7, **kwargs):
        '''
        :param timeout: Maximum amount of time to wait until the GTable is closed
        :type timeout: integer
        :raises: :exc:`~dxpy.exceptions.DXError` if the timeout is reached before the remote GTable has been closed

        Waits until the remote GTable is closed.
        '''
        self._wait_on_close(timeout, **kwargs)

    @staticmethod
    def make_column_desc(name, typename):
        """
        :param name: Column name
        :type name: string
        :param typename: Data type for the column
        :type typename: string
        :returns: A specially formatted dict that represents a column in the API
        :rtype: dict

        Returns a column descriptor with the given name and type. See
        the `API specification for GenomicTables
        <https://wiki.dnanexus.com/API-Specification-v1.0.0/GenomicTables#Columns>`_
        for information about the available types and acceptable column
        names.

        """

        return {"name": name, "type": typename}

    @staticmethod
    def genomic_range_index(chr, lo, hi, name="gri"):
        """
        :param chr: Name of the column containing chromosome names; must be a column of type string
        :type chr: string
        :param lo: Name of the column containing the low boundary of a genomic interval; must be a column of integral type
        :type lo: string
        :param hi: Name of the column containing the high boundary of a genomic interval; must be a column of integral type
        :type hi: string
        :param name: Name of the index
        :type name: string
        :returns: A specially formatted dict that represents an index schema in the API
        :rtype: dict

        Returns a genomic range index descriptor, for use with the
        :meth:`_new` method.

        """
        return {"name": name, "type": "genomic",
                "chr": chr, "lo": lo, "hi": hi}

    @staticmethod
    def lexicographic_index(columns, name):
        """
        :param columns: list of columns to be indexed, in order
        :type columns: list of hashes, each one of the form returned by :meth:`lexicographic_index_column()`
        :param name: Name of the index
        :type name: string
        :returns: A specially formatted dict that represents an index schema in the API
        :rtype: dict

        Returns a lexicographic index descriptor, for use with the
        :meth:`_new` method.

        """
        for column in columns:
            if type(column) is list:
                warn("Warning: passing a list of lists to lexicographic_index() is deprecated, please use lexicographic_index_column instead.")

        return {"name": name, "type": "lexicographic", "columns": columns}

    @staticmethod
    def lexicographic_index_column(name, ascending=True, case_sensitive=None):
        """
        :param name: Name of the column to be indexed
        :type name: str
        :param ascending: Whether to order entries of this column in ascending order
        :type ascending: bool
        :param case_sensitive: if False, compare strings case-insensitively (default is True; only valid on string columns)
        :type case_sensitive: bool

        Returns a column descriptor for a lexicographic index, for use with the
        :meth:`lexicographic_index()` method.
        """
        result = {"name": name, "order": "asc" if ascending else "desc"}
        if case_sensitive is not None:
            result['caseSensitive'] = case_sensitive
        return result

    @staticmethod
    def genomic_range_query(chr, lo, hi, mode="overlap", index="gri"):
        """
        :param chr: Name of chromosome to be queried
        :type chr: string
        :param lo: Low boundary of query interval
        :type lo: integer
        :param hi: High boundary of query interval
        :type hi: integer
        :param mode: The type of query to perform ("overlap" or "enclose")
        :type mode: string
        :param index: Name of the genomic range index to use
        :type index: string
        :returns: A specially formatted dict that represents a GTable query in the API
        :rtype: dict

        Returns a query against a genomic range index of the table, for
        use with the :meth:`get_rows` or :meth:`iterate_query_rows`
        methods.

        """

        return {"index": index, "parameters": {"mode": mode,
                                               "coords": [chr, lo, hi] } }

    @staticmethod
    def lexicographic_query(query, index):
        """
        :param query: MongoDB-style query
        :type query: dict
        :param index: Name of the lexicographic index to use
        :type index: string
        :returns: A specially formatted dict that represents a GTable query in the API
        :rtype: dict

        Returns a query against a lexicographic index of the table, for
        use with the :meth:`get_rows` or :meth:`iterate_query_rows`
        methods.

        """

        return {"index": index, "parameters": query}

    def _async_add_rows_request(self, *args, **kwargs):
        kwargs['always_retry'] = True

        DXGTable._ensure_http_threadpool()

        while len(self._http_threadpool_futures) >= self._http_threadpool_size:
            future = dxpy.utils.wait_for_a_future(self._http_threadpool_futures)
            if future.exception() != None:
                raise future.exception()
            self._http_threadpool_futures.remove(future)
            del future

        future = self._http_threadpool.submit(dxpy.api.gtable_add_rows, *args, **kwargs)
        self._http_threadpool_futures.add(future)

    def _generate_read_requests(self, start_row=0, end_row=None, query=None, columns=None, **kwargs):
        if end_row is None:
            end_row = int(self.describe(**kwargs)['length'])
        kwargs['query'] = query
        kwargs['columns'] = columns
        cursor = start_row
        while cursor < end_row:
            request_size = min(self._read_row_buffer_size, end_row - cursor)
            my_kwargs = dict(kwargs)
            my_kwargs['starting'] = cursor
            my_kwargs['limit'] = request_size
            yield self.get_rows, [], my_kwargs
            cursor += request_size
