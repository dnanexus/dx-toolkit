"""
DXGTable Handler
****************
"""

import os, sys, json
import cStringIO as StringIO
import concurrent.futures
from dxpy.bindings import *

# TODO: adaptive buffer size
DEFAULT_TABLE_ROW_BUFFER_SIZE = 40000
DEFAULT_TABLE_REQUEST_SIZE = 1024*1024*32 # bytes

class DXGTable(DXDataObject):
    '''Remote GenomicTable object handler

    .. automethod:: _new
    '''

    _class = "gtable"

    _describe = staticmethod(dxpy.api.gtableDescribe)
    _add_types = staticmethod(dxpy.api.gtableAddTypes)
    _remove_types = staticmethod(dxpy.api.gtableRemoveTypes)
    _get_details = staticmethod(dxpy.api.gtableGetDetails)
    _set_details = staticmethod(dxpy.api.gtableSetDetails)
    _set_visibility = staticmethod(dxpy.api.gtableSetVisibility)
    _rename = staticmethod(dxpy.api.gtableRename)
    _set_properties = staticmethod(dxpy.api.gtableSetProperties)
    _add_tags = staticmethod(dxpy.api.gtableAddTags)
    _remove_tags = staticmethod(dxpy.api.gtableRemoveTags)
    _close = staticmethod(dxpy.api.gtableClose)
    _list_projects = staticmethod(dxpy.api.gtableListProjects)

    _http_threadpool = None
    _http_threadpool_size = NUM_HTTP_THREADS

    @classmethod
    def set_http_threadpool_size(cls, num_threads):
        cls._http_threadpool_size = num_threads

    def __init__(self, dxid=None, project=None, keep_open=False,
                 request_size=DEFAULT_TABLE_REQUEST_SIZE):
        self._keep_open = keep_open

        self._request_size = request_size
        self._row_buf = []
        self._row_buffer_size = DEFAULT_TABLE_ROW_BUFFER_SIZE
        self._string_row_buf = None
        self._http_threadpool_futures = set()

        if dxid is not None:
            self.set_ids(dxid, project)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.flush()
        if not self._keep_open:
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

    def _new(self, dx_hash, **kwargs):
        '''
        :param dx_hash: Standard hash populated in :func:`dxpy.bindings.DXDataObject.new()`
        :type dx_hash: dict
        :param columns: An ordered list containing column descriptors.  See :meth:`make_column_desc` (required)
        :type columns: list of column descriptors
        :param indices: An ordered list containing index descriptors.  See :meth:`genomic_range_index()`, :meth:`lexicographic_index()`, and :meth:`substring_index()`. (optional)
        :type indices: list of index descriptors
        :param init_from: GTable from which to initialize the metadata including column and index specs
        :type init_from: :class:`GTable`

        Creates a new gtable with the given column names in *columns*
        and the indices described in *indices*.

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

        resp = dxpy.api.gtableNew(dx_hash, **kwargs)
        self.set_ids(resp["id"], dx_hash["project"])

    def set_ids(self, dxid, project=None):
        '''
        :param dxid: Object ID
        :type dxid: string
        :param project: Project ID
        :type project: string

        Discards the currently stored ID and associates the handler
        with *dxid*.  As a side effect, it also flushes the buffer for
        the previous gtable object if the buffer is nonempty.
        '''
        self.flush()

        DXDataObject.set_ids(self, dxid, project)

    def get_rows(self, query=None, columns=None, starting=None, limit=None, **kwargs):
        '''
        :param query: Query with which to fetch the rows; see :meth:`genomic_range_query()`, :meth:`lexicographic_query()`, :meth:`substring_query()`
        :type query: dict
        :param columns: List of columns to be included; all columns will be included if not set
        :type columns: list of strings
        :param starting: An optional offset indicating where the search should resume; this value only corresponds to a row ID if *query* is None, and it should otherwise either be set to 0 or to the value of "next" that was given in a previous call to :meth:`get_rows()`
        :type starting: integer
        :param limit: Max number of rows to be returned
        :type limit: integer
        :returns: A hash with the key-value pairs "length": the number of rows returned, "next": a value to use as "starting" to get the next chunk of rows, and "data": a list of rows satisfying the query.
        
        Queries the gtable for rows using the given parameters.  If
        *columns* is set, the order of elements in the returned rows
        follows the ordering in *columns*.  The *starting* and *limit*
        options restrict the search further.

        Note that a row will be returned as a list containing the row
        id and the values for each of the columns.

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

        return dxpy.api.gtableGet(self._dxid, get_rows_params, always_retry=True, **kwargs)

    def get_col_names(self, **kwargs):
        '''
        :returns: A list of column names
        :rtype: list of strings

        Queries the gtable for its columns and returns a list of all
        column names.
        '''
        desc = self.describe(**kwargs)
        col_names = []
        for col_desc in desc["columns"]:
            col_names.append(col_desc["name"])
        return col_names

    def iterate_rows(self, start=0, end=None, **kwargs):
        """
        :param start: The row ID of the first row to return
        :type start: integer
        :param end: Return all rows before this row (return all rows until the end if None)
        :type end: integer or None
        :rtype: generator

        Returns a generator which will yield the rows with IDs in the
        interval [*start*, *end*).

        """
        if self._http_threadpool is None:
            DXGTable._http_threadpool = concurrent.futures.ThreadPoolExecutor(max_workers=self._http_threadpool_size)

        request_iterator = self._generate_read_requests(start_row=start, end_row=end, get_rows_params={}, **kwargs)

        for response in dxpy.utils.response_iterator(request_iterator, self._http_threadpool, max_active_tasks=self._http_threadpool_size):
            for row in response['data']:
                yield row

    def iterate_query_rows(self, query=None, columns=None, **kwargs):
        """
        :param query: Query with which to fetch the rows; see :meth:`genomic_range_query()`, :meth:`lexicographic_query()`, :meth:`substring_query()`
        :type query: dict
        :param columns: List of columns to be included; all columns will be included if not set
        :type columns: list of strings
        :rtype: generator

        Returns a generator which iterates through the rows of the
        table while using the given query parameters.  If *query* is
        not given, all rows are returned in order of the row ID.

        Example::

            dxgtable = open_dxgtable(dxid)
            for row in dxgtable.iterate_query_rows(genomic_range_query(chr, lo, hi), [colname1, colname2]):
                print row

        """
        cursor = 0
        while cursor is not None:
            resp = self.get_rows(query=query, columns=columns,
                                 starting=cursor,
                                 limit=self._row_buffer_size)
            buffer = resp['data']
            cursor = resp['next']
            if len(buffer) < 1: break
            for row in buffer:
                yield row

    def __iter__(self):
        return self.iterate_rows()

    def extend(self, columns, indices=None, keep_open=False, **kwargs):
        '''
        :param columns: List of new column names
        :type columns: list of strings
        :param indices: An ordered list containing index descriptors.  See :meth:`genomic_range_index()`, :meth:`lexicographic_index()`, and :meth:`substring_index()`.
        :type indices: list of index descriptors
        :rtype: :class:`dxpy.bindings.DXGTable`

        Additional optional parameters not listed: all those under
        :func:`dxpy.bindings.DXDataObject.new`.

        Extends the current gtable object with the column names in
        *columns*, creating a new remote gtable as a result.  Returns
        the handler for this new gtable.  Note that any indices
        created for the original table are not automatically carried
        over to this new table, and any new indices for the new table
        must be given at creation time.

        '''
        dx_hash, remaining_kwargs = DXDataObject._get_creation_params(kwargs)
        dx_hash["columns"] = columns
        if indices is not None:
            dx_hash["indices"] = indices
        resp = dxpy.api.gtableExtend(self._dxid, dx_hash, **remaining_kwargs)
        return DXGTable(resp["id"], dx_hash["project"],
                        keep_open)

    def add_rows(self, data, part=None, **kwargs):
        '''
        :param data: List of rows to be added
        :type data: list of lists
        :param part: The part ID to label the rows in data. Optional; it will be selected automatically if not given.
        :type part: integer
        :raises: :exc:`dxpy.exceptions.DXGTableError`

        Adds the rows listed in data to the current gtable.  If *part*
        is not given, rows may be queued up for addition internally
        and will be flushed to the remote server periodically.

        Example::

            with new_dxgtable([dxpy.DXGTable.make_column_desc("a", "string"),
                               dxpy.DXGTable.make_column_desc("b", "int32")]) as dxgtable:
                dxgtable.add_rows([["foo", 23], ["bar", 7]])

        '''

        if part is None:
            for row in data:
                self._row_buf.append(row)
                if len(self._row_buf) >= self._row_buffer_size:
                    self._flush_row_buf_to_string_buf()
                    if self._string_row_buf.tell() > self._request_size:
                        self._finalize_string_row_buf()
                        self._async_add_rows_request(self._dxid, self._string_row_buf.getvalue(), jsonify_data=False, **kwargs)
                        self._string_row_buf = None
        else:
            dxpy.api.gtableAddRows(self._dxid, {"data": data, "part": part}, **kwargs)

    def add_row(self, row, **kwargs):
        '''
        :param row: Row to be added
        :type data: list
        :raises: :exc:`dxpy.exceptions.DXGTableError`

        Adds a single row to the current gtable. Rows may be queued up for addition internally
        and will be flushed to the remote server periodically.

        Example::

            with new_dxgtable([dxpy.DXGTable.make_column_desc("a", "string"),
                               dxpy.DXGTable.make_column_desc("b", "int32")]) as dxgtable:
                for i in range(1000):
                    dxgtable.add_row(["foo", i])
        '''
        self.add_rows([row], **kwargs)

    def get_unused_part_id(self, **kwargs):
        '''
        :returns: An unused part id
        :rtype: integer

        Queries the API server for an unused part ID.  The same part
        ID will not be returned more than once by this method.

        '''
        return dxpy.api.gtableNextPart(self._dxid, **kwargs)['part']

    def _flush_row_buf_to_string_buf(self):
        if self._string_row_buf == None:
            self._string_row_buf = StringIO.StringIO()
            self._string_row_buf.write('{"data": [')

        if len(self._row_buf) > 0:
            self._string_row_buf.write(json.dumps(self._row_buf)[1:])
            self._string_row_buf.seek(-1, os.SEEK_END) # chop off trailing "]"
            self._string_row_buf.write(", ")
            self._row_buf = []

    def _finalize_string_row_buf(self, part_id=None):
        if part_id == None:
            part_id = self.get_unused_part_id()
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
            if multithread:
                self._async_add_rows_request(self._dxid, self._string_row_buf.getvalue(), jsonify_data=False, **kwargs)
            else:
                dxpy.api.gtableAddRows(self._dxid, self._string_row_buf.getvalue(), jsonify_data=False, **kwargs)
            self._string_row_buf = None

        if len(self._http_threadpool_futures) > 0:
            concurrent.futures.wait(self._http_threadpool_futures)
            for future in self._http_threadpool_futures:
                if future.exception() != None:
                    raise future.exception()
            self._http_threadpool_futures = set()

    def close(self, block=False, **kwargs):
        '''
        :param block: Indicates whether this function should block until the remote gtable has closed or not.
        :type block: boolean

        Closes the gtable.

        '''
        self.flush(**kwargs)

        dxpy.api.gtableClose(self._dxid, **kwargs)

        if block:
            self._wait_on_close(**kwargs)

    def wait_on_close(self, timeout=sys.maxint, **kwargs):
        '''
        :param timeout: Max amount of time to wait until the gtable is closed.
        :type timeout: integer
        :raises: :exc:`dxpy.exceptions.DXError` if the timeout is reached before the remote gtable has been closed

        Wait until the remote gtable is closed.
        '''
        self._wait_on_close(timeout, **kwargs)

    @staticmethod
    def make_column_desc(name, type_):
        """
        :param name: Column name
        :type name: string
        :param type_: Data type for the column (one of "boolean", "uint8", "int32", "int64", "float", "double", "string")
        :type type_: string

        Returns a column descriptor with the given name and type.

        """

        return {"name": name, "type": type_}

    @staticmethod
    def genomic_range_index(chr, lo, hi, name="gri"):
        """
        :param chr: Name of the column containing chromosome names; must be a column of type string
        :type chr: string
        :param lo: Name of the column containing the low boundary of a genomic interval; must be a column of type int32
        :type lo: string
        :param hi: Name of the column containing the high boundary of a genomic interval; must be a column of type int32
        :type hi: string
        :param name: Name of the index
        :type name: string

        Creates a genomic range index descriptor for use with the new() call.

        """
        return {"name": name, "type": "genomic",
                "chr": chr, "lo": lo, "hi": hi}

    @staticmethod
    def lexicographic_index(columns, name):
        """
        :param columns: Required parameter for a lexicographic index: Ordered list of lists of the form [<column name>, "ASC"|"DESC"]
        :type columns: list of lists containing two strings each
        :param name: Name of the index
        :type name: string

        Creates a lexicographic index descriptor for use with the new() call.

        """

        return {"name": name, "type": "lexicographic",
                "columns": columns}

    @staticmethod
    def substring_index(column, name):
        """
        :param column: Column name to index by
        :type column: string
        :param name: Name of the index
        :type name: string

        Creates a substring index descriptor for use with the new() call.

        """
        return {"name": name, "type": "substring", "column": column}

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

        Constructs a query for a genomic range index of the table.

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

        Constructs a query for a lexicographic index of the table.

        """

        return {"index": index, "parameters": query}

    @staticmethod
    def substring_query(string, mode, index):
        """
        :param string: String to match
        :type string: string
        :param mode: Mode in which to match the string ("equal", "substring", or "prefix")
        :type mode: string
        :param index: Name of the substring index to use
        :type index: string

        Constructs a query for a substring index of the table.

        """
        query = {"index": index, "parameters": {} }
        if mode == "equal":
            query["parameters"]["$eq"] = string
        elif mode == "substring":
            query["parameters"]["$substr"] = string
        elif mode == "prefix":
            query["parameters"]["$prefix"] = string
        else:
            raise DXGTableError("Unrecognized substring index query mode: " + \
                                str(mode))
        return query

    def _async_add_rows_request(self, *args, **kwargs):
        if self._http_threadpool is None:
            DXGTable._http_threadpool = concurrent.futures.ThreadPoolExecutor(max_workers=self._http_threadpool_size)

        while len(self._http_threadpool_futures) >= self._http_threadpool_size:
            future = concurrent.futures.as_completed(self._http_threadpool_futures).next()
            if future.exception() != None:
                raise future.exception()
            self._http_threadpool_futures.remove(future)

        future = self._http_threadpool.submit(dxpy.api.gtableAddRows, *args, **kwargs)
        self._http_threadpool_futures.add(future)

    # TODO: deal with invalid start_row, end_row
    def _generate_read_requests(self, start_row=0, end_row=None, get_rows_params={}, **kwargs):
        kwargs['always_retry'] = True
        if end_row is None:
            end_row = int(self.describe(**kwargs)['length'])
        cursor = start_row
        while cursor < end_row:
            request_size = min(self._row_buffer_size, end_row - cursor)
            my_get_rows_params = dict(get_rows_params)
            my_get_rows_params['starting'] = cursor
            my_get_rows_params['limit'] = request_size
            yield dxpy.api.gtableGet, [self._dxid, my_get_rows_params], kwargs
            cursor += request_size
