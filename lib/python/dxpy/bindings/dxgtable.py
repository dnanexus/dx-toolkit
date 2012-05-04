"""
DXGTable Handler
****************
"""

import cStringIO as StringIO
import json
from dxpy.bindings import *

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

    # Default maximum buffer size is 20MB
    _row_buf_maxsize = 1024*1024*20

    def __init__(self, dxid=None, project=None, keep_open=False,
                 buffer_size=40000, part_id_min=0, part_id_max=250000):
        self._keep_open = keep_open
        self._bufsize = buffer_size
        self._row_buf = StringIO.StringIO()
        self._part_id_min = part_id_min
        self._part_id_max = part_id_max
        self._part_id = self._part_id_min
        if dxid is not None:
            self.set_ids(dxid, project)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if not self._keep_open:
            self.close()
        if self._row_buf.tell() > 0:
            self.flush()

    def __del__(self):
        if self._row_buf.tell() > 0:
            self.flush()

    def _new(self, dx_hash, **kwargs):
        '''
        :param dx_hash: Standard hash populated in :func:`dxpy.bindings.DXDataObject.new()`
        :type dx_hash: dict
        :param columns: An ordered list containing column descriptors.  See :meth:`make_column_desc` (required)
        :type columns: list of column descriptors
        :param indices: An ordered list containing index descriptors.  See :meth:`genomic_range_index()`, :meth:`lexicographic_index()`, and :meth:`substring_index()`. (optional)
        :type indices: list of index descriptors

        Creates a new gtable with the given column names in *columns*
        and the indices described in *indices*.

        '''

        dx_hash["columns"] = kwargs["columns"]
        del kwargs["columns"]

        if "indices" in kwargs:
            if kwargs["indices"] is not None:
                dx_hash["indices"] = kwargs["indices"]
            del kwargs["indices"]

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
        if self._row_buf.tell() > 0:
            self.flush()

        DXDataObject.set_ids(self, dxid, project)

        # Reset state
        self._part_id = self._part_id_min

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
        :returns: A hash with the key-value pairs "size": the number of rows returned, "next": a value to use as "starting" to get the next chunk of rows, and "data": a list of rows satisfying the query.
        
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

        return dxpy.api.gtableGet(self._dxid, get_rows_params, **kwargs)

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
        :param end: The row ID of the last row to return (to the end if None)
        :type end: integer or None
        :rtype: generator

        Returns a generator which will yield the rows with IDs in the
        interval [*start*, *end*).

        """

        if end is None:
            end = int(self.describe(**kwargs)['size'])
        cursor = start
        while cursor < end:
            request_size = self._bufsize
            if end is not None:
                request_size = min(request_size, end - cursor)
            buffer = self.get_rows(starting=cursor, limit=request_size, **kwargs)['data']
            if len(buffer) < 1: break
            for row in buffer:
                yield row
                cursor += 1
                if cursor >= end: break

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
                                 limit=self._bufsize)
            buffer = resp['data']
            cursor = resp['next']
            if len(buffer) < 1: break
            for row in buffer:
                yield row

    def __iter__(self, **kwargs):
        return self.iterate_rows(**kwargs)

    def extend(self, columns, indices=None, keep_open=False,
               buffer_size=40000, part_id_min=0, part_id_max=250000,
               **kwargs):
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
                        keep_open, buffer_size, part_id_min, part_id_max)

    def add_rows(self, data, part=None, **kwargs):
        '''
        :param data: List of rows to be added
        :type data: list of list
        :param part: The part ID to label the rows in data.
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
                rowjson = json.dumps(row)
                if self._row_buf.tell() > 0:
                    self._row_buf.write(", ")
                self._row_buf.write(rowjson)
                if self._row_buf.tell() >= self._row_buf_maxsize:
                    self.flush()
        else:
            dxpy.api.gtableAddRows(self._dxid, {"data": data, "part": part}, **kwargs)

    def get_unused_part_id(self, **kwargs):
        '''
        :returns: An unused part id
        :rtype: integer

        Queries the API server for a part ID that has not yet been
        used to upload gtable rows.  Note that calling this function
        will internally mark the returned part ID as used, and so it
        should not be called if the value will not be used.

        '''
        desc = self.describe(**kwargs)
        if len(desc["parts"]) > self._part_id_max:
            raise DXGTableError("All available part indices already used.")

        while self._part_id <= self._part_id_max:
            self._part_id += 1
            if str(self._part_id) not in desc["parts"]:
                return self._part_id
        
        raise DXGTableError("Usable part ID not found.")

    def flush(self, **kwargs):
        '''
        Sends any rows in the internal buffer to the API server.  
        '''
        dxpy.api.gtableAddRows(self._dxid,
                              '{"data": [' + self._row_buf.getvalue() + '], "part":' + \
                                  str(self.get_unused_part_id())+'}',
                              jsonify_data=False, **kwargs)

        self._row_buf.close()
        self._row_buf = StringIO.StringIO()

    def close(self, block=False, **kwargs):
        '''
        :param block: Indicates whether this function should block until the remote gtable has closed or not.
        :type block: boolean

        Closes the gtable.

        '''
        if self._row_buf.tell() > 0:
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
