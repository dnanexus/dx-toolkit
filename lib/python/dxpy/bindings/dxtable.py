"""
DXTable Handler
***************
"""

import json
from dxpy.bindings import *

class DXTable(DXClass):
    '''Remote table object handler'''

    _class = "table"

    _describe = staticmethod(dxpy.api.tableDescribe)
    _get_properties = staticmethod(dxpy.api.tableGetProperties)
    _set_properties = staticmethod(dxpy.api.tableSetProperties)
    _add_types = staticmethod(dxpy.api.tableAddTypes)
    _remove_types = staticmethod(dxpy.api.tableRemoveTypes)
    _destroy = staticmethod(dxpy.api.tableDestroy)

    _keep_open = False

    _row_buf = ""
    # Default maximum buffer size is 100MB
    _row_buf_maxsize = 1024*1024*100
    _part_index = 0

    def __init__(self, dxid=None, keep_open=False, buffer_size=40000):
        if dxid is not None:
            self.set_id(dxid)
        self._keep_open = keep_open
        self._bufsize = buffer_size

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if not self._keep_open:
            self.close()

    def __del__(self):
        if len(self._row_buf) > 0:
            self.flush()

    def new(self, columns, chr_col=None, lo_col=None, hi_col=None):
        '''
        :param columns: An ordered list containing strings of the form "confidence:double" to indicate a column called "confidence" containing doubles.
        :type columns: list
        :param chr_col: Name of the column containing chromosome names; must be a column of type string
        :type chr_col: string
        :param lo_col: Name of the column containing the low boundary of a genomic interval; must be a column of type int32
        :type lo_col: string
        :param hi_col: Name of the column containing the high boundary of a genomic interval; must be a column of type int32
        :type hi_col: string

        Creates a new table with the given column names in *columns*.
        If *chr_col*, *lo_col*, and *hi_col* are given, the rows of the
        table will be indexed by a genomic range index when the table
        is closed.

        '''

        table_params = {"columns": columns}
        try:
            indexStr = chr_col + "." + lo_col + "." + hi_col
            table_params['index'] = indexStr
        except:
            pass
        resp = dxpy.api.tableNew(table_params)
        self.set_id(resp["id"])


    def set_id(self, dxid):
        '''
        :param dxid: Object ID
        :type dxid: string
        :raises: :exc:`dxpy.exceptions.DXError` if *dxid* does not match class type

        Discards the currently stored ID and associates the handler
        with *dxid*.  As a side effect, it also flushes the buffer for
        the previous table object if the buffer is nonempty.
        '''
        if len(self._row_buf) > 0:
            self.flush()

        DXClass.set_id(self, dxid)

        # Reset state
        self._part_index = 0

    def get_rows(self, chr=None, lo=None, hi=None, columns=None, starting=None, limit=None):
        '''
        :param chr: Name of chromosome to be queried
        :type chr: string
        :param lo: Low boundary of query interval
        :type lo: integer
        :param hi: High boundary of query interval
        :type hi: integer
        :param columns: List of columns to be included; all columns will be included if not set
        :type columns: list of strings
        :param starting: Lowest row ID to be returned
        :type starting: integer
        :param limit: Max number of rows to be returned
        :type limit: integer
        :rtype: generator
        
        Queries the table for rows using the given parameters.  If the
        table has been built with a genomic range index, results will
        be rows that have an interval which overlaps with [*lo*, *hi*]
        on the chromosome *chr*.  If *columns* is not set, all columns
        will be included, and data is returned in the order in which
        columns were specified for the table.  If *columns* is set,
        the order of elements in the returned rows follows the
        ordering in *columns*.  The *starting* and *limit* options
        restrict the search further, but it should be noted that this
        method returns a generator and will attempt to pre-fetch rows
        in batches.

        Note that a row will be returned as a list containing the row
        id and the values for each of the columns.

        Example::

            dxtable = open_dxtable("table-xxxx")
            for row in dxtable.get_rows(chr="chromosome18", 30, 2049):
                rowid = row[0]
                first_col_data = row[1]

        '''
        get_rows_params = {}
        if columns is not None:
            get_rows_params["columns"] = columns
        if starting is not None:
            get_rows_params["starting"] = starting
        if limit is not None:
            get_rows_params["limit"] = limit

        try:
            query = chr + "." + lo + "." + hi
            get_rows_params['query'] = query
        except:
            pass

        return dxpy.api.tableGet(self._dxid, get_rows_params)

    def __iter__(self):
        cursor = 0
        nrows = int(self.describe()['size'])

        while cursor < nrows:
            buffer = self.get_rows(starting=cursor, limit=self._bufsize)['data']
            for row in buffer:
                yield row
            cursor += self._bufsize

    def extend(self, columns):
        '''
        :param columns: List of new column names
        :type columns: list of strings
        :rtype: :class:`dxpy.bindings.DXTable`

        Extends the current table object with the column names in
        *columns*, creating a new remote table as a result.  Returns
        the handler for this new table.

        '''
        resp = dxpy.api.tableExtend(self._dxid, {"columns": columns})
        return DXTable(resp["id"])

    def add_rows(self, data, index=None):
        '''
        :param data: List of rows to be added
        :type data: list of list
        :raises: :exc:`dxpy.exceptions.DXTableError`

        Adds the rows listed in data to the current table.  If *index*
        is not given, rows may be queued up for addition internally
        and will be flushed to the remote server periodically.

        Example::

            with new_dxtable(["colname:string", "secondcolname:int32"]) as dxtable:
                dxtable.add_rows([["foo", 23], ["bar", 7]])

        '''

        if index is None:
            for row in data:
                rowjson = json.dumps(row)
                if self._row_buf == "":
                    self._row_buf = rowjson
                else:
                    self._row_buf += ", "
                    self._row_buf += rowjson

                if len(self._row_buf) >= self._row_buf_maxsize:
                    self.flush()
        else:
            dxpy.api.tableAddRows(self._dxid, {"data": data, "index": index})

    def get_unused_part_index(self):
        '''
        :returns: An unused part index
        :rtype: integer

        Queries the API server for a part index that has not yet been
        used to upload table rows.  Note that calling this function
        will internally mark the returned part index as used, and so
        it should not be called if the value will not be used.

        '''
        desc = self.describe()
        if len(desc["parts"]) == 250000:
            raise DXTableError("250000 part indices already used.")
        self._part_index += 1
        while self._part_index <= 250000:
            if str(self._part_index) not in desc["parts"]:
                return self._part_index

        raise DXTableError("Usable part index not found.")

    def flush(self):
        '''
        Sends any rows in the internal buffer to the API server.  
        '''
        dxpy.api.tableAddRows(self._dxid,
                              '{"data": [' + self._row_buf + '], "index":' + \
                                  str(self.get_unused_part_index())+'}',
                              jsonify_data=False)
        self._row_buf = []
        self._row_buf_size = 0

    def close(self, block=False):
        '''
        :param block: Indicates whether this function should block until the remote table has closed or not.
        :type block: boolean

        Closes the table.

        '''
        if len(self._row_buf) > 0:
            self.flush()

        dxpy.api.tableClose(self._dxid)
        
        if block:
            self._wait_on_close()

    def wait_on_close(self, timeout=sys.maxint):
        '''
        :param timeout: Max amount of time to wait until the table is closed.
        :type timeout: integer
        :raises: :exc:`dxpy.exceptions.DXError` if the timeout is reached before the remote table has been closed

        Wait until the remote table is closed.
        '''
        self._wait_on_close(timeout)
