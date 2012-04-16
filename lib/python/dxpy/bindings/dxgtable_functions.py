"""
Opening and Creating
********************

The following functions allow opening an existing remote table
(read-only) and creating new remote tables (write-only).  All these
methods return a remote table handler.

"""

from dxpy.bindings import *

def open_dxgtable(dxid, **kwargs):
    '''
    :param dxid: table ID
    :type dxid: string
    :rtype: :class:`dxpy.bindings.DXGTable`

    Given the object ID of an existing table, this function returns a
    DXGTable object on which get_rows() can be called.

    Example::

      with open_dxgtable("table-xxxx") as dxgtable:
          for row in dxgtable.get_rows():
	      print row[1] # Prints the value in the first column (after the row ID) for this row

    Note that this function is shorthand for the following::

        DXGTable(dxid)

    '''

    return DXGTable(dxid, **kwargs)

def new_dxgtable(columns, **kwargs):
    '''
    :param columns: An ordered list containing strings of the form "confidence:double" to indicate a column called "confidence" containing doubles.
    :type columns: list
    :param indices: An ordered list containing index descriptors.  See :func:`dxpy.bindings.dxgtable.make_index_desc`
    :type indices: list of index descriptors
    :returns: Remote table handler for the created table
    :rtype: :class:`dxpy.bindings.DXGTable`

    Creates a new remote table with the given column names in
    *columns*.  If *chr_col*, *lo_col*, and *hi_col* are given, the rows
    of the table will be indexed by a genomic range index when the
    table is closed.

    Example::

        with new_dxgtable(["colname:string", "secondcolname:int32"]) as dxgtable:
            dxgtable.add_rows([["foo", 23], ["bar", 7]])

        indexedTable = new_dxgtable(["chr:string", "lo:int32", "hi:int32"], "chr", "lo", "hi")

    Note that this function is shorthand for the following::

        dxgtable = DXGTable()
        dxgtable.new(columns, chr_col, lo_col, hi_col)

    '''
    
    dxgtable = DXGTable()
    dxgtable.new(columns=columns, **kwargs)
    return dxgtable

def extend_dxgtable(dxid, columns, **kwargs):
    '''
    :param dxid: Object ID of table to extend
    :type dxid: string
    :param columns: An ordered list containing strings of the form "confidence:double" to indicate a new column called "confidence" containing doubles.
    :type columns: list
    :rtype: :class:`dxpy.bindings.DXGTable`

    Given the object ID of an existing table and a list of new columns
    with which to extend the table, this function creates a new remote
    table that is ready to be written to.

    Example::

        with extend_dxgtable("table-xxxx", ["newcol:double", "anothernewcol:int32"]) as dxgtable:
            dxgtable.add_rows([[2.5498, 93]])

    Note that this function is shorthand for the following::

        DXGTable(dxid).extend(columns)

    '''

    return DXGTable(dxid).extend(columns, **kwargs)
