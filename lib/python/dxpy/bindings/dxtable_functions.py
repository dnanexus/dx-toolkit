"""
Opening and Creating
********************

The following functions allow opening an existing remote table
(read-only) and creating new remote tables (write-only).  All these
methods return a remote table handler.

"""

from dxpy.bindings import *

def open_dxtable(dxid):
    '''
    :param dxid: table ID
    :type dxid: string
    :rtype: :class:`dxpy.bindings.DXTable`

    Given the object ID of an existing table, this function returns a
    DXTable object on which get_rows() can be called.

    Example::

      with open_dxtable("table-xxxx") as dxtable:
          for row in dxtable.get_rows():
	      print row[1] # Prints the value in the first column (after the row ID) for this row

    Note that this function is shorthand for the following::

        DXTable(dxid)

    '''

    return DXTable(dxid)

def new_dxtable(columns, chr_col=None, lo_col=None, hi_col=None):
    '''
    :param columns: An ordered list containing strings of the form "confidence:double" to indicate a column called "confidence" containing doubles.
    :type columns: list
    :param chr_col: Name of the column containing chromosome names; must be a column of type string
    :type chr_col: string
    :param lo_col: Name of the column containing the low boundary of a genomic interval; must be a column of type int32
    :type lo_col: string
    :param hi_col: Name of the column containing the high boundary of a genomic interval; must be a column of type int32
    :type hi_col: string
    :returns: Remote table handler for the created table
    :rtype: :class:`dxpy.bindings.DXTable`

    Creates a new remote table with the given column names in
    *columns*.  If *chr_col*, *lo_col*, and *hi_col* are given, the rows
    of the table will be indexed by a genomic range index when the
    table is closed.

    Example::

        with new_dxtable(["colname:string", "secondcolname:int32"]) as dxtable:
            dxtable.add_rows([["foo", 23], ["bar", 7]])

        indexedTable = new_dxtable(["chr:string", "lo:int32", "hi:int32"], "chr", "lo", "hi")

    Note that this function is shorthand for the following::

        dxtable = DXTable()
        dxtable.new(columns, chr_col, lo_col, hi_col)

    '''
    
    dxtable = DXTable()
    dxtable.new(columns, chr_col, lo_col, hi_col)
    return dxtable

def extend_dxtable(dxid, columns):
    '''
    :param dxid: Object ID of table to extend
    :type dxid: string
    :param columns: An ordered list containing strings of the form "confidence:double" to indicate a new column called "confidence" containing doubles.
    :type columns: list
    :rtype: :class:`dxpy.bindings.DXTable`

    Given the object ID of an existing table and a list of new columns
    with which to extend the table, this function creates a new remote
    table that is ready to be written to.

    Example::

        with extend_dxtable("table-xxxx", ["newcol:double", "anothernewcol:int32"]) as dxtable:
            dxtable.add_rows([[2.5498, 93]])

    Note that this function is shorthand for the following::

        DXTable(dxid).extend(columns)

    '''

    return DXTable(dxid).extend(columns)
