"""
Opening and Creating
********************

The following functions allow opening an existing remote table
(read-only) and creating new remote tables (write-only).  All these
methods return a remote table handler.

"""

from dxpy.bindings import *

def open_dxgtable(dxid, project=None):
    '''
    :param dxid: table ID
    :type dxid: string
    :rtype: :class:`dxpy.bindings.dxgtable.DXGTable`

    Given the object ID of an existing table, this function returns a
    DXGTable object on which get_rows() can be called.

    Example::

      with open_dxgtable("table-xxxx") as dxgtable:
          for row in dxgtable.get_rows():
	      print row[1] # Prints the value in the first column (after the row ID) for this row

    Note that this function is shorthand for the following::

        DXGTable(dxid)

    '''

    return DXGTable(dxid, project=project)

def new_dxgtable(columns=None, indices=None, init_from=None, keep_open=False,
                 **kwargs):
    '''
    :param columns: An ordered list containing column descriptors.  See :meth:`dxpy.bindings.dxgtable.DXGTable.make_column_desc` (required if init_from is not provided)
    :type columns: list of column descriptors
    :param indices: An ordered list containing index descriptors.  See :func:`dxpy.bindings.dxgtable.DXGTable._new` for more details. (optional)
    :type indices: list of index descriptors
    :param init_from: GTable from which to initialize the metadata including column and index specs
    :type init_from: :class:`GTable`
    :returns: Remote table handler for the created table
    :rtype: :class:`dxpy.bindings.dxgtable.DXGTable`

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`.

    Creates a new remote table with the given columns.  If indices are
    given, the GenomicTable will be indexed by the requested indices
    when closed.

    Example (after importing dxpy)::

        col_descs = [dxpy.DXGTable.make_column_desc("a", "string"),
                     dxpy.DXGTable.make_column_desc("b", "int32")]
        with new_dxgtable(columns=col_descs) as dxgtable:
            dxgtable.add_rows([["foo", 23], ["bar", 7]])

        gri_cols = [dxpy.DXGTable.make_column_desc("chr", "string"),
                    dxpy.DXGTable.make_column_desc("lo", "int32"),
                    dxpy.DXGTable.make_column_desc("hi", "int32")]
        gri_index = dxpy.DXGTable.genomic_range_index("chr", "lo", "hi")
        indexedTable = new_dxgtable(columns=gri_cols, indices=[gri_index])

    Note that this function is shorthand for the following::

        dxgtable = DXGTable()
        dxgtable.new(columns, **kwargs)

    '''
    
    dxgtable = DXGTable(keep_open=keep_open)
    dxgtable.new(columns=columns, indices=indices, init_from=init_from, **kwargs)
    return dxgtable

def extend_dxgtable(dxid, columns, indices=None, keep_open=False, **kwargs):
    '''
    :param dxid: Object ID of table to extend
    :type dxid: string
    :param columns: An ordered list containing column descriptors.  See :meth:`dxpy.bindings.dxgtable.DXGTable.make_column_desc` (required)
    :type columns: list of column descriptors
    :param indices: An ordered list containing index descriptors.  See :func:`dxpy.bindings.dxgtable.DXGTable.extend` for more details. (optional)
    :type indices: list of index descriptors
    :rtype: :class:`dxpy.bindings.dxgtable.DXGTable`

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`.

    Given the object ID of an existing table and a list of new columns
    with which to extend the table, this function creates a new remote
    table that is ready to be written to.

    Example::

        new_cols = [dxpy.DXGTable.make_column_desc("newcol", "double"),
                    dxpy.DXGTable.make_column_desc("anothercol", "int32")]
        with extend_dxgtable(old_dxgtable.get_id(), columns=new_cols, name="extended") as dxgtable:
            dxgtable.add_rows([[2.5498, 93]])

    Note that this function is shorthand for the following::

        DXGTable(dxid).extend(columns, **kwargs)

    '''

    return DXGTable(dxid).extend(columns, indices,
                                 keep_open, **kwargs)
