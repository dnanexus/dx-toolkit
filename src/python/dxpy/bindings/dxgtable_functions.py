# Copyright (C) 2013-2015 DNAnexus, Inc.
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
Helper Functions
****************

The following functions allow opening an existing remote table (for
reading or writing) and creating new remote tables (write-only). All of
these methods return a remote table handler.

"""

from __future__ import (print_function, unicode_literals)

from . import DXGTable

def open_dxgtable(dxid, project=None, mode=None):
    '''
    :param dxid: table ID
    :type dxid: string
    :param mode: One of "r", "w", or "a" for read, write, and append modes, respectively
    :type mode: string
    :rtype: :class:`~dxpy.bindings.dxgtable.DXGTable`

    Given the object ID of an existing table, returns a
    :class:`~dxpy.bindings.dxgtable.DXGTable` object for reading (with
    :meth:`~dxpy.bindings.dxgtable.DXGTable.get_rows`) or writing (with
    :meth:`~dxpy.bindings.dxgtable.DXGTable.add_row` or
    :meth:`~dxpy.bindings.dxgtable.DXGTable.add_rows`).

    Example::

      with open_dxgtable("gtable-xxxx") as dxgtable:
          for row in dxgtable.get_rows():
              print row[1] # Prints the value in the first column (after the row ID) for this row

    Note that this function is shorthand for the following::

        DXGTable(dxid)

    '''

    return DXGTable(dxid, project=project, mode=mode)

def new_dxgtable(columns=None, indices=None, init_from=None, mode=None, **kwargs):
    '''
    :param columns: An ordered list containing column descriptors.  See :meth:`~dxpy.bindings.dxgtable.DXGTable.make_column_desc` (required if *init_from* is not provided)
    :type columns: list of column descriptors
    :param indices: An ordered list containing index descriptors. See description in :func:`~dxpy.bindings.dxgtable.DXGTable._new`.
    :type indices: list of index descriptors
    :param init_from: GTable from which to initialize the metadata including column and index specs
    :type init_from: :class:`~dxpy.bindings.dxgtable.DXGTable`
    :param mode: One of "w" or "a" for write and append modes, respectively
    :type mode: string
    :returns: Remote table handler for the newly created table
    :rtype: :class:`~dxpy.bindings.dxgtable.DXGTable`

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`.

    Creates a new remote GTable with the given columns. If indices are
    given, the GTable will be indexed by the requested indices at the
    time that the table is closed.

    Example::

        col_descs = [dxpy.DXGTable.make_column_desc("a", "string"),
                     dxpy.DXGTable.make_column_desc("b", "int32")]
        with new_dxgtable(columns=col_descs, mode='w') as dxgtable:
            dxgtable.add_rows([["foo", 23], ["bar", 7]])

        gri_cols = [dxpy.DXGTable.make_column_desc("chr", "string"),
                    dxpy.DXGTable.make_column_desc("lo", "int32"),
                    dxpy.DXGTable.make_column_desc("hi", "int32")]
        gri_index = dxpy.DXGTable.genomic_range_index("chr", "lo", "hi")
        indexed_table = new_dxgtable(columns=gri_cols, indices=[gri_index])

    Note that this function is shorthand for the following::

        dxgtable = DXGTable()
        dxgtable.new(columns, **kwargs)

    '''

    dxgtable = DXGTable(mode=mode)
    dxgtable.new(columns=columns, indices=indices, init_from=init_from, **kwargs)
    return dxgtable
