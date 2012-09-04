GenomicTable Bindings
+++++++++++++++++++++

You can obtain a handle to a new or existing GenomicTable object with
:func:`~dxpy.bindings.dxgtable_functions.new_dxgtable` or
:func:`~dxpy.bindings.dxgtable_functions.open_dxgtable`, respectively. The
"mode" parameter to either of these functions allows you to use the resulting
object in a Python context manager. The modes are interpreted as follows:

- "r": for read-only access. The GTable is immutable and no cleanup is needed.
- "w": at the exit of the block, any data to be added is flushed and table closing commences.
- "a": at the exit of the block, any data to be added is flushed and the table is left open.

The "w" mode can be used to create a new GTable, populate it, and close it
within a single process. The "a" mode leaves the GTable open after all data is
written. You can use it when you wish to open a GTable for writing in multiple
sub-jobs in parallel. (You then need to close the table, once, after all those
jobs have finished.)

Here is an example of a context-managed GTable handle::

  with open_dxgtable('table-xxxx', mode='w') as gtable:
      for line in input_file:
          gtable.add_row(line.split(','))

The use of the context-managed GTable is optional; that is, you may use the
object without a "with" block (and omit the "mode" parameter). However, if you
write any data to a GTable using a non context-managed GTable handle, you must
call :meth:`~dxpy.bindings.dxgtable.DXGTable.flush` or
:meth:`~dxpy.bindings.dxgtable.DXGTable.close` explicitly yourself::

  gtable = open_dxgtable('table-xxxx')
  for line in input_file:
      gtable.add_row(line.split(','))
  gtable.flush()


.. automodule:: dxpy.bindings.dxgtable_functions
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dxpy.bindings.dxgtable
   :members:
   :undoc-members:
   :show-inheritance:
