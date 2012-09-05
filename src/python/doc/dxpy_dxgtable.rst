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

Here is an example of writing to a new GTable via a context-managed GTable
handle::

  # Open a GTable for writing
  with open_dxgtable('table-xxxx', mode='w') as gtable:
      for line in input_file:
          gtable.add_row(line.split(','))

The use of the context-managed GTable is optional for read-only objects; that
is, you may use the object without a "with" block (and omit the "mode"
parameter), for example::

  # Open a GTable for reading
  gtable = open_dxgtable('table-xxxx')
  for row in gtable.get_rows(...):
      process(row)

.. warning:: If you write any data to a GTable (with
   :meth:`~dxpy.bindings.dxgtable.DXGTable.add_row` or
   :meth:`~dxpy.bindings.dxgtable.DXGTable.add_rows`) and you choose to use a
   non context-managed GTable handle, you **must** call
   :meth:`~dxpy.bindings.dxgtable.DXGTable.flush` or
   :meth:`~dxpy.bindings.dxgtable.DXGTable.close` when you are done, for
   example::

     # Open a GTable for writing; we will flush it explicitly ourselves
     gtable = open_dxgtable('table-xxxx')
     for line in input_file:
         gtable.add_row(line.split(','))
     gtable.flush()

   If you do not do so, and there is still unflushed data when the DXGTable
   object is garbage collected, the DXGTable will attempt to flush it then, in
   the destructor. However, any errors in the resulting API calls (or, in
   general, any exception in a destructor) will **not** be propagated back to
   your program! That is, *your writes can silently fail if you rely on the
   destructor to flush your data*.

   DXGTable will print a warning if it detects unflushed data as the destructor
   is running (but again, it will attempt to flush it anyway).

.. automodule:: dxpy.bindings.dxgtable_functions
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dxpy.bindings.dxgtable
   :members:
   :undoc-members:
   :show-inheritance:
