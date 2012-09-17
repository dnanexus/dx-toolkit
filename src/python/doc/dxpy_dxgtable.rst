GenomicTables
+++++++++++++

A GenomicTable (GTable) is an immutable tabular dataset suitable for
large-scale genomic applications.

You can obtain a handle to a new or existing GTable object with
:func:`~dxpy.bindings.dxgtable_functions.new_dxgtable` or
:func:`~dxpy.bindings.dxgtable_functions.open_dxgtable`, respectively.

GTables are tristate objects:

* When initially created, a GTable is in the "open" state and row data
  can be written to it. After you have written your row data to the
  GTable, call the :meth:`~dxpy.bindings.dxgtable.DXGTable.close`
  method.
* The GTable enters the "closing" state while it is finalized in the
  platform.
* Some time later, the GTable enters the "closed" state and can be read.

Many methods that return a :class:`~dxpy.bindings.dxgtable.DXGTable`
object take a *mode* parameter. In general the available modes are as
follows (some methods create a new GTable and consequently do not
support immediate reading from it with the "r" mode):

* "r" (read): for read-only access. No data is expected to be written to
  the GTable.
* "w" (write): for writing. When the object exits scope, all buffers are
  flushed and GTable closing commences.
* "a" (append): for writing. When the object exits scope, all buffers
  are flushed but the GTable is left open.

.. note:: The automatic flush and close operations implied by the "w" or
   "a" modes **only** happen if the
   :class:`~dxpy.bindings.dxgtable.DXGTable` object is used in a Python
   context-managed scope (see the following examples).

The "w" mode can be used to create a new GTable, populate it, and close
it within a single process. The "a" mode leaves the GTable open after
all data is written. You can use it when you wish to open a GTable for
writing in multiple sub-jobs in parallel. (You then need to close the
table, once, after all those jobs have finished.)

Here is an example of writing to a GTable via a context-managed GTable
handle::

  # Open a GTable for writing
  with open_dxgtable('table-xxxx', mode='w') as gtable:
      for line in input_file:
          gtable.add_row(line.split(','))

The use of the context-managed GTable is optional for read-only objects;
that is, you may use the object without a "with" block (and omit the
*mode* parameter), for example::

  # Open a GTable for reading
  gtable = open_dxgtable('table-xxxx')
  for row in gtable.iterate_rows(...):
      process(row)

.. warning:: If you write any data to a GTable (with
   :meth:`~dxpy.bindings.dxgtable.DXGTable.add_row` or
   :meth:`~dxpy.bindings.dxgtable.DXGTable.add_rows`) and you choose to
   use a non context-managed GTable handle, you **must** call
   :meth:`~dxpy.bindings.dxgtable.DXGTable.flush` or
   :meth:`~dxpy.bindings.dxgtable.DXGTable.close` when you are done, for
   example::

     # Open a GTable for writing; we will flush it explicitly ourselves
     gtable = open_dxgtable('table-xxxx')
     for line in input_file:
         gtable.add_row(line.split(','))
     gtable.flush()

   If you do not do so, and there is still unflushed data when the
   :class:`~dxpy.bindings.dxgtable.DXGTable` object is garbage
   collected, the :class:`~dxpy.bindings.dxgtable.DXGTable` will attempt
   to flush it then, in the destructor. However, any errors in the
   resulting API calls (or, in general, any exception in a destructor)
   are **not** propagated back to your program! That is, *your writes
   can silently fail if you rely on the destructor to flush your data*.

   :class:`~dxpy.bindings.dxgtable.DXGTable` will print a warning if it
   detects unflushed data as the destructor is running (but again, it
   will attempt to flush it anyway).

.. note:: Writing to a GTable with the "w" mode calls
   :meth:`~dxpy.bindings.dxgtable.DXGTable.close` but does not wait for
   the GTable to finish closing. If the GTable you are writing is one of
   the outputs of your app or applet, you can use `job-based object
   references
   <http://wiki.dnanexus.com/API-Specification-v1.0.0/Jobs#Job-based-Object-References>`_,
   which will make downstream jobs wait for closing to finish before
   they can begin. However, if you intend to subsequently read from the
   GTable in the same process, you will need to call
   :meth:`~dxpy.bindings.dxgtable.DXGTable.wait_on_close` to ensure the
   GTable is ready to be read.

.. automodule:: dxpy.bindings.dxgtable_functions
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dxpy.bindings.dxgtable
   :members:
   :undoc-members:
   :show-inheritance:
