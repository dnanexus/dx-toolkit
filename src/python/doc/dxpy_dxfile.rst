File Bindings
+++++++++++++

You can obtain a handle to a new or existing File object with
:func:`~dxpy.bindings.dxfile_functions.new_dxfile` or
:func:`~dxpy.bindings.dxfile_functions.open_dxfile`, respectively. Both return
a remote file handler, which is a file-like object.

Here is an example of writing to a new file object via a context-managed File
handle::

  # Open a file for writing
  with open_dxfile('file-xxxx', mode='w') as fd:
      for line in input_file:
          fd.write(line)

The use of the context-managed File is optional for read-only objects; that is,
you may use the object without a "with" block (and omit the "mode" parameter),
for example::

  # Open a file for reading
  fd = open_dxfile('file-xxxx')
  for line in fd:
      print line

.. warning:: If you write any data to a file and you choose to use a non
   context-managed File handle, you **must** call
   :meth:`~dxpy.bindings.dxfile.DXFile.flush` or
   :meth:`~dxpy.bindings.dxfile.DXFile.close` when you are done, for example::

     # Open a file for writing; we will flush it explicitly ourselves
     fd = open_dxfile('file-xxxx')
     for line in input_file:
         fd.write(line)
     fd.flush()

   If you do not do so, and there is still unflushed data when the DXFile
   object is garbage collected, the DXFile will attempt to flush it then, in
   the destructor. However, any errors in the resulting API calls (or, in
   general, any exception in a destructor) will **not** be propagated back to
   your program! That is, *your writes can silently fail if you rely on the
   destructor to flush your data*.

   DXFile will print a warning if it detects unflushed data as the destructor
   is running (but again, it will attempt to flush it anyway).

There are also helper functions
(:func:`~dxpy.bindings.dxfile_functions.download_dxfile`,
:func:`~dxpy.bindings.dxfile_functions.upload_local_file`, and
:func:`~dxpy.bindings.dxfile_functions.upload_string`) for directly downloading
and uploading existing files or strings as a whole.

.. automodule:: dxpy.bindings.dxfile_functions
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dxpy.bindings.dxfile
   :members:
   :undoc-members:
   :show-inheritance:
