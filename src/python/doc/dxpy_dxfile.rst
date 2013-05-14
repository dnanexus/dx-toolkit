Files
+++++

Files can be used to store an immutable opaque sequence of bytes.

You can obtain a handle to a new or existing file object with
:func:`~dxpy.bindings.dxfile_functions.new_dxfile` or
:func:`~dxpy.bindings.dxfile_functions.open_dxfile`, respectively. Both
return a remote file handler, which is a Python file-like object. There
are also helper functions
(:func:`~dxpy.bindings.dxfile_functions.download_dxfile`,
:func:`~dxpy.bindings.dxfile_functions.upload_local_file`, and
:func:`~dxpy.bindings.dxfile_functions.upload_string`) for directly
downloading and uploading existing files or strings in a single
operation.

Files are tristate objects:

* When initially created, a file is in the "open" state and data can be
  written to it. After you have written your data to the file, call the
  :meth:`~dxpy.bindings.dxfile.DXFile.close` method.
* The file enters the "closing" state while it is finalized in the
  platform.
* Some time later, the file enters the "closed" state and can be read.

Many methods that return a :class:`~dxpy.bindings.dxfile.DXFile` object
take a *mode* parameter. In general the available modes are as follows
(some methods create a new file and consequently do not support
immediate reading from it with the "r" mode):

* "r" (read): for read-only access. No data is expected to be written to
  the file.
* "w" (write): for writing. When the object exits scope, all buffers are
  flushed and closing commences.
* "a" (append): for writing. When the object exits scope, all buffers
  are flushed but the file is left open.

.. note:: The automatic flush and close operations implied by the "w" or
   "a" modes **only** happen if the
   :class:`~dxpy.bindings.dxfile.DXFile` object is used in a Python
   context-managed scope (see the following examples).

Here is an example of writing to a file object via a context-managed
file handle::

  # Open a file for writing
  with open_dxfile('file-xxxx', mode='w') as fd:
      for line in input_file:
          fd.write(line)

The use of the context-managed file is optional for read-only objects;
that is, you may use the object without a "with" block (and omit the
*mode* parameter), for example::

  # Open a file for reading
  fd = open_dxfile('file-xxxx')
  for line in fd:
      print line

.. warning:: If you write any data to a file and you choose to use a non
   context-managed file handle, you **must** call
   :meth:`~dxpy.bindings.dxfile.DXFile.flush` or
   :meth:`~dxpy.bindings.dxfile.DXFile.close` when you are done, for
   example::

     # Open a file for writing; we will flush it explicitly ourselves
     fd = open_dxfile('file-xxxx')
     for line in input_file:
         fd.write(line)
     fd.flush()

   If you do not do so, and there is still unflushed data when the
   :class:`~dxpy.bindings.dxfile.DXFile` object is garbage collected,
   the :class:`~dxpy.bindings.dxfile.DXFile` will attempt to flush it
   then, in the destructor. However, any errors in the resulting API
   calls (or, in general, any exception in a destructor) are **not**
   propagated back to your program! That is, *your writes can silently
   fail if you rely on the destructor to flush your data*.

   :class:`~dxpy.bindings.dxfile.DXFile` will print a warning if it
   detects unflushed data as the destructor is running (but again, it
   will attempt to flush it anyway).

.. note:: Writing to a file with the "w" mode calls
   :meth:`~dxpy.bindings.dxfile.DXFile.close` but does not wait for the
   file to finish closing. If the file you are writing is one of the
   outputs of your app or applet, you can use `job-based object
   references
   <https://wiki.dnanexus.com/API-Specification-v1.0.0/Jobs#Job-based-Object-References>`_,
   which will make downstream jobs wait for closing to finish before
   they can begin. However, if you intend to subsequently read from the
   file in the same process, you will need to call
   :meth:`~dxpy.bindings.dxfile.DXFile.wait_on_close` to ensure the file
   is ready to be read.

.. automodule:: dxpy.bindings.dxfile_functions
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dxpy.bindings.dxfile
   :members:
   :undoc-members:
   :show-inheritance:
