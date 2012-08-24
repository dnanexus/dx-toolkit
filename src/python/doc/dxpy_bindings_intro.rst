:mod:`dxpy.bindings` Module
---------------------------

Documentation on classes and methods:

.. toctree::
   :maxdepth: 9

   dxpy_bindings
   dxpy_functions
   dxpy_dxproject
   dxpy_dxrecord
   dxpy_dxfile
   dxpy_dxgtable
   dxpy_apps
   dxpy_search

This module contains useful Python bindings for calling API methods on
the DNAnexus platform. Data objects (such as records, files,
GenomicTables, tables, and applets) can be represented locally by a
handler that inherits from the abstract class :class:`DXDataObject`.
This abstract base class supports functionality common to all of the
data object classes--for example, setting properties and types, as
well as removing the object from a project, moving it to a different
folder in the project, or cloning it to a different project.  Note
that while this documentation will largely refer to data containers as
simply "projects", both project and container IDs can be used and
will be returned as appropriate.

A remote handler for a data object always has two IDs associated with
it: one ID representing the underlying data, and a project ID to
indicate which project's copy it represents.  The ID of a data object
remains the same regardless of whether it is moved within a project or
cloned to another project.  To access a preexisting object, a remote
handler for that class can be set up via two methods: the constructor
or the :meth:`dxpy.bindings.DXDataObject.set_ids` method.  For example::

    dxFileHandle = DXFile("file-1234")

    dxOtherFH = DXFile()
    dxOtherFH.set_ids("file-4321")

Both these methods do not perform API calls and merely set the state
of the remote file handler.  The object ID and project ID stored in
the handler can be overwritten with subsequent calls to
:meth:`dxpy.bindings.DXDataObject.set_ids`.

Creation of a new object can be performed using the method
:meth:`DXDataObject.new` which usually has a different
specification for each subclass of :class:`DXDataObject`
that can take in class-specific arguments::

    newDXFileHandle = DXFile()
    newDXFileHandle.new(media_type="application/json")

Additional functions that are shorthand for some of these common use
cases are provided for some of the classes.  For instance, there is a
function for opening a preexisting remote file
(:func:`dxpy.bindings.dxfile_functions.open_dxfile`), and one for opening a new file to
be modified (:func:`dxpy.bindings.dxfile_functions.new_dxfile`), both of which
return a remote object handler on which the other methods can be
called.

In addition, class-specific handlers such as
:class:`dxpy.bindings.dxfile.DXFile` provide extra functionality for the
respective class.  For example, in the case of files, reading,
writing, downloading, and uploading files are all supported.  

Though not explicitly documented in each method as such, all methods
which interact with the API server may raise the exception
:exc:`dxpy.exceptions.DXAPIError`.
