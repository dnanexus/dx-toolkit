:mod:`dxpy.bindings` Module
---------------------------

Documentation on classes and methods:

.. toctree::
   :maxdepth: 1

   dxpy_bindings
   dxpy_functions
   dxpy_dxproject
   dxpy_dxrecord
   dxpy_dxfile
   dxpy_apps
   dxpy_search

This module contains useful Python bindings for calling API methods on
the DNAnexus Platform. Data objects (such as records, files, and
applets) are represented locally by a handler that inherits from the
abstract class :class:`~dxpy.bindings.DXDataObject`. This abstract base
class supports functionality common to all of the data object
classes--for example, setting properties and types, as well as cloning
the object to a different project, moving it to a different folder in
the same project, or removing the object from a project.

.. note:: While this documentation will largely refer to data containers
   as simply "projects", both project and container IDs can generally be
   provided as input and will be returned as output anywhere a "project"
   is expected, except in methods of the
   :class:`~dxpy.bindings.dxproject.DXProject` class specifically or
   where otherwise noted.

.. rubric:: Object and Project IDs

A remote handler for a data object has two IDs associated with it: one
ID representing the underlying data and a project ID to indicate which
project's copy it represents. (If not explicitly specified for a
particular handler, the project defaults to the default data container.)
The ID of a data object remains the same when it is moved within a
project or cloned to another project.

The project ID is **only** relevant when using certain metadata fields
that are tied to a particular project. These are the name, properties,
and tags fields, and are read and updated using the following methods:
:meth:`~dxpy.bindings.DXDataObject.describe` ("name", "properties", and
"tags" fields), :meth:`~dxpy.bindings.DXDataObject.rename`,
:meth:`~dxpy.bindings.DXDataObject.get_properties`,
:meth:`~dxpy.bindings.DXDataObject.set_properties`,
:meth:`~dxpy.bindings.DXDataObject.add_tags`,
:meth:`~dxpy.bindings.DXDataObject.remove_tags`.

.. rubric:: Creating new handlers and remote objects

To access a preexisting object, a remote handler for that class can be
set up via two methods: the constructor or the
:meth:`~dxpy.bindings.DXDataObject.set_ids` method. For example::

    # Provide ID in constructor
    dxFileHandle = DXFile("file-1234")

    # Provide no ID initially, then call set_ids()
    dxOtherFH = DXFile()
    dxOtherFH.set_ids("file-4321")

Neither of these methods perform API calls; they merely set the local
state of the remote file handler. The object ID and project ID stored in
the handler can be overwritten with subsequent calls to
:meth:`~dxpy.bindings.DXDataObject.set_ids`.

The object handler ``__init__`` methods do not create new remote
objects; they only initialize whatever local state the handler needs.
Creation of a new remote object can be performed using the method
:meth:`dxpy.bindings.DXDataObject.new`. In each subclass of
:class:`~dxpy.bindings.DXDataObject` the method can take class-specific
arguments, for example::

    newDXFileHandle = DXFile()
    newDXFileHandle.new(media_type="application/json")

Some of the classes provide additional functions that are shorthand for
some of these common use cases. For instance,
:func:`dxpy.bindings.dxfile_functions.open_dxfile` opens a preexisting
file, and :func:`dxpy.bindings.dxfile_functions.new_dxfile` creates a
new file and opens it for writing. Both of those methods return a remote
object handler on which additional methods can be called.

In addition, class-specific handlers provide extra functionality for
their respective classes. For example,
:class:`~dxpy.bindings.dxfile.DXFile` provides functionality for
reading, writing, downloading, and uploading files.

Though not explicitly documented in each method as such, all methods
that interact with the API server may raise the exception
:exc:`dxpy.exceptions.DXAPIError`.

.. rubric:: Thread safety

:mod:`dxpy.bindings` are designed for single threaded use, however, it
is possible to use multiple threads on different bindings. For
example, using two threads to modify an applet is not allowed, but
using two threads to download two different files is allowed. Note
that the Python multiprocessing library, in its default process pool
mode, is incompatible with dxpy. In order to use that library, please
employ the thread pool instead.

