dxpy Package
============

This package includes three modules:

* :mod:`dxpy.bindings`: Contains useful Pythonic bindings for interacting with remote objects managed by the API server.  For convenience, this is automatically imported directly into the namespace under :mod:`dxpy` when :mod:`dxpy` is imported.
* :mod:`dxpy.api`: Contains low-level wrappers which can be called directly to make the respective API calls to the API server.
* :mod:`dxpy.exceptions`: Contains exceptions used in the other modules.

It has the following external dependencies:

* :mod:`requests`: To install on Linux, use 'sudo pip install
  requests'.  Other installation options can be found at
  http://docs.python-requests.org/en/latest/user/install

* :mod:`futures`: To install on Linux, use 'sudo pip install
  futures'.  Other installation options can be found at
  http://code.google.com/p/pythonfutures/


:mod:`dxpy` Package Initialization
----------------------------------

.. automodule:: dxpy
   :members:
   :undoc-members:

:mod:`bindings` Module
----------------------

.. automodule:: dxpy.bindings
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dxpy.bindings.dxdataobject_functions
   :members:
   :undoc-members:
   :show-inheritance:

Records
+++++++

.. automodule:: dxpy.bindings.dxrecord
   :members:
   :undoc-members:
   :show-inheritance:

File Bindings
+++++++++++++

.. automodule:: dxpy.bindings.dxfile_functions
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dxpy.bindings.dxfile
   :members:
   :undoc-members:
   :show-inheritance:

GenomicTable Bindings
+++++++++++++++++++++

.. automodule:: dxpy.bindings.dxgtable_functions
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dxpy.bindings.dxgtable
   :members:
   :undoc-members:
   :show-inheritance:

Applets, Apps, and Jobs
+++++++++++++++++++++++

.. automodule:: dxpy.app_builder
   :members:
   :undoc-members:

.. automodule:: dxpy.bindings.dxapplet
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dxpy.bindings.dxapp
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dxpy.bindings.dxjob
   :members:
   :undoc-members:
   :show-inheritance:

Search
++++++

.. automodule:: dxpy.bindings.search
   :members:
   :undoc-members:

:mod:`api` Module
-----------------

.. automodule:: dxpy.api_doc

.. automodule:: dxpy.api
   :members:
   :undoc-members:
   :show-inheritance:

:mod:`exceptions` Module
------------------------

.. automodule:: dxpy.exceptions
   :members:
   :undoc-members:
   :show-inheritance:
