dxpy Package
============

This Python 2.7 package includes three key modules:

* :mod:`dxpy.bindings`: Contains useful Pythonic bindings for
  interacting with remote objects via the DNAnexus API server. For
  convenience, this is automatically imported directly into the
  namespace under :mod:`dxpy` when :mod:`dxpy` is imported.

* :mod:`dxpy.api`: Contains low-level wrappers that can be called
  directly to make the respective API calls to the API server.

* :mod:`dxpy.exceptions`: Contains exceptions used in the other
  :mod:`dxpy` modules.

It has the following external dependencies:

* :mod:`requests`: To install on Linux, use ``sudo pip install
  requests``. Other installation options can be found at
  http://docs.python-requests.org/en/latest/user/install

* :mod:`futures`: To install on Linux, use ``sudo pip install futures``.
  Other installation options can be found at
  http://code.google.com/p/pythonfutures/


Package Initialization
----------------------

.. automodule:: dxpy
   :members:
   :undoc-members:
