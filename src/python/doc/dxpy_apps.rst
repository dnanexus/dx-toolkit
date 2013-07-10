Applets, Apps, Workflows, and Jobs
++++++++++++++++++++++++++++++++++

An executable (applet or app) defines application logic that is to be
run in the DNAnexus Platform's Execution Environment. In order to
facilitate parallel processing, an executable may define multiple
functions (or *entry points*) that can invoke each other; a running job
can use :meth:`~dxpy.bindings.dxjob.new_dxjob` to invoke any function
defined in the same executable, creating a new job that runs that
function on a different machine (possibly even launching multiple such
jobs in parallel).

To create an executable from scratch, we encourage you to use the
command-line tools `dx-app-wizard
<https://wiki.dnanexus.com/Developer-Tutorials/Intro-to-Building-Apps>`_
and `dx build
<https://wiki.dnanexus.com/Command-Line-Client/Index-of-dx-Commands#build>`_
rather than using
the API or bindings directly. The following handlers for applets, apps,
and jobs are most useful for running preexisting executables and
monitoring their resulting jobs.

Workflows created from the website UI can also be run using the
:class:`~dxpy.bindings.dxworkflow.DXWorkflow` workflow handler.

.. automodule:: dxpy.bindings.dxapplet
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dxpy.bindings.dxapp
   :members:
   :undoc-members:
   :show-inheritance:

For **DXApp.run()**, see :meth:`~dxpy.bindings.dxapplet.DXExecutable.run`.

.. automodule:: dxpy.bindings.dxworkflow
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dxpy.bindings.dxjob
   :members:
   :undoc-members:
   :show-inheritance:
