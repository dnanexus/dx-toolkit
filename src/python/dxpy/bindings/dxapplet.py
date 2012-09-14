"""
DXApplet Handler
++++++++++++++++

Applets are data objects that store an executable and specifications for input,
output, and execution. They can be run by calling the :func:`DXApplet.run`
method.

"""

import dxpy
from dxpy.bindings import *

############
# DXApplet #
############

class DXApplet(DXDataObject):
    '''
    Remote applet object handler.

    .. automethod:: _new
    '''

    _class = "applet"

    _describe = staticmethod(dxpy.api.appletDescribe)
    _add_types = staticmethod(dxpy.api.appletAddTypes)
    _remove_types = staticmethod(dxpy.api.appletRemoveTypes)
    _get_details = staticmethod(dxpy.api.appletGetDetails)
    _set_details = staticmethod(dxpy.api.appletSetDetails)
    _set_visibility = staticmethod(dxpy.api.appletSetVisibility)
    _rename = staticmethod(dxpy.api.appletRename)
    _set_properties = staticmethod(dxpy.api.appletSetProperties)
    _add_tags = staticmethod(dxpy.api.appletAddTags)
    _remove_tags = staticmethod(dxpy.api.appletRemoveTags)
    _close = staticmethod(dxpy.api.appletClose)
    _list_projects = staticmethod(dxpy.api.appletListProjects)

    def _new(self, dx_hash, **kwargs):
        '''
        :param dx_hash: Standard hash populated in :func:`dxpy.bindings.DXDataObject.new()`
        :type dx_hash: dict
        :param runSpec: run specification
        :type runSpec: dict
        :param inputSpec: input specification (optional)
        :type inputSpec: dict
        :param outputSpec: output specification (optional)
        :type outputSpec: dict
        :param access: access specification (optional)
        :type access: dict
        :param dxapi: API version string
        :type dxapi: string
        :param title: title string (optional)
        :type title: string
        :param summary: summary string (optional)
        :type summary: string
        :param description: description string (optional)
        :type description: string

        .. note:: It is highly recommended that the higher-level module
           :mod:`dxpy.app_builder` or (preferably) its frontend
           `dx-build-applet <http://wiki.dnanexus.com/DxBuildApplet>`_ be used
           instead for applet creation.

        Creates an applet with the given parameters (see API
        documentation for the correct syntax).  The applet is not run
        until :meth:`run()` is called.

        '''
        for field in 'runSpec', 'dxapi':
            if field not in kwargs:
                raise DXError("%s: Keyword argument %s is required" % (self.__class__.__name__, field))
            dx_hash[field] = kwargs[field]
            del kwargs[field]
        for field in 'inputSpec', 'outputSpec', 'access', 'title', 'summary', 'description':
            if field in kwargs:
                dx_hash[field] = kwargs[field]
                del kwargs[field]

        resp = dxpy.api.appletNew(dx_hash, **kwargs)
        self.set_ids(resp["id"], dx_hash["project"])

    def get(self, **kwargs):
        """
        Returns the contents of the applet.
        """
        return dxpy.api.appletGet(self._dxid, **kwargs)

    def run(self, applet_input, project=None, folder="/", **kwargs):
        '''
        :param applet_input: Hash of the applet's input arguments
        :type applet_input: dict
        :param project: Project ID of the project context
        :type project: string
        :param folder: Folder in which applet's outputs will be placed in *project*
        :type folder: string
        :returns: Object handler of the created job now running the applet
        :rtype: :class:`~dxpy.bindings.dxjob.DXJob`

        Creates a new job that executes the function "main" of this applet with
        the given input *applet_input*.

        '''
        if project is None:
            project = dxpy.WORKSPACE_ID

        run_input = {"input": applet_input,
                     "folder": folder}

        if dxpy.JOB_ID is None:
            run_input["project"] = project

        return DXJob(dxpy.api.appletRun(self._dxid, run_input,
                                        **kwargs)["id"])
