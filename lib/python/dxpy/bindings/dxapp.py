"""
DXApp Handler
+++++++++++++++++

Apps are data objects which provide a way for programs to be distributed to users in the system.
They store an executable and specifications for input, output, execution, access, and billing. 
They can be run by calling the :func:`DXApp.run` method.

"""

from dxpy.bindings import *

#########
# DXApp #
#########

class DXApp(DXDataObject):
    '''
    Remote app object handler

    .. automethod:: _new
    '''

    _class = "app"

    _describe = staticmethod(dxpy.api.appDescribe)

    def _new(self, dx_hash, **kwargs):
        '''
        :param dx_hash: Standard hash populated in :func:`dxpy.bindings.DXDataObject.new()`
        :type dx_hash: dict
        :param program: ID of the program which the app will be created from
        :type program: string
        :param subtitle: A short description of the app (optional)
        :type subtitle: string
        :param description: An extended description of the app (optional)
        :type description: string
        :param version: app's version
        :type version: string
        :param owner: ID of the user or organization who will own the app (optional if an app with this name already exists)
        :type owner: string
        :param billing: billing specification (optional)
        :type billing: dict
        :param access: access specification (optional)
        :type access: dict
        :param globalWorkspace: Contents to be put into the app's global workspace
        :type globalWorkspace: array

        It is highly recommended that :mod:`dxpy.program_builder` is used for program and app creation.

        Creates an app with the given parameters (see API documentation for the correct syntax).  The app is only
        available to its developers until :meth:`publish()` is called, and is not run until :meth:`run()` is called.

        '''
        for field in 'program', 'globalWorkspace', 'version':
            if field not in kwargs:
                raise DXError("%s: Keyword argument %s is required" % (self.__class__.__name__, field))
            dx_hash[field] = kwargs[field]
            del kwargs[field]

        for field in 'subtitle', 'description', 'owner', 'billing', 'access':
            if field in kwargs:
                dx_hash[field] = kwargs[field]
                del kwargs[field]

        resp = dxpy.api.appNew(dx_hash, **kwargs)
        self.set_ids(resp["id"], None)

    def update(self, **kwargs):
        '''
        :param program: ID of the program to replace the app's contents with
        :type program: string
        :param billing: billing specification (optional)
        :type billing: dict
        :param access: access specification (optional)
        :type access: dict
        :param globalWorkspace: Contents to be put into the app's global workspace
        :type globalWorkspace: array

        Update parameters of an existing app.

        '''
        updates = {}
        for field in 'program', 'billing', 'access', 'globalWorkspace':
            if field in kwargs:
                updates[field] = kwargs[field]
                del kwargs[field]

        resp = dxpy.api.appUpdate(self._dxid, updates, **kwargs)

    def addTags(self, tags, **kwargs):
        """
        :param tags: Tags to add to the app
        :type tags: array

        Adds application name tags (aliases) to this app.
        """
        return dxpy.api.appAddTags(self._dxid, tags, **kwargs)

    def removeTag(self, **kwargs):
        """
        Remove the application name tag (alias) that the app is being addressed by.
        """
        return dxpy.api.appRemoveTag(self._dxid, **kwargs)

    def install(self, **kwargs):
        """
        Installs the app in the current user's account.
        """
        return dxpy.api.appInstall(self._dxid, **kwargs)

    def uninstall(self, **kwargs):
        """
        Uninstalls the app from the current user's account.
        """
        return dxpy.api.appUninstall(self._dxid, **kwargs)

    def get(self, **kwargs):
        """
        Returns the contents of the app.
        """
        return dxpy.api.appGet(self._dxid, **kwargs)

    def publish(self, **kwargs):
        """
        Publishes the app, so all users can find it on the platform.
        """
        return dxpy.api.appPublish(self._dxid, **kwargs)

    def destroy(self, **kwargs):
        """
        Removes this app object from the platform.
        """
        return dxpy.api.appDestroy(self._dxid, **kwargs)

    def run(self, app_input, project=None, folder="/", **kwargs):
        '''
        :param app_input: Hash of the app's input arguments
        :type app_input: dict
        :param project: Project ID of the project context
        :type project: string
        :param folder: Folder in which the app's outputs will be placed in *project*
        :type folder: string
        :returns: Object handler of the created job now running the app
        :rtype: :class:`dxpy.bindings.DXJob`

        Creates a new job to execute the function "main" of this app
        with the given input *app_input*.

        '''
        if project is None and "DX_JOB_ID" not in os.environ:
            project = self._proj

        return DXJob(dxpy.api.appRun(self._dxid, {"input": app_input,
                                                  "project": project,
                                                  "folder": folder},
                                     **kwargs)["id"])
