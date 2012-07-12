"""
DXApp Handler
+++++++++++++

Apps are data objects which provide a way for programs to be distributed to users in the system.
They store an executable and specifications for input, output, execution, access, and billing. 
They can be run by calling the :func:`DXApp.run` method.

"""

import dxpy
from dxpy.bindings import *

#########
# DXApp #
#########

class DXApp(object):
    '''
    Remote app object handler

    '''

    _class = "app"

    def __init__(self, dxid=None, name=None, alias=None):
        if dxid is not None or name is not None:
            self.set_id(dxid=dxid, name=name, alias=alias)

    def set_id(self, dxid=None, name=None, alias=None):
        '''
        :param dxid: App ID
        :type dxid: string
        :param name: App name
        :type name: string
        :param alias: App version or tag
        :type alias: string
        :raises: :exc:`dxpy.exceptions.DXError` if *dxid* and some other input are both given or if neither *dxid* nor *name* are given

        Discards the currently stored ID and associates the handler
        with the requested parameters.  Note that if *dxid* is given,
        the other fields should not be given, and if *name* is given,
        *alias* has default value "default".

        '''
        self._dxid = None
        self._name = None
        self._alias = None
        if dxid is not None:
            if name is not None or alias is not None:
                raise DXError("Did not expect name or alias to be given if dxid is given")
            if re.match("app-[0-9a-zA-Z]{24}", dxid) is None or \
                    len(dxid) != len('app') + 25:
                raise DXError("Given app ID does not match expected format")
            self._dxid = dxid
        elif name is not None:
            self._name = name
            if alias is not None:
                self._alias = alias
            else:
                self._alias = 'default'
        else:
            raise DXError("Did not expect name or alias to be given if dxid is given")

    def get_id(self):
        if self._dxid is not None:
            return self._dxid
        else:
            return 'app-' + self._name + '/' + self._alias

    def new(self, **kwargs):
        '''
        :param program: ID of the program which the app will be created from
        :type program: string
        :param name: Name of the app (optional; inherited from name if not given)
        :type name: string
        :param title: Title or brand name of the app (optional)
        :type title: string
        :param summary: A short description of the app (optional)
        :type summary: string
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
        :param globalWorkspace: Contents to be put into the app's global workspace (existing project ID or list of object IDs)
        :type globalWorkspace: string or array

        It is highly recommended that :mod:`dxpy.program_builder` is used for program and app creation.

        Creates an app with the given parameters (see API documentation for the correct syntax).  The app is only
        available to its developers until :meth:`publish()` is called, and is not run until :meth:`run()` is called.

        '''
        dx_hash = {}
        for field in 'program', 'version':
            if field not in kwargs:
                raise DXError("%s: Keyword argument %s is required" % (self.__class__.__name__, field))
            dx_hash[field] = kwargs[field]
            del kwargs[field]

        for field in 'name', 'title', 'summary', 'description', 'owner', 'billing', 'access', 'globalWorkspace':
            if field in kwargs:
                dx_hash[field] = kwargs[field]
                del kwargs[field]

        resp = dxpy.api.appNew(dx_hash, **kwargs)
        self.set_id(dxid=resp["id"])

    def describe(self, **kwargs):
        if self._dxid is not None:
            return dxpy.api.appDescribe(self._dxid, **kwargs)
        else:
            return dxpy.api.appDescribe('app-' + self._name, alias=self._alias, **kwargs)

    def update(self, **kwargs):
        '''
        :param program: ID of the program to replace the app's contents with
        :type program: string
        :param details: Metadata to store with the app
        :type details: dict or list
        :param billing: billing specification (optional)
        :type billing: dict
        :param access: access specification (optional)
        :type access: dict
        :param globalWorkspace: Contents to be put into the app's global workspace
        :type globalWorkspace: array

        Update parameters of an existing app.

        '''
        updates = {}
        for field in 'program', 'billing', 'access', 'globalWorkspace', 'details':
            if field in kwargs:
                updates[field] = kwargs[field]
                del kwargs[field]

        if self._dxid is not None:
            resp = dxpy.api.appUpdate(self._dxid, input_params=updates, **kwargs)
        else:
            resp = dxpy.api.appUpdate('app-' + self._name, alias=self._alias,
                                      input_params=updates, **kwargs)

    def addTags(self, tags, **kwargs):
        """
        :param tags: Tags to add to the app
        :type tags: array

        Adds application name tags (aliases) to this app.
        """
        if self._dxid is not None:
            return dxpy.api.appAddTags(self._dxid, input_params=tags, **kwargs)
        else:
            return dxpy.api.appAddTags('app-' + self._name, alias=self._alias,
                                       input_params=tags, **kwargs)

    def removeTags(self, **kwargs):
        """
        :param tags: Tags to remove from the app
        :type tags: array

        Remove the application name tags (aliases) that the app is
        being addressed by.
        """
        if self._dxid is not None:
            return dxpy.api.appRemoveTags(self._dxid, **kwargs)
        else:
            return dxpy.api.appRemoveTags('app-' + self._name, alias=self._alias, **kwargs)

    def install(self, **kwargs):
        """
        Installs the app in the current user's account.
        """
        if self._dxid is not None:
            return dxpy.api.appInstall(self._dxid, **kwargs)
        else:
            return dxpy.api.appInstall('app-' + self._name, alias=self._alias, **kwargs)

    def uninstall(self, **kwargs):
        """
        Uninstalls the app from the current user's account.
        """
        if self._dxid is not None:
            return dxpy.api.appUninstall(self._dxid, **kwargs)
        else:
            return dxpy.api.appUninstall('app-' + self._name, alias=self._alias, **kwargs)

    def get(self, **kwargs):
        """
        Returns the contents of the app.
        """
        if self._dxid is not None:
            return dxpy.api.appGet(self._dxid, **kwargs)
        else:
            return dxpy.api.appGet('app-' + self._name, alias=self._alias, **kwargs)

    def publish(self, **kwargs):
        """
        Publishes the app, so all users can find it on the platform.
        """
        if self._dxid is not None:
            return dxpy.api.appPublish(self._dxid, **kwargs)
        else:
            return dxpy.api.appPublish('app-' + self._name, alias=self._alias, **kwargs)

    def delete(self, **kwargs):
        """
        Removes this app object from the platform.
        """
        if self._dxid is not None:
            return dxpy.api.appDelete(self._dxid, **kwargs)
        else:
            return dxpy.api.appDelete('app-' + self._name, alias=self._alias, **kwargs)

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
        if project is None:
            project = dxpy.WORKSPACE_ID

        run_input = {"input": app_input,
                     "folder": folder}

        if dxpy.JOB_ID is None:
            run_input["project"] = project

        if self._dxid is not None:
            return DXJob(dxpy.api.appRun(
                    self._dxid,
                    input_params=run_input,
                    **kwargs)["id"])
        else:
            return DXJob(dxpy.api.appRun(
                    'app-' + self._name, alias=self._alias,
                    input_params=run_input,
                    **kwargs)["id"])
