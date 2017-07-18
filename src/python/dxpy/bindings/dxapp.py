# Copyright (C) 2013-2016 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

"""
DXApp Handler
+++++++++++++

Apps allow for application logic to be distributed to users in the
system, and they allow for analyses to be run in a reproducible and
composable way.

Apps extend the functionality of applets to require input/output
specifications as well as to allow for versioning, collaborative
development, and policies for billing and data access. Similarly to
applets, apps can be run by calling their
:meth:`~dxpy.bindings.dxapp.DXApp.run` method.

Unlike applets, apps are not data objects and do not live in projects.
Instead, they share a single global namespace. An app may have multiple
different versions (e.g. "1.0.0", "1.0.1", etc.) associated with a
single name (which is of the form "app-APPNAME"). A particular version
of an app may be identified in two ways, either by specifying a
combination of its name and a version (or a *tag*), or by specifying its
unique identifier.

Each app has a list of developers, which are the users that are
authorized to publish new versions of an app; perform administrative
tasks, such as assigning categories, and attaching new tags to versions
of the app; and add or remove other developers. When the first version
of an app with a given name is created, the creating user initially
becomes the sole developer of the app.

"""

from __future__ import print_function, unicode_literals, division, absolute_import

import dxpy
from . import DXObject, DXExecutable, DXJob, verify_string_dxid
from ..exceptions import DXError
from ..compat import basestring

#########
# DXApp #
#########

_app_required_keys = ['name', 'title', 'summary', 'dxapi', 'openSource',
                      'version', 'inputSpec', 'outputSpec', 'runSpec',
                      'developers', 'authorizedUsers', 'regionalOptions']

# These are optional keys for apps, not sure what to do with them
_app_optional_keys = ['details', 'categories', 'access']

_app_describe_output_keys = []

_app_cleanup_keys = ['name', 'title', 'summary', 'dxapi', 'openSource',
                     'version', 'runSpec', 'developers', 'authorizedUsers']

class DXApp(DXObject, DXExecutable):
    '''
    Remote app object handler.

    '''

    _class = "app"

    def __init__(self, dxid=None, name=None, alias=None):
        DXObject.__init__(self)
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
        :raises: :exc:`~dxpy.exceptions.DXError` if *dxid* and some other input are both given or if neither *dxid* nor *name* are given

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
            verify_string_dxid(dxid, self._class)
            self._dxid = dxid
        elif name is not None:
            self._name = name
            if not isinstance(name, basestring):
                raise DXError("App name needs to be a string: %r" % (name,))
            if alias is not None:
                if not isinstance(alias, basestring):
                    raise DXError("App alias needs to be a string: %r" % (alias,))
                self._alias = alias
            else:
                self._alias = 'default'

    def get_id(self):
        '''
        :returns: Object ID of associated app
        :rtype: string

        Returns the object ID of the app that the handler is currently
        associated with.
        '''
        if self._dxid is not None:
            return self._dxid
        else:
            return 'app-' + self._name + '/' + self._alias

    def new(self, **kwargs):
        '''
        :param initializeFrom: ID of an existing app object from which to initialize the app
        :type initializeFrom: string
        :param applet: ID of the applet that the app will be created from
        :type applet: string
        :param name: Name of the app (inherits from *initializeFrom* if possible)
        :type name: string
        :param title: Title or brand name of the app (optional)
        :type title: string
        :param summary: A short description of the app (optional)
        :type summary: string
        :param description: An extended description of the app (optional)
        :type description: string
        :param details: Arbitrary JSON to be associated with the app (optional)
        :type details: dict or list
        :param version: Version number
        :type version: string
        :param bill_to: ID of the user or organization who will own the app and be billed for its space usage (optional if an app with this name already exists)
        :type bill_to: string
        :param access: Access specification (optional)
        :type access: dict
        :param resources: Specifies what is to be put into the app's resources container. Must be a string containing a project ID, or a list containing object IDs. (optional)
        :type resources: string or list

        .. note:: It is highly recommended that the higher-level module
           :mod:`dxpy.app_builder` or (preferably) its frontend `dx build --create-app
           <https://wiki.dnanexus.com/Command-Line-Client/Index-of-dx-Commands#build>`_
           be used instead for app creation.

        Creates an app with the given parameters by using the specified
        applet or app as a base and overriding its attributes. See the
        API documentation for the `/app/new
        <https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%3A-%2Fapp%2Fnew>`_
        method for more info.

        Exactly one of *initializeFrom* and *applet* must be provided.

        The app is only available to its developers until
        :meth:`publish()` is called, and is not run until :meth:`run()`
        is called.

        '''
        dx_hash = {}
        if 'applet' not in kwargs and 'initializeFrom' not in kwargs:
            raise DXError("%s: One of the keyword arguments %s and %s is required" % (self.__class__.__name__, 'applet', 'initializeFrom'))

        for field in ['version']:
            if field not in kwargs:
                raise DXError("%s: Keyword argument %s is required" % (self.__class__.__name__, field))
            dx_hash[field] = kwargs[field]
            del kwargs[field]

        for field in 'initializeFrom', 'applet', 'name', 'title', 'summary', 'description', 'billing', 'access', 'resources':
            if field in kwargs:
                dx_hash[field] = kwargs[field]
                del kwargs[field]

        if "bill_to" in kwargs:
            dx_hash['billTo'] = kwargs['bill_to']
            del kwargs["bill_to"]

        resp = dxpy.api.app_new(dx_hash, **kwargs)
        self.set_id(dxid=resp["id"])

    def describe(self, fields=None, **kwargs):
        '''
        :param fields: Hash where the keys are field names that should be returned, and values should be set to True (default is that all fields are returned)
        :type fields: dict
        :returns: Description of the remote app object
        :rtype: dict

        Returns a dict with a description of the app. The result
        includes the key-value pairs as specified in the API
        documentation for the `/app-xxxx/describe
        <https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%253A-%252Fapp-xxxx%255B%252Fyyyy%255D%252Fdescribe>`_
        method.

        '''
        describe_input = {}
        if fields:
            describe_input['fields'] = fields
        if self._dxid is not None:
            self._desc = dxpy.api.app_describe(self._dxid, input_params=describe_input, **kwargs)
        else:
            self._desc = dxpy.api.app_describe('app-' + self._name, alias=self._alias,
                                               input_params=describe_input, **kwargs)

        return self._desc

    def update(self, **kwargs):
        '''
        :param applet: ID of the applet to replace the app's contents with
        :type applet: string
        :param details: Metadata to store with the app (optional)
        :type details: dict or list
        :param access: Access specification (optional)
        :type access: dict
        :param resources: Specifies what is to be put into the app's resources container. Must be a string containing a project ID, or a list containing object IDs. (optional)
        :type resources: string or list

        Updates the parameters of an existing app. See the API
        documentation for the `/app/update
        <https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%253A-%252Fapp-xxxx%255B%252Fyyyy%255D%252Fupdate>`_
        method for more info.

        The current user must be a developer of the app.

        '''
        updates = {}
        for field in 'applet', 'billing', 'access', 'resources', 'details':
            if field in kwargs:
                updates[field] = kwargs[field]
                del kwargs[field]

        if self._dxid is not None:
            resp = dxpy.api.app_update(self._dxid, input_params=updates, **kwargs)
        else:
            resp = dxpy.api.app_update('app-' + self._name, alias=self._alias,
                                       input_params=updates, **kwargs)

    def add_tags(self, tags, **kwargs):
        """
        :param tags: Tags to add to the app
        :type tags: array

        Adds the specified application name tags (aliases) to this app.

        The current user must be a developer of the app.

        """
        if self._dxid is not None:
            return dxpy.api.app_add_tags(self._dxid, input_params={"tags": tags}, **kwargs)
        else:
            return dxpy.api.app_add_tags('app-' + self._name, alias=self._alias,
                                         input_params={"tags": tags}, **kwargs)

    def addTags(self, tags, **kwargs):
        """
        .. deprecated:: 0.72.0
           Use :meth:`add_tags()` instead.
        """
        return self.add_tags(tags, **kwargs)

    def remove_tags(self, tags, **kwargs):
        """
        :param tags: Tags to remove from the app
        :type tags: array

        Removes the specified application name tags (aliases) from this
        app, so that it is no longer addressable by those aliases.

        The current user must be a developer of the app.

        """
        if self._dxid is not None:
            return dxpy.api.app_remove_tags(self._dxid, input_params={"tags": tags}, **kwargs)
        else:
            return dxpy.api.app_remove_tags('app-' + self._name, alias=self._alias,
                                            input_params={"tags": tags}, **kwargs)

    def removeTags(self, tags, **kwargs):
        """
        .. deprecated:: 0.72.0
           Use :meth:`remove_tags()` instead.
        """
        return self.remove_tags(tags, **kwargs)

    def install(self, **kwargs):
        """
        Installs the app in the current user's account.
        """
        if self._dxid is not None:
            return dxpy.api.app_install(self._dxid, **kwargs)
        else:
            return dxpy.api.app_install('app-' + self._name, alias=self._alias, **kwargs)

    def uninstall(self, **kwargs):
        """
        Uninstalls the app from the current user's account.
        """
        if self._dxid is not None:
            return dxpy.api.app_uninstall(self._dxid, **kwargs)
        else:
            return dxpy.api.app_uninstall('app-' + self._name, alias=self._alias, **kwargs)

    def get(self, **kwargs):
        """
        :returns: Full specification of the remote app object
        :rtype: dict

        Returns the contents of the app. The result includes the
        key-value pairs as specified in the API documentation for the
        `/app-xxxx/get
        <https://wiki.dnanexus.com/API-Specification-v1.0.0/Apps#API-method%253A-%252Fapp-xxxx%255B%252Fyyyy%255D%252Fget>`_
        method.
        """
        if self._dxid is not None:
            return dxpy.api.app_get(self._dxid, **kwargs)
        else:
            return dxpy.api.app_get('app-' + self._name, alias=self._alias, **kwargs)

    def publish(self, **kwargs):
        """
        Publishes the app, so all users can find it on the platform.

        The current user must be a developer of the app.
        """
        if self._dxid is not None:
            return dxpy.api.app_publish(self._dxid, **kwargs)
        else:
            return dxpy.api.app_publish('app-' + self._name, alias=self._alias, **kwargs)

    def delete(self, **kwargs):
        """
        Removes this app object from the platform.

        The current user must be a developer of the app.
        """
        if self._dxid is not None:
            return dxpy.api.app_delete(self._dxid, **kwargs)
        else:
            return dxpy.api.app_delete('app-' + self._name, alias=self._alias, **kwargs)

    def _run_impl(self, run_input, **kwargs):
        if self._dxid is not None:
            return DXJob(dxpy.api.app_run(self._dxid, input_params=run_input, **kwargs)["id"])
        else:
            return DXJob(dxpy.api.app_run('app-' + self._name, alias=self._alias,
                                          input_params=run_input,
                                          **kwargs)["id"])

    def _get_run_input(self, executable_input, **kwargs):
        # May need to be changed when workflow apps are enabled
        return DXExecutable._get_run_input_fields_for_applet(executable_input, **kwargs)

    def _get_required_keys(self):
        return _app_required_keys

    def _get_optional_keys(self):
        return _app_optional_keys

    def _get_describe_output_keys(self):
        return _app_describe_output_keys

    def _get_cleanup_keys(self):
        return _app_cleanup_keys

    def run(self, app_input, *args, **kwargs):
        """
        Creates a new job that executes the function "main" of this app with
        the given input *app_input*.

        See :meth:`dxpy.bindings.dxapplet.DXExecutable.run` for the available
        args.
        """
        # Rename app_input to preserve API compatibility when calling
        # DXApp.run(app_input=...).
        return super(DXApp, self).run(app_input, *args, **kwargs)
