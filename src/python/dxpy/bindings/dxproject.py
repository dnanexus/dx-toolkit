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

Projects (:class:`~dxpy.bindings.dxproject.DXProject`) are platform
entities that serve as general-purpose containers for data, and are the
unit of collaboration.

Containers (:class:`~dxpy.bindings.dxproject.DXContainer`) are
special-purpose data containers that support a subset of the methods
available to projects. Containers behave like projects with the
PROTECTED flag unset (so that temporary intermediate data files can be
deleted), except they cannot be explicitly created or destroyed, and
their permissions are fixed.

:class:`~dxpy.bindings.dxproject.DXProject` is implemented as a subclass
of :class:`~dxpy.bindings.dxproject.DXContainer`.

"""

from __future__ import print_function, unicode_literals, division, absolute_import

import dxpy
from . import DXObject
from ..exceptions import DXError

###############
# DXContainer #
###############

class DXContainer(DXObject):
    '''Remote container handler.'''

    _class = "container"

    def __init__(self, dxid=None):
        DXObject.__init__(self)

        if dxid is not None:
            self.set_id(dxid)
        else:
            self.set_id(dxpy.WORKSPACE_ID)

    def describe(self, **kwargs):
        """
        :returns: A hash containing attributes of the project or container.
        :rtype: dict

        Returns a hash with key-value pairs as specified by the API
        specification for the `/project-xxxx/describe
        <https://documentation.dnanexus.com/developer/api/data-containers/projects#api-method-project-xxxx-describe>`_
        method. This will usually include keys such as "id", "name",
        "class", "billTo", "created", "modified", and "dataUsage".

        """
        # TODO: link to /container-xxxx/describe
        api_method = dxpy.api.container_describe
        if isinstance(self, DXProject):
            api_method = dxpy.api.project_describe
        self._desc = api_method(self._dxid, **kwargs)
        return self._desc

    def new_folder(self, folder, parents=False, **kwargs):
        """
        :param folder: Full path to the new folder to create
        :type folder: string
        :param parents: If True, recursively create any parent folders that are missing
        :type parents: boolean

        Creates a new folder in the project or container.

        """
        api_method = dxpy.api.container_new_folder
        if isinstance(self, DXProject):
            api_method = dxpy.api.project_new_folder

        api_method(self._dxid, {"folder": folder,
                                "parents": parents},
                   **kwargs)

    def list_folder(self, folder="/", describe=False, only="all", includeHidden=False, **kwargs):
        """
        :param folder: Full path to the folder to list
        :type folder: string
        :param describe: If True, returns the output of ``/describe`` on each object (see below for notes)
        :type describe: bool or dict
        :param only: Indicate "objects" for only objects, "folders" for only folders, or "all" for both
        :type only: string
        :param includeHidden: Indicate whether hidden objects should be returned
        :type includeHidden: bool
        :returns: A hash with key "objects" for the list of object IDs and key "folders" for the list of folder routes
        :rtype: dict

        Returns a hash containing a list of objects that reside directly
        inside the specified folder, and a list of strings representing
        the full paths to folders that reside directly inside the
        specified folder.

        By default, the list of objects is provided as a list containing
        one hash ``{"id": "class-XXXX"}`` with the ID of each matching
        object. If *describe* is not False, the output of ``/describe``
        is also included in an additional field "describe" for each
        object. If *describe* is True, ``/describe`` is called with the
        default arguments. *describe* may also be a hash, indicating the
        input hash to be supplied to each ``/describe`` call.

        """
        # TODO: it would be nice if we could supply describe
        # fields/defaultFields in a similar way to what we pass to the
        # high-level describe method, rather than having to construct
        # the literal API input

        api_method = dxpy.api.container_list_folder
        if isinstance(self, DXProject):
            api_method = dxpy.api.project_list_folder

        return api_method(self._dxid, {"folder": folder,
                                       "describe": describe,
                                       "only": only,
                                       "includeHidden": includeHidden},
                          **kwargs)

    def move(self, destination, objects=[], folders=[], **kwargs):
        """
        :param destination: Path of destination folder
        :type destination: string
        :param objects: List of object IDs to move
        :type objects: list of strings
        :param folders: List of full paths to folders to move
        :type folders: list of strings

        Moves the specified objects and folders into the folder
        represented by *destination*. Moving a folder also moves all
        contained folders and objects. If an object or folder is
        explicitly specified but also appears inside another specified
        folder, it will be removed from its parent folder and placed
        directly in *destination*.

        """
        api_method = dxpy.api.container_move
        if isinstance(self, DXProject):
            api_method = dxpy.api.project_move

        api_method(self._dxid, {"objects": objects,
                                "folders": folders,
                                "destination": destination},
                   **kwargs)

    def move_folder(self, folder, destination, **kwargs):
        """
        :param folder: Full path to the folder to move
        :type folder: string
        :param destination: Full path to the destination folder that will contain *folder*
        :type destination: string

        Moves *folder* to reside in *destination* in the same project or
        container. All objects and subfolders inside *folder* are also
        moved.

        """
        api_method = dxpy.api.container_move
        if isinstance(self, DXProject):
            api_method = dxpy.api.project_move

        api_method(self._dxid, {"folders": [folder],
                                "destination": destination},
                   **kwargs)

    def remove_folder(self, folder, recurse=False, force=False, **kwargs):
        """
        :param folder: Full path to the folder to remove
        :type folder: string
        :param recurse: If True, recursively remove all objects and subfolders in the folder
        :type recurse: bool
        :param force: If True, will suppress errors for folders that do not exist
        :type force: bool

        Removes the specified folder from the project or container. It
        must be empty to be removed, unless *recurse* is True.

        Removal propagates to any hidden objects that become unreachable
        from any visible object in the same project or container as a
        result of this operation. (This can only happen if *recurse* is
        True.)

        """
        api_method = dxpy.api.container_remove_folder
        if isinstance(self, DXProject):
            api_method = dxpy.api.project_remove_folder

        completed = False
        while not completed:
            resp = api_method(self._dxid,
                              {"folder": folder, "recurse": recurse, "force": force, "partial": True},
                              always_retry=force,  # api call is idempotent under 'force' semantics
                              **kwargs)
            if 'completed' not in resp:
                raise DXError('Error removing folder')
            completed = resp['completed']

    def remove_objects(self, objects, force=False, **kwargs):
        """
        :param objects: List of object IDs to remove from the project or container
        :type objects: list of strings
        :param force: If True, will suppress errors for objects that do not exist
        :type force: bool

        Removes the specified objects from the project or container.

        Removal propagates to any hidden objects that become unreachable
        from any visible object in the same project or container as a
        result of this operation.

        """
        api_method = dxpy.api.container_remove_objects
        if isinstance(self, DXProject):
            api_method = dxpy.api.project_remove_objects

        api_method(self._dxid,
                   {"objects": objects, "force": force},
                   always_retry=force,  # api call is idempotent under 'force' semantics
                   **kwargs)

    def clone(self, container, destination="/", objects=[], folders=[], parents=False, **kwargs):
        """
        :param container: Destination container ID
        :type container: string
        :param destination: Path of destination folder in the destination container
        :type destination: string
        :param objects: List of object IDs to move
        :type objects: list of strings
        :param folders: List of full paths to folders to move
        :type folders: list of strings
        :param parents: Whether the destination folder and/or parent folders should be created if they do not exist
        :type parents: boolean

        Clones (copies) the specified objects and folders in the
        container into the folder *destination* in the container
        *container*. Cloning a folder also clones all all folders and
        objects it contains. If an object or folder is explicitly
        specified but also appears inside another specified folder, it
        will be removed from its parent folder and placed directly in
        *destination*. No objects or folders are modified in the source
        container.

        Objects must be in the "closed" state to be cloned.

        """
        api_method = dxpy.api.container_clone
        if isinstance(self, DXProject):
            api_method = dxpy.api.project_clone

        return api_method(self._dxid,
                          {"objects": objects,
                           "folders": folders,
                           "project": container,
                           "destination": destination,
                           "parents": parents},
                          **kwargs)

#############
# DXProject #
#############

class DXProject(DXContainer):
    '''Remote project handler.'''

    _class = "project"

    def new(self, name, summary=None, description=None, protected=None,
            restricted=None, download_restricted=None, contains_phi=None,
            tags=None, properties=None, bill_to=None, database_ui_view_only=None,
            **kwargs):
        """
        :param name: The name of the project
        :type name: string
        :param summary: If provided, a short summary of what the project contains
        :type summary: string
        :param description: If provided, the new project description
        :type name: string
        :param protected: If provided, whether the project should be protected
        :type protected: boolean
        :param restricted: If provided, whether the project should be restricted
        :type restricted: boolean
        :param download_restricted: If provided, whether external file downloads and external access to database objects should be restricted
        :type download_restricted: boolean
        :param contains_phi: If provided, whether the project should be marked as containing protected health information (PHI)
        :type contains_phi: boolean
        :param tags: If provided, tags to associate with the project
        :type tags: list of strings
        :param properties: If provided, properties to associate with the project
        :type properties: dict
        :param bill_to: If provided, ID of the entity to which any costs associated with this project will be billed; must be the ID of the requesting user or an org of which the requesting user is a member with allowBillableActivities permission
        :type bill_to: string
        :param database_ui_view_only: If provided, whether the viewers on the project can access the database data directly
        :type database_ui_view_only: boolean

        Creates a new project. Initially only the user performing this action
        will be in the permissions/member list, with ADMINISTER access.
        See the API documentation for the `/project/new
        <https://documentation.dnanexus.com/developer/api/data-containers/projects#api-method-project-new>`_
        method for more info.

        """
        input_hash = {}
        input_hash["name"] = name
        if summary is not None:
            input_hash["summary"] = summary
        if description is not None:
            input_hash["description"] = description
        if protected is not None:
            input_hash["protected"] = protected
        if restricted is not None:
            input_hash["restricted"] = restricted
        if download_restricted is not None:
            input_hash["downloadRestricted"] = download_restricted
        if contains_phi is not None:
            input_hash["containsPHI"] = contains_phi
        if bill_to is not None:
            input_hash["billTo"] = bill_to
        if database_ui_view_only is not None:
            input_hash["databaseUIViewOnly"] = database_ui_view_only
        if tags is not None:
            input_hash["tags"] = tags
        if properties is not None:
            input_hash["properties"] = properties

        self.set_id(dxpy.api.project_new(input_hash, **kwargs)["id"])
        self._desc = {}
        return self._dxid

    def update(self, name=None, summary=None, description=None, protected=None,
               restricted=None, download_restricted=None, version=None,
               allowed_executables=None, unset_allowed_executables=None,
               database_ui_view_only=None, **kwargs):
        """
        :param name: If provided, the new project name
        :type name: string
        :param summary: If provided, the new project summary
        :type summary: string
        :param description: If provided, the new project description
        :type name: string
        :param protected: If provided, whether the project should be protected
        :type protected: boolean
        :param restricted: If provided, whether the project should be restricted
        :type restricted: boolean
        :param download_restricted: If provided, whether external downloads should be restricted
        :type download_restricted: boolean
        :param allowed_executables: If provided, these are the only executable ID(s) allowed to run as root executions in this project
        :type allowed_executables: list
        :param database_ui_view_only: If provided, whether the viewers on the project can access the database data directly
        :type database_ui_view_only: boolean
        :param version: If provided, the update will only occur if the value matches the current project's version number
        :type version: int

        Updates the project with the new fields. All fields are
        optional. Fields that are not provided are not changed.
        See the API documentation for the `/project-xxxx/update
        <https://documentation.dnanexus.com/developer/api/data-containers/projects#api-method-project-xxxx-update>`_
        method for more info.

        """
        update_hash = {}
        if name is not None:
            update_hash["name"] = name
        if summary is not None:
            update_hash["summary"] = summary
        if description is not None:
            update_hash["description"] = description
        if protected is not None:
            update_hash["protected"] = protected
        if restricted is not None:
            update_hash["restricted"] = restricted
        if download_restricted is not None:
            update_hash["downloadRestricted"] = download_restricted
        if version is not None:
            update_hash["version"] = version
        if allowed_executables is not None:
            update_hash["allowedExecutables"] = allowed_executables
        if unset_allowed_executables is not None:
            update_hash["allowedExecutables"] = None
        if database_ui_view_only is not None:
            update_hash["databaseUIViewOnly"] = database_ui_view_only
        dxpy.api.project_update(self._dxid, update_hash, **kwargs)

    def invite(self, invitee, level, send_email=True, **kwargs):
        """
        :param invitee: Username (of the form "user-USERNAME") or email address of person to be invited to the project; use "PUBLIC" to make the project publicly available (in which case level must be set to "VIEW").
        :type invitee: string
        :param level: Permissions level that the invitee would get ("VIEW", "UPLOAD", "CONTRIBUTE", or "ADMINISTER")
        :type level: string
        :param send_email: Determines whether user receives email notifications regarding the project invitation
        :type send_email: boolean

        Invites the specified user to have access to the project.

        """

        return dxpy.api.project_invite(self._dxid,
                                       {"invitee": invitee, "level": level,
                                        "suppressEmailNotification": not send_email},
                                       **kwargs)

    def decrease_perms(self, member, level, **kwargs):
        """
        :param member: Username (of the form "user-USERNAME") of the project member whose permissions will be decreased.
        :type member: string
        :param level: Permissions level that the member will have after this operation (None, "VIEW", "UPLOAD", or "CONTRIBUTE")
        :type level: string or None

        Decreases the permissions that the specified user has in the project.

        """

        input_hash = {}
        input_hash[member] = level

        return dxpy.api.project_decrease_permissions(self._dxid,
                                                     input_hash,
                                                     **kwargs)

    def destroy(self, **kwargs):
        """
        Destroys the project.
        """

        dxpy.api.project_destroy(self._dxid, **kwargs)

    def set_properties(self, properties, **kwargs):
        """
        :param properties: Property names and values given as key-value pairs of strings
        :type properties: dict

        Given key-value pairs in *properties* for property names and
        values, the properties are set on the project for the given
        property names. Any property with a value of :const:`None`
        indicates the property will be deleted.

        .. note:: Any existing properties not mentioned in *properties*
           are not modified by this method.

        """

        return dxpy.api.project_set_properties(self._dxid, {"properties": properties}, **kwargs)
