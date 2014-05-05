# Copyright (C) 2013-2014 DNAnexus, Inc.
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

from __future__ import (print_function, unicode_literals)

import dxpy
from . import DXObject

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
        <https://wiki.dnanexus.com/API-Specification-v1.0.0/Projects#API-method%3A-%2Fproject-xxxx%2Fdescribe>`_
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

    def remove_folder(self, folder, recurse=False, **kwargs):
        """
        :param folder: Full path to the folder to remove
        :type folder: string
        :param recurse: If True, recursively remove all objects and subfolders in the folder
        :type recurse: bool

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

        api_method(self._dxid, {"folder": folder, "recurse": recurse},
                   **kwargs)

    def remove_objects(self, objects, **kwargs):
        """
        :param objects: List of object IDs to remove from the project or container
        :type objects: list of strings

        Removes the specified objects from the project or container.

        Removal propagates to any hidden objects that become unreachable
        from any visible object in the same project or container as a
        result of this operation.

        """
        api_method = dxpy.api.container_remove_objects
        if isinstance(self, DXProject):
            api_method = dxpy.api.project_remove_objects

        api_method(self._dxid, {"objects": objects},
                   **kwargs)

    def clone(self, container, destination="/", objects=[], folders=[],
              include_hidden_links=True, **kwargs):
        """
        :param container: Destination container ID
        :type container: string
        :param destination: Path of destination folder in the destination container
        :type destination: string
        :param objects: List of object IDs to move
        :type objects: list of strings
        :param folders: List of full paths to folders to move
        :type folders: list of strings
        :param include_hidden_links: If True, also clone objects that are hidden and linked to from any of the objects that would be cloned
        :type include_hidden_links: boolean

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
                           "includeHiddenLinks": include_hidden_links},
                          **kwargs)

#############
# DXProject #
#############

class DXProject(DXContainer):
    '''Remote project handler.'''

    _class = "project"

    def update(self, name=None, summary=None, description=None, protected=None,
               restricted=None, version=None, **kwargs):
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
        :param version: If provided, the update will only occur if the value matches the current project's version number
        :type version: int

        Updates the project with the new fields. All fields are
        optional. Fields that are not provided are not changed.
        See the API documentation for the `/project-xxxx/update
        <https://wiki.dnanexus.com/API-Specification-v1.0.0/Projects#API-method%3A-%2Fproject-xxxx%2Fupdate>`_
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
        if version is not None:
            update_hash["version"] = version
        dxpy.api.project_update(self._dxid, update_hash, **kwargs)

    def invite(self, invitee, level, **kwargs):
        """
        :param invitee: Username (of the form "user-USERNAME") or email address of person to be invited to the project; use "PUBLIC" to make the project publicly available (in which case level must be set to "VIEW").
        :type invitee: string
        :param level: Permissions level that the invitee would get ("VIEW", "UPLOAD", "CONTRIBUTE", or "ADMINISTER")
        :type level: string

        Invites the specified user to have access to the project.

        """

        return dxpy.api.project_invite(self._dxid,
                                       {"invitee": invitee, "level": level},
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
