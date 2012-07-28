"""
DXProject and DXContainer handlers
+++++++++++++++++

Projects are platform entities which serve as containers for data, and are the unit of collaboration.

Containers are special-purpose data containers which behave like projects with the PROTECTED flag unset (so that
temporary intermediate data files can be deleted), except they cannot be explicitly created or destroyed, and their
permissions are fixed.  In particular, the container class shares with the project class the following methods with
the exact same syntax: newFolder, listFolder, renameFolder, removeFolder, move, removeObjects, and clone.  There is
also a minimalist describe method that returns metadata about the objects and folders inside the container.
"""

import dxpy
from dxpy.bindings import *

###############
# DXContainer #
###############

class DXContainer(object):
    '''Remote container handler'''

    def __init__(self, dxid=None):
        if dxid is not None:
            self.set_id(dxid)
        else:
            self.set_id(dxpy.WORKSPACE_ID)

    def __str__(self):
        desc = "dxpy." + self.__class__.__name__ + " (" + self._class + ") object: "
        try:
            desc += self.get_id()
        except:
            desc += "no ID stored"
        return desc


    def set_id(self, dxid):
        '''
        :param dxid: Project or container ID
        :type dxid: string
        :raises: :exc:`dxpy.exceptions.DXError` if *dxid* does not match class type

        Discards the currently stored ID and associates the handler
        with *dxid*.

        '''
        if isinstance(self, DXProject):
            if re.match("project-[0-9a-zA-Z]{24}", dxid) is None or \
                    len(dxid) != len('project') + 25:
                raise DXError("Given project ID does not match expected format")
        else:
            if re.match("container-[0-9a-zA-Z]{24}", dxid) is None or \
                    len(dxid) != len('container') + 25:
                raise DXError("Given container ID does not match expected format")

        self._dxid = dxid

    def get_id(self):
        '''
        :returns: ID of the associated project or container
        :rtype: string

        Returns the project or container ID that the handler is currently associated
        with.

        '''

        return self._dxid

    def describe(self, **kwargs):
        """
        :returns: A hash containing attributes of the project or container.
        :rtype: dict

        Returns a hash which will include the keys "id", "class",
        "name", "description", "protected", "restricted", and
        "created".

        """
        api_method = dxpy.api.containerDescribe
        if isinstance(self, DXProject):
            api_method = dxpy.api.projectDescribe
        return api_method(self._dxid, **kwargs)

    def new_folder(self, folder, parents=False, **kwargs):
        """
        :param folder: Full path to the new folder to create
        :type folder: string
        :param parents: Whether to recursively create all parent folders if they are missing
        :type parents: boolean

        Creates a new folder in the project or container

        """
        api_method = dxpy.api.containerNewFolder
        if isinstance(self, DXProject):
            api_method = dxpy.api.projectNewFolder

        api_method(self._dxid, {"folder": folder,
                                "parents": parents},
                   **kwargs)

    def list_folder(self, folder="/", describe=False, only="all", includeHidden=False, **kwargs):
        """
        :param folder: Full path to the folder to list
        :type folder: string
        :param describe: Either false or the input to /describe to be called on each object
        :type describe: bool or dict
        :param only: Indicate "objects" for only objects, "folders" for only folders, or "all" for both
        :type only: string
        :param includeHidden: Indicate whether hidden objects should be returned
        :type includeHidden: bool
        :returns: A hash with key "objects" for the list of object IDs and key "folders" for the list of folder routes
        :rtype: dict

        Returns a hash containing a list of object IDs for objects
        residing directly inside the specified folder, and a list of
        folders (with their full routes) directly inside the specified
        folder.

        """
        api_method = dxpy.api.containerListFolder
        if isinstance(self, DXProject):
            api_method = dxpy.api.projectListFolder

        return api_method(self._dxid, {"folder": folder,
                                       "describe": describe,
                                       "only": only,
                                       "includeHidden": includeHidden},
                          **kwargs)

    def move(self, destination, objects=[], folders=[], **kwargs):
        """
        :param objects: List of object IDs to move
        :type objects: list of strings
        :param folders: List of full paths to folders to move
        :type folders: list of strings
        :param destination: Path of destination folder
        :type destination: string

        Moves the specified objects and folders into the folder
        represented by *destination*.  Moving a folder also moves all
        contained folders and objects.  If an object or folder is
        explicitly specified but also appears inside another specified
        folder, it will be removed from the larger folder and placed
        directly in *destination*.

        """
        api_method = dxpy.api.containerMove
        if isinstance(self, DXProject):
            api_method = dxpy.api.projectMove

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

        Moves *folder* to reside in *destination* in the same project or container.
        Note that all contained objects and subfolders are also moved.

        """
        api_method = dxpy.api.containerMove
        if isinstance(self, DXProject):
            api_method = dxpy.api.projectMove

        api_method(self._dxid, {"folders": [folder],
                                "destination": destination},
                   **kwargs)

    def remove_folder(self, folder, recurse=False, **kwargs):
        """
        :param folder: Full path to the folder to remove
        :type folder: string
        :param recurse: Whether to remove all objects in the folder as well
        :type recurse: bool

        Removes the specified folder in the project or container; it must be empty
        to be removed.

        """
        api_method = dxpy.api.containerRemoveFolder
        if isinstance(self, DXProject):
            api_method = dxpy.api.projectRemoveFolder

        api_method(self._dxid, {"folder": folder, "recurse": recurse},
                   **kwargs)

    def remove_objects(self, objects, **kwargs):
        """
        :param objects: List of object IDs to remove from the project or container
        :type objects: list of strings

        Removes the specified objects in the project or container; removal
        propagates to any linked hidden objects that would otherwise
        be unreachable from any visible object in the project or container.

        """
        api_method = dxpy.api.containerRemoveObjects
        if isinstance(self, DXProject):
            api_method = dxpy.api.projectRemoveObjects

        api_method(self._dxid, {"objects": objects},
                   **kwargs)

    def clone(self, project, destination="/", objects=[], folders=[],
              include_hidden_links=True, **kwargs):
        """
        :param objects: List of object IDs to move
        :type objects: list of strings
        :param folders: List of full paths to folders to move
        :type folders: list of strings
        :param project: Destination project ID
        :type project: string
        :param destination: Path of destination folder in the destination project
        :type destination: string
        :param include_hidden_links: Whether to also clone objects that are hidden and linked from any of the objects that would be cloned
        :type include_hidden_links: boolean

        Clones (copies) the specified objects and folders in the
        project to the project specified in *project* and into the
        folder *destination*.  Cloning a folder also clones all
        contained folders and objects.  If an object or folder is
        explicitly specified but also appears inside another specified
        folder, it will be removed from the larger folder and placed
        directly in *destination*.  Note that objects must be in the
        "closed" state to be cloned, and that no objects or folders
        are modified in the source project..

        """
        api_method = dxpy.api.containerClone
        if isinstance(self, DXProject):
            api_method = dxpy.api.projectClone

        return api_method(self._dxid,
                          {"objects": objects,
                           "folders": folders,
                           "project": project,
                           "destination": destination,
                           "includeHiddenLinks": include_hidden_links},
                          **kwargs)

#############
# DXProject #
#############

class DXProject(DXContainer):
    def update(self, name=None, description=None, protected=None,
               restricted=None, **kwargs):
        """
        :param name: New project name
        :type name: string
        :param description: New project description
        :type name: string
        :param protected: Whether the project should become protected
        :type protected: boolean
        :param restricted: Whether the project should become restricted
        :type restricted: boolean

        Updates the project with the new fields.  Each field is
        optional.  Fields that are not provided are not changed.

        """
        update_hash = {}
        if name is not None:
            update_hash["name"] = name
        if description is not None:
            update_hash["description"] = description
        if protected is not None:
            update_hash["protected"] = protected
        if restricted is not None:
            update_hash["restricted"] = restricted
        dxpy.api.projectUpdate(self._dxid, update_hash, **kwargs)

    def invite(self, invitee, level, **kwargs):
        """
        :param invitee: Username or email of person to be invited to the project; use "PUBLIC" to make it publicly viewable (level must be set to "VIEW")
        :type invitee: string
        :param level: Permissions level that the invitee would get ("LIST", "VIEW", "CONTRIBUTE", or "ADMINISTER")
        :type level: string

        """

        return dxpy.api.projectInvite(self._dxid,
                                      {"invitee": invitee,
                                       "level": level}, **kwargs)

    def decrease_perms(self, member, level, **kwargs):
        """
        :param member: Username of the project member whose permissions will be decreased
        :type member: string
        :param level: Permissions level that the member will now have (None, "LIST", "VIEW", or "CONTRIBUTE")
        :type level: string or None

        """

        input_hash = {}
        input_hash[member] = level

        return dxpy.api.projectDecreasePermissions(self._dxid,
                                                   input_hash,
                                                   **kwargs)

    def destroy(self, **kwargs):
        """
        Destroys the project.
        """

        dxpy.api.projectDestroy(self._dxid, **kwargs)
