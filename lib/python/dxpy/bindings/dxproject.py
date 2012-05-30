"""
DXProject and DXWorkspace handlers
+++++++++++++++++

Projects are platform entities which serve as containers for data, and are the unit of collaboration.

Workspaces are special-purpose data containers which behave like projects with the PROTECTED flag unset (so that
temporary intermediate data files can be deleted), except they cannot be explicitly created or destroyed, and their
permissions are fixed.  In particular, the workspace class shares with the project class the following methods with
the exact same syntax: newFolder, listFolder, renameFolder, removeFolder, move, removeObjects, and clone.  There is
also a minimalist describe method that returns metadata about the objects and folders inside the workspace.
"""

from dxpy.bindings import *

###############
# DXWorkspace #
###############

class DXWorkspace(object):
    '''Remote workspace handler'''

    def __init__(self, dxid=None):
        if dxid is not None:
            self.set_id(dxid)
        else:
            global WORKSPACE_ID
            self.set_id(WORKSPACE_ID)

    def __str__(self):
        desc = "dxpy." + self.__class__.__name__ + " (" + self._class + ") object: "
        try:
            desc += self.get_id()
        except:
            desc += "no ID stored"
        return desc


    def set_id(self, dxid):
        '''
        :param dxid: Project or workspace ID
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
            if re.match("workspace-[0-9a-zA-Z]{24}", dxid) is None or \
                    len(dxid) != len('workspace') + 25:
                raise DXError("Given workspace ID does not match expected format")

        self._dxid = dxid

    def get_id(self):
        '''
        :returns: ID of the associated project or workspace
        :rtype: string

        Returns the project or workspace ID that the handler is currently associated
        with.

        '''

        return self._dxid

    def describe(self, **kwargs):
        """
        :returns: A hash containing attributes of the project or workspace.
        :rtype: dict

        Returns a hash which will include the keys "id", "class",
        "name", "description", "protected", "restricted", and
        "created".

        """
        api_method = dxpy.api.workspaceDescribe
        if isinstance(self, DXProject):
            api_method = dxpy.api.projectDescribe
        return api_method(self._dxid, **kwargs)

    def new_folder(self, folder, parents=False, **kwargs):
        """
        :param folder: Full path to the new folder to create
        :type folder: string
        :param parents: Whether to recursively create all parent folders if they are missing
        :type parents: boolean

        Creates a new folder in the project or workspace

        """
        api_method = dxpy.api.workspaceNewFolder
        if isinstance(self, DXProject):
            api_method = dxpy.api.projectNewFolder

        api_method(self._dxid, {"folder": folder,
                                "parents": parents},
                   **kwargs)

    def list_folder(self, folder="/", describe=False, **kwargs):
        """
        :param folder: Full path to the folder to list
        :type folder: string
        :param describe: Either false or the input to /describe to be called on each object
        :type describe: bool or dict
        :returns: A hash with key "objects" for the list of object IDs and key "folders" for the list of folder routes
        :rtype: dict

        Returns a hash containing a list of object IDs for objects
        residing directly inside the specified folder, and a list of
        folders (with their full routes) directly inside the specified
        folder.

        """
        api_method = dxpy.api.workspaceListFolder
        if isinstance(self, DXProject):
            api_method = dxpy.api.projectListFolder

        return api_method(self._dxid, {"folder": folder,
                                       "describe": describe},
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
        api_method = dxpy.api.workspaceMove
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

        Moves *folder* to reside in *destination* in the same project or workspace.
        Note that all contained objects and subfolders are also moved.

        """
        api_method = dxpy.api.workspaceMove
        if isinstance(self, DXProject):
            api_method = dxpy.api.projectMove

        api_method(self._dxid, {"folders": [folder],
                                "destination": destination},
                   **kwargs)

    def remove_folder(self, folder, **kwargs):
        """
        :param folder: Full path to the folder to remove
        :type folder: string

        Removes the specified folder in the project or workspace; it must be empty
        to be removed.

        """
        api_method = dxpy.api.workspaceRemoveFolder
        if isinstance(self, DXProject):
            api_method = dxpy.api.projectRemoveFolder

        api_method(self._dxid, {"folder": folder},
                   **kwargs)

    def remove_objects(self, objects, **kwargs):
        """
        :param objects: List of object IDs to remove from the project or workspace
        :type objects: list of strings

        Removes the specified objects in the project or workspace; removal
        propagates to any linked hidden objects that would otherwise
        be unreachable from any visible object in the project or workspace.

        """
        api_method = dxpy.api.workspaceRemoveObjects
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
        api_method = dxpy.api.workspaceClone
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

class DXProject(DXWorkspace):
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


    def destroy(self, **kwargs):
        """
        Destroys the project.
        """

        dxpy.api.projectDestroy(self._dxid, **kwargs)
