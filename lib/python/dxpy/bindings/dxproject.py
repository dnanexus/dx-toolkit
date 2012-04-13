"""
TODO: Write something here
"""

from dxpy.bindings import *

#############
# DXProject #
#############

class DXProject():
    '''Remote project handler'''

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
        :param dxid: Project ID
        :type dxid: string
        :raises: :exc:`dxpy.exceptions.DXError` if *dxid* does not match class type

        Discards the currently stored ID and associates the handler
        with *dxid*.

        '''
        if re.match("project-[0-9a-zA-Z]{24}", dxid) is None or \
                len(dxid) != len('project') + 25:
            raise DXError("Given project ID does not match expected format")

        self._dxid = dxid

    def get_id(self):
        '''
        :returns: Project ID of the associated project
        :rtype: string

        Returns the project ID that the handler is currently associated
        with.

        '''

        return self._dxid

    def describe(self):
        """
        :returns: A hash containing attributes of the project.
        :rtype: dict

        Returns a hash which will include the keys "id", "class",
        "name", "description", "protected", "restricted", and
        "created".

        """
        return dxpy.api.projectDescribe(self._dxid)

    def update(self, name=None, description=None, protected=None, restricted=None):
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
        dxpy.api.projectUpdate(self._dxid, update_hash)

    def new_folder(self, folder, parents=False):
        """
        :param folder: Full path to the new folder to create
        :type folder: string
        :param parents: Whether to recursively create all parent folders if they are missing
        :type parents: boolean

        Creates a new folder in the project

        """

        dxpy.api.projectNewFolder(self._dxid, {"folder": folder,
                                               "parents": parents})

    def list_folder(self, folder="/"):
        """
        :param folder: Full path to the folder to list
        :type folder: string
        :returns: A hash with key "objects" for the list of object IDs and key "folders" for the list of folder routes
        :rtype: dict

        Returns a hash containing a list of object IDs for objects
        residing directly inside the specified folder, and a list of
        folders (with their full routes) directly inside the specified
        folder.

        """

        return dxpy.api.projectListFolder(self._dxid, {"folder": folder})

    def move(self, destination, objects=[], folders=[]):
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

        dxpy.api.projectMove(self._dxid, {"objects": objects,
                                          "folders": folders,
                                          "destination": destination})

    def move_folder(self, folder, destination):
        """
        :param folder: Full path to the folder to move
        :type folder: string
        :param destination: Full path to the destination folder that will contain *folder*
        :type destination: string

        Moves *folder* to reside in *destination* in the same project.
        Note that all contained objects and subfolders are also moved.

        """

        dxpy.api.projectMove(self._dxid, {"folders": [folder],
                                          "destination": destination})

    def remove_folder(self, folder):
        """
        :param folder: Full path to the folder to remove
        :type folder: string

        Removes the specified folder in the project; it must be empty
        to be removed.

        """

        dxpy.api.projectRemoveFolder(self._dxid, {"folder": folder})

    def remove_objects(self, objects):
        """
        :param objects: List of object IDs to remove from the project
        :type objects: list of strings

        Removes the specified objects in the project; removal
        propagates to any linked hidden objects that would otherwise
        be unreachable from any visible object in the project.

        """

        dxpy.api.projectRemoveObjects(self._dxid, {"objects": objects})

    def clone(self, project, destination="/", objects=[], folders=[],
              include_hidden_links=True):
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

        return dxpy.api.projectClone(self._dxid, {"objects": objects,
                                                  "folders": folders,
                                                  "project": project,
                                                  "folders": folders,
                                                  "destination": destination,
                                                  "includeHiddenLinks": include_hidden_links})

    def destroy(self):
        """
        Destroys the project.
        """

        dxpy.api.projectDestroy(self._dxid)
