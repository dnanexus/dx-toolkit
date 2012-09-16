"""
The :class:`DXObject` class defined below is the abstract base class
for all remote object handlers, and the :class:`DXDataObject` class is
the abstract base class for all data object handlers.
"""

import time, re, requests, sys, json
from dxpy import *
import dxpy.api
from dxpy.exceptions import *
import copy

NUM_HTTP_THREADS = 4


class DXObject(object):
    """Abstract base class for all remote object handlers."""

    def __init__(self, dxid=None, project=None):
        raise NotImplementedError("DXObject is an abstract class; a subclass should be initialized instead.")

    def __str__(self):
        dxid, dxproj_id = "no ID stored", ""
        try:
            dxid = self._dxid
            if isinstance(self, DXDataObject):
                try:
                    dxproj_id = " (%s)" % self._proj
                except:
                    dxproj_id = " (no project ID stored)"
        except:
            pass

        desc = "<{module}.{classname} object: {dxid}{dxproj_id}>".format(module=self.__module__,
                                                                         classname=self.__class__.__name__,
                                                                         dxid=dxid,
                                                                         dxproj_id = dxproj_id)
        return desc


class DXDataObject(DXObject):
    """Abstract base class for all remote data object handlers."""

    def __init__(self, dxid=None, project=None):
        try:
            self._class
        except:
            raise NotImplementedError(
                "DXDataObject is an abstract class; a subclass should" + \
                    "be initialized instead.")

        self.set_ids(dxid, project)

    @staticmethod
    def _get_creation_params(kwargs):
        common_creation_params = set(["project", "name", "tags", "types", "hidden", "properties", "details", "folder", "parents"])

        dx_hash = {p: kwargs[p] for p in kwargs if p in common_creation_params and kwargs[p] is not None}
        remaining_kwargs = {p: kwargs[p] for p in kwargs if p not in common_creation_params}

        if "project" not in dx_hash:
            dx_hash["project"] = dxpy.WORKSPACE_ID

        return dx_hash, remaining_kwargs

    def new(self, **kwargs):
        '''
        :param project: Project ID in which to create the new remote object
        :type project: string
        :param name: Name for the object
        :type name: string
        :param tags: Tags to add for the object
        :type tags: list of strings
        :param types: Types to add to the object
        :type types: list of strings
        :param hidden: Whether the object is to be hidden or not
        :type hidden: boolean
        :param properties: Properties given as key-value pairs of strings
        :type properties: dict
        :param details: Details to set for the object
        :type details: dict or list
        :param folder: Full path to the destination folder
        :type folder: string
        :param parents: Whether to recursively create all parent folders if they are missing
        :type parents: boolean

        Creates the data object with the given fields.  Only *project*
        is required if no default project or workspace is set; the
        rest are optional and have default behavior as specified in
        the API documentation.

        '''
        try:
            self._class
        except:
            raise NotImplementedError(
                "DXDataObject is an abstract class; a subclass should" + \
                    "be initialized instead.")

        dx_hash, remaining_kwargs = self._get_creation_params(kwargs)
        self._new(dx_hash, **remaining_kwargs)

    def set_ids(self, dxid, project=None):
        '''
        :param dxid: Object ID
        :type dxid: string
        :param project: Project ID
        :type project: string

        Discards the currently stored ID and associates the handler
        with *dxid*.  Associates the handler with the copy of the
        object in *project*.  Uses the current workspace ID as the default.

        '''
        self._proj = None
        if is_dxlink(dxid):
            self._dxid, self._proj = get_dxlink_ids(dxid)
        else:
            self._dxid = dxid

        if self._proj is None and project is None:
            self._proj = dxpy.WORKSPACE_ID
        else:
            self._proj = project

    def get_id(self):
        '''
        :returns: Object ID of associated object
        :rtype: string

        Returns the object ID that the handler is currently associated
        with.

        '''

        return self._dxid

    def get_proj_id(self):
        '''
        :returns: Project ID of associated object
        :rtype: string

        Returns the project ID, if any, that the handler is currently associated with.

        '''

        return self._proj

    def describe(self, incl_properties=False, incl_details=False, **kwargs):
        """
        :param incl_properties: Whether to also include the properties of the object
        :type incl_properties: boolean
        :param incl_details: Whether to also include the details of the object
        :type incl_details: boolean
        :returns: Description of the remote object
        :rtype: dict

        Returns a dict with a description of the remote data object. The result
        includes the key-value pairs as specified in the API documentation,
        such as "id", "class", etc. Different classes of objects may have
        different fields.
        """

        if self._proj is not None:
            return self._describe(self._dxid, {"project": self._proj,
                                               "properties": incl_properties,
                                               "details": incl_details},
                                  **kwargs)
        else:
            return self._describe(self._dxid, {"properties": incl_properties,
                                               "details": incl_details},
                                  **kwargs)

    def add_types(self, types, **kwargs):
        """
        :param types: Types to add to the object
        :type types: list of strings

        Adds the list of types to the remote object.  Takes no action
        for types that are already listed for the object.

        This method can only be called if the object is in the "open"
        state.

        """

        self._add_types(self._dxid, {"types": types}, **kwargs)

    def remove_types(self, types, **kwargs):
        """
        :param types: Types to remove from the object
        :type types: list of strings

        Removes the list of types from the remote object.  Takes no
        action for types that the object does not currently have.

        This method can only be called if the object is in the "open"
        state.

        """

        self._remove_types(self._dxid, {"types": types}, **kwargs)

    def get_details(self, **kwargs):
        """
        Returns the contents of the details of the object.

        :rtype: list or dict
        """

        return self._get_details(self._dxid, **kwargs)

    def set_details(self, details, **kwargs):
        """
        :param details: Details to set for the object
        :type details: dict or list

        Sets the details for the remote object with the specified
        value.  If the input contains the string "$dnanexus_link" as a
        key in a hash, it must be the only key in the hash, and its
        value must be a valid ID of an existing object.

        This method can only be called if the object is in the "open"
        state.

        """

        return self._set_details(self._dxid, details, **kwargs)

    def hide(self, **kwargs):
        """
        Hides the remote object.

        This method can only be called if the object is in the "open"
        state.

        """

        return self._set_visibility(self._dxid, {"hidden": True}, **kwargs)

    def unhide(self, **kwargs):
        """
        Makes the remote object visible.

        This method can only be called if the object is in the "open"
        state.

        """

        return self._set_visibility(self._dxid, {"hidden": False}, **kwargs)

    def rename(self, name, **kwargs):
        """
        :param name: New name for the object
        :type name: string

        Renames the remote object.

        """

        return self._rename(self._dxid, {"project": self._proj,
                                         "name": name}, **kwargs)

    def get_properties(self, **kwargs):
        """
        :returns: Properties given as key-value pairs of strings
        :rtype: dict

        Returns the properties of the object.

        """
        return self.describe(incl_properties=True, **kwargs)["properties"]

    def set_properties(self, properties, **kwargs):
        """
        :param properties: Properties given as key-value pairs of strings; a value of :const:`None` indicates a property should be deleted
        :type properties: dict

        Given key-value pairs in *properties* for property names and
        values, the properties are set on the object for the given
        property names.  Note that existing properties not mentioned
        in *properties* are not changed by this method.

        The following example sets the properties for "name" and
        "project" for a remote gtable::

            dxgtable.set_properties({"name": "George", "project": "cancer"})

        Subsequently, the following would delete the property "project"::

            dxgtable.set_properties({"project": None})

        """

        self._set_properties(self._dxid, {"project": self._proj,
                                          "properties": properties},
                             **kwargs)

    def add_tags(self, tags, **kwargs):
        """
        :param tags: Tags to add to the object
        :type tags: list of strings

        Adds the list of tags to the remote object.  Takes no action
        for tags that are already listed for the object.

        """

        self._add_tags(self._dxid, {"project": self._proj, "tags": tags},
                       **kwargs)

    def remove_tags(self, tags, **kwargs):
        """
        :param tags: Tags to remove from the object
        :type tags: list of strings

        Removes the list of tags from the remote object.  Takes no
        action for tags that the object does not currently have.

        """

        self._remove_tags(self._dxid, {"project": self._proj, "tags": tags},
                          **kwargs)

    def close(self, **kwargs):
        """
        Closes the object for further modification to its types,
        details, visibility, and contents.

        """

        return self._close(self._dxid, **kwargs)

    def list_projects(self, **kwargs):
        """
        Returns a list of project IDs for the projects that contain
        this object and are visible to the requesting user.

        """

        return self._list_projects(self._dxid, **kwargs)

    def remove(self, **kwargs):
        '''
        :raises: :exc:`~dxpy.exceptions.DXError` if no project is associated with the object.

        Permanently removes the associated remote object from the associated project.
        '''

        if self._proj is None:
            raise DXError("Remove called when a project ID was not associated with this object handler")

        dxpy.api.projectRemoveObjects(self._proj, {"objects": [self._dxid]},
                                      **kwargs)

        # Reset internal state
        del self._dxid
        del self._proj

    def move(self, folder, **kwargs):
        '''
        :param folder: Folder route to which to move the object
        :type folder: string
        :raises: :exc:`~dxpy.exceptions.DXError` if no project is associated with the object

        Moves the associated remote object to *folder*.

        '''

        if self._proj is None:
            raise DXError("Move called when a project ID was not associated with this object handler")

        dxpy.api.projectMove(self._proj, {"objects": [self._dxid],
                                          "destination": folder},
                             **kwargs)


    def clone(self, project, folder="/", include_hidden_links=True,
              **kwargs):
        '''
        :param project: Destination project ID
        :type project: string
        :param folder: Folder route to which to move the object
        :type folder: string
        :raises: :exc:`~dxpy.exceptions.DXError` if no project is associated with the object
        :returns: An object handler for the new cloned object
        :rtype: :class:`DXDataObject`

        Clones the associated remote object to *folder* in *project*
        and returns an object handler for the new object.

        '''

        if self._proj is None:
            raise DXError("Clone called when a project ID was not associated with this object handler")

        dxpy.api.projectClone(self._proj,
                              {"objects": [self._dxid],
                               "project": project,
                               "destination": folder,
                               "includeHiddenLinks": include_hidden_links},
                              **kwargs)
        cloned_copy = copy.copy(self)
        cloned_copy.set_ids(cloned_copy.get_id(), project)
        return cloned_copy

    def _get_state(self, **kwargs):
        '''
        :returns: State of the remote object
        :rtype: string

        Queries the API server for the object's state.  Returns a string
        in {"open", "closing", "closed"}.

        Note that this function is shorthand for:

            dxclass.describe()["state"]

        '''

        return self.describe(**kwargs)["state"]

    def _wait_on_close(self, timeout=sys.maxint, **kwargs):
        elapsed = 0
        while True:
            state = self._get_state(**kwargs)
            if state == "closed":
                break
            if state != "closing":
                raise DXError("Unexpected state: " + state)

            if elapsed >= timeout or elapsed < 0:
                raise DXError("Reached timeout while waiting for the remote object to close")

            time.sleep(2)
            elapsed += 2

from dxfile import *
from dxfile_functions import *
from dxgtable import *
from dxgtable_functions import *
from dxrecord import *
from dxproject import *
from dxjob import *
from dxapplet import *
from dxapp import *
from search import *
from dxdataobject_functions import *
