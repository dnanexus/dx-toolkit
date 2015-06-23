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
The :class:`DXObject` class is the abstract base class for all remote
object handlers, and its subclass :class:`DXDataObject` is the abstract
base class for all remote data object handlers.
"""

from __future__ import (print_function, unicode_literals)

import time, copy, re

import dxpy.api
from ..exceptions import (DXError, DXAPIError, DXFileError, DXGTableError, DXSearchError, DXAppletError,
                          DXJobFailureError, AppError, AppInternalError, DXCLIError)

def verify_string_dxid(dxid, expected_classes):
    '''
    :param dxid: Value to verify as a DNAnexus ID of class *expected_class*
    :param expected_classes: Single string or list of strings of allowed classes of the ID, e.g. "file" or ["project", "container"]
    :type expected_classes: string or list of strings
    :raises: :exc:`~dxpy.exceptions.DXError` if *dxid* is not a string or is not a valid DNAnexus ID of the expected class
    '''
    if isinstance(expected_classes, basestring):
        expected_classes = [expected_classes]
    if not isinstance(expected_classes, list) or len(expected_classes) == 0:
        raise DXError('verify_string_dxid: expected_classes should be a string or list of strings')
    if not (isinstance(dxid, basestring) and
            re.match('^(' + '|'.join(expected_classes) + ')-[0-9a-zA-Z]{24}$', dxid)):
        if len(expected_classes) == 1:
            str_expected_classes = expected_classes[0]
        elif len(expected_classes) == 2:
            str_expected_classes = ' or '.join(expected_classes)
        else:
            str_expected_classes = ', '.join(expected_classes[:-1]) + ', or ' + expected_classes[-1]

        raise DXError('Invalid ID of class %s: %r' % (str_expected_classes, dxid))

class DXObject(object):
    """Abstract base class for all remote object handlers."""

    def __init__(self, dxid=None, project=None):
        # Initialize _dxid and _proj to None values, and have
        # subclasses actually perform the setting of the values once
        # they have been validated.
        self._dxid, self._proj = None, None
        self._desc = {}

    def _repr(self, use_name=False):
        dxid = self._dxid if self._dxid is not None else "no ID stored"
        dxproj_id = self._proj if self._proj is not None else "no project ID stored"

        if use_name:
            if self._class not in ["container", "project", "app"]:
                desc = "<dxpy.{classname}: {name} ({dxid} ({dxproj_id}))>"
            else:
                desc = "<dxpy.{classname}: {name} ({dxid})>"
        else:
            if self._class not in ["container", "project", "app"]:
                desc = "<{module}.{classname} object at 0x{mem_loc:x}: {dxid} ({dxproj_id})>"
            else:
                desc = "<{module}.{classname} object at 0x{mem_loc:x}: {dxid}>"

        desc = desc.format(module=self.__module__,
                           classname=self.__class__.__name__,
                           dxid=dxid,
                           dxproj_id = dxproj_id,
                           mem_loc=id(self),
                           name=self._desc.get('name'))
        return desc

    def __str__(self):
        return self._repr(use_name=True)

    def __repr__(self):
        return self._repr()

    def __getattr__(self, attr):
        if not self._desc:
            self.describe()
        try:
            return self._desc[attr]
        except:
            raise AttributeError()

    def set_id(self, dxid):
        '''
        :param dxid: New ID to be associated with the handler
        :type dxid: string

        Discards the currently stored ID and associates the handler with *dxid*
        '''
        if dxid is not None:
            verify_string_dxid(dxid, self._class)

        self._dxid = dxid

    def get_id(self):
        '''
        :returns: ID of the associated object
        :rtype: string

        Returns the ID that the handler is currently associated with.

        '''

        return self._dxid

class DXDataObject(DXObject):
    """Abstract base class for all remote data object handlers.

    .. note:: The attribute values below are current as of the last time
              :meth:`~dxpy.bindings.DXDataObject.describe` was run.
              (Access to any of the below attributes causes
              :meth:`~dxpy.bindings.DXDataObject.describe` to be called
              if it has never been called before.)

    .. py:attribute:: name

       String giving the name of the object

    .. py:attribute:: folder

       String giving the full path to the folder containing the object

    .. py:attribute:: types

       List of strings indicating the types associated with the object

    .. py:attribute:: state

       A string containing one of the values "open", "closing", or "closed"

    .. py:attribute:: hidden

       Boolean indicating whether the object is hidden or not

    .. py:attribute:: links

       List of strings indicating object IDs that are pointed to by the
       object

    .. py:attribute:: sponsored

       Boolean indicating whether the object is sponsored by DNAnexus

    .. py:attribute:: tags

       List of strings indicating the tags that are assocated with the
       object

    .. py:attribute:: created

       Timestamp at which the object was created, in milliseconds since
       January 1, 1970 at midnight (UTC).

    .. py:attribute:: modified

       Timestamp at which the object was last modified, in milliseconds
       since January 1, 1970 at midnight (UTC).

    .. py:attribute:: createdBy

       dict containing the following keys and values:

       * user: the string ID of the user who created the object or
         launched the job that created it
       * job (optional): the string ID of the job that created the
         object, if a job created the object
       * executable (optional): the string ID of the app or applet that
         the job was running, if a job created the object

    """

    def __init__(self, dxid=None, project=None):
        if not hasattr(self, '_class'):
            raise NotImplementedError(
                "DXDataObject is an abstract class; a subclass should be initialized instead.")

        DXObject.__init__(self)
        self.set_ids(dxid, project)

    @staticmethod
    def _get_creation_params(kwargs):
        common_creation_params = {"project", "name", "tags", "types", "hidden", "properties", "details", "folder", "parents"}

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
        :param hidden: Whether the object is to be hidden
        :type hidden: boolean
        :param properties: Properties given as key-value pairs of strings
        :type properties: dict
        :param details: Details to set for the object
        :type details: dict or list
        :param folder: Full path to the destination folder
        :type folder: string
        :param parents: If True, recursively create all parent folders if they are missing
        :type parents: boolean

        :rtype: :class:`DXDataObject`

        Creates a data object with the given fields. Only *project* is
        required, and only if no default project or workspace is set;
        the remaining arguments are optional and have default behavior
        as specified in the API documentation for the ``/new`` method of
        each data object class.

        '''
        if not hasattr(self, '_class'):
            raise NotImplementedError(
                "DXDataObject is an abstract class; a subclass should" + \
                    "be initialized instead.")

        dx_hash, remaining_kwargs = self._get_creation_params(kwargs)
        self._new(dx_hash, **remaining_kwargs)

    def set_id(self, dxid):
        '''
        :param dxid: Object ID or a DNAnexus link (a dict with key "$dnanexus_link"); if a project ID is provided in the DNAnexus link, it will also be used to set the project ID
        :type dxid: string or dict

        Equivalent to calling
        :meth:`~dxpy.bindings.DXDataObject.set_ids` with the same
        arguments.
        '''
        self.set_ids(dxid)

    def set_ids(self, dxid, project=None):
        '''
        :param dxid: Object ID or a DNAnexus link (a dict with key "$dnanexus_link"); if a project ID is provided in the DNAnexus link, it will be used as *project* unless *project* has been explictly provided
        :type dxid: string or dict
        :param project: Project ID
        :type project: string

        Discards the currently stored ID and associates the handler with
        *dxid*. Associates the handler with the copy of the object in
        *project* (if no project is explicitly specified, the default
        data container is used).

        '''
        if is_dxlink(dxid):
            dxid, project_from_link = get_dxlink_ids(dxid)
            if project is None:
                project = project_from_link

        if dxid is not None:
            verify_string_dxid(dxid, self._class)
        self._dxid = dxid

        if project is None:
            self._proj = dxpy.WORKSPACE_ID
        elif project is not None:
            verify_string_dxid(project, ['project', 'container'])
            self._proj = project

    def get_proj_id(self):
        '''
        :returns: Project ID of associated object
        :rtype: string

        Returns the project ID, if any, that the handler is currently
        associated with.

        '''

        return self._proj

    def describe(self, fields=None, default_fields=None, incl_properties=False, incl_details=False, **kwargs):
        """
        :param fields: set of fields to include in the output, for
            example ``{'name', 'modified'}``. The field ``id`` is always
            implicitly included. If ``fields`` is specified, the default
            fields are not included (that is, only the fields specified
            here, and ``id``, are included) unless ``default_fields`` is
            additionally set to True.
        :type fields: set or sequence of str
        :param default_fields: if True, include the default fields in
            addition to fields requested in ``fields``, if any; if
            False, only the fields specified in ``fields``, if any, are
            returned (defaults to False if ``fields`` is specified, True
            otherwise)
        :type default_fields: bool
        :param incl_properties: if true, includes the properties of the
            object in the output (deprecated; use
            ``fields={'properties'}, default_fields=True`` instead)
        :type incl_properties: bool
        :param incl_details: if true, includes the details of the object
            in the output (deprecated; use ``fields={'details'},
            default_fields=True`` instead)
        :type incl_details: bool
        :returns: Description of the remote object
        :rtype: dict

        Return a dict with a description of the remote data object.

        The result includes the key-value pairs as specified in the API
        documentation for the ``/describe`` method of each data object
        class. The API defines some default set of fields that will be
        included (at a minimum, "id", "class", etc. should be available,
        and there may be additional fields that vary based on the
        class); the set of fields may be customized using ``fields`` and
        ``default_fields``.

        Any project-specific metadata fields (name, properties, and
        tags) are obtained from the copy of the object in the project
        associated with the handler, if possible.

        """

        if self._dxid is None:
            raise DXError('This {handler} handler has not been initialized with a {_class} ID and cannot be described'.format(
                handler=self.__class__.__name__,
                _class=self._class)
            )

        if (incl_properties or incl_details) and (fields is not None or default_fields is not None):
            raise ValueError('Cannot specify properties or details in conjunction with fields or default_fields')

        if incl_properties or incl_details:
            describe_input = dict(properties=incl_properties, details=incl_details)
        else:
            describe_input = {}
            if default_fields is not None:
                describe_input['defaultFields'] = default_fields
            if fields is not None:
                describe_input['fields'] = {field_name: True for field_name in fields}

        if self._proj is not None:
            describe_input["project"] = self._proj

        self._desc = self._describe(self._dxid, describe_input, **kwargs)

        return self._desc

    def add_types(self, types, **kwargs):
        """
        :param types: Types to add to the object
        :type types: list of strings
        :raises: :class:`~dxpy.exceptions.DXAPIError` if the object is not in the "open" state

        Adds each of the specified types to the remote object. Takes no
        action for types that are already listed for the object.

        """

        self._add_types(self._dxid, {"types": types}, **kwargs)

    def remove_types(self, types, **kwargs):
        """
        :param types: Types to remove from the object
        :type types: list of strings
        :raises: :class:`~dxpy.exceptions.DXAPIError` if the object is not in the "open" state

        Removes each the specified types from the remote object. Takes
        no action for types that the object does not currently have.

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
        :raises: :class:`~dxpy.exceptions.DXAPIError` if the object is not in the "open" state

        Sets the details for the remote object with the specified value.
        If the input contains the string ``"$dnanexus_link"`` as a key
        in a hash, it must be the only key in the hash, and its value
        must be a valid ID of an existing object.

        """

        return self._set_details(self._dxid, details, **kwargs)

    def hide(self, **kwargs):
        """
        :raises: :class:`~dxpy.exceptions.DXAPIError` if the object is not in the "open" state

        Hides the remote object.

        """

        return self._set_visibility(self._dxid, {"hidden": True}, **kwargs)

    def unhide(self, **kwargs):
        """
        :raises: :class:`~dxpy.exceptions.DXAPIError` if the object is not in the "open" state

        Makes the remote object visible.

        """

        return self._set_visibility(self._dxid, {"hidden": False}, **kwargs)

    def rename(self, name, **kwargs):
        """
        :param name: New name for the object
        :type name: string

        Renames the remote object.

        The name is changed on the copy of the object in the project
        associated with the handler.

        """

        return self._rename(self._dxid, {"project": self._proj,
                                         "name": name}, **kwargs)

    def get_properties(self, **kwargs):
        """
        :returns: Properties given as key-value pairs of strings
        :rtype: dict

        Returns the properties of the object.

        The properties are read from the copy of the object in the
        project associated with the handler.

        """
        return self.describe(incl_properties=True, **kwargs)["properties"]

    def set_properties(self, properties, **kwargs):
        """
        :param properties: Property names and values given as key-value pairs of strings
        :type properties: dict

        Given key-value pairs in *properties* for property names and
        values, the properties are set on the object for the given
        property names. Any property with a value of :const:`None`
        indicates the property will be deleted.

        .. note:: Any existing properties not mentioned in *properties*
           are not modified by this method.

        The properties are written to the copy of the object in the
        project associated with the handler.

        The following example sets the properties for "name" and
        "project" for a remote GTable::

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

        Adds each of the specified tags to the remote object. Takes no
        action for tags that are already listed for the object.

        The tags are added to the copy of the object in the project
        associated with the handler.

        """

        self._add_tags(self._dxid, {"project": self._proj, "tags": tags},
                       **kwargs)

    def remove_tags(self, tags, **kwargs):
        """
        :param tags: Tags to remove from the object
        :type tags: list of strings

        Removes each of the specified tags from the remote object. Takes
        no action for tags that the object does not currently have.

        The tags are removed from the copy of the object in the project
        associated with the handler.

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
        :rtype: list of strings

        Returns a list of project IDs of the projects that contain this
        object and are visible to the requesting user.

        """

        return self._list_projects(self._dxid, **kwargs)

    def remove(self, **kwargs):
        '''
        :raises: :exc:`~dxpy.exceptions.DXError` if no project is associated with the object

        Permanently removes the associated remote object from the
        associated project.
        '''

        if self._proj is None:
            raise DXError("Remove called when a project ID was not associated with this object handler")

        dxpy.api.project_remove_objects(self._proj, {"objects": [self._dxid]},
                                        **kwargs)

        # Reset internal state
        self._dxid = None
        self._proj = None
        self._desc = {}

    def move(self, folder, **kwargs):
        '''
        :param folder: Folder route to which to move the object
        :type folder: string
        :raises: :exc:`~dxpy.exceptions.DXError` if no project is associated with the object

        Moves the associated remote object to *folder*.

        '''

        if self._proj is None:
            raise DXError("Move called when a project ID was not associated with this object handler")

        dxpy.api.project_move(self._proj, {"objects": [self._dxid],
                                           "destination": folder},
                              **kwargs)


    def clone(self, project, folder="/", include_hidden_links=True,
              **kwargs):
        '''
        :param project: Destination project ID
        :type project: string
        :param folder: Folder route to which to move the object
        :type folder: string
        :param include_hidden_links: If True, hidden objects linked to by this object are also cloned into the destination project
        :type include_hidden_links: boolean
        :raises: :exc:`~dxpy.exceptions.DXError` if no project is associated with the object
        :returns: An object handler for the new cloned object
        :rtype: :class:`DXDataObject`

        Clones the associated remote object to *folder* in *project* and
        returns an object handler for the new object in the destination
        project.

        '''

        if self._proj is None:
            raise DXError("Clone called when a project ID was not associated with this object handler")

        dxpy.api.project_clone(self._proj,
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

        return self.describe(fields={'state'}, **kwargs)["state"]

    def _wait_on_close(self, timeout=3600*24*1, **kwargs):
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

from .dxfile import DXFile, DXFILE_HTTP_THREADS, DEFAULT_BUFFER_SIZE
from .dxfile_functions import open_dxfile, new_dxfile, download_dxfile, upload_local_file, upload_string
from .dxgtable import DXGTable, NULL, DXGTABLE_HTTP_THREADS
from .dxgtable_functions import open_dxgtable, new_dxgtable
from .dxrecord import DXRecord, new_dxrecord
from .dxproject import DXContainer, DXProject
from .dxjob import DXJob, new_dxjob
from .dxanalysis import DXAnalysis
from .dxapplet import DXExecutable, DXApplet
from .dxapp import DXApp
from .dxworkflow import DXWorkflow, new_dxworkflow
from .auth import user_info, whoami
from .dxdataobject_functions import dxlink, is_dxlink, get_dxlink_ids, get_handler, describe, get_details, remove
from .search import (find_data_objects, find_executions, find_jobs, find_analyses, find_projects, find_apps,
                     find_one_data_object, find_one_project, find_one_app)
