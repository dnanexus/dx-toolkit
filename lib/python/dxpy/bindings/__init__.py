"""
This module contains useful Python bindings for interacting with the
Platform API.  The :func:`dxpy.bindings.search` function provides
search functionality over all remote objects managed by the API
server: users, groups, JSON objects, files, tables, collections, apps,
and jobs.  Each of these remote objects can be represented locally by
a handler that inherits from the abstract class
:class:`dxpy.bindings.DXClass`.  This abstract base class supports
functionality common to all of the remote object classes: getting and
setting properties, permissions, and types, as well as remotely
destroying the object permanently.

To access a preexisting object, a remote handler for that class can be
set up via two methods: the constructor or the
:meth:`dxpy.bindings.DXClass.setID` method.  For example::

    dxFileHandle = DXFile("file-1234")

    dxOtherFH = DXFile()
    dxOtherFH.set_id("file-4321")

Both these methods do not perform API calls and merely sets the state
of the remote file handler.  The object ID stored in the handler can
be overwritten with subsequent calls to
:meth:`dxpy.bindings.DXClass.setID`.

Creation of a new object can be performed using the method
:meth:`dxpy.bindings.DXClass.new` which usually has a different
specification for each subclass of :class:`dxpy.bindings.DXClass`::

    newDXFileHandle = DXFile().new()

Additional functions that are shorthand for some of these common use
cases are provided for some of the classes.  For instance, there is a
function for opening a preexisting remote file
(:func:`dxpy.bindings.open_dxfile`), and one for opening a new file to
be modified (:func:`dxpy.bindings.new_dxfile`), both of which
return a remote object handler on which the other methods can be
called.

In addition, class-specific handlers such as
:class:`dxpy.bindings.DXFile` provide extra functionality for the
respective class.  For example, in the case of files, reading,
writing, downloading, and uploading files are all supported.  

Though not yet documented as such, all methods which interact with the
API server may raise the exception :exc:`dxpy.exceptions.DXAPIError`

"""

import time, re, requests, sys
import dxpy.api
from dxpy.exceptions import *

def search(classname=None, properties=None, typename=None, #permission=None,
           describe=False):
    """
    :param classname: Class with which to restrict the search, i.e. one of {"user", "group", "json", "file", "table", "collection", "app", "job"}
    :type classname: string
    :param properties: Properties (key-value pairs) that each result must have
    :type properties: dict
    :param typename: Type that each result must conform to
    :type typename: string
..    :param permission: Permission pair of subject and permission type that each result must have
..    :type permission: list of two str
    :param describe: Whether to return each item as the output of calling describe() on the object (if given True) or to return each item as its object ID (False)
    :type describe: boolean
    :rtype: generator
    :raises: :exc:`dxpy.exceptions.DXAPIError`

    This is a generator function which returns the search results and
    handles fetching of future chunks if necessary.  The search is not
    restricted by any fields which are omitted and otherwise imposes
    the restrictions requested.

    These two examples iterates through all tables with property
    "project" set to "cancer project" and prints their object IDs::

        for result in search(classname="table", properties={"project": "cancer project"}):
            print "Found table with object id " + result

        for result in search(classname="table", properties={"project": "cancer project"}, describe=True):
            print "Found table with object id " + result["id"]

    """
    query = {}
    if classname is not None:
        query["class"] = classname
    if properties is not None:
        query["properties"] = properties
    if typename is not None:
        query["type"] = typename
    # if permission is not None:
    #     query["permission"] = permission
    query["describe"] = describe

    while True:
        resp = dxpy.api.systemSearch(query)
        
        for i in resp["results"]:
            yield i

        # set up next query
        if resp["next"] is not None:
            query["starting"] = resp["next"]
        else:
            raise StopIteration()

class DXClass(object):
    """Abstract base class for all remote object handlers"""

    def __init__(self, dxid=None):
        """Direct initialization of this class is not allowed.

        """
        try:
            self._class
        except:
            raise NotImplementedError(
                "DXClass is an abstract class; a subclass should" + \
                    "be initialized instead.")
        finally:
            if dxid is not None:
                self.set_id(dxid)

    def __str__(self):
        desc = "dxpy." + self.__class__.__name__ + " (" + self._class + ") object: "
        try:
            desc += self.get_id()
        except:
            desc += "no ID stored"
        return desc

    def set_id(self, dxid):
        '''
        :param dxid: Object ID
        :type dxid: string
        :raises: :exc:`dxpy.exceptions.DXError` if *dxid* does not match class type

        Discards the currently stored ID and associates the handler
        with *dxid*.

        '''
        if re.match(self._class + "-[0-9a-fA-F]{24}", dxid) is None or \
                len(dxid) != len(self._class) + 25:
            raise DXError("Given object ID does not match expected format")

        self._dxid = dxid

    def get_id(self):
        '''
        :returns: Object ID of associated object
        :rtype: string

        Returns the object ID that the handler is currently associated
        with.

        '''

        return self._dxid

    def new(self):
        '''
        :raises: NotImplementedError

        This is a virtual method for creating a new object.  Most
        subclasses will have specialized input for creating the object
        that should be called instead.

        '''

        raise NotImplementedError("This is a virtual method that should"+
                                  " not be called.")

    def describe(self):
        """
        :returns: Description of the remote object
        :rtype: dict

        Returns a dictionary which will include the keys "id",
        "class", "types", and "createdAt".  Other fields may also be
        included, depending on the class.

        """

        return self._describe(self._dxid)

    def get_properties(self, keys=None):
        """
        :param keys: List of keys to look up
        :type keys: list
        :returns: The requested properties, keyed by their respective keys
        :rtype: dict

        Returns the properties for the requested keys.  If no keys are
        given, then all properties are reported.  If the property is
        not found, the value for the key is :const:`None`

        The following example queries the remote table represented by
        *dxtable* for the properties with keys "name" and "project"::

            results = dxtable.get_properties(["name", "project"])
            print "Table name is " + results["name"]

        """

        input_ = {}
        if keys is not None:
            input_ = {"keys": keys}

        return self._get_properties(self._dxid, input_)

    def set_properties(self, properties):
        """
        :param properties: Properties given as key-value pairs; a value of :const:`None` indicates a property should be deleted
        :type properties: dict

        Given key-value pairs in *properties* for property names and
        values, the properties are set on the object for the given
        property names.  Note that existing properties not mentioned
        in *properties* are not changed by this method.

        The following example sets the properties for "name" and
        "project" for a remote table::

            dxtable.set_properties({"name": "George", "project": "cancer"})

        The following line would delete the property "project"::

            dxtable.set_properties({"project": None})

        """

        self._set_properties(self._dxid, properties)

    def get_permissions(self):
        """
        :returns: List of permission pairs
        :rtype: list of lists

        Returns all permissions listed for the object.  Example::

            >>> print dxtable.get_permissions()
            [["user-xxxx", "OWN"], ["group-xxxx", "LIST"], ["group-xxxx", "READ"]]

        """
        raise NotImplementedError()

    def grant_permission(self, subject_id, permission):
        """
        :param subject_id: Object ID of the entity to be granted some permission
        :type subject_id: string
        :param permission: Permission type from the set {LIST, EDIT, READ, WRITE, RUN, AUDIT, MANAGE, OWN}
        :type permission: string

        Grants the entity with object ID *subject_id* with with
        permission type *permission*.  See the API for details on what
        each permission type provides.

        The following gives a group with ID "group-xxxx" permissions
        to modify a particular json object::

            dxjson.grant_permission("group-xxxx", "WRITE")

        """
        raise NotImplementedError()

    def revoke_permission(self, subject_id, permission):
        """
        :param subject_id: Object ID of the entity to have its permission revoked
        :type subject_id: string
        :param permission: Permission type from the set {LIST, EDIT, READ, WRITE, RUN, AUDIT, MANAGE, OWN}
        :type permission: string

        Removes the permission type *permission* from the entity with
        ID *subject_id*.  See the API for details on what each
        permission type provides.

        The following removes the permissions for a group with ID
        "group-xxxx" to modify a particular json object::

            dxjson.revoke_permission("group-xxxx", "WRITE")

        """
        raise NotImplementedError()

    def get_types(self):
        """
        :returns: List of types
        :rtype: list of strings

        Returns a list of the types with which the object has been
        labelled.

        Note that this function is shorthand for:

            desc = self.describe()
            desc["types"]

        """

        desc = self.describe()
        return desc["types"]

    def add_types(self, types):
        """
        :param types: Types to add to the object
        :type types: list of strings

        Adds the list of types to the remote object.  Takes no action
        for types that are already listed for the object.

        """

        self._add_types(self._dxid, {"types": types})

    def remove_types(self, types):
        """
        :param types: Types to remove from the object
        :type types: list of strings

        Removes the list of types from the remote object.  Takes no
        action for types that the object does not currently have.

        """

        self._remove_types(self._dxid, {"types": types})

    def destroy(self):
        '''Permanently destroy the associated remote object.
        '''

        self._destroy(self._dxid)

        # Reset internal state
        del self._dxid

    def _get_state(self):
        '''
        :returns: State of the remote object
        :rtype: string

        Queries the API server for the object's state.  Returns a string
        in {"open", "closing", "closed"}.

        Note that this function is shorthand for:

            dxclass.describe()["state"]

        '''

        return self.describe()["state"]

    def _wait_on_close(self, timeout=sys.maxint):
        elapsed = 0
        while True:
            state = self._get_state()
            if state == "closed":
                break
            if state != "closing":
                raise DXError("Unexpected state: " + state)

            if elapsed >= timeout or elapsed < 0:
                raise DXError("Reached timeout while waiting for the remote object to close")

            time.sleep(2)
            elapsed += 2

from dxusergroup import *
from dxfile import *
from dxfile_functions import *
from dxtable import *
from dxtable_functions import *
from dxjsoncollection import *
from dxappjob import *
