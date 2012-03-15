"""
TODO: Put a docstring here
"""

from dxpy.bindings import *

##########
# DXUser #
##########

class DXUser(DXClass):
    """
    Remote user object handler

    .. note::

        User must already exist; user creation and deletion are not possible with this API.  Only the other remaining common methods of :class:`dxpy.bindings.DXClass` are available.

    """

    _class = "user"

    _describe = staticmethod(dxpy.api.userDescribe)
    _get_properties = staticmethod(dxpy.api.userGetProperties)
    _set_properties = staticmethod(dxpy.api.userSetProperties)
    _add_types = staticmethod(dxpy.api.userAddTypes)
    _remove_types = staticmethod(dxpy.api.userRemoveTypes)

    def destroy(self):
        """
        :raises: :exc:`NotImplementedError`

        User deletion is disallowed.  This method will always result
        in an exception.

        """

        raise NotImplementedError("Users cannot be destroyed using this API.")

###########
# DXGroup #
###########

def new_dxgroup(members=None):
    """
    :param members: List of object IDs of group members
    :type members: list of strings
    :rtype: :class:`dxpy.bindings.DXGroup`

    Creates a new group; if *members* is set, the group is initialized
    with the provided list of members.

    Note that this function is shorthand for::

        dxgroup = DXGroup()
        dxgroup.new()
        dxgroup.add_members(members)

    """

    dxgroup = DXGroup()
    dxgroup.new()
    dxgroup.add_members(members)
    return dxgroup

class DXGroup(DXClass):
    """Remote group object handler"""

    _class = "group"

    _describe = staticmethod(dxpy.api.groupDescribe)
    _get_properties = staticmethod(dxpy.api.groupGetProperties)
    _set_properties = staticmethod(dxpy.api.groupSetProperties)
    _add_types = staticmethod(dxpy.api.groupAddTypes)
    _remove_types = staticmethod(dxpy.api.groupRemoveTypes)
    _destroy = staticmethod(dxpy.api.groupDestroy)

    def new(self):
        """
        Creates a new group.
        """
        resp = dxpy.api.groupNew()
        self.set_id(resp["id"])

    def get_members(self):
        """
        :returns: List of members in the group by object ID
        :rtype: list of strings

        Returns a list of the members currently in the group.

        """
        raise NotImplementedError()

    def add_members(self, members):
        """
        :param members: List of object IDs of new members to add
        :type members: list of strings

        Adds the provided list of users to the group.  No action is
        taken for users already in the group.

        """
        raise NotImplementedError()

    def remove_members(self, members):
        """
        :param members: List of object IDs of members to remove
        :type members: list of strings

        Removes the provided list of users from the group.  No action
        is taken for users missing from the group.

        """
        raise NotImplementedError()
