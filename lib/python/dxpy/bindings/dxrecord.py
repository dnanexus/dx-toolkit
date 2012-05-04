"""
Records are the most basic data object and do not store additional
data beyond associated details, properties, etc.
"""

from dxpy.bindings import *

############
# DXRecord #
############

def new_dxrecord(**kwargs):
    '''
    :rtype: :class:`DXRecord`

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`.

    Creates a new remote record object with contents set to *project*
    and returns the appropriate handler.

    Note that this function is shorthand for::

        dxrecord = DXRecord()
        dxrecord.new(**kwargs)

    '''
    dxrecord = DXRecord()
    dxrecord.new(**kwargs)
    return dxrecord

class DXRecord(DXDataObject):
    '''
    Remote record object handler

    '''

    _class = "record"

    _describe = staticmethod(dxpy.api.recordDescribe)
    _add_types = staticmethod(dxpy.api.recordAddTypes)
    _remove_types = staticmethod(dxpy.api.recordRemoveTypes)
    _get_details = staticmethod(dxpy.api.recordGetDetails)
    _set_details = staticmethod(dxpy.api.recordSetDetails)
    _set_visibility = staticmethod(dxpy.api.recordSetVisibility)
    _rename = staticmethod(dxpy.api.recordRename)
    _set_properties = staticmethod(dxpy.api.recordSetProperties)
    _add_tags = staticmethod(dxpy.api.recordAddTags)
    _remove_tags = staticmethod(dxpy.api.recordRemoveTags)
    _close = staticmethod(dxpy.api.recordClose)
    _list_projects = staticmethod(dxpy.api.recordListProjects)

    def _new(self, dx_hash, **kwargs):
        """
        :param dx_hash: Standard hash populated in :func:`dxpy.bindings.DXDataObject.new()`
        :type dx_hash: dict
        :param init_from: Record from which to initialize the metadata
        :type init_from: :class:`DXRecord`

        Create a new remote record object.

        """

        if "init_from" in kwargs and kwargs["init_from"] is not None:
            if not isinstance(kwargs["init_from"], DXRecord):
                raise DXError("Expected instance of DXRecord to init_from")
            dx_hash["initializeFrom"] = \
                {"id": kwargs["init_from"].get_id(),
                 "project": kwargs["init_from"].get_proj_id()}
            del kwargs["init_from"]
        resp = dxpy.api.recordNew(dx_hash, **kwargs)
        self.set_ids(resp["id"], dx_hash["project"])
