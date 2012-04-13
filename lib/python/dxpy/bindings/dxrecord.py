"""
TODO: Write something here
"""

from dxpy.bindings import *

############
# DXRecord #
############

def new_dxrecord(**kwargs):
    '''
    :param project: Project ID in which to create the new remote object
    :type project: string
    :rtype: :class:`dxpy.bindings.DXRecord`

    Creates a new remote record object with contents set to *project*
    and returns the appropriate handler.

    Note that this function is shorthand for::

        dxrecord = DXRecord()
        dxrecord.new(**kwargs)

    '''
    dxrecord = DXRecord()
    dxrecord.new(**kwargs)
    return dxrecord

class DXRecord(DXDataObjClass):
    '''Remote record object handler'''

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
        :param project: Project ID in which to create the new remote object
        :type project: string

        Create a new remote record object.

        """
        resp = dxpy.api.recordNew(dx_hash)
        self.set_ids(resp["id"], dx_hash["project"])
