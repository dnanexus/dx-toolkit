# Copyright (C) 2013-2016 DNAnexus, Inc.
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
Records are the most basic data object and do not store additional data beyond those available in
all data objects (details, properties, etc.).
"""

from __future__ import print_function, unicode_literals, division, absolute_import

import dxpy
from . import DXDataObject
from ..exceptions import DXError

############
# DXRecord #
############

def new_dxrecord(details=None, **kwargs):
    '''
    :rtype: :class:`DXRecord`

    :param details: The contents of the record to be created.
    :type details: dict

    Additional optional parameters not listed: all those under
    :func:`dxpy.bindings.DXDataObject.new`, except `details`.

    Creates a new remote record object with project set to *project*
    and returns the appropriate handler.

    Example:

        r = dxpy.new_dxrecord({"x": 1, "y": 2})

    Note that this function is shorthand for::

        dxrecord = DXRecord()
        dxrecord.new(**kwargs)

    '''
    dxrecord = DXRecord()
    dxrecord.new(details=details, **kwargs)
    return dxrecord

class DXRecord(DXDataObject):
    '''
    Remote record object handler.
    '''

    _class = "record"

    _describe = staticmethod(dxpy.api.record_describe)
    _add_types = staticmethod(dxpy.api.record_add_types)
    _remove_types = staticmethod(dxpy.api.record_remove_types)
    _get_details = staticmethod(dxpy.api.record_get_details)
    _set_details = staticmethod(dxpy.api.record_set_details)
    _set_visibility = staticmethod(dxpy.api.record_set_visibility)
    _rename = staticmethod(dxpy.api.record_rename)
    _set_properties = staticmethod(dxpy.api.record_set_properties)
    _add_tags = staticmethod(dxpy.api.record_add_tags)
    _remove_tags = staticmethod(dxpy.api.record_remove_tags)
    _close = staticmethod(dxpy.api.record_close)
    _list_projects = staticmethod(dxpy.api.record_list_projects)

    def _new(self, dx_hash, close=False, **kwargs):
        """
        :param dx_hash: Standard hash populated in :func:`dxpy.bindings.DXDataObject.new()` containing attributes common to all data object classes.
        :type dx_hash: dict
        :param init_from: Record from which to initialize the metadata
        :type init_from: :class:`DXRecord`
        :param close: Whether or not to close the record immediately after creating it
        :type close: boolean

        Create a new remote record object.

        """

        if "init_from" in kwargs:
            if kwargs["init_from"] is not None:
                if not isinstance(kwargs["init_from"], DXRecord):
                    raise DXError("Expected instance of DXRecord to init_from")
                dx_hash["initializeFrom"] = \
                    {"id": kwargs["init_from"].get_id(),
                     "project": kwargs["init_from"].get_proj_id()}
            del kwargs["init_from"]

        if close:
            dx_hash["close"] = True

        resp = dxpy.api.record_new(dx_hash, **kwargs)
        self.set_ids(resp["id"], dx_hash["project"])
