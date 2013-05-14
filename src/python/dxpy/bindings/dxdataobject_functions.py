# Copyright (C) 2013 DNAnexus, Inc.
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

'''

These functions provide shorthand functional interfaces for actions such
as getting a :class:`~dxpy.bindings.DXDataObject` handler from an ID or
`link
<https://wiki.dnanexus.com/API-Specification-v1.0.0/Details-and-Links#Linking>`_,
or creating a link from a handler. In addition, there are functions for
performing simple actions with an ID or link as input without creating a
full object handler.

'''

from dxpy.bindings import *

def dxlink(object_id, project_id=None):
    '''
    :param object_id: Object ID or the object handler itself
    :type object_id: string or :class:`~dxpy.bindings.DXDataObject`
    :param project_id: A project ID, if creating a cross-project DXLink
    :type project_id: string

    Creates a DXLink (a dict formatted as a symbolic DNAnexus object
    reference) to the specified object.
    '''
    if isinstance(object_id, DXDataObject):
        object_id = object_id.get_id()
    if project_id is None:
        return {'$dnanexus_link': object_id}
    else:
        return {'$dnanexus_link': {'project': project_id, 'id': object_id}}

def is_dxlink(x):
    '''
    :param x: A potential DNAnexus link

    Returns whether *x* appears to be a DNAnexus link (is a dict with
    key ``"$dnanexus_link"``) with a referenced data object.
    '''
    return isinstance(x, dict) and '$dnanexus_link' in x and (isinstance(x['$dnanexus_link'], basestring) or isinstance(x['$dnanexus_link'], dict) and 'id' in x['$dnanexus_link'])

def get_dxlink_ids(link):
    '''
    :param link: A DNAnexus link
    :type link: dict
    :returns: Object ID, Project ID (or :const:`None` if no project specified in the link)
    :rtype: tuple

    Returns the object and project IDs stored in the given DNAnexus
    link.
    '''
    if isinstance(link['$dnanexus_link'], dict):
        return link['$dnanexus_link']['id'], link['$dnanexus_link'].get('project')
    else:
        return link['$dnanexus_link'], None

def _guess_link_target_type(link):
    if is_dxlink(link):
        # Guaranteed by is_dxlink that one of the following will work
        if isinstance(link['$dnanexus_link'], basestring):
            link = link['$dnanexus_link']
        else:
            link = link['$dnanexus_link']['id']
    class_name, _id = link.split("-")
    class_name = 'DX'+class_name.capitalize()
    if class_name == 'DXGtable':
        class_name = 'DXGTable'
    cls = dxpy.__dict__[class_name]
    return cls

def get_handler(id_or_link, project=None):
    '''
    :param id_or_link: String containing an object ID or dict containing a DXLink
    :rtype: :class:`~dxpy.bindings.DXDataObject` or :class:`~dxpy.bindings.DXProject`

    Parses a string or DXLink dict. Creates and returns an object handler for it.

    Example::

        get_handler("gtable-1234").get_col_names()
    '''
    try:
        cls = _guess_link_target_type(id_or_link)
        if project is not None:
            return cls(id_or_link, project=project)
        else:
            # This case is important for the DXProject handler
            return cls(id_or_link)
    except Exception as e:
        raise DXError("Could not parse link "+str(id_or_link))

def describe(id_or_link, **kwargs):
    '''
    :param id_or_link: String containing an object ID or dict containing a DXLink

    Given an object ID, calls :meth:`~dxpy.bindings.DXDataObject.describe` on the object.

    Example::

        describe("file-1234")
    '''
    handler = get_handler(id_or_link)
    return handler.describe(**kwargs)

def get_details(id_or_link, **kwargs):
    '''
    :param id_or_link: String containing an object ID or dict containing a DXLink

    Given an object ID, calls :meth:`~dxpy.bindings.DXDataObject.get_details` on the object.

    Example::

        get_details("file-1234")
    '''
    handler = get_handler(id_or_link)
    return handler.get_details(**kwargs)

def remove(id_or_link, **kwargs):
    '''
    :param id_or_link: String containing an object ID or dict containing a DXLink

    Given an object ID, calls :meth:`~dxpy.bindings.DXDataObject.remove` on the object.

    Example::

        remove("file-1234")
    '''
    handler = get_handler(id_or_link)
    return handler.remove(**kwargs)
