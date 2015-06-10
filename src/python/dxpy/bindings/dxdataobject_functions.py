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

'''

These functions provide shorthand functional interfaces for actions such
as getting a :class:`~dxpy.bindings.DXDataObject` handler from an ID or
`link
<https://wiki.dnanexus.com/API-Specification-v1.0.0/Details-and-Links#Linking>`_,
or creating a link from a handler. In addition, there are functions for
performing simple actions with an ID or link as input without creating a
full object handler.

'''

from __future__ import (print_function, unicode_literals)

import dxpy
from . import DXDataObject
from . import __dict__ as all_bindings
from ..exceptions import DXError

def dxlink(object_id, project_id=None):
    '''
    :param object_id: Object ID or the object handler itself
    :type object_id: string or :class:`~dxpy.bindings.DXDataObject`
    :param project_id: A project ID, if creating a cross-project DXLink
    :type project_id: string

    Creates a DXLink (a dict formatted as a symbolic DNAnexus object
    reference) to the specified object.  Returns *object_id* if it
    appears to be a DXLink already.
    '''
    if isinstance(object_id, DXDataObject):
        object_id = object_id.get_id()
    if isinstance(object_id, dict):
        if '$dnanexus_link' in object_id:
            # In this case, dxlink was called on something that
            # already looks like a link
            return object_id
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
    if not isinstance(x, dict):
        return False
    if '$dnanexus_link' not in x:
        return False
    link = x['$dnanexus_link']
    if isinstance(link, basestring):
        return True
    elif isinstance(link, dict):
        return ('id' in link or 'job' in link)
    else:
        return False

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
    cls = all_bindings[class_name]
    return cls

def get_handler(id_or_link, project=None):
    '''
    :param id_or_link: String containing an object ID or dict containing a DXLink
    :type id_or_link: string or dict
    :param project: String project ID to use as the context if the the object is a data object
    :type project: string
    :rtype: :class:`~dxpy.bindings.DXObject`

    Parses a string or DXLink dict. Creates and returns an object handler for it.

    Example::

        get_handler("gtable-1234").get_col_names()
    '''
    try:
        cls = _guess_link_target_type(id_or_link)
    except Exception as e:
        raise DXError("Could not parse link {}: {}".format(id_or_link, e))

    if cls == dxpy.DXApp:
        # This special case should translate identifiers of the form
        # "app-name" or "app-name/version_or_tag" to the appropriate
        # arguments
        if dxpy.utils.resolver.is_hashid(id_or_link):
            return cls(id_or_link)
        else:
            slash_pos = id_or_link.find('/')
            if slash_pos == -1:
                return cls(name=id_or_link[4:])
            else:
                return cls(name=id_or_link[4:slash_pos],
                           alias=id_or_link[slash_pos + 1:])
    elif project is None or cls in [dxpy.DXJob, dxpy.DXAnalysis, dxpy.DXProject, dxpy.DXContainer]:
        # This case is important for the handlers which do not
        # take a project field
        return cls(id_or_link)
    else:
        return cls(id_or_link, project=project)

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
