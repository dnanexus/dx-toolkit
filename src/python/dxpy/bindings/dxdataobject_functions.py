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

'''

These functions provide shorthand functional interfaces for actions such
as getting a :class:`~dxpy.bindings.DXDataObject` handler from an ID or
`link
<https://wiki.dnanexus.com/API-Specification-v1.0.0/Details-and-Links#Linking>`_,
or creating a link from a handler. In addition, there are functions for
performing simple actions with an ID or link as input without creating a
full object handler.

'''

from __future__ import print_function, unicode_literals, division, absolute_import

import dxpy
from . import DXDataObject
from . import __dict__ as all_bindings
from ..exceptions import DXError
from ..compat import basestring

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
    class_name, _id = link.split("-", 1)
    class_name = 'DX'+class_name.capitalize()
    if class_name == 'DXGtable':
        class_name = 'DXGTable'
    if class_name == 'DXGlobalworkflow':
        class_name = 'DXGlobalWorkflow'
    cls = all_bindings[class_name]
    return cls

def get_handler(id_or_link, project=None):
    '''
    :param id_or_link: String containing an object ID or dict containing a DXLink
    :type id_or_link: string or dict
    :param project: String project ID to use as the context if the the object is a data object
    :type project: string
    :rtype: :class:`~dxpy.bindings.DXObject`, :class:`~dxpy.bindings.DXApp`, or :class:`~dxpy.bindings.DXGlobalWorkflow`

    Parses a string or DXLink dict. Creates and returns an object handler for it.

    Example::

        get_handler("file-1234")
    '''
    try:
        cls = _guess_link_target_type(id_or_link)
    except Exception as e:
        raise DXError("Could not parse link {}: {}".format(id_or_link, e))

    if cls in [dxpy.DXApp, dxpy.DXGlobalWorkflow]:
        # This special case should translate identifiers of the form
        # "app-name" or "app-name/version_or_tag" to the appropriate
        # arguments
        if dxpy.utils.resolver.is_hashid(id_or_link):
            return cls(id_or_link)
        else:
            slash_pos = id_or_link.find('/')
            dash_pos = id_or_link.find('-')
            if slash_pos == -1:
                return cls(name=id_or_link[dash_pos+1:])
            else:
                return cls(name=id_or_link[dash_pos+1:slash_pos],
                           alias=id_or_link[slash_pos + 1:])
    elif project is None or cls in [dxpy.DXJob, dxpy.DXAnalysis, dxpy.DXProject, dxpy.DXContainer]:
        # This case is important for the handlers which do not
        # take a project field
        return cls(id_or_link)
    else:
        return cls(id_or_link, project=project)

def describe(id_or_link, **kwargs):
    '''
    :param id_or_link: String containing an object ID or dict containing a DXLink,
                       or a list of object IDs or dicts containing a DXLink.

    Given an object ID, calls :meth:`~dxpy.bindings.DXDataObject.describe` on the object.

    Example::

        describe("file-1234")

    Given a list of object IDs, calls :meth:`~dxpy.api.system_describe_data_objects`.

    Example::

        describe(["file-1234", "workflow-5678"])

    Note: If id_or_link is a list and **kwargs contains a "fields" parameter, these
    fields will be returned in the response for each data object in addition to the
    fields included by default. Additionally, describe options can be provided for
    each data object class in the "classDescribeOptions" kwargs argument. See
    https://wiki.dnanexus.com/API-Specification-v1.0.0/System-Methods#API-method:-/system/describeDataObjects
    for input parameters used with the multiple object describe method.
    '''
    # If this is a list, extract the ids.
    # TODO: modify the procedure to use project ID when possible
    if isinstance(id_or_link, basestring) or is_dxlink(id_or_link):
        handler = get_handler(id_or_link)
        return handler.describe(**kwargs)
    else:
        links = []
        for link in id_or_link:
            # If this entry is a dxlink, then get the id.
            if is_dxlink(link):
                # Guaranteed by is_dxlink that one of the following will work
                if isinstance(link['$dnanexus_link'], basestring):
                    link = link['$dnanexus_link']
                else:
                    link = link['$dnanexus_link']['id']
            links.append(link)

        # Prepare input to system_describe_data_objects, the same fields will be passed
        # for all data object classes; if a class doesn't include a field in its describe
        # output, it will be ignored
        describe_input = \
            dict([(field, True) for field in kwargs['fields']]) if kwargs.get('fields', []) else True
        describe_links_input = [{'id': link, 'describe': describe_input} for link in links]
        bulk_describe_input = {'objects': describe_links_input}

        if 'classDescribeOptions' in kwargs:
            bulk_describe_input['classDescribeOptions'] = kwargs['classDescribeOptions']

        data_object_descriptions = dxpy.api.system_describe_data_objects(bulk_describe_input)
        return [desc['describe'] for desc in data_object_descriptions['results']]

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
