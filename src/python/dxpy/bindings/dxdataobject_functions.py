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
<https://documentation.dnanexus.com/developer/api/data-object-lifecycle/details-and-links#linking>`_,
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

def dxlink(object_id, project_id=None, field=None):
    '''
    :param object_id: Object ID or the object handler itself
    :type object_id: string or :class:`~dxpy.bindings.DXDataObject`
    :param project_id: A project ID, if creating a cross-project DXLink
    :type project_id: string
    :param field: A field name, if creating a job-based object reference
    :type field: string
    :returns: A dict formatted as a symbolic DNAnexus object reference
    :rtype: dict

    Creates a DXLink to the specified object.

    If `object_id` is already a link, it is returned without modification.

    If `object_id is a `~dxpy.bindings.DXDataObject`, the object ID is
    retrieved via its `get_id()` method.

    If `field` is not `None`, `object_id` is expected to be of class 'job'
    and the link created is a Job Based Object Reference (JBOR), which is
    of the form::

        {'$dnanexus_link': {'job': object_id, 'field': field}}

    If `field` is `None` and `project_id` is not `None`, the link created
    is a project-specific link of the form::

        {'$dnanexus_link': {'project': project_id, 'id': object_id}}
    '''
    if is_dxlink(object_id):
        return object_id
    if isinstance(object_id, DXDataObject):
        object_id = object_id.get_id()
    if not any((project_id, field)):
        return {'$dnanexus_link': object_id}
    elif field:
        dxpy.verify_string_dxid(object_id, "job")
        return {'$dnanexus_link': {'job': object_id, 'field': field}}
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
        return any(key in link for key in ('id', 'job'))
    return False

def get_dxlink_ids(link):
    '''
    :param link: A DNAnexus link
    :type link: dict
    :returns: (Object ID, Project ID) if the link is to a data object (or :const:`None`
        if no project specified in the link), or (Job ID, Field) if the link is
        a job-based object reference (JBOR).
    :rtype: tuple

    Get the object ID and detail from a link. There are three types of links:

    * Simple link of the form ``{"$dnanexus_link": "file-XXXX"}`` returns
      ``("file-XXXX", None)``.
    * Data object link of the form ``{"$dnanexus_link': {"id": "file-XXXX",
      "project": "project-XXXX"}}`` returns ``("file-XXXX", "project-XXXX")``.
    * Job-based object reference (JBOR) of the form ``{"$dnanexus_link":
      {"job": "job-XXXX", "field": "foo"}}`` returns ``("job-XXXX", "foo")``.
    '''
    if not is_dxlink(link):
        raise DXError('Invalid link: %r' % link)
    if isinstance(link['$dnanexus_link'], basestring):
        return link['$dnanexus_link'], None
    elif 'id' in link['$dnanexus_link']:
        return link['$dnanexus_link']['id'], link['$dnanexus_link'].get('project')
    else:
        return link['$dnanexus_link']['job'], link['$dnanexus_link']['field']

def _guess_link_target_type(id_or_link):
    # Get the object ID if the input is a link
    object_id = get_dxlink_ids(id_or_link)[0] if is_dxlink(id_or_link) else id_or_link
    class_name = 'DX' + object_id.split("-", 1)[0].capitalize()
    if class_name not in all_bindings:
        class_name = {
            'DXGlobalworkflow': 'DXGlobalWorkflow'
        }.get(class_name)
    if class_name not in all_bindings:
        raise DXError("Invalid class name: %s", class_name)
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
    https://documentation.dnanexus.com/developer/api/system-methods#api-method-system-describedataobjects
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
