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
Accessing App-Specific Containers
+++++++++++++++++++++++++++++++++

Apps have associated resource containers and project cache containers.
To easily access these, the following utility functions are provided.
These functions are meant to be called only by a job.
'''

from __future__ import print_function, unicode_literals, division, absolute_import

import os

import dxpy
from ..exceptions import DXError
from .search import find_one_data_object

def load_app_resource(**kwargs):
    '''
    :param kwargs: keyword args for :func:`~dxpy.bindings.search.find_one_data_object`, with the exception of "project"
    :raises: :exc:`~dxpy.exceptions.DXError` if "project" is given, if this is called with dxpy.JOB_ID not set, or if "DX_RESOURCES_ID" or "DX_PROJECT_CONTEXT_ID" is not found in the environment variables
    :returns: None if no matching object is found; otherwise returns a dxpy object handler for that class of object

    Searches for a data object in the app resources container matching the given keyword arguments.  If found, the
    object will be cloned into the running job's workspace container, and the handler for it will be returned. If the
    app resources container ID is not found in DX_RESOURCES_ID, falls back to looking in the current project.

    Example::

        @dxpy.entry_point('main')
        def main(*args, **kwargs):
            x = load_app_resource(name="Indexed genome", classname='file')
            dxpy.download_dxfile(x)
    '''

    if 'project' in kwargs:
        raise DXError('Unexpected kwarg: "project"')
    if dxpy.JOB_ID is None:
        raise DXError('Not called by a job')
    if 'DX_RESOURCES_ID' not in os.environ and 'DX_PROJECT_CONTEXT_ID' not in os.environ:
        raise DXError('App resources container ID could not be found')

    kwargs['project'] = os.environ.get('DX_RESOURCES_ID', os.environ.get('DX_PROJECT_CONTEXT_ID'))
    kwargs['return_handler'] = True

    return find_one_data_object(**kwargs)

def load_from_cache(**kwargs):
    '''
    :param kwargs: keyword args for :func:`~dxpy.bindings.search.find_one_data_object`, with the exception of "project"
    :raises: :exc:`~dxpy.exceptions.DXError` if "project" is given, if this is called with dxpy.JOB_ID not set, or if "DX_PROJECT_CACHE_ID" is not found in the environment variables
    :returns: None if no matching object is found; otherwise returns a dxpy object handler for that class of object

    Searches for a data object in the project cache container matching
    the given keyword arguments.  If found, the object will be cloned
    into the running job's workspace container, and the handler for it
    will be returned.

    Example::

        @dxpy.entry_point('main')
        def main(*args, **kwargs):
            x = load_from_cache(name="Indexed genome", classname='file')
            if x is None:
                x = compute_result(*args)
                save_to_cache(x)
    '''

    if 'project' in kwargs:
        raise DXError('Unexpected kwarg: "project"')
    if dxpy.JOB_ID is None:
        raise DXError('Not called by a job')
    if 'DX_PROJECT_CACHE_ID' not in os.environ:
        raise DXError('Project cache ID could not be found in the environment variable DX_PROJECT_CACHE_ID')

    kwargs['project'] = os.environ.get('DX_PROJECT_CACHE_ID')
    kwargs['return_handler'] = True

    cached_object = find_one_data_object(**kwargs)

    if cached_object is None:
        return None

    return cached_object.clone(dxpy.WORKSPACE_ID)

# Maybe this should be a member function of a data object?
def save_to_cache(dxobject):
    '''
    :param dxobject: a dxpy object handler for an object to save to the cache
    :raises: :exc:`~dxpy.exceptions.DXError` if this is called with dxpy.JOB_ID not set, or if "DX_PROJECT_CACHE_ID" is not found in the environment variables

    Clones the given object to the project cache.

    Example::

        @dxpy.entry_point('main')
        def main(*args, **kwargs):
            x = load_from_cache(name="Indexed genome", classname='file')
            if x is None:
                x = compute_result(*args)
                save_to_cache(x)
    '''

    if dxpy.JOB_ID is None:
        raise DXError('Not called by a job')
    if 'DX_PROJECT_CACHE_ID' not in os.environ:
        raise DXError('Project cache ID could not be found in the environment variable DX_PROJECT_CACHE_ID')

    dxobject.clone(os.environ.get('DX_PROJECT_CACHE_ID'))
