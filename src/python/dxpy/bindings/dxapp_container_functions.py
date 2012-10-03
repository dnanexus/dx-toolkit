'''
Accessing App-Specific Containers
+++++++++++++++++++++++++++++++++

Apps have associated resource containers and project cache containers.
To easily access these, the following utility functions are provided.
These functions are meant to be called only by a job.
'''

import dxpy
from dxpy.bindings import *
import os

def load_app_resource(**kwargs):
    '''
    :param kwargs: keyword args for :func:`~dxpy.bindings.search.find_one_data_object`, with the exception of "project"
    :raises: :exc:`~dxpy.exceptions.DXError` if "project" is given, if this is called with dxpy.JOB_ID not set, or if "DX_RESOURCES_ID" is not found in the environment variables
    :returns: None if no matching object is found; otherwise returns a dxpy object handler for that class of object

    Searches for a data object in the project cache container matching
    the given keyword arguments.  If found, the object will be cloned
    into the running job's workspace container, and the handler for it
    will be returned.
    '''

    if 'project' in kwargs:
        raise DXError('Unexpected kwarg: "project"')
    if dxpy.JOB_ID is None:
        raise DXError('Not called by a job')
    if 'DX_RESOURCES_ID' not in os.environ:
        raise DXError('App resources container ID could not be found in the environment variable DX_RESOURCES_ID')

    kwargs['project'] = os.environ.get('DX_RESOURCES_ID')
    kwargs['get_handler'] = True

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
    '''

    if 'project' in kwargs:
        raise DXError('Unexpected kwarg: "project"')
    if dxpy.JOB_ID is None:
        raise DXError('Not called by a job')
    if 'DX_PROJECT_CACHE_ID' not in os.environ:
        raise DXError('Project cache ID could not be found in the environment variable DX_PROJECT_CACHE_ID')

    kwargs['project'] = os.environ.get('DX_PROJECT_CACHE_ID')
    kwargs['get_handler'] = True

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
    '''

    if dxpy.JOB_ID is None:
        raise DXError('Not called by a job')
    if 'DX_PROJECT_CACHE_ID' not in os.environ:
        raise DXError('Project cache ID could not be found in the environment variable DX_PROJECT_CACHE_ID')

    dxobject.clone(os.environ.get('DX_PROJECT_CACHE_ID'))
