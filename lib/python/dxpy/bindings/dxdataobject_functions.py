'''
Helper Functions
****************

These functions provide shorthand functional interfaces for manipulating DNAnexus data object references.
'''

from dxpy.bindings import *

def dxlink(object_id, project_id=None):
    '''
    :param object_id: String containing an object ID or the object handler itself
    :param project_id: String containing a project ID, if creating a cross-project DXLink

    Creates a DXLink (a dict formatted as a symbolic DNAnexus object reference).
    '''
    if isinstance(object_id, DXDataObject):
        object_id = object_id.get_id()
    if project_id is None:
        return {'$dnanexus_link': object_id}
    else:
        return {'$dnanexus_link': {'project': project_id, 'id': object_id}}

def is_dxlink(x):
    return isinstance(x, dict) and '$dnanexus_link' in x

def get_dxlink_ids(link):
    if isinstance(link['$dnanexus_link'], dict):
        return link['$dnanexus_link']['id'], link['$dnanexus_link']['project']
    else:
        return link['$dnanexus_link'], None

def _guess_link_target_type(link):
    if is_dxlink(link):
        link = link['$dnanexus_link']
    class_name, _id = link.split("-")
    class_name = 'DX'+class_name.capitalize()
    if class_name == 'DXGtable':
        class_name = 'DXGTable'
    cls = dxpy.__dict__[class_name]
    return cls

def get_handler(link):
    '''
    :param link: String containing an object ID or dict containing a DXLink
    Parses a string or DXLink dict. Creates and returns an object handler for it.

    Example::
    
        get_handler("gtable-1234").get_col_names()
    '''
    try:
        cls = _guess_link_target_type(link)
        return cls(link)
    except Exception as e:
        raise DXError("Could not parse link "+str(link))

def describe(link, **kwargs):
    '''
    :param link: String containing an object ID or dict containing a DXLink
    Given an object ID, recognizes the class and calls describe on it.

    Example::

        describe("file-1234")
    '''
    handler = get_handler(link)
    return handler.describe(**kwargs)

def get_details(link, **kwargs):
    '''
    :param link: String containing an object ID or dict containing a DXLink
    Given an object ID, recognizes the class and calls getDetails on it.

    Example::

        get_details("file-1234")
    '''
    handler = get_handler(link)
    return handler.get_details(**kwargs)

def remove(link, **kwargs):
    '''
    :param link: String containing an object ID or dict containing a DXLink
    Given an object ID, recognizes the class and calls remove on it.

    Example::

        remove("file-1234")
    '''
    handler = get_handler(link)
    return handler.remove(**kwargs)
