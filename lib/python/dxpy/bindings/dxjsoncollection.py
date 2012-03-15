"""
TODO: Write something here
"""

from dxpy.bindings import *

##########
# DXJSON #
##########

def new_dxjson(to_json):
    '''
    :param to_json: Pythonized JSON object to store
    :type to_json: dict or list
    :rtype: :class:`dxpy.bindings.DXJSON`

    Creates a new remote JSON object with contents set to *to_json*
    and returns the appropriate handler.

    Note that this function is shorthand for::

        dxjson = DXJSON()
        dxjson.new(to_json)

    '''
    dxjson = DXJSON()
    dxjson.new(to_json)
    return dxjson

class DXJSON(DXClass):
    '''Remote JSON object handler'''

    _class = "json"

    _describe = staticmethod(dxpy.api.jsonDescribe)
    _get_properties = staticmethod(dxpy.api.jsonGetProperties)
    _set_properties = staticmethod(dxpy.api.jsonSetProperties)
    _add_types = staticmethod(dxpy.api.jsonAddTypes)
    _remove_types = staticmethod(dxpy.api.jsonRemoveTypes)
    _destroy = staticmethod(dxpy.api.jsonDestroy)

    def new(self, to_json):
        """
        :param to_json: Pythonized JSON object to store
        :type to_json: dict or list

        Create a new remote JSON object and store *to_json* in it.

        """
        resp = dxpy.api.jsonNew(to_json)
        self.set_id(resp["id"])

    def get(self):
        """
        :returns: Pythonized JSON object stored in the associated JSON object
        :rtype: dict or list

        Returns the contents of the remote JSON object.

        """
        return dxpy.api.jsonGet(self._dxid)

    def set(self, to_json):
        """
        :param to_json: Pythonized JSON object to store
        :type to_json: dict or list

        Stores *to_json* in the associated remote JSON object

        """
        dxpy.api.jsonSet(self._dxid, to_json)

################
# DXCollection #
################

def new_dxcollection(collection):
    '''
    :param collection: Pythonized JSON object to be stored in the collection
    :type collection: dict or list
    :rtype: :class:`dxpy.bindings.DXCollection`

    Creates a new remote collection and stores the JSON of
    *collection*, assuming it is a valid collection.

    Note that this function is shorthand for::

        dxcollection = DXCollection()
        dxcollection.new(collection)

    '''

    coll = DXCollection()
    coll.new(collection)
    return coll

class DXCollection(DXClass):
    '''Remote collection object handler'''

    _class = "collection"

    _describe = staticmethod(dxpy.api.collectionDescribe)
    _get_properties = staticmethod(dxpy.api.collectionGetProperties)
    _set_properties = staticmethod(dxpy.api.collectionSetProperties)
    _add_types = staticmethod(dxpy.api.collectionAddTypes)
    _remove_types = staticmethod(dxpy.api.collectionRemoveTypes)
    _destroy = staticmethod(dxpy.api.collectionDestroy)

    def new(self, collection):
        """
        :param collection: Pythonized JSON object to be stored in the collection
        :type collection: dict or list

        Creates a new remote collection and stores the JSON of
        *collection*, assuming it is a valid collection.

        """
        resp = dxpy.api.collectionNew(collection)
        self.set_id(resp["id"])

    def get(self):
        """
        :returns: Contents of remote collection
        :rtype: dict or list

        Returns the contents of the stored collection

        """
        raise NotImplementedError()

    def set(self, collection):
        """
        :param collection: Pythonized JSON object to be stored in the collection
        :type collection: dict or list

        Overwrites the current remote collection's contents with the
        given parameter *collection*.

        """

        raise NotImplementedError()
