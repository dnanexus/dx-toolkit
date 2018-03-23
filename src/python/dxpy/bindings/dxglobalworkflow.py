# Copyright (C) 2013-2018 DNAnexus, Inc.
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
DXGlobalWorkflow Handler
++++++++++++++++++++++++

"""

from __future__ import print_function, unicode_literals, division, absolute_import

import dxpy
from . import DXObject, DXExecutable, DXJob, verify_string_dxid
from ..exceptions import DXError
from ..compat import basestring

####################
# DXGlobalWorkflow #
####################

class DXGlobalWorkflow(DXObject, DXExecutable):
    '''
    Remote global workflow object handler.
    '''

    _class = "globalworkflow"

    def __init__(self, dxid=None, name=None, alias=None):
        DXObject.__init__(self)
        if dxid is not None or name is not None:
            self.set_id(dxid=dxid, name=name, alias=alias)

    def set_id(self, dxid=None, name=None, alias=None):
        '''
        :param dxid: Global workflow ID
        :type dxid: string
        :param name: Global workflow name
        :type name: string
        :param alias: Global workflow version or tag
        :type alias: string
        :raises: :exc:`~dxpy.exceptions.DXError` if *dxid* and some other input are both given or if neither *dxid* nor *name* are given

        Discards the currently stored ID and associates the handler
        with the requested parameters.  Note that if *dxid* is given,
        the other fields should not be given, and if *name* is given,
        *alias* has default value "default".

        '''
        self._dxid = None
        self._name = None
        self._alias = None
        if dxid is not None:
            if name is not None or alias is not None:
                raise DXError("Did not expect name or alias to be given if dxid is given")
            verify_string_dxid(dxid, self._class)
            self._dxid = dxid
        elif name is not None:
            self._name = name
            if not isinstance(name, basestring):
                raise DXError("Global workflow name needs to be a string: %r" % (name,))
            if alias is not None:
                if not isinstance(alias, basestring):
                    raise DXError("Global workflow alias needs to be a string: %r" % (alias,))
                self._alias = alias
            else:
                self._alias = 'default'

    def get_id(self):
        '''
        :returns: Object ID of associated global workflow
        :rtype: string

        Returns the object ID of the global workflow that the handler is currently
        associated with.
        '''
        if self._dxid is not None:
            return self._dxid
        else:
            return 'globalworkflow-' + self._name + '/' + self._alias

    def new(self, **kwargs):
        '''

        .. note:: It is highly recommended that `dx build --create-global-workflow
           <https://wiki.dnanexus.com/Command-Line-Client/Index-of-dx-Commands#build>`_
           be used instead for global workflow creation.

        Creates a workflow in the global space with the given parameters by using
        the specified workflow(s) in regionalOptions as a base.

        The workflow is available only to its developers until
        :meth:`publish()` is called, and is not run until :meth:`run()`
        is called.

        '''
        dx_hash = {}

        for field in ['version', 'name', 'regionalOptions']:
            if field not in kwargs:
                raise DXError("%s: Keyword argument %s is required" % (self.__class__.__name__, field))
            dx_hash[field] = kwargs[field]
            del kwargs[field]

        resp = dxpy.api.global_workflow_new(dx_hash, **kwargs)
        self.set_id(dxid=resp["id"])

    def describe(self, fields=None, **kwargs):
        '''
        :param fields: Hash where the keys are field names that should be returned, and values should be set to True (default is that all fields are returned)
        :type fields: dict
        :returns: Description of the remote global workflow object
        :rtype: dict

        Returns a dict with a description of the workflow.

        '''
        describe_input = {}
        if fields:
            describe_input['fields'] = fields

        if self._dxid is not None:
            self._desc = dxpy.api.global_workflow_describe(self._dxid, input_params=describe_input, **kwargs)
        else:
            self._desc = dxpy.api.global_workflow_describe('globalworkflow-' + self._name, alias=self._alias,
                                                           input_params=describe_input, **kwargs)

        return self._desc
