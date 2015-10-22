# Copyright (C) 2013-2015 DNAnexus, Inc.
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
DXAnalysis Handler
++++++++++++++++++

Analyses are DNAnexus entities that capture an instantiation of a
running workflow. They can be created from
:func:`dxpy.bindings.dxworkflow.DXWorkflow.run` or from an
existing analysis ID.

"""

from __future__ import (print_function, unicode_literals)

import time
import dxpy
from dxpy.bindings import (DXObject, )
from dxpy.exceptions import DXJobFailureError

##############
# DXAnalysis #
##############

class DXAnalysis(DXObject):
    '''
    Remote analysis object handler.
    '''

    _class = "analysis"

    def __init__(self, dxid=None):
        self._test_harness_result = None
        DXObject.__init__(self, dxid=dxid)
        self.set_id(dxid)

    def describe(self, fields=None, **kwargs):
        """
        :param fields: dict where the keys are field names that should
            be returned, and values should be set to True (by default,
            all fields are returned)
        :type fields: dict
        :returns: Description of the analysis
        :rtype: dict

        Returns a hash with key-value pairs containing information
        about the analysis

        """
        describe_input = {}
        if fields is not None:
            describe_input['fields'] = fields
        self._desc = dxpy.api.analysis_describe(self._dxid, describe_input, **kwargs)
        return self._desc

    def add_tags(self, tags, **kwargs):
        """
        :param tags: Tags to add to the analysis
        :type tags: list of strings

        Adds each of the specified tags to the analysis. Takes no
        action for tags that are already listed for the analysis.

        """

        dxpy.api.analysis_add_tags(self._dxid, {"tags": tags}, **kwargs)

    def remove_tags(self, tags, **kwargs):
        """
        :param tags: Tags to remove from the analysis
        :type tags: list of strings

        Removes each of the specified tags from the analysis. Takes
        no action for tags that the analysis does not currently have.

        """

        dxpy.api.analysis_remove_tags(self._dxid, {"tags": tags}, **kwargs)

    def set_properties(self, properties, **kwargs):
        """
        :param properties: Property names and values given as key-value pairs of strings
        :type properties: dict

        Given key-value pairs in *properties* for property names and
        values, the properties are set on the analysis for the given
        property names. Any property with a value of :const:`None`
        indicates the property will be deleted.

        .. note:: Any existing properties not mentioned in *properties*
           are not modified by this method.

        """

        dxpy.api.analysis_set_properties(self._dxid, {"properties": properties}, **kwargs)

    def wait_on_done(self, interval=2, timeout=3600*24*7, **kwargs):
        '''
        :param interval: Number of seconds between queries to the analysis's state
        :type interval: integer
        :param timeout: Maximum amount of time to wait, in seconds, until the analysis is done (or at least partially failed)
        :type timeout: integer
        :raises: :exc:`~dxpy.exceptions.DXError` if the timeout is reached before the analysis has finished running, or :exc:`~dxpy.exceptions.DXJobFailureError` if some job in the analysis has failed

        Waits until the analysis has finished running.
        '''

        elapsed = 0
        while True:
            state = self._get_state(**kwargs)
            if state == "done":
                break
            if state in ["failed", "partially_failed"]:
                desc = self.describe(**kwargs)
                err_msg = "Analysis has failed because of {failureReason}: {failureMessage}".format(**desc)
                if desc.get("failureFrom") != None and desc["failureFrom"]["id"] != desc["id"]:
                    err_msg += " (failure from {id})".format(id=desc['failureFrom']['id'])
                raise DXJobFailureError(err_msg)
            if state == "terminated":
                raise DXJobFailureError("Analysis was terminated.")

            if elapsed >= timeout or elapsed < 0:
                raise DXJobFailureError("Reached timeout while waiting for the analysis to finish")

            time.sleep(interval)
            elapsed += interval

    def terminate(self, **kwargs):
        '''
        Terminates the associated analysis.
        '''
        dxpy.api.analysis_terminate(self._dxid, **kwargs)

    def get_output_ref(self, field, index=None, metadata=None):
        '''
        :param field: Output field name of this analysis
        :type field: string
        :param index: If the referenced field is an array, optionally specify an index (starting from 0) to indicate a particular member of the array
        :type index: int
        :param metadata: If the referenced field is of a data object class, a string indicating the metadata that should be read, e.g. "name", "properties.propkey", "details.refgenome"
        :type metadata: string

        Returns a dict containing a valid reference to an output of this analysis.
        '''

        link = {"$dnanexus_link": {"analysis": self._dxid, "field": field}}
        if index is not None:
            link["$dnanexus_link"]["index"] = index
        if metadata is not None:
            link["$dnanexus_link"]["metadata"] = metadata
        return link

    def _get_state(self, **kwargs):
        '''
        :returns: State of the remote object
        :rtype: string

        Queries the API server for the analysis's state.

        Note that this function is shorthand for:

            dxanalysis.describe(**kwargs)["state"]

        '''

        return self.describe(fields=dict(state=True), **kwargs)["state"]
