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

"""
DXAnalysis Handler
++++++++++++++++++

Analyses are DNAnexus entities that capture an instantiation of a
running workflow. They can be created from
:func:`dxpy.bindings.dxworkflow.DXAnalysisWorkflow.run` or from an
existing analysis ID.

"""

import sys, time
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

    def set_id(self, dxid):
        '''
        :param dxid: Object ID
        :type dxid: string

        Discards the currently stored ID and associates the handler
        with *dxid*.
        '''
        self._dxid = dxid

    def get_id(self):
        '''
        :returns: Analysis ID of associated analysis
        :rtype: string

        Returns the analysis ID that the handler is currently
        associated with.

        '''

        return self._dxid

    def describe(self, **kwargs):
        """
        :returns: Description of the analysis
        :rtype: dict

        Returns a hash with key-value pairs containing information
        about the analysis

        """
        self._desc = dxpy.api.analysis_describe(self._dxid, {}, **kwargs)
        return self._desc

    def wait_on_done(self, interval=2, timeout=sys.maxint, **kwargs):
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
                err_msg = "Job has failed because of {failureReason}: {failureMessage}".format(**desc)
                if desc.get("failureFrom") != None and desc["failureFrom"]["id"] != desc["id"]:
                    err_msg += " (failure from {id})".format(id=desc['failureFrom']['id'])
                raise DXJobFailureError(err_msg)
            if state == "terminated":
                raise DXJobFailureError("Job was terminated.")

            if elapsed >= timeout or elapsed < 0:
                raise DXJobFailureError("Reached timeout while waiting for the analysis to finish")

            time.sleep(interval)
            elapsed += interval

    def terminate(self, **kwargs):
        '''
        Terminates the associated analysis.
        '''
        dxpy.api.analysis_terminate(self._dxid, **kwargs)

    def get_output_ref(self, field):
        '''
        :param field: Output field name of this analysis
        :type field: string

        Returns a dict containing a valid job-based object reference
        to refer to an output of this job.
        '''

        return {"$dnanexus_link": {"analysis": self._dxid, "field": field}}

    def _get_state(self, **kwargs):
        '''
        :returns: State of the remote object
        :rtype: string

        Queries the API server for the analysis's state.

        Note that this function is shorthand for:

            dxanalysis.describe(**kwargs)["state"]

        '''

        return self.describe(**kwargs)["state"]
