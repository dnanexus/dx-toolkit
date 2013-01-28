#!/usr/bin/env python
#
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

import dxpy.bindings as dxpy

def main(json_dxid, job_id_json):
    dxjob = dxpy.new_dxjob({"json_dxid": json_dxid}, "for_job_to_run")
    dxjobid = dxpy.DXJSON(job_id_json)
    dxjobid.set({"jobid": dxjob.get_id()})

def for_job_to_run(json_dxid):
    json = dxpy.DXJSON(json_dxid)
    json.set({"jobsuccess": True})
