#!/usr/bin/env python

import dxpy.bindings as dxpy

def main(json_dxid, job_id_json):
    dxjob = dxpy.new_dxjob({"json_dxid": json_dxid}, "for_job_to_run")
    dxjobid = dxpy.DXJSON(job_id_json)
    dxjobid.set({"jobid": dxjob.get_id()})

def for_job_to_run(json_dxid):
    json = dxpy.DXJSON(json_dxid)
    json.set({"jobsuccess": True})
