'''
Functions and classes used when launching platform executables from the CLI.
'''

# TODO: refactor all dx run helper functions here

import os, sys, json, collections

def stage_to_job_refs(x, launched_jobs):
    ''' Used by run() to parse stage inputs bound to other stages when executing a workflow '''
    if isinstance(x, collections.Mapping):
        if "connectedTo" in x:
            return {'job': launched_jobs[x['connectedTo']['stage']].get_id(), "field": x['connectedTo']['output']}
        for key, value in x.iteritems():
            x[key] = stage_to_job_refs(value, launched_jobs)
    elif isinstance(x, list):
        for i in range(len(x)):
            x[i] = stage_to_job_refs(x[i], launched_jobs)
    return x
