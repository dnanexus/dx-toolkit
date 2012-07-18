'''
Utilities used in the DNAnexus execution environment and test harness.
'''

import os, sys, json, collections, logging
from functools import wraps
import dxpy

ENTRY_POINT_TABLE = {}

def run(function_name=None, function_input=None):
    '''
    Triggers the execution environment entry point processor.

    Use this function in the program entry point code:

    import dxpy

    @dxpy.entry_point('main')
    def hello(i):
        pass

    dxpy.run()

    If the environment variable *DX_JOB_ID* is set, the processor retrieves the job with that ID from the API server.
    The job's *job.function* field is used to invoke the entry point function in the module from which run() has been
    called. The function name is looked up in the table of all functions decorated with *@dxpy.entry_point('name')*.
    This is the mode of operation used in the DNAnexus execution environment.
    WARNING: the parameters *function* and *input* are disregarded in this mode of operation.

    If the environment variable *DX_JOB_ID* is not set, the function name may be given in *function*; if not set, it is
    assumed to be *main*. The function input may be given in *input*; if not set, it is set by parsing JSON from the
    environment variable *DX_JOB_INPUT*; if that is not set, no input is given to the function.

    The absence of *DX_JOB_ID* signals to run() that execution is happening in the debug harness. In this mode of
    operation, all calls to *dxpy.api.jobNew* (and higher level handler methods which use it) are intercepted, and run()
    is invoked instead with appropriate inputs. The initial invocation of *dxpy.run()* (with no arguments) need not be
    changed; instead, use the environment variable *DX_JOB_INPUT*. Thus, no program code requires changing between the
    two modes.
    '''
    if 'DX_JOB_ID' in os.environ:
        logging.basicConfig()

        try:
            logging.getLogger().addHandler(dxpy.DXLogHandler())
        except dxpy.exceptions.DXError:
            print "TODO: FIXME: the EE client should die if logging is not available"

        dx_working_dir = os.getcwd()

        job = dxpy.describe(os.environ['DX_JOB_ID'])
    else:
        if function_name is None:
            function_name = 'main'
        if function_input is None:
            function_input = json.loads(os.environ.get('DX_JOB_INPUT', '{}'))
        job = {'function': function_name, 'input': function_input}
    print "Invoking", job.get('function'), "with", job.get('input')

    try:
        result = ENTRY_POINT_TABLE[job['function']](**job['input'])
    except dxpy.ProgramError as e:
        if 'DX_JOB_ID' in os.environ:
            os.chdir(dx_working_dir)           
            with open("job_error.json", "w") as fh:
                fh.write(json.dumps({"error": {"type": "ProgramError", "message": str(e)}}) + "\n")
        raise

    if 'DX_JOB_ID' in os.environ:
        # TODO: protect against client removing its original working directory
        os.chdir(dx_working_dir)
        with open("job_output.json", "w") as fh:
            fh.write(json.dumps(result) + "\n")
    else:
        result = resolve_job_refs_in_test(result)

    return result

def resolve_job_refs_in_test(x):
    if isinstance(x, collections.Mapping):
        if "job" in x and "field" in x:
            job_result = dxpy.bindings.dxjob._test_harness_jobs[x["job"]]._test_harness_result
            return job_result[x["field"]]
        for key, value in x.iteritems():
            x[key] = resolve_job_refs_in_test(value)
    elif isinstance(x, list):
        for i in range(len(x)):
            x[i] = resolve_job_refs_in_test(x[i])
    return x

def entry_point(entry_point_name):
    '''
    Use this to decorate a DNAnexus execution environment entry point. Example:

    @dxpy.entry_point('main')
    def hello(i):
        pass

    '''
    def wrap(f):
        ENTRY_POINT_TABLE[entry_point_name] = f
        @wraps(f)
        def wrapped_f(*args, **kwargs):
            f(*args, **kwargs)
        return wrapped_f
    return wrap
