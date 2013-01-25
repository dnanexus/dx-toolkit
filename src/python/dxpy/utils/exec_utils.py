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

'''
Utilities used in the DNAnexus execution environment and test harness.
'''

import os, json, collections, logging, argparse
from functools import wraps
import dxpy

ENTRY_POINT_TABLE = {}

RUN_COUNT = 0

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
    WARNING: The parameters *function_name* and *function_input* are disregarded in this mode of operation.

    If the environment variable *DX_JOB_ID* is not set, the function name may be given in *function_name*; if not set,
    it is assumed to be *main*. The function input may be given in *function_input*; if not set, it is set by parsing
    JSON from the environment variable *DX_JOB_INPUT*; if that is not set, no input is given to the function.

    The absence of *DX_JOB_ID* signals to run() that execution is happening in the debug harness. In this mode of
    operation, all calls to *dxpy.api.jobNew* (and higher level handler methods which use it) are intercepted, and run()
    is invoked instead with appropriate inputs. The initial invocation of *dxpy.run()* (with no arguments) need not be
    changed; instead, use the environment variable *DX_JOB_INPUT* and/or command line arguments:

        script_name --spec=path/to/dxapp.spec --input1=value1 --input2=value2 ...

    With this, no program code requires changing between the two modes.
    '''
    global RUN_COUNT
    RUN_COUNT += 1

    if dxpy.JOB_ID is not None:
        logging.basicConfig()

        try:
            logging.getLogger().addHandler(dxpy.DXLogHandler())
        except dxpy.exceptions.DXError:
            print "TODO: FIXME: the EE client should die if logging is not available"

        dx_working_dir = os.getcwd()

        job = dxpy.describe(dxpy.JOB_ID)
    else:
        if function_name is None:
            function_name = 'main'
        if function_input is None:
            function_input = json.loads(os.environ.get('DX_JOB_INPUT', '{}'))

            # Try to parse args from the command line
            args, remaining_args = None, None
            try:
                parser = argparse.ArgumentParser()
                parser.add_argument("-s", "--spec", help="Path to app metadata definition file (dxapp.json)")
                args, remaining_args = parser.parse_known_args()
            except:
                pass

            if args is not None and args.spec is not None:
                function_input.update(parse_args_as_job_input(args=remaining_args, app_spec=json.load(open(args.spec))))

        job = {'function': function_name, 'input': function_input}
    job['input'] = resolve_job_refs_in_test(job['input'])
    print "Invoking", job.get('function'), "with", job.get('input')

    try:
        result = ENTRY_POINT_TABLE[job['function']](**job['input'])
    except dxpy.AppError as e:
        if dxpy.JOB_ID is not None:
            os.chdir(dx_working_dir)
            with open("job_error.json", "w") as fh:
                fh.write(json.dumps({"error": {"type": "AppError", "message": unicode(e)}}) + "\n")
        raise
    except Exception as e:
        if dxpy.JOB_ID is not None:
            os.chdir(dx_working_dir)
            with open("job_error.json", "w") as fh:
                fh.write(json.dumps({"error": {"type": "AppInternalError", "message": unicode(e)}}) + "\n")
        raise

    result = convert_handlers_to_dxlinks(result)

    if dxpy.JOB_ID is not None:
        if result is not None:
            # TODO: protect against client removing its original working directory
            os.chdir(dx_working_dir)
            with open("job_output.json", "w") as fh:
                fh.write(json.dumps(result) + "\n")
    else:
        result = resolve_job_refs_in_test(result)

    return result

# TODO: make this less naive with respect to cycles and any other things json.dumps() can handle
def convert_handlers_to_dxlinks(x):
    if isinstance(x, dxpy.DXObject):
        x = dxpy.dxlink(x)
    elif isinstance(x, collections.Mapping):
        for key, value in x.iteritems():
            x[key] = convert_handlers_to_dxlinks(value)
    elif isinstance(x, list):
        for i in range(len(x)):
            x[i] = convert_handlers_to_dxlinks(x[i])
    return x

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

def parse_args_as_job_input(args, app_spec):
    parser = argparse.ArgumentParser()
    json_inputs = set()
    for ispec in app_spec.get("inputSpec", []):
        kwargs = {}
        if ispec.get("type") == "int":
            kwargs["type"] = int
        elif ispec.get("type") == "float":
            kwargs["type"] = float
        elif ispec.get("type") == "boolean":
            kwargs["type"] = bool
        elif ispec.get("type") != "string":
            json_inputs.add(ispec["name"])

        if ispec.get("optional") != None:
            kwargs["required"] = not ispec["optional"]

        parser.add_argument("--" + ispec["name"], **kwargs)

    inputs = {}
    for i, value in vars(parser.parse_args(args)).iteritems():
        if value is None:
            continue
        if i in json_inputs:
            inputs[i] = json.loads(value)
        else:
            inputs[i] = value

    return inputs

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
