# Copyright (C) 2013-2014 DNAnexus, Inc.
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

from __future__ import print_function

import os, json, collections, logging, argparse, string
from functools import wraps
import dxpy
from ..compat import USING_PYTHON2

ENTRY_POINT_TABLE = {}

RUN_COUNT = 0

# Locale-independent version of string.printable
ASCII_PRINTABLE = string.ascii_letters + string.digits + string.punctuation + string.whitespace
def _safe_unicode(o):
    """
    Returns an equivalent unicode object, trying harder to avoid
    dependencies on the Python default encoding.
    """
    def clean(s):
        return u''.join([c if c in ASCII_PRINTABLE else '?' for c in s])
    if USING_PYTHON2:
        try:
            return unicode(o)
        except:
            try:
                s = str(o)
                try:
                    return s.decode("utf-8")
                except:
                    return clean(s[:2048]) + u" [Raw error message: " + unicode(s.encode("hex"), 'utf-8') + u"]"
            except:
                return u"(Unable to decode Python exception message)"
    else:
        return str(o)

def _format_exception_message(e):
    """
    Formats the specified exception.
    """
    # Prevent duplication of "AppError" in places that print "AppError"
    # and then this formatted string
    if isinstance(e, dxpy.AppError):
        return _safe_unicode(e)
    if USING_PYTHON2:
        return unicode(e.__class__.__name__, 'utf-8') + ": " + _safe_unicode(e)
    else:
        return e.__class__.__name__ + ": " + _safe_unicode(e)

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

    If the environment variable *DX_JOB_ID* is not set, the function
    name may be given in *function_name*; if not set, it is set by the
    environment variable *DX_TEST_FUNCTION*. The function input may be
    given in *function_input*; if not set, it is set by the local file
    *job_input.json* which is expected to be present.

    The absence of *DX_JOB_ID* signals to run() that execution is happening in the debug harness. In this mode of
    operation, all calls to *dxpy.bindings.DXJob.new* (and higher level handler methods which use it) are intercepted, and run()
    is invoked instead with appropriate inputs. The initial invocation of *dxpy.run()* (with no arguments) need not be
    changed; instead, use a local file *job_input.json*.

    With this, no program code requires changing between the two modes.
    '''

    global RUN_COUNT
    RUN_COUNT += 1

    dx_working_dir = os.getcwd()

    if dxpy.JOB_ID is not None:
        logging.basicConfig()

        try:
            logging.getLogger().addHandler(dxpy.DXLogHandler())
        except dxpy.exceptions.DXError:
            print("TODO: FIXME: the EE client should die if logging is not available")

        job = dxpy.describe(dxpy.JOB_ID)
    else:
        if function_name is None:
            function_name = os.environ.get('DX_TEST_FUNCTION', 'main')
        if function_input is None:
            with open("job_input.json", "r") as fh:
                function_input = json.load(fh)

        job = {'function': function_name, 'input': function_input}

    with open("job_error_reserved_space", "w") as fh:
        fh.write("This file contains reserved space for writing job errors in case the filesystem becomes full.\n" + " "*1024*64)

    print("Invoking", job.get('function'), "with", job.get('input'))

    try:
        result = ENTRY_POINT_TABLE[job['function']](**job['input'])
    except dxpy.AppError as e:
        if dxpy.JOB_ID is not None:
            os.chdir(dx_working_dir)
            with open("job_error.json", "w") as fh:
                fh.write(json.dumps({"error": {"type": "AppError", "message": _format_exception_message(e)}}) + "\n")
        raise
    except Exception as e:
        if dxpy.JOB_ID is not None:
            os.chdir(dx_working_dir)
            try:
                os.unlink("job_error_reserved_space")
            except:
                pass
            with open("job_error.json", "w") as fh:
                fh.write(json.dumps({"error": {"type": "AppInternalError", "message": _format_exception_message(e)}}) + "\n")
        raise

    if result is not None:
        # TODO: protect against client removing its original working directory
        os.chdir(dx_working_dir)
        with open("job_output.json", "w") as fh:
            fh.write(json.dumps(result, indent=2, cls=DXJSONEncoder))
            fh.write("\n")

    return result

# TODO: make this less naive with respect to cycles and any other things json.dumps() can handle
def convert_handlers_to_dxlinks(x):
    if isinstance(x, dxpy.DXObject):
        x = dxpy.dxlink(x)
    elif isinstance(x, collections.Mapping):
        for key, value in x.items():
            x[key] = convert_handlers_to_dxlinks(value)
    elif isinstance(x, list):
        for i in range(len(x)):
            x[i] = convert_handlers_to_dxlinks(x[i])
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
    for i, value in vars(parser.parse_args(args)).items():
        if value is None:
            continue
        if i in json_inputs:
            try:
                inputs[i] = json.loads(value)
            except ValueError:
                from dxpy.utils.resolver import resolve_existing_path
                project, path, results = resolve_existing_path(value, ask_to_resolve=False, describe={'id': True}, allow_mult=False)
                print(project, path, results)
                if results is None or len(results) != 1:
                    raise ValueError("Value {v} could not be resolved".format(v=value))
                inputs[i] = dxpy.dxlink(results[0]['id'], project_id=project)
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
            return f(*args, **kwargs)
        return wrapped_f
    return wrap

class DXJSONEncoder(json.JSONEncoder):
    ''' Like json.JSONEncoder, but converts DXObject objects into dxlinks.
    '''
    def default(self, obj):
        if isinstance(obj, dxpy.DXObject):
            return dxpy.dxlink(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)
