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

from __future__ import (print_function, unicode_literals)

import os, sys, json, subprocess, pipes
import collections, datetime

import dxpy
from dxpy.utils.describe import (get_field_from_jbor, get_job_from_jbor, get_index_from_jbor,
                                 is_job_ref, job_output_to_str, JOB_STATES)
from dxpy.utils.printing import (GREEN, BLUE, BOLD, ENDC, fill)
from dxpy.utils.resolver import is_localjob_id
from dxpy.compat import open, str, environ, USING_PYTHON2
from dxpy.utils import file_load_utils

def exit_with_error(msg):
    '''
    :param msg: string message to print before exiting

    Print the error message, as well as a blurb on where to find the
    job workspaces
    '''
    msg += '\n'
    msg += 'Local job workspaces can be found in: ' + str(environ.get('DX_TEST_JOB_HOMEDIRS'))
    sys.exit(msg)

def has_local_job_refs(io_hash):
    '''
    :param io_hash: input/output hash
    :type io_hash: dict
    :returns: boolean indicating whether any job-based object references are found in *io_hash*
    '''
    q = []

    for field in io_hash:
        if is_job_ref(io_hash[field]):
            if get_job_from_jbor(io_hash[field]).startswith('localjob'):
                return True
        elif isinstance(io_hash[field], list) or isinstance(io_hash[field], dict):
            q.append(io_hash[field])

    while len(q) > 0:
        thing = q.pop()
        if isinstance(thing, list):
            for i in range(len(thing)):
                if is_job_ref(thing[i]):
                    if get_job_from_jbor(thing[i]).startswith('localjob'):
                        return True
                elif isinstance(thing[i], list) or isinstance(thing[i], dict):
                    q.append(thing[i])
        else:
            for field in thing:
                if is_job_ref(thing[field]):
                    if get_job_from_jbor(thing[field]).startswith('localjob'):
                        return True
                elif isinstance(thing[field], list) or isinstance(thing[field], dict):
                    q.append(thing[field])

    return False

def resolve_job_ref(jbor, job_outputs={}, should_resolve=True):
    '''
    :param jbor: a dict that is a valid job-based object reference
    :type jbor: dict
    :param job_outputs: a dict of finished local jobs to their output hashes
    :type job_outputs: :class:`collections.OrderedDict`
    :returns: the referenced value if present
    :raises: :exc:`Exception` if the job-based object reference cannot be resolved

    TODO: Support metadata references
    '''
    ref_job_id = get_job_from_jbor(jbor)
    ref_job_field = get_field_from_jbor(jbor)
    ref_job_index = get_index_from_jbor(jbor)
    def resolve_from_hash(output_hash):
        if ref_job_index is None:
            return output_hash[ref_job_field]
        else:
            return output_hash[ref_job_field][ref_job_index]
    if is_localjob_id(ref_job_id):
        if job_outputs.get(ref_job_id) is None:
            if should_resolve:
                raise Exception('Job ' + ref_job_id + ' not found in local finished jobs')
            else:
                return jbor
        if ref_job_field not in job_outputs[ref_job_id]:
            raise Exception('Cannot resolve a JBOR with job ID ' + ref_job_id + ' because field "' + ref_job_field + '" was not found in its output')
        return resolve_from_hash(job_outputs[ref_job_id])
    else:
        dxjob = dxpy.DXJob(ref_job_id)
        try:
            dxjob.wait_on_done()
        except Exception as e:
            raise Exception('Could not wait for ' + ref_job_id + ' to finish: ' + str(e))
        job_desc = dxjob.describe()
        if ref_job_field not in job_desc['output']:
            raise Exception('Cannot resolve a JBOR with job ID ' + ref_job_id + ' because field "' + ref_job_field + '" was not found in its output')
        return resolve_from_hash(job_desc['output'])

def resolve_job_references(io_hash, job_outputs, should_resolve=True):
    '''
    :param io_hash: an input or output hash in which to resolve any job-based object references possible
    :type io_hash: dict
    :param job_outputs: a mapping of finished local jobs to their output hashes
    :type job_outputs: dict
    :param should_resolve: whether it is an error if a job-based object reference in *io_hash* cannot be resolved yet
    :type should_resolve: boolean

    Modifies *io_hash* in-place.
    '''
    q = []

    for field in io_hash:
        if is_job_ref(io_hash[field]):
            io_hash[field] = resolve_job_ref(io_hash[field], job_outputs, should_resolve)
        elif isinstance(io_hash[field], list) or isinstance(io_hash[field], dict):
            q.append(io_hash[field])

    while len(q) > 0:
        thing = q.pop()
        if isinstance(thing, list):
            for i in range(len(thing)):
                if is_job_ref(thing[i]):
                    thing[i] = resolve_job_ref(thing[i], job_outputs, should_resolve)
                elif isinstance(thing[i], list) or isinstance(thing[i], dict):
                    q.append(thing[i])
        else:
            for field in thing:
                if is_job_ref(thing[field]):
                    thing[field] = resolve_job_ref(thing[field], job_outputs, should_resolve)
                elif isinstance(thing[field], list) or isinstance(thing[field], dict):
                    q.append(thing[field])

def get_nonclosed_data_obj_link(thing):
    obj_id = None
    if isinstance(thing, dict) and '$dnanexus_link' in thing:
        if isinstance(thing['$dnanexus_link'], basestring):
            obj_id = thing['$dnanexus_link']
        elif isinstance(thing['$dnanexus_link'], dict):
            obj_id = thing['$dnanexus_link'].get('id')
    if obj_id is None:
        return None

    obj_desc = dxpy.describe(obj_id)
    if obj_desc.get('state') != 'closed':
        return obj_id

def get_implicit_depends_on(input_hash, depends_on):
    '''
    Add DNAnexus links to non-closed data objects in input_hash to depends_on
    '''
    q = []

    for field in input_hash:
        possible_dep = get_nonclosed_data_obj_link(input_hash[field])
        if possible_dep is not None:
            depends_on.append(possible_dep)
        elif isinstance(input_hash[field], list) or isinstance(input_hash[field], dict):
            q.append(input_hash[field])

    while len(q) > 0:
        thing = q.pop()
        if isinstance(thing, list):
            for i in range(len(thing)):
                possible_dep = get_nonclosed_data_obj_link(thing[i])
                if possible_dep is not None:
                    depends_on.append(possible_dep)
                elif isinstance(thing[i], list) or isinstance(thing[i], dict):
                    q.append(thing[i])
        else:
            for field in thing:
                possible_dep = get_nonclosed_data_obj_link(thing[field])
                if possible_dep is not None:
                    depends_on.append(possible_dep)
                elif isinstance(thing[field], list) or isinstance(thing[field], dict):
                    q.append(thing[field])

def wait_for_depends_on(depends_on, all_job_outputs):
    # Wait for depends_on and any data objects in the input to close
    if len(depends_on) > 0:
        print(fill('Processing dependsOn and any DNAnexus links to closing objects in the input'))
        for an_id in depends_on:
            try:
                print('  Waiting for ' + an_id + '...')
                if an_id.startswith('localjob'):
                    if all_job_outputs.get(an_id) is None:
                        raise Exception('Job ' + an_id + ' could not be found in local finished jobs')
                elif an_id.startswith('job'):
                    dxjob = dxpy.DXJob(an_id)
                    dxjob.wait_on_done()
                else:
                    handler = dxpy.get_handler(an_id)
                    desc = handler.describe()
                    handler._wait_on_close()
            except Exception as e:
                raise Exception('Could not wait for ' + an_id + ': ' + str(e))

def ensure_env_vars():
    for var in ['DX_FS_ROOT',
                'DX_TEST_CODE_PATH',
                'DX_TEST_JOB_HOMEDIRS']:
        if var not in environ:
            sys.exit('Error: Cannot run an entry point locally if the environment variable ' + var + ' has not been set')

def queue_entry_point(function, input_hash, depends_on=[], name=None):
    '''
    :param function: function to run
    :param input_hash: input to new job
    :param depends_on: list of data object IDs and/or job IDs (local or remote) to wait for before the job can be run
    :type depends_on: list of strings
    :param name: job name (optional)
    :returns: new local job ID

    This function should only be called by a locally running job, so
    all relevant DX_TEST_* environment variables should be set.

    This function will set up the home directory for the job, add an
    entry in job_outputs.json, and append the job information to the
    job_queue.json file.  (Both files found in
    $DX_TEST_JOB_HOMEDIRS.)
    '''
    ensure_env_vars()

    all_job_outputs_path = os.path.join(environ['DX_TEST_JOB_HOMEDIRS'], 'job_outputs.json')

    with open(all_job_outputs_path, 'r') as fd:
        all_job_outputs = json.load(fd, object_pairs_hook=collections.OrderedDict)
        job_id = 'localjob-' + str(len(all_job_outputs))

    with open(all_job_outputs_path, 'wb') as fd:
        all_job_outputs[job_id] = None
        json.dump(all_job_outputs, fd, indent=4)
        fd.write(b'\n')

    job_homedir = os.path.join(environ['DX_TEST_JOB_HOMEDIRS'], job_id)
    os.mkdir(job_homedir)

    job_queue_path = os.path.join(environ['DX_TEST_JOB_HOMEDIRS'], 'job_queue.json')
    with open(job_queue_path, 'r') as fd:
        job_queue = json.load(fd)
    job_entry = {"id": job_id,
                 "function": function,
                 "input_hash": input_hash,
                 "depends_on": depends_on}
    if name is not None:
        job_entry['name'] = name
    job_queue.append(job_entry)
    with open(job_queue_path, 'wb') as fd:
        json.dump(job_queue, fd, indent=4)
        fd.write(b'\n')

    return job_id

def run_one_entry_point(job_id, function, input_hash, run_spec, depends_on, name=None):
    '''
    :param job_id: job ID of the local job to run
    :type job_id: string
    :param function: function to run
    :type function: string
    :param input_hash: input for the job (may include job-based object references)
    :type input_hash: dict
    :param run_spec: run specification from the dxapp.json of the app
    :type run_spec: dict

    Runs the specified entry point and retrieves the job's output,
    updating job_outputs.json (in $DX_TEST_JOB_HOMEDIRS) appropriately.
    '''
    print('======')

    job_homedir = os.path.join(environ['DX_TEST_JOB_HOMEDIRS'], job_id)

    job_env = environ.copy()
    job_env['HOME'] = os.path.join(environ['DX_TEST_JOB_HOMEDIRS'], job_id)

    all_job_outputs_path = os.path.join(environ['DX_TEST_JOB_HOMEDIRS'], 'job_outputs.json')

    with open(all_job_outputs_path, 'r') as fd:
        all_job_outputs = json.load(fd, object_pairs_hook=collections.OrderedDict)

    if isinstance(name, basestring):
        name += ' (' + job_id + ':' + function + ')'
    else:
        name = job_id + ':' + function
    job_name = BLUE() + BOLD() + name + ENDC()
    print(job_name)

    # Resolve local job-based object references
    try:
        resolve_job_references(input_hash, all_job_outputs)
    except Exception as e:
        exit_with_error(job_name + ' ' + JOB_STATES('failed') + ' when resolving input:\n' + fill(str(e)))

    # Get list of non-closed data objects in the input that appear as
    # DNAnexus links; append to depends_on
    if depends_on is None:
        depends_on = []
    get_implicit_depends_on(input_hash, depends_on)

    try:
        wait_for_depends_on(depends_on, all_job_outputs)
    except Exception as e:
        exit_with_error(job_name + ' ' + JOB_STATES('failed') + ' when processing depends_on:\n' + fill(str(e)))

    # Save job input to job_input.json
    with open(os.path.join(job_homedir, 'job_input.json'), 'wb') as fd:
        json.dump(input_hash, fd, indent=4)
        fd.write(b'\n')

    print(job_output_to_str(input_hash, title=(BOLD() + 'Input: ' + ENDC()),
                            title_len=len("Input: ")).lstrip())

    if run_spec['interpreter'] == 'bash':
        # Save job input to env vars
        env_path = os.path.join(job_homedir, 'environment')
        with open(env_path, 'w') as fd:
            job_input_file = os.path.join(job_homedir, 'job_input.json')
            var_defs_hash = file_load_utils.gen_bash_vars(job_input_file, job_homedir=job_homedir)
            for key, val in var_defs_hash.iteritems():
                fd.write("{}={}\n".format(key, val))

    print(BOLD() + 'Logs:' + ENDC())
    start_time = datetime.datetime.now()
    if run_spec['interpreter'] == 'bash':
        script = '''
          cd {homedir};
          . {env_path};
          . {code_path};
          if [[ $(type -t {function}) == "function" ]];
          then {function};
          else echo "$0: Global scope execution complete. Not invoking entry point function {function} because it was not found" 1>&2;
          fi'''.format(homedir=pipes.quote(job_homedir),
                       env_path=pipes.quote(os.path.join(job_env['HOME'], 'environment')),
                       code_path=pipes.quote(environ['DX_TEST_CODE_PATH']),
                       function=function)
        invocation_args = ['bash', '-c', '-e'] + (['-x'] if environ.get('DX_TEST_X_FLAG') else []) + [script]
    elif run_spec['interpreter'] == 'python2.7':
        script = '''#!/usr/bin/env python
import os
os.chdir({homedir})

{code}

import dxpy, json
if dxpy.utils.exec_utils.RUN_COUNT == 0:
    dxpy.run()
'''.format(homedir=repr(job_homedir),
           code=run_spec['code'])

        job_env['DX_TEST_FUNCTION'] = function
        invocation_args = ['python', '-c', script]

    if USING_PYTHON2:
        invocation_args = [arg.encode(sys.stdout.encoding) for arg in invocation_args]
        env = {k: v.encode(sys.stdout.encoding) for k, v in job_env.items()}
    else:
        env = job_env

    fn_process = subprocess.Popen(invocation_args, env=env)

    fn_process.communicate()
    end_time = datetime.datetime.now()

    if fn_process.returncode != 0:
        exit_with_error(job_name + ' ' + JOB_STATES('failed') + ', exited with error code ' + str(fn_process.returncode) + ' after ' + str(end_time - start_time))

    # Now updating job output aggregation file with job's output
    job_output_path = os.path.join(job_env['HOME'], 'job_output.json')
    if os.path.exists(job_output_path):
        try:
            with open(job_output_path, 'r') as fd:
                job_output = json.load(fd, object_pairs_hook=collections.OrderedDict)
        except Exception as e:
            exit_with_error('Error: Could not load output of ' + job_name + ':\n' + fill(str(e.__class__) + ': ' + str(e)))
    else:
        job_output = {}

    print(job_name + ' -> ' + GREEN() + 'finished running' + ENDC() + ' after ' + str(end_time - start_time))
    print(job_output_to_str(job_output, title=(BOLD() + "Output: " + ENDC()),
                            title_len=len("Output: ")).lstrip())

    with open(os.path.join(environ['DX_TEST_JOB_HOMEDIRS'], 'job_outputs.json'), 'r') as fd:
        all_job_outputs = json.load(fd, object_pairs_hook=collections.OrderedDict)
    all_job_outputs[job_id] = job_output

    # Before dumping, see if any new jbors should be resolved now
    for other_job_id in all_job_outputs:
        if all_job_outputs[other_job_id] is None:
            # Skip if job is not done yet (true for ancestor jobs)
            continue
        resolve_job_references(all_job_outputs[other_job_id], all_job_outputs, should_resolve=False)

    with open(os.path.join(environ['DX_TEST_JOB_HOMEDIRS'], 'job_outputs.json'), 'wb') as fd:
        json.dump(all_job_outputs, fd, indent=4)
        fd.write(b'\n')

def run_entry_points(run_spec):
    '''
    :param run_spec: run specification from the dxapp.json of the app
    :type run_spec: dict

    Runs all job entry points found in
    $DX_TEST_JOB_HOMEDIRS/job_queue.json in a first-in, first-out
    manner until it is an empty array (or an error occurs).
    '''
    job_queue_path = os.path.join(environ['DX_TEST_JOB_HOMEDIRS'], 'job_queue.json')
    all_job_outputs_path = os.path.join(environ['DX_TEST_JOB_HOMEDIRS'], 'job_outputs.json')

    while True:
        with open(job_queue_path, 'r') as fd:
            job_queue = json.load(fd)
        if len(job_queue) == 0:
            return

        with open(all_job_outputs_path, 'r') as fd:
            all_job_outputs = json.load(fd)

        entry_point_to_run = None
        for i, entry_point in enumerate(job_queue):
            runnable = True
            # See if its inputs are ready
            while has_local_job_refs(entry_point['input_hash']):
                try:
                    resolve_job_references(entry_point['input_hash'], all_job_outputs)
                except:
                    runnable = False
                    break
            if runnable:
                entry_point_to_run = job_queue.pop(i)
                break

        if entry_point_to_run is None:
            # Just run the first entry point and let the runner throw
            # the appropriate error
            entry_point_to_run = job_queue.pop(0)

        with open(job_queue_path, 'wb') as fd:
            # Update job queue with updated inputs and after having
            # popped the entry point to be run
            json.dump(job_queue, fd)
            fd.write(b'\n')

        run_one_entry_point(job_id=entry_point_to_run['id'],
                            function=entry_point_to_run['function'],
                            input_hash=entry_point_to_run['input_hash'],
                            run_spec=run_spec,
                            depends_on=entry_point_to_run.get('depends_on', []),
                            name=entry_point_to_run.get('name'))
