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

import os, sys, json, subprocess, pipes
import collections, datetime
import dxpy
from dxpy.utils.printing import *
from dxpy.utils.resolver import *
from dxpy.cli.exec_io import *

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
    if is_localjob_id(ref_job_id):
        if job_outputs.get(ref_job_id) is None:
            if should_resolve:
                sys.exit('Error: Job ' + ref_job_id + ' not found in local finished jobs')
            else:
                return jbor
        if ref_job_field not in job_outputs[ref_job_id]:
            sys.exit('Error: Cannot resolve a JBOR with job ID ' + ref_job_id + ' because field ' + ref_job_field + ' was not found in its output')
        return job_outputs[ref_job_id][ref_job_field]
    else:
        dxjob = dxpy.DXJob(ref_job_id)
        try:
            dxjob.wait_on_done()
        except Exception as e:
            sys.exit('Error: Could not wait for ' + ref_job_id + ' to finish: ' + str(e))
        job_desc = dxjob.describe()
        if ref_job_field not in job_desc['output']:
            sys.exit('Error: Cannot resolve a JBOR with job ID ' + ref_job_id + ' because field ' + ref_job_field + ' was not found in its output')
        return job_desc['output'][ref_job_field]

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

def ensure_env_vars():
    for var in ['DX_TEST_CODE_PATH',
                'DX_TEST_RESOURCES_PATH',
                'DX_TEST_JOB_HOMEDIRS']:
        if var not in os.environ:
            sys.exit('Error: Cannot run an entry point locally if the environment variable ' + var + ' has not been set')

def queue_entry_point(function, input_hash):
    '''
    :param function: function to run
    :param input_hash: input to new job
    :returns: new local job ID

    This function should only be called by a locally running job, so
    all relevant DX_TEST_* environment variables should be set.

    This function will set up the home directory for the job, add an
    entry in job_outputs.json, and append the job information to the
    job_queue.json file.  (Both files found in
    $DX_TEST_JOB_HOMEDIRS.)
    '''
    ensure_env_vars()

    all_job_outputs_path = os.path.join(os.environ['DX_TEST_JOB_HOMEDIRS'], 'job_outputs.json')

    with open(all_job_outputs_path, 'r') as fd:
        all_job_outputs = json.load(fd, object_pairs_hook=collections.OrderedDict)
        job_id = 'localjob-' + str(len(all_job_outputs))

    with open(all_job_outputs_path, 'w') as fd:
        all_job_outputs[job_id] = None
        json.dump(all_job_outputs, fd, indent=4)
        fd.write('\n')

    job_homedir = os.path.join(os.environ['DX_TEST_JOB_HOMEDIRS'], job_id)
    os.mkdir(job_homedir)

    job_queue_path = os.path.join(os.environ['DX_TEST_JOB_HOMEDIRS'], 'job_queue.json')
    with open(job_queue_path, 'r') as fd:
        job_queue = json.load(fd)
    job_queue.append({"id": job_id,
                      "function": function,
                      "input_hash": input_hash})
    with open(job_queue_path, 'w') as fd:
        json.dump(job_queue, fd, indent=4)
        fd.write('\n')

    return job_id

def run_one_entry_point(job_id, function, input_hash, run_spec):
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
    job_homedir = os.path.join(os.environ['DX_TEST_JOB_HOMEDIRS'], job_id)
    log_filenames = []

    watch = os.environ.get('DX_TEST_WATCH') == '1'

    if os.environ.get('DX_TEST_SPLIT_LOGS'):
        stdout_path = os.path.join(os.environ['DX_TEST_JOB_HOMEDIRS'], job_id + '-stdout.log')
        log_filenames.append(stdout_path)
        job_stdout = open(stdout_path, 'w+')

        stderr_path = os.path.join(os.environ['DX_TEST_JOB_HOMEDIRS'], job_id + '-stderr.log')
        log_filenames.append(stderr_path)
        job_stderr = open(stderr_path, 'w+')
    else:
        stdouterr_path = os.path.join(os.environ['DX_TEST_JOB_HOMEDIRS'], job_id + '.log')
        log_filenames.append(stdouterr_path)
        job_stdout = open(stdouterr_path, 'w+')
        job_stderr = job_stdout

    job_env = os.environ.copy()
    job_env['HOME'] = os.path.join(os.environ['DX_TEST_JOB_HOMEDIRS'], job_id)

    all_job_outputs_path = os.path.join(os.environ['DX_TEST_JOB_HOMEDIRS'], 'job_outputs.json')

    with open(all_job_outputs_path, 'r') as fd:
        all_job_outputs = json.load(fd, object_pairs_hook=collections.OrderedDict)

    # Resolve local job-based object references
    resolve_job_references(input_hash, all_job_outputs)

    # Save job input to job_input.json
    with open(os.path.join(job_homedir, 'job_input.json'), 'w') as fd:
        json.dump(input_hash, fd, indent=4)
        fd.write('\n')

    if run_spec['interpreter'] == 'bash':
        # Save job input to env vars
        env_path = os.path.join(job_homedir, 'environment')
        with open(env_path, 'w') as fd:
            # Following code is what is used to generate env vars on the remote worker
            fd.write("\n".join(["export {k}=( {vlist} )".format(k=k, vlist=" ".join([pipes.quote(vitem if isinstance(vitem, basestring) else json.dumps(vitem)) for vitem in v])) if isinstance(v, list) else "export {k}={v}".format(k=k, v=pipes.quote(v if isinstance(v, basestring) else json.dumps(v))) for k, v in input_hash.iteritems()]))

    print job_id + ':' + function + ' -> ' + JOB_STATES('running')
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
                       code_path=pipes.quote(os.environ['DX_TEST_CODE_PATH']),
                       function=function)

        fn_process = subprocess.Popen(['bash', '-c', '-e'] + \
                                          (['-x'] if os.environ.get('DX_TEST_X_FLAG') else []) + \
                                          [script],
                                      stdout=job_stdout,
                                      stderr=job_stderr,
                                      env=job_env)
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
        job_env['DX_TEST_JOB_INPUT'] = json.dumps(input_hash)

        fn_process = subprocess.Popen(['python', '-c', script],
                                      stdout=job_stdout,
                                      stderr=job_stderr,
                                      env=job_env)

    fn_process.communicate()
    end_time = datetime.datetime.now()
    job_stderr.write('Exit code: ' + str(fn_process.returncode) + '\n')

    if watch:
        print "Logs"
        print '-'*len("Logs")
        if os.environ.get('DX_TEST_SPLIT_LOGS'):
            job_stdout.seek(0)
            print GREEN() + 'stdout:' + ENDC()
            for line in job_stdout:
                print '> ' + line.strip()
            job_stderr.seek(0)
            print YELLOW() + 'stderr:' + ENDC()
            for line in job_stderr:
                print '> ' + line.strip()
        else:
            job_stdout.seek(0)
            for line in job_stdout:
                print '> ' + line.strip()

    if fn_process.returncode != 0:
        sys.exit(fill(job_id + ':' + function + ' ' + JOB_STATES('failed') + ' (error code ' + str(fn_process.returncode) + ')') + ('' if watch else '\n' + fill('Consult the job\'s logs in:') + '\n  ' + '\n  '.join(log_filenames)))

    # Now updating job output aggregation file with job's output
    job_output_path = os.path.join(job_env['HOME'], 'job_output.json')
    if os.path.exists(job_output_path):
        try:
            with open(job_output_path, 'r') as fd:
                job_output = json.load(fd, object_pairs_hook=collections.OrderedDict)
        except BaseException as e:
            sys.exit(fill(str(e.__class__) + ': ' + str(e)))
    else:
        job_output = {}

    print job_id + ':' + function + ' -> ' + GREEN() + 'finished running' + ENDC() + ' after ' + str(end_time - start_time)
    print job_output_to_str(job_output, prefix='  ')

    with open(os.path.join(os.environ['DX_TEST_JOB_HOMEDIRS'], 'job_outputs.json'), 'r') as fd:
        all_job_outputs = json.load(fd, object_pairs_hook=collections.OrderedDict)
    all_job_outputs[job_id] = job_output

    # Before dumping, see if any new jbors should be resolved now
    for other_job_id in all_job_outputs:
        if all_job_outputs[other_job_id] is None:
            # Skip if job is not done yet (true for ancestor jobs)
            continue
        resolve_job_references(all_job_outputs[other_job_id], all_job_outputs, should_resolve=False)

    with open(os.path.join(os.environ['DX_TEST_JOB_HOMEDIRS'], 'job_outputs.json'), 'w') as fd:
        json.dump(all_job_outputs, fd, indent=4)
        fd.write('\n')

def run_entry_points(run_spec):
    '''
    :param run_spec: run specification from the dxapp.json of the app
    :type run_spec: dict

    Runs all job entry points found in
    $DX_TEST_JOB_HOMEDIRS/job_queue.json in a first-in, first-out
    manner until it is an empty array (or an error occurs).
    '''
    job_queue_path = os.path.join(os.environ['DX_TEST_JOB_HOMEDIRS'], 'job_queue.json')
    while True:
        with open(job_queue_path, 'r') as fd:
            job_queue = json.load(fd)
        if len(job_queue) == 0:
            return
        entry_point_to_run = job_queue[0]
        with open(job_queue_path, 'w') as fd:
            json.dump(job_queue[1:], fd)
            fd.write('\n')
        run_one_entry_point(job_id=entry_point_to_run['id'],
                            function=entry_point_to_run['function'],
                            input_hash=entry_point_to_run['input_hash'],
                            run_spec=run_spec)
