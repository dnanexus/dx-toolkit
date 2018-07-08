# Copyright (C) 2016 DNAnexus, Inc.
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

import sys
import subprocess
import time
import dxpy
from .utils.printing import (RED, BOLD)
from .exceptions import err_exit
from .utils.resolver import get_app_from_path

from sys import platform
NOTEBOOK_APP = '_notebook_server'
LOUPE_APP = '_10x_loupe_server'
SERVER_READY_TAG = 'server_running'
SLEEP_PERIOD = 5

def setup_ssh_tunnel(job_id, local_port, remote_port):
    """
    Setup an ssh tunnel to the given job-id.  This will establish
    the port over the given local_port to the given remote_port
    and then exit, keeping the tunnel in place until the job is
    terminated.
    """
    cmd = ['dx', 'ssh', '--suppress-running-check', job_id, '-o', 'StrictHostKeyChecking no']
    cmd += ['-f', '-L', '{0}:localhost:{1}'.format(local_port, remote_port), '-N']
    subprocess.check_call(cmd)


def poll_for_server_running(job_id):
    """
    Poll for the job to start running and post the SERVER_READY_TAG.
    """
    sys.stdout.write('Waiting for server in {0} to initialize ...'.format(job_id))
    sys.stdout.flush()
    desc = dxpy.describe(job_id)
    # Keep checking until the server has begun or it has failed.
    while(SERVER_READY_TAG not in desc['tags'] and desc['state'] != 'failed'):
        time.sleep(SLEEP_PERIOD)
        sys.stdout.write('.')
        sys.stdout.flush()
        desc = dxpy.describe(job_id)

    # If the server job failed, provide friendly advice.
    if desc['state'] == 'failed':
        msg = RED('Error:') + ' Server failed to run.\n'
        msg += 'You may want to check the job logs by running:'
        msg += BOLD('dx watch {0}'.format(job_id))
        err_exit(msg)


def multi_platform_open(cmd):
    """
    Take the given command and use the OS to automatically open the appropriate
    resource.  For instance, if a URL is provided, this will have the OS automatically
    open the URL in the default web browser.
    """
    if platform == "linux" or platform == "linux2":
        cmd = ['xdg-open', cmd]
    elif platform == "darwin":
        cmd = ['open', cmd]
    elif platform == "win32":
        cmd = ['start', cmd]
    subprocess.check_call(cmd)


def get_notebook_app_versions():
    """
    Get the valid version numbers of the notebook app.
    """
    notebook_apps = dxpy.find_apps(name=NOTEBOOK_APP, all_versions=True)
    versions = [str(dxpy.describe(app['id'])['version']) for app in notebook_apps]
    return versions


def run_notebook(args, ssh_config_check):
    """
    Launch the notebook server.
    """
    # Check that ssh is setup.  Currently notebooks require ssh for tunelling.
    ssh_config_check()
    if args.only_check_config:
        return

    # If the user requested a specific version of the notebook server,
    # get the executable id.
    if args.version is not None:
        executable = get_app_from_path('app-{0}/{1}'.format(NOTEBOOK_APP, args.version))
        if executable is not None and 'id' in executable:
            executable = executable['id']
        else:
            msg = RED('Warning:') + ' Invalid notebook version: {0}\nValid versions are: '.format(args.version)
            msg += BOLD('{0}'.format(str(get_notebook_app_versions())))
            err_exit(msg)
    else:
        executable = 'app-{0}'.format(NOTEBOOK_APP)

    # Compose the command to launch the notebook
    cmd = ['dx', 'run', executable, '-inotebook_type={0}'.format(args.notebook_type)]
    cmd += ['-iinput_files={0}'.format(f) for f in args.notebook_files]
    cmd += ['-itimeout={0}'.format(args.timeout), '-y', '--brief', '--allow-ssh', '--instance-type', args.instance_type]
    if args.spark:
        cmd += ['-iinstall_spark=true']
    if args.snapshot:
        cmd += ['-isnapshot={0}'.format(args.snapshot)]
    job_id = subprocess.check_output(cmd).strip()

    poll_for_server_running(job_id)

    if args.notebook_type in {'jupyter', 'jupyter_lab', 'jupyter_notebook'}:
        remote_port = 8888

    setup_ssh_tunnel(job_id, args.port, remote_port)

    if args.open_server:
        multi_platform_open('http://localhost:{0}'.format(args.port))
        print('A web browser should have opened to connect you to your notebook.')
    print('If no browser appears, or if you need to reopen a browser at any point, you should be able to point your browser to http://localhost:{0}'.format(args.port))


def run_loupe(args):
    cmd = ['dx', 'run', 'app-{0}'.format(LOUPE_APP)]
    cmd += ['-iloupe_files={0}'.format(f) for f in args.loupe_files]
    cmd += ['-itimeout={0}'.format(args.timeout, '-y', '--brief', '--allow-ssh', '--instance-type', args.instance_type)]
    job_id = subprocess.check_output(cmd).strip()

    poll_for_server_running(job_id)

    remote_port = 3000

    setup_ssh_tunnel(job_id, args.port, remote_port)

    if args.open_server:
        multi_platform_open('http://localhost:{0}'.format(args.port))
        print('A web browser should have opened to connect you to your notebook.')
    print('If no browser appears, or if you need to reopen a browser at any point, you should be able to point your browser to http://localhost:{0}'.format(args.port))
    print('Your Loupe session is scheduled to terminate in {0}.  If you wish to terminate before this, please run:'.format(args.timeout))
    print('dx terminate {0}'.format(job_id))
