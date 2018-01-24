# Copyright (C) 2013-2018 DNAnexus, Inc.
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
Utilities used when running executables in a batch. For example, running an analysis on
a set of data samples.
"""

from __future__ import print_function, unicode_literals, division, absolute_import
import os, sys, json, re

import dxpy
from ..compat import open
from ..exceptions import err_exit, DXError


# Parse the CSV file.
# Create a dictionary with the input arguments for each execution.
#
def batch_expand_args(executable, input_json, batch_csv_file):
    raise Exception("unimplemented")

def batch_run(executable, expanded_args_json, run_kwargs):
    # validate
    # launch
    # return the job-ids

    # Run the executable
    try:
        dxexecution = executable.run(input_json, **run_kwargs)
        if not args.brief:
            print(dxexecution._class.capitalize() + " ID: " + dxexecution.get_id())
        else:
            print(dxexecution.get_id())
        sys.stdout.flush()

        if isinstance(dxexecution, dxpy.DXJob):
            if args.watch:
                watch_args = parser.parse_args(['watch', dxexecution.get_id()])
                print('')
                print('Job Log')
                print('-------')
                watch(watch_args)
            elif args.ssh:
                if args.ssh_proxy:
                    ssh_args = parser.parse_args(
                        ['ssh', '--ssh-proxy', args.ssh_proxy, dxexecution.get_id()])
                else:
                    ssh_args = parser.parse_args(['ssh', dxexecution.get_id()])
                ssh(ssh_args, ssh_config_verified=True)
    except Exception:
        err_exit()

    return dxexecution
