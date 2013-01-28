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
Logic for determining environment variable values.  See external
documentation
http://wiki.dnanexus.com/Command-Line-Client/Environment%20Variables
for more details.
'''

import os, shlex, sys, textwrap

def parse_env_file(filename):
    env_vars = {}
    try:
        with open(filename, 'r') as fd:
            for line in fd:
                if line.startswith('export DX'):
                    env_vars[line[7: line.find('=')]] = ''.join(shlex.split(line[line.find('=') + 1:]))
    except:
        pass
    return env_vars

def parse_user_env_file():
    return parse_env_file(os.path.expanduser('~/.dnanexus_config/environment'))

def parse_installed_env_file():
    return parse_env_file('/opt/dnanexus/environment')

def get_env(suppress_warning=False):
    '''
    :returns env_vars: mapping of environment variable names to resolved values
    :type env_vars: dict

    This method looks up the known environment variables, and if they
    are not found, then attempts to resolve them by looking in the
    file ~/.dnanexus_config/environment, followed by the installed
    defaults in /opt/dnanexus/environment.
    '''

    env_vars = {
        'DX_APISERVER_HOST': os.environ.get('DX_APISERVER_HOST', None),
        'DX_APISERVER_PORT': os.environ.get('DX_APISERVER_PORT', None),
        'DX_APISERVER_PROTOCOL': os.environ.get('DX_APISERVER_PROTOCOL', None),
        'DX_PROJECT_CONTEXT_ID': os.environ.get('DX_PROJECT_CONTEXT_ID', None),
        'DX_WORKSPACE_ID': os.environ.get('DX_WORKSPACE_ID', None),
        'DX_CLI_WD': os.environ.get('DX_CLI_WD', None),
        'DX_USERNAME': os.environ.get('DX_USERNAME', None),
        'DX_PROJECT_CONTEXT_NAME': os.environ.get('DX_PROJECT_CONTEXT_NAME', None),
        'DX_SECURITY_CONTEXT': os.environ.get('DX_SECURITY_CONTEXT', None)
        }

    user_file_env_vars = parse_user_env_file()
    installed_file_env_vars = parse_installed_env_file()

    for var in env_vars:
        if env_vars[var] is None:
            if var in user_file_env_vars:
                env_vars[var] = user_file_env_vars[var]
            elif var in installed_file_env_vars:
                env_vars[var] = installed_file_env_vars[var]

    for standalone_var in 'DX_CLI_WD', 'DX_USERNAME', 'DX_PROJECT_CONTEXT_NAME':
        if env_vars[standalone_var] is None:
            try:
                with open(os.path.expanduser('~/.dnanexus_config/' + standalone_var)) as fd:
                    env_vars[standalone_var] = fd.read()
            except:
                pass

    if sys.stdout.isatty():
        already_set = []
        for var in user_file_env_vars:
            if var in env_vars and user_file_env_vars[var] != env_vars[var]:
                already_set.append(var)

        if not suppress_warning and len(already_set) > 0:
            sys.stderr.write(textwrap.fill("WARNING: The following environment variables were found to be different than the values last stored by dx: " + ", ".join(already_set), width=80) + '\n')
            sys.stderr.write(textwrap.fill("To use the values stored by dx, unset the environment variables in your shell by running \"source ~/.dnanexus_config/unsetenv\".  To clear the dx-stored values, run \"dx clearenv\".", width=80) + '\n')

    return env_vars
