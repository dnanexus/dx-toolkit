'''
Logic for determining environment variable values.  See external
documentation [TODO: put link here] for more details.
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

def get_env():
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
        'DX_CLI_WD': os.environ.get('DX_CLI_WD', None)
        }

    user_file_env_vars = parse_user_env_file()
    installed_file_env_vars = parse_installed_env_file()

    for var in env_vars:
        if env_vars[var] is None:
            if var in user_file_env_vars:
                env_vars[var] = user_file_env_vars[var]
            elif var in installed_file_env_vars:
                env_vars[var] = installed_file_env_vars[var]

    if sys.stdout.isatty():
        already_set = []
        for var in user_file_env_vars:
            if var in env_vars and user_file_env_vars[var] != env_vars[var]:
                already_set.append(var)

        if len(already_set) > 0:
            print textwrap.fill("WARNING: The following environment variables were found to be different than the values last stored by dx.  To use the values stored by dx, run \"source ~/.dnanexus_config/environment\" to set your environment variables in your shell.  To clear the dx-stored values, run \"dx clearenv\"")
            print '  ' + '\n  '.join(already_set) + '\n'

    return env_vars
