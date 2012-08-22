'''
Logic for determining environment variable values.  See external
documentation [TODO: put link here] for more details.
'''

import os, shlex, sys, textwrap, argparse, json

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
        'DX_CLI_WD': os.environ.get('DX_CLI_WD', None),
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

    if env_vars['DX_CLI_WD'] is None:
        try:
            with open(os.path.expanduser('~/.dnanexus_config/DX_CLI_WD')) as fd:
                env_vars['DX_CLI_WD'] = fd.read()
        except:
            pass

    if sys.stdout.isatty():
        already_set = []
        for var in user_file_env_vars:
            if var in env_vars and user_file_env_vars[var] != env_vars[var]:
                already_set.append(var)

        if len(already_set) > 0:
            print textwrap.fill("WARNING: The following environment variables were found to be different than the values last stored by dx: " + ", ".join(already_set), width=80) + '\n'
            print textwrap.fill("To use the values stored by dx, unset the environment variables in your shell by running \"source ~/.dnanexus_config/unsetenv\".  To clear the dx-stored values, run \"dx clearenv\".", width=80)

    return env_vars

def set_env_from_args(args):
    ''' Sets the environment variables for this process from arguments (argparse.Namespace)
    and calls dxpy._initialize() to reset any values that it has already set.
    '''
    args = vars(args)
    if args.get('apiserver_host') is not None:
        os.environ['DX_APISERVER_HOST'] = args['apiserver_host']
    if args.get('apiserver_port') is not None:
        os.environ['DX_APISERVER_PORT'] = args['apiserver_port']
    if args.get('apiserver_protocol') is not None:
        os.environ['DX_APISERVER_PROTOCOL'] = args['apiserver_protocol']
    if args.get('project_context_id') is not None:
        os.environ['DX_PROJECT_CONTEXT_ID'] = args['project_context_id']
    if args.get('workspace_id') is not None:
        os.environ['DX_WORKSPACE_ID'] = args['workspace_id']
    if args.get('cli_wd') is not None:
        os.environ['DX_CLI_WD'] = args['cli_wd']
    if args.get('security_context') is not None:
        os.environ['DX_SECURITY_CONTEXT'] = args['security_context']
    if args.get('token') is not None:
        os.environ['DX_SECURITY_CONTEXT'] = json.dumps({"auth_token": args['token'],
                                                        "auth_token_type": "Bearer"})
    from dxpy import _initialize
    _initialize()

env_overrides_parser = argparse.ArgumentParser(add_help=False)
env_overrides_parser.add_argument('--apiserver-host', help='API Server host')
env_overrides_parser.add_argument('--apiserver-port', help='API Server port')
env_overrides_parser.add_argument('--apiserver-protocol', help='API Server protocol')
env_overrides_parser.add_argument('--project-context-id', help='Project Context ID')
env_overrides_parser.add_argument('--workspace-id', help='Workspace ID')
env_overrides_parser.add_argument('--security_context', help='Security Context')
env_overrides_parser.add_argument('--token', help='Authentication Token')
