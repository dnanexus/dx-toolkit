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
This file contains parsers with no added help that can be inherited by
other parsers, as well as utility functions for parsing the input to
those parsers.
'''

import argparse, json, os
from ..utils.printing import fill
from ..utils.resolver import split_unescaped
from ..exceptions import DXError

class DXParserError(DXError):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

all_arg = argparse.ArgumentParser(add_help=False)
all_arg.add_argument('-a', '--all', help=fill('Apply to all results with the same name without prompting', width_adjustment=-24), action='store_true')

no_color_arg = argparse.ArgumentParser(add_help=False)
no_color_arg.add_argument('--color',
                          help=fill('Set when color is used (color=auto is used when stdout is a TTY)', width_adjustment=-24),
                          choices=['off', 'on', 'auto'], default='auto')

delim_arg = argparse.ArgumentParser(add_help=False)
delim_arg.add_argument('--delimiter', '--delim',
                       dest='delimiter',
                       help=fill('Always use exactly one of DELIMITER to separate fields to be printed; if no delimiter is provided with this flag, TAB will be used', width_adjustment=-24),
                       nargs='?',
                       const="\t")

json_arg = argparse.ArgumentParser(add_help=False)
json_arg.add_argument('--json', help='Display return value in JSON', action='store_true')

stdout_args = argparse.ArgumentParser(add_help=False)
stdout_args_gp = stdout_args.add_mutually_exclusive_group()
stdout_args_gp.add_argument('--brief', help=fill('Display a brief version of the return value; for most commands, prints a DNAnexus ID per line', width_adjustment=-24), action='store_true')
stdout_args_gp.add_argument('--summary', help='Display summary output (default)', action='store_true')
stdout_args_gp.add_argument('--verbose', help='If available, displays extra verbose output',
                            action='store_true')

def process_output_args(args):
    if not args.brief and not args.summary and not args.verbose:
        args.summary = True

parser_dataobject_args = argparse.ArgumentParser(add_help=False)
parser_dataobject_args_gp = parser_dataobject_args.add_argument_group('metadata arguments')
parser_dataobject_args_gp.add_argument('--visibility', choices=['hidden', 'visible'], dest='hidden', default='visible', help='Whether the object is hidden or not')
parser_dataobject_args_gp.add_argument('--property', dest='properties', metavar='KEY=VALUE', help=fill('Key-value pair to add as a property; repeat as necessary,', width_adjustment=-24) + '\n' + fill('e.g. "--property key1=val1 --property key2=val2"', width_adjustment=-24, initial_indent=' ', subsequent_indent=' ', break_on_hyphens=False), action='append')
parser_dataobject_args_gp.add_argument('--type', metavar='TYPE', dest='types', help=fill('Type of the data object; repeat as necessary,', width_adjustment=-24) + '\n' + fill('e.g. "--type type1 --type type2"', width_adjustment=-24, break_on_hyphens=False, initial_indent=' ', subsequent_indent=' '), action='append')
parser_dataobject_args_gp.add_argument('--tag', metavar='TAG', dest='tags', help=fill('Tag of the data object; repeat as necessary,', width_adjustment=-24) + '\n' + fill('e.g. "--tag tag1 --tag tag2"', width_adjustment=-24, break_on_hyphens=False, initial_indent=' ', subsequent_indent=' '), action='append')
parser_dataobject_args_gp.add_argument('--details', help='JSON to store as details')
parser_dataobject_args_gp.add_argument('-p', '--parents', help='Create any parent folders necessary', action='store_true')

parser_single_dataobject_output_args = argparse.ArgumentParser(add_help=False)
parser_single_dataobject_output_args.add_argument('-o', '--output', help=argparse.SUPPRESS)
parser_single_dataobject_output_args.add_argument('path', help=fill('DNAnexus path for the new data object (default uses current project and folder if not provided)', width_adjustment=-24), nargs='?')

find_by_properties_and_tags_args = argparse.ArgumentParser(add_help=False)
find_by_properties_and_tags_args.add_argument('--property', dest='properties',
                                              metavar='KEY[=VALUE]',
                                              help='Key-value pair of a property or simply a property key; if only a key is provided, matches a result that has the key with any value; repeat as necessary, e.g. "--property key1=val1 --property key2"',
                                              action='append')
find_by_properties_and_tags_args.add_argument('--tag',
                                              help='Tag to match; repeat as necessary, e.g. "--tag tag1 --tag tag2" will require both tags',
                                              action='append')

def process_properties_args(args):
    # Properties
    properties = None
    if args.properties is not None:
        properties = {}
        for keyeqval in args.properties:
            substrings = split_unescaped('=', keyeqval, include_empty_strings=True)
            if len(substrings) != 2:
                raise DXParserError('Property key-value pair must be given using syntax "property_key=property_value"')
            elif substrings[0] == '':
                raise DXParserError('Property keys must be nonempty strings')
            else:
                properties[substrings[0]] = substrings[1]
    args.properties = properties

def process_find_by_property_args(args):
    properties = None
    if args.properties is not None:
        properties = {}
        for keyeqval in args.properties:
            substrings = split_unescaped('=', keyeqval, include_empty_strings=True)
            if len(substrings) > 2:
                raise DXParserError('Property value must be given using syntax "property_key" or "property_key=property_value"')
            elif substrings[0] == '':
                raise DXParserError('Property keys must be nonempty strings')
            elif len(substrings) == 1:
                properties[keyeqval] = True
            else:
                properties[substrings[0]] = substrings[1]

    args.properties = properties

def process_dataobject_args(args):
    process_properties_args(args)

    # Visibility
    args.hidden = (args.hidden == 'hidden')

    # Details
    if args.details is not None:
        try:
            args.details = json.loads(args.details)
        except:
            raise DXParserError('Error: details could not be parsed as JSON')

def process_single_dataobject_output_args(args):
    if args.path is not None and args.output is not None:
        raise DXParserError('Error: Cannot provide both the positional PATH and -o/--output arguments')
    elif args.output is None:
        args.output = args.path

_env_args = argparse.ArgumentParser(add_help=False, prog='dx command ...')
_env_args.add_argument('--apiserver-host', help='API server host')
_env_args.add_argument('--apiserver-port', help='API server port')
_env_args.add_argument('--apiserver-protocol', help='API server protocol (http or https)')
_env_args.add_argument('--project-context-id', help='Default project or project context ID')
_env_args.add_argument('--workspace-id', help='Workspace ID (for jobs only)')
_env_args.add_argument('--security-context', help='JSON string of security context')
_env_args.add_argument('--auth-token', help='Authentication token')

class EnvHelpAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        _env_args.print_help()
        parser.exit(0)

env_args = argparse.ArgumentParser(add_help=False)
env_args.add_argument('--apiserver-host', help=argparse.SUPPRESS)
env_args.add_argument('--apiserver-port', help=argparse.SUPPRESS)
env_args.add_argument('--apiserver-protocol', help=argparse.SUPPRESS)
env_args.add_argument('--project-context-id', help=argparse.SUPPRESS)
env_args.add_argument('--workspace-id', help=argparse.SUPPRESS)
env_args.add_argument('--security-context', help=argparse.SUPPRESS)
env_args.add_argument('--auth-token', help=argparse.SUPPRESS)
env_args.add_argument('--env-help', help=fill('Display help message for overriding environment variables', width_adjustment=-24), action=EnvHelpAction, nargs=0)

def set_env_from_args(args):
    ''' Sets the environment variables for this process from arguments (argparse.Namespace)
    and calls dxpy._initialize() to reset any values that it has already set.
    '''
    args = vars(args)

    require_initialize = False

    if args.get('apiserver_host') is not None:
        os.environ['DX_APISERVER_HOST'] = args['apiserver_host']
        require_initialize = True
    if args.get('apiserver_port') is not None:
        os.environ['DX_APISERVER_PORT'] = args['apiserver_port']
        require_initialize = True
    if args.get('apiserver_protocol') is not None:
        os.environ['DX_APISERVER_PROTOCOL'] = args['apiserver_protocol']
        require_initialize = True
    if args.get('project_context_id') is not None:
        os.environ['DX_PROJECT_CONTEXT_ID'] = args['project_context_id']
        require_initialize = True
    if args.get('workspace_id') is not None:
        os.environ['DX_WORKSPACE_ID'] = args['workspace_id']
        require_initialize = True
    if args.get('cli_wd') is not None:
        os.environ['DX_CLI_WD'] = args['cli_wd']
        require_initialize = True
    if args.get('security_context') is not None:
        os.environ['DX_SECURITY_CONTEXT'] = args['security_context']
        require_initialize = True
    if args.get('auth_token') is not None:
        os.environ['DX_SECURITY_CONTEXT'] = json.dumps({"auth_token": args['auth_token'],
                                                        "auth_token_type": "Bearer"})
        require_initialize = True

    if require_initialize:
        from dxpy import _initialize
        _initialize(suppress_warning=True)

extra_args = argparse.ArgumentParser(add_help=False)
extra_args.add_argument('--extra-args', help=fill("Arguments (in JSON format) to pass to the underlying API method, overriding the default settings", width_adjustment=-24))

def process_extra_args(args):
    if args.extra_args is not None:
        try:
            args.extra_args = json.loads(args.extra_args)
        except:
            raise DXParserError('Value given for --extra-args could not be parsed as JSON')
