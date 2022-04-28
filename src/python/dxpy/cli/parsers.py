# Copyright (C) 2013-2016 DNAnexus, Inc.
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

from __future__ import print_function, unicode_literals, division, absolute_import

import argparse, json
from .. import config
from ..utils.printing import fill
from ..utils.pretty_print import format_table
from ..utils.resolver import split_unescaped
from ..utils.completer import InstanceTypesCompleter
from ..exceptions import (DXError, DXCLIError)
from ..compat import basestring

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
stdout_args_gp.add_argument('--verbose', help='If available, displays extra verbose output',
                            action='store_true')

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
                                              help=fill('Key-value pair of a property or simply a property key; if only a key is provided, matches a result that has the key with any value; repeat as necessary, e.g. "--property key1=val1 --property key2"', width_adjustment=-24),
                                              action='append')
find_by_properties_and_tags_args.add_argument('--tag',
                                              help=fill('Tag to match; repeat as necessary, e.g. "--tag tag1 --tag tag2" will require both tags', width_adjustment=-24),
                                              action='append')

find_executions_args = argparse.ArgumentParser()
find_executions_args.add_argument('--id', help=fill('Show only the job tree or job containing this job ID', width_adjustment=-24))
find_executions_args.add_argument('--name', help=fill('Restrict the search by job name (accepts wildcards "*" and "?")', width_adjustment=-24))
find_executions_args.add_argument('--user', help=fill('Username who launched the job (use "self" to ask for your own jobs)', width_adjustment=-24))
find_executions_args.add_argument('--project', help=fill('Project context (output project), default is current project if set', width_adjustment=-24))
find_executions_args.add_argument('--all-projects', '--allprojects', help=fill('Extend search to all projects', width_adjustment=-24), action='store_true')
find_executions_args.add_argument('--app', '--applet', '--executable', dest='executable', help=fill('Applet or App ID that job is running', width_adjustment=-24))
find_executions_args.add_argument('--state', help=fill('State of the job, e.g. \"done\", \"failed\"', width_adjustment=-24))
find_executions_args.add_argument('--origin', help=fill('Job ID of the top-level job', width_adjustment=-24)) # Redundant but might as well
find_executions_args.add_argument('--parent', help=fill('Job ID of the parent job; implies --all-jobs', width_adjustment=-24))
find_executions_args.add_argument('--created-after', help=fill('Date (e.g. --created-after="2021-12-01" or --created-after="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --created-after=1642196636000) after which the job was last created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --created-after=-2d for executions created in the last 2 days)', width_adjustment=-24))
find_executions_args.add_argument('--created-before', help=fill('Date (e.g. --created-before="2021-12-01" or --created-before="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --created-before=1642196636000) before which the job was last created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --created-before=-2d for executions created earlier than 2 days ago)', width_adjustment=-24))
find_executions_args.add_argument('--no-subjobs', help=fill('Do not show any subjobs', width_adjustment=-24), action='store_true')
find_executions_args.add_argument('--root-execution', '--root', help=fill('Execution ID of the top-level (user-initiated) job or analysis', width_adjustment=-24))
find_executions_args.add_argument('-n', '--num-results', metavar='N', type=int, help=fill('Max number of results (trees or jobs, as according to the search mode) to return (default 10)', width_adjustment=-24), default=10)
find_executions_args.add_argument('-o', '--show-outputs', help=fill('Show job outputs in results', width_adjustment=-24), action='store_true')

def add_find_executions_search_gp(parser):
    find_executions_search_gp = parser.add_argument_group('Search mode')
    find_executions_search = find_executions_search_gp.add_mutually_exclusive_group()
    find_executions_search.add_argument('--trees', help=fill('Show entire job trees for all matching results (default)', width_adjustment=-24), action='store_true')
    find_executions_search.add_argument('--origin-jobs', help=fill('Search and display only top-level origin jobs', width_adjustment=-24), action='store_true')
    find_executions_search.add_argument('--all-jobs', help=fill('Search for jobs at all depths matching the query (no tree structure shown)', width_adjustment=-24), action='store_true')

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

    if args.get('apiserver_host') is not None:
        config['DX_APISERVER_HOST'] = args['apiserver_host']
    if args.get('apiserver_port') is not None:
        config['DX_APISERVER_PORT'] = args['apiserver_port']
    if args.get('apiserver_protocol') is not None:
        config['DX_APISERVER_PROTOCOL'] = args['apiserver_protocol']
    if args.get('project_context_id') is not None:
        config['DX_PROJECT_CONTEXT_ID'] = args['project_context_id']
    if args.get('workspace_id') is not None:
        config['DX_WORKSPACE_ID'] = args['workspace_id']
    if args.get('cli_wd') is not None:
        config['DX_CLI_WD'] = args['cli_wd']
    if args.get('security_context') is not None:
        config['DX_SECURITY_CONTEXT'] = args['security_context']
    if args.get('auth_token') is not None:
        config['DX_SECURITY_CONTEXT'] = json.dumps({"auth_token": args['auth_token'],
                                                    "auth_token_type": "Bearer"})

extra_args = argparse.ArgumentParser(add_help=False)
extra_args.add_argument('--extra-args', help=fill("Arguments (in JSON format) to pass to the underlying API method, overriding the default settings", width_adjustment=-24))

def process_extra_args(args):
    if args.extra_args is not None:
        try:
            args.extra_args = json.loads(args.extra_args)
        except:
            raise DXParserError('Value given for --extra-args could not be parsed as JSON')

exec_input_args = argparse.ArgumentParser(add_help=False)
exec_input_args.add_argument('-i', '--input', help=fill('An input to be added using "<input name>[:<class>]=<input value>" (provide "class" if there is no input spec; it can be any job IO class, e.g. "string", "array:string", or "array"; if "class" is "array" or not specified, the value will be attempted to be parsed as JSON and is otherwise treated as a string)', width_adjustment=-24), action='append')
exec_input_args.add_argument('-j', '--input-json', help=fill('The full input JSON (keys=input field names, values=input field values)', width_adjustment=-24))
exec_input_args.add_argument('-f', '--input-json-file', dest='filename', help=fill('Load input JSON from FILENAME ("-" to use stdin)'))

class PrintInstanceTypeHelp(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        print("Help: Specifying instance types for " + parser.prog)
        print()
        print(fill('A single instance type can be requested to be used by all entry points by providing the instance type name.  Different instance types can also be requested for different entry points of an app or applet by providing a JSON string mapping from function names to instance types, e.g.'))
        print()
        print('    {"main": "mem2_hdd2_v2_x2", "other_function": "mem1_ssd1_v2_x2"}')
        if parser.prog == 'dx run':
            print()
            print(fill('If running a workflow, different stages can have different instance type ' +
                       'requests by prepending the request with "<stage identifier>=" (where a ' +
                       'stage identifier is an ID, a numeric index, or a unique stage name) and ' +
                       'repeating the argument for as many stages as desired.  If no stage ' +
                       'identifier is provided, the value is applied as a default for all stages.'))
            print()
            print(fill('The following example runs all entry points of the first stage with ' +
                       'mem2_hdd2_v2_x2, the stage named "BWA" with mem1_ssd1_v2_x2, and all other ' +
                       'stages with mem2_hdd2_v2_x4'))
            print()
            print('    Example: dx run workflow --instance-type 0=mem2_hdd2_v2_x2 \\')
            print('               --instance-type BWA=mem1_ssd1_v2_x2 --instance-type mem2_hdd2_v2_x4')
        print()
        print('Available instance types:')
        print()
        print(format_table(InstanceTypesCompleter.instance_types.values(),
                           column_names=list(InstanceTypesCompleter.instance_types.values())[0]._fields))
        parser.exit(0)

instance_type_arg = argparse.ArgumentParser(add_help=False)
instance_type_arg.add_argument('--instance-type',
                               metavar='INSTANCE_TYPE_OR_MAPPING',
                               help=fill('Specify instance type(s) for jobs this executable will run; see --instance-type-help for more details', width_adjustment=-24),
                               action='append').completer = InstanceTypesCompleter()
instance_type_arg.add_argument('--instance-type-help',
                               nargs=0,
                               help=fill('Print help for specifying instance types'),
                               action=PrintInstanceTypeHelp)

property_args = argparse.ArgumentParser(add_help=False)
property_args.add_argument('--property', dest='properties', metavar='KEY=VALUE',
                           help=(fill('Key-value pair to add as a property; repeat as necessary,',
                                      width_adjustment=-24) + '\n' +
                                 fill('e.g. "--property key1=val1 --property key2=val2"',
                                      width_adjustment=-24, initial_indent=' ', subsequent_indent=' ',
                                      break_on_hyphens=False)),
                           action='append')

tag_args = argparse.ArgumentParser(add_help=False)
tag_args.add_argument('--tag', metavar='TAG', dest='tags',
                      help=fill('Tag for the resulting execution; repeat as necessary,', width_adjustment=-24) + '\n' +
                      fill('e.g. "--tag tag1 --tag tag2"', width_adjustment=-24,
                           break_on_hyphens=False, initial_indent=' ', subsequent_indent=' '),
                      action='append')

contains_phi = argparse.ArgumentParser(add_help=False)
contains_phi.add_argument('--phi', dest='containsPHI', choices=["true", "false"],
                          help='If set to true, only projects that contain PHI data will be retrieved. ' +
                          'If set to false, only projects that do not contain PHI data will be retrieved.')

def _parse_dictionary_or_string_input(thing, arg_name):
    if thing.strip().startswith('{'):
        # expects a map, e.g of entry point to instance type or instance count
        try:
            return json.loads(thing)
        except ValueError:
            raise DXCLIError("Error while parsing JSON value for " + arg_name)
    else:
        return thing

def _parse_inst_type(thing):
    return _parse_dictionary_or_string_input(thing, "--instance-type")

def _parse_inst_count(thing):
    return _parse_dictionary_or_string_input(thing, "--instance-count")

def process_instance_type_arg(args, for_workflow=False):
    if args.instance_type:
        if for_workflow:
            args.stage_instance_types = {}
            new_inst_type_val = {}
            for inst_type_req in args.instance_type:
                if '=' in inst_type_req:
                    index_of_eql = inst_type_req.rfind('=')
                    args.stage_instance_types[inst_type_req[:index_of_eql]] = _parse_inst_type(
                        inst_type_req[index_of_eql + 1:]
                    )
                else:
                    new_inst_type_val = _parse_inst_type(inst_type_req)
            args.instance_type = new_inst_type_val
        elif not isinstance(args.instance_type, basestring):
            args.instance_type = _parse_inst_type(args.instance_type[-1])
        else:
            # is a string
            args.instance_type = _parse_inst_type(args.instance_type)

def process_instance_count_arg(args):
    if args.instance_count:
        # If --instance-count was used multiple times, the last one
        # from the list will override the previous ones
        if not isinstance(args.instance_count, basestring):
            args.instance_count = _parse_inst_count(args.instance_count[-1])
        else:
            # is a string
            args.instance_count = _parse_inst_count(args.instance_count)

def get_update_project_args(args):
    input_params = {}
    if args.name is not None:
        input_params["name"] = args.name
    if args.summary is not None:
        input_params["summary"] = args.summary
    if args.description is not None:
        input_params["description"] = args.description
    if args.protected is not None:
        input_params["protected"] = True if args.protected == 'true' else False
    if args.restricted is not None:
        input_params["restricted"] = True if args.restricted == 'true' else False
    if args.download_restricted is not None:
        input_params["downloadRestricted"] = True if args.download_restricted == 'true' else False
    if args.containsPHI is not None:
        input_params["containsPHI"] = True if args.containsPHI == 'true' else False
    if args.database_ui_view_only is not None:
        input_params["databaseUIViewOnly"] = True if args.database_ui_view_only == 'true' else False
    if args.bill_to is not None:
        input_params["billTo"] = args.bill_to
    if args.allowed_executables is not None:
        input_params['allowedExecutables'] = args.allowed_executables
    if args.unset_allowed_executables:
        input_params['allowedExecutables'] = None
    return input_params


def process_phi_param(args):
    if args.containsPHI is not None:
        if args.containsPHI == "true":
            args.containsPHI = True
        elif args.containsPHI == "false":
            args.containsPHI = False
