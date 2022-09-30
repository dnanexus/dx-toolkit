#!/usr/bin/env python
# coding: utf-8
#
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

from __future__ import print_function, unicode_literals, division, absolute_import

import os, sys, datetime, getpass, collections, re, json, argparse, copy, hashlib, io, time, subprocess, glob, logging, functools, platform
import shlex # respects quoted substrings when splitting

import requests
import csv

logging.basicConfig(level=logging.INFO)

from ..compat import (USING_PYTHON2, basestring, str, input, wrap_stdio_in_codecs, decode_command_line_args,
                      unwrap_stream, sys_encoding)

wrap_stdio_in_codecs()
decode_command_line_args()

import dxpy
from dxpy.scripts import dx_build_app
from dxpy import workflow_builder
from dxpy.exceptions import PermissionDenied, InvalidState, ResourceNotFound

from ..cli import try_call, prompt_for_yn, INTERACTIVE_CLI
from ..cli import workflow as workflow_cli
from ..cli.cp import cp
from ..cli.dataset_utilities import extract_dataset
from ..cli.download import (download_one_file, download_one_database_file, download)
from ..cli.parsers import (no_color_arg, delim_arg, env_args, stdout_args, all_arg, json_arg, parser_dataobject_args,
                           parser_single_dataobject_output_args, process_properties_args,
                           find_by_properties_and_tags_args, process_find_by_property_args, process_dataobject_args,
                           process_single_dataobject_output_args, find_executions_args, add_find_executions_search_gp,
                           set_env_from_args, extra_args, process_extra_args, DXParserError, exec_input_args,
                           instance_type_arg, process_instance_type_arg, process_instance_count_arg, get_update_project_args,
                           property_args, tag_args, contains_phi, process_phi_param)
from ..cli.exec_io import (ExecutableInputs, format_choices_or_suggestions)
from ..cli.org import (get_org_invite_args, add_membership, remove_membership, update_membership, new_org, update_org,
                       find_orgs, org_find_members, org_find_projects, org_find_apps)
from ..exceptions import (err_exit, DXError, DXCLIError, DXAPIError, network_exceptions, default_expected_exceptions,
                          format_exception)
from ..utils import warn, group_array_by_field, normalize_timedelta, normalize_time_input
from ..utils.batch_utils import (batch_run, batch_launch_args)

from ..app_categories import APP_CATEGORIES
from ..utils.printing import (CYAN, BLUE, YELLOW, GREEN, RED, WHITE, UNDERLINE, BOLD, ENDC, DNANEXUS_LOGO,
                              DNANEXUS_X, set_colors, set_delimiter, get_delimiter, DELIMITER, fill,
                              tty_rows, tty_cols, pager, format_find_results, nostderr)
from ..utils.pretty_print import format_tree, format_table
from ..utils.resolver import (clean_folder_path, pick, paginate_and_pick, is_hashid, is_data_obj_id, is_container_id, is_job_id,
                              is_analysis_id, get_last_pos_of_char, resolve_container_id_or_name, resolve_path,
                              resolve_existing_path, get_app_from_path, resolve_app, resolve_global_executable, get_exec_handler,
                              split_unescaped, ResolutionError, resolve_to_objects_or_project, is_project_explicit,
                              object_exists_in_project, is_jbor_str, parse_input_keyval)
from ..utils.completer import (path_completer, DXPathCompleter, DXAppCompleter, LocalCompleter,
                               ListCompleter, MultiCompleter)
from ..utils.describe import (print_data_obj_desc, print_desc, print_ls_desc, get_ls_l_desc, print_ls_l_header,
                              print_ls_l_desc, get_ls_l_desc_fields, get_io_desc, get_find_executions_string)
from ..system_requirements import SystemRequirementsDict

try:
    import colorama
    colorama.init()
except:
    pass

if '_ARGCOMPLETE' not in os.environ:
    try:
        # Hack: on some operating systems, like Mac, readline spews
        # escape codes into the output at import time if TERM is set to
        # xterm (or xterm-256color). This can be a problem if dx is
        # being used noninteractively (e.g. --json) and its output will
        # be redirected or parsed elsewhere.
        #
        # http://reinout.vanrees.org/weblog/2009/08/14/readline-invisible-character-hack.html
        old_term_setting = None
        if 'TERM' in os.environ and os.environ['TERM'].startswith('xterm'):
            old_term_setting = os.environ['TERM']
            os.environ['TERM'] = 'vt100'
        # Import pyreadline3 on Windows with Python >= 3.5
        if platform.system() == 'Windows' and  sys.version_info >= (3, 5):
            import pyreadline3 as readline
        else:
            try:
                # Import gnureadline if installed for macOS
                import gnureadline as readline
            except ImportError as e:
                import readline
        if old_term_setting:
            os.environ['TERM'] = old_term_setting

    except ImportError:
        if os.name != 'nt':
            print('Warning: readline module is not available, tab completion disabled', file=sys.stderr)

state = {"interactive": False,
         "colors": "auto",
         "delimiter": None,
         "currentproj": None}
parser_map = {}
parser_categories_sorted = ["all", "session", "fs", "data", "metadata", "workflow", "exec", "org", "other"]
parser_categories = {"all": {"desc": "\t\tAll commands",
                             "cmds": []},
                     "session": {"desc": "\tManage your login session",
                                 "cmds": []},
                     "fs": {"desc": "\t\tNavigate and organize your projects and files",
                            "cmds": []},
                     "data": {"desc": "\t\tView, download, and upload data",
                              "cmds": []},
                     "metadata": {"desc": "\tView and modify metadata for projects, data, and executions",
                                 "cmds": []},
                     "workflow": {"desc": "\tView and modify workflows",
                                  "cmds": []},
                     "exec": {"desc": "\t\tManage and run apps, applets, and workflows",
                              "cmds": []},
                     "org": {"desc": "\t\tAdminister and operate on orgs",
                             "cmds": []},
                     "other": {"desc": "\t\tMiscellaneous advanced utilities",
                               "cmds": []}}

class ResultCounter():
    def __init__(self):
        self.counter = 0

    def __call__(self):
        self.counter += 1
        return ('\n' if self.counter > 1 else '') + UNDERLINE() + 'Result ' + \
            str(self.counter) + ':' + ENDC()

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def get_json_from_stdin():
    user_json_str = input('Type JSON here> ')
    user_json = None
    try:
        user_json = json.loads(user_json_str)
    except ValueError:
        raise DXCLIError('Error: user input could not be parsed as JSON')
    return user_json

def set_cli_colors(args=argparse.Namespace()):
    if 'color' in args:
        state['colors'] = args.color
    if state['colors'] == 'auto':
        set_colors(sys.stdout.isatty())
    else:
        set_colors(state['colors'] == 'on')

def set_delim(args=argparse.Namespace()):
    if 'delimiter' in args:
        state['delimiter'] = args.delimiter
    else:
        state['delimiter'] = None
    set_delimiter(state['delimiter'])

# Loading command line arguments
args_list = sys.argv[1:]

# Loading other variables used for pretty-printing
if "LESS" in os.environ:
    os.environ["LESS"] = os.environ["LESS"] + " -RS"
else:
    os.environ["LESS"] = "-RS"

# This completer is for the command line in the shell (i.e., `dx sh`). It
# assumes the first word is always a subcommand and that if the first word is a
# subcommand with further subcommands, then the second word must be an
# appropriate sub-subcommand.
class DXCLICompleter():
    subcommands = {'find': ['data ', 'projects ', 'apps ', 'jobs ', 'executions ', 'analyses ', 'orgs ', 'org '],
                   'new': ['record ', 'project ', 'workflow ', 'org ', 'user '],
                   'add': ['developers ', 'users ', 'stage ', 'member '],
                   'remove': ['developers ', 'users ', 'stage ', 'member '],
                   'update': ['stage ', 'workflow ', 'org ', 'member ', 'project '],
                   'org': ['projects ', 'members ']}

    silent_commands = set(['export'])

    def __init__(self):
        self.commands = [subcmd + ' ' for subcmd in list(subparsers.choices.keys()) if subcmd not in self.silent_commands]
        self.matches = []
        self.text = None

    def get_command_matches(self, prefix):
        self.matches = [cmd for cmd in self.commands if cmd.startswith(prefix)]

    def get_subcommand_matches(self, command, prefix):
        if command in self.subcommands:
            self.matches = [command + ' ' + sub for sub in self.subcommands[command] if sub.startswith(prefix)]

    def get_matches(self, text, want_prefix=False):
        self.text = text
        space_pos = get_last_pos_of_char(' ', text)
        words = split_unescaped(' ', text)
        if len(words) > 0 and space_pos == len(text) - 1:
            words.append('')
        num_words = len(words)
        self.matches = []
        if num_words == 0:
            self.get_command_matches('')
        elif num_words == 1:
            self.get_command_matches(words[0])
        elif num_words == 2 and words[0] in self.subcommands:
            self.get_subcommand_matches(words[0], words[1])
        else:
            if words[0] == 'run':
                path_matches = path_completer(words[-1],
                                              classes=['applet', 'workflow'],
                                              visibility="visible")
            elif words[0] in ['cd', 'rmdir', 'mkdir', 'tree']:
                path_matches = path_completer(words[-1],
                                              expected='folder')
            elif words[0] in ['head', 'cat', 'download']:
                path_matches = path_completer(words[-1],
                                              classes=['file'])
            elif words[0] in ['ls', 'rm', 'mv', 'cp']:
                path_matches = path_completer(words[-1])
            elif words[0] in ['get_details', 'set_details', 'set_visibility', 'add_types', 'remove_types', 'close', 'get']:
                path_matches = path_completer(words[-1])
            elif words[0] in ['describe', 'rename', 'set_properties', 'unset_properties']:
                path_matches = path_completer(words[-1], include_current_proj=True)
            elif words[0] in ['rmproject', 'invite']:
                path_matches = path_completer(words[-1], expected='project', include_current_proj=True)
            else:
                path_matches = []

            if want_prefix:
                self.matches = [text[:space_pos + 1] + match for match in path_matches]
            else:
                self.matches = path_matches

            # Also find app name matches and append to
            # self.matches, preferably a list of installed apps
            if words[0] in ['run', 'install', 'uninstall']:
                try:
                    initial_results = list(dxpy.find_apps(describe={"fields": {"name": True,
                                                                               "installed": True}}))
                    if words[0] in ['run', 'uninstall']:
                        filtered_results = [result for result in initial_results if
                                            result['describe']['installed']]
                    else:
                        filtered_results = [result for result in initial_results if
                                            not result['describe']['installed']]
                    app_names = [result['describe']['name'] for result in filtered_results]
                    app_matches = [name for name in app_names if name.startswith(words[-1])]
                    if want_prefix:
                        self.matches += [text[:space_pos + 1] + match for match in app_matches]
                    else:
                        self.matches += app_matches
                except:
                    pass

        return self.matches

    def complete(self, text, state):
        if state == 0:
            self.get_matches(text, want_prefix=True)

        if state < len(self.matches):
            return self.matches[state]
        else:
            return None

def login(args):
    if not state['interactive']:
        args.save = True

    default_authserver = 'https://auth.dnanexus.com'
    using_default = False

    if args.auth_token and not args.token:
        args.token = args.auth_token
        args.auth_token = None

    # API server should have already been set up if --host or one of
    # the --special-host flags has been set.
    if args.token is None:
        if (args.host is None) != (args.port is None):
            err_exit('Error: Only one of --host and --port were provided; provide either both or neither of the values', 2)
        authserver = dxpy.get_auth_server_name(args.host, args.port, args.protocol)

        using_default = authserver == default_authserver

        def get_token(**data):
            return dxpy.DXHTTPRequest(authserver+"/system/newAuthToken", data,
                                      prepend_srv=False, auth=None, always_retry=True)

        def get_credentials(reuse=None, get_otp=False):
            if reuse:
                username, password = reuse
            else:
                username = None
                while not username:
                    if 'DX_USERNAME' in os.environ:
                        username = input('Username [' + os.environ['DX_USERNAME'] + ']: ') or os.environ['DX_USERNAME']
                    else:
                        username = input('Username: ')
                dxpy.config.write("DX_USERNAME", username)
                with unwrap_stream('stdin'):
                    password = getpass.getpass()

            otp = input('Verification code: ') if get_otp else None
            return dict(username=username, password=password, otp=otp)

        print('Acquiring credentials from ' + authserver)
        attempt, using_otp, reuse = 1, False, None
        while attempt <= 3:
            try:
                credentials = get_credentials(reuse=reuse, get_otp=using_otp)
                token_res = get_token(expires=normalize_time_input(args.timeout, future=True, default_unit='s'),
                                      **credentials)
                break
            except (KeyboardInterrupt, EOFError):
                err_exit()
            except dxpy.DXAPIError as e:
                if e.name == 'OTPRequiredError':
                    using_otp = True
                    reuse = (credentials['username'], credentials['password'])
                    continue
                elif e.name in ('UsernameOrPasswordError', 'OTPMismatchError'):
                    if attempt < 3:
                        if e.name == 'UsernameOrPasswordError':
                            warn("Incorrect username and/or password")
                        else:
                            warn("Incorrect verification code")
                        attempt += 1
                        continue
                    else:
                        err_exit("Incorrect username and/or password", arg_parser=parser)
                else:
                    err_exit("Login error: {}".format(e), arg_parser=parser)
            except Exception as e:
                err_exit("Login error: {}".format(e), arg_parser=parser)

        sec_context=json.dumps({'auth_token': token_res["access_token"], 'auth_token_type': token_res["token_type"]})

        if using_default:
            set_api(dxpy.DEFAULT_APISERVER_PROTOCOL, dxpy.DEFAULT_APISERVER_HOST, dxpy.DEFAULT_APISERVER_PORT, args.save)
    else:
        sec_context = '{"auth_token":"' + args.token + '","auth_token_type":"Bearer"}'
        # Ensure correct API server
        if args.host is None:
            set_api(dxpy.DEFAULT_APISERVER_PROTOCOL, dxpy.DEFAULT_APISERVER_HOST, dxpy.DEFAULT_APISERVER_PORT, args.save)
            using_default = True

    os.environ['DX_SECURITY_CONTEXT'] = sec_context
    dxpy.set_security_context(json.loads(sec_context))
    if args.save:
        dxpy.config.write("DX_SECURITY_CONTEXT", sec_context)

    # If login via token, obtain current username from auth server.
    if args.token is not None:
        host, port = None, None
        if dxpy.APISERVER_HOST not in ['api.dnanexus.com', 'stagingapi.dnanexus.com']:
            host, port = args.host, args.port
        try:
            dxpy.config.write("DX_USERNAME", dxpy.user_info(host, port)['username'])
        except DXError as details:
            # Consider failure to obtain username to be a non-fatal error.
            print("Could not obtain username from auth server. Consider setting both --host and --port.", file=sys.stderr)
            print(fill(str(details)), file=sys.stderr)

    if using_default or args.staging:
        try:
            greeting = dxpy.api.system_greet({'client': 'dxclient', 'version': 'v'+dxpy.TOOLKIT_VERSION})
            if greeting.get('messages'):
                print(BOLD("New messages from ") + DNANEXUS_LOGO())
                for message in greeting['messages']:
                    print(BOLD("Date:    ") + datetime.datetime.fromtimestamp(message['date']//1000).ctime())
                    print(BOLD("Subject: ") + fill(message['title'], subsequent_indent=' '*9))
                    body = message['body'].splitlines()
                    if len(body) > 0:
                        print(BOLD("Message: ") + body[0])
                        for line in body[1:]:
                            print(' '*9 + line)
        except Exception as e:
            warn("Error while retrieving greet data: {}".format(e))

    args.current = False
    args.name = None
    args.level = 'CONTRIBUTE'
    args.public = False

    if args.host is not None and not args.staging and not using_default:
        setenv(args)
    elif args.projects:
        pick_and_set_project(args)

    if args.save and not args.token:
        msg = "You are now logged in. Your credentials are stored in {conf_dir} and will expire in {timeout}. {tip}"
        tip = "Use " + BOLD("dx login --timeout") + " to control the expiration date, or " + BOLD("dx logout") + \
              " to end this session."
        timeout = datetime.timedelta(seconds=normalize_time_input(args.timeout, default_unit='s') // 1000)
        print(fill(msg.format(conf_dir=dxpy.config.get_user_conf_dir(),
                              timeout=timeout,
                              tip=tip)))

def logout(args):
    if dxpy.AUTH_HELPER is not None:
        authserver = dxpy.get_auth_server_name(args.host, args.port, args.protocol)
        print("Deleting credentials from {}...".format(authserver))
        token = dxpy.AUTH_HELPER.security_context["auth_token"]
        try:
            if not USING_PYTHON2:
                # python 3 requires conversion to bytes before hashing
                token = token.encode(sys_encoding)
            token_sig = hashlib.sha256(token).hexdigest()
            response = dxpy.DXHTTPRequest(authserver + "/system/destroyAuthToken",
                                          dict(tokenSignature=token_sig),
                                          prepend_srv=False,
                                          max_retries=1)
            print("Deleted token with signature", token_sig)
        except dxpy.DXAPIError as e:
            print(format_exception(e))
        except:
            err_exit()
        if state["interactive"]:
            dxpy.AUTH_HELPER = None
        else:
            dxpy.config.write("DX_SECURITY_CONTEXT", None)

def set_api(protocol, host, port, write):
    dxpy.config.update(DX_APISERVER_PROTOCOL=protocol,
                       DX_APISERVER_HOST=host,
                       DX_APISERVER_PORT=port)
    if write:
        dxpy.config.save()

def set_project(project, write, name=None):
    if dxpy.JOB_ID is None:
        dxpy.config["DX_PROJECT_CONTEXT_ID"] = project
        dxpy.config["DX_PROJECT_CONTEXT_NAME"] = name
    else:
        dxpy.config["DX_WORKSPACE_ID"] = project
    if write:
        dxpy.config.save()
    dxpy.set_workspace_id(project)

def set_wd(folder, write):
    dxpy.config.update(DX_CLI_WD=folder)
    if write:
        dxpy.config.save()

# Will raise KeyboardInterrupt, EOFError
def prompt_for_env_var(prompt_str, env_var_str):
    prompt = prompt_str
    default = None
    if env_var_str in os.environ:
        default = os.environ[env_var_str]
        prompt += ' [' + default + ']: '
    else:
        prompt += ': '
    while True:
        value = input(prompt)
        if value != '':
            return value
        elif default is not None:
            return default


def pick_and_set_project(args):
    try:
        result_generator = dxpy.find_projects(describe=True,
                                              name=args.name, name_mode='glob',
                                              level=('VIEW' if args.public else args.level),
                                              explicit_perms=(not args.public if not args.public else None),
                                              public=(args.public if args.public else None),
                                              first_page_size=10)
    except:
        err_exit('Error while listing available projects')
    any_results = False
    first_pass = True
    while True:
        results = []
        for _ in range(10):
            try:
                retval = next(result_generator, None)
            except:
                err_exit('Error while listing available projects')
            if retval is None:
                break
            results.append(retval)
            any_results = True
        if not any_results:
            parser.exit(0, '\n' + fill("No projects to choose from.  You can create one with the command " +
                                       BOLD("dx new project") + ".  To pick from projects for which you only have " +
                                       " VIEW permissions, use " + BOLD("dx select --level VIEW") + " or " +
                                       BOLD("dx select --public") + ".") + '\n')
        elif len(results) == 0:
            err_exit('No projects left to choose from.', 3)

        if first_pass:
            if not args.public and args.level == "CONTRIBUTE":
                print('')
                print(fill("Note: Use " + BOLD("dx select --level VIEW") + " or " + BOLD("dx select --public") +
                           " to select from projects for which you only have VIEW permissions."))
            first_pass = False

        project_ids = [result['id'] for result in results]

        # Eliminate current default if it is not a found project
        try:
            default = project_ids.index(dxpy.WORKSPACE_ID)
        except:
            default = None

        print("")
        if args.public:
            print("Available public projects:")
        else:
            print("Available projects ({level} or higher):".format(level=args.level))
        choice = try_call(pick,
                          [result['describe']['name'] + ' (' + result['level'] + ')' for result in results],
                          default,
                          more_choices=(len(results) == 10))
        if choice == 'm':
            continue
        else:
            print('Setting current project to: ' + BOLD(results[choice]['describe']['name']))
            set_project(project_ids[choice], not state['interactive'] or args.save, name=results[choice]['describe']['name'])
            state['currentproj'] = results[choice]['describe']['name']
            set_wd('/', not state['interactive'] or args.save)
            return

def whoami(args):
    if dxpy.AUTH_HELPER is None:
        err_exit('You are not logged in; run "dx login" to obtain a token.', 3)
    user_id = dxpy.whoami()
    if args.user_id:
        print(user_id)
    else:
        print(dxpy.api.user_describe(user_id)['handle'])

def setenv(args):
    if not state['interactive']:
        args.save = True
    if args.current:
        dxpy.config.save()
    else:
        try:
            api_protocol = prompt_for_env_var('API server protocol (choose "http" or "https")', 'DX_APISERVER_PROTOCOL')
            api_host = prompt_for_env_var('API server host', 'DX_APISERVER_HOST')
            api_port = prompt_for_env_var('API server port', 'DX_APISERVER_PORT')
            set_api(api_protocol, api_host, api_port, args.save)
        except:
            raise DXCLIError("Error setting up API variables")

    if args.projects:
        args.name = None
        args.public = False
        args.current = False
        args.level = 'CONTRIBUTE'
        pick_and_set_project(args)

def clearenv(args):
    if args.interactive:
        print("The clearenv command is not available in the interactive shell")
        return
    dxpy.config.clear(reset=args.reset)

def env(args):
    if args.bash:
        if dxpy.AUTH_HELPER is not None:
            print("export DX_SECURITY_CONTEXT='" + json.dumps(dxpy.AUTH_HELPER.security_context) + "'")
        if dxpy.APISERVER_PROTOCOL is not None:
            print("export DX_APISERVER_PROTOCOL=" + dxpy.APISERVER_PROTOCOL)
        if dxpy.APISERVER_HOST is not None:
            print("export DX_APISERVER_HOST=" + dxpy.APISERVER_HOST)
        if dxpy.APISERVER_PORT is not None:
            print("export DX_APISERVER_PORT=" + dxpy.APISERVER_PORT)
        if dxpy.WORKSPACE_ID is not None:
            print("export DX_PROJECT_CONTEXT_ID=" + dxpy.WORKSPACE_ID)
    elif args.dx_flags:
        flags_str = ''
        if dxpy.AUTH_HELPER is not None:
            token = dxpy.AUTH_HELPER.security_context.get('auth_token', None)
            if token is not None:
                flags_str += ' --auth-token ' + token
        if dxpy.APISERVER_PROTOCOL is not None:
            flags_str += ' --apiserver-protocol ' + dxpy.APISERVER_PROTOCOL
        if dxpy.APISERVER_HOST is not None:
            flags_str += ' --apiserver-host ' + dxpy.APISERVER_HOST
        if dxpy.APISERVER_PORT is not None:
            flags_str += ' --apiserver-port ' + dxpy.APISERVER_PORT
        if dxpy.WORKSPACE_ID is not None:
            flags_str += ' --project-context-id ' + dxpy.WORKSPACE_ID
        print(flags_str)
    else:
        if dxpy.AUTH_HELPER is not None:
            print("Auth token used\t\t" + dxpy.AUTH_HELPER.security_context.get("auth_token", "none"))
        print("API server protocol\t" + dxpy.APISERVER_PROTOCOL)
        print("API server host\t\t" + dxpy.APISERVER_HOST)
        print("API server port\t\t" + dxpy.APISERVER_PORT)
        print("Current workspace\t" + str(dxpy.WORKSPACE_ID))
        if "DX_PROJECT_CONTEXT_NAME" in os.environ:
            print('Current workspace name\t"{n}"'.format(n=dxpy.config.get("DX_PROJECT_CONTEXT_NAME")))
        print("Current folder\t\t" + dxpy.config.get("DX_CLI_WD", "None"))
        print("Current user\t\t" + str(os.environ.get("DX_USERNAME")))

def get_pwd():
    pwd_str = None
    if dxpy.WORKSPACE_ID is not None:
        if state['currentproj'] is None:
            try:
                proj_name = dxpy.api.project_describe(dxpy.WORKSPACE_ID)['name']
                state['currentproj'] = proj_name
            except:
                pass
    if state['currentproj'] is not None:
        pwd_str = state['currentproj'] + ':' + dxpy.config.get('DX_CLI_WD', '/')
    return pwd_str

def pwd(args):
    pwd_str = get_pwd()
    if pwd_str is not None:
        print(pwd_str)
    else:
        err_exit('Current project is not set', 3)

def api(args):
    json_input = json.loads(args.input_json)
    if args.input is not None:
        with (sys.stdin if args.input == '-' else open(args.input, 'r')) as fd:
            data = fd.read()
            try:
                json_input = json.loads(data)
            except ValueError:
                err_exit('Error: file contents could not be parsed as JSON', 3)
    resp = None
    try:
        resp = dxpy.DXHTTPRequest('/' + args.resource + '/' + args.method,
                                  json_input)
    except:
        err_exit()
    try:
        print(json.dumps(resp, indent=4))
    except ValueError:
        err_exit('Error: server response could not be parsed as JSON', 3)

def invite(args):
    # If --project is a valid project (ID or name), then appending ":"
    # should not hurt the path resolution.
    if ':' not in args.project:
        args.project += ':'
    project, _none, _none = try_call(resolve_existing_path,
                                     args.project, 'project')
    if args.invitee != 'PUBLIC' and '-' not in args.invitee and '@' not in args.invitee:
        args.invitee = 'user-' + args.invitee.lower()
    project_invite_input = {"invitee": args.invitee, "level": args.level}
    if not args.send_email:
        project_invite_input["suppressEmailNotification"] = not args.send_email
    try:
        resp = dxpy.api.project_invite(project, project_invite_input)
    except:
        err_exit()
    print('Invited ' + args.invitee + ' to ' + project + ' (' + resp['state'] + ')')

def uninvite(args):
    # If --project is a valid project (ID or name), then appending ":"
    # should not hurt the path resolution.
    if ':' not in args.project:
        args.project += ':'
    project, _none, _none = try_call(resolve_existing_path,
                                     args.project, 'project')
    if args.entity != 'PUBLIC' and '-' not in args.entity:
        args.entity = 'user-' + args.entity.lower()
    try:
        dxpy.api.project_decrease_permissions(project, {args.entity: None})
    except:
        err_exit()
    print('Uninvited ' + args.entity + ' from ' + project)

def select(args):
    if args.project is not None:
        if get_last_pos_of_char(':', args.project) != -1:
            args.path = args.project
        else:
            args.path = args.project + ':'
        cd(args)
        print("Selected project", split_unescaped(":", args.project)[0].replace("\\:", ":"))
    else:
        pick_and_set_project(args)

def cd(args):
    # entity_result should be None because expected='folder'
    project, folderpath = try_call(resolve_existing_path, args.path, 'folder')[:2]

    if project is not None:
        project_name = try_call(dxpy.get_handler(project).describe)['name']

        # It is obvious what the project is
        if project != dxpy.WORKSPACE_ID or 'DX_PROJECT_CONTEXT_NAME' not in os.environ:
            # Cache ID and name if necessary
            set_project(project, not state['interactive'], name=project_name)
            state['currentproj'] = project_name
    else:
        err_exit('Error: No current project was given', 3)

    # TODO: attempt to add caching later if it's an issue
    # if project in cached_project_paths and folderpath in cached_project_paths[project]:
    #     set_wd(folderpath, not interactive)

    try:
        dxproj = dxpy.get_handler(dxpy.WORKSPACE_ID)
        dxproj.list_folder(folder=folderpath, only='folders')
    except:
        err_exit(fill(folderpath + ': No such file or directory found in project ' + dxpy.WORKSPACE_ID), 3)

    set_wd(folderpath, not state['interactive'])

def cmp_names(x):
    return x['describe']['name'].lower()

def ls(args):
    project, folderpath, entity_results = try_call(resolve_existing_path, # TODO: this needs to honor "ls -a" (all) (args.obj/args.folders/args.full)
                                                   args.path,
                                                   ask_to_resolve=False)

    if project is None:
        err_exit('Current project must be set or specified before any data can be listed', 3)
    dxproj = dxpy.get_handler(project)
    only = ""
    if args.obj and not args.folders and not args.full:
        only = "objects"
    elif not args.obj and args.folders and not args.full:
        only = "folders"
    else:
        only = "all"

    resp = None
    if entity_results is None:
        try:
            # Request the minimal set of describe fields possible
            if args.brief:
                describe_input = dict(fields={'id': True, 'name': True})
            elif args.verbose:
                describe_input = dict(fields=get_ls_l_desc_fields())
            else:
                describe_input = dict(fields={'id': True, 'class': True, 'name': True})
            resp = dxproj.list_folder(folder=folderpath,
                                      describe=describe_input,
                                      only=only,
                                      includeHidden=args.all)

            # Listing the folder was successful

            if args.verbose:
                print(UNDERLINE('Project:') + ' ' + dxproj.describe()['name'] + ' (' + project + ')')
                print(UNDERLINE('Folder :') + ' ' + folderpath)

            if not args.obj:
                folders_to_print = ['/.', '/..'] if args.all else []
                folders_to_print += resp['folders']
                for folder in folders_to_print:
                    if args.full:
                        print(BOLD() + BLUE() + folder + ENDC())
                    else:
                        print(BOLD() + BLUE() + os.path.basename(folder) + '/' + ENDC())
            if not args.folders:
                resp["objects"] = sorted(resp["objects"], key=cmp_names)
                if args.verbose:
                    if len(resp['objects']) > 0:
                        print_ls_l_header()
                    else:
                        print("No data objects found in the folder")
                if not args.brief and not args.verbose:
                    name_counts = collections.Counter(obj['describe']['name'] for obj in resp['objects'])
                for obj in resp['objects']:
                    if args.brief:
                        print(obj['id'])
                    elif args.verbose:
                        print_ls_l_desc(obj['describe'], include_project=False)
                    else:
                        print_ls_desc(obj['describe'], print_id=True if name_counts[obj['describe']['name']] > 1 else False)
        except:
            err_exit()
    else:
        # We have results to describe
        name_counts = collections.Counter(obj['describe']['name'] for obj in entity_results)
        for result in entity_results:
            # TODO: Figure out the right way to reason when to hide hidden files:
            # if result['describe']['hidden'] and not args.all:
            #     continue
            if result['describe']['project'] == project:
                if args.brief:
                    print(result['id'])
                elif args.verbose:
                    print_ls_l_desc(result['describe'], include_project=False)
                else:
                    print_ls_desc(result['describe'], print_id=True if name_counts[result['describe']['name']] > 1 else False)

def mkdir(args):
    had_error = False
    for path in args.paths:
        # Resolve the path and add it to the list
        try:
            project, folderpath, _none = resolve_path(path, expected='folder')
        except ResolutionError as details:
            print(fill('Could not resolve "' + path + '": ' + str(details)))
            had_error = True
            continue
        if project is None:
            print(fill('Could not resolve the project of "' + path + '"'))
        try:
            dxpy.api.project_new_folder(project, {"folder": folderpath, "parents": args.parents})
        except Exception as details:
            print("Error while creating " + folderpath + " in " + project)
            print("  " + str(details))
            had_error = True
    if had_error:
        err_exit('', 3)

def rmdir(args):
    had_error = False
    for path in args.paths:
        try:
            project, folderpath, _none = resolve_path(path, expected='folder')
        except ResolutionError as details:
            print(fill('Could not resolve "' + path + '": ' + str(details)))
            had_error = True
            continue
        if project is None:
            print(fill('Could not resolve the project of "' + path + '"'))
        try:
            completed = False
            while not completed:
                resp = dxpy.api.project_remove_folder(project, {"folder": folderpath,
                                                                "partial": True})
                if 'completed' not in resp:
                    raise DXError('Error removing folder')
                completed = resp['completed']
        except Exception as details:
            print("Error while removing " + folderpath + " in " + project)
            print("  " + str(details))
            had_error = True
    if had_error:
        err_exit('', 3)




def rm(args):
    had_error = False
    projects = {}

    # Caution user when performing a recursive removal before any removal operation takes place
    if args.recursive and not args.force:
        for path in args.paths:
            try:
                with nostderr():
                    project, folderpath, entity_results = resolve_existing_path(path, allow_mult=True, all_mult=args.all)
                if folderpath == '/' and entity_results is None:
                    print("")
                    print("===========================================================================")
                    print("*     {}: Recursive deletion will remove all files in project!     *".format(RED("RED ALERT")))
                    print("*                                                                         *")
                    print("*                  {}                       *".format(project))
                    print("*                                                                         *")
                    print("*   Please issue 'dx rm -r --force' if you are sure you want to do this.  *")
                    print("===========================================================================")
                    print("")

                    err_exit('', 3)
            except Exception as details:
                continue


    for path in args.paths:
        # Resolve the path and add it to the list
        try:
            project, folderpath, entity_results = resolve_existing_path(path, allow_mult=True, all_mult=args.all)
        except Exception as details:
            print(fill('Could not resolve "' + path + '": ' + str(details)))
            had_error = True
            continue
        if project is None:
            had_error = True
            print(fill('Could not resolve "' + path + '" to a project'))
            continue
        if project not in projects:
            projects[project] = {"folders": [], "objects": []}
        if entity_results is None:
            if folderpath is not None:
                if not args.recursive:
                    print(fill('Did not find "' + path + '" as a data object; if it is a folder, cannot remove it without setting the "-r" flag'))
                    had_error = True
                    continue
                else:
                    projects[project]['folders'].append(folderpath)
            else:
                print(fill('Path ' + path + ' resolved to a project; cannot remove a project using "rm"'))
                had_error = True
                continue
        else:
            projects[project]['objects'] += [result['id'] for result in entity_results]

    for project in projects:
        for folder in projects[project]['folders']:
            try:
                # set force as true so the underlying API requests are idempotent
                completed = False
                while not completed:
                    resp = dxpy.api.project_remove_folder(project,
                                                          {"folder": folder, "recurse": True,
                                                           "force": True, "partial": True},
                                                          always_retry=True)
                    if 'completed' not in resp:
                        raise DXError('Error removing folder')
                    completed = resp['completed']
            except Exception as details:
                print("Error while removing " + folder + " from " + project)
                print("  " + str(details))
                had_error = True
        try:
            # set force as true so the underlying API requests are idempotent
            dxpy.api.project_remove_objects(project,
                                            {"objects": projects[project]['objects'], "force": True},
                                            always_retry=True)
        except Exception as details:
            print("Error while removing " + json.dumps(projects[project]['objects']) + " from " + project)
            print("  " + str(details))
            had_error = True
    if had_error:
        # TODO: 'dx rm' and related commands should separate out user error exceptions and internal code exceptions
        err_exit('', 3)

def rmproject(args):
    had_error = False
    for project in args.projects:
        # Be forgiving if they offer an extraneous colon
        substrings = split_unescaped(':', project)
        if len(substrings) > 1 or (len(substrings) == 1 and project[0] == ':'):
            print(fill('Unable to remove "' + project + '": a nonempty string was found to the right of an unescaped colon'))
            had_error = True
            continue
        if len(substrings) == 0:
            if project[0] == ':':
                print(fill('Unable to remove ":": to remove the current project, use its name or ID'))
                had_error = True
                continue
        proj_id = try_call(resolve_container_id_or_name, substrings[0])
        if proj_id is None:
            print(fill('Unable to remove "' + project + '": could not resolve to a project ID'))
            had_error = True
            continue
        try:
            proj_desc = dxpy.api.project_describe(proj_id)
            if args.confirm:
                value = input(fill('About to delete project "' + proj_desc['name'] + '" (' + proj_id + ')') + '\nPlease confirm [y/n]: ')
                if len(value) == 0 or value.lower()[0] != 'y':
                    had_error = True
                    print(fill('Aborting deletion of project "' + proj_desc['name'] + '"'))
                    continue
            try:
                dxpy.api.project_destroy(proj_id, {"terminateJobs": not args.confirm})
            except dxpy.DXAPIError as apierror:
                if apierror.name == 'InvalidState':
                    value = input(fill('WARNING: there are still unfinished jobs in the project.') + '\nTerminate all jobs and delete the project? [y/n]: ')
                    if len(value) == 0 or value.lower()[0] != 'y':
                        had_error = True
                        print(fill('Aborting deletion of project "' + proj_desc['name'] + '"'))
                        continue
                    dxpy.api.project_destroy(proj_id, {"terminateJobs": True})
                else:
                    raise apierror
            if not args.quiet:
                print(fill('Successfully deleted project "' + proj_desc['name'] + '"'))
        except EOFError:
            err_exit('', 3)
        except KeyboardInterrupt:
            err_exit('', 3)
        except Exception as details:
            print(fill('Was unable to remove ' + project + ', ' + str(details)))
            had_error = True
    if had_error:
        err_exit('', 3)

# ONLY for within the SAME project.  Will exit fatally otherwise.
def mv(args):
    dest_proj, dest_path, _none = try_call(resolve_path, args.destination, expected='folder')
    try:
        if dest_path is None:
            raise ValueError()
        dx_dest = dxpy.get_handler(dest_proj)
        dx_dest.list_folder(folder=dest_path, only='folders')
    except:
        if dest_path is None:
            err_exit('Cannot move to a hash ID', 3)
        # Destination folder path is new => renaming
        if len(args.sources) != 1:
            # Can't rename more than one object
            err_exit('The destination folder does not exist', 3)
        last_slash_pos = get_last_pos_of_char('/', dest_path)
        if last_slash_pos == 0:
            dest_folder = '/'
        else:
            dest_folder = dest_path[:last_slash_pos]
        dest_name = dest_path[last_slash_pos + 1:].replace('\/', '/')
        try:
            dx_dest.list_folder(folder=dest_folder, only='folders')
        except:
            err_exit('The destination folder does not exist', 3)

        # Either rename the data object or rename the folder
        src_proj, src_path, src_results = try_call(resolve_existing_path,
                                                   args.sources[0],
                                                   allow_mult=True, all_mult=args.all)

        if src_proj != dest_proj:
            err_exit(fill('Error: Using "mv" for moving something from one project to another is unsupported.'), 3)

        if src_results is None:
            if src_path == '/':
                err_exit(fill('Cannot rename root folder; to rename the project, please use the "dx rename" subcommand.'), 3)
            try:
                dxpy.api.project_rename_folder(src_proj, {"folder": src_path, "newpath": dest_path})
                return
            except:
                err_exit()
        else:
            try:
                if src_results[0]['describe']['folder'] != dest_folder:
                    dxpy.api.project_move(src_proj,
                                          {"objects": [result['id'] for result in src_results],
                                           "destination": dest_folder})
                for result in src_results:
                    dxpy.DXHTTPRequest('/' + result['id'] + '/rename',
                                       {"project": src_proj,
                                        "name": dest_name})
                return
            except:
                err_exit()

    if len(args.sources) == 0:
        err_exit('No sources provided to move', 3)
    src_objects = []
    src_folders = []
    for source in args.sources:
        src_proj, src_folderpath, src_results = try_call(resolve_existing_path,
                                                         source,
                                                         allow_mult=True, all_mult=args.all)
        if src_proj != dest_proj:
            err_exit(fill('Using "mv" for moving something from one project to another is unsupported.  Please use "cp" and "rm" instead.'), 3)

        if src_results is None:
            src_folders.append(src_folderpath)
        else:
            src_objects += [result['id'] for result in src_results]
    try:
        dxpy.api.project_move(src_proj,
                              {"objects": src_objects,
                               "folders": src_folders,
                               "destination": dest_path})
    except:
        err_exit()


def tree(args):
    project, folderpath, _none = try_call(resolve_existing_path, args.path,
                                          expected='folder')

    if project is None:
        err_exit(fill('Current project must be set or specified before any data can be listed'), 3)
    dxproj = dxpy.get_handler(project)

    tree = collections.OrderedDict()
    try:
        folders = [folder for folder in dxproj.describe(input_params={"folders": True})['folders']
                   if folder.startswith((folderpath + '/') if folderpath != '/' else '/')]
        folders = [ folder[len(folderpath):] for folder in folders ]
        for folder in folders:
            subtree = tree
            for path_element in folder.split("/"):
                if path_element == "":
                    continue
                path_element_desc = BOLD() + BLUE() + path_element + ENDC()
                subtree.setdefault(path_element_desc, collections.OrderedDict())
                subtree = subtree[path_element_desc]

        for item in sorted(dxpy.find_data_objects(project=project, folder=folderpath,
                                                  recurse=True, describe=dict(fields=get_ls_l_desc_fields())),
                           key=cmp_names):
            subtree = tree
            for path_element in item['describe']['folder'][len(folderpath):].split("/"):
                if path_element == "":
                    continue
                path_element_desc = BOLD() + BLUE() + path_element + ENDC()
                subtree = subtree[path_element_desc]
            if args.long:
                item_desc = get_ls_l_desc(item['describe'])
            else:
                item_desc = item['describe']['name']
                if item['describe']['class'] in ['applet', 'workflow']:
                    item_desc = BOLD() + GREEN() + item_desc + ENDC()
            subtree[item_desc] = None

        print(format_tree(tree, root=(BOLD() + BLUE() + args.path + ENDC())))
    except:
        err_exit()

def describe(args):

    def describe_global_executable(json_output, args, exec_type):
        """
        Describes a global executable, i.e. either app or global workflow
        depending on the provided exec_type. Appends the result to json_output.
        Returns True if any matches were found
        """
        assert(exec_type in ('app', 'globalworkflow'))
        found_match = False

        try:
            if exec_type == 'app':
                desc = dxpy.api.app_describe(args.path)
            else:
                desc = dxpy.api.global_workflow_describe(args.path)
                desc = dxpy.append_underlying_workflow_describe(desc)
            if args.json:
                json_output.append(desc)
            elif args.name:
                print(desc['name'])
            else:
                print(get_result_str())
                print_desc(desc, args.verbose)
            found_match = True
        except dxpy.DXAPIError as details:
            if details.code != requests.codes.not_found:
                raise
        return found_match

    def find_global_executable(json_output, args):
        """
        Makes a find_apps API call and, if no matches are found, a find_global_workflows call.
        Since these two objects share namespace, either app or a global workflow will be
        found, not both. The results are appended to json_output and printed to STDOUT.
        """

        def append_to_output_json_and_print(result):
            if args.json:
                json_output.append(result['describe'])
            elif args.name:
                print(result['describe']['name'])
            else:
                print(get_result_str())
                print_desc(result['describe'], args.verbose)

        found_match = False
        for result in dxpy.find_apps(name=args.path, describe=True):
            append_to_output_json_and_print(result)
            found_match = True
        if not found_match:
            for result in dxpy.find_global_workflows(name=args.path, describe=True):
                result['describe'] = dxpy.append_underlying_workflow_describe(result['describe'])
                append_to_output_json_and_print(result)
                found_match = True
        return found_match

    try:
        if len(args.path) == 0:
            raise DXCLIError('Must provide a nonempty string to be described')

        # Attempt to resolve name
        # First, if it looks like a hash id, do that.
        json_input = {}
        json_input["properties"] = True
        if args.name and (args.verbose or args.details or args.json):
            raise DXCLIError('Cannot request --name in addition to one of --verbose, --details, or --json')
        # Always retrieve details too (just maybe don't render them)
        json_input["details"] = True
        if is_data_obj_id(args.path):
            # Should prefer the current project's version if possible
            if dxpy.WORKSPACE_ID is not None:
                try:
                    # But only put it in the JSON if you still have
                    # access.
                    dxpy.api.project_list_folder(dxpy.WORKSPACE_ID)
                    json_input['project'] = dxpy.WORKSPACE_ID
                except dxpy.DXAPIError as details:
                    if details.code != requests.codes.not_found:
                        raise

        if is_job_id(args.path):
            if args.verbose:
                json_input['defaultFields'] = True
                json_input['fields'] = {'internetUsageIPs': True}

        # Otherwise, attempt to look for it as a data object or
        # execution
        try:
            project, _folderpath, entity_results = resolve_existing_path(args.path,
                                                                         expected='entity',
                                                                         ask_to_resolve=False,
                                                                         describe=json_input)
        except ResolutionError as details:
            # PermissionDenied or InvalidAuthentication
            if str(details).endswith('code 401'):
                # Surface permissions-related errors here (for data
                # objects, jobs, and analyses). Other types of errors
                # may be recoverable below.
                #
                # TODO: better way of obtaining the response code when
                # the exception corresponds to an API error
                raise DXCLIError(str(details))
            project, entity_results = None, None

        found_match = False

        json_output = []

        get_result_str = ResultCounter()

        # Could be a project
        json_input = {}
        json_input['properties'] = True
        if args.verbose:
            json_input["permissions"] = True
            json_input['appCaches'] = True
        if entity_results is None:
            if args.path[-1] == ':' and project is not None:
                # It is the project.
                try:
                    desc = dxpy.api.project_describe(project, json_input)
                    found_match = True
                    if args.json:
                        json_output.append(desc)
                    elif args.name:
                        print(desc['name'])
                    else:
                        print(get_result_str())
                        print_desc(desc, args.verbose)
                except dxpy.DXAPIError as details:
                    if details.code != requests.codes.not_found:
                        raise
            elif is_container_id(args.path):
                try:
                    desc = dxpy.api.project_describe(args.path, json_input)
                    found_match = True
                    if args.json:
                        json_output.append(desc)
                    elif args.name:
                        print(desc['name'])
                    else:
                        print(get_result_str())
                        print_desc(desc, args.verbose)
                except dxpy.DXAPIError as details:
                    if details.code != requests.codes.not_found:
                        raise

        # Found data object or is an id
        if entity_results is not None:
            if len(entity_results) > 0:
                found_match = True
            for result in entity_results:
                if args.json:
                    json_output.append(result['describe'])
                elif args.name:
                    print(result['describe']['name'])
                else:
                    print(get_result_str())
                    print_desc(result['describe'], args.verbose or args.details)

        if not is_hashid(args.path) and ':' not in args.path:

            # Could be a name of an app or a global workflow
            if args.path.startswith('app-') or args.path.startswith('globalworkflow-'):
                found = describe_global_executable(json_output, args, args.path.partition('-')[0])
            else:
                found = find_global_executable(json_output, args)
            if found:
                found_match = True

            if args.path.startswith('user-'):
                # User
                try:
                    desc = dxpy.api.user_describe(args.path, {"appsInstalled": True, "subscriptions": True})
                    found_match = True
                    if args.json:
                        json_output.append(desc)
                    elif args.name:
                        print(str(desc['first']) + ' ' + str(desc['last']))
                    else:
                        print(get_result_str())
                        print_desc(desc, args.verbose)
                except dxpy.DXAPIError as details:
                    if details.code != requests.codes.not_found:
                        raise
            elif args.path.startswith('org-') or args.path.startswith('team-'):
                # Org or team
                try:
                    desc = dxpy.DXHTTPRequest('/' + args.path + '/describe', {})
                    found_match = True
                    if args.json:
                        json_output.append(desc)
                    elif args.name:
                        print(desc['id'])
                    else:
                        print(get_result_str())
                        print_desc(desc, args.verbose)
                except dxpy.DXAPIError as details:
                    if details.code != requests.codes.not_found:
                        raise

        if args.json:
            if args.multi:
                print(json.dumps(json_output, indent=4))
            elif len(json_output) > 1:
                raise DXCLIError('More than one match found for ' + args.path + '; to get all of them in JSON format, also provide the --multi flag.')
            elif len(json_output) == 0:
                raise DXCLIError('No match found for ' + args.path)
            else:
                print(json.dumps(json_output[0], indent=4))
        elif not found_match:
            raise DXCLIError("No matches found for " + args.path)
    except:
        err_exit()


def _validate_new_user_input(args):
    # TODO: Support interactive specification of `args.username`.
    # TODO: Support interactive specification of `args.email`.

    if args.org is None and len(DXNewUserOrgArgsAction.user_specified_opts) > 0:
        raise DXCLIError("Cannot specify {opts} without specifying --org".format(
            opts=DXNewUserOrgArgsAction.user_specified_opts
        ))


def _get_user_new_args(args):
    """
    PRECONDITION: `_validate_new_user_input()` has been called on `args`.
    """
    user_new_args = {"username": args.username,
                     "email": args.email}
    if args.first is not None:
        user_new_args["first"] = args.first
    if args.last is not None:
        user_new_args["last"] = args.last
    if args.middle is not None:
        user_new_args["middle"] = args.middle
    if args.token_duration is not None:
        token_duration_ms = normalize_timedelta(args.token_duration)
        if token_duration_ms > 30 * 24 * 60 * 60 * 1000:
            raise ValueError("--token-duration must be 30 days or less")
        else:
            user_new_args["tokenDuration"] = token_duration_ms
    if args.occupation is not None:
        user_new_args["occupation"] = args.occupation
    if args.set_bill_to is True:
        user_new_args["billTo"] = args.org
    if args.on_behalf_of is not None:
        user_new_args["provisioningOrg"] = args.on_behalf_of
    return user_new_args


def new_user(args):
    _validate_new_user_input(args)

    # Create user account.
    #
    # We prevent retries here because authserver is closing the server-side
    # connection in certain situations. We cannot simply set `always_retry` to
    # False here because we receive a 504 error code from the server.
    # TODO: Allow retries when authserver issue is resolved.
    dxpy.DXHTTPRequest(dxpy.get_auth_server_name() + "/user/new",
                       _get_user_new_args(args),
                       prepend_srv=False,
                       max_retries=0)

    user_id = "user-" + args.username.lower()
    if args.org is not None:
        # Invite new user to org.
        dxpy.api.org_invite(args.org, get_org_invite_args(user_id, args))

    if args.brief:
        print(user_id)
    else:
        print(fill("Created new user account ({u})".format(u=user_id)))


def new_project(args):
    if args.name == None:
        if INTERACTIVE_CLI:
            args.name = input("Enter name for new project: ")
        else:
            err_exit(parser_new_project.format_help() + fill("No project name supplied, and input is not interactive"), 3)
    inputs = {"name": args.name}
    if args.bill_to:
        inputs["billTo"] = args.bill_to
    if args.region:
        inputs["region"] = args.region
    if args.phi:
        inputs["containsPHI"] = True
    if args.database_ui_view_only:
        inputs["databaseUIViewOnly"] = True

    try:
        resp = dxpy.api.project_new(inputs)
        if args.brief:
            print(resp['id'])
        else:
            print(fill('Created new project called "' + args.name + '" (' + resp['id'] + ')'))
        if args.select or (INTERACTIVE_CLI and prompt_for_yn("Switch to new project now?", default=False)):
            set_project(resp['id'], write=True, name=args.name)
            set_wd('/', write=True)
    except:
        err_exit()


def new_record(args):
    try_call(process_dataobject_args, args)
    try_call(process_single_dataobject_output_args, args)
    init_from = None

    if args.init is not None:
        init_project, _init_folder, init_result = try_call(resolve_existing_path,
                                                           args.init,
                                                           expected='entity')
        init_from = dxpy.DXRecord(dxid=init_result['id'], project=init_project)

    if args.output is None:
        project = dxpy.WORKSPACE_ID
        folder = dxpy.config.get('DX_CLI_WD', '/')
        name = None
    else:
        project, folder, name = try_call(resolve_path, args.output)

    dxrecord = None
    try:
        dxrecord = dxpy.new_dxrecord(project=project, name=name,
                                     tags=args.tags, types=args.types,
                                     hidden=args.hidden, properties=args.properties,
                                     details=args.details,
                                     folder=folder,
                                     close=args.close,
                                     parents=args.parents, init_from=init_from)
        if args.brief:
            print(dxrecord.get_id())
        else:
            print_desc(dxrecord.describe(incl_properties=True, incl_details=True), args.verbose)
    except:
        err_exit()

def set_visibility(args):
    had_error = False
    # Attempt to resolve name
    _project, _folderpath, entity_results = try_call(resolve_existing_path,
                                                     args.path,
                                                     expected='entity',
                                                     allow_mult=True, all_mult=args.all)

    if entity_results is None:
        err_exit(fill('Could not resolve "' + args.path + '" to a name or ID'), 3)

    for result in entity_results:
        try:
            dxpy.DXHTTPRequest('/' + result['id'] + '/setVisibility',
                               {"hidden": (args.visibility == 'hidden')})
        except (dxpy.DXAPIError,) + network_exceptions as details:
            print(format_exception(details), file=sys.stderr)
            had_error = True

    if had_error:
        err_exit('', 3)

def get_details(args):
    # Attempt to resolve name
    _project, _folderpath, entity_result = try_call(resolve_existing_path,
                                                    args.path, expected='entity')

    if entity_result is None:
        err_exit(fill('Could not resolve "' + args.path + '" to a name or ID'), 3)

    try:
        print(json.dumps(dxpy.DXHTTPRequest('/' + entity_result['id'] + '/getDetails', {}), indent=4))
    except:
        err_exit()

def set_details(args):
    had_error = False
    # Attempt to resolve name
    _project, _folderpath, entity_results = try_call(resolve_existing_path,
                                                     args.path, expected='entity',
                                                     allow_mult=True, all_mult=args.all)

    if entity_results is None:
        err_exit(exception=ResolutionError('Could not resolve "' + args.path + '" to a name or ID'),
                 expected_exceptions=(ResolutionError,))

    # Throw error if both -f/--details-file and details supplied.
    if args.details is not None and args.details_file is not None:
        err_exit(exception=DXParserError('Cannot provide both -f/--details-file and details'),
                 expected_exceptions=(DXParserError,))

    elif args.details is not None:
        try:
            details = json.loads(args.details)
        except ValueError as e:
            err_exit('Error: Details could not be parsed as JSON', expected_exceptions=(ValueError,), exception=e)

    elif args.details_file is not None:
        with (sys.stdin if args.details_file == '-' else open(args.details_file, 'r')) as fd:
            data = fd.read()
            try:
                details = json.loads(data)
            except ValueError as e:
                err_exit('Error: File contents could not be parsed as JSON', expected_exceptions=(ValueError,),
                         exception=e)

    # Throw error if missing arguments.
    else:
        err_exit(exception=DXParserError('Must set one of -f/--details-file or details'),
                 expected_exceptions=(DXParserError,))

    for result in entity_results:
        try:
            dxpy.DXHTTPRequest('/' + result['id'] + '/setDetails', details)
        except (dxpy.DXAPIError,) + network_exceptions as exc_details:
            print(format_exception(exc_details), file=sys.stderr)
            had_error = True

    if had_error:
        err_exit('', 3)

def add_types(args):
    had_error = False
    # Attempt to resolve name
    _project, _folderpath, entity_results = try_call(resolve_existing_path,
                                                     args.path,
                                                     expected='entity',
                                                     allow_mult=True, all_mult=args.all)

    if entity_results is None:
        err_exit(fill('Could not resolve "' + args.path + '" to a name or ID'), 3)

    for result in entity_results:
        try:
            dxpy.DXHTTPRequest('/' + result['id'] + '/addTypes',
                               {"types": args.types})
        except (dxpy.DXAPIError,) + network_exceptions as details:
            print(format_exception(details), file=sys.stderr)
            had_error = True
    if had_error:
        err_exit('', 3)

def remove_types(args):
    had_error = False
    # Attempt to resolve name
    _project, _folderpath, entity_results = try_call(resolve_existing_path,
                                                     args.path,
                                                     expected='entity',
                                                     allow_mult=True, all_mult=args.all)

    if entity_results is None:
        err_exit(fill('Could not resolve "' + args.path + '" to a name or ID'), 3)

    for result in entity_results:
        try:
            dxpy.DXHTTPRequest('/' + result['id'] + '/removeTypes',
                               {"types": args.types})
        except (dxpy.DXAPIError,) + network_exceptions as details:
            print(format_exception(details), file=sys.stderr)
            had_error = True
    if had_error:
        err_exit('', 3)

def add_tags(args):
    had_error = False
    # Attempt to resolve name
    project, _folderpath, entity_results = try_call(resolve_to_objects_or_project,
                                                    args.path,
                                                    args.all)

    if entity_results is not None:
        for result in entity_results:
            try:
                dxpy.DXHTTPRequest('/' + result['id'] + '/addTags',
                                   {"project": project,
                                    "tags": args.tags})
            except (dxpy.DXAPIError,) + network_exceptions as details:
                print(format_exception(details), file=sys.stderr)
                had_error = True
        if had_error:
            err_exit('', 3)
    elif not project.startswith('project-'):
        err_exit('Cannot add tags to a non-project data container', 3)
    else:
        try:
            dxpy.DXHTTPRequest('/' + project + '/addTags',
                               {"tags": args.tags})
        except:
            err_exit()

def remove_tags(args):
    had_error = False
    # Attempt to resolve name
    project, _folderpath, entity_results = try_call(resolve_to_objects_or_project,
                                                    args.path,
                                                    args.all)

    if entity_results is not None:
        for result in entity_results:
            try:
                dxpy.DXHTTPRequest('/' + result['id'] + '/removeTags',
                                   {"project": project,
                                    "tags": args.tags})
            except (dxpy.DXAPIError,) + network_exceptions as details:
                print(format_exception(details), file=sys.stderr)
                had_error = True
        if had_error:
            err_exit('', 3)
    elif not project.startswith('project-'):
        err_exit('Cannot remove tags from a non-project data container', 3)
    else:
        try:
            dxpy.DXHTTPRequest('/' + project + '/removeTags',
                               {"tags": args.tags})
        except:
            err_exit()

def rename(args):
    had_error = False
    # Attempt to resolve name
    project, _folderpath, entity_results = try_call(resolve_to_objects_or_project,
                                                    args.path,
                                                    args.all)

    if entity_results is not None:
        for result in entity_results:
            try:
                dxpy.DXHTTPRequest('/' + result['id'] + '/rename',
                                   {"project": project,
                                    "name": args.name})
            except (dxpy.DXAPIError,) + network_exceptions as details:
                print(format_exception(details), file=sys.stderr)
                had_error = True
        if had_error:
            err_exit('', 3)
    elif not project.startswith('project-'):
        err_exit('Cannot rename a non-project data container', 3)
    else:
        try:
            dxpy.api.project_update(project, {"name": args.name})
        except:
            err_exit()

def set_properties(args):
    had_error = False
    # Attempt to resolve name
    project, _folderpath, entity_results = try_call(resolve_to_objects_or_project,
                                                    args.path,
                                                    args.all)

    try_call(process_properties_args, args)
    if entity_results is not None:
        for result in entity_results:
            try:
                dxpy.DXHTTPRequest('/' + result['id'] + '/setProperties',
                                   {"project": project,
                                    "properties": args.properties})
            except (dxpy.DXAPIError,) + network_exceptions as details:
                print(format_exception(details), file=sys.stderr)
                had_error = True
        if had_error:
            err_exit('', 3)
    elif not project.startswith('project-'):
        err_exit('Cannot set properties on a non-project data container', 3)
    else:
        try:
            dxpy.api.project_set_properties(project, {"properties": args.properties})
        except:
            err_exit()

def unset_properties(args):
    had_error = False
    # Attempt to resolve name
    project, _folderpath, entity_results = try_call(resolve_to_objects_or_project,
                                                    args.path,
                                                    args.all)
    properties = {}
    for prop in args.properties:
        properties[prop] = None
    if entity_results is not None:
        for result in entity_results:
            try:
                dxpy.DXHTTPRequest('/' + result['id'] + '/setProperties',
                                   {"project": project,
                                    "properties": properties})
            except (dxpy.DXAPIError,) + network_exceptions as details:
                print(format_exception(details), file=sys.stderr)
                had_error = True
        if had_error:
            err_exit('', 3)
    elif not project.startswith('project-'):
        err_exit('Cannot unset properties on a non-project data container', 3)
    else:
        try:
            dxpy.api.project_set_properties(project, {"properties": properties})
        except:
            err_exit()


def make_download_url(args):
    project, _folderpath, entity_result = try_call(resolve_existing_path, args.path, expected='entity')
    if entity_result is None:
        err_exit(fill('Could not resolve ' + args.path + ' to a data object'), 3)

    if entity_result['describe']['class'] != 'file':
        err_exit(fill('Error: dx download is only for downloading file objects'), 3)

    if args.filename is None:
        args.filename = entity_result['describe']['name']

    # TODO: how to do data egress billing for make_download_url?
    try:
        dxfile = dxpy.DXFile(entity_result['id'], project=project)
        # Only provide project ID, not job workspace container ID
        project = dxfile.project if re.match(r"^project-[a-zA-Z0-9]{24}$", dxfile.project) else dxpy.DXFile.NO_PROJECT_HINT
        url, _headers = dxfile.get_download_url(preauthenticated=True,
                                                duration=normalize_timedelta(args.duration)//1000 if args.duration else 24*3600,
                                                filename=args.filename,
                                                project=project)
        print(url)
    except:
        err_exit()


def get_record(entity_result, args):
    if args.output == '-':
        fd = sys.stdout
    else:
        filename = args.output
        if filename is None:
            filename = entity_result['describe']['name'].replace('/', '%2F')
        if args.output is None and not args.no_ext:
            filename += '.json'
        if not args.overwrite and os.path.exists(filename):
            err_exit(fill('Error: path "' + filename + '" already exists but -f/--overwrite was not set'), 3)
        try:
            fd = open(filename, 'w')
        except:
            err_exit('Error opening destination file ' + filename)

    try:
        details = dxpy.api.record_get_details(entity_result['id'])
    except:
        err_exit()

    fd.write(json.dumps(details, indent=4))

    if args.output != '-':
        fd.close()


def get_output_path(obj_name, obj_class, args):
    path_name = obj_name.replace('/', '%2F')
    if args.output == '-':
        err_exit('Error: {} '.format(obj_class) + 'objects cannot be dumped to stdout, please specify a directory', 3)
    output_base = args.output or '.'
    if os.path.isdir(output_base):
        output_path = os.path.join(output_base, path_name)
    else:
        output_path = output_base
    if os.path.isfile(output_path):
        if not args.overwrite:
            err_exit(fill('Error: path "' + output_path + '" already exists but -f/--overwrite was not set'), 3)
        os.unlink(output_path)
    # Here, output_path either points to a directory or a nonexistent path
    if not os.path.exists(output_path):
        print('Creating "{}" output directory'.format(output_path), file=sys.stderr)
        os.mkdir(output_path)
    # Here, output_path points to a directory
    if len(os.listdir(output_path)):
        # For safety, refuse to remove an existing non-empty
        # directory automatically. Exception: if we are downloading
        # database files and -f/--overwrite was set, then we can
        # proceed, and downloaded files will be added to the existing
        # directory structure.
        if not (obj_class == 'database' and args.overwrite):
            err_exit(fill('Error: path "' + output_path + '" already exists. Remove it and try again.'), 3)
    return output_path


def get_applet(project, entity_result, args):
    obj_name = entity_result['describe']['name']
    obj_id = entity_result['id']
    output_path = get_output_path(obj_name,
                                  entity_result['describe']['class'],
                                  args)
    from dxpy.utils.executable_unbuilder import dump_executable
    print("Downloading applet data", file=sys.stderr)
    dx_obj = dxpy.DXApplet(obj_id, project=project)
    describe_output = dx_obj.describe(incl_properties=True,
                                      incl_details=True)
    dump_executable(dx_obj,
                    output_path,
                    omit_resources=args.omit_resources,
                    describe_output=describe_output)


def get_app(entity_result, args):
    obj_name = entity_result['describe']['name']
    obj_id = entity_result['id']
    output_path = get_output_path(obj_name,
                                  entity_result['describe']['class'],
                                  args)
    from dxpy.utils.executable_unbuilder import dump_executable
    print("Downloading application data", file=sys.stderr)
    dx_obj = dxpy.DXApp(obj_id)
    dump_executable(dx_obj, output_path, omit_resources=args.omit_resources)


def get_workflow(entity_result, args):
    obj_name = entity_result['describe']['name']
    obj_id = entity_result['id']
    output_path = get_output_path(obj_name,
                                  entity_result['describe']['class'],
                                  args)
    from dxpy.utils.executable_unbuilder import dump_executable
    print("Downloading workflow data", file=sys.stderr)

    if entity_result['describe']['class'] == 'workflow':
        dx_obj = dxpy.DXWorkflow(obj_id)
    else:
        dx_obj = dxpy.DXGlobalWorkflow(obj_id)
    describe_output = entity_result['describe']
    dump_executable(dx_obj, output_path, omit_resources=True, describe_output=describe_output)

def do_debug(msg):
    logging.debug(msg)

def get_database(entity_result, args):
    do_debug("dx.py#get_database - entity_result = {}".format(entity_result))
    do_debug("dx.py#get_database - args = {}".format(args))
    obj_id = entity_result['id']
    project = entity_result['describe']['project']
    do_debug("dx.py#get_database - project = {}".format(project))
    # output_path = root output directory for the database
    output_path = get_output_path(obj_id,
                                  entity_result['describe']['class'],
                                  args)
    do_debug("dx.py#get_database - output_path = {}".format(output_path))
    from dxpy.utils.executable_unbuilder import dump_executable
    print("Downloading database files", file=sys.stderr)
    dx_obj = dxpy.DXDatabase(obj_id)
    describe_output = entity_result['describe']
    do_debug("dx.py#get_database - dx_obj = {}".format(dx_obj))

    # If filename is omitted, this is an error unless --allow-all-files is True
    if args.filename is None or args.filename == '/' or args.filename == '':
        if not args.allow_all_files:
            err_exit('Error: downloading all files from a database not allowed unless --allow-all-files argument is specified.', 3)

    # Call /database-xxx/listFolder to fetch database file metadata
    list_folder_args = {"folder": args.filename, "recurse": args.recurse}
    list_folder_resp = dxpy.api.database_list_folder(obj_id, list_folder_args)
    do_debug("dx.py#get_database - list_folder_resp = {}".format(list_folder_resp))
    results = list_folder_resp["results"]
    for dbfilestatus in results:
        # Skip the entries that represent directories, because the local directory structure
        # will be created automatically as real files are downloaded.
        try:
            is_dir = dbfilestatus["isDirectory"]
        except:
            is_dir = True
        if is_dir == False:
            src_filename = dbfilestatus["path"]
            idx = src_filename.rfind("database-")
            if idx != -1:
                src_filename = src_filename[idx + 34:]
            print(src_filename)
            download_one_database_file(project, entity_result['describe'], output_path, src_filename, dbfilestatus, args)

def get(args):
    # Decide what to do based on entity's class
    if not is_hashid(args.path) and ':' not in args.path and args.path.startswith('app-'):
        desc = dxpy.api.app_describe(args.path)
        entity_result = {"id": desc["id"], "describe": desc}
    elif not is_hashid(args.path) and ':' not in args.path and args.path.startswith('globalworkflow-'):
        desc = dxpy.api.global_workflow_describe(args.path)
        entity_result = {"id": desc["id"], "describe": desc}
    else:
        project, _folderpath, entity_result = try_call(resolve_existing_path,
                                                       args.path,
                                                       expected='entity')

    if entity_result is None:
        err_exit('Could not resolve ' + args.path + ' to a data object', 3)

    entity_result_class = entity_result['describe']['class']

    if entity_result_class == 'file':
        download_one_file(project,
                          entity_result['describe'],
                          entity_result['describe']['name'],
                          args)
    elif entity_result_class == 'record':
        get_record(entity_result, args)
    elif entity_result_class == 'applet':
        get_applet(project, entity_result, args)
    elif entity_result_class == 'app':
        get_app(entity_result, args)
    elif entity_result_class in ('workflow', 'globalworkflow'):
        get_workflow(entity_result, args)
    elif entity_result_class == 'database':
        get_database(entity_result, args)
    else:
        err_exit('Error: The given object is of class ' + entity_result['describe']['class'] +
                 ' but an object of class file, record, applet, app, or workflow was expected', 3)

def cat(args):
    for path in args.path:
        project, _folderpath, entity_result = try_call(resolve_existing_path, path)

        if entity_result is None:
            err_exit('Could not resolve ' + path + ' to a data object', 3)

        if entity_result['describe']['class'] != 'file':
            err_exit('Error: expected a file object', 3)

        # If the user did not explicitly provide the project, don't pass any
        # project parameter to the API call but continue with download resolution
        path_has_explicit_proj = is_project_explicit(path) or is_jbor_str(path)
        if not path_has_explicit_proj:
            project = None
        elif is_jbor_str(path):
            project = entity_result['describe']['project']
        # If the user explicitly provided the project and it doesn't contain
        # the file, don't allow the download.
        if path_has_explicit_proj and project is not None and \
           not object_exists_in_project(entity_result['describe']['id'], project):
            err_exit('Error: project does not contain specified file object', 3)

        # We assume the file is binary, unless specified otherwise
        mode = "rb"
        if args.unicode_text is True:
            mode = "r"
        try:
            dxfile = dxpy.DXFile(entity_result['id'], mode=mode)
            while True:
                # If we decided the project specification was not explicit, do
                # not allow the workspace setting to bleed through
                chunk = dxfile.read(1024*1024, project=project or dxpy.DXFile.NO_PROJECT_HINT)
                if len(chunk) == 0:
                    break
                if mode == 'rb':
                    sys.stdout.buffer.write(chunk)
                else:
                    sys.stdout.write(chunk)
        except:
            err_exit()


def download_or_cat(args):
    if args.output == '-':
        cat_args = parser.parse_args(['cat'] + args.paths)
        cat_args.unicode_text = args.unicode_text
        cat(cat_args)
        return
    download(args)


def head(args):
    # Attempt to resolve name
    project, _folderpath, entity_result = try_call(resolve_existing_path,
                                                   args.path, expected='entity')
    if entity_result is None:
        err_exit('Could not resolve ' + args.path + ' to a data object', 3)
    if not entity_result['describe']['class'] in ['file']:
        err_exit('Error: The given object is of class ' + entity_result['describe']['class'] +
                 ' but an object of class file was expected', 3)

    handler = dxpy.get_handler(entity_result['id'], project=project)

    counter = 0
    if args.lines > 0:
        try:
            if handler._class == 'file':
                try:
                    handler._read_bufsize = 1024*32
                    for line in handler:
                        print(line)
                        counter += 1
                        if counter == args.lines:
                            break
                except UnicodeDecodeError:
                    sys.stdout.write("File contains binary data")
            else:
                err_exit("Class type " + handler._class + " not supported for dx head")
        except StopIteration:
            pass
        except:
            err_exit()

def upload(args, **kwargs):
    if args.output is not None and args.path is not None:
        raise DXParserError('Error: Cannot provide both the -o/--output and --path/--destination arguments')
    elif args.path is None:
        args.path = args.output

    # multithread is an argument taken by DXFile.write() but we
    # have to expose a `--singlethread` option for `dx upload` since
    # it has multithreaded upload set by default
    args.multithread = not args.singlethread

    if len(args.filename) > 1 and args.path is not None and not args.path.endswith("/"):
        # When called as "dx upload x --dest /y", we upload to "/y"; with --dest "/y/", we upload to "/y/x".
        # Called as "dx upload x y --dest /z", z is implicitly a folder, so append a slash to avoid incorrect path
        # resolution.
        args.path += "/"

    paths = copy.copy(args.filename)
    for path in paths:
        args.filename = path
        upload_one(args, **kwargs)

upload_seen_paths = set()
def upload_one(args):
    try_call(process_dataobject_args, args)

    args.show_progress = args.show_progress and not args.brief

    if args.path is None:
        project = dxpy.WORKSPACE_ID
        folder = dxpy.config.get('DX_CLI_WD', '/')
        name = None if args.filename == '-' else os.path.basename(args.filename)
    else:
        project, folder, name = try_call(resolve_path, args.path)
        if name is None and args.filename != '-':
            name = os.path.basename(args.filename)

    if os.path.isdir(args.filename):
        if not args.recursive:
            err_exit('Error: {f} is a directory but the -r/--recursive option was not given'.format(f=args.filename), 3)
        norm_path = os.path.realpath(args.filename)
        if norm_path in upload_seen_paths:
            print("Skipping {f}: directory loop".format(f=args.filename), file=sys.stderr)
            return
        else:
            upload_seen_paths.add(norm_path)

        dir_listing = os.listdir(args.filename)
        if len(dir_listing) == 0: # Create empty folder
            dxpy.api.project_new_folder(project, {"folder": os.path.join(folder, os.path.basename(args.filename)),
                                                  "parents": True})
        else:
            for f in dir_listing:
                sub_args = copy.copy(args)
                sub_args.mute = True
                sub_args.filename = os.path.join(args.filename, f)
                sub_args.path = "{p}:{f}/{sf}/".format(p=project, f=folder, sf=os.path.basename(args.filename))
                sub_args.parents = True
                upload_one(sub_args)
    else:
        try:
            dxfile = dxpy.upload_local_file(filename=(None if args.filename == '-' else args.filename),
                                            file=(sys.stdin.buffer if args.filename == '-' else None),
                                            write_buffer_size=(None if args.write_buffer_size is None
                                                               else int(args.write_buffer_size)),
                                            name=name,
                                            tags=args.tags,
                                            types=args.types,
                                            hidden=args.hidden,
                                            project=project,
                                            properties=args.properties,
                                            details=args.details,
                                            folder=folder,
                                            parents=args.parents,
                                            show_progress=args.show_progress,
                                            multithread=args.multithread)
            if args.wait:
                dxfile._wait_on_close()
            if args.brief:
                print(dxfile.get_id())
            elif not args.mute:
                print_desc(dxfile.describe(incl_properties=True, incl_details=True))
        except:
            err_exit()

def find_executions(args):
    try_call(process_find_by_property_args, args)
    if not (args.origin_jobs or args.all_jobs):
        args.trees = True
    if args.origin_jobs and args.parent is not None and args.parent != 'none':
        return
    project = dxpy.WORKSPACE_ID
    origin = None
    more_results = False
    include_io = (args.verbose and args.json) or args.show_outputs
    include_internetUsageIPs = args.verbose and args.json
    if args.classname == 'job':
        describe_args = {
        "defaultFields": True, 
        "fields": {
            "runInput": include_io,
            "originalInput": include_io,
            "input": include_io,
            "output": include_io,
            "internetUsageIPs":include_internetUsageIPs
        }
    }
    else:
        describe_args = {"io": include_io}
    id_desc = None

    # Now start parsing flags
    if args.id is not None:
        id_desc = try_call(dxpy.api.job_describe, args.id, {"io": False})
        origin = id_desc.get('originJob', None)
        if args.origin_jobs and args.id != origin:
            return
        if args.origin is not None and origin != args.origin:
            return
        project = None
        args.user = None
    else:
        origin = args.origin
        if args.project is not None:
            if get_last_pos_of_char(':', args.project) == -1:
                args.project = args.project + ':'
            project, _none, _none = try_call(resolve_existing_path,
                                             args.project, 'project')
        if args.user is not None and args.user != 'self' and not args.user.startswith('user-'):
            args.user = 'user-' + args.user.lower()
        if args.all_projects:
            project = None
    query = {'classname': args.classname,
             'launched_by': args.user,
             'executable': args.executable,
             'project': project,
             'state': args.state,
             'origin_job': origin,
             'parent_job': "none" if args.origin_jobs else args.parent,
             'describe': describe_args,
             'created_after': args.created_after,
             'created_before': args.created_before,
             'name': args.name,
             'name_mode': 'glob',
             'tags': args.tag,
             'properties': args.properties,
             'include_subjobs': False if args.no_subjobs else True,
             'root_execution': args.root_execution}
    if args.num_results < 1000 and not args.trees:
        query['limit'] = args.num_results + 1

    json_output = []                        # for args.json

    def build_tree(root, executions_by_parent, execution_descriptions, is_cached_result=False):
        tree, root_string = {}, ''
        if args.json:
            json_output.append(execution_descriptions[root])
        elif args.brief:
            print(root)
        else:
            root_string = get_find_executions_string(execution_descriptions[root],
                                                     has_children=root in executions_by_parent,
                                                     show_outputs=args.show_outputs,
                                                     is_cached_result=is_cached_result)
            tree[root_string] = collections.OrderedDict()
        for child_execution in executions_by_parent.get(root, {}):
            child_is_cached_result = is_cached_result or (execution_descriptions[child_execution].get('outputReusedFrom') is not None)
            subtree, _subtree_root = build_tree(child_execution,
                                                executions_by_parent,
                                                execution_descriptions,
                                                is_cached_result=child_is_cached_result)
            if tree:
                tree[root_string].update(subtree)
        return tree, root_string

    def process_tree(result, executions_by_parent, execution_descriptions):
        is_cached_result = False
        if 'outputReusedFrom' in result and result['outputReusedFrom'] is not None:
            is_cached_result = True
        tree, root = build_tree(result['id'], executions_by_parent, execution_descriptions, is_cached_result)
        if tree:
            print(format_tree(tree[root], root))

    try:
        num_processed_results = 0
        roots = collections.OrderedDict()
        for execution_result in dxpy.find_executions(**query):
            if args.trees:
                if args.classname == 'job':
                    root = execution_result['describe']['originJob']
                else:
                    root = execution_result['describe']['rootExecution']
                if root not in roots:
                    num_processed_results += 1
            else:
                num_processed_results += 1

            if (num_processed_results > args.num_results):
                more_results = True
                break

            if args.json:
                json_output.append(execution_result['describe'])
            elif args.trees:
                roots[root] = root
                if args.classname == 'analysis' and root.startswith('job-'):
                    # Analyses in trees with jobs at their root found in "dx find analyses" are displayed unrooted,
                    # and only the last analysis found is displayed.
                    roots[root] = execution_result['describe']['id']
            elif args.brief:
                print(execution_result['id'])
            elif not args.trees:
                print(format_tree({}, get_find_executions_string(execution_result['describe'],
                                                                 has_children=False,
                                                                 single_result=True,
                                                                 show_outputs=args.show_outputs)))
        if args.trees:
            executions_by_parent, descriptions = collections.defaultdict(list), {}
            root_field = 'origin_job' if args.classname == 'job' else 'root_execution'
            parent_field = 'masterJob' if args.no_subjobs else 'parentJob'
            query = {'classname': args.classname,
                     'describe': describe_args,
                     'include_subjobs': False if args.no_subjobs else True,
                     root_field: list(roots.keys())}
            if not args.all_projects:
                # If the query doesn't specify a project, the server finds all projects to which the user has explicit
                # permissions, but doesn't search through public projects.
                # In "all projects" mode, we don't specify a project in the initial query, and so don't need to specify
                # one in the follow-up query here (because the initial query can't return any jobs in projects to which
                # the user doesn't have explicit permissions).
                # When searching in a specific project, we set a project in the query here, in case this is a public
                # project and the user doesn't have explicit permissions (otherwise, the follow-up query would return
                # empty results).
                query['project'] = project

            def process_execution_result(execution_result):
                execution_desc = execution_result['describe']
                parent = execution_desc.get(parent_field) or execution_desc.get('parentAnalysis')
                descriptions[execution_result['id']] = execution_desc
                if parent:
                    executions_by_parent[parent].append(execution_result['id'])

                # If an analysis with cached children, also insert those
                if execution_desc['class'] == 'analysis':
                    for stage_desc in execution_desc['stages']:
                        if 'parentAnalysis' in stage_desc['execution'] and stage_desc['execution']['parentAnalysis'] != execution_result['id'] and \
                           (args.classname != 'analysis' or stage_desc['execution']['class'] == 'analysis'):
                            # this is a cached stage (with a different parent)
                            executions_by_parent[execution_result['id']].append(stage_desc['execution']['id'])
                            if stage_desc['execution']['id'] not in descriptions:
                                descriptions[stage_desc['execution']['id']] = stage_desc['execution']

            # Short-circuit the find_execution API call(s) if there are
            # no root executions (and therefore we would have gotten 0
            # results anyway)
            if len(list(roots.keys())) > 0:
                for execution_result in dxpy.find_executions(**query):
                    process_execution_result(execution_result)

                # ensure roots are sorted by their creation time
                sorted_roots = sorted(roots, key=lambda root: -descriptions[roots[root]]['created'])

                for root in sorted_roots:
                    process_tree(descriptions[roots[root]], executions_by_parent, descriptions)
        if args.json:
            print(json.dumps(json_output, indent=4))

        if more_results and get_delimiter() is None and not (args.brief or args.json):
            print(fill("* More results not shown; use -n to increase number of results or --created-before to show older results", subsequent_indent='  '))
    except:
        err_exit()

def find_data(args):
    # --folder deprecated to --path.
    if args.folder is None and args.path is not None:
        args.folder = args.path
    elif args.folder is not None and args.path is not None:
        err_exit(exception=DXParserError('Cannot supply both --folder and --path.'),
                 expected_exceptions=(DXParserError,))

    try_call(process_find_by_property_args, args)
    if args.all_projects:
        args.project = None
        args.folder = None
        args.recurse = True
    elif args.project is None:
        args.project = dxpy.WORKSPACE_ID
    else:
        if get_last_pos_of_char(':', args.project) == -1:
            args.project = args.project + ':'

        if args.folder is not None and get_last_pos_of_char(':', args.folder) != -1:
            err_exit(exception=DXParserError('Cannot supply both --project and --path PROJECTID:FOLDERPATH.'),
                     expected_exceptions=(DXParserError,))

        args.project, _none, _none = try_call(resolve_existing_path,
                                              args.project, 'project')

    if args.folder is not None and not args.folder.startswith('/'):
        args.project, args.folder, _none = try_call(resolve_path, args.folder, expected='folder')

    if args.brief:
        describe_input = dict(fields=dict(project=True, id=True))
    elif args.verbose:
        describe_input = True
    else:
        describe_input = dict(fields=get_ls_l_desc_fields())
    try:
        results = dxpy.find_data_objects(classname=args.classname,
                                         state=args.state,
                                         visibility=args.visibility,
                                         properties=args.properties,
                                         name=args.name,
                                         name_mode='glob',
                                         typename=args.type,
                                         tags=args.tag, link=args.link,
                                         project=args.project,
                                         folder=args.folder,
                                         recurse=(args.recurse if not args.recurse else None),
                                         modified_after=args.mod_after,
                                         modified_before=args.mod_before,
                                         created_after=args.created_after,
                                         created_before=args.created_before,
                                         region=args.region,
                                         describe=describe_input)
        if args.json:
            print(json.dumps(list(results), indent=4))
            return
        if args.brief:
            for result in results:
                print(result['project'] + ':' + result['id'])
        else:
            for result in results:
                if args.verbose:
                    print("")
                    print_data_obj_desc(result["describe"])
                else:
                    print_ls_l_desc(result["describe"], include_folder=True, include_project=args.all_projects)
    except:
        err_exit()


def find_projects(args):
    try_call(process_find_by_property_args, args)
    try_call(process_phi_param, args)
    try:
        results = dxpy.find_projects(name=args.name, name_mode='glob',
                                     properties=args.properties, tags=args.tag,
                                     level=('VIEW' if args.public else args.level),
                                     describe=(not args.brief),
                                     explicit_perms=(not args.public if not args.public else None),
                                     public=(args.public if args.public else None),
                                     created_after=args.created_after,
                                     created_before=args.created_before,
                                     region=args.region,
                                     containsPHI=args.containsPHI)
    except:
        err_exit()
    format_find_results(args, results)

def find_apps_result(args):
    raw_results = dxpy.find_apps(name=args.name, name_mode='glob', category=args.category,
                                 all_versions=args.all,
                                 published=(not args.unpublished),
                                 billed_to=args.billed_to,
                                 created_by=args.creator,
                                 developer=args.developer,
                                 created_after=args.created_after,
                                 created_before=args.created_before,
                                 modified_after=args.mod_after,
                                 modified_before=args.mod_before,
                                 describe={"fields": {"name": True,
                                                      "installed": args.installed,
                                                      "title": not args.brief,
                                                      "version": not args.brief,
                                                      "published": args.verbose,
                                                      "billTo": not args.brief}})

    if args.installed:
        maybe_filtered_by_install = (result for result in raw_results if result['describe']['installed'])
    else:
        maybe_filtered_by_install = raw_results

    if args.brief:
        results = ({"id": result['id']} for result in maybe_filtered_by_install)
    else:
        results = sorted(maybe_filtered_by_install, key=lambda result: result['describe']['name'])
    return results

def find_global_workflows_result(args):
    raw_results = dxpy.find_global_workflows(name=args.name, name_mode='glob', category=args.category,
                                 all_versions=args.all,
                                 published=(not args.unpublished),
                                 billed_to=args.billed_to,
                                 created_by=args.creator,
                                 developer=args.developer,
                                 created_after=args.created_after,
                                 created_before=args.created_before,
                                 modified_after=args.mod_after,
                                 modified_before=args.mod_before,
                                 describe={"fields": {"name": True,
                                                      "title": not args.brief,
                                                      "version": not args.brief,
                                                      "published": args.verbose,
                                                      "billTo": not args.brief}})

    if args.brief:
        results = ({"id": result['id']} for result in raw_results)
    else:
        results = sorted(raw_results, key=lambda result: result['describe']['name'])
    return results

def print_find_results(results, args):
    def maybe_x(result):
        return DNANEXUS_X() if result['describe']['billTo'] in ['org-dnanexus', 'org-dnanexus_apps'] else ' '

    if args.json:
        print(json.dumps(list(results), indent=4))
        return
    if args.brief:
        for result in results:
            print(result['id'])
    elif not args.verbose:
        for result in results:
            print(maybe_x(result) + DELIMITER(" ") + result['describe'].get('title', result['describe']['name']) + DELIMITER(' (') + result["describe"]["name"] + DELIMITER("), v") + result["describe"]["version"])
    else:
        for result in results:
            print(maybe_x(result) + DELIMITER(" ") + result["id"] + DELIMITER(" ") + result['describe'].get('title', result['describe']['name']) + DELIMITER(' (') + result["describe"]["name"] + DELIMITER('), v') + result['describe']['version'] + DELIMITER(" (") + ("published" if result["describe"].get("published", 0) > 0 else "unpublished") + DELIMITER(")"))

def find_apps(args):
    try:
        results = find_apps_result(args)
        print_find_results(results, args)
    except:
        err_exit()

def find_global_workflows(args):
    try:
        results = find_global_workflows_result(args)
        print_find_results(results, args)
    except:
        err_exit()

def update_project(args):
    input_params = get_update_project_args(args)

    # The resolver expects a ':' to separate projects from folders.
    if ':' not in args.project_id:
        args.project_id += ':'

    project, _none, _none = try_call(resolve_existing_path,
                                     args.project_id, 'project')
    try:
        results = dxpy.api.project_update(object_id=project, input_params=input_params)
        if args.brief:
            print(results['id'])
        else:
            print(json.dumps(results))
    except:
        err_exit()

def close(args):
    if '_DX_FUSE' in os.environ:
        from xattr import xattr

    handlers = []
    had_error = False

    for path in args.path:
        # Attempt to resolve name
        try:
            project, _folderpath, entity_results = resolve_existing_path(path,
                                                                         expected='entity',
                                                                         allow_mult=True,
                                                                         all_mult=args.all)
        except:
            project, entity_results = None, None

        if entity_results is None:
            print(fill('Could not resolve "' + path + '" to a name or ID'))
            had_error = True
        else:
            for result in entity_results:
                try:
                    obj = dxpy.get_handler(result['id'], project=project)
                    if '_DX_FUSE' in os.environ:
                        xattr(path)['state'] = 'closed'
                    else:
                        obj.close()
                    handlers.append(obj)
                except Exception as details:
                    print(fill(str(details)))

    if args.wait:
        for handler in handlers:
            handler._wait_on_close()

    if had_error:
        err_exit('', 3)

def wait(args):
    had_error = False
    # If only one path was provided, together with the --from-file argument,
    # check to see if it is a local file and if so gather actual paths
    # on which to wait from the contents of the file.
    if args.from_file and len(args.path) == 1 and os.path.isfile(args.path[0]):
        try:
            args.path = open(args.path[0]).read().strip().split('\n')
        except IOError as e:
            raise DXCLIError(
                'Could not open {}. The problem was: {}' % (args.path[0], e))

    for path in args.path:
        if is_job_id(path) or is_analysis_id(path):
            dxexecution = dxpy.get_handler(path)
            print("Waiting for " + path + " to finish running...")
            try_call(dxexecution.wait_on_done)
            print("Done")
        else:
            # Attempt to resolve name
            try:
                project, _folderpath, entity_result = resolve_existing_path(path, expected='entity')
            except:
                project, entity_result = None, None

            if entity_result is None:
                print(fill('Could not resolve ' + path + ' to a data object'))
                had_error = True
            else:
                handler = dxpy.get_handler(entity_result['id'], project=entity_result['describe']['project'])
                print("Waiting for " + path + " to close...")
                try_call(handler._wait_on_close)
                print("Done")

    if had_error:
        err_exit('', 3)

def build(args):
    sys.argv = ['dx build'] + sys.argv[2:]

    def get_source_exec_desc(source_exec_path):
        """
        Return source executable description when --from option is used

        Accecptable format of source_exec_path:
            - applet-ID/workflow-ID
            - project-ID-or-name:applet-ID/workflow-ID
            - project-ID-or-name:folder/path/to/exec-name
              where exec-name must be the name of only one applet or workflow

        :param source_exec_path: applet/workflow path given using --from
        :type source_exec_path: string
        :return: applet/workflow description
        :rtype: dict
        """
        exec_describe_fields={'fields':{"properties":True, "details":True},'defaultFields':True}
        _, _, exec_result = try_call(resolve_existing_path,
                                     source_exec_path,
                                     expected='entity',
                                     ask_to_resolve=False,
                                     expected_classes=["applet", "workflow"],
                                     all_mult=False,
                                     allow_mult=False,
                                     describe=exec_describe_fields)

        if exec_result is None:
            err_exit('Could not resolve {} to an existing applet or workflow.'.format(source_exec_path), 3)
        elif len(exec_result)>1:
            err_exit('More than one match found for {}. Please use an applet/workflow ID instead.'.format(source_exec_path), 3)
        else:
            if exec_result[0]["id"].startswith("applet") or exec_result[0]["id"].startswith("workflow"):
                return exec_result[0]["describe"]
            else:
                err_exit('Could not resolve {} to a valid applet/workflow ID'.format(source_exec_path), 3)

    def get_mode(args):
        """
        Returns an applet or a workflow mode based on whether
        the source directory contains dxapp.json or dxworkflow.json.

        If --from option is used, it will set it to:
        app if --from has been resolved to applet-xxxx
        globalworkflow if --from has been resolved to workflow-xxxx
        Note: dictionaries of regional options that can replace optionally
        ID strings will be supported in the future
        """
        if args._from is not None:
            if args._from["id"].startswith("applet"):
                return "app"
            elif args._from["id"].startswith("workflow"):
                return "globalworkflow"

        if not os.path.isdir(args.src_dir):
            parser.error("{} is not a directory".format(args.src_dir))

        if os.path.exists(os.path.join(args.src_dir, "dxworkflow.json")):
            return "workflow"
        else:
            return "applet"

    def get_validated_source_dir(args):
        if args._from is not None:
            if args.src_dir is not None:
                build_parser.error('Source directory and --from cannot be specified together')
            return None

        src_dir = args.src_dir
        if src_dir is None:
            src_dir = os.getcwd()
            if USING_PYTHON2:
                src_dir = src_dir.decode(sys.getfilesystemencoding())
        return src_dir

    def handle_arg_conflicts(args):
        """
        Raises parser error (exit code 3) if there are any conflicts in the specified options.
        """
        if args.mode == "app" and args.destination != '.':
            build_parser.error("--destination cannot be used when creating an app (only an applet)")
        
        if args.mode == "globalworkflow" and args.destination != '.':
            build_parser.error("--destination cannot be used when creating a global workflow (only a workflow)")

        if args.mode == "applet" and args.region:
            build_parser.error("--region cannot be used when creating an applet (only an app)")

        if args.overwrite and args.archive:
            build_parser.error("Options -f/--overwrite and -a/--archive cannot be specified together")

        if args.run is not None and args.dry_run:
            build_parser.error("Options --dry-run and --run cannot be specified together")

        if args.run and args.remote and args.mode == 'app':
            build_parser.error("Options --remote, --app, and --run cannot all be specified together. Try removing --run and then separately invoking dx run.")

        # conflicts and incompatibilities with --from

        if args._from is not None and args.ensure_upload:
            build_parser.error("Options --from and --ensure-upload cannot be specified together")

        if args._from is not None and args.force_symlinks:
            build_parser.error("Options --from and --force-symlinks cannot be specified together")

        if args._from is not None and args.remote:
            build_parser.error("Options --from and --remote cannot be specified together")

        if args._from is not None and not args.parallel_build:
            build_parser.error("Options --from and --no-parallel-build cannot be specified together")

        if args._from is not None and (args.mode != "app" and args.mode != "globalworkflow"):
            build_parser.error("--from can only be used to build an app from an applet or a global workflow from a project-based workflow")

        if args._from is not None and not args.version_override:
            build_parser.error("--version must be specified when using the --from option")

        if args.mode == "app" and args._from is not None and not args._from["id"].startswith("applet"):
            build_parser.error("app can only be built from an applet (--from should be set to an applet ID)")

        if args.mode == "globalworkflow" and args._from is not None and not args._from["id"].startswith("workflow"):
            build_parser.error("globalworkflow can only be built from an workflow (--from should be set to a workflow ID)")

        if args._from and args.dry_run:
            build_parser.error("Options --dry-run and --from cannot be specified together")

        if args.mode in ("globalworkflow", "applet", "app") and args.keep_open:
            build_parser.error("Global workflows, applets and apps cannot be kept open")

        if args.repository and not args.nextflow:
            build_parser.error("Repository argument is available only when building a Nextflow pipeline. Did you mean 'dx build --nextflow'?")

        if args.repository and args.remote:
            build_parser.error("Nextflow pipeline built from a remote Git repository is always built using the Nextflow Pipeline Importer app. This is not compatible with --remote.")

        if args.git_credentials and not args.repository:
            build_parser.error("Git credentials can be supplied only when building Nextflow pipeline from a Git repository.")

        if args.nextflow and args.mode == "app":
            build_parser.error("Building Nextflow apps is not supported. Build applet instead.")

        # options not supported by workflow building

        if args.mode == "workflow":
            unsupported_options = {
                '--ensure-upload': args.ensure_upload,
                '--force-symlinks': args.force_symlinks,
                '--[no-]publish': args.publish,
                '--[no-]dry_run': args.dry_run,
                '--run': args.run,
                '--remote': args.remote,
                '--version': args.version_override,
                '--bill-to': args.bill_to,
                '--archive': args.archive,
                #TODO: Handle the options below, they are always set to
                # True by default and will be currently silently ignored
                #'--[no-]watch': args.watch,
                #'--parallel-build': args.parallel_build,
                #'--[no]version-autonumbering': args.version_autonumbering,
                #'--[no]update': args.update,
                '--region': args.region,
                '--extra-args': args.extra_args}
            used_unsupported_options = {k: v for k, v in list(unsupported_options.items()) if v}
            if used_unsupported_options:
                build_parser.error("Options {} are not supported with workflows"
                                   .format(", ".join(used_unsupported_options)))

    args = build_parser.parse_args()

    if dxpy.AUTH_HELPER is None and not args.dry_run:
        build_parser.error('Authentication required to build an executable on the platform; please run "dx login" first')

    try:
        args.src_dir = get_validated_source_dir(args)

        if args._from is not None:
            args._from = get_source_exec_desc(args._from)

        # If mode is not specified, determine it by the json file or by --from
        if args.mode is None:
            args.mode = get_mode(args)

        handle_arg_conflicts(args)
        if args.mode in ("app", "applet"):
            dx_build_app.build(args)
        elif args.mode in ("workflow", "globalworkflow"):
            workflow_builder.build(args, build_parser)
        else:
            msg = "Unrecognized mode. Accepted options: --app, --applet, --workflow, --globalworkflow."
            msg += " If not provided, an attempt is made to build either an applet or a workflow, depending on"
            msg += " whether a dxapp.json or dxworkflow.json file is found in the source directory, respectively."
            build_parser.error(msg)
    except Exception as e:
        print("Error: {}".format(e), file=sys.stderr)
        err_exit()


def process_list_of_usernames(thing):
    return ['user-' + name.lower() if name != 'PUBLIC' and
            not name.startswith('org-') and
            not name.startswith('user-')
            else name
            for name in thing]

def add_users(args):
    desc = try_call(resolve_global_executable, args.app)
    args.users = process_list_of_usernames(args.users)

    try:
        if desc['class'] == 'app':
            dxpy.api.app_add_authorized_users(desc['id'], input_params={"authorizedUsers": args.users})
        else:
            dxpy.api.global_workflow_add_authorized_users(desc['id'], input_params={"authorizedUsers": args.users})
    except:
        err_exit()

def remove_users(args):
    desc = try_call(resolve_global_executable, args.app)
    args.users = process_list_of_usernames(args.users)

    try:
        if desc['class'] == 'app':
            dxpy.api.app_remove_authorized_users(desc['id'], input_params={"authorizedUsers": args.users})
        else:
            dxpy.api.global_workflow_remove_authorized_users(desc['id'], input_params={"authorizedUsers": args.users})
    except:
        err_exit()

def list_users(args):
    desc = try_call(resolve_global_executable, args.app)
    users = desc['authorizedUsers']

    for user in users:
        print(user)

def add_developers(args):
    desc = try_call(resolve_global_executable, args.app)
    args.developers = process_list_of_usernames(args.developers)

    try:
        if desc['class'] == 'app':
            dxpy.api.app_add_developers(desc['id'], input_params={"developers": args.developers})
        else:
            dxpy.api.global_workflow_add_developers(desc['id'], input_params={"developers": args.developers})
    except:
        err_exit()

def list_developers(args):
    desc = try_call(resolve_global_executable, args.app)

    try:
        if desc['class'] == 'app':
            developers = dxpy.api.app_list_developers(desc['id'])['developers']
        else:
            developers = dxpy.api.global_workflow_list_developers(desc['id'])['developers']

        for d in developers:
            print(d)
    except:
        err_exit()

def render_timestamp(epochSeconds):
    # This is the format used by 'aws s3 ls'
    return datetime.datetime.fromtimestamp(epochSeconds//1000).strftime('%Y-%m-%d %H:%M:%S')


def list_database_files(args):
    try:
        # check if database was given as an object hash id
        if is_hashid(args.database):
            desc = dxpy.api.database_describe(args.database)
            entity_result = {"id": desc["id"], "describe": desc}
        else:
        # otherwise it was provided as a path, so try and resolve
            project, _folderpath, entity_result = try_call(resolve_existing_path,
                                                           args.database,
                                                           expected='entity')

        # if we couldn't resolved the entity, fail
        if entity_result is None:
            err_exit('Could not resolve ' + args.database + ' to a data object', 3)
        else:
        # else check and verify that the found entity is a database object
            entity_result_class = entity_result['describe']['class']
            if entity_result_class != 'database':
                err_exit('Error: The given object is of class ' + entity_result_class +
                 ' but an object of class database was expected', 3)
            
        results = dxpy.api.database_list_folder(
            entity_result['id'],
            input_params={"folder": args.folder, "recurse": args.recurse, "timeout": args.timeout})
        for r in results["results"]:
            date_str = render_timestamp(r["modified"]) if r["modified"] != 0 else ''
            if (args.csv == True):
                print("{}{}{}{}{}".format(
                    date_str, DELIMITER(","), r["size"], DELIMITER(","), r["path"]))
            else:
                print("{}{}{}{}{}".format(
                    date_str.rjust(19), DELIMITER(" "), str(r["size"]).rjust(12), DELIMITER(" "), r["path"]))
    except:
        err_exit()

def remove_developers(args):
    desc = try_call(resolve_global_executable, args.app)
    args.developers = process_list_of_usernames(args.developers)

    try:
        if desc['class'] == 'app':
            dxpy.api.app_remove_developers(desc['id'], input_params={"developers": args.developers})
        else:
            dxpy.api.global_workflow_remove_developers(desc['id'], input_params={"developers": args.developers})
    except:
        err_exit()


def install(args):
    app_desc = try_call(resolve_app, args.app)

    try:
        dxpy.api.app_install(app_desc['id'])
        print('Installed the ' + app_desc['name'] + ' app')
    except:
        err_exit()

def uninstall(args):
    app_desc = get_app_from_path(args.app)
    if app_desc:
        try_call(dxpy.api.app_uninstall, app_desc['id'])
    else:
        user_data = dxpy.api.user_describe(dxpy.whoami(), {"fields": {"appsInstalled": True}})
        if args.app in user_data['appsInstalled']:
            args.app = 'app-' + args.app
        if args.app.startswith('app-'):
            try_call(dxpy.api.app_uninstall, args.app)
            print('Uninstalled the {app} app'.format(app=args.app))
        else:
            err_exit('Could not find the app', 3)

def _get_input_for_run(args, executable, preset_inputs=None, input_name_prefix=None):
    """
    Returns an input dictionary that can be passed to executable.run()
    """
    # The following may throw if the executable is a workflow with no
    # input spec available (because a stage is inaccessible)
    exec_inputs = try_call(ExecutableInputs,
                           executable,
                           input_name_prefix=input_name_prefix,
                           active_region=args.region)

    # Use input and system requirements from a cloned execution
    if args.input_json is None and args.filename is None:
        # --input-json and --input-json-file completely override input
        # from the cloned job
        exec_inputs.update(args.input_from_clone, strip_prefix=False)

    # Update with inputs passed to the this function
    if preset_inputs is not None:
        exec_inputs.update(preset_inputs, strip_prefix=False)

    # Update with inputs passed with -i, --input_json, --input_json_file, etc.
    # If batch_tsv is set, do not prompt for missing arguments
    require_all_inputs = (args.batch_tsv is None)
    try_call(exec_inputs.update_from_args, args, require_all_inputs)

    return exec_inputs.inputs

def run_one(args, executable, dest_proj, dest_path, input_json, run_kwargs):
    # Print inputs used for the run
    if not args.brief:
        print()
        print('Using input JSON:')
        print(json.dumps(input_json, indent=4))
        print()

    # Ask for confirmation if a tty and if input was not given as a
    # single JSON.
    if args.confirm and INTERACTIVE_CLI:
        if not prompt_for_yn('Confirm running the executable with this input', default=True):
            parser.exit(0)

    if not args.brief:
        print(fill("Calling " + executable.get_id() + " with output destination " + dest_proj + ":" + dest_path,
                   subsequent_indent='  ') + '\n')

    # Run the executable
    try:
        dxexecution = executable.run(input_json, **run_kwargs)
        if not args.brief:
            print(dxexecution._class.capitalize() + " ID: " + dxexecution.get_id())
        else:
            print(dxexecution.get_id())
        sys.stdout.flush()

        if args.wait:
            dxexecution.wait_on_done()
        elif args.confirm and INTERACTIVE_CLI and not (args.watch or args.ssh) and isinstance(dxexecution, dxpy.DXJob):
            answer = input("Watch launched job now? [Y/n] ")
            if len(answer) == 0 or answer.lower()[0] == 'y':
                args.watch = True

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
    except PermissionDenied as e:
        if run_kwargs.get("detach") and os.environ.get("DX_RUN_DETACH") == "1" and "detachedJob" in e.msg:
            print("Unable to start detached job in given project. "
                  "To disable running jobs as detached by default, please unset the environment variable DX_RUN_DETACH ('unset DX_RUN_DETACH')")
        raise(e)
    except Exception:
        err_exit()

    return dxexecution


def run_batch_all_steps(args, executable, dest_proj, dest_path, input_json, run_kwargs):
    if (args.wait or
        args.watch or
        args.ssh or
        args.ssh_proxy or
        args.clone):
        raise Exception("Options {wait, watch, ssh, ssh_proxy, clone} do not work with batch execution")

    b_args = batch_launch_args(executable, input_json, args.batch_tsv)

    if not args.brief:
        # print all the table rows we are going to run
        print('Batch run, calling executable with arguments:')
        for d in b_args["launch_args"]:
            print(json.dumps(d, indent=4))
        print()

    # Ask for confirmation if a tty and if input was not given as a
    # single JSON.
    if args.confirm and INTERACTIVE_CLI:
        if not prompt_for_yn('Confirm running the executable with this input', default=True):
            parser.exit(0)

    if not args.brief:
        print(fill("Calling " + executable.get_id() + " with output destination " + dest_proj + ":" + dest_path,
                   subsequent_indent='  ') + '\n')

    # Run the executable on all the input dictionaries
    dx_execs = batch_run(executable, b_args, run_kwargs, args.batch_folders)
    exec_ids = [dxe.get_id() for dxe in dx_execs]
    print(",".join(exec_ids))
    sys.stdout.flush()

# Shared code for running an executable ("dx run executable"). At the end of this method,
# there is a fork between the case of a single executable, and a batch run.
def run_body(args, executable, dest_proj, dest_path, preset_inputs=None, input_name_prefix=None):
    input_json = _get_input_for_run(args, executable, preset_inputs)

    if args.sys_reqs_from_clone and not isinstance(args.instance_type, str):
        args.instance_type = dict({stage: reqs['instanceType'] for stage, reqs in list(args.sys_reqs_from_clone.items())},
                                  **(args.instance_type or {}))

    if args.sys_reqs_from_clone and not isinstance(args.instance_count, str):
        # extract instance counts from cloned sys reqs and override them with args provided with "dx run"
        args.instance_count = dict({fn: reqs['clusterSpec']['initialInstanceCount']
                                        for fn, reqs in list(args.sys_reqs_from_clone.items()) if 'clusterSpec' in reqs},
                                   **(args.instance_count or {}))

    executable_describe = None
    srd_cluster_spec = SystemRequirementsDict(None)
    if args.instance_count is not None:
        executable_describe = executable.describe()
        srd_default = SystemRequirementsDict.from_sys_requirements(
            executable_describe['runSpec'].get('systemRequirements', {}), _type='clusterSpec')
        srd_requested = SystemRequirementsDict.from_instance_count(args.instance_count)
        srd_cluster_spec = srd_default.override_cluster_spec(srd_requested)

    if args.debug_on:
        if 'All' in args.debug_on:
            args.debug_on = ['AppError', 'AppInternalError', 'ExecutionError']

    run_kwargs = {
        "project": dest_proj,
        "folder": dest_path,
        "name": args.name,
        "tags": args.tags,
        "properties": args.properties,
        "details": args.details,
        "depends_on": args.depends_on or None,
        "allow_ssh": args.allow_ssh,
        "ignore_reuse": args.ignore_reuse or None,
        "ignore_reuse_stages": args.ignore_reuse_stages or None,
        "debug": {"debugOn": args.debug_on} if args.debug_on else None,
        "delay_workspace_destruction": args.delay_workspace_destruction,
        "priority": args.priority,
        "instance_type": args.instance_type,
        "stage_instance_types": args.stage_instance_types,
        "stage_folders": args.stage_folders,
        "rerun_stages": args.rerun_stages,
        "cluster_spec": srd_cluster_spec.as_dict(),
        "detach": args.detach,
        "cost_limit": args.cost_limit,
        "rank": args.rank,
        "extra_args": args.extra_args
    }

    if isinstance(executable, dxpy.DXApplet) or isinstance(executable, dxpy.DXApp):
        run_kwargs["head_job_on_demand"] = args.head_job_on_demand

    if any([args.watch or args.ssh or args.allow_ssh]):
        if run_kwargs["priority"] in ["low", "normal"]:
            if not args.brief:
                print(fill(BOLD("WARNING") + ": You have requested that jobs be run under " +
                        BOLD(run_kwargs["priority"]) +
                        " priority, which may cause them to be restarted at any point, interrupting interactive work."))
                print()
        else: # if run_kwargs["priority"] is None
            run_kwargs["priority"] = "high"
    
    if run_kwargs["priority"] in ["low", "normal"] and not args.brief:
        special_access = set()
        executable_desc = executable_describe or executable.describe()
        write_perms = ['UPLOAD', 'CONTRIBUTE', 'ADMINISTER']
        def check_for_special_access(access_spec):
            if not access_spec:
                return
            if access_spec.get('developer'):
                special_access.add('access to apps as a developer')
            if access_spec.get('network'):
                special_access.add('Internet access')
            if access_spec.get('project') in write_perms or \
               access_spec.get('allProjects') in write_perms:
                special_access.add('write access to one or more projects')
        if isinstance(executable, dxpy.DXWorkflow):
            for stage_desc in executable_desc['stages']:
                stage_exec_desc = dxpy.describe(stage_desc['executable'])
                check_for_special_access(stage_exec_desc.get('access'))
        else:
            check_for_special_access(executable_desc.get('access'))
        if special_access:
            print(fill(BOLD("WARNING") + ": You have requested that jobs be run under " +
                       BOLD(run_kwargs["priority"]) +
                       " priority, which may cause them to be restarted at any point, but " +
                       "the executable you are trying to run has " +
                       "requested extra permissions (" + ", ".join(sorted(special_access)) + ").  " +
                       "Unexpected side effects or failures may occur if the executable has not " +
                       "been written to behave well when restarted."))
            print()

    if not args.brief:
        if isinstance(executable, dxpy.DXWorkflow):
            try:
                dry_run = dxpy.api.workflow_dry_run(executable.get_id(),
                                                    executable._get_run_input(input_json, **run_kwargs))
                # print which stages are getting rerun
                # Note: information may be out of date if the dryRun
                # is performed too soon after the candidate execution
                # has been constructed (and the jobs have not yet been
                # created in the system); this errs on the side of
                # assuming such stages will be re-run.
                num_cached_stages = len([stage for stage in dry_run['stages'] if
                                         'parentAnalysis' in stage['execution'] and
                                         stage['execution']['parentAnalysis'] != dry_run['id']])
                if num_cached_stages > 0:
                    print(fill('The following ' + str(num_cached_stages) + ' stage(s) will reuse results from a previous analysis:'))
                    for i, stage in enumerate(dry_run['stages']):
                        if 'parentAnalysis' in stage['execution'] and \
                           stage['execution']['parentAnalysis'] != dry_run['id']:
                            stage_name = stage['execution']['name']
                            print('  Stage ' + str(i) + ': ' + stage_name + \
                                  ' (' + stage['execution']['id'] + ')')
                    print()
            except DXAPIError:
                # Just don't print anything for now if the dryRun
                # method is not yet available
                pass

    if args.batch_tsv is None:
        run_one(args, executable, dest_proj, dest_path, input_json, run_kwargs)
    else:
        run_batch_all_steps(args, executable, dest_proj, dest_path, input_json, run_kwargs)

def print_run_help(executable="", alias=None):
    if executable == "":
        parser_map['run'].print_help()
    else:
        exec_help = 'usage: dx run ' + executable + ('' if alias is None else ' --alias ' + alias)
        handler = try_call(get_exec_handler, executable, alias)

        is_app = isinstance(handler, dxpy.bindings.DXApp)
        is_global_workflow = isinstance(handler, dxpy.bindings.DXGlobalWorkflow)

        exec_desc = handler.describe()
        if is_global_workflow:
            current_project = dxpy.WORKSPACE_ID
            if not current_project:
                err_exit(exception=DXCLIError(
                    'A project must be selected. You can use "dx select" to select a project'))
            current_region = dxpy.api.project_describe(current_project,
                                                       input_params={"fields": {"region": True}})["region"]
            if current_region not in exec_desc['regionalOptions']:
                err_exit(exception=DXCLIError(
                    'The global workflow is not enabled in the current region. ' +
                    'Please run "dx select" to set the working project from one of the regions ' +
                    'the workflow is enabled in: {}'.format(
                       ",".join(list(exec_desc['regionalOptions'].keys())))
                ))
            exec_desc = handler.append_underlying_workflow_desc(exec_desc, current_region)

        exec_help += ' [-iINPUT_NAME=VALUE ...]\n\n'

        if is_app:
            exec_help += BOLD("App: ")
            exec_details = exec_desc.get('details', '')
        elif is_global_workflow:
            exec_help += BOLD("Global workflow: ")
            exec_details = exec_desc.get('details', '')
        else:
            exec_help += BOLD(exec_desc['class'].capitalize() + ": ")
            exec_details = handler.get_details()
        advanced_inputs = exec_details.get("advancedInputs", []) if isinstance(exec_details, dict) else []
        exec_help += exec_desc.get('title', exec_desc['name']) + '\n\n'
        if is_app or is_global_workflow:
            exec_help += BOLD("Version: ")
            exec_help += exec_desc.get('version')
            if int(exec_desc.get('published', -1)) > -1:
                exec_help += " (published)"
            else:
                exec_help += " (unpublished)"
            exec_help += '\n\n'
        summary = exec_desc.get('summary', '') or ''
        if summary != '':
            exec_help += fill(summary) + "\n\n"

        # Contact URL here
        #TODO: add a similar note for a global workflow
        if is_app:
            exec_help += "See the app page for more information:\n  https://platform.dnanexus.com/app/" + exec_desc['name'] +"\n\n"

        exec_help += BOLD("Inputs:")
        advanced_inputs_help = "Advanced Inputs:"
        if exec_desc.get('inputs') is not None or 'inputSpec' in exec_desc:
            input_spec = []
            if exec_desc.get('inputs') is not None:
                # workflow-level inputs were defined for the workflow
                input_spec = exec_desc['inputs']
            elif 'inputSpec' in exec_desc:
                input_spec = exec_desc['inputSpec']

            if len(input_spec) == 0:
                exec_help += " <none>\n"
            else:
                for group, params in list(group_array_by_field(input_spec).items()):
                    if group is not None:
                        exec_help += "\n " + BOLD(group)
                    for param in params:
                        param_string = "\n  "
                        param_string += UNDERLINE(param.get('label', param['name'])) + ": "
                        param_string += get_io_desc(param, app_help_version=True) + "\n"
                        helpstring = param.get('help', '')

                        stanzas = []

                        if 'choices' in param:
                            stanzas.append(format_choices_or_suggestions('Choices:',
                                                                         param['choices'],
                                                                         param['class']))
                        if helpstring != '':
                            stanzas.append(fill(helpstring, initial_indent='        ', subsequent_indent='        '))

                        if param.get('suggestions'):
                            stanzas.append(format_choices_or_suggestions('Suggestions:',
                                                                         param['suggestions'],
                                                                         param['class']))
                        param_string += "\n\n".join(stanzas) + ("\n" if stanzas else "")

                        if param['name'] in advanced_inputs:
                            advanced_inputs_help += param_string
                        else:
                            exec_help += param_string
                if len(advanced_inputs) > 0:
                    exec_help += "\n" + advanced_inputs_help
        else:
            exec_help += " no specification provided"
        exec_help += "\n"

        exec_help += BOLD("Outputs:")
        if exec_desc.get('outputs') is not None or 'outputSpec' in exec_desc:
            output_spec = []
            if exec_desc.get('outputs') is not None:
                # workflow-level outputs were defined for the workflow
                output_spec = exec_desc['outputs']
            elif 'outputSpec' in exec_desc:
                output_spec = exec_desc['outputSpec']

            if len(output_spec) == 0:
                exec_help += " <none>\n"
            else:
                for param in output_spec:
                    exec_help += "\n  "
                    exec_help += UNDERLINE(param.get('label', param['name'])) + ": "
                    exec_help += get_io_desc(param) + "\n"
                    helpstring = param.get('help', '')
                    if helpstring != '':
                        exec_help += fill(helpstring,
                                          initial_indent='        ',
                                          subsequent_indent='        ') + "\n"
        else:
            exec_help += " no specification provided"

        pager(exec_help)

    parser.exit(0)

def print_run_input_help():
    print('Help: Specifying input for dx run\n')
    print(fill('There are several ways to specify inputs.  In decreasing order of precedence, they are:'))
    print('''
  1) inputs given in the interactive mode
  2) inputs listed individually with the -i/--input command line argument
  3) JSON given in --input-json
  4) JSON given in --input-json-file
  5) if cloning a job with --clone, the input that the job was run with
     (this will get overridden completely if -j/--input-json or
      -f/--input-json-file are provided)
  6) default values set in a workflow or an executable's input spec
''')
    print('SPECIFYING INPUTS BY NAME\n\n' + fill('Use the -i/--input flag to specify each input field by ' + BOLD('name') + ' and ' + BOLD('value') + '.', initial_indent='  ', subsequent_indent='  '))
    print('''
    Syntax :  -i<input name>=<input value>
    Example:  dx run myApp -inum=34 -istr=ABC -ifiles=reads1.fq.gz -ifiles=reads2.fq.gz
''')
    print(fill('The example above runs an app called "myApp" with 3 inputs called num (class int), str (class string), and files (class array:file).  (For this method to work, the app must have an input spec so inputs can be interpreted correctly.)  The same input field can be used multiple times if the input class is an array.', initial_indent='  ', subsequent_indent='  '))
    print('\n' + fill(BOLD('Job-based object references') + ' can also be provided using the <job id>:<output name> syntax:', initial_indent='  ', subsequent_indent='  '))
    print('''
    Syntax :  -i<input name>=<job id>:<output name>
    Example:  dx run mapper -ireads=job-B0fbxvGY00j9jqGQvj8Q0001:reads
''')
    print(fill('You can ' + BOLD('extract an element of an array output') +
               ' using the <job id>:<output name>.<element> syntax:',
               initial_indent='  ', subsequent_indent='  '))
    print('''
    Syntax :  -i<input name>=<job id>:<output name>.<element>
    Example:  dx run mapper -ireadsfile=job-B0fbxvGY00j9jqGQvj8Q0001:reads.1
              # Extracts second element of array output
''')
    print(fill('When executing ' + BOLD('workflows') + ', stage inputs can be specified using the <stage key>.<input name>=<value> syntax:', initial_indent='  ', subsequent_indent='  '))

    print('''
    Syntax :  -i<stage key>.<input name>=<input value>
    Example:  dx run my_workflow -i0.reads="My reads file"
''')

    print(fill('<stage key> may be either the ID of the stage, name of the stage, or the number of the stage in the workflow (0 indicates first stage)'))

    print(fill('If the ' + BOLD('workflow') + ' has explicit, workflow-level inputs, input values must be passed to these workflow-level input fields using the <workflow input name>=<value> syntax:', initial_indent='  ', subsequent_indent='  '))
    print('''
    Syntax :  -i<workflow input name>=<input value>
    Example:  dx run my_workflow -ireads="My reads file"

SPECIFYING JSON INPUT
''')
    print(fill('JSON input can be used directly using the -j/--input-json or -f/--input-json-file flags.  When running an ' + BOLD('app') + ' or ' + BOLD('applet') + ', the keys should be the input field names for the app or applet.  When running a ' + BOLD('workflow') + ', the keys should be the input field names for each stage, prefixed by the stage key and a period, e.g. "my_stage.reads" for the "reads" input of stage "my_stage".', initial_indent='  ', subsequent_indent='  ') + '\n')
    parser.exit(0)


def run(args):
    if args.help:
        print_run_help(args.executable, args.alias)
    client_ip = None
    if args.allow_ssh is not None:
        # --allow-ssh without IP retrieves client IP
        if any(ip is None for ip in args.allow_ssh):
            args.allow_ssh = list(filter(None, args.allow_ssh))
            client_ip = get_client_ip()
            args.allow_ssh.append(client_ip)
    if args.allow_ssh is None and ((args.ssh or args.debug_on) and not args.allow_ssh):
        client_ip = get_client_ip()
        args.allow_ssh = [client_ip]
    if args.ssh_proxy and not args.ssh:
        err_exit(exception=DXCLIError("Option --ssh-proxy cannot be specified without --ssh"))
    if args.ssh_proxy:
        args.allow_ssh.append(args.ssh_proxy.split(':'[0]))
    if args.ssh or args.allow_ssh or args.debug_on:
        verify_ssh_config()
    if not args.brief and client_ip is not None:
        print("Detected client IP as '{}'. Setting allowed IP ranges to '{}'. To change the permitted IP addresses use --allow-ssh.".format(client_ip, ', '.join(args.allow_ssh)))

    try_call(process_extra_args, args)
    try_call(process_properties_args, args)

    if args.clone is None and args.executable == "":
        err_exit(parser_map['run'].format_help() +
                 fill("Error: Either the executable must be specified, or --clone must be used to indicate a job or analysis to clone"), 2)

    args.input_from_clone, args.sys_reqs_from_clone = {}, {}

    dest_proj, dest_path = None, None

    if args.project is not None:
        if args.folder is not None:
            err_exit(exception=DXCLIError(
                "Options --project and --folder/--destination cannot be specified together.\nIf specifying both a project and a folder, please include them in the --folder option."
            ))
        dest_proj = resolve_container_id_or_name(args.project, is_error=True, multi=False)

    if args.folder is not None:
        dest_proj, dest_path, _none = try_call(resolve_existing_path,
                                               args.folder,
                                               expected='folder')

    # at this point, allow the --clone options to set the destination
    # project and path if available

    # Process the --stage-output-folder and
    # --stage-relative-output-folder options if provided
    if args.stage_output_folder or args.stage_relative_output_folder:
        stage_folders = {}
        for stage, stage_folder in args.stage_output_folder:
            _proj, stage_folder, _none = try_call(resolve_existing_path,
                                                  stage_folder,
                                                  expected='folder')
            stage_folders[stage] = stage_folder
        for stage, stage_folder in args.stage_relative_output_folder:
            stage_folders[stage] = stage_folder.lstrip('/')
        if stage_folders:
            args.stage_folders = stage_folders

    clone_desc = None
    if args.clone is not None:
        # Resolve job ID or name
        if is_job_id(args.clone) or is_analysis_id(args.clone):
            clone_desc = dxpy.api.job_describe(args.clone)
        else:
            iterators = []
            if ":" in args.clone:
                colon_pos = args.clone.find(":")
                try:
                    # Resolve args.clone[:args.clone.find(":")] to a project name or ID
                    # And find jobs in that with that name
                    proj_id = resolve_container_id_or_name(args.clone[:colon_pos])
                    if proj_id is not None:
                        execution_name_or_id = args.clone[colon_pos + 1:]
                        if is_job_id(execution_name_or_id) or is_analysis_id(execution_name_or_id):
                            clone_desc = dxpy.api.job_describe(execution_name_or_id)
                        else:
                            iterators.append(dxpy.find_executions(name=execution_name_or_id,
                                                                  describe={"io": False},
                                                                  project=proj_id))
                except:
                    pass

            if clone_desc is None:
                if dxpy.WORKSPACE_ID is not None:
                    try:
                        iterators.append(dxpy.find_jobs(name=args.clone,
                                                        describe={"io": False},
                                                        project=dxpy.WORKSPACE_ID))
                    except:
                        pass
                import itertools

                result_choice = paginate_and_pick(itertools.chain(*iterators),
                                                  (lambda result:
                                                       get_find_executions_string(result["describe"],
                                                                                  has_children=False,
                                                                                  single_result=True)))
                if result_choice == "none found":
                    err_exit("dx run --clone: No matching execution found. Please use a valid job or analysis name or ID.", 3)
                elif result_choice == "none picked":
                    err_exit('', 3)
                else:
                    clone_desc = dxpy.api.job_describe(result_choice["id"])

        if args.folder is None:
            dest_proj = dest_proj or clone_desc["project"]
            dest_path = clone_desc["folder"]

        # set name, tags, properties, and priority from the cloned
        # execution if the options have not been explicitly set
        if args.name is None:
            match_obj = re.search("\(re-run\)$", clone_desc["name"])
            if match_obj is None:
                args.name = clone_desc["name"] + " (re-run)"
            else:
                args.name = clone_desc["name"]
        for metadata in 'tags', 'properties', 'priority':
            if getattr(args, metadata) is None:
                setattr(args, metadata, clone_desc.get(metadata))

        if clone_desc['class'] == 'job':
            if args.executable == "":
                args.executable = clone_desc.get("applet", clone_desc.get("app", ""))
            args.input_from_clone = clone_desc["runInput"]
            args.sys_reqs_from_clone = clone_desc["systemRequirements"]
            if args.details is None:
                args.details = {
                    "clonedFrom": {
                        "id": clone_desc["id"],
                        "executable": clone_desc.get("applet", clone_desc.get("app", "")),
                        "project": clone_desc["project"],
                        "folder": clone_desc["folder"],
                        "name": clone_desc["name"],
                        "runInput": clone_desc["runInput"],
                        "systemRequirements": clone_desc["systemRequirements"]
                    }
                }
        else:
            if args.executable != "":
                error_mesg = "Workflow executable (\"{}\") cannot be provided when re-running an analysis with 'dx run --clone'. ".format(args.executable)
                error_mesg += "You can instead run 'dx run --clone {}', with other optional CLI arguments, to re-run the previously run analysis.".format(args.clone)
                err_exit(exception=DXParserError(error_mesg),
                         expected_exceptions=(DXParserError,))
            # make a temporary workflow
            args.executable = dxpy.api.workflow_new({"project": dest_proj,
                                                     "initializeFrom": {"id": clone_desc["id"]},
                                                     "temporary": True})["id"]

    handler = try_call(get_exec_handler, args.executable, args.alias)

    is_workflow = isinstance(handler, dxpy.DXWorkflow)
    is_global_workflow = isinstance(handler, dxpy.DXGlobalWorkflow)

    if args.depends_on and (is_workflow or is_global_workflow):
        err_exit(exception=DXParserError("-d/--depends-on cannot be supplied when running workflows."),
                 expected_exceptions=(DXParserError,))
    if args.head_job_on_demand and (is_workflow or is_global_workflow):
        err_exit(exception=DXParserError("--head-job-on-demand cannot be used when running workflows"),
                 expected_exceptions=(DXParserError,))

    # if the destination project has still not been set, use the
    # current project
    if dest_proj is None:
        dest_proj = dxpy.WORKSPACE_ID
        if dest_proj is None:
            err_exit(exception=DXCLIError(
                'Unable to find project to run the app in. ' +
                'Please run "dx select" to set the working project, or use --folder=project:path'
            ))


    # Get region from the project context
    args.region = None
    if is_global_workflow:
        args.region = dxpy.api.project_describe(dest_proj,
                                                input_params={"fields": {"region": True}})["region"]
    if not args.detach:
        args.detach = os.environ.get("DX_RUN_DETACH") == "1"

    # if the destination path has still not been set, use the current
    # directory as the default; but only do this if not running a
    # workflow with outputFolder already set
    if dest_path is None:
        if is_workflow:
            dest_path = getattr(handler, 'outputFolder', None)
        if dest_path is None:
            dest_path = dxpy.config.get('DX_CLI_WD', '/')

    process_instance_type_arg(args, is_workflow or is_global_workflow)

    # Validate and process instance_count argument
    if args.instance_count:
        if is_workflow or is_global_workflow:
            err_exit(exception=DXCLIError(
                '--instance-count is not supported for workflows'
            ))
        process_instance_count_arg(args)

    run_body(args, handler, dest_proj, dest_path)

def terminate(args):
    for jobid in args.jobid:
        try:
            dxpy.api.job_terminate(jobid)
        except:
            err_exit()

def watch(args):
    level_colors = {level: RED() for level in ("EMERG", "ALERT", "CRITICAL", "ERROR")}
    level_colors.update({level: YELLOW() for level in ("WARNING", "STDERR")})
    level_colors.update({level: GREEN() for level in ("NOTICE", "INFO", "DEBUG", "STDOUT")})

    msg_callback, log_client = None, None
    if args.get_stdout:
        args.levels = ['STDOUT']
        args.format = "{msg}"
        args.job_info = False
    elif args.get_stderr:
        args.levels = ['STDERR']
        args.format = "{msg}"
        args.job_info = False
    elif args.get_streams:
        args.levels = ['STDOUT', 'STDERR']
        args.format = "{msg}"
        args.job_info = False
    elif args.format is None:
        if args.job_ids:
            args.format = BLUE("{job_name} ({job})") + " {level_color}{level}" + ENDC() + " {msg}"
        else:
            args.format = BLUE("{job_name}") + " {level_color}{level}" + ENDC() + " {msg}"
        if args.timestamps:
            args.format = "{timestamp} " + args.format

        def msg_callback(message):
            message['timestamp'] = str(datetime.datetime.fromtimestamp(message.get('timestamp', 0)//1000))
            message['level_color'] = level_colors.get(message.get('level', ''), '')
            message['job_name'] = log_client.seen_jobs[message['job']]['name'] if message['job'] in log_client.seen_jobs else message['job']
            print(args.format.format(**message))

    from dxpy.utils.job_log_client import DXJobLogStreamClient

    input_params = {"numRecentMessages": args.num_recent_messages,
                    "recurseJobs": args.tree,
                    "tail": args.tail}

    if args.levels:
        input_params['levels'] = args.levels

    if not re.match("^job-[0-9a-zA-Z]{24}$", args.jobid):
        err_exit(args.jobid + " does not look like a DNAnexus job ID")

    job_describe = dxpy.describe(args.jobid)
    if 'outputReusedFrom' in job_describe and job_describe['outputReusedFrom'] is not None:
      args.jobid = job_describe['outputReusedFrom']
      if not args.quiet:
        print("Output reused from %s" %(args.jobid))

    log_client = DXJobLogStreamClient(args.jobid, input_params=input_params, msg_callback=msg_callback,
                                      msg_output_format=args.format, print_job_info=args.job_info)

    # Note: currently, the client is synchronous and blocks until the socket is closed.
    # If this changes, some refactoring may be needed below
    try:
        if not args.quiet:
            print("Watching job %s%s. Press Ctrl+C to stop watching." % (args.jobid, (" and sub-jobs" if args.tree else "")), file=sys.stderr)
        log_client.connect()
    except Exception as details:
        err_exit(fill(str(details)), 3)

def get_client_ip():
    return dxpy.api.system_whoami({"fields": {"clientIp": True}}).get('clientIp')

def ssh_config(args):
    user_id = try_call(dxpy.whoami)

    if args.revoke:
        dxpy.api.user_update(user_id, {"sshPublicKey": None})
        print(fill("SSH public key has been revoked"))
    else:
        dnanexus_conf_dir = dxpy.config.get_user_conf_dir()
        if not os.path.exists(dnanexus_conf_dir):
            msg = "The DNAnexus configuration directory {d} does not exist. Use {c} to create it."
            err_exit(msg.format(d=dnanexus_conf_dir, c=BOLD("dx login")))

        print(fill("Select an SSH key pair to use when connecting to DNAnexus jobs. The public key will be saved to your " +
                   "DNAnexus account (readable only by you). The private key will remain on this computer.") + "\n")

        key_dest = os.path.join(dnanexus_conf_dir, 'ssh_id')
        pub_key_dest = key_dest + ".pub"

        if os.path.exists(os.path.realpath(key_dest)) and os.path.exists(os.path.realpath(pub_key_dest)):
            print(BOLD("dx") + " is already configured to use the SSH key pair at:\n    {}\n    {}".format(key_dest,
                                                                                                           pub_key_dest))
            if pick(["Use this SSH key pair", "Select or create another SSH key pair..."]) == 1:
                os.remove(key_dest)
                os.remove(pub_key_dest)
            else:
                update_pub_key(user_id, pub_key_dest)
                return
        elif os.path.exists(key_dest) or os.path.exists(pub_key_dest):
            os.remove(key_dest)
            os.remove(pub_key_dest)

        keys = [k for k in glob.glob(os.path.join(os.path.expanduser("~/.ssh"), "*.pub")) if os.path.exists(k[:-4])]

        choices = ['Generate a new SSH key pair using ssh-keygen'] + keys + ['Select another SSH key pair...']
        choice = pick(choices, default=0)

        if choice == 0:
            try:
                subprocess.check_call(['ssh-keygen', '-f', key_dest] + args.ssh_keygen_args)
            except subprocess.CalledProcessError:
                err_exit("Unable to generate a new SSH key pair", expected_exceptions=(subprocess.CalledProcessError, ))
        else:
            if choice == len(choices) - 1:
                key_src = input('Enter the location of your SSH key: ')
                pub_key_src = key_src + ".pub"
                if os.path.exists(key_src) and os.path.exists(pub_key_src):
                    print("Using {} and {} as the key pair".format(key_src, pub_key_src))
                elif key_src.endswith(".pub") and os.path.exists(key_src[:-4]) and os.path.exists(key_src):
                    key_src, pub_key_src = key_src[:-4], key_src
                    print("Using {} and {} as the key pair".format(key_src, pub_key_src))
                else:
                    err_exit("Unable to find {k} and {k}.pub".format(k=key_src))
            else:
                key_src, pub_key_src = choices[choice][:-4], choices[choice]

            os.symlink(key_src, key_dest)
            os.symlink(pub_key_src, pub_key_dest)

        update_pub_key(user_id, pub_key_dest)

def update_pub_key(user_id, pub_key_file):
    with open(pub_key_file) as fh:
        pub_key = fh.read()
        dxpy.api.user_update(user_id, {"sshPublicKey": pub_key})

    print("Updated public key for user {}".format(user_id))
    print(fill("Your account has been configured for use with SSH. Use " + BOLD("dx run") + " with the --allow-ssh, " +
               "--ssh, or --debug-on options to launch jobs and connect to them."))

def verify_ssh_config():
    try:
        with open(os.path.join(dxpy.config.get_user_conf_dir(), 'ssh_id.pub')) as fh:
            user_desc = try_call(dxpy.api.user_describe, try_call(dxpy.whoami))
            if 'sshPublicKey' not in user_desc:
                raise DXError("User's SSH public key is not set")
            if fh.read() != user_desc['sshPublicKey']:
                raise DXError("Public key mismatch")
    except Exception as e:
        msg = RED("Warning:") + " Unable to verify configuration of your account for SSH connectivity: {}".format(e) + \
              ". SSH connection will likely fail. To set up your account for SSH, quit this command and run " + \
              BOLD("dx ssh_config") + ". Continue with the current command?"
        if not prompt_for_yn(fill(msg), default=False):
            err_exit(expected_exceptions=(IOError, DXError))

def ssh(args, ssh_config_verified=False):
    if not re.match("^job-[0-9a-zA-Z]{24}$", args.job_id):
        err_exit(args.job_id + " does not look like a DNAnexus job ID")
    job_desc = try_call(dxpy.describe, args.job_id)

    if job_desc['state'] in ['done', 'failed', 'terminated']:
        err_exit(args.job_id + " is in a terminal state, and you cannot connect to it")

    if not ssh_config_verified:
        verify_ssh_config()

    job_allow_ssh = job_desc.get('allowSSH', [])

    # Check requested IPs (--allow-ssh or client IP) against job's allowSSH field and update if necessary
    if not args.no_firewall_update:
        if args.allow_ssh is not None:
            args.allow_ssh = [i for i in args.allow_ssh if i is not None]
        else:
            # Get client IP from API if --allow-ssh not provided
            args.allow_ssh = [get_client_ip()]
        if args.ssh_proxy:
            args.allow_ssh.append(args.ssh_proxy.split(':')[0])
        # If client IP or args.allow_ssh already exist in job's allowSSH, skip firewall update
        if not all(ip in job_allow_ssh for ip in args.allow_ssh):
            # Append new IPs to existing job allowSSH
            for ip in args.allow_ssh:
                if ip not in job_allow_ssh:
                    job_allow_ssh.append(ip)
            sys.stdout.write("Updating allowed IP ranges for SSH to '{}'\n".format(', '.join(job_allow_ssh)))
            dxpy.api.job_update(object_id=args.job_id, input_params={"allowSSH": job_allow_ssh})

    sys.stdout.write("Waiting for {} to start...".format(args.job_id))
    sys.stdout.flush()
    while job_desc['state'] not in ['running', 'debug_hold']:
        time.sleep(1)
        job_desc = dxpy.describe(args.job_id)
        sys.stdout.write(".")
        sys.stdout.flush()
    sys.stdout.write("\n")

    sys.stdout.write("Resolving job hostname and SSH host key...")
    sys.stdout.flush()
    host, host_key, ssh_port = None, None, None
    for i in range(90):
        host = job_desc.get('host')
        host_key = job_desc.get('sshHostKey') or job_desc['properties'].get('ssh_host_rsa_key')
        ssh_port = job_desc.get('sshPort') or 22
        if host and host_key:
            break
        else:
            time.sleep(1)
            job_desc = dxpy.describe(args.job_id)
            sys.stdout.write(".")
            sys.stdout.flush()
    sys.stdout.write("\n")

    if not (host and host_key):
        msg = "Cannot resolve hostname or hostkey for {}. Please check your permissions and run settings."
        err_exit(msg.format(args.job_id))

    known_hosts_file = os.path.join(dxpy.config.get_user_conf_dir(), 'ssh_known_hosts')
    with open(known_hosts_file, 'a') as fh:
        fh.write("{job_id}.dnanex.us {key}\n".format(job_id=args.job_id, key=host_key.rstrip()))

    import socket
    connected = False
    sys.stdout.write("Checking connectivity to {}:{}".format(host, ssh_port))
    if args.ssh_proxy:
        proxy_args = args.ssh_proxy.split(':')
        sys.stdout.write(" through proxy {}".format(proxy_args[0]))
    sys.stdout.write("...")
    sys.stdout.flush()
    for i in range(20):
        try:
            if args.ssh_proxy:
            # Test connecting to host through proxy
                proxy_socket = socket.socket()
                proxy_socket.connect((proxy_args[0], int(proxy_args[1])))
                proxy_file = proxy_socket.makefile('r+')
                proxy_file.write('CONNECT {host}:{port} HTTP/1.0\r\nhost: {host}\r\n\r\n'
                                 .format(host=host, port=ssh_port))
            else:
                socket.create_connection((host, ssh_port), timeout=5)
            connected = True
            break
        except Exception:
            time.sleep(3)
            sys.stdout.write(".")
            sys.stdout.flush()
    if args.ssh_proxy:
    # Force close sockets to prevent memory leaks
        try:
            proxy_file.close()
        except:
            pass
        try:
            proxy_socket.close()
        except:
            pass
    if connected:
        sys.stdout.write(GREEN("OK") + "\n")
    else:
        msg = "Failed to connect to {h}. Please check your connectivity, verify your ssh client IP is added to job's allowedSSH list by describing the job and if needed, retry the command with additional --allow-ssh ADDRESS argument."
        err_exit(msg.format(h=host, job_id=args.job_id, cmd=BOLD("dx ssh {}".format(args.job_id))),
                 exception=DXCLIError())

    print("Connecting to {}:{}".format(host, ssh_port))
    ssh_args = ['ssh', '-i', os.path.join(dxpy.config.get_user_conf_dir(), 'ssh_id'),
                '-o', 'HostKeyAlias={}.dnanex.us'.format(args.job_id),
                '-o', 'UserKnownHostsFile={}'.format(known_hosts_file),
                '-p', str(ssh_port), '-l', 'dnanexus', host]
    if args.ssh_proxy:
        ssh_args += ['-o', 'ProxyCommand=nc -X connect -x {proxy} %h %p'.
                     format(proxy=args.ssh_proxy)]
    ssh_args += args.ssh_args
    exit_code = subprocess.call(ssh_args)
    try:
        job_desc = dxpy.describe(args.job_id)
        if args.check_running and job_desc['state'] == 'running':
            msg = "Job {job_id} is still running. Terminate now?".format(job_id=args.job_id)
            if prompt_for_yn(msg, default=False):
                dxpy.api.job_terminate(args.job_id)
                print("Terminated {}.".format(args.job_id))
    except default_expected_exceptions as e:
        tip = "Unable to check the state of {job_id}. Please check it and use " + BOLD("dx terminate {job_id}") + \
              " to stop it if necessary."
        print(fill(tip.format(job_id=args.job_id)))
    exit(exit_code)

def upgrade(args):
    if len(args.args) == 0:
        try:
            greeting = dxpy.api.system_greet({'client': 'dxclient', 'version': 'v'+dxpy.TOOLKIT_VERSION}, auth=None)
            if greeting['update']['available']:
                recommended_version = greeting['update']['version']
            else:
                err_exit("Your SDK is up to date.", code=0)
        except default_expected_exceptions as e:
            print(e)
            recommended_version = "current"
        print("Upgrading to", recommended_version)
        args.args = [recommended_version]

    try:
        cmd = os.path.join(os.environ['DNANEXUS_HOME'], 'build', 'upgrade.sh')
        args.args.insert(0, cmd)
        os.execv(cmd, args.args)
    except:
        err_exit()

def generate_batch_inputs(args):

    # Internally restricted maximum batch size for a TSV
    MAX_BATCH_SIZE = 500
    project, folder, _none = try_call(resolve_path, args.path, expected='folder')

    # Parse input values
    input_dict = dict([parse_input_keyval(keyeqval) for keyeqval in args.input])

    # Call API for batch expansion
    try:
       api_result = dxpy.api.system_generate_batch_inputs({"project":  project, "folder": folder, "inputs": input_dict})['batchInputs']
    except:
       err_exit()

    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    successful = [b for b in api_result if 'error' not in b]
    errors = [b for b in api_result if 'error' in b]

    batches = list(chunks(successful, MAX_BATCH_SIZE))

    eprint("Found {num_success} valid batch IDs matching desired pattern.".format(num_success=len(successful)))

    input_names = sorted(list(input_dict.keys()))

    # Output TSV Batch.  This procedure generates a TSV file with file names and IDs grouped by pattern
    for i,batch in enumerate(batches):
        def flatten_batch(b):
            return [b['batchPattern']] + [ival['name'] for iname, ival in sorted(b['inputs'].items())] + [project + ":" + ival['ids'][0] for iname, ival in sorted(b['inputs'].items())]

        batch_fname = "{}.{:04d}.tsv".format(args.output_prefix, i)

        # In python-3 we need to open the file in textual mode.
        if USING_PYTHON2:
            write_mode = 'wb'
            delimiter = '\t'.encode('ascii')
        else:
            write_mode = 'w'
            delimiter = '\t'

        with open(batch_fname, write_mode) as csvfile:
            batchwriter = csv.writer(csvfile, delimiter=delimiter)
            # Write headers of TSV
            headers = ['batch ID'] + [iname for iname in input_names] + [iname+" ID" for iname in input_names]
            batchwriter.writerow(headers)
            for bi in batch:
                batchwriter.writerow(flatten_batch(bi))
        eprint("Created batch file {}".format(batch_fname))


    eprint("")
    for bi in errors:
        eprint("ERROR processing batch ID matching pattern \"{id}\"".format(id=bi['batchPattern']))
        input_names_i = sorted(bi['inputs'].keys())
        if input_names !=  input_names_i:
            eprint("    Mismatched set of input names.")
            eprint("        Required input names: {required}".format(required=", ".join(input_names)))
            eprint("        Matched input names: {matched}".format(matched=", ".join(input_names_i)))
        for input_name, matches in list(bi['inputs'].items()):
            if len(matches['ids']) > 1:
                eprint("Input {iname} is associated with a file name that matches multiple IDs:".format(iname=input_name))
                eprint("    {fname} => {ids}".format(fname=matches['name'], ids=", ".join(matches['ids'])))
        eprint("")

    eprint("CREATED {num_batches} batch files each with at most {max} batch IDs.".format(num_batches=len(batches), max=MAX_BATCH_SIZE))
    if len(errors) > 0:
        err_exit("ERROR SUMMARY: Found {num_errors} batch IDs with incomplete or ambiguous results.  Details above.".format(num_errors=len(errors)), 3)

def publish(args):
    desc = try_call(resolve_global_executable, args.executable, is_version_required=True)

    try:
        if desc['class'] == 'app':
            dxpy.api.app_publish(desc['id'], input_params={"makeDefault": args.make_default})
        else:
            dxpy.api.global_workflow_publish(desc['id'], input_params={"makeDefault": args.make_default})

        eprint("Published {} successfully".format(args.executable))

        if desc['authorizedUsers']:
            eprint("It is now available to the authorized users: {}".format(", ".join(desc['authorizedUsers'])))

        eprint("You can add or remove users with:")
        eprint("  dx add users {} user-xxxx".format(desc['name']))
        eprint("  dx remove users {} user-yyyy".format(desc['name']))
    except:
        err_exit()

def archive(args):
    def send_archive_request(target_project, request_input, request_func):
        api_errors = [InvalidState, ResourceNotFound, PermissionDenied]
        try:
            res = request_func(target_project, request_input)
        except Exception as e:            
            eprint("Failed request: {}".format(request_input))
            if type(e) in api_errors:
                eprint("     API error: {}. {}".format(e.name, e.msg))
            else: 
                eprint("     Unexpected error: {}".format(format_exception(e)))
            
            err_exit("Failed request: {}. {}".format(request_input, format_exception(e)), code=3)
        return res              

    def get_valid_archival_input(args, target_files, target_folder, target_project):
        request_input = {}
        if target_files: 
            target_files = list(target_files)
            request_input = {"files": target_files}
        elif target_folder:
            request_input = {"folder": target_folder, "recurse":args.recurse}
        else:
            err_exit("No input file/folder is found in project {}".format(target_project), code=3)
        
        request_mode = args.request_mode    
        options = {}
        if request_mode == "archival":
            options = {"allCopies": args.all_copies}
            request_func = dxpy.api.project_archive
        elif request_mode == "unarchival":
            options = {"rate": args.rate}
            request_func = dxpy.api.project_unarchive        
        
        request_input.update(options)
        return request_mode, request_func, request_input

    def get_archival_paths(args):
        target_project = None
        target_folder = None
        target_files = set()
        
        paths = [split_unescaped(':', path, include_empty_strings=True) for path in args.path]
        possible_projects = set()
        possible_folder = set()
        possible_files = set()

        # Step 0: parse input paths into projects and objects
        for p in paths:
            if len(p)>2: 
                err_exit("Path '{}' is invalid. Please check the inputs or check --help for example inputs.".format(":".join(p)), code=3)
            elif len(p) == 2:
                possible_projects.add(p[0])
            elif len(p) == 1:
                possible_projects.add('')
            
            obj = p[-1]
            if obj[-1] == '/':
                folder, entity_name = clean_folder_path(('' if obj.startswith('/') else '/') + obj)
                if entity_name:
                    possible_files.add(obj)
                else:
                    possible_folder.add(folder)
            else:
                possible_files.add(obj)
        
        # Step 1: find target project
        for proj in possible_projects:
            # is project ID
            if is_container_id(proj) and proj.startswith('project-'):
                pass
            # is "": use current project
            elif proj == '':
                if not dxpy.PROJECT_CONTEXT_ID:
                    err_exit("Cannot find current project. Please check the environment.", code=3)
                proj = dxpy.PROJECT_CONTEXT_ID
            # name is given
            else:
                try:
                    project_results = list(dxpy.find_projects(name=proj, describe=True))
                except:
                    err_exit("Cannot find project with name {}".format(proj), code=3)
                
                if project_results:
                    choice = pick(["{} ({})".format(result['describe']['name'], result['id']) for result in project_results], allow_mult=False)
                    proj = project_results[choice]['id']
                else:
                    err_exit("Cannot find project with name {}".format(proj), code=3)

            if target_project and proj!= target_project:
                err_exit("All paths must refer to files/folder in a single project, but two project ids: '{}' and '{}' are given. ".format(
                                target_project, proj), code=3)
            elif not target_project:
                target_project = proj

        # Step 2: check 1) target project
        #               2) either one folder or a list of files
        if not target_project:
            err_exit('No target project has been set. Please check the input or check your permission to the given project.', code=3)
        if len(possible_folder) >1:
            err_exit("Only one folder is allowed for each request. Please check the inputs or check --help for example inputs.".format(p), code=3)
        if possible_folder and possible_files:
            err_exit('Expecting either a single folder or a list of files for each API request', code=3)
        
        # Step 3: assign target folder or target files
        if possible_folder:
            target_folder = possible_folder.pop()
        else:
            for fp in possible_files:
                # find a filename
                # is file ID
                if is_data_obj_id(fp) and fp.startswith("file-"):
                    target_files.add(fp)
                # is folderpath/filename
                else:
                    folderpath, filename = clean_folder_path(('' if obj.startswith('/') else '/') + fp)
                    try: 
                        file_results = list(dxpy.find_data_objects(classname="file", name=filename,project=target_project,folder=folderpath,describe=True,recurse=False))
                    except:
                        err_exit("Input '{}' is not found as a file in project '{}'".format(fp, target_project), code=3)
                    
                    if not file_results:
                        err_exit("Input '{}' is not found as a file in project '{}'".format(fp, target_project), code=3)
                    # elif file_results
                    if not args.all:
                        choice = pick([ "{} ({})".format(result['describe']['name'], result['id']) for result in file_results],allow_mult=True)
                        if choice == "*" :
                            target_files.update([file['id'] for file in file_results])
                        else:
                            target_files.add(file_results[choice]['id'])
                    else: 
                        target_files.update([file['id'] for file in file_results])
        
        return target_files, target_folder, target_project

    # resolve paths  
    target_files, target_folder, target_project = get_archival_paths(args)
    
    # set request command and add additional options
    request_mode, request_func, request_input = get_valid_archival_input(args, target_files, target_folder, target_project)
                 
    # ask for confirmation if needed
    if args.confirm and INTERACTIVE_CLI:
        if request_mode == "archival":
            if target_files:
                counts = len(target_files)
                print('Will tag {} file(s) for archival in {}'.format(counts,target_project))
            else: 
                print('Will tag file(s) for archival in folder {}:{} {}recursively'.format(target_project, target_folder, 'non-' if not args.recurse else ''))
        elif request_mode == "unarchival":
            dryrun_request_input = copy.deepcopy(request_input)
            dryrun_request_input.update(dryRun=True)
            dryrun_res = send_archive_request(target_project, dryrun_request_input, request_func)
            print('Will tag {} file(s) for unarchival in {}, totalling {} GB, costing ${}'.format(dryrun_res["files"], target_project, dryrun_res["size"],dryrun_res["cost"]/1000))

        if not prompt_for_yn('Confirm all paths?', default=True):
            parser.exit(0)
    
    # send request and display final results
    res = send_archive_request(target_project, request_input, request_func)
    
    if not args.quiet:
        print()
        if request_mode == "archival":
            print('Tagged {} file(s) for archival in {}'.format(res["count"],target_project))
        elif request_mode == "unarchival":
            print('Tagged {} file(s) for unarchival, totalling {} GB, costing ${}'.format(res["files"], res["size"],res["cost"]/1000))
        print()    

def print_help(args):
    if args.command_or_category is None:
        parser_help.print_help()
    elif args.command_or_category in parser_categories:
        print('dx ' + args.command_or_category + ': ' + parser_categories[args.command_or_category]['desc'].lstrip())
        print('\nCommands:\n')
        for cmd in parser_categories[args.command_or_category]['cmds']:
            print('  ' + cmd[0] + ' '*(18-len(cmd[0])) + fill(cmd[1], width_adjustment=-20, subsequent_indent=' '*20))
    elif args.command_or_category not in parser_map:
        err_exit('Unrecognized command: ' + args.command_or_category, 3)
    elif args.command_or_category == 'export' and args.subcommand is not None:
        if args.subcommand not in exporters:
            err_exit('Unsupported format for dx export: ' + args.subcommand, 3)
        new_args = argparse.Namespace()
        setattr(new_args, 'exporter_args', ['-h'])
        exporters[args.subcommand](new_args)
    elif args.command_or_category == 'run':
        if args.subcommand is None:
            parser_map[args.command_or_category].print_help()
        else:
            print_run_help(args.subcommand)
    elif args.subcommand is None:
        parser_map[args.command_or_category].print_help()
    elif (args.command_or_category + ' ' + args.subcommand) not in parser_map:
        err_exit('Unrecognized command and subcommand combination: ' + args.command_or_category + ' ' + args.subcommand, 3)
    else:
        parser_map[args.command_or_category + ' ' + args.subcommand].print_help()

def exit_shell(args):
    if state['interactive']:
        raise StopIteration()

class runHelp(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if namespace.executable is None:
            setattr(namespace, 'executable', '')
        setattr(namespace, 'help', True)

class runInputHelp(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        print_run_input_help()

class SetStagingEnv(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, 'host', 'stagingauth.dnanexus.com')
        setattr(namespace, 'port', '443')
        setattr(namespace, 'protocol', 'https')
        setattr(namespace, 'staging', True)
        set_api(protocol='https', host='stagingapi.dnanexus.com', port='443',
                write=(not state['interactive'] or namespace.save))

class PrintDXVersion(argparse.Action):
    # Prints to stdout instead of the default stderr that argparse
    # uses (note: default changes to stdout in 3.4)
    def __call__(self, parser, namespace, values, option_string=None):
        print('dx v%s' % (dxpy.TOOLKIT_VERSION,))
        parser.exit(0)

class PrintCategoryHelp(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):

        if parser.prog == "dx find globalworkflows":
            exectype = "workflows"
        else:
            exectype = "apps"

        print('usage: ' + parser.prog + ' --category CATEGORY')
        print()
        print(fill('List only the {et} that belong to a particular category by providing a category name.'.format(et=exectype)))
        print()
        print('Common category names include:')
        print('  ' + '\n  '.join(sorted(APP_CATEGORIES)))
        parser.exit(0)


# Callable "action" class used by the "dx new user" parser for org-related
# arguments to allow us to distinguish between user-specified arguments and
# default arguments. If an argument has a `default` that is a bool, then its
# `nargs` will be 0.
#
# PRECONDITION: If an argument has a `default` that is a bool, then specifying
# that argument on the command-line must imply the logical opposite of its
# `default`.
class DXNewUserOrgArgsAction(argparse.Action):
    user_specified_opts = []

    def __init__(self, option_strings, dest, required=False, default=None,
                 nargs=None, **kwargs):
        if isinstance(default, bool):
            nargs = 0
        super(DXNewUserOrgArgsAction, self).__init__(
            option_strings=option_strings, dest=dest, required=required,
            default=default, nargs=nargs, **kwargs
        )

    # __call__ is only invoked when the user specifies this `option_string` on
    # the command-line.
    def __call__(self, parser, namespace, values, option_string):
        DXNewUserOrgArgsAction.user_specified_opts.append(option_string)
        if isinstance(self.default, bool):
            setattr(namespace, self.dest, not self.default)
        else:
            setattr(namespace, self.dest, values)


class DXArgumentParser(argparse.ArgumentParser):
    def _print_message(self, message, file=None):
        if message:
            if message.startswith("usage: dx [-h] [--version] command") and "dx: error: argument command: invalid choice:" in message:
                message = message.replace(", notebook","")
                message = message.replace(", loupe-viewer","")
            pager(message, file=file)

    def _check_value(self, action, value):
        # Override argparse.ArgumentParser._check_value to eliminate "u'x'" strings in output that result from repr()
        # calls in the original, and to line wrap the output

        # converted value must be one of the choices (if specified)
        if action.choices is not None and value not in action.choices:
            choices = fill("(choose from {})".format(", ".join(action.choices)))
            msg = "invalid choice: {choice}\n{choices}".format(choice=value, choices=choices)

            if len(args_list) == 1:
                from dxpy.utils import spelling_corrector
                suggestion = spelling_corrector.correct(value, action.choices)
                if suggestion in action.choices:
                    msg += "\n\nDid you mean: " + BOLD("dx " + suggestion)

            err = argparse.ArgumentError(action, msg)
            if USING_PYTHON2:
                err.message = err.message.encode(sys_encoding)
                if err.argument_name is not None:
                    err.argument_name = err.argument_name.encode(sys_encoding)
            raise err

    def exit(self, status=0, message=None):
        if isinstance(status, basestring):
            message = message + status if message else status
            status = 1
        if message:
            self._print_message(message, sys.stderr)
        sys.exit(status)

    def error(self, message):
        if USING_PYTHON2:
            message = message.decode(sys_encoding)
        self.exit(2, '{help}\n{prog}: error: {msg}\n'.format(help=self.format_help(),
                                                             prog=self.prog,
                                                             msg=message))


def register_parser(parser, subparsers_action=None, categories=('other', ), add_help=True):
    """Attaches `parser` to the global ``parser_map``. If `add_help` is truthy,
    then adds the helpstring of `parser` into the output of ``dx help...``, for
    each category in `categories`.

    :param subparsers_action: A special action object that is returned by
    ``ArgumentParser.add_subparsers(...)``, or None.
    :type subparsers_action: argparse._SubParsersAction, or None.
    """
    name = re.sub('^dx ', '', parser.prog)
    if subparsers_action is None:
        subparsers_action = subparsers
    if isinstance(categories, basestring):
        categories = (categories, )

    parser_map[name] = parser
    if add_help:
        _help = subparsers_action._choices_actions[-1].help
        parser_categories['all']['cmds'].append((name, _help))
        for category in categories:
            parser_categories[category]['cmds'].append((name, _help))

def positive_integer(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue

parser = DXArgumentParser(description=DNANEXUS_LOGO() + ' Command-Line Client, API v%s, client v%s' % (dxpy.API_VERSION, dxpy.TOOLKIT_VERSION) + '\n\n' + fill('dx is a command-line client for interacting with the DNAnexus platform.  You can log in, navigate, upload, organize and share your data, launch analyses, and more.  For a quick tour of what the tool can do, see') + '\n\n  https://documentation.dnanexus.com/getting-started/tutorials/cli-quickstart#quickstart-for-cli\n\n' + fill('For a breakdown of dx commands by category, run "dx help".') + '\n\n' + fill('dx exits with exit code 3 if invalid input is provided or an invalid operation is requested, and exit code 1 if an internal error is encountered.  The latter usually indicate bugs in dx; please report them at') + "\n\n  https://github.com/dnanexus/dx-toolkit/issues",
                          formatter_class=argparse.RawTextHelpFormatter,
                          parents=[env_args],
                          usage='%(prog)s [-h] [--version] command ...')
parser.add_argument('--version', action=PrintDXVersion, nargs=0, help="show program's version number and exit")

subparsers = parser.add_subparsers(help=argparse.SUPPRESS, dest='command')
subparsers.metavar = 'command'

#####################################
# login
#####################################
parser_login = subparsers.add_parser('login', help='Log in (interactively or with an existing API token)',
                                     description='Log in interactively and acquire credentials.  Use "--token" to log in with an existing API token.',
                                     prog='dx login', parents=[env_args])
parser_login.add_argument('--token', help='Authentication token to use')
host_action = parser_login.add_argument('--host', help='Log into the given auth server host (port must also be given)')
port_action = parser_login.add_argument('--port', type=int, help='Log into the given auth server port (host must also be given)')
protocol_action = parser_login.add_argument('--protocol', help='Used in conjunction with host and port arguments, gives the protocol to use when contacting auth server', default='https')
host_action.help = port_action.help = protocol_action.help = argparse.SUPPRESS
parser_login.add_argument('--noprojects', dest='projects', help='Do not print available projects', action='store_false')
parser_login.add_argument('--save', help='Save token and other environment variables for future sessions',
                          action='store_true')
parser_login.add_argument('--timeout', default='30d',
                          help='Timeout for this login token (in seconds, or use suffix s, m, h, d, w, M, y)')
parser_login.add_argument('--staging', nargs=0, help=argparse.SUPPRESS, action=SetStagingEnv)
parser_login.set_defaults(staging=False, func=login)
register_parser(parser_login, categories='session')

#####################################
# logout
#####################################
parser_logout = subparsers.add_parser('logout',
                                      help='Log out and remove credentials',
                                      description='Log out and remove credentials',
                                      prog='dx logout',
                                      parents=[env_args])
parser_logout.add_argument('--host', help='Log out of the given auth server host (port must also be given)')
parser_logout.add_argument('--port', type=int, help='Log out of the given auth server port (host must also be given)')
parser_logout.add_argument('--protocol', help='Used in conjunction with host and port arguments, gives the protocol to use when contacting auth server', default='https')
parser_logout.set_defaults(func=logout)
register_parser(parser_logout, categories='session')

#####################################
# exit
#####################################
parser_exit = subparsers.add_parser('exit', help='Exit out of the interactive shell',
                                    description='Exit out of the interactive shell', prog='dx exit')
parser_exit.set_defaults(func=exit_shell)
register_parser(parser_exit, categories='session')

#####################################
# whoami
#####################################
parser_whoami = subparsers.add_parser('whoami', help='Print the username of the current user',
                                      description='Print the username of the current user, ' +
                                                  'in the form "user-USERNAME"',
                                      prog='dx whoami',
                                      parents=[env_args])
parser_whoami.add_argument('--id', help='Print user ID instead of username', action='store_true', dest='user_id')
parser_whoami.set_defaults(func=whoami)
register_parser(parser_whoami, categories='session')

#####################################
# env
#####################################
parser_env = subparsers.add_parser('env', help='Print all environment variables in use',
                                   description=fill('Prints all environment variables in use as they have been resolved from environment variables and configuration files.  For more details, see') + '\n\nhttps://documentation.dnanexus.com/user/helpstrings-of-sdk-command-line-utilities#overriding-environment-variables',
                                   formatter_class=argparse.RawTextHelpFormatter, prog='dx env',
                                   parents=[env_args])
parser_env.add_argument('--bash', help=fill('Prints a list of bash commands to export the environment variables', width_adjustment=-14),
                        action='store_true')
parser_env.add_argument('--dx-flags', help=fill('Prints the dx options to override the environment variables', width_adjustment=-14),
                        action='store_true')
parser_env.set_defaults(func=env)
register_parser(parser_env, categories='session')

#####################################
# setenv
#####################################
parser_setenv = subparsers.add_parser('setenv',
                                      help='Sets environment variables for the session',
                                      description='Sets environment variables for communication with the API server',
                                      prog='dx setenv')
parser_setenv.add_argument('--noprojects', dest='projects', help='Do not print available projects', action='store_false')
parser_setenv.add_argument('--save', help='Save settings for future sessions.  Only one set of settings can be saved at a time.  Always set to true if login is run in a non-interactive session',
                           action='store_true')
parser_setenv.add_argument('--current', help='Do not prompt for new values and just save current settings for future sessions.  Overrides --save to be true.',
                           action='store_true')
parser_setenv.set_defaults(func=setenv)
register_parser(parser_setenv, categories='other')

#####################################
# clearenv
#####################################
parser_clearenv = subparsers.add_parser('clearenv', help='Clears all environment variables set by dx',
                                        description='Clears all environment variables set by dx.  More specifically, it removes local state stored in ~/.dnanexus_config/environment.  Does not affect the environment variables currently set in your shell.', prog='dx clearenv')
parser_clearenv.add_argument('--reset', help='Reset dx environment variables to empty values. Use this to avoid interference between multiple dx sessions when using shell environment variables.',
                             action='store_true')
parser_clearenv.set_defaults(func=clearenv, interactive=False)
register_parser(parser_clearenv, categories='session')

#####################################
# invite
#####################################
parser_invite = subparsers.add_parser('invite',
                                      help='Invite another user to a project or make it public',
                                      description='Invite a DNAnexus entity to a project. If the invitee is not recognized as a DNAnexus ID, it will be treated as a username, i.e. "dx invite alice : VIEW" is equivalent to inviting the user with user ID "user-alice" to view your current default project.',
                                      prog='dx invite',
                                      parents=[env_args])
parser_invite.add_argument('invitee', help='Entity to invite')
parser_invite.add_argument('project', help='Project to invite the invitee to', default=':', nargs='?')
parser_invite.add_argument('level', help='Permissions level the new member should have',
                           choices=['VIEW', 'UPLOAD', 'CONTRIBUTE', 'ADMINISTER'], default='VIEW', nargs='?')
parser_invite.add_argument('--no-email', dest='send_email', action='store_false', help='Disable email notifications to invitee')
parser_invite.set_defaults(func=invite)
# parser_invite.completer = TODO
register_parser(parser_invite, categories='other')

#####################################
# uninvite
#####################################
parser_uninvite = subparsers.add_parser('uninvite',
                                        help='Revoke others\' permissions on a project you administer',
                                        description='Revoke others\' permissions on a project you administer. If the entity is not recognized as a DNAnexus ID, it will be treated as a username, i.e. "dx uninvite alice :" is equivalent to revoking the permissions of the user with user ID "user-alice" to your current default project.',
                                        prog='dx uninvite',
                                        parents=[env_args])
parser_uninvite.add_argument('entity', help='Entity to uninvite')
parser_uninvite.add_argument('project', help='Project to revoke permissions from', default=':', nargs='?')
parser_uninvite.set_defaults(func=uninvite)
register_parser(parser_uninvite, categories='other')

#####################################
# ls
#####################################
parser_ls = subparsers.add_parser('ls', help='List folders and/or objects in a folder',
                                  description='List folders and/or objects in a folder',
                                  parents=[no_color_arg, delim_arg, env_args, stdout_args],
                                  prog='dx ls')
parser_ls.add_argument('-a', '--all', help='show hidden files', action='store_true')
ls_output_args = parser_ls.add_mutually_exclusive_group()
ls_output_args.add_argument('-l', '--long', dest='verbose', help='Alias for "verbose"', action='store_true')
parser_ls.add_argument('--obj', help='show only objects', action='store_true')
parser_ls.add_argument('--folders', help='show only folders', action='store_true')
parser_ls.add_argument('--full', help='show full paths of folders', action='store_true')
ls_path_action = parser_ls.add_argument('path', help='Folder (possibly in another project) to list the contents of, default is the current directory in the current project.  Syntax: projectID:/folder/path',
                                        nargs='?', default='.')
ls_path_action.completer = DXPathCompleter()
parser_ls.set_defaults(func=ls)
register_parser(parser_ls, categories='fs')

#####################################
# tree
#####################################
parser_tree = subparsers.add_parser('tree', help='List folders and objects in a tree',
                                    description='List folders and objects in a tree',
                                    parents=[no_color_arg, env_args],
                                    prog='dx tree')
parser_tree.add_argument('-a', '--all', help='show hidden files', action='store_true')
parser_tree.add_argument('-l', '--long', help='use a long listing format', action='store_true')
tree_path_action = parser_tree.add_argument('path', help='Folder (possibly in another project) to list the contents of, default is the current directory in the current project.  Syntax: projectID:/folder/path',
                                            nargs='?', default='.')
tree_path_action.completer = DXPathCompleter(expected='folder')
parser_tree.set_defaults(func=tree)
register_parser(parser_tree, categories='fs')

#####################################
# pwd
#####################################
parser_pwd = subparsers.add_parser('pwd', help='Print current working directory',
                                   description='Print current working directory',
                                   prog='dx pwd',
                                   parents=[env_args])
parser_pwd.set_defaults(func=pwd)
register_parser(parser_pwd, categories='fs')

#####################################
# select
#####################################
parser_select = subparsers.add_parser('select', help='List and select a project to switch to',
                                      description='Interactively list and select a project to switch to.  By default, only lists projects for which you have at least CONTRIBUTE permissions.  Use --public to see the list of public projects.',
                                      prog='dx select',
                                      parents=[env_args])
select_project_action = parser_select.add_argument('project', help='Name or ID of a project to switch to; if not provided a list will be provided for you',
                                                   nargs='?', default=None)
select_project_action.completer = DXPathCompleter(expected='project', include_current_proj=False)
parser_select.add_argument('--name', help='Name of the project (wildcard patterns supported)')
parser_select.add_argument('--level', choices=['VIEW', 'UPLOAD', 'CONTRIBUTE', 'ADMINISTER'],
                           help='Minimum level of permissions expected', default='CONTRIBUTE')
parser_select.add_argument('--public', help='Include ONLY public projects (will automatically set --level to VIEW)',
                           action='store_true')
parser_select.set_defaults(func=select, save=False)
register_parser(parser_select, categories='fs')

#####################################
# cd
#####################################
parser_cd = subparsers.add_parser('cd', help='Change the current working directory',
                                  description='Change the current working directory', prog='dx cd',
                                  parents=[env_args])
cd_path_action = parser_cd.add_argument('path', nargs='?', default='/',
                                        help='Folder (possibly in another project) to which to change the current working directory, default is "/" in the current project')
cd_path_action.completer = DXPathCompleter(expected='folder')
parser_cd.set_defaults(func=cd)
register_parser(parser_cd, categories='fs')

#####################################
# cp
#####################################
parser_cp = subparsers.add_parser('cp', help='Copy objects and/or folders between different projects',
                                  formatter_class=argparse.RawTextHelpFormatter,
                                  description=fill('Copy objects and/or folders between different projects.  Folders will automatically be copied recursively.  To specify which project to use as a source or destination, prepend the path or ID of the object/folder with the project ID or name and a colon.') + '''

EXAMPLES

  ''' + fill('The first example copies a file in a project called "FirstProj" to the current directory of the current project.  The second example copies the object named "reads.fq.gz" in the current directory to the folder /folder/path in the project with ID "project-B0VK6F6gpqG6z7JGkbqQ000Q", and finally renaming it to "newname.fq.gz".', width_adjustment=-2, subsequent_indent='  ') + '''

  $ dx cp FirstProj:file-B0XBQFygpqGK8ZPjbk0Q000q .
  $ dx cp reads.fq.gz project-B0VK6F6gpqG6z7JGkbqQ000Q:/folder/path/newname.fq.gz
''',
                                  prog='dx cp',
                                  parents=[env_args, all_arg])
cp_sources_action = parser_cp.add_argument('sources', help='Objects and/or folder names to copy', metavar='source',
                                           nargs='+')
cp_sources_action.completer = DXPathCompleter()
parser_cp.add_argument('destination', help=fill('Folder into which to copy the sources or new pathname (if only one source is provided).  Must be in a different project/container than all source paths.', width_adjustment=-15))
parser_cp.set_defaults(func=cp)
register_parser(parser_cp, categories='fs')

#####################################
# mv
#####################################
parser_mv = subparsers.add_parser('mv', help='Move or rename objects and/or folders inside a project',
                                  formatter_class=argparse.RawTextHelpFormatter,
                                  description=fill('Move or rename data objects and/or folders inside a single project.  To copy data between different projects, use \'dx cp\' instead.'),
                                  prog='dx mv',
                                  parents=[env_args, all_arg])
mv_sources_action = parser_mv.add_argument('sources', help='Objects and/or folder names to move', metavar='source',
                                           nargs='+')
mv_sources_action.completer = DXPathCompleter()
parser_mv.add_argument('destination', help=fill('Folder into which to move the sources or new pathname (if only one source is provided).  Must be in the same project/container as all source paths.', width_adjustment=-15))
parser_mv.set_defaults(func=mv)
register_parser(parser_mv, categories='fs')

#####################################
# mkdir
#####################################
parser_mkdir = subparsers.add_parser('mkdir', help='Create a new folder',
                                     description='Create a new folder', prog='dx mkdir',
                                     parents=[env_args])
parser_mkdir.add_argument('-p', '--parents', help='no error if existing, create parent directories as needed',
                          action='store_true')
mkdir_paths_action = parser_mkdir.add_argument('paths', help='Paths to folders to create', metavar='path', nargs='+')
mkdir_paths_action.completer = DXPathCompleter(expected='folder')
parser_mkdir.set_defaults(func=mkdir)
register_parser(parser_mkdir, categories='fs')

#####################################
# rmdir
#####################################
parser_rmdir = subparsers.add_parser('rmdir', help='Remove a folder',
                                     description='Remove a folder', prog='dx rmdir',
                                     parents=[env_args])
rmdir_paths_action = parser_rmdir.add_argument('paths', help='Paths to folders to remove', metavar='path', nargs='+')
rmdir_paths_action.completer = DXPathCompleter(expected='folder')
parser_rmdir.set_defaults(func=rmdir)
register_parser(parser_rmdir, categories='fs')

#####################################
# rm
#####################################
parser_rm = subparsers.add_parser('rm', help='Remove data objects and folders',
                                  description='Remove data objects and folders.', prog='dx rm',
                                  parents=[env_args, all_arg])
rm_paths_action = parser_rm.add_argument('paths', help='Paths to remove', metavar='path', nargs='+')
rm_paths_action.completer = DXPathCompleter()
parser_rm.add_argument('-r', '--recursive', help='Recurse into a directory', action='store_true')
parser_rm.add_argument('-f', '--force', help='Force removal of files', action='store_true')

parser_rm.set_defaults(func=rm)
register_parser(parser_rm, categories='fs')

# data

#####################################
# describe
#####################################
parser_describe = subparsers.add_parser('describe', help='Describe a remote object',
                                        description=fill('Describe a DNAnexus entity.  Use this command to describe data objects by name or ID, jobs, apps, users, organizations, etc.  If using the "--json" flag, it will thrown an error if more than one match is found (but if you would like a JSON array of the describe hashes of all matches, then provide the "--multi" flag).  Otherwise, it will always display all results it finds.') + '\n\nNOTES:\n\n- ' + fill('The project found in the path is used as a HINT when you are using an object ID; you may still get a result if you have access to a copy of the object in some other project, but if it exists in the specified project, its description will be returned.') + '\n\n- ' + fill('When describing apps or applets, options marked as advanced inputs will be hidden unless --verbose is provided'),
                                        formatter_class=argparse.RawTextHelpFormatter,
                                        parents=[json_arg, no_color_arg, delim_arg, env_args],
                                        prog='dx describe')
parser_describe.add_argument('--details', help='Include details of data objects', action='store_true')
parser_describe.add_argument('--verbose', help='Include all possible metadata', action='store_true')
parser_describe.add_argument('--name', help='Only print the matching names, one per line', action='store_true')
parser_describe.add_argument('--multi', help=fill('If the flag --json is also provided, then returns a JSON array of describe hashes of all matching results', width_adjustment=-24),
                             action='store_true')
describe_path_action = parser_describe.add_argument('path', help=fill('Object ID or path to an object (possibly in another project) to describe.', width_adjustment=-24))
describe_path_action.completer = DXPathCompleter()
parser_describe.set_defaults(func=describe)
register_parser(parser_describe, categories=('data', 'metadata'))

#####################################
# upload
#####################################
parser_upload = subparsers.add_parser('upload', help='Upload file(s) or directory',
                                      description='Upload local file(s) or directory.  If "-" is provided, stdin will be used instead.  By default, the filename will be used as its new name.  If --path/--destination is provided with a path ending in a slash, the filename will be used, and the folder path will be used as a destination.  If it does not end in a slash, then it will be used as the final name.',
                                      parents=[parser_dataobject_args, stdout_args, env_args],
                                      prog="dx upload")
upload_filename_action = parser_upload.add_argument('filename', nargs='+',
                                                    help='Local file or directory to upload ("-" indicates stdin input); provide multiple times to upload multiple files or directories')
upload_filename_action.completer = LocalCompleter()
parser_upload.add_argument('-o', '--output', help=argparse.SUPPRESS) # deprecated; equivalent to --path/--destination
parser_upload.add_argument('--path', '--destination',
                           help=fill('DNAnexus path to upload file(s) to (default uses current project and folder if not provided)', width_adjustment=-24),
                           nargs='?')
parser_upload.add_argument('-r', '--recursive', help='Upload directories recursively', action='store_true')
parser_upload.add_argument('--wait', help='Wait until the file has finished closing', action='store_true')
parser_upload.add_argument('--no-progress', help='Do not show a progress bar', dest='show_progress',
                           action='store_false', default=sys.stderr.isatty())
parser_upload.add_argument('--buffer-size', help='Set the write buffer size (in bytes)', dest='write_buffer_size')
parser_upload.add_argument('--singlethread', help='Enable singlethreaded uploading', dest='singlethread', action='store_true')
parser_upload.set_defaults(func=upload, mute=False)
register_parser(parser_upload, categories='data')

#####################################
# download
#####################################
parser_download = subparsers.add_parser('download', help='Download file(s)',
                                        description='Download the contents of a file object or multiple objects.  Use "-o -" to direct the output to stdout.',
                                        prog='dx download',
                                        parents=[env_args])
parser_download_paths_arg = parser_download.add_argument('paths', help='Data object ID or name, or folder to download',
                                                         nargs='+', metavar='path')
parser_download_paths_arg.completer = DXPathCompleter(classes=['file'])
parser_download.add_argument('-o', '--output', help='Local filename or directory to be used ("-" indicates stdout output); if not supplied or a directory is given, the object\'s name on the platform will be used, along with any applicable extensions')
parser_download.add_argument('-f', '--overwrite', help='Resume an interupted download if the local and remote file signatures match.  If the signatures do not match the local file will be overwritten.', action='store_true')
parser_download.add_argument('-r', '--recursive', help='Download folders recursively', action='store_true')
parser_download.add_argument('-a', '--all', help='If multiple objects match the input, download all of them',
                             action='store_true')
parser_download.add_argument('--no-progress', help='Do not show a progress bar', dest='show_progress',
                             action='store_false', default=sys.stderr.isatty())
parser_download.add_argument('--lightweight', help='Skip some validation steps to make fewer API calls',
                             action='store_true')
parser_download.add_argument('--symlink-max-tries', help='Set maximum number of tries for downloading symlinked files using aria2c',
                             type=positive_integer,
                             default=15)
parser_download.add_argument('--unicode', help='Display the characters as text/unicode when writing to stdout',
                             dest="unicode_text", action='store_true')
parser_download.set_defaults(func=download_or_cat)
register_parser(parser_download, categories='data')

#####################################
# make_download_url
#####################################
parser_make_download_url = subparsers.add_parser('make_download_url', help='Create a file download link for sharing',
                                                 description='Creates a pre-authenticated link that can be used to download a file without logging in.',
                                                 prog='dx make_download_url')
path_action = parser_make_download_url.add_argument('path', help='Data object ID or name to access')
path_action.completer = DXPathCompleter(classes=['file'])
parser_make_download_url.add_argument('--duration', help='Time for which the URL will remain valid (in seconds, or use suffix s, m, h, d, w, M, y). Default: 1 day')
parser_make_download_url.add_argument('--filename', help='Name that the server will instruct the client to save the file as (default is the filename)')
parser_make_download_url.set_defaults(func=make_download_url)
register_parser(parser_make_download_url, categories='data')

#####################################
# cat
#####################################
parser_cat = subparsers.add_parser('cat', help='Print file(s) to stdout', prog='dx cat',
                                   parents=[env_args])
cat_path_action = parser_cat.add_argument('path', help='File ID or name(s) to print to stdout', nargs='+')
cat_path_action.completer = DXPathCompleter(classes=['file'])
parser_cat.add_argument('--unicode', help='Display the characters as text/unicode when writing to stdout',
                        dest="unicode_text", action='store_true')
parser_cat.set_defaults(func=cat)
register_parser(parser_cat, categories='data')

#####################################
# head
#####################################
parser_head = subparsers.add_parser('head',
                                    help='Print part of a file',
                                    description='Print the first part of a file.  By default, prints the first 10 lines.',
                                    parents=[no_color_arg, env_args],
                                    prog='dx head')
parser_head.add_argument('-n', '--lines', type=int, metavar='N', help='Print the first N lines (default 10)',
                         default=10)
head_path_action = parser_head.add_argument('path', help='File ID or name to access')
head_path_action.completer = DXPathCompleter(classes=['file'])
parser_head.set_defaults(func=head)
register_parser(parser_head, categories='data')

#####################################
# build
#####################################
build_parser = subparsers.add_parser('build', help='Create a new applet/app, or a workflow',
                                     description='Build an applet, app, or workflow object from a local source directory or an app from an existing applet in the platform. You can use ' + BOLD("dx-app-wizard") + ' to generate a skeleton directory of an app/applet with the necessary files.',
                                     prog='dx build',
                                     parents=[env_args, stdout_args])

app_and_globalworkflow_options = build_parser.add_argument_group('Options for creating apps or globalworkflows', '(Only valid when --app/--create-app/--globalworkflow/--create-globalworkflow is specified)')
applet_and_workflow_options = build_parser.add_argument_group('Options for creating applets or workflows', '(Only valid when --app/--create-app/--globalworkflow/--create-globalworkflow is NOT specified)')
nextflow_options = build_parser.add_argument_group('Options for creating Nextflow applets', '(Only valid when --nextflow is specified)')

# COMMON OPTIONS
build_parser.add_argument("--ensure-upload", help="If specified, will bypass computing checksum of " +
                                            "resources directory and upload it unconditionally; " +
                                            "by default, will compute checksum and upload only if " +
                                            "it differs from a previously uploaded resources bundle.",
                    action="store_true")
build_parser.add_argument("--force-symlinks", help="If specified, will not attempt to dereference "+
                                            "symbolic links pointing outside of the resource " +
                                            "directory.  By default, any symlinks within the resource " +
                                            "directory are kept as links while links to files " +
                                            "outside the resource directory are dereferenced (note "+
                                            "that links to directories outside of the resource directory " +
                                            "will cause an error).",
                    action="store_true")

src_dir_action = build_parser.add_argument("src_dir", help="Source directory that contains dxapp.json, dxworkflow.json or *.nf (for --nextflow option). (default: current directory)", nargs='?')
src_dir_action.completer = LocalCompleter()

build_parser.add_argument("--app", "--create-app", help="Create an app.", action="store_const", dest="mode", const="app")
build_parser.add_argument("--create-applet", help=argparse.SUPPRESS, action="store_const", dest="mode", const="applet")
build_parser.add_argument("--workflow", "--create-workflow", help="Create a workflow.", action="store_const", dest="mode", const="workflow")

build_parser.add_argument("--globalworkflow", "--create-globalworkflow", help="Create a global workflow.", action="store_const", dest="mode", const="globalworkflow")

applet_and_workflow_options.add_argument("-d", "--destination", help="Specifies the destination project, destination folder, and/or name for the applet, in the form [PROJECT_NAME_OR_ID:][/[FOLDER/][NAME]]. Overrides the project, folder, and name fields of the dxapp.json or dxworkflow.json, if they were supplied.", default='.')

# --[no-]dry-run
#
# The --dry-run flag can be used to see the applet spec that would be
# provided to /applet/new, for debugging purposes. However, the output
# would deviate from that of a real run in the following ways:
#
# * Any bundled resources are NOT uploaded and are not reflected in the
#   app(let) spec.
# * No temporary project is created (if building an app) and the
#   "project" field is not set in the app spec.
build_parser.set_defaults(dry_run=False)
build_parser.add_argument("--dry-run", "-n", help="Do not create an app(let): only perform local checks and compilation steps, and show the spec of the app(let) that would have been created.", action="store_true", dest="dry_run")
build_parser.add_argument("--no-dry-run", help=argparse.SUPPRESS, action="store_false", dest="dry_run")

# --[no-]publish
app_and_globalworkflow_options.set_defaults(publish=False)
app_and_globalworkflow_options.add_argument("--publish", help="Publish the resulting app/globalworkflow and make it the default.", action="store_true",
                         dest="publish")
app_and_globalworkflow_options.add_argument("--no-publish", help=argparse.SUPPRESS, action="store_false", dest="publish")
app_and_globalworkflow_options.add_argument("--from", help="ID or path of the source applet/workflow to create an app/globalworkflow from. Source directory src_dir cannot be given when using this option",
                          dest="_from").completer = DXPathCompleter(classes=['applet','workflow'])


# --[no-]remote
build_parser.set_defaults(remote=False)
build_parser.add_argument("--remote", help="Build the app remotely by uploading the source directory to the DNAnexus Platform and building it there. This option is useful if you would otherwise need to cross-compile the app(let) to target the Execution Environment.", action="store_true", dest="remote")
build_parser.add_argument("--no-watch", help="Don't watch the real-time logs of the remote builder. (This option only applicable if --remote or --repository was specified).", action="store_false", dest="watch")
build_parser.add_argument("--no-remote", help=argparse.SUPPRESS, action="store_false", dest="remote")

applet_and_workflow_options.add_argument("-f", "--overwrite", help="Remove existing applet(s) of the same name in the destination folder. This option is not yet supported for workflows.",
                            action="store_true", default=False)
applet_and_workflow_options.add_argument("-a", "--archive", help="Archive existing applet(s) of the same name in the destination folder. This option is not yet supported for workflows.",
                            action="store_true", default=False)
build_parser.add_argument("-v", "--version", help="Override the version number supplied in the manifest. This option needs to be specified when using --from option.", default=None,
                    dest="version_override", metavar='VERSION')
app_and_globalworkflow_options.add_argument("-b", "--bill-to", help="Entity (of the form user-NAME or org-ORGNAME) to bill for the app/globalworkflow.",
                         default=None, dest="bill_to", metavar='USER_OR_ORG')

# --[no-]check-syntax
build_parser.set_defaults(check_syntax=True)
build_parser.add_argument("--check-syntax", help=argparse.SUPPRESS, action="store_true", dest="check_syntax")
build_parser.add_argument("--no-check-syntax", help="Warn but do not fail when syntax problems are found (default is to fail on such errors)", action="store_false", dest="check_syntax")

# --[no-]version-autonumbering
app_and_globalworkflow_options.set_defaults(version_autonumbering=True)
app_and_globalworkflow_options.add_argument("--version-autonumbering", help=argparse.SUPPRESS, action="store_true", dest="version_autonumbering")
app_and_globalworkflow_options.add_argument("--no-version-autonumbering", help="Only attempt to create the version number supplied in the manifest (that is, do not try to create an autonumbered version such as 1.2.3+git.ab1b1c1d if 1.2.3 already exists and is published).", action="store_false", dest="version_autonumbering")
# --[no-]update
app_and_globalworkflow_options.set_defaults(update=True)
app_and_globalworkflow_options.add_argument("--update", help=argparse.SUPPRESS, action="store_true", dest="update")
app_and_globalworkflow_options.add_argument("--no-update", help="Never update an existing unpublished app/globalworkflow in place.", action="store_false", dest="update")
# --[no-]dx-toolkit-autodep
build_parser.add_argument("--dx-toolkit-legacy-git-autodep", help=argparse.SUPPRESS, action="store_const", dest="dx_toolkit_autodep", const="git")
build_parser.add_argument("--dx-toolkit-stable-autodep", help=argparse.SUPPRESS, action="store_const", dest="dx_toolkit_autodep", const="stable")
build_parser.add_argument("--dx-toolkit-autodep", help=argparse.SUPPRESS, action="store_const", dest="dx_toolkit_autodep", const="stable")
build_parser.add_argument("--no-dx-toolkit-autodep", help=argparse.SUPPRESS, action="store_false", dest="dx_toolkit_autodep")

# --[no-]parallel-build
build_parser.set_defaults(parallel_build=True)
build_parser.add_argument("--parallel-build", help=argparse.SUPPRESS, action="store_true", dest="parallel_build")
build_parser.add_argument("--no-parallel-build", help="Build with " + BOLD("make") + " instead of " + BOLD("make -jN") + ".", action="store_false",
                    dest="parallel_build")

app_and_globalworkflow_options.set_defaults(use_temp_build_project=True)
# Original help: "When building an app, build its applet in the current project instead of a temporary project".
app_and_globalworkflow_options.add_argument("--no-temp-build-project", help="When building an app in a single region, build its applet in the current project instead of a temporary project.", action="store_false", dest="use_temp_build_project")

# --yes
app_and_globalworkflow_options.add_argument('-y', '--yes', dest='confirm', help='Do not ask for confirmation for potentially dangerous operations', action='store_false')

# --[no-]json (undocumented): dumps the JSON describe of the app or
# applet that was created. Useful for tests.
build_parser.set_defaults(json=False)
build_parser.add_argument("--json", help=argparse.SUPPRESS, action="store_true", dest="json")
build_parser.add_argument("--no-json", help=argparse.SUPPRESS, action="store_false", dest="json")
build_parser.add_argument("--extra-args", help="Arguments (in JSON format) to pass to the /applet/new API method, overriding all other settings")
build_parser.add_argument("--run", help="Run the app or applet after building it (options following this are passed to "+BOLD("dx run")+"; run at high priority by default)", nargs=argparse.REMAINDER)

# --region
app_and_globalworkflow_options.add_argument("--region", action="append", help="Enable the app/globalworkflow in this region. This flag can be specified multiple times to enable the app/globalworkflow in multiple regions. If --region is not specified, then the enabled region(s) will be determined by 'regionalOptions' in dxapp.json, or the project context.")

# --keep-open
build_parser.add_argument('--keep-open', help=fill("Do not close workflow after building it. Cannot be used when building apps, applets or global workflows.",
                                                   width_adjustment=-24), action='store_true')

# --nextflow
build_parser.add_argument('--nextflow', help=fill("Build Nextflow applet.",
                                                   width_adjustment=-24), action='store_true')

# --profile
nextflow_options.add_argument('--profile', help=fill("Default profile for the Nextflow pipeline.",
                                                   width_adjustment=-24), dest="profile")

# --repository
nextflow_options.add_argument('--repository', help=fill("Specifies a Git repository of a Nextflow pipeline. Incompatible with --remote.",
                                                   width_adjustment=-24), dest="repository")
# --tag
nextflow_options.add_argument('--repository-tag', help=fill("Specifies tag for Git repository. Can be used only with --repository.",
                                                   width_adjustment=-24), dest="tag")

# --git-credentials
nextflow_options.add_argument('--git-credentials', help=fill("Git credentials used to access Nextflow pipelines from private Git repositories. Can be used only with --repository. "
                                                            "More information about the file syntax can be found at https://www.nextflow.io/blog/2021/configure-git-repositories-with-nextflow.html.",
                                                   width_adjustment=-24), dest="git_credentials").completer = DXPathCompleter(classes=['file'])

build_parser.set_defaults(func=build)
register_parser(build_parser, categories='exec')

#####################################
# build_asset
#####################################
from ..asset_builder import build_asset
parser_build_asset = subparsers.add_parser(
    "build_asset",
    help='Build an asset bundle',
    formatter_class=argparse.RawTextHelpFormatter,
    description=fill('Build an asset from a local source directory. The directory must have a file called '
         '"dxasset.json" containing valid JSON. For more details, see '
         '\n\nhttps://documentation.dnanexus.com/developer/apps/dependency-management/asset-build-process'),
    prog="dx build_asset")
parser_build_asset.add_argument("src_dir", help="Asset source directory (default: current directory)", nargs='?')
parser_build_asset.add_argument("-d", "--destination",
                                help=fill("Specifies the destination project and destination folder for the asset,"
                                          " in the form [PROJECT_NAME_OR_ID:][/[FOLDER/][NAME]]"),
                                default='.')
parser_build_asset.add_argument("--json", help=fill("Show ID of resulting asset bundle in JSON format"),
                                action="store_true", dest="json")
parser_build_asset.add_argument("--no-watch", help=fill("Don't watch the real-time logs of the asset-builder job."),
                                action="store_false", dest="watch")
parser_build_asset.add_argument("--priority", choices=['normal', 'high'], help=argparse.SUPPRESS)
parser_build_asset.set_defaults(func=build_asset)
register_parser(parser_build_asset)

#####################################
# add
#####################################
parser_add = subparsers.add_parser('add', help='Add one or more items to a list',
                                   description='Use this command with one of the availabile subcommands to perform various actions such as adding other users or orgs to the list of developers or authorized users of an app',
                                   prog='dx add')
subparsers_add = parser_add.add_subparsers(parser_class=DXArgumentParser)
subparsers_add.metavar = 'list_type'
register_parser(parser_add, categories=())

parser_add_users = subparsers_add.add_parser('users', help='Add authorized users for an app',
                                             description='Add users or orgs to the list of authorized users of an app.  Published versions of the app will only be accessible to users represented by this list and to developers of the app.  Unpublished versions are restricted to the developers.',
                                             prog='dx add users', parents=[env_args])
parser_add_users.add_argument('app', help='Name or ID of an app').completer = DXAppCompleter(installed=True)
parser_add_users.add_argument('users', metavar='authorizedUser',
                              help='One or more users or orgs to add',
                              nargs='+')
parser_add_users.set_defaults(func=add_users)
register_parser(parser_add_users, subparsers_action=subparsers_add, categories='exec')

parser_add_developers = subparsers_add.add_parser('developers', help='Add developers for an app',
                                                  description='Add users or orgs to the list of developers for an app.  Developers are able to build and publish new versions of the app, and add or remove others from the list of developers and authorized users.',
                                                  prog='dx add developers', parents=[env_args])
parser_add_developers.add_argument('app', help='Name or ID of an app').completer = DXAppCompleter(installed=True)
parser_add_developers.add_argument('developers', metavar='developer', help='One or more users or orgs to add',
                              nargs='+')
parser_add_developers.set_defaults(func=add_developers)
register_parser(parser_add_developers, subparsers_action=subparsers_add, categories='exec')

parser_add_stage = subparsers_add.add_parser('stage', help='Add a stage to a workflow',
                                             description='Add a stage to a workflow.  Default inputs for the stage can also be set at the same time.',
                                             parents=[exec_input_args, stdout_args, env_args,
                                                      instance_type_arg],
                                             prog='dx add stage')
parser_add_stage.add_argument('workflow', help='Name or ID of a workflow').completer = DXPathCompleter(classes=['workflow'])
parser_add_stage.add_argument('executable', help='Name or ID of an executable to add as a stage in the workflow').completer = MultiCompleter([DXAppCompleter(),
                                                                                                                                              DXPathCompleter(classes=['applet'])])
parser_add_stage.add_argument('--alias', '--version', '--tag', dest='alias',
                              help='Tag or version of the app to add if the executable is an app (default: "default" if an app)')
parser_add_stage.add_argument('--name', help='Stage name')
parser_add_stage.add_argument('--id',  dest='stage_id', help='Stage ID')
add_stage_folder_args = parser_add_stage.add_mutually_exclusive_group()
add_stage_folder_args.add_argument('--output-folder', help='Path to the output folder for the stage (interpreted as an absolute path)')
add_stage_folder_args.add_argument('--relative-output-folder', help='A relative folder path for the stage (interpreted as relative to the workflow\'s output folder)')
parser_add_stage.set_defaults(func=workflow_cli.add_stage)
register_parser(parser_add_stage, subparsers_action=subparsers_add, categories='workflow')

parser_add_member = subparsers_add.add_parser("member", help="Grant a user membership to an org", description="Grant a user membership to an org", prog="dx add member", parents=[stdout_args, env_args])
parser_add_member.add_argument("org_id", help="ID of the org")
parser_add_member.add_argument("username_or_user_id", help="Username or ID of user")
parser_add_member.add_argument("--level", required=True, choices=["ADMIN", "MEMBER"], help="Org membership level that will be granted to the specified user")
parser_add_member.add_argument("--allow-billable-activities", default=False, action="store_true", help='Grant the specified user "allowBillableActivities" in the org')
parser_add_member.add_argument("--no-app-access", default=True, action="store_false", dest="app_access", help='Disable "appAccess" for the specified user in the org')
parser_add_member.add_argument("--project-access", choices=["ADMINISTER", "CONTRIBUTE", "UPLOAD", "VIEW", "NONE"], default="CONTRIBUTE", help='The default implicit maximum permission the specified user will receive to projects explicitly shared with the org; default CONTRIBUTE')
parser_add_member.add_argument("--no-email", default=False, action="store_true", help="Disable org invitation email notification to the specified user")
parser_add_member.set_defaults(func=add_membership)
register_parser(parser_add_member, subparsers_action=subparsers_add, categories="org")

#####################################
# list
#####################################
parser_list = subparsers.add_parser('list', help='Print the members of a list',
                                   description='Use this command with one of the availabile subcommands to perform various actions such as printing the list of developers or authorized users of an app.',
                                   prog='dx list')
subparsers_list = parser_list.add_subparsers(parser_class=DXArgumentParser)
subparsers_list.metavar = 'list_type'
register_parser(parser_list, categories=())

parser_list_users = subparsers_list.add_parser('users', help='List authorized users for an app',
                                               description='List the authorized users of an app.  Published versions of the app will only be accessible to users represented by this list and to developers of the app.  Unpublished versions are restricted to the developers',
                                               prog='dx list users', parents=[env_args])
parser_list_users.add_argument('app', help='Name or ID of an app').completer = DXAppCompleter(installed=True)
parser_list_users.set_defaults(func=list_users)
register_parser(parser_list_users, subparsers_action=subparsers_list, categories='exec')

parser_list_developers = subparsers_list.add_parser('developers', help='List developers for an app',
                                                    description='List the developers for an app.  Developers are able to build and publish new versions of the app, and add or remove others from the list of developers and authorized users.',
                                                    prog='dx list developers', parents=[env_args])
parser_list_developers.add_argument('app', help='Name or ID of an app').completer = DXAppCompleter(installed=True)
parser_list_developers.set_defaults(func=list_developers)
register_parser(parser_list_developers, subparsers_action=subparsers_list, categories='exec')

parser_list_stages = subparsers_list.add_parser('stages', help='List the stages in a workflow',
                                                description='List the stages in a workflow.',
                                                parents=[env_args],
                                                prog='dx list stages')
parser_list_stages.add_argument('workflow', help='Name or ID of a workflow').completer = DXPathCompleter(classes=['workflow'])
parser_list_stages.set_defaults(func=workflow_cli.list_stages)
register_parser(parser_list_stages, subparsers_action=subparsers_list, categories='workflow')

parser_list_database = subparsers_list.add_parser(
    "database",
    help=fill("List entities associated with a specific database. For example,") + "\n\n\t" +
         fill('"dx list database files" lists database files associated with a specific database.') + "\n\n\t" +
         fill('Please execute "dx list database -h" for more information.'),
    description=fill("List entities associated with a specific database."),
    prog="dx list database"
)
register_parser(parser_list_database, subparsers_action=subparsers_list)

subparsers_list_database = parser_list_database.add_subparsers(parser_class=DXArgumentParser)
subparsers_list_database.metavar = "entities"

parser_list_database_files = subparsers_list_database.add_parser(
    'files',
    help='List files associated with a specific database',
    description=fill('List files associated with a specific database'),
    parents=[env_args],
    prog='dx list database files'
)
parser_list_database_files.add_argument('database', help='Data object ID or path of the database.')
parser_list_database_files.add_argument('--folder', default='/', help='Name of folder (directory) in which to start searching for database files. This will typically match the name of the table whose files are of interest. The default value is "/" which will start the search at the root folder of the database.')
parser_list_database_files.add_argument("--recurse", default=False, help='Look for files recursively down the directory structure. Otherwise, by default, only look on one level.', action='store_true')
parser_list_database_files.add_argument("--csv", default=False, help='Write output as comma delimited fields, suitable as CSV format.', action='store_true')
parser_list_database_files.add_argument("--timeout", default=120, help='Number of seconds to wait before aborting the request. If omitted, default timeout is 120 seconds.', type=int)
parser_list_database_files.set_defaults(func=list_database_files)
register_parser(parser_list_database_files, subparsers_action=subparsers_list_database, categories='data')


#####################################
# remove
#####################################
parser_remove = subparsers.add_parser('remove', help='Remove one or more items to a list',
                                      description='Use this command with one of the available subcommands to perform various actions such as removing other users from the list of developers or authorized users of an app.',
                                      prog='dx remove')
subparsers_remove = parser_remove.add_subparsers(parser_class=DXArgumentParser)
subparsers_remove.metavar = 'list_type'
register_parser(parser_remove, categories=())

parser_remove_users = subparsers_remove.add_parser('users', help='Remove authorized users for an app',
                                                   description='Remove users or orgs from the list of authorized users of an app.  Published versions of the app will only be accessible to users represented by this list and to developers of the app.  Unpublished versions are restricted to the developers',
                                                   prog='dx remove users', parents=[env_args])
parser_remove_users.add_argument('app', help='Name or ID of an app').completer = DXAppCompleter(installed=True)
parser_remove_users.add_argument('users', metavar='authorizedUser',
                                 help='One or more users or orgs to remove',
                                 nargs='+')
parser_remove_users.set_defaults(func=remove_users)
register_parser(parser_remove_users, subparsers_action=subparsers_remove, categories='exec')

parser_remove_developers = subparsers_remove.add_parser('developers', help='Remove developers for an app',
                                                        description='Remove users or orgs from the list of developers for an app.  Developers are able to build and publish new versions of the app, and add or remove others from the list of developers and authorized users.',
                                                        prog='dx remove developers', parents=[env_args])
parser_remove_developers.add_argument('app', help='Name or ID of an app').completer = DXAppCompleter(installed=True)
parser_remove_developers.add_argument('developers', metavar='developer', help='One or more users to remove',
                                      nargs='+')
parser_remove_developers.set_defaults(func=remove_developers)
register_parser(parser_remove_developers, subparsers_action=subparsers_remove, categories='exec')

parser_remove_stage = subparsers_remove.add_parser('stage', help='Remove a stage from a workflow',
                                                   description='Remove a stage from a workflow.  The stage should be indicated either by an integer (0-indexed, i.e. "0" for the first stage), or a stage ID.',
                                                   parents=[stdout_args, env_args],
                                                   prog='dx remove stage')
parser_remove_stage.add_argument('workflow', help='Name or ID of a workflow').completer = DXPathCompleter(classes=['workflow'])
parser_remove_stage.add_argument('stage', help='Stage (index or ID) of the workflow to remove')
parser_remove_stage.set_defaults(func=workflow_cli.remove_stage)
register_parser(parser_remove_stage, subparsers_action=subparsers_remove, categories='workflow')

parser_remove_member = subparsers_remove.add_parser("member", help="Revoke the org membership of a user", description="Revoke the org membership of a user", prog="dx remove member", parents=[stdout_args, env_args])
parser_remove_member.add_argument("org_id", help="ID of the org")
parser_remove_member.add_argument("username_or_user_id", help="Username or ID of user")
parser_remove_member.add_argument("--keep-explicit-project-permissions", default=True, action="store_false", dest="revoke_project_permissions", help="Disable revocation of explicit project permissions of the specified user to projects billed to the org; implicit project permissions (i.e. those granted to the specified user via his membership in this org) will always be revoked")
parser_remove_member.add_argument("--keep-explicit-app-permissions", default=True, action="store_false", dest="revoke_app_permissions", help="Disable revocation of explicit app developer and user permissions of the specified user to apps billed to the org; implicit app permissions (i.e. those granted to the specified user via his membership in this org) will always be revoked")
parser_remove_member.add_argument("-y", "--yes", action="store_false", dest="confirm", help="Do not ask for confirmation")
parser_remove_member.set_defaults(func=remove_membership)
register_parser(parser_remove_member, subparsers_action=subparsers_remove, categories="org")

#####################################
# update
#####################################
parser_update = subparsers.add_parser('update', help='Update certain types of metadata',
                                      description='''
Use this command with one of the available targets listed below to update
their metadata that are not covered by the other
subcommands.''',
                                      prog='dx update')
subparsers_update = parser_update.add_subparsers(parser_class=DXArgumentParser)
subparsers_update.metavar = 'target'
register_parser(parser_update, categories=())

parser_update_org = subparsers_update.add_parser('org',
                                                 help='Update information about an org',
                                                 description='Update information about an org',
                                                 parents=[stdout_args, env_args],
                                                 prog='dx update org')
parser_update_org.add_argument('org_id', help='ID of the org')
parser_update_org.add_argument('--name', help='New name of the org')
parser_update_org.add_argument('--member-list-visibility', help='New org membership level that is required to be able to view the membership level and/or permissions of any other member in the specified org (corresponds to the memberListVisibility org policy)', choices=['ADMIN', 'MEMBER', 'PUBLIC'])
parser_update_org.add_argument('--project-transfer-ability', help='New org membership level that is required to be able to change the billing account of a project that is billed to the specified org, to some other entity (corresponds to the restrictProjectTransfer org policy)', choices=['ADMIN', 'MEMBER'])
parser_update_org.add_argument('--saml-idp', help='New SAML identity provider')
update_job_reuse_args = parser_update_org.add_mutually_exclusive_group(required=False)
update_job_reuse_args.add_argument('--enable-job-reuse', action='store_true',  help='Enable job reuse for projects where the org is the billTo')
update_job_reuse_args.add_argument('--disable-job-reuse', action='store_true', help='Disable job reuse for projects where the org is the billTo')
parser_update_org.set_defaults(func=update_org)
register_parser(parser_update_org, subparsers_action=subparsers_update, categories='org')


parser_update_workflow = subparsers_update.add_parser('workflow', help='Update the metadata for a workflow',
                                                      description='Update the metadata for an existing workflow',
                                                      parents=[stdout_args, env_args],
                                                      prog='dx update workflow')
parser_update_workflow.add_argument('workflow', help='Name or ID of a workflow').completer = DXPathCompleter(classes=['workflow'])
update_workflow_title_args = parser_update_workflow.add_mutually_exclusive_group()
update_workflow_title_args.add_argument('--title', help='Workflow title')
update_workflow_title_args.add_argument('--no-title', help='Unset the workflow title', action='store_true')
parser_update_workflow.add_argument('--summary', help='Workflow summary')
parser_update_workflow.add_argument('--description', help='Workflow description')
update_workflow_output_folder_args = parser_update_workflow.add_mutually_exclusive_group()
update_workflow_output_folder_args.add_argument('--output-folder', help='Default output folder for the workflow')
update_workflow_output_folder_args.add_argument('--no-output-folder', help='Unset the default output folder for the workflow', action='store_true')
parser_update_workflow.set_defaults(func=workflow_cli.update_workflow)
register_parser(parser_update_workflow, subparsers_action=subparsers_update, categories='workflow')

parser_update_stage = subparsers_update.add_parser('stage', help='Update the metadata for a stage in a workflow',
                                                   description='Update the metadata for a stage in a workflow',
                                                   parents=[exec_input_args, stdout_args, env_args,
                                                            instance_type_arg],
                                                   prog='dx update stage')
parser_update_stage.add_argument('workflow', help='Name or ID of a workflow').completer = DXPathCompleter(classes=['workflow'])
parser_update_stage.add_argument('stage', help='Stage (index or ID) of the workflow to update')
parser_update_stage.add_argument('--executable', help='Name or ID of an executable to replace in the stage').completer = MultiCompleter([DXAppCompleter(),
                                                                                                                                         DXPathCompleter(classes=['applet'])])
parser_update_stage.add_argument('--alias', '--version', '--tag', dest='alias',
                                 help='Tag or version of the app to use if replacing the stage executable with an app (default: "default" if an app)')
parser_update_stage.add_argument('--force',
                                 help='Whether to replace the executable even if it the new one cannot be verified as compatible with the previous version',
                                 action='store_true')
update_stage_name_args = parser_update_stage.add_mutually_exclusive_group()
update_stage_name_args.add_argument('--name', help='Stage name')
update_stage_name_args.add_argument('--no-name', help='Unset the stage name', action='store_true')
update_stage_folder_args = parser_update_stage.add_mutually_exclusive_group()
update_stage_folder_args.add_argument('--output-folder', help='Path to the output folder for the stage (interpreted as an absolute path)')
update_stage_folder_args.add_argument('--relative-output-folder', help='A relative folder path for the stage (interpreted as relative to the workflow\'s output folder)')
parser_update_stage.set_defaults(func=workflow_cli.update_stage)
register_parser(parser_update_stage, subparsers_action=subparsers_update, categories='workflow')

parser_update_member = subparsers_update.add_parser("member", help="Update the membership of a user in an org", description="Update the membership of a user in an org", prog="dx update member", parents=[stdout_args, env_args])
parser_update_member.add_argument("org_id", help="ID of the org")
parser_update_member.add_argument("username_or_user_id", help="Username or ID of user")
parser_update_member.add_argument("--level", choices=["ADMIN", "MEMBER"], help="The new org membership level of the specified user")
parser_update_member.add_argument("--allow-billable-activities", choices=["true", "false"], help='The new "allowBillableActivities" membership permission of the specified user in the org; default false if demoting the specified user from ADMIN to MEMBER')
parser_update_member.add_argument("--app-access", choices=["true", "false"], help='The new "appAccess" membership permission of the specified user in the org; default true if demoting the specified user from ADMIN to MEMBER')
parser_update_member.add_argument("--project-access", choices=["ADMINISTER", "CONTRIBUTE", "UPLOAD", "VIEW", "NONE"], help='The new default implicit maximum permission the specified user will receive to projects explicitly shared with the org; default CONTRIBUTE if demoting the specified user from ADMIN to MEMBER')
parser_update_member.set_defaults(func=update_membership)
register_parser(parser_update_member, subparsers_action=subparsers_update, categories="org")

parser_update_project = subparsers_update.add_parser("project",
                                                     help="Updates a specified project with the specified options",
                                                     description="", prog="dx update project",
                                                     parents=[stdout_args, env_args])
parser_update_project.add_argument('project_id', help="Project ID or project name")
parser_update_project.add_argument('--name', help="New project name")
parser_update_project.add_argument('--summary', help="Project summary")
parser_update_project.add_argument('--description', help="Project description")
parser_update_project.add_argument('--protected', choices=["true", "false"],
                                   help="Whether the project should be PROTECTED")
parser_update_project.add_argument('--restricted', choices=["true", "false"],
                                   help="Whether the project should be RESTRICTED")
parser_update_project.add_argument('--download-restricted', choices=["true", "false"],
                                   help="Whether the project should be DOWNLOAD RESTRICTED")
parser_update_project.add_argument('--containsPHI', choices=["true"],
                                   help="Flag to tell if project contains PHI")
parser_update_project.add_argument('--database-ui-view-only', choices=["true", "false"],
                                   help="Whether the viewers on the project can access the database data directly")
parser_update_project.add_argument('--bill-to', help="Update the user or org ID of the billing account", type=str)
allowed_executables_group = parser_update_project.add_mutually_exclusive_group()
allowed_executables_group.add_argument('--allowed-executables', help='Executable ID(s) this project is allowed to run.  This operation overrides any existing list of executables.', type=str, nargs="+")
allowed_executables_group.add_argument('--unset-allowed-executables', help='Removes any restriction to run executables as set by --allowed-executables', action='store_true')

parser_update_project.set_defaults(func=update_project)
register_parser(parser_update_project, subparsers_action=subparsers_update, categories="metadata")


#####################################
# install
#####################################
parser_install = subparsers.add_parser('install', help='Install an app',
                                       description='Install an app by name.  To see a list of apps you can install, hit <TAB> twice after "dx install" or run "' + BOLD('dx find apps') + '" to see a list of available apps.', prog='dx install',
                                       parents=[env_args])
install_app_action = parser_install.add_argument('app', help='ID or name of app to install')
install_app_action.completer = DXAppCompleter(installed=False)
parser_install.set_defaults(func=install)
register_parser(parser_install, categories='exec')

#####################################
# uninstall
#####################################
parser_uninstall = subparsers.add_parser('uninstall', help='Uninstall an app',
                                         description='Uninstall an app by name.', prog='dx uninstall',
                                         parents=[env_args])
uninstall_app_action = parser_uninstall.add_argument('app', help='ID or name of app to uninstall')
uninstall_app_action.completer = DXAppCompleter(installed=True)
parser_uninstall.set_defaults(func=uninstall)
register_parser(parser_uninstall, categories='exec')

#####################################
# run
#####################################
parser_run = subparsers.add_parser('run', help='Run an applet, app, or workflow', add_help=False,
                                   description=(fill('Run an applet, app, or workflow.  To see a list of executables you can run, hit <TAB> twice after "dx run" or run "' + BOLD('dx find apps') + '" or "' + BOLD('dx find globalworkflows') + '" to see a list of available apps and global workflows.') + '\n\n' + fill('If any inputs are required but not specified, an interactive mode for selecting inputs will be launched.  Inputs can be set in multiple ways.  Run "' + BOLD('dx run --input-help') + '" for more details.') + '\n\n' + fill('Run "' + BOLD('dx run --instance-type-help') + '" to see a list of specifications for computers available to run executables.')),
                                   prog='dx run',
                                   formatter_class=argparse.RawTextHelpFormatter,
                                   parents=[exec_input_args, stdout_args, env_args, extra_args,
                                            instance_type_arg, property_args, tag_args])
run_executable_action = parser_run.add_argument('executable',
                                                help=fill('Name or ID of an applet, app, or workflow to run; must be provided if --clone is not set', width_adjustment=-24),
                                                nargs="?", default="")
run_executable_action.completer = MultiCompleter([DXAppCompleter(),
                                                  DXPathCompleter(classes=['applet', 'workflow'], visibility="visible")])

parser_run.add_argument('-d', '--depends-on',
                        help=fill('ID of job, analysis, or data object that must be in the "done" or ' +
                                  '"closed" state, as appropriate, before this executable can be run; ' +
                                  'repeat as necessary (e.g. "--depends-on id1 ... --depends-on idN"). ' +
                                  'Cannot be supplied when running workflows',
                                  width_adjustment=-24),
                        action='append', type=str)

parser_run.add_argument('-h', '--help', help='show this help message and exit', nargs=0, action=runHelp)
parser_run.add_argument('--clone', help=fill('Job or analysis ID or name from which to use as default options (will use the exact same executable ID, destination project and folder, job input, instance type requests, and a similar name unless explicitly overridden by command-line arguments. When using an analysis with --clone a workflow executable cannot be overriden and should not be provided.)', width_adjustment=-24))
parser_run.add_argument('--alias', '--version', dest='alias',
                        help=fill('Alias (tag) or version of the app to run (default: "default" if an app)', width_adjustment=-24))
parser_run.add_argument('--destination', '--folder', metavar='PATH', dest='folder', help=fill('The full project:folder path in which to output the results. By default, the current working directory will be used.', width_adjustment=-24))
parser_run.add_argument('--batch-folders', dest='batch_folders',
                        help=fill('Output results to separate folders, one per batch, using batch ID as the name of the output folder. The batch output folder location will be relative to the path set in --destination', width_adjustment=-24),
                        action='store_true')
parser_run.add_argument('--project', metavar='PROJECT',
                        help=fill('Project name or ID in which to run the executable. This can also ' +
                                  'be specified together with the output folder in --destination.',
                                  width_adjustment=-24))
parser_run.add_argument('--stage-output-folder', metavar=('STAGE_ID', 'FOLDER'),
                        help=fill('A stage identifier (ID, name, or index), and a folder path to ' +
                                  'use as its output folder',
                                  width_adjustment=-24),
                        nargs=2,
                        action='append',
                        default=[])
parser_run.add_argument('--stage-relative-output-folder', metavar=('STAGE_ID', 'FOLDER'),
                        help=fill('A stage identifier (ID, name, or index), and a relative folder ' +
                                  'path to the workflow output folder to use as the output folder',
                                  width_adjustment=-24),
                        nargs=2,
                        action='append',
                        default=[])
parser_run.add_argument('--name', help=fill('Name for the job (default is the app or applet name)', width_adjustment=-24))
parser_run.add_argument('--delay-workspace-destruction',
                        help=fill('Whether to keep the job\'s temporary workspace around for debugging purposes for 3 days after it succeeds or fails', width_adjustment=-24),
                        action='store_true')
parser_run.add_argument('--priority',
                        choices=['low', 'normal', 'high'],
                        help=fill('Request a scheduling priority for all resulting jobs. ' +
                                  'Defaults to high when --watch, --ssh, or --allow-ssh flags are used.', 
                                  width_adjustment=-24))
parser_run.add_argument('--head-job-on-demand', action='store_true',
                        help=fill('Requests that the head job of an app or applet be run in an on-demand instance. ' +
                                  'Note that --head-job-on-demand option will override the --priority setting for the head job',
                                  width_adjustment=-24))
parser_run.add_argument('-y', '--yes', dest='confirm', help='Do not ask for confirmation', action='store_false')
parser_run.add_argument('--wait', help='Wait until the job is done before returning', action='store_true')
parser_run.add_argument('--watch', help="Watch the job after launching it. Defaults --priority to high.", action='store_true')
parser_run.add_argument('--allow-ssh', action='append', nargs='?', metavar='ADDRESS',
                        help=fill('Configure the job to allow SSH access. Defaults --priority to high. If an argument is ' +
                                  'supplied, it is interpreted as an IP range, e.g. "--allow-ssh 1.2.3.4". ' +
                                  'If no argument is supplied then the client IP visible to the DNAnexus API server will be used by default',
                                  width_adjustment=-24))
parser_run.add_argument('--ssh',
                        help=fill("Configure the job to allow SSH access and connect to it after launching. " +
                                  "Defaults --priority to high.",
                                  width_adjustment=-24),
                        action='store_true')
parser_run.add_argument('--ssh-proxy', metavar=('<address>:<port>'),
                        help=fill('SSH connect via proxy, argument supplied is used as the proxy address and port',
                                  width_adjustment=-24))
parser_run.add_argument('--debug-on', action='append', choices=['AppError', 'AppInternalError', 'ExecutionError', 'All'],
                        help=fill("Configure the job to hold for debugging when any of the listed errors occur",
                                  width_adjustment=-24))

ignore_reuse = parser_run.add_mutually_exclusive_group()
ignore_reuse.add_argument('--ignore-reuse',
                        help=fill("Disable job reuse for execution",
                                  width_adjustment=-24),
                        action='store_true')
ignore_reuse.add_argument('--ignore-reuse-stage', metavar='STAGE_ID', dest='ignore_reuse_stages',
                        help=fill('A stage (using its ID, name, or index) for which job reuse should be disabled, ' +
                                  'if a stage points to another (nested) workflow the ignore reuse option will be applied to the whole subworkflow. ' +
                                  'This option overwrites any ignoreReuse fields set on app(let)s or the workflow during build time; ' +
                                  'repeat as necessary',
                                  width_adjustment=-24),
                        action='append')
parser_run.add_argument('--rerun-stage', metavar='STAGE_ID', dest='rerun_stages',
                        help=fill('A stage (using its ID, name, or index) to rerun, or "*" to ' +
                                  'indicate all stages should be rerun; repeat as necessary',
                                  width_adjustment=-24),
                        action='append')
parser_run.add_argument('--batch-tsv', dest='batch_tsv', metavar="FILE",
                        help=fill('A file in tab separated value (tsv) format, with a subset ' +
                                  'of the executable input arguments. A job will be launched ' +
                                  'for each table row.',
                                  width_adjustment=-24))
ic_format = '\'{"entrypoint": <number of instances>}\''
parser_run.add_argument('--instance-count',
                               metavar='INSTANCE_COUNT_OR_MAPPING',
                               help=fill('Specify spark cluster instance count(s). It can be an int or a mapping of the format {ic}'.format(ic=ic_format),
                                         width_adjustment=-24),
                               action='append')
parser_run.add_argument('--input-help',
                        help=fill('Print help and examples for how to specify inputs',
                                  width_adjustment=-24),
                        action=runInputHelp, nargs=0)
parser_run.add_argument('--detach', help=fill("When invoked from a job, detaches the new job from the creator job so the "
                                              "new job will appear as a typical root execution. Setting DX_RUN_DETACH "
                                              "environment variable to 1 causes this option to be set by default.",
                                              width_adjustment=-24), action='store_true')
parser_run.add_argument('--cost-limit', help=fill("Maximum cost of the job before termination. In case of workflows it is cost of the "
                                                  "entire analysis job. For batch run, this limit is applied per job.",
                                              width_adjustment=-24), metavar='cost_limit', type=float)
parser_run.add_argument('-r', '--rank', type=int, help='Set the rank of the root execution, integer between -1024 and 1023. Requires executionRankEnabled license feature for the billTo. Default is 0.', default=None)
parser_run.set_defaults(func=run, verbose=False, help=False, details=None,
                        stage_instance_types=None, stage_folders=None, head_job_on_demand=None)
register_parser(parser_run, categories='exec')

#####################################
# watch
#####################################
parser_watch = subparsers.add_parser('watch', help='Watch logs of a job and its subjobs', prog='dx watch',
                                     description='Monitors logging output from a running job',
                                     parents=[env_args, no_color_arg])
parser_watch.add_argument('jobid', help='ID of the job to watch')
# .completer = TODO
parser_watch.add_argument('-n', '--num-recent-messages', help='Number of recent messages to get',
                          type=int, default=1024*256)
parser_watch.add_argument('--tree', help='Include the entire job tree', action='store_true')
parser_watch.add_argument('-l', '--levels', action='append', choices=["EMERG", "ALERT", "CRITICAL", "ERROR", "WARNING",
                                                                      "NOTICE", "INFO", "DEBUG", "STDERR", "STDOUT"])
parser_watch.add_argument('--get-stdout', help='Extract stdout only from this job', action='store_true')
parser_watch.add_argument('--get-stderr', help='Extract stderr only from this job', action='store_true')
parser_watch.add_argument('--get-streams', help='Extract only stdout and stderr from this job', action='store_true')
parser_watch.add_argument('--no-timestamps', help='Omit timestamps from messages', action='store_false',
                          dest='timestamps')
parser_watch.add_argument('--job-ids', help='Print job ID in each message', action='store_true')
parser_watch.add_argument('--no-job-info', help='Omit job info and status updates', action='store_false',
                          dest='job_info')
parser_watch.add_argument('-q', '--quiet', help='Do not print extra info messages', action='store_true')
parser_watch.add_argument('-f', '--format', help='Message format. Available fields: job, level, msg, date')
parser_watch.add_argument('--no-wait', '--no-follow', action='store_false', dest='tail',
                          help='Exit after the first new message is received, instead of waiting for all logs')
parser_watch.set_defaults(func=watch)
register_parser(parser_watch, categories='exec')

#####################################
# shh_config
#####################################
parser_ssh_config = subparsers.add_parser('ssh_config', help='Configure SSH keys for your DNAnexus account',
                                   description='Configure SSH access credentials for your DNAnexus account',
                                   prog='dx ssh_config',
                                   parents=[env_args])
parser_ssh_config.add_argument('ssh_keygen_args', help='Command-line arguments to pass to ssh-keygen',
                               nargs=argparse.REMAINDER)
parser_ssh_config.add_argument('--revoke', help='Revoke SSH public key associated with your DNAnexus account; you will no longer be able to SSH into any jobs.', action='store_true')
parser_ssh_config.set_defaults(func=ssh_config)
register_parser(parser_ssh_config, categories='exec')

#####################################
# ssh
#####################################
parser_ssh = subparsers.add_parser('ssh', help='Connect to a running job via SSH',
                                   description='Use an SSH client to connect to a job being executed on the DNAnexus ' +
                                               'platform. The job must be launched using "dx run --allow-ssh" or ' +
                                               'equivalent API options. Use "dx ssh_config" or the Profile page on ' +
                                               'the DNAnexus website to configure SSH for your DNAnexus account.',
                                   prog='dx ssh',
                                   parents=[env_args])
parser_ssh.add_argument('job_id', help='Name of job to connect to')
parser_ssh.add_argument('ssh_args', help='Command-line arguments to pass to the SSH client', nargs=argparse.REMAINDER)
parser_ssh.add_argument('--ssh-proxy', metavar=('<address>:<port>'),
                        help='SSH connect via proxy, argument supplied is used as the proxy address and port')
parser_ssh_firewall = parser_ssh.add_mutually_exclusive_group()
parser_ssh_firewall.add_argument('--no-firewall-update', help='Do not update the allowSSH allowed IP ranges before connecting with ssh', action='store_true', default=False)
parser_ssh_firewall.add_argument('--allow-ssh', action='append', nargs='?', metavar='ADDRESS',
                        help=fill('Configure the job to allow SSH access from an IP range, e.g. "--allow-ssh 1.2.3.4". ' +
                                  'If no argument is supplied then the client IP visible to the DNAnexus API server will be used by default',
                                  width_adjustment=-24))
# If ssh is run with the  supress-running-check flag, then dx won't prompt
# the user whether they would like to terminate the currently running job
# after they exit ssh.  Among other things, this will allow users to setup
# ssh tunnels using dx ssh, and exit the ssh command with the tunnel still
# in place, and not be prompted to terminate the instance (which would close
# the tunnel).
parser_ssh.add_argument('--suppress-running-check', action='store_false', help=argparse.SUPPRESS, dest='check_running')
parser_ssh.set_defaults(func=ssh)
register_parser(parser_ssh, categories='exec')

#####################################
# terminate
#####################################
parser_terminate = subparsers.add_parser('terminate', help='Terminate jobs or analyses',
                                         description='Terminate one or more jobs or analyses',
                                         prog='dx terminate',
                                         parents=[env_args])
parser_terminate.add_argument('jobid', help='ID of a job or analysis to terminate', nargs='+')
parser_terminate.set_defaults(func=terminate)
parser_map['terminate'] = parser_terminate
parser_categories['all']['cmds'].append((subparsers._choices_actions[-1].dest, subparsers._choices_actions[-1].help))
parser_categories['exec']['cmds'].append((subparsers._choices_actions[-1].dest, subparsers._choices_actions[-1].help))

#####################################
# rmproject
#####################################
parser_rmproject = subparsers.add_parser('rmproject', help='Delete a project',
                                         description='Delete projects and all their associated data',
                                         prog='dx rmproject',
                                         parents=[env_args])
projects_action = parser_rmproject.add_argument('projects', help='Projects to remove', metavar='project', nargs='+')
projects_action.completer = DXPathCompleter(expected='project', include_current_proj=True)
parser_rmproject.add_argument('-y', '--yes', dest='confirm', help='Do not ask for confirmation', action='store_false')
parser_rmproject.add_argument('-q', '--quiet', help='Do not print purely informational messages', action='store_true')
parser_rmproject.set_defaults(func=rmproject)
register_parser(parser_rmproject, categories='fs')

#####################################
# new
#####################################
parser_new = subparsers.add_parser('new', help='Create a new project or data object',
                                   description='Use this command with one of the available subcommands (classes) to create a new project or data object from scratch.  Not all data types are supported.  See \'dx upload\' for files and \'dx build\' for applets.',
                                   prog="dx new")
subparsers_new = parser_new.add_subparsers(parser_class=DXArgumentParser)
subparsers_new.metavar = 'class'
register_parser(parser_new, categories='data')

parser_new_user = subparsers_new.add_parser("user", help="Create a new user account", description="Create a new user account", parents=[stdout_args, env_args], prog="dx new user")
parser_new_user_user_opts = parser_new_user.add_argument_group("User options")
parser_new_user_user_opts.add_argument("-u", "--username", required=True, help="Username")
parser_new_user_user_opts.add_argument("--email", required=True, help="Email address")
parser_new_user_user_opts.add_argument("--first", help="First name")
parser_new_user_user_opts.add_argument("--middle", help="Middle name")
parser_new_user_user_opts.add_argument("--last", help="Last name")
parser_new_user_user_opts.add_argument("--token-duration", help='Time duration for which the newly generated auth token for the new user will be valid (default 30 days; max 30 days). An integer will be interpreted as seconds; you can append a suffix (s, m, h, d) to indicate different units (e.g. "--token-duration 10m" to indicate 10 minutes).')
parser_new_user_user_opts.add_argument("--occupation", help="Occupation")
parser_new_user_user_opts.add_argument("--on-behalf-of", help="On behalf of which org is the account provisioned")
parser_new_user_org_opts = parser_new_user.add_argument_group("Org options", "Optionally invite the new user to an org with the specified parameters")
parser_new_user_org_opts.add_argument("--org", help="ID of the org")
parser_new_user_org_opts.add_argument("--level", choices=["ADMIN", "MEMBER"], default="MEMBER", action=DXNewUserOrgArgsAction, help="Org membership level that will be granted to the new user; default MEMBER")
parser_new_user_org_opts.add_argument("--set-bill-to", default=False, action=DXNewUserOrgArgsAction, help='Set the default "billTo" field of the new user to the org; implies --allow-billable-activities')
parser_new_user_org_opts.add_argument("--allow-billable-activities", default=False, action=DXNewUserOrgArgsAction, help='Grant the new user "allowBillableActivities" in the org')
parser_new_user_org_opts.add_argument("--no-app-access", default=True, action=DXNewUserOrgArgsAction, dest="app_access", help='Disable "appAccess" for the new user in the org')
parser_new_user_org_opts.add_argument("--project-access", choices=["ADMINISTER", "CONTRIBUTE", "UPLOAD", "VIEW", "NONE"], default="CONTRIBUTE", action=DXNewUserOrgArgsAction, help='The "projectAccess" to grant the new user in the org; default CONTRIBUTE')
parser_new_user_org_opts.add_argument("--no-email", default=False, action=DXNewUserOrgArgsAction, help="Disable org invitation email notification to the new user")
parser_new_user.set_defaults(func=new_user)
register_parser(parser_new_user, subparsers_action=subparsers_new,
                   categories="other")

parser_new_org = subparsers_new.add_parser('org', help='Create new non-billable org',
                                           description='Create a new non-billable org. Contact sales@dnanexus.com for the creation of billable orgs',
                                           parents=[stdout_args, env_args],
                                           prog='dx new org')
parser_new_org.add_argument('name', help='Descriptive name of the org', nargs='?')
parser_new_org.add_argument('--handle', required=True, help='Unique handle for the org. The specified handle will be converted to lowercase and appended to "org-" to form the org ID')
parser_new_org.add_argument('--member-list-visibility', default="ADMIN", help='Org membership level required to be able to list the members of the org, or to view the membership level or permissions of any other member of the org; default ADMIN', choices=["ADMIN", "MEMBER", "PUBLIC"])
parser_new_org.add_argument('--project-transfer-ability', default="ADMIN", help='Org membership level required to be able to change the billing account of an org-billed project to any other entity; default ADMIN', choices=["ADMIN", "MEMBER"])
parser_new_org.set_defaults(func=new_org)
register_parser(parser_new_org, subparsers_action=subparsers_new, categories='org')

parser_new_project = subparsers_new.add_parser('project', help='Create a new project',
                                               description='Create a new project',
                                               parents=[stdout_args, env_args],
                                               prog='dx new project')
parser_new_project.add_argument('name', help='Name of the new project', nargs='?')
parser_new_project.add_argument('--region', help='Region affinity of the new project')
parser_new_project.add_argument('-s', '--select', help='Select the new project as current after creating',
                                action='store_true')
parser_new_project.add_argument('--bill-to', help='ID of the user or org to which the project will be billed. The default value is the billTo of the requesting user.')
parser_new_project.add_argument('--phi', help='Add PHI protection to project', default=False,
                                action='store_true')
parser_new_project.add_argument('--database-ui-view-only', help='If set to true, viewers of the project will not be able to access database data directly', default=False,
                                action='store_true')
parser_new_project.set_defaults(func=new_project)
register_parser(parser_new_project, subparsers_action=subparsers_new, categories='fs')

parser_new_record = subparsers_new.add_parser('record', help='Create a new record',
                                              description='Create a new record',
                                              parents=[parser_dataobject_args, parser_single_dataobject_output_args,
                                                       stdout_args, env_args],
                                              formatter_class=argparse.RawTextHelpFormatter,
                                              prog='dx new record')
init_action = parser_new_record.add_argument('--init', help='Path to record from which to initialize all metadata')
parser_new_record.add_argument('--close', help='Close the record immediately after creating it', action='store_true')
init_action.completer = DXPathCompleter(classes=['record'])
parser_new_record.set_defaults(func=new_record)
register_parser(parser_new_record, subparsers_action=subparsers_new, categories='fs')

parser_new_workflow = subparsers_new.add_parser('workflow', help='Create a new workflow',
                                                description='Create a new workflow',
                                                parents=[parser_dataobject_args, parser_single_dataobject_output_args,
                                                         stdout_args, env_args],
                                                formatter_class=argparse.RawTextHelpFormatter,
                                                prog='dx new workflow')
parser_new_workflow.add_argument('--title', help='Workflow title')
parser_new_workflow.add_argument('--summary', help='Workflow summary')
parser_new_workflow.add_argument('--description', help='Workflow description')
parser_new_workflow.add_argument('--output-folder', help='Default output folder for the workflow')
init_action = parser_new_workflow.add_argument('--init', help=fill('Path to workflow or an analysis ID from which to initialize all metadata', width_adjustment=-24))
init_action.completer = DXPathCompleter(classes=['workflow'])
parser_new_workflow.set_defaults(func=workflow_cli.new_workflow)
register_parser(parser_new_workflow, subparsers_action=subparsers_new, categories='workflow')

#####################################
# get_details
#####################################
parser_get_details = subparsers.add_parser('get_details', help='Get details of a data object',
                                           description='Get the JSON details of a data object.', prog="dx get_details",
                                           parents=[env_args])
parser_get_details.add_argument('path', help='ID or path to data object to get details for').completer = DXPathCompleter()
parser_get_details.set_defaults(func=get_details)
register_parser(parser_get_details, categories='metadata')

#####################################
# set_details
#####################################
parser_set_details = subparsers.add_parser('set_details', help='Set details on a data object',
                                           description='Set the JSON details of a data object.', prog="dx set_details",
                                           parents=[env_args, all_arg])
parser_set_details.add_argument('path', help='ID or path to data object to modify').completer = DXPathCompleter()
parser_set_details.add_argument('details', help='JSON to store as details', nargs='?')
parser_set_details.add_argument('-f', '--details-file', help='Path to local file containing JSON to store as details')
parser_set_details.set_defaults(func=set_details)
register_parser(parser_set_details, categories='metadata')

#####################################
# set_visibility
#####################################
parser_set_visibility = subparsers.add_parser('set_visibility', help='Set visibility on a data object',
                                              description='Set visibility on a data object.', prog='dx set_visibility',
                                              parents=[env_args, all_arg])
parser_set_visibility.add_argument('path', help='ID or path to data object to modify').completer = DXPathCompleter()
parser_set_visibility.add_argument('visibility', choices=['hidden', 'visible'],
                                   help='Visibility that the object should have')
parser_set_visibility.set_defaults(func=set_visibility)
register_parser(parser_set_visibility, categories='metadata')

#####################################
# add_types
#####################################
parser_add_types = subparsers.add_parser('add_types', help='Add types to a data object',
                                         description='Add types to a data object.  See https://documentation.dnanexus.com/developer/api/data-object-lifecycle/types for a list of DNAnexus types.',
                                         prog='dx add_types',
                                         parents=[env_args, all_arg])
parser_add_types.add_argument('path', help='ID or path to data object to modify').completer = DXPathCompleter()
parser_add_types.add_argument('types', nargs='+', metavar='type', help='Types to add')
parser_add_types.set_defaults(func=add_types)
register_parser(parser_add_types, categories='metadata')

#####################################
# remove_types
#####################################
parser_remove_types = subparsers.add_parser('remove_types', help='Remove types from a data object',
                                            description='Remove types from a data object.  See https://documentation.dnanexus.com/developer/api/data-object-lifecycle/types for a list of DNAnexus types.',
                                            prog='dx remove_types',
                                            parents=[env_args, all_arg])
parser_remove_types.add_argument('path', help='ID or path to data object to modify').completer = DXPathCompleter()
parser_remove_types.add_argument('types', nargs='+', metavar='type', help='Types to remove')
parser_remove_types.set_defaults(func=remove_types)
register_parser(parser_remove_types, categories='metadata')

#####################################
# tag
#####################################
parser_tag = subparsers.add_parser('tag', help='Tag a project, data object, or execution', prog='dx tag',
                                   description='Tag a project, data object, or execution.  Note that a project context must be either set or specified for data object IDs or paths.',
                                   parents=[env_args, all_arg])
parser_tag.add_argument('path', help='ID or path to project, data object, or execution to modify').completer = DXPathCompleter()
parser_tag.add_argument('tags', nargs='+', metavar='tag', help='Tags to add')
parser_tag.set_defaults(func=add_tags)
register_parser(parser_tag, categories='metadata')

#####################################
# untag
#####################################
parser_untag = subparsers.add_parser('untag', help='Untag a project, data object, or execution', prog='dx untag',
                                     description='Untag a project, data object, or execution.  Note that a project context must be either set or specified for data object IDs or paths.',
                                     parents=[env_args, all_arg])
parser_untag.add_argument('path', help='ID or path to project, data object, or execution to modify').completer = DXPathCompleter()
parser_untag.add_argument('tags', nargs='+', metavar='tag', help='Tags to remove')
parser_untag.set_defaults(func=remove_tags)
register_parser(parser_untag, categories='metadata')

#####################################
# rename
#####################################
parser_rename = subparsers.add_parser('rename',
                                      help='Rename a project or data object',
                                      description='Rename a project or data object.  To rename folders, use \'dx mv\' instead.  Note that a project context must be either set or specified to rename a data object.  To specify a project or a project context, append a colon character ":" after the project ID or name.',
                                      prog='dx rename',
                                      parents=[env_args, all_arg])
path_action = parser_rename.add_argument('path', help='Path to project or data object to rename')
path_action.completer = DXPathCompleter(include_current_proj=True)
parser_rename.add_argument('name', help='New name')
parser_rename.set_defaults(func=rename)
register_parser(parser_rename, categories='metadata')

#####################################
# set_properties
#####################################
parser_set_properties = subparsers.add_parser('set_properties', help='Set properties of a project, data object, or execution',
                                              description='Set properties of a project, data object, or execution.  Note that a project context must be either set or specified for data object IDs or paths.', prog='dx set_properties',
                                              parents=[env_args, all_arg])
parser_set_properties.add_argument('path', help='ID or path to project, data object, or execution to modify').completer = DXPathCompleter()
parser_set_properties.add_argument('properties', nargs='+', metavar='propertyname=value',
                                   help='Key-value pairs of property names and their new values')
parser_set_properties.set_defaults(func=set_properties)
register_parser(parser_set_properties, categories='metadata')

#####################################
# unset_properties
#####################################
parser_unset_properties = subparsers.add_parser('unset_properties', help='Unset properties of a project, data object, or execution',
                                                description='Unset properties of a project, data object, or execution.  Note that a project context must be either set or specified for data object IDs or paths.',
                                                prog='dx unset_properties',
                                                parents=[env_args, all_arg])
path_action = parser_unset_properties.add_argument('path', help='ID or path to project, data object, or execution to modify')
path_action.completer = DXPathCompleter()
parser_unset_properties.add_argument('properties', nargs='+', metavar='propertyname', help='Property names to unset')
parser_unset_properties.set_defaults(func=unset_properties)
register_parser(parser_unset_properties, categories='metadata')

#####################################
# close
#####################################
parser_close = subparsers.add_parser('close', help='Close data object(s)',
                                     description='Close a remote data object or set of objects.',
                                     prog='dx close',
                                     parents=[env_args, all_arg])
parser_close.add_argument('path', help='Path to a data object to close', nargs='+').completer = DXPathCompleter()
parser_close.add_argument('--wait', help='Wait for the object(s) to close', action='store_true')
parser_close.set_defaults(func=close)
register_parser(parser_close, categories=('data', 'metadata'))

#####################################
# wait
#####################################
parser_wait = subparsers.add_parser('wait', help='Wait for data object(s) to close or job(s) to finish',
                                    description='Polls the state of specified data object(s) or job(s) until they are all in the desired state.  Waits until the "closed" state for a data object, and for any terminal state for a job ("terminated", "failed", or "done").  Exits with a non-zero code if a job reaches a terminal state that is not "done".  Can also provide a local file containing a list of data object(s) or job(s), one per line; the file will be read if "--from-file" argument is added.',
                                    prog='dx wait',
                                    parents=[env_args])
path_action = parser_wait.add_argument('path', help='Path to a data object, job ID, or file with IDs to wait for', nargs='+')
path_action.completer = DXPathCompleter()
parser_wait.add_argument('--from-file', help='Read the list of objects to wait for from the file provided in path', action='store_true')
parser_wait.set_defaults(func=wait)
register_parser(parser_wait, categories=('data', 'metadata', 'exec'))

#####################################
# get
#####################################
parser_get = subparsers.add_parser('get', help='Download records, apps, applets, workflows, files, and databases.',
                                   description='Download the contents of some types of data (records, apps, applets, workflows, files, and databases).  Downloading an app, applet or a workflow will attempt to reconstruct a source directory that can be used to rebuild it with "dx build".  Use "-o -" to direct the output to stdout.',
                                   prog='dx get',
                                   parents=[env_args])
parser_get.add_argument('path', help='Data object ID or name to access').completer = DXPathCompleter(classes=['file', 'record', 'applet', 'app', 'workflow', 'database'])
parser_get.add_argument('-o', '--output', help='local file path where the data is to be saved ("-" indicates stdout output for objects of class file and record). If not supplied, the object\'s name on the platform will be used, along with any applicable extensions. For app(let) and workflow objects, if OUTPUT does not exist, the object\'s source directory will be created there; if OUTPUT is an existing directory, a new directory with the object\'s name will be created inside it.')
parser_get.add_argument('--filename', default='/', help='When downloading from a database, name of the file or folder to be downloaded. If omitted, all files in the database will be downloaded, so use caution and include the --allow-all-files argument.')
parser_get.add_argument("--allow-all-files", default=False, help='When downloading from a database, this allows all files in a database to be downloaded when --filename argument is omitted.', action='store_true', dest='allow_all_files')
parser_get.add_argument("--recurse", default=False, help='When downloading from a database, look for files recursively down the directory structure. Otherwise, by default, only look on one level.', action='store_true')
parser_get.add_argument('--no-ext', help='If -o is not provided, do not add an extension to the filename', action='store_true')
parser_get.add_argument('--omit-resources', help='When downloading an app(let), omit fetching the resources associated with the app(let).', action='store_true')
parser_get.add_argument('-f', '--overwrite', help='Overwrite the local file if necessary', action='store_true')
parser_get.set_defaults(func=get)
register_parser(parser_get, categories='data')

#####################################
# find
#####################################
parser_find = subparsers.add_parser('find', help='Search functionality over various DNAnexus entities',
                                    description='Search functionality over various DNAnexus entities.',
                                    formatter_class=argparse.RawTextHelpFormatter,
                                    prog='dx find')
subparsers_find = parser_find.add_subparsers(parser_class=DXArgumentParser)
subparsers_find.metavar = 'category'
register_parser(parser_find, categories=())

parser_find_apps = subparsers_find.add_parser(
    'apps',
    help=fill('List available apps'),
    description=fill('Finds apps subject to the given search parameters. Use --category to restrict by a category; '
                     'common categories are available as tab completions and can be listed with --category-help.'),
    parents=[stdout_args, json_arg, delim_arg, env_args],
    prog='dx find apps'
)
parser_find_apps.add_argument('--name', help='Name of the app')
parser_find_apps.add_argument('--category', help='Category of the app').completer = ListCompleter(APP_CATEGORIES)
parser_find_apps.add_argument('--category-help',
                              help='Print a list of common app categories',
                              nargs=0,
                              action=PrintCategoryHelp)
parser_find_apps.add_argument('-a', '--all', help='Return all versions of each app', action='store_true')
parser_find_apps.add_argument('--unpublished', help='Return only unpublished apps (if omitted, returns only published apps)', action='store_true')
parser_find_apps.add_argument('--installed', help='Return only installed apps', action='store_true')
parser_find_apps.add_argument('--billed-to', help='User or organization responsible for the app')
parser_find_apps.add_argument('--creator', help='Creator of the app version')
parser_find_apps.add_argument('--developer', help='Developer of the app')
parser_find_apps.add_argument('--created-after', help='''Date (e.g. --created-after="2021-12-01" or --created-after="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --created-after=1642196636000) after which the app created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --created-after=-2d for apps created in the last 2 days)''')
parser_find_apps.add_argument('--created-before', help='''Date (e.g. --created-before="2021-12-01" or --created-before="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --created-before=1642196636000) before which the app was created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --created-before=-2d for apps created earlier than 2 days ago)''')
parser_find_apps.add_argument('--mod-after',help='''Date (e.g. --mod-after="2021-12-01" or --mod-after="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --mod-after=1642196636000) after which the app modified. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --mod-after=-2d for apps modified in the last 2 days)''')
parser_find_apps.add_argument('--mod-before', help='''Date (e.g. --mod-before="2021-12-01" or --mod-before="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --mod-before=1642196636000) after which the app modified. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --mod-before=-2d for apps modified earlier than 2 days ago)''')
parser_find_apps.set_defaults(func=find_apps)
register_parser(parser_find_apps, subparsers_action=subparsers_find, categories='exec')

parser_find_globalworkflows = subparsers_find.add_parser(
    'globalworkflows',
    help=fill('List available global workflows'),
    description=fill('Finds global workflows subject to the given search parameters. Use --category to restrict by a category; '
                     'common categories are available as tab completions and can be listed with --category-help.'),
    parents=[stdout_args, json_arg, delim_arg, env_args],
    prog='dx find globalworkflows'
)
parser_find_globalworkflows.add_argument('--name', help='Name of the workflow')
parser_find_globalworkflows.add_argument('--category', help='Category of the workflow').completer = ListCompleter(APP_CATEGORIES)
parser_find_globalworkflows.add_argument('--category-help',
                              help='Print a list of common global workflow categories',
                              nargs=0,
                              action=PrintCategoryHelp)
parser_find_globalworkflows.add_argument('-a', '--all', help='Return all versions of each workflow', action='store_true')
parser_find_globalworkflows.add_argument('--unpublished', help='Return only unpublished workflows (if omitted, returns only published workflows)', action='store_true')
parser_find_globalworkflows.add_argument('--billed-to', help='User or organization responsible for the workflow')
parser_find_globalworkflows.add_argument('--creator', help='Creator of the workflow version')
parser_find_globalworkflows.add_argument('--developer', help='Developer of the workflow')
parser_find_globalworkflows.add_argument('--created-after', help='''Date (e.g. --created-after="2021-12-01" or --created-after="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --created-after=1642196636000) after which the workflow was created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --created-after=-2d for workflows created in the last 2 days).''')
parser_find_globalworkflows.add_argument('--created-before', help='''Date (e.g. --created-before="2021-12-01" or --created-before="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --created-before=1642196636000) before which the workflow was created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --created-before=-2d for workflows created earlier than 2 days ago)''')
parser_find_globalworkflows.add_argument('--mod-after',help='''Date (e.g. --mod-after="2021-12-01" or --mod-after="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --mod-after=1642196636000) after which the workflow was created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --mod-after=-2d for workflows modified in the last 2 days)''')
parser_find_globalworkflows.add_argument('--mod-before', help='''Date (e.g. --mod-before="2021-12-01" or --mod-before="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --mod-before=1642196636000) before which the workflow was created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --mod-before=-2d for workflows modified earlier than 2 days ago)''')
parser_find_globalworkflows.set_defaults(func=find_global_workflows)
register_parser(parser_find_globalworkflows, subparsers_action=subparsers_find, categories='exec')

parser_find_jobs = subparsers_find.add_parser(
    'jobs',
    help=fill('List jobs in the current project'),
    description=fill('Finds jobs subject to the given search parameters. By default, output is formatted to show the '
                     'last several job trees that you\'ve run in the current project.'),
    parents=[find_executions_args, stdout_args, json_arg, no_color_arg, delim_arg, env_args,
             find_by_properties_and_tags_args],
    formatter_class=argparse.RawTextHelpFormatter,
    conflict_handler='resolve',
    prog='dx find jobs'
)
add_find_executions_search_gp(parser_find_jobs)
parser_find_jobs.set_defaults(func=find_executions, classname='job')
parser_find_jobs.completer = DXPathCompleter(expected='project')
register_parser(parser_find_jobs, subparsers_action=subparsers_find, categories='exec')

parser_find_analyses = subparsers_find.add_parser(
    'analyses',
    help=fill('List analyses in the current project'),
    description=fill('Finds analyses subject to the given search parameters. By default, output is formatted to show '
                     'the last several job trees that you\'ve run in the current project.'),
    parents=[find_executions_args, stdout_args, json_arg, no_color_arg, delim_arg, env_args,
             find_by_properties_and_tags_args],
    formatter_class=argparse.RawTextHelpFormatter,
    conflict_handler='resolve',
    prog='dx find analyses'
)
add_find_executions_search_gp(parser_find_analyses)
parser_find_analyses.set_defaults(func=find_executions, classname='analysis')
parser_find_analyses.completer = DXPathCompleter(expected='project')
register_parser(parser_find_analyses, subparsers_action=subparsers_find, categories='exec')

parser_find_executions = subparsers_find.add_parser(
    'executions',
    help=fill('List executions (jobs and analyses) in the current project'),
    description=fill('Finds executions (jobs and analyses) subject to the given search parameters. By default, output '
                     'is formatted to show the last several job trees that you\'ve run in the current project.'),
    parents=[find_executions_args, stdout_args, json_arg, no_color_arg, delim_arg, env_args,
             find_by_properties_and_tags_args],
    formatter_class=argparse.RawTextHelpFormatter,
    conflict_handler='resolve',
    prog='dx find executions'
)
add_find_executions_search_gp(parser_find_executions)
parser_find_executions.set_defaults(func=find_executions, classname=None)
parser_find_executions.completer = DXPathCompleter(expected='project')
register_parser(parser_find_executions, subparsers_action=subparsers_find, categories='exec')

parser_find_data = subparsers_find.add_parser(
    'data',
    help=fill('List data objects in the current project'),
    description=fill('Finds data objects subject to the given search parameters. By default, restricts the search to '
                     'the current project if set. To search over all projects (excluding public projects), use '
                     '--all-projects (overrides --path and --norecurse).'),
    parents=[stdout_args, json_arg, no_color_arg, delim_arg, env_args, find_by_properties_and_tags_args],
    prog='dx find data'
)
parser_find_data.add_argument('--class', dest='classname', choices=['record', 'file', 'applet', 'workflow', 'database'],
    help='Data object class',
    metavar='{record,file,applet,workflow,database}'
)
parser_find_data.add_argument('--state', choices=['open', 'closing', 'closed', 'any'], help='State of the object')
parser_find_data.add_argument('--visibility', choices=['hidden', 'visible', 'either'], default='visible', help='Whether the object is hidden or not')
parser_find_data.add_argument('--name', help='Name of the object')
parser_find_data.add_argument('--type', help='Type of the data object')
parser_find_data.add_argument('--link', help='Object ID that the data object links to')
parser_find_data.add_argument('--all-projects', '--allprojects', help='Extend search to all projects (excluding public projects)', action='store_true')
parser_find_data.add_argument('--project', help=argparse.SUPPRESS)
parser_find_data.add_argument('--folder', help=argparse.SUPPRESS).completer = DXPathCompleter(expected='folder')
parser_find_data.add_argument('--path', help='Project and/or folder in which to restrict the results',
                              metavar='PROJECT:FOLDER').completer = DXPathCompleter(expected='folder')
parser_find_data.add_argument('--norecurse', dest='recurse', help='Do not recurse into subfolders', action='store_false')
parser_find_data.add_argument('--created-after', help='''Date (e.g. --created-after="2021-12-01" or --created-after="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --created-after=1642196636000) after which the object was created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --created-after=-2d for objects created in the last 2 days).''')
parser_find_data.add_argument('--created-before', help='''Date (e.g. --created-before="2021-12-01" or --created-before="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --created-before=1642196636000) before which the object was created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --created-before=-2d for objects created earlier than 2 days ago)''')
parser_find_data.add_argument('--mod-after',help='''Date (e.g. --mod-after="2021-12-01" or --mod-after="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --mod-after=1642196636000) after which the object was modified. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --mod-after=-2d for objects modified in the last 2 days)''')
parser_find_data.add_argument('--mod-before', help='''Date (e.g. --mod-before="2021-12-01" or --mod-before="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --mod-before=1642196636000) before which the object was modified. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --mod-before=-2d for objects modified earlier than 2 days ago)''')
parser_find_data.add_argument('--region', help='Restrict the search to the provided region')

parser_find_data.set_defaults(func=find_data)
register_parser(parser_find_data, subparsers_action=subparsers_find, categories=('data', 'metadata'))

parser_find_projects = subparsers_find.add_parser(
    'projects',
    help=fill('List projects'),
    description=fill('Finds projects subject to the given search parameters. Use the --public flag to list all public '
                     'projects.'),
    parents=[stdout_args, json_arg, delim_arg, env_args, find_by_properties_and_tags_args, contains_phi],
    prog='dx find projects'
)
parser_find_projects.add_argument('--name', help='Name of the project')
parser_find_projects.add_argument('--level', choices=['VIEW', 'UPLOAD', 'CONTRIBUTE', 'ADMINISTER'],
                                  help='Minimum level of permissions expected')
parser_find_projects.add_argument('--public',
                                  help='Include ONLY public projects (will automatically set --level to VIEW)',
                                  action='store_true')
parser_find_projects.add_argument('--created-after',
                                  help='''Date (e.g. --created-after="2021-12-01" or --created-after="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --created-after=1642196636000) after which the project was created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --created-after=-2d for projects created in the last 2 days).''')
parser_find_projects.add_argument('--created-before',
                                  help='''Date (e.g. --created-before="2021-12-01" or --created-before="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --created-before=1642196636000) before which the project was created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --created-before=-2d for projects created earlier than 2 days ago)''')
parser_find_projects.add_argument('--region', help='Restrict the search to the provided region')
parser_find_projects.set_defaults(func=find_projects)
register_parser(parser_find_projects, subparsers_action=subparsers_find, categories='data')

parser_find_org = subparsers_find.add_parser(
    "org",
    help=fill("List entities within a specific org.") + "\n\n\t" +
         fill('"dx find org members" lists members in the specified org') + "\n\n\t" +
         fill('"dx find org projects" lists projects billed to the specified org') + "\n\n\t" +
         fill('"dx find org apps" lists apps billed to the specified org') + "\n\n" +
         fill('Please execute "dx find org -h" for more information.'),
    description=fill("List entities within a specific org."),
    prog="dx find org",
)
register_parser(parser_find_org, subparsers_action=subparsers_find)

subparsers_find_org = parser_find_org.add_subparsers(parser_class=DXArgumentParser)
subparsers_find_org.metavar = "entities"

parser_find_org_members = subparsers_find_org.add_parser(
    'members',
    help='List members in the specified org',
    description=fill('Finds members in the specified org subject to the given search parameters'),
    parents=[stdout_args, json_arg, delim_arg, env_args],
    prog='dx find org members'
)
parser_find_org_members.add_argument('org_id', help='Org ID')
parser_find_org_members.add_argument('--level', choices=["ADMIN", "MEMBER"], help='Restrict the result set to contain only members at the specified membership level.')
parser_find_org_members.set_defaults(func=org_find_members)
register_parser(parser_find_org_members, subparsers_action=subparsers_find_org, categories='org')

parser_find_org_projects = subparsers_find_org.add_parser(
    'projects',
    help='List projects billed to the specified org',
    description=fill('Finds projects billed to the specified org subject to the given search parameters. You must '
                     'be an ADMIN of the specified org to use this command. It allows you to identify projects billed '
                     'to the org that have not been shared with you explicitly.'),
    parents=[stdout_args, json_arg, delim_arg, env_args, find_by_properties_and_tags_args, contains_phi],
    prog='dx find org projects'
)
parser_find_org_projects.add_argument('org_id', help='Org ID')
parser_find_org_projects.add_argument('--name', help='Name of the projects')
parser_find_org_projects.add_argument('--ids', nargs='+', help='Possible project IDs. May be specified like "--ids project-1 project-2"')
find_org_projects_public = parser_find_org_projects.add_mutually_exclusive_group()
find_org_projects_public.add_argument('--public-only', dest='public', help='Include only public projects', action='store_true', default=None)
find_org_projects_public.add_argument('--private-only', dest='public', help='Include only private projects', action='store_false', default=None)
parser_find_org_projects.add_argument('--created-after', help='''Date (e.g. --created-after="2021-12-01" or --created-after="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --created-after=1642196636000) after which the project was created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --created-after=-2d for projects created in the last 2 days).''')
parser_find_org_projects.add_argument('--created-before', help='''Date (e.g. --created-before="2021-12-01" or --created-before="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --created-before=1642196636000) before which the project was created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --created-before=-2d for projects created earlier than 2 days ago)''')
parser_find_org_projects.add_argument('--region', help='Restrict the search to the provided region')
parser_find_org_projects.set_defaults(func=org_find_projects)
register_parser(parser_find_org_projects, subparsers_action=subparsers_find_org, categories=('data', 'org'))

parser_find_org_apps = subparsers_find_org.add_parser(
    'apps',
    help='List apps billed to the specified org',
    description=fill('Finds apps billed to the specified org subject to the given search parameters. You must '
                     'be an ADMIN of the specified org to use this command. It allows you to identify apps billed '
                     'to the org that have not been shared with you explicitly.'),
    parents=[stdout_args, json_arg, delim_arg, env_args],
    prog='dx find org apps'
)
parser_find_org_apps.add_argument('org_id', help='Org ID')
parser_find_org_apps.add_argument('--name', help='Name of the apps')
parser_find_org_apps.add_argument('--category', help='Category of the app').completer = ListCompleter(APP_CATEGORIES)
parser_find_org_apps.add_argument('--category-help',
                                  help='Print a list of common app categories',
                                  nargs=0,
                                  action=PrintCategoryHelp)

parser_find_org_apps.add_argument('-a', '--all', help='Return all versions of each app', action='store_true')
parser_find_org_apps.add_argument('--unpublished', help='Return only unpublished apps (if omitted, returns all apps)', action='store_true')
parser_find_org_apps.add_argument('--installed', help='Return only installed apps', action='store_true')
parser_find_org_apps.add_argument('--creator', help='Creator of the app version')
parser_find_org_apps.add_argument('--developer', help='Developer of the app')
parser_find_org_apps.add_argument('--created-after', help='''Date (e.g. --created-after="2021-12-01" or --created-after="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --created-after=1642196636000) after which the app was created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --created-after=-2d for apps created in the last 2 days).''')
parser_find_org_apps.add_argument('--created-before', help='''Date (e.g. --created-before="2021-12-01" or --created-before="2021-12-01 19:01:33") or integer Unix epoch timestamp in milliseconds (e.g. --created-before=1642196636000) before which the app was created. You can also specify negative numbers to indicate a time period in the past suffixed by s, m, h, d, w, M or y to indicate seconds, minutes, hours, days, weeks, months or years (e.g. --created-before=-2d for apps created earlier than 2 days ago)''')
parser_find_org_apps.add_argument('--mod-after', help='''Date (e.g. 2012-01-01) or integer timestamp after which the app was last modified (negative number means seconds in the past, or use suffix s, m, h, d, w, M, y) 
                                                         Negative input example "--mod-after=-2d"''')
parser_find_org_apps.add_argument('--mod-before', help='''Date (e.g. 2012-01-01) or integer timestamp before which the app was last modified (negative number means seconds in the past, or use suffix s, m, h, d, w, M, y) 
                                                          Negative input example "--mod-before=-2d"''')
parser_find_org_apps.set_defaults(func=org_find_apps)
register_parser(parser_find_org_apps, subparsers_action=subparsers_find_org, categories=('exec', 'org'))

parser_find_orgs = subparsers_find.add_parser(
    "orgs",
    help=fill("List orgs"),
    description="Finds orgs subject to the given search parameters.",
    parents=[stdout_args, env_args, delim_arg, json_arg],
    prog="dx find orgs"
)
parser_find_orgs.add_argument("--level", choices=["ADMIN", "MEMBER"], required=True, help="Restrict the result set to contain only orgs in which the requesting user has at least the specified membership level")
parser_find_orgs_with_billable_activities = parser_find_orgs.add_mutually_exclusive_group()
parser_find_orgs_with_billable_activities.add_argument("--with-billable-activities", action="store_true", help="Restrict the result set to contain only orgs in which the requesting user can perform billable activities; mutually exclusive with --without-billable-activities")
parser_find_orgs_with_billable_activities.add_argument("--without-billable-activities", dest="with_billable_activities", action="store_false", help="Restrict the result set to contain only orgs in which the requesting user **cannot** perform billable activities; mutually exclusive with --with-billable-activities")
parser_find_orgs.set_defaults(func=find_orgs, with_billable_activities=None)
register_parser(parser_find_orgs, subparsers_action=subparsers_find, categories="org")

#####################################
# notebook
#####################################

from ..ssh_tunnel_app_support import run_notebook
parser_notebook = subparsers.add_parser('notebook', help='Launch a web notebook inside DNAnexus.',
                                        description='Launch a web notebook inside DNAnexus.',
                                        formatter_class=argparse.RawTextHelpFormatter,
                                        prog='dx notebook')
parser_notebook.add_argument('notebook_type', help='Type of notebook to launch', choices=['jupyter_lab', 'jupyter_notebook', 'jupyter'])
parser_notebook.add_argument('--notebook_files', help='Files to include on notebook instance', default=[], nargs='*').completer = DXPathCompleter(classes=['file'])
parser_notebook.add_argument('--spark', help='Install spark infrastructure.', action='store_true', default=False)
parser_notebook.add_argument('--port', help='local port to use to access the notebook.', default='2001')
parser_notebook.add_argument('--snapshot', help='A snapshot file to reform on the server.').completer = DXPathCompleter(classes=['file'])
parser_notebook.add_argument('--timeout', help='How long to keep the notebook open (smhwMy).', default='1h')
parser_notebook.add_argument('-q', '--quiet', help='Do not launch web browser.', action='store_false', dest='open_server')
parser_notebook.add_argument('--version', help='What version of the notebook app to launch.', default=None)
parser_notebook.add_argument('--instance-type', help='Instance type to run the notebook on.', default='mem1_ssd1_x4')
parser_notebook.add_argument('--only_check_config', help='Only check SSH config do not launch app', action='store_true')
notebook_with_ssh_config_check = functools.partial(run_notebook, ssh_config_check=verify_ssh_config)
parser_notebook.set_defaults(func=notebook_with_ssh_config_check)
register_parser(parser_notebook, categories='data', add_help=False)

from ..ssh_tunnel_app_support import run_loupe
parser_loupe_viewer = subparsers.add_parser('loupe-viewer', help='Launch the Loupe viewer for 10x data on DNAnexus.',
                                            description='Launch the Loupe viewer for 10x data on DNAnexus.',
                                            formatter_class=argparse.RawTextHelpFormatter,
                                            prog='dx loupe-viewer')
parser_loupe_viewer.add_argument('loupe_files', help='Files to include in loupe viewer', default=[], nargs=argparse.REMAINDER).completer = DXPathCompleter(classes=['file'])
parser_loupe_viewer.add_argument('--port', help='local port to use to access the Loupe viewer.', default='2001')
parser_loupe_viewer.add_argument('--timeout', help='How long to keep the Loupe viewer open (smhwMy).', default='1h')
parser_loupe_viewer.add_argument('-q', '--quiet', help='Do not launch web browser.', action='store_false', dest='open_server')
parser_loupe_viewer.add_argument('--instance-type', help='Instance type to run the Loupe viewer on.', default='mem1_ssd1_x4')
parser_loupe_viewer.set_defaults(func=run_loupe)
register_parser(parser_loupe_viewer, categories='data', add_help=False)

#####################################
# api
#####################################
parser_api = subparsers.add_parser('api', help='Call an API method',
                                   formatter_class=argparse.RawTextHelpFormatter,
                                   description=fill('Call an API method directly.  The JSON response from the API server will be returned if successful.  No name resolution is performed; DNAnexus IDs must always be provided.  The API specification can be found at') + '''

https://documentation.dnanexus.com/developer/api

EXAMPLE

  In the following example, a project's description is changed.

  $ dx api project-B0VK6F6gpqG6z7JGkbqQ000Q update '{"description": "desc"}'
  {
      "id": "project-B0VK6F6gpqG6z7JGkbqQ000Q"
  }

''',
                                   prog='dx api',
                                   parents=[env_args])
parser_api.add_argument('resource', help=fill('One of "system", a class name (e.g. "record"), or an entity ID such as "record-xxxx".  Use "app-name/1.0.0" to refer to version "1.0.0" of the app named "name".', width_adjustment=-17))
parser_api.add_argument('method', help=fill('Method name for the resource as documented by the API specification', width_adjustment=-17))
parser_api.add_argument('input_json', nargs='?', default="{}", help='JSON input for the method (if not given, "{}" is used)')
parser_api.add_argument('--input', help=fill('Load JSON input from FILENAME ("-" to use stdin)', width_adjustment=-17))
parser_api.set_defaults(func=api)
# parser_api.completer = TODO
register_parser(parser_api)

#####################################
# upgrade
#####################################
parser_upgrade = subparsers.add_parser('upgrade', help='Upgrade dx-toolkit (the DNAnexus SDK and this program)',
                                       description='Upgrades dx-toolkit (the DNAnexus SDK and this program) to the latest recommended version, or to a specified version and platform.',
                                       prog='dx upgrade')
parser_upgrade.add_argument('args', nargs='*')
parser_upgrade.set_defaults(func=upgrade)
register_parser(parser_upgrade)

#####################################
# generate_batch_inputs
#####################################

parser_generate_batch_inputs = subparsers.add_parser('generate_batch_inputs', help='Generate a batch plan (one or more TSV files) for batch execution',
                                       description='Generate a table of input files matching desired regular expressions for each input.',
                                       prog='dx generate_batch_inputs')
parser_generate_batch_inputs.add_argument('-i', '--input', help=fill('An input to be batch-processed "-i<input name>=<input pattern>" where <input_pattern> is a regular expression with a group corresponding to the desired region to match (e.g. "-iinputa=SRR(.*)_1.gz" "-iinputb=SRR(.*)_2.gz")', width_adjustment=-24), action='append')
parser_generate_batch_inputs.add_argument('--path', help='Project and/or folder to which the search for input files will be restricted',
                              metavar='PROJECT:FOLDER', default='').completer = DXPathCompleter(expected='folder')
parser_generate_batch_inputs.add_argument('-o', '--output_prefix', help='Prefix for output file', default="dx_batch")
parser_generate_batch_inputs.set_defaults(func=generate_batch_inputs)
register_parser(parser_generate_batch_inputs)

#####################################
# publish
#####################################
parser_publish = subparsers.add_parser('publish', help='Publish an app or a global workflow',
                                   description='Release a version of the executable (app or global workflow) to authorized users.',
                                   prog='dx publish')
parser_publish.add_argument('executable',
                            help='ID or name and version of an app/global workflow, e.g. myqc/1.0.0').completer = DXPathCompleter(classes=['app', 'globalworkflow'])
parser_publish.add_argument('--no-default',
                            help='Do not set a "default" alias on the published version',
                            action='store_false', dest='make_default')
parser_publish.set_defaults(func=publish)
register_parser(parser_publish)

#####################################
# archive
#####################################
                               
parser_archive = subparsers.add_parser(
    'archive', 
    help='Requests for the specified set files or for the files in a single specified folder in one project to be archived on the platform', 
    description=
'''
Requests for {} or for the files in {} in {} to be archived on the platform.
For each file, if this is the last copy of a file to have archival requested, it will trigger the full archival of the object. 
Otherwise, the file will be marked in an archival state denoting that archival has been requested.
'''.format(BOLD('the specified set files'), BOLD('a single specified folder'), BOLD('ONE project')) +
'''
The input paths should be either 1 folder path or up to 1000 files, and all path(s) need to be in the same project. 
To specify which project to use, prepend the path or ID of the file/folder with the project ID or name and a colon. 

EXAMPLES:

    # archive 3 files in project "FirstProj" with project ID project-B0VK6F6gpqG6z7JGkbqQ000Q
    $ dx archive FirstProj:file-B0XBQFygpqGK8ZPjbk0Q000Q FirstProj:/path/to/file1 project-B0VK6F6gpqG6z7JGkbqQ000Q:/file2
    
    # archive 2 files in current project. Specifying file ids saves time by avoiding file name resolution.
    $ dx select FirstProj
    $ dx archive file-A00000ygpqGK8ZPjbk0Q000Q file-B00000ygpqGK8ZPjbk0Q000Q

    # archive all files recursively in project-B0VK6F6gpqG6z7JGkbqQ000Q
    $ dx archive project-B0VK6F6gpqG6z7JGkbqQ000Q:/
  ''',
  formatter_class=argparse.RawTextHelpFormatter,
  parents=[all_arg],
  prog='dx archive')

parser_archive.add_argument('-q', '--quiet', help='Do not print extra info messages', 
                            action='store_true')
parser_archive.add_argument(
    '--all-copies', 
    dest = "all_copies", 
    help=fill('If true, archive all the copies of files in projects with the same billTo org.' ,width_adjustment=-24)+ '\n'+ fill('See https://documentation.dnanexus.com/developer/api/data-containers/projects#api-method-project-xxxx-archive for details.',width_adjustment=-24), 
                            default=False, action='store_true')
parser_archive.add_argument(
    '-y','--yes', dest='confirm',
    help=fill('Do not ask for confirmation.' , width_adjustment=-24), 
    default=True, action='store_false')
parser_archive.add_argument('--no-recurse', dest='recurse',help=fill('When `path` refers to a single folder, this flag causes only files in the specified folder and not its subfolders to be archived. This flag has no impact when `path` input refers to a collection of files.', width_adjustment=-24), action='store_false')

parser_archive.add_argument(
    'path', 
    help=fill('May refer to a single folder or specify up to 1000 files inside a project.',width_adjustment=-24),
    default=[], nargs='+').completer = DXPathCompleter() 

parser_archive_output = parser_archive.add_argument_group(title='Output', description='If -q option is not specified, prints "Tagged <count> file(s) for archival"')

parser_archive.set_defaults(func=archive, request_mode = "archival")  
register_parser(parser_archive, categories='fs')

#####################################
# unarchive
#####################################

parser_unarchive = subparsers.add_parser(
    'unarchive', 
    help='Requests for the specified set files or for the files in a single specified folder in one project to be unarchived on the platform.',    
    description=
'''
Requests for {} or for the files in {} in {} to be unarchived on the platform.
The requested copy will eventually be transitioned over to the live state while all other copies will move over to the archival state.
'''.format(BOLD('a specified set files'), BOLD('a single specified folder'), BOLD('ONE project')) +
'''
The input paths should be either 1 folder path or up to 1000 files, and all path(s) need to be in the same project. 
To specify which project to use, prepend the path or ID of the file/folder with the project ID or name and a colon.

EXAMPLES:

    # unarchive 3 files in project "FirstProj" with project ID project-B0VK6F6gpqG6z7JGkbqQ000Q 
    $ dx unarchive FirstProj:file-B0XBQFygpqGK8ZPjbk0Q000Q FirstProj:/path/to/file1 project-B0VK6F6gpqG6z7JGkbqQ000Q:/file2
 
    # unarchive 2 files in current project. Specifying file ids saves time by avoiding file name resolution.
    $ dx select FirstProj
    $ dx unarchive file-A00000ygpqGK8ZPjbk0Q000Q file-B00000ygpqGK8ZPjbk0Q000Q

    # unarchive all files recursively in project-B0VK6F6gpqG6z7JGkbqQ000Q
    $ dx unarchive project-B0VK6F6gpqG6z7JGkbqQ000Q:/
  ''',
    formatter_class=argparse.RawTextHelpFormatter,
    parents=[all_arg],
    prog='dx unarchive')

parser_unarchive.add_argument('--rate', help=fill('The speed at which all files in this request are unarchived.', width_adjustment=-24) + '\n'+ fill('- Azure regions: {Expedited, Standard}', width_adjustment=-24,initial_indent='  ') + '\n'+ 
fill('- AWS regions: {Expedited, Standard, Bulk}', width_adjustment=-24,initial_indent='  '), choices=["Expedited", "Standard", "Bulk"], default="Standard")

parser_unarchive.add_argument('-q', '--quiet', help='Do not print extra info messages', action='store_true')
parser_unarchive.add_argument(
    '-y','--yes', dest='confirm',
    help=fill('Do not ask for confirmation.' , width_adjustment=-24), 
    default=True, action='store_false')
parser_unarchive.add_argument('--no-recurse', dest='recurse',help=fill('When `path` refers to a single folder, this flag causes only files in the specified folder and not its subfolders to be unarchived. This flag has no impact when `path` input refers to a collection of files.', width_adjustment=-24), action='store_false')

parser_unarchive.add_argument(
    'path', 
    help=fill('May refer to a single folder or specify up to 1000 files inside a project.', width_adjustment=-24),
    default=[], nargs='+').completer = DXPathCompleter() 

parser_unarchive.add_argument_group(title='Output', description='If -q option is not specified, prints "Tagged <> file(s) for unarchival, totalling <> GB, costing <> "')
parser_unarchive.set_defaults(func=archive, request_mode="unarchival")
register_parser(parser_unarchive, categories='fs')

#####################################
# extract_dataset
#####################################
parser_extract_dataset = subparsers.add_parser('extract_dataset', help="Retrieves the data or generates SQL to retrieve the data from a dataset or cohort for a set of entity.fields. Additionally, the dataset's dictionary can be extracted independently or in conjunction with data.",
                                   description="Retrieves the data or generates SQL to retrieve the data from a dataset or cohort for a set of entity.fields. Additionally, the dataset's dictionary can be extracted independently or in conjunction with data.",
                                   prog='dx extract_dataset')
parser_extract_dataset.add_argument('path', help='v3.0 Dataset or Cohort object ID (project-id:record-id where "record-id" indicates the record ID in the currently selected project) or name')
parser_extract_dataset.add_argument('-ddd', '--dump-dataset-dictionary', action="store_true", default=False, help='If provided, the three dictionary files, <record_name>.data_dictionary.csv, <record_name>.entity_dictionary.csv, and <record_name>.codings.csv will be generated. Files will be comma delimited and written to the local working directory, unless otherwise specified using --delimiter and --output arguments. If any of the three dictionary files does not contain data (i.e. the dictionary is empty), then that particular file will not be created.')
parser_extract_dataset.add_argument('--fields', nargs='+', help='A comma-separated string where each value is the phenotypic entity name and field name, separated by a dot.  For example: "<entity_name>.<field_name>,<entity_name>.<field_name>". If multiple entities are provided, field values will be automatically inner joined. If only the --fields argument is provided, data will be retrieved and returned. If both --fields and --sql arguments are provided, a SQL statement to retrieve the specified field data will be automatically generated and returned.')
parser_extract_dataset.add_argument('--sql', action="store_true", default=False, help='If provided, a SQL statement (string) will be returned to query the set of entity.fields, instead of returning stored values from the set of entity.fields')
parser_extract_dataset.add_argument('--delim', '--delimiter', nargs='?', const=',', default=',', help='Always use exactly one of DELIMITER to separate fields to be printed; if no delimiter is provided with this flag, COMMA will be used')
parser_extract_dataset.add_argument('-o', '--output', help='Local filename or directory to be used ("-" indicates stdout output). If not supplied, output will create a file with a default name in the current folder')
parser_extract_dataset.set_defaults(func=extract_dataset)
register_parser(parser_extract_dataset)

#####################################
# help
#####################################
category_list = '\n  '.join([category + parser_categories[category]['desc'] for category in parser_categories_sorted])
parser_help = subparsers.add_parser('help', help='Display help messages and dx commands by category',
                                    description=fill('Displays the help message for the given command (and subcommand if given), or displays the list of all commands in the given category.') + '\n\nCATEGORIES\n\n  ' + category_list + '''

EXAMPLE

  ''' + fill('To find all commands related to running and monitoring a job and then display the help message for the command "run", run', subsequent_indent='  ') + '''

  $ dx help exec
    <list of all execution-related dx commands>
  $ dx help run
    <help message for dx run>
''', formatter_class=argparse.RawTextHelpFormatter, prog='dx help')
parser_help.add_argument('command_or_category', help=fill('Display the help message for the given command, or the list of all available commands for the given category', width_adjustment=-24), nargs='?', default=None)
parser_help.add_argument('subcommand', help=fill('Display the help message for the given subcommand of the command', width_adjustment=-23), nargs='?', default=None)
parser_help.set_defaults(func=print_help)
# TODO: make this completer conditional on whether "help run" is in args
# parser_help.completer = MultiCompleter([DXAppCompleter(),
#                                         DXPathCompleter(classes=['applet'])])
parser_map['help'] = parser_help # TODO: a special help completer
parser_map['help run'] = parser_help
for category in parser_categories:
    parser_categories[category]['cmds'].append(('help', subparsers._choices_actions[-1].help))
parser_categories['all']['cmds'].sort()


def main():
    # Bash argument completer hook
    if '_ARGCOMPLETE' in os.environ:
        import argcomplete

        # In python-3 we need to use a binary output stream
        if USING_PYTHON2:
            output_stream = sys.stdout
        else:
            output_stream = sys.stdout.buffer
        argcomplete.autocomplete(parser,
                                 always_complete_options=False,
                                 exclude=['gtable', 'export'],
                                 output_stream=output_stream if '_DX_ARC_DEBUG' in os.environ else None)

    if len(args_list) > 0:
        args = parser.parse_args(args_list)
        dxpy.USER_AGENT += " {prog}-{command}".format(prog=parser.prog, command=getattr(args, 'command', ''))
        set_cli_colors(args)
        set_delim(args)
        set_env_from_args(args)
        if not hasattr(args, 'func'):
            # Something was wrong in the command line. Print the help message for
            # this particular combination of command line words.
            parser.parse_args(args_list + ["--help"])
            sys.exit(1)
        try:
            args.func(args)
            # Flush buffered data in stdout before interpreter shutdown to ignore broken pipes
            sys.stdout.flush()
        except:
            err_exit()
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == '__main__':
    main()
