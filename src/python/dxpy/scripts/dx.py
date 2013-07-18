#!/usr/bin/env python
# coding: utf-8
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

import os, sys, datetime, urlparse, getpass, collections, re, json, time, urllib, argparse, textwrap, copy, hashlib, errno, httplib
import shlex # respects quoted substrings when splitting

from ..exceptions import err_exit, default_expected_exceptions, DXError, DXCLIError
from ..packages import requests

# Try to reset encoding to utf-8
# Note: This is incompatible with pypy
# Note: In addition to PYTHONIOENCODING=UTF-8, this also enables command-line arguments to be decoded properly.
try:
    import sys, locale
    reload(sys).setdefaultencoding(locale.getdefaultlocale()[1])
except:
    pass

try:
    import colorama
    colorama.init()
except:
    pass

if not os.environ.has_key('_ARGCOMPLETE'):
    try:
        # Hack: on some operating systems, like Mac, readline spews
        # escape codes into the output at import time if TERM is set to
        # xterm (or xterm-256color). This can be a problem if dx is
        # being used noninteractively (e.g. --json) and its output will
        # be redirected or parsed elsewhere.
        #
        # http://reinout.vanrees.org/weblog/2009/08/14/readline-invisible-character-hack.html
        old_term_setting = None
        if os.environ.has_key('TERM') and os.environ['TERM'].startswith('xterm'):
            old_term_setting = os.environ['TERM']
            os.environ['TERM'] = 'vt100'
        import readline
        if old_term_setting:
            os.environ['TERM'] = old_term_setting

        if 'libedit' in readline.__doc__:
            print >>sys.stderr, 'Warning: incompatible readline module detected (libedit), tab completion disabled'
    except ImportError:
        if os.name != 'nt':
            print >>sys.stderr, 'Warning: readline module is not available, tab completion disabled'

state = {"interactive": False,
         "colors": "auto",
         "delimiter": None,
         "currentproj": None}
parser_map = {}
parser_categories_sorted = ["all", "session", "fs", "data", "metadata", "exec", "other"]
parser_categories = {"all": {"desc": "\t\tAll commands",
                             "cmds": []},
                     "session": {"desc": "\tManage your login session",
                                 "cmds": []},
                     "fs": {"desc": "\t\tNavigate and organize your projects and files",
                            "cmds": []},
                     "data": {"desc": "\t\tView, download, and upload data",
                              "cmds": []},
                     "metadata": {"desc": "\tView and modify metadata for projects and data objects",
                                 "cmds": []},
                     "exec": {"desc": "\t\tManage and run apps, applets, and workflows",
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

def try_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except:
        err_exit(expected_exceptions=default_expected_exceptions + (DXError,))

def get_json_from_stdin():
    user_json_str = raw_input('Type JSON here> ')
    user_json = None
    try:
        user_json = json.loads(user_json_str)
    except ValueError:
        parser.exit(1, 'Error: user input could not be parsed as JSON\n')
        return None
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

# Loading environment

#args_list = [unicode(arg, 'utf-8') for arg in sys.argv[1:]]
args_list = map(unicode, sys.argv[1:])

# Hard-coding a shortcut so that it won't print out the warning in
# import dxpy when clearing it anyway.
if len(args_list) > 0 and args_list[0] == 'clearenv':
    from dxpy.utils.env import clearenv
    clearenv(argparse.Namespace(interactive=False, reset=True if '--reset' in args_list else False))
    exit(0)

# importing dxpy will now appropriately load env variables
import dxpy
from dxpy.utils import group_array_by_field, normalize_timedelta, normalize_time_input
from dxpy.utils.env import clearenv, write_env_var
from dxpy.utils.printing import (CYAN, BLUE, YELLOW, GREEN, RED, WHITE, UNDERLINE, BOLD, ENDC, DNANEXUS_LOGO,
                                 DNANEXUS_X, set_colors, set_delimiter, get_delimiter, DELIMITER, fill,
                                 tty_rows, tty_cols)
from dxpy.utils.pretty_print import format_tree, format_table
from dxpy.utils.resolver import (pick, paginate_and_pick, is_hashid, is_data_obj_id, is_container_id, is_job_id,
                                 get_last_pos_of_char, resolve_container_id_or_name, resolve_path,
                                 resolve_existing_path, get_app_from_path, cached_project_names, split_unescaped,
                                 ResolutionError)
from dxpy.utils.completer import (path_completer, DXPathCompleter, DXAppCompleter, LocalCompleter, NoneCompleter,
                                  InstanceTypesCompleter, ListCompleter, MultiCompleter)
from dxpy.utils.describe import (print_data_obj_desc, print_desc, print_ls_desc, get_ls_l_desc, print_ls_l_desc,
                                 get_io_desc, get_find_jobs_string)
from dxpy.cli.parsers import (no_color_arg, delim_arg, env_args, stdout_args, all_arg, json_arg, parser_dataobject_args, parser_single_dataobject_output_args,
                              get_output_flag, process_properties_args, process_dataobject_args, process_single_dataobject_output_args, set_env_from_args)
from dxpy.cli.exec_io import (ExecutableInputs, stage_to_job_refs, format_choices_or_suggestions)

# Loading other variables used for pretty-printing
if "LESS" in os.environ:
    os.environ["LESS"] = os.environ["LESS"] + " -RS"
else:
    os.environ["LESS"] = "-RS"

# This completer is for the command-line in the shell.  It assumes the
# first word is always a subcommand and that if the first word is a
# subcommand with further subcommands, then the second word must be an
# appropriate sub-subcommand.
class DXCLICompleter():
    subcommands = {'find': ['jobs ', 'data ', 'projects ', 'apps '],
                   'new': ['record ', 'gtable ', 'project ']}

    def __init__(self):
        global subparsers
        self.commands = map(lambda subcmd: subcmd + ' ',
                            subparsers.choices.keys())
        self.matches = []
        self.text = None

    def get_command_matches(self, prefix):
        self.matches = filter(lambda command: command.startswith(prefix),
                              self.commands)

    def get_subcommand_matches(self, command, prefix):
        if command in self.subcommands:
            self.matches = map(lambda sub: command + ' ' + sub,
                               filter(lambda subcommand: subcommand.startswith(prefix),
                                      self.subcommands[command]))

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
                                              classes=['applet'])
            elif words[0] in ['cd', 'rmdir', 'mkdir', 'tree']:
                path_matches = path_completer(words[-1],
                                              expected='folder')
            elif words[0] in ['export']:
                path_matches = path_completer(words[-1],
                                              classes=['gtable'])
            elif words[0] in ['head']:
                path_matches = path_completer(words[-1],
                                              classes=['gtable', 'file'])
            elif words[0] in ['cat', 'download']:
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
                self.matches = map(lambda match: text[:space_pos + 1] + match, path_matches)
            else:
                self.matches = path_matches

            # Also find app name matches and append to
            # self.matches, preferably a list of installed apps
            if words[0] in ['run', 'install', 'uninstall']:
                try:
                    app_names = map(lambda result:
                                        result['describe']['name'],
                                    filter(lambda result:
                                               result['describe']['installed'] if words[0] in ['run', 'uninstall'] else not result['describe']['installed'],
                                           list(dxpy.find_apps(describe={"fields": {"name": True, "installed": True}}))))
                    app_matches = filter(lambda app_name:
                                             app_name.startswith(words[-1]),
                                         app_names)
                    if want_prefix:
                        self.matches += map(lambda match:
                                                text[:space_pos + 1] + match,
                                            app_matches)
                    else:
                        self.matches += app_matches
                except:
                    pass

        return self.matches

    def get_argcomplete_matches(self, cline, cpoint, prefix, suffix):
        # Remove the leading "dx " and match only on the point up to the cursor
        return self.get_matches(cline[3:cpoint])
        
    def complete(self, text, state):
        if state == 0 and self.text != text:
            self.get_matches(text, want_prefix=True)

        if state < len(self.matches):
            return self.matches[state]
        else:
            return None

def login(args):
    if not state['interactive']:
        args.save = True

    default_authserver = 'https://auth.dnanexus.com'

    # API server should have already been set up if --host or one of
    # the --special-host flags has been set.
    if args.token is None:
        if args.host is not None or args.port is not None:
            if args.host is None or args.port is None:
                parser.exit(2, fill('Error: Only one of --host and --port were provided; provide either both or neither of the values') + '\n')
            protocol = args.protocol or ("https" if (args.port == 443) else "http")
            authserver = protocol + '://' + args.host
            authserver += ':' + str(args.port)
        else:
            authserver = default_authserver

        print 'Acquiring credentials from ' + authserver

        try:
            username = raw_input('Username: ')
            write_env_var('DX_USERNAME', username)
            password = getpass.getpass()
        except (KeyboardInterrupt, EOFError):
            parser.exit(1, '\n')

        def get_token(**data):
            return dxpy.DXHTTPRequest(authserver+"/authorizations", data, prepend_srv=False, auth=None)
        try:
            token_res = get_token(username=username, password=password,
                                  expires=normalize_time_input(args.timeout, future=True))
        except dxpy.DXAPIError as e:
            if e.name == 'OTPRequiredError':
                otp = raw_input('Verification code: ')
                try:
                    token_res = get_token(username=username, password=password, otp=otp,
                                          expires=normalize_time_input(args.timeout, future=True))
                except:
                    err_exit("Login error", arg_parser=parser)
            elif e.name == 'UsernameOrPasswordError':
                err_exit("Incorrect username and/or password", arg_parser=parser)
            else:
                err_exit("Login error", arg_parser=parser)
        except:
            err_exit("Login error", arg_parser=parser)

        sec_context=json.dumps({'auth_token': token_res["access_token"], 'auth_token_type': token_res["token_type"]})

        if authserver == default_authserver:
            set_api('https', 'api.dnanexus.com', '443', args.save)
    else:
        sec_context = '{"auth_token":"' + args.token + '","auth_token_type":"Bearer"}'
        # Ensure correct API server
        if args.host is None:
            set_api('https', 'api.dnanexus.com', '443', args.save)

    os.environ['DX_SECURITY_CONTEXT'] = sec_context
    dxpy.set_security_context(json.loads(sec_context))
    if args.save:
        write_env_var('DX_SECURITY_CONTEXT', sec_context)

    greeting = dxpy.api.system_greet({'client': 'dxclient', 'version': dxpy.TOOLKIT_VERSION})
    if greeting.get('messages'):
        print BOLD("New messages from ") + DNANEXUS_LOGO()
        for message in greeting['messages']:
            print BOLD("Date:    ") + datetime.datetime.fromtimestamp(message['date']/1000).ctime()
            print BOLD("Subject: ") + fill(message['title'], subsequent_indent=' '*9)
            body = message['body'].splitlines()
            if len(body) > 0:
                print BOLD("Message: ") + body[0]
                for line in body[1:]:
                    print ' '*9 + line

    args.current = False
    args.name = None
    args.level = 'CONTRIBUTE'
    args.public = False

    if args.host is not None and not args.staging and not args.prod and not args.preprod:
        setenv(args)
    elif args.projects:
        pick_and_set_project(args)

def logout(args):
    if dxpy.AUTH_HELPER is not None:
        if args.host is not None or args.port is not None:
            authserver = 'http://' + args.host
            authserver += ':' + str(args.port)
        elif dxpy.APISERVER_HOST == 'stagingapi.dnanexus.com':
            authserver = 'https://stagingauth.dnanexus.com'
        elif dxpy.APISERVER_HOST == 'prodapi.dnanexus.com':
            authserver = 'https://prodauth.dnanexus.com'
        elif dxpy.APISERVER_HOST == 'api.dnanexus.com':
            authserver = 'https://auth.dnanexus.com'
        elif dxpy.APISERVER_HOST == 'preprodapi.dnanexus.com':
            authserver = 'https://preprodauth.dnanexus.com'
        else:
            parser.exit(3, fill("Please specify the authserver host and port to log out from") + "\n")
        print 'Deleting credentials from ' + authserver + '...'
        session = requests.session()
        token = dxpy.AUTH_HELPER.security_context['auth_token']
        try:
            token_sig = hashlib.sha256(token).hexdigest()
            response = session.delete(authserver + '/authorizations/' + token_sig, auth=dxpy.AUTH_HELPER)
            if response.status_code not in (requests.codes.forbidden, requests.codes.not_found):
                response.raise_for_status()
            if response.status_code == requests.codes.ok:
                print 'Deleted token with signature', token_sig
        except:
            err_exit()
        if not state['interactive']:
            write_env_var("DX_SECURITY_CONTEXT", None)
        else:
            dxpy.AUTH_HELPER = None

def set_api(protocol, host, port, write):
    os.environ['DX_APISERVER_PROTOCOL'] = protocol
    os.environ['DX_APISERVER_HOST'] = host
    os.environ['DX_APISERVER_PORT'] = port
    if write:
        write_env_var("DX_APISERVER_PROTOCOL", protocol)
        write_env_var("DX_APISERVER_HOST", host)
        write_env_var("DX_APISERVER_PORT", port)
    dxpy.set_api_server_info(host=host, port=port, protocol=protocol)

def set_project(project, write, name=None):
    if dxpy.JOB_ID is None:
        os.environ['DX_PROJECT_CONTEXT_ID'] = project
        if name is not None:
            os.environ["DX_PROJECT_CONTEXT_NAME"] = name
        if write:
            write_env_var("DX_PROJECT_CONTEXT_ID", project)
            if name is not None:
                write_env_var("DX_PROJECT_CONTEXT_NAME", name)
            else:
                try:
                    os.remove(os.path.expanduser('~/.dnanexus_config/DX_PROJECT_CONTEXT_NAME'))
                except:
                    pass
    else:
        os.environ['DX_WORKSPACE_ID'] = project
        if write:
            write_env_var('DX_WORKSPACE_ID', project)
    dxpy.set_workspace_id(project)

def set_wd(folder, write):
    os.environ['DX_CLI_WD'] = folder
    if write:
        write_env_var("DX_CLI_WD", folder)

# Will raise KeyboardInterrupt, EOFError
def prompt_for_var(prompt_str, env_var_str):
    prompt = prompt_str
    default = None
    if env_var_str in os.environ:
        default = os.environ[env_var_str]
        prompt += ' [' + default + ']: '
    else:
        prompt += ': '
    while True:
        value = raw_input(prompt)
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
                                              public=(args.public if args.public else None))
    except:
        err_exit('Error while listing available projects')
    any_results = False
    first_pass = True
    while True:
        results = []
        for i in range(10):
            try:
                results.append(result_generator.next())
                any_results = True
            except StopIteration:
                break
            except:
                err_exit('Error while listing available projects')
        if not any_results:
            parser.exit(0, '\n' + fill('No projects to choose from.  You can create one with the command "dx new project".  To pick from projects for which you only have VIEW permissions, use \"dx select --level VIEW\" or \"dx select --public\".') + '\n')
        elif len(results) == 0:
            parser.exit(1, 'No projects left to choose from.\n')

        if first_pass:
            if not args.public and args.level == "CONTRIBUTE":
                print ''
                print fill("Note: Use \"dx select --level VIEW\" or \"dx select --public\" to select from projects for which you only have VIEW permissions.")
            first_pass = False

        project_ids = [result['id'] for result in results]

        # Eliminate current default if it is not a found project
        try:
            default = project_ids.index(dxpy.WORKSPACE_ID)
        except:
            default = None

        print ""
        if args.public:
            print "Available public projects:"
        else:
            print "Available projects ({level} or higher):".format(level=args.level)
        choice = try_call(pick,
                          map(lambda result:
                                  result['describe']['name'] + ' (' + result['level'] + ')',
                              results),
                          default,
                          more_choices=(len(results) == 10))
        if choice == 'm':
            continue
        else:
            print 'Setting current project to: ' + results[choice]['describe']['name']
            set_project(project_ids[choice], not state['interactive'] or args.save, name=results[choice]['describe']['name'])
            state['currentproj'] = results[choice]['describe']['name']
            set_wd('/', not state['interactive'] or args.save)
            return

def setenv(args):
    if not state['interactive']:
        args.save = True
    if args.current:
        env_vars = ['DX_SECURITY_CONTEXT', 'DX_APISERVER_HOST', 'DX_APISERVER_PORT', 'DX_PROJECT_CONTEXT_ID', 'DX_CLI_WD', 'DX_USERNAME', 'DX_WORKSPACE_ID']
        for var in env_vars:
            if var in os.environ:
                write_env_var(var, os.environ[var])
    else:
        try:
            api_protocol = prompt_for_var('API server protocol (choose "http" or "https")', 'DX_APISERVER_PROTOCOL')
            api_host = prompt_for_var('API server host', 'DX_APISERVER_HOST')
            api_port = prompt_for_var('API server port', 'DX_APISERVER_PORT')
            set_api(api_protocol, api_host, api_port, args.save)
        except:
            parser.exit(1, '\n')

    if args.projects:
        args.name = None
        args.public = False
        args.current = False
        args.level = 'CONTRIBUTE'
        pick_and_set_project(args)

def env(args):
    if args.bash:
        if dxpy.AUTH_HELPER is not None:
            print "export DX_SECURITY_CONTEXT='" + json.dumps(dxpy.AUTH_HELPER.security_context) + "'"
        if dxpy.APISERVER_PROTOCOL is not None:
            print "export DX_APISERVER_PROTOCOL=" + dxpy.APISERVER_PROTOCOL
        if dxpy.APISERVER_HOST is not None:
            print "export DX_APISERVER_HOST=" + dxpy.APISERVER_HOST
        if dxpy.APISERVER_PORT is not None:
            print "export DX_APISERVER_PORT=" + dxpy.APISERVER_PORT
        if dxpy.WORKSPACE_ID is not None:
            print "export DX_PROJECT_CONTEXT_ID=" + dxpy.WORKSPACE_ID
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
        print flags_str
    else:
        if dxpy.AUTH_HELPER is not None:
            print "Auth token used\t\t" + dxpy.AUTH_HELPER.security_context.get("auth_token", "none")
        print "API server protocol\t" + dxpy.APISERVER_PROTOCOL
        print "API server host\t\t" + dxpy.APISERVER_HOST
        print "API server port\t\t" + dxpy.APISERVER_PORT
        print "Current workspace\t" + str(dxpy.WORKSPACE_ID)
        if "DX_PROJECT_CONTEXT_NAME" in os.environ:
            print "Current workspace name\t\"{n}\"".format(n=os.environ.get("DX_PROJECT_CONTEXT_NAME"))
        print "Current folder\t\t" + str(os.environ.get("DX_CLI_WD"))
        print "Current user\t\t" + str(os.environ.get("DX_USERNAME"))

def get_pwd():
    pwd_str = None
    if dxpy.WORKSPACE_ID is not None:
        if state['currentproj'] is None:
            try:
                proj_name = dxpy.DXHTTPRequest('/' + dxpy.WORKSPACE_ID + '/describe', {})['name']
                state['currentproj'] = proj_name
            except:
                pass
    if state['currentproj'] is not None:
        pwd_str = state['currentproj'] + ':' + os.environ.get('DX_CLI_WD', '/')
    return pwd_str

def pwd(args):
    pwd_str = get_pwd()
    if pwd_str is not None:
        print pwd_str
    else:
        parser.exit(1, 'Current project is not set\n')

def api(args):
    json_input = json.loads(args.input_json)
    if args.input is not None:
        with (sys.stdin if args.input == '-' else open(args.input, 'r')) as fd:
            data = fd.read()
            try:
                json_input = json.loads(data)
            except ValueError:
                parser.exit(1, 'Error: file contents could not be parsed as JSON\n')
    resp = None
    try:
        resp = dxpy.DXHTTPRequest('/' + args.resource + '/' + args.method,
                                  json_input)
    except:
        err_exit()
    try:
        print json.dumps(resp, indent=4)
    except ValueError:
        parser.exit(1, 'Error: server response could not be parsed as JSON\n')

def invite(args):
    # If --project is a valid project (ID or name), then appending ":"
    # should not hurt the path resolution.
    if ':' not in args.project:
        args.project += ':'
    project, none, none = try_call(resolve_existing_path,
                                   args.project, 'project')
    if args.invitee != 'PUBLIC' and not '-' in args.invitee and not '@' in args.invitee:
        args.invitee = 'user-' + args.invitee.lower()
    try:
        resp = dxpy.DXHTTPRequest('/' + project + '/invite',
                                  {"invitee": args.invitee, "level": args.level})
    except:
        err_exit()
    print 'Invited ' + args.invitee + ' to ' + project + ' (' + resp['state'] + ')'

def uninvite(args):
    project, none, none = try_call(resolve_existing_path,
                                   args.project, 'project')
    if args.entity != 'PUBLIC' and not '-' in args.entity:
        args.entity = 'user-' + args.entity.lower()
    try:
        dxpy.DXHTTPRequest('/' + project + '/decreasePermissions',
                           {args.entity: None})
    except:
        err_exit()
    print 'Uninvited ' + args.entity + ' from ' + project

def select(args):
    if args.project is not None:
        if get_last_pos_of_char(':', args.project) != -1:
            args.path = args.project
        else:
            args.path = args.project + ':'
        cd(args)
        print "Selected project", split_unescaped(":", args.project)[0].replace("\\:", ":")
    else:
        pick_and_set_project(args)

def cd(args):
    # entity_result should be None because expected='folder'
    project, folderpath, none = try_call(resolve_existing_path,
                                         args.path, 'folder')

    if project is not None:
        project_name = try_call(dxpy.DXProject(project).describe)['name']

        # It is obvious what the project is
        if project != dxpy.WORKSPACE_ID or 'DX_PROJECT_CONTEXT_NAME' not in os.environ:
            # Cache ID and name if necessary
            set_project(project, not state['interactive'], name=project_name)
            state['currentproj'] = project_name
    else:
        parser.exit(1, 'Error: No current project was given\n')

    # TODO: attempt to add caching later if it's an issue
    # if project in cached_project_paths and folderpath in cached_project_paths[project]:
    #     set_wd(folderpath, not interactive)

    try:
        dxproj = dxpy.get_handler(dxpy.WORKSPACE_ID)
        dxproj.list_folder(folder=folderpath)
    except:
        parser.exit(1, fill(folderpath + ': No such file or directory found in project ' + dxpy.WORKSPACE_ID) + '\n')
        return

    set_wd(folderpath, not state['interactive'])

def cmp_names(x, y):
    return cmp(x['describe']['name'].lower(), y['describe']['name'].lower())

def ls(args):
    project, folderpath, entity_results = try_call(resolve_existing_path, # TODO: this needs to honor "ls -a" (all) (args.obj/args.folders/args.full)
                                                   args.path,
                                                   ask_to_resolve=False)

    if project is None:
        parser.exit(1, fill('Current project must be set or specified before any data can be listed') + '\n')
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
            resp = dxproj.list_folder(folder=folderpath,
                                      describe={},
                                      only=only,
                                      includeHidden=args.all)

            # Listing the folder was successful

            if args.verbose:
                print UNDERLINE() + 'Project:' + ENDC() + ' ' + dxproj.describe()['name'] + ' (' + project + ')'
                print UNDERLINE() + 'Folder :' + ENDC() + ' ' + folderpath

            if not args.obj:
                folders_to_print = ['/.', '/..'] if args.all else []
                folders_to_print += resp['folders']
                for folder in folders_to_print:
                    if args.full:
                        print BOLD() + BLUE() + folder + ENDC()
                    else:
                        print BOLD() + BLUE() + os.path.basename(folder) + '/' + ENDC()
            if not args.folders:
                resp["objects"].sort(cmp=cmp_names)
                if args.verbose:
                    if len(resp['objects']) > 0:
                        print BOLD() + 'State' + DELIMITER('\t') + 'Last modified' + DELIMITER('       ') + 'Size' + DELIMITER('     ') + 'Name' + DELIMITER(' (') + 'ID' + DELIMITER(')') + ENDC()
                    else:
                        print "No data objects found in the folder"
                name_counts = collections.Counter(obj['describe']['name'] for obj in resp['objects'])
                for obj in resp['objects']:
                    if args.brief:
                        print obj['id']
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
                    print result['id']
                elif args.verbose:
                    print_ls_l_desc(result['describe'], include_project=False)
                else:
                    print_ls_desc(result['describe'], print_id=True if name_counts[result['describe']['name']] > 1 else False)

def mkdir(args):
    had_error = False
    for path in args.paths:
        # Resolve the path and add it to the list
        try:
            project, folderpath, none = resolve_path(path, expected='folder')
        except ResolutionError as details:
            print fill('Could not resolve \"' + path + '\": ' + unicode(details))
            had_error = True
            continue
        if project is None:
            print fill('Could not resolve the project of \"' + path + '\"')
        try:
            dxpy.DXHTTPRequest('/' + project + '/newFolder', {"folder": folderpath, "parents": args.parents})
        except BaseException as details:
            print "Error while creating " + folderpath + " in " + project
            print "  " + unicode(details)
            had_error = True
    if had_error:
        parser.exit(1)

def rmdir(args):
    had_error = False
    for path in args.paths:
        try:
            project, folderpath, none = resolve_path(path, expected='folder')
        except ResolutionError as details:
            print fill('Could not resolve \"' + path + '\": ' + unicode(details))
            had_error = True
            continue
        if project is None:
            print fill('Could not resolve the project of \"' + path + '\"')
        try:
            dxpy.DXHTTPRequest('/' + project + '/removeFolder', {"folder": folderpath})
        except BaseException as details:
            print "Error while removing " + folderpath + " in " + project
            print "  " + unicode(details)
            had_error = True
    if had_error:
        parser.exit(1)

def rm(args):
    had_error = False
    projects = {}
    for path in args.paths:
        # Resolve the path and add it to the list
        try:
            project, folderpath, entity_results = resolve_existing_path(path, allow_mult=True, all_mult=args.all)
        except BaseException as details:
            print fill('Could not resolve \"' + path + '\": ' + unicode(details))
            had_error = True
            continue
        if project is None:
            had_error = True
            print fill('Could not resolve \"' + path + '\" to a project')
            continue
        if project not in projects:
            projects[project] = {"folders": [], "objects": []}
        if entity_results is None:
            if folderpath is not None:
                if not args.recursive:
                    print fill(u'Did not find \"' + path + '\" as a data object; if it is a folder, cannot remove it without setting the \"-r\" flag')
                    had_error = True
                    continue
                else:
                    projects[project]['folders'].append(folderpath)
            else:
                print fill('Path ' + path + ' resolved to a project; cannot remove a project using \"rm\"')
                had_error = True
                continue
        else:
            projects[project]['objects'] += map(lambda result: result['id'],
                                                entity_results)

    for project in projects:
        for folder in projects[project]['folders']:
            try:
                dxpy.DXHTTPRequest('/' + project + '/removeFolder',
                                   {"folder": folder,
                                    "recurse": True})
            except BaseException as details:
                print "Error while removing " + folder + " from " + project
                print "  " + unicode(details)
                had_error = True
        try:
            dxpy.DXHTTPRequest('/' + project + '/removeObjects',
                               {"objects": projects[project]['objects']})
        except BaseException as details:
            print "Error while removing " + json.dumps(projects[project]['objects']) + " from " + project
            print "  " + unicode(details)
            had_error = True
    if had_error:
        parser.exit(1)

def rmproject(args):
    had_error = False
    for project in args.projects:
        # Be forgiving if they offer an extraneous colon
        substrings = split_unescaped(':', project)
        if len(substrings) > 1 or (len(substrings) == 1 and project[0] == ':'):
            print fill('Unable to remove \"' + project + '\": a nonempty string was found to the right of an unescaped colon')
            had_error = True
            continue
        if len(substrings) == 0:
            if project[0] == ':':
                print fill('Unable to remove \":\": to remove the current project, use its name or ID')
                had_error = True
                continue
        proj_id = try_call(resolve_container_id_or_name, substrings[0])
        if proj_id is None:
            print fill('Unable to remove \"' + project + '\": could not resolve to a project ID')
            had_error = True
            continue
        try:
            proj_desc = dxpy.DXHTTPRequest('/' + proj_id + '/describe', {})
            if args.confirm:
                value = raw_input(fill('About to delete project \"' + proj_desc['name'] + '\" (' + proj_id + ')') + '\nPlease confirm [y/n]: ')
                if len(value) == 0 or value.lower()[0] != 'y':
                    had_error = True
                    print fill('Aborting deletion of project \"' + proj_desc['name'] + '\"')
                    continue
            try:
                dxpy.DXHTTPRequest('/' + proj_id + '/destroy', {})
            except dxpy.DXAPIError as apierror:
                if apierror.name == 'InvalidState':
                    value = raw_input(fill('WARNING: there are still unfinished jobs in the project.') + '\nTerminate all jobs and delete the project? [y/n]: ')
                    if len(value) == 0 or value.lower()[0] != 'y':
                        had_error = True
                        print fill('Aborting deletion of project \"' + proj_desc['name'] + '\"')
                        continue
                    dxpy.DXHTTPRequest('/' + proj_id + '/destroy', {"terminateJobs": True})
                else:
                    raise apierror
            print fill('Successfully deleted project \"' + proj_desc['name'] + '\"')
        except EOFError:
            print ''
            parser.exit(1)
        except KeyboardInterrupt:
            print ''
            parser.exit(1)
        except BaseException as details:
            print fill('Was unable to remove ' + project + ', ' + unicode(details))
            had_error = True
    if had_error:
        parser.exit(1)

# ONLY for within the SAME project.  Will exit fatally otherwise.
def mv(args):
    dest_proj, dest_path, none = try_call(resolve_path,
                                          args.destination, 'folder')
    try:
        if dest_path is None:
            raise ValueError()
        dx_dest = dxpy.get_handler(dest_proj)
        dx_dest.list_folder(folder=dest_path, only='folders')
    except:
        if dest_path is None:
            parser.exit(1, 'Cannot move to a hash ID\n')
        # Destination folder path is new => renaming
        if len(args.sources) != 1:
            # Can't rename more than one object
            parser.exit(1, 'The destination folder does not exist\n')
        last_slash_pos = get_last_pos_of_char('/', dest_path)
        if last_slash_pos == 0:
            dest_folder = '/'
        else:
            dest_folder = dest_path[:last_slash_pos]
        dest_name = dest_path[last_slash_pos + 1:].replace('\/', '/')
        try:
            dx_dest.list_folder(folder=dest_folder, only='folders')
        except:
            parser.exit(1, 'The destination folder does not exist\n')

        # Either rename the data object or rename the folder
        src_proj, src_path, src_results = try_call(resolve_existing_path,
                                                   args.sources[0],
                                                   allow_mult=True, all_mult=args.all)

        if src_proj != dest_proj:
            parser.exit(1, fill('Error: Using \"mv\" for moving something from one project to another is unsupported.') + '\n')

        if src_results is None:
            if src_path == '/':
                parser.exit(1, fill('Cannot rename root folder; to rename the project, please use the "dx rename" subcommand.') + '\n')
            try:
                dxpy.DXHTTPRequest('/' + src_proj + '/renameFolder',
                                   {"folder": src_path,
                                    "newpath": dest_path})
                return
            except:
                err_exit()
        else:
            try:
                if src_results[0]['describe']['folder'] != dest_folder:
                    dxpy.DXHTTPRequest('/' + src_proj + '/move',
                                       {"objects": map(lambda result:
                                                           result['id'],
                                                       src_results),
                                        "destination": dest_folder})
                for result in src_results:
                    dxpy.DXHTTPRequest('/' + result['id'] + '/rename',
                                       {"project": src_proj,
                                        "name": dest_name})
                return
            except:
                err_exit()

    if len(args.sources) == 0:
        parser.exit(1, 'No sources provided to move\n')
    src_objects = []
    src_folders = []
    for source in args.sources:
        src_proj, src_folderpath, src_results = try_call(resolve_existing_path,
                                                         source,
                                                         allow_mult=True, all_mult=args.all)
        if src_proj != dest_proj:
            parser.exit(1, fill('Using \"mv\" for moving something from one project to another is unsupported.  Please use \"cp\" and \"rm\" instead.') + '\n')

        if src_results is None:
            src_folders.append(src_folderpath)
        else:
            src_objects += map(lambda result: result['id'], src_results)
    try:
        dxpy.DXHTTPRequest('/' + src_proj + '/move',
                           {"objects": src_objects,
                            "folders": src_folders,
                            "destination": dest_path})
    except:
        err_exit()

# ONLY for between DIFFERENT projects.  Will exit fatally otherwise.
def cp(args):
    dest_proj, dest_path, none = try_call(resolve_path,
                                          args.destination, 'folder')
    try:
        if dest_path is None:
            raise ValueError()
        dx_dest = dxpy.get_handler(dest_proj)
        dx_dest.list_folder(folder=dest_path, only='folders')
    except:
        if dest_path is None:
            parser.exit(1, 'Cannot copy to a hash ID\n')
        # Destination folder path is new => renaming
        if len(args.sources) != 1:
            # Can't copy and rename more than one object
            parser.exit(1, 'The destination folder does not exist\n')
        last_slash_pos = get_last_pos_of_char('/', dest_path)
        if last_slash_pos == 0:
            dest_folder = '/'
        else:
            dest_folder = dest_path[:last_slash_pos]
        dest_name = dest_path[last_slash_pos + 1:].replace('\/', '/')
        try:
            dx_dest.list_folder(folder=dest_folder, only='folders')
        except dxpy.DXAPIError as details:
            if details.code == requests.codes.not_found:
                parser.exit(1, 'The destination folder does not exist\n')
            else:
                raise
        except:
            err_exit()

        # Clone and rename either the data object or the folder
        # src_result is None if it could not be resolved to an object
        src_proj, src_path, src_results = try_call(resolve_existing_path,
                                                   args.sources[0],
                                                   allow_mult=True, all_mult=args.all)

        if src_proj == dest_proj:
            if is_hashid(args.sources[0]):
                # This is the only case in which the source project is
                # purely assumed, so give a better error message.
                parser.exit(1, fill('Error: You must specify a source project for ' + args.sources[0]) + '\n')
            else:
                parser.exit(1, fill('A source path and the destination path resolved to the same project or container.  Please specify different source and destination containers, e.g.') + '\n  dx cp source-project:source-id-or-path dest-project:dest-path' + '\n')

        if src_results is None:
            try:
                contents = dxpy.DXHTTPRequest('/' + src_proj + '/listFolder',
                                              {"folder": src_path,
                                               "includeHidden": True})
                dxpy.DXHTTPRequest('/' + dest_proj + '/newFolder',
                                   {"folder": dest_path})
                exists = dxpy.DXHTTPRequest('/' + src_proj + '/clone',
                                          {"folders": contents['folders'],
                                           "objects": map(lambda result: result['id'], contents['objects']),
                                           "project": dest_proj,
                                           "destination": dest_path})['exists']
                if len(exists) > 0:
                    print fill('The following objects already existed in the destination container and were not copied:') + '\n ' + '\n '.join(json.dumps(exists))
                return
            except:
                err_exit()
        else:
            try:
                exists = dxpy.DXHTTPRequest('/' + src_proj + '/clone',
                                            {"objects": map(lambda result: result['id'],
                                                            src_results),
                                             "project": dest_proj,
                                             "destination": dest_folder})['exists']
                if len(exists) > 0:
                    print fill('The following objects already existed in the destination container and were not copied:') + '\n ' + '\n '.join(json.dumps(exists))
                for result in src_results:
                    if result['id'] not in exists:
                        dxpy.DXHTTPRequest('/' + result['id'] + '/rename',
                                           {"project": dest_proj,
                                            "name": dest_name})
                return
            except:
                err_exit()

    if len(args.sources) == 0:
        parser.exit(1, 'No sources provided to copy to another project\n')
    src_objects = []
    src_folders = []
    for source in args.sources:
        src_proj, src_folderpath, src_results = try_call(resolve_existing_path,
                                                         source,
                                                         allow_mult=True, all_mult=args.all)
        if src_proj == dest_proj:
            if is_hashid(source):
                # This is the only case in which the source project is
                # purely assumed, so give a better error message.
                parser.exit(1, fill('Error: You must specify a source project for ' + source) + '\n')
            else:
                parser.exit(1, fill('Error: A source path and the destination path resolved to the same project or container.  Please specify different source and destination containers, e.g.') + '\n  dx cp source-project:source-id-or-path dest-project:dest-path' + '\n')

        if src_proj is None:
            parser.exit(1, fill('Error: A source project must be specified or a current project set in order to clone objects between projects') + '\n')

        if src_results is None:
            src_folders.append(src_folderpath)
        else:
            src_objects += map(lambda result: result['id'], src_results)
    try:
        exists = dxpy.DXHTTPRequest('/' + src_proj + '/clone',
                                    {"objects": src_objects,
                                     "folders": src_folders,
                                     "project": dest_proj,
                                     "destination": dest_path})['exists']
        if len(exists) > 0:
            print fill('The following objects already existed in the destination container and were left alone:') + '\n ' + '\n '.join(exists)
    except:
        err_exit()

def tree(args):
    project, folderpath, none = try_call(resolve_existing_path, args.path,
                                         expected='folder')

    if project is None:
        parser.exit(1, fill('Current project must be set or specified before any data can be listed') + '\n')
    dxproj = dxpy.get_handler(project)

    tree = collections.OrderedDict()
    try:
        folders = filter(lambda folder: folder.startswith((folderpath + '/') if folderpath != '/' else '/'),
                           dxproj.describe(input_params={"folders": True})['folders'])
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
                                                  recurse=True, describe=True),
                           cmp_names):
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
                if item['describe']['class'] == 'applet' or (item['describe']['class'] == 'record' and 'pipeline' in item['describe']['types']):
                    item_desc = BOLD() + GREEN() + item_desc + ENDC()
            subtree[item_desc] = None

        print format_tree(tree, root=(BOLD() + BLUE() + args.path + ENDC()))
    except:
        err_exit()

def describe(args):
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

        # Otherwise, attempt to look for it as a data object.
        try:
            project, folderpath, entity_results = resolve_existing_path(args.path,
                                                                        expected='entity',
                                                                        ask_to_resolve=False,
                                                                        describe=json_input)
        except ResolutionError:
            project, folderpath, entity_results = None, None, None

        found_match = False

        json_output = []

        get_result_str = ResultCounter()

        # Could be a project
        json_input = {}
        json_input['countObjects'] = True
        if args.verbose:
            json_input["permissions"] = True
            json_input['appCaches'] = True
        if entity_results is None:
            if args.path[-1] == ':':
                # It is the project.
                try:
                    desc = dxpy.DXHTTPRequest('/' + project + '/describe',
                                              json_input)
                    found_match = True
                    if args.json:
                        json_output.append(desc)
                    elif args.name:
                        print desc['name']
                    else:
                        print get_result_str()
                        print_desc(desc, args.verbose)
                except dxpy.DXAPIError as details:
                    if details.code != requests.codes.not_found:
                        raise
            elif is_container_id(args.path):
                try:
                    desc = dxpy.DXHTTPRequest('/' + args.path + '/describe',
                                              json_input)
                    found_match = True
                    if args.json:
                        json_output.append(desc)
                    elif args.name:
                        print desc['name']
                    else:
                        print get_result_str()
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
                    print result['describe']['name']
                else:
                    print get_result_str()
                    print_desc(result['describe'], args.verbose or args.details)

        if not is_hashid(args.path) and ':' not in args.path:

            # Could be an app name
            if args.path.startswith('app-'):
                try:
                    desc = dxpy.DXHTTPRequest('/' + args.path + '/describe', {})
                    if args.json:
                        json_output.append(desc)
                    elif args.name:
                        print desc['name']
                    else:
                        print get_result_str()
                        print_desc(desc, args.verbose)
                    found_match = True
                except dxpy.DXAPIError as details:
                    if details.code != requests.codes.not_found:
                        raise
            else:
                for result in dxpy.find_apps(name=args.path, describe=True):
                    if args.json:
                        json_output.append(result['describe'])
                    elif args.name:
                        print result['describe']['name']
                    else:
                        print get_result_str()
                        print_desc(result['describe'], args.verbose)
                    found_match = True

            # Could be a user
            if args.path.startswith('user-'):
                try:
                    desc = dxpy.DXHTTPRequest('/' + args.path + '/describe', {"appsInstalled": True, "subscriptions": True})
                    found_match = True
                    if args.json:
                        json_output.append(desc)
                    elif args.name:
                        print unicode(desc['first']) + ' ' + unicode(desc['last'])
                    else:
                        print get_result_str()
                        print_desc(desc, args.verbose)
                except dxpy.DXAPIError as details:
                    if details.code != requests.codes.not_found:
                        raise
            elif re.match("^[A-Za-z][0-9A-Za-z_\.]{2,}$", args.path):
                # Try describing as a user if it's a valid handle (no
                # hyphens, etc.)
                try:
                    desc = dxpy.DXHTTPRequest('/user-' + args.path.lower() + '/describe',
                                              {"appsInstalled": True,
                                               "subscriptions": True})
                    found_match = True
                    if args.json:
                        json_output.append(desc)
                    elif args.name:
                        print unicode(desc['first']) + ' ' + unicode(desc['last'])
                    else:
                        print get_result_str()
                        print_desc(desc, args.verbose)
                except dxpy.DXAPIError as details:
                    if details.code != requests.codes.not_found:
                        raise

            # Could be an org or team
            if args.path.startswith('org-') or args.path.startswith('team-'):
                try:
                    desc = dxpy.DXHTTPRequest('/' + args.path + '/describe', {})
                    found_match = True
                    if args.json:
                        json_output.append(desc)
                    elif args.name:
                        print desc['id']
                    else:
                        print get_result_str()
                        print_desc(desc, args.verbose)
                except dxpy.DXAPIError as details:
                    if details.code != requests.codes.not_found:
                        raise

        if args.json:
            if args.multi:
                print json.dumps(json_output, indent=4)
            elif len(json_output) > 1:
                raise DXCLIError('More than one match found for ' + args.path + '; to get all of them in JSON format, also provide the --multi flag.')
            elif len(json_output) == 0:
                raise DXCLIError('No match found for ' + args.path)
            else:
                print json.dumps(json_output[0], indent=4)
        elif not found_match:
            raise DXCLIError("No matches found for " + args.path)
    except:
        err_exit()

def new_project(args):
    if args.name == None:
        if sys.stdin.isatty():
            args.name = raw_input("Enter name for new project: ")
        else:
            parser_new_project.print_help()
            parser.exit(1, fill("No project name supplied, and input is not interactive") + '\n')

    get_output_flag(args)
    try:
        resp = dxpy.DXHTTPRequest('/project/new',
                                  {"name": args.name})
        if args.brief:
            print resp['id']
        else:
            print fill('Created new project called \"' + args.name + '\" (' + resp['id'] + ')')
        if args.select:
            set_project(resp['id'], write=True, name=args.name)
    except:
        err_exit()

def new_record(args):
    get_output_flag(args)
    try_call(process_dataobject_args, args)
    try_call(process_single_dataobject_output_args, args)
    init_from = None
    if args.init is not None:
        init_project, init_folder, init_result = try_call(resolve_existing_path,
                                                          args.init,
                                                          expected='entity')
        init_from = dxpy.DXRecord(dxid=init_result['id'], project=init_project)
    if args.output is None:
        project = dxpy.WORKSPACE_ID
        folder = os.environ.get('DX_CLI_WD', '/')
        name = None
    else:
        project, folder, name = resolve_path(args.output)

    dxrecord = None
    try:
        dxrecord = dxpy.new_dxrecord(project=project, name=name,
                                     tags=args.tags, types=args.types,
                                     hidden=args.hidden, properties=args.properties,
                                     details=args.details,
                                     folder=folder,
                                     parents=args.parents, init_from=init_from)
        if args.brief:
            print dxrecord.get_id()
        else:
            print_desc(dxrecord.describe(incl_properties=True, incl_details=True), args.verbose)
    except:
        err_exit()

def new_gtable(args):
    get_output_flag(args)
    try_call(process_dataobject_args, args)
    try_call(process_single_dataobject_output_args, args)

    if args.output is None:
        project = dxpy.WORKSPACE_ID
        folder = os.environ.get('DX_CLI_WD', '/')
        name = None
    else:
        project, folder, name = resolve_path(args.output)

    args.columns = split_unescaped(',', args.columns)
    for i in range(len(args.columns)):
        if ':' in args.columns[i]:
            try:
                col_name, col_type = args.columns[i].split(':')
            except ValueError:
                parser.exit(1, 'Too many colons found in column spec ' + args.columns[i] + '\n')
            if col_type.startswith('bool'):
                col_type = 'boolean'
        else:
            col_name = args.columns[i]
            col_type = 'string'
        args.columns[i] = {'name': col_name, 'type': col_type}
    args.indices = [] if args.indices is None else json.loads(args.indices)
    if args.gri is not None:
        args.indices.append(dxpy.DXGTable.genomic_range_index(args.gri[0], args.gri[1], args.gri[2]))
        args.types = ['gri'] if args.types is None else args.types + ['gri']

    try:
        dxgtable = dxpy.new_dxgtable(project=project, name=name,
                                     tags=args.tags, types=args.types,
                                     hidden=args.hidden, properties=args.properties,
                                     details=args.details,
                                     folder=folder,
                                     parents=args.parents,
                                     columns=args.columns,
                                     indices=args.indices)
        if args.brief:
            print dxgtable.get_id()
        else:
            print_desc(dxgtable.describe(incl_properties=True, incl_details=True))
    except:
        err_exit();

def set_visibility(args):
    had_error = False
    # Attempt to resolve name
    project, folderpath, entity_results = try_call(resolve_existing_path,
                                                   args.path,
                                                   expected='entity',
                                                   allow_mult=True, all_mult=args.all)

    if entity_results is None:
        parser.exit(1, fill('Could not resolve \"' + args.path + '\" to a name or ID') + '\n')

    for result in entity_results:
        try:
            dxpy.DXHTTPRequest('/' + result['id'] + '/setVisibility',
                               {"hidden": (args.visibility == 'hidden')})
        except dxpy.DXAPIError as details:
            print fill(unicode(details))
            had_error = True
        except (requests.ConnectionError, requests.HTTPError, requests.Timeout, httplib.HTTPException) as details:
            print fill(details.__class__.__name__ + ': ' + unicode(details))
            had_error = True

    if had_error:
        parser.exit(1)

def get_details(args):
    # Attempt to resolve name
    project, folderpath, entity_result = try_call(resolve_existing_path,
                                                  args.path, expected='entity')

    if entity_result is None:
        parser.exit(1, fill('Could not resolve \"' + args.path + '\" to a name or ID') + '\n')

    try:
        print json.dumps(dxpy.DXHTTPRequest('/' + entity_result['id'] + '/getDetails', {}), indent=4)
    except:
        err_exit()

def set_details(args):
    had_error = False
    # Attempt to resolve name
    project, folderpath, entity_results = try_call(resolve_existing_path,
                                                   args.path, expected='entity',
                                                   allow_mult=True, all_mult=args.all)

    if entity_results is None:
        parser.exit(1, fill('Could not resolve \"' + args.path + '\" to a name or ID') + '\n')

    try:
        args.details = json.loads(args.details)
    except ValueError:
        parser.exit(1, 'Error: details could not be parsed as JSON')

    for result in entity_results:
        try:
            dxpy.DXHTTPRequest('/' + result['id'] + '/setDetails',
                               args.details)
        except dxpy.DXAPIError as details:
            print fill(unicode(details))
            had_error = True
        except (requests.ConnectionError, requests.HTTPError, requests.Timeout, httplib.HTTPException) as details:
            print fill(details.__class__.__name__ + ': ' + unicode(details))
            had_error = True
    if had_error:
        parser.exit(1)

def add_types(args):
    had_error = False
    # Attempt to resolve name
    project, folderpath, entity_results = try_call(resolve_existing_path,
                                                   args.path,
                                                   expected='entity',
                                                   allow_mult=True, all_mult=args.all)

    if entity_results is None:
        parser.exit(1, fill('Could not resolve \"' + args.path + '\" to a name or ID') + '\n')

    for result in entity_results:
        try:
            dxpy.DXHTTPRequest('/' + result['id'] + '/addTypes',
                               {"types": args.types})
        except dxpy.DXAPIError as details:
            print fill(unicode(details))
            had_error = True
        except (requests.ConnectionError, requests.HTTPError, requests.Timeout, httplib.HTTPException) as details:
            print fill(details.__class__.__name__ + ': ' + unicode(details))
            had_error = True
    if had_error:
        parser.exit(1)

def remove_types(args):
    had_error = False
    # Attempt to resolve name
    project, folderpath, entity_results = try_call(resolve_existing_path,
                                                   args.path,
                                                   expected='entity',
                                                   allow_mult=True, all_mult=args.all)

    if entity_results is None:
        parser.exit(1, fill('Could not resolve \"' + args.path + '\" to a name or ID') + '\n')

    for result in entity_results:
        try:
            dxpy.DXHTTPRequest('/' + result['id'] + '/removeTypes',
                               {"types": args.types})
        except dxpy.DXAPIError as details:
            print fill(unicode(details))
            had_error = True
        except (requests.ConnectionError, requests.HTTPError, requests.Timeout, httplib.HTTPException) as details:
            print fill(details.__class__.__name__ + ': ' + unicode(details))
            had_error = True
    if had_error:
        parser.exit(1)

def add_tags(args):
    had_error = False
    # Attempt to resolve name
    project, folderpath, entity_results = try_call(resolve_existing_path,
                                                   args.path,
                                                   expected='entity',
                                                   allow_mult=True, all_mult=args.all)

    if entity_results is None:
        parser.exit(1, fill('Could not resolve \"' + args.path + '\" to a name or ID') + '\n')

    for result in entity_results:
        try:
            dxpy.DXHTTPRequest('/' + result['id'] + '/addTags',
                               {"project": project,
                                "tags": args.tags})
        except dxpy.DXAPIError as details:
            print fill(unicode(details))
            had_error = True
        except (requests.ConnectionError, requests.HTTPError, requests.Timeout, httplib.HTTPException) as details:
            print fill(details.__class__.__name__ + ': ' + unicode(details))
            had_error = True
    if had_error:
        parser.exit(1)

def remove_tags(args):
    had_error = False
    # Attempt to resolve name
    project, folderpath, entity_results = try_call(resolve_existing_path,
                                                   args.path,
                                                   expected='entity',
                                                   allow_mult=True, all_mult=args.all)

    if entity_results is None:
        parser.exit(1, 'Could not resolve \"' + args.path + '\" to a name or ID\n')

    for result in entity_results:
        try:
            dxpy.DXHTTPRequest('/' + result['id'] + '/removeTags',
                               {"project": project,
                                "tags": args.tags})
        except dxpy.DXAPIError as details:
            print fill(unicode(details))
            had_error = True
        except (requests.ConnectionError, requests.HTTPError, requests.Timeout, httplib.HTTPException) as details:
            print fill(details.__class__.__name__ + ': ' + unicode(details))
            had_error = True
    if had_error:
        parser.exit(1)

def rename(args):
    had_error = False
    # Attempt to resolve name
    project, folderpath, entity_results = try_call(resolve_existing_path,
                                                   args.path,
                                                   expected='entity',
                                                   allow_mult=True, all_mult=args.all)

    if entity_results is None and not is_container_id(args.path):
        if project is None:
            parser.exit(1, 'Could not resolve \"' + args.path + '\" to a name or ID\n')
        elif folderpath != None and folderpath != '/':
            parser.exit(1,
                        'Could not resolve \"' + args.path + \
                            '''\" to an existing data object or folder; if you
were attempting to refer to a project, please append a colon ":" to indicate that it
is a project.\n''')

    if entity_results is not None:
        for result in entity_results:
            try:
                dxpy.DXHTTPRequest('/' + result['id'] + '/rename',
                                   {"project": project,
                                    "name": args.name})
            except dxpy.DXAPIError as details:
                print fill(unicode(details))
                had_error = True
            except (requests.ConnectionError, requests.HTTPError, requests.Timeout, httplib.HTTPException) as details:
                print fill(details.__class__.__name__ + ': ' + unicode(details))
                had_error = True
        if had_error:
            parser.exit(1)
    elif not project.startswith('project-'):
        parser.exit(1, 'Cannot rename a non-project data container\n')
    else:
        try:
            dxpy.DXHTTPRequest('/' + project + '/update',
                               {"name": args.name})
        except:
            err_exit()

def set_properties(args):
    had_error = False
    # Attempt to resolve name
    project, folderpath, entity_results = try_call(resolve_existing_path,
                                                   args.path,
                                                   expected='entity',
                                                   allow_mult=True, all_mult=args.all)

    if entity_results is None and project is None:
        parser.exit(1, 'Could not resolve \"' + args.path + '\" to a name or ID\n')

    try_call(process_properties_args, args)
    if entity_results is not None:
        for result in entity_results:
            try:
                dxpy.DXHTTPRequest('/' + result['id'] + '/setProperties',
                                   {"project": project,
                                    "properties": args.properties})
            except dxpy.DXAPIError as details:
                print fill(unicode(details))
                had_error = True
            except (requests.ConnectionError, requests.HTTPError, requests.Timeout, httplib.HTTPException) as details:
                print fill(details.__class__.__name__ + ': ' + unicode(details))
                had_error = True
        if had_error:
            parser.exit(1)
    elif not project.startswith('project-'):
        parser.exit(1, 'Cannot set properties on a non-project data container\n')
    else:
        try:
            dxpy.DXHTTPRequest('/' + project + '/setProperties',
                               {"properties": args.properties})
        except:
            err_exit()

def unset_properties(args):
    had_error = False
    # Attempt to resolve name
    project, folderpath, entity_results = try_call(resolve_existing_path,
                                                   args.path,
                                                   expected='entity',
                                                   allow_mult=True, all_mult=args.all)

    if entity_results is None and project is None:
        parser.exit(1, 'Could not resolve \"' + args.path + '\" to a name or ID\n')

    properties = {}
    for prop in args.properties:
        properties[prop] = None
    if entity_results is not None:
        for result in entity_results:
            try:
                dxpy.DXHTTPRequest('/' + result['id'] + '/setProperties',
                                   {"project": project,
                                    "properties": properties})
            except dxpy.DXAPIError as details:
                print fill(unicode(details))
                had_error = True
            except (requests.ConnectionError, requests.HTTPError, requests.Timeout, httplib.HTTPException) as details:
                print fill(details.__class__.__name__ + ': ' + unicode(details))
                had_error = True
        if had_error:
            parser.exit(1)
    elif not project.startswith('project-'):
        parser.exit(1, 'Cannot unset properties on a non-project data container\n')
    else:
        try:
            dxpy.DXHTTPRequest('/' + project + '/setProperties',
                               {"properties": properties})
        except:
            err_exit()

def make_download_url(args):
    project, folderpath, entity_result = try_call(resolve_existing_path, args.path, expected='entity')
    if entity_result is None:
        parser.exit(1, fill('Could not resolve ' + args.path + ' to a data object') + '\n')

    if entity_result['describe']['class'] != 'file':
        parser.exit(1, fill('Error: dx download is only for downloading file objects') + '\n')

    try:
        dxfile = dxpy.DXFile(entity_result['id'], project=project)
        url, headers = dxfile.get_download_url(preauthenticated=True,
                                               duration=normalize_timedelta(args.duration)/1000 if args.duration else 24*3600,
                                               filename=args.filename,
                                               project=project)
        print url
    except:
        err_exit()

def download(args, **kwargs):
    paths = copy.copy(args.path)
    for path in paths:
        args.path = path
        download_one(args, **kwargs)

def download_one(args, already_parsed=False, project=None, folderpath=None, entity_result=None):
    if args.output == '-':
        cat(parser.parse_args(['cat', args.path]))
        return

    def download_one_file(project, id, dest_filename):
        if not args.overwrite and os.path.exists(dest_filename):
            parser.exit(1, fill('Error: path \"' + dest_filename + '\" already exists but -f/--overwrite was not set') + '\n')
        try:
            show_progress = args.show_progress
        except AttributeError:
            show_progress = False
        try:
            dxpy.download_dxfile(id, dest_filename, show_progress=show_progress,
                                 project=project)
        except:
            err_exit()

    if not already_parsed:
        # Attempt to resolve name
        project, folderpath, entity_result = try_call(resolve_existing_path, args.path, allow_empty_string=False)

    if entity_result is None:
        folders = dxpy.describe(project, input_params={'folders': True})['folders']
        if folderpath not in folders:
            parser.exit(1, fill('Error: {path} is neither a file nor a folder name'.format(path=args.path)) + '\n')

        if not args.recursive:
            parser.exit("Error: {path} is a folder but the -r/--recursive option was not given".format(path=args.path))

        destdir = args.output if args.output is not None else os.getcwd()

        for folder in folders:
            if not folder.startswith(folderpath):
                continue
            dir_path = destdir
            for part in folder.split('/'):
                if part == '':
                    continue
                dir_path = os.path.join(dir_path, part)
                if not os.path.exists(dir_path):
                    os.mkdir(dir_path)

        # TODO: control visibility=hidden
        for f in dxpy.search.find_data_objects(classname='file', state='closed', project=project, folder=folderpath,
                                               recurse=True, describe=True):
            file_desc = f['describe']
            dest_filename = os.path.join(destdir, file_desc['folder'].lstrip('/'), file_desc['name'])
            download_one_file(project, file_desc['id'], dest_filename)
    else:
        if entity_result['describe']['class'] != 'file':
            parser.exit(1, fill('Error: {path} is neither a file nor a folder name'.format(path=args.path)) + '\n')

        filename = args.output
        if filename is None:
            filename = entity_result['describe']['name'].replace('/', '%2F')
        elif os.path.isdir(filename):
            filename += entity_result['describe']['name'].replace('/', '%2F')

        download_one_file(project, entity_result['id'], filename)

def get(args):
    # Attempt to resolve name
    project, folderpath, entity_result = try_call(resolve_existing_path,
                                                  args.path, expected='entity')

    if entity_result is None:
        parser.exit(1, fill('Could not resolve ' + args.path + ' to a data object') + '\n')

    if entity_result['describe']['class'] == 'file':
        download_one(args, True, project, folderpath, entity_result)
        return

    if entity_result['describe']['class'] not in ['record', 'applet']:
        parser.exit(1, 'Error: The given object is of class ' + entity_result['describe']['class'] + ' but an object of class record or applet was expected\n')

    if args.output == '-':
        fd = sys.stdout
    else:
        filename = args.output
        if filename is None:
            filename = entity_result['describe']['name'].replace('/', '%2F')
        if args.output is None and not args.no_ext:
            if entity_result['describe']['class'] == 'record':
                filename += '.json'
            elif entity_result['describe']['class'] == 'applet':
                if entity_result['describe']['runSpec']['interpreter'] == 'python2.7':
                    filename += '.py'
                elif entity_result['describe']['runSpec']['interpreter'] == 'v8cgi':
                    filename += '.js'
                elif entity_result['describe']['runSpec']['interpreter'] == 'bash':
                    filename += '.sh'
            if not args.overwrite and os.path.exists(filename):
                parser.exit(1, fill('Error: path \"' + filename + '\" already exists but -f/--overwrite was not set') + '\n')
            try:
                fd = open(filename, 'w')
            except:
                err_exit('Error opening destination file ' + filename)

    if entity_result['describe']['class'] == 'record':
        try:
            details = dxpy.DXHTTPRequest('/' + entity_result['id'] + '/getDetails',
                                         {})
        except:
            err_exit()
        fd.write(json.dumps(details, indent=4))
    elif entity_result['describe']['class'] == 'applet':
        dxapplet = dxpy.DXApplet(entity_result['id'], project=project)
        try:
            resp = dxapplet.get()
            run_spec = resp['runSpec']
        except:
            err_exit()
        fd.write(run_spec['code'])
    if args.output != '-':
        fd.close()

def cat(args):
    for path in args.path:
        project, folderpath, entity_result = try_call(resolve_existing_path, path)

        if entity_result is None:
            parser.exit(1, fill('Could not resolve ' + path + ' to a data object') + '\n')

        if entity_result['describe']['class'] != 'file':
            parser.exit(1, fill('Error: expected a file object') + '\n')

        try:
            dxfile = dxpy.DXFile(entity_result['id'], project=project)
            while True:
                chunk = dxfile.read(1024*1024)
                if len(chunk) == 0:
                    break
                sys.stdout.write(chunk)
        except:
            err_exit()

def head(args):
    # Attempt to resolve name
    project, folderpath, entity_result = try_call(resolve_existing_path,
                                                  args.path, expected='entity')
    if entity_result is None:
        parser.exit(1, fill('Could not resolve ' + args.path + ' to a data object') + '\n')
    if not entity_result['describe']['class'] in ['gtable', 'file']:
        parser.exit(1, 'Error: The given object is of class ' + entity_result['describe']['class'] + ' but an object of class gtable or file was expected\n')

    handler = dxpy.get_handler(entity_result['id'], project=project)

    counter = 0
    if args.lines > 0:
        try:
            if handler._class == 'file':
                handler._read_bufsize = 1024*32;
                for line in handler:
                    print line
                    counter += 1
                    if counter == args.lines:
                        break
            else:
                if args.gri is not None:
                    try:
                        lo = int(args.gri[1])
                        hi = int(args.gri[2])
                    except:
                        parser.exit(1, fill('Error: the LO and HI arguments to --gri must be integers') + '\n')
                    gri_query = dxpy.DXGTable.genomic_range_query(args.gri[0],
                                                                  lo,
                                                                  hi,
                                                                  args.gri_mode,
                                                                  args.gri_name)
                    table_text, table_rows, table_cols = format_table(list(handler.iterate_query_rows(query=gri_query, limit=args.lines)),
                                                                  column_specs = entity_result['describe']['columns'],
                                                                  report_dimensions=True,
                                                                  max_col_width=args.max_col_width)
                else:
                    table_text, table_rows, table_cols = format_table(list(handler.iterate_rows(start=args.starting,
                                                                                                end=args.starting + args.lines)),
                                                                      column_specs = entity_result['describe']['columns'],
                                                                      report_dimensions=True,
                                                                      max_col_width=args.max_col_width)
                    more_rows = entity_result['describe']['length'] - args.starting - args.lines
                    if more_rows > 0:
                        table_text += "\n{nrows} more rows".format(nrows=more_rows)
                if sys.stdout.isatty():
                    if tty_rows <= table_rows or tty_cols <= table_cols:
                        try:
                            pipe = os.popen('less -RS', 'w')
                            pipe.write(table_text.encode('utf-8'))
                            pipe.close()
                            return
                        except:
                            pass
                sys.stdout.write(table_text + '\n')
        except StopIteration:
            pass
        except:
            err_exit()

def upload(args, **kwargs):
    if args.output is not None and args.path is not None:
        raise DXParserError('Error: Cannot provide both the -o/--output and --path/--destination arguments')
    elif args.path is None:
        args.path = args.output

    paths = copy.copy(args.filename)
    for path in paths:
        args.filename = path
        upload_one(args, **kwargs)

upload_seen_paths=set()
def upload_one(args):
    try_call(process_dataobject_args, args)

    args.show_progress = args.show_progress and not args.brief

    if args.path is None:
        project = dxpy.WORKSPACE_ID
        folder = os.environ.get('DX_CLI_WD', '/')
        name = None if args.filename == '-' else os.path.basename(args.filename)
    else:
        project, folder, name = resolve_path(args.path)
        if name is None and args.filename != '-':
            name = os.path.basename(args.filename)

    if os.path.isdir(args.filename):
        if not args.recursive:
            parser.exit("Error: {f} is a directory but the -r/--recursive option was not given".format(f=args.filename))
        norm_path = os.path.realpath(args.filename)
        if norm_path in upload_seen_paths:
            print >>sys.stderr, "Skipping {f}: directory loop".format(f=args.filename)
            return
        else:
            upload_seen_paths.add(norm_path)

        dir_listing = os.listdir(args.filename)
        if len(dir_listing) == 0: # Create empty folder
            dxpy.api.project_new_folder(project, {"folder": os.path.join(folder, os.path.basename(args.filename)), "parents": True})
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
                                            file=(sys.stdin if args.filename == '-' else None),
                                            name=name,
                                            tags=args.tags,
                                            types=args.types,
                                            hidden=args.hidden,
                                            project=project,
                                            properties=args.properties,
                                            details=args.details,
                                            folder=folder,
                                            parents=args.parents,
                                            show_progress=args.show_progress)
            if args.wait:
                dxfile._wait_on_close()
            if args.brief:
                print dxfile.get_id()
            elif not args.mute:
                print_desc(dxfile.describe(incl_properties=True, incl_details=True))
        except:
            err_exit()

def import_csv(args):
    sys.argv = [sys.argv[0] + ' import csv'] + args.importer_args
    from dxpy.scripts import dx_csv_to_gtable
    dx_csv_to_gtable.main()

def import_tsv(args):
    sys.argv = [sys.argv[0] + ' import tsv'] + args.importer_args
    from dxpy.scripts import dx_tsv_to_gtable
    dx_tsv_to_gtable.main()

importers = {
    "tsv": import_tsv,
    "csv": import_csv
}

def dximport(args):
    if args.format.lower() not in importers:
        parser.exit(1, fill('Unsupported format: \"' + args.format + '\".  For a list of supported formats, run "dx help import"') + '\n')
    importers[args.format.lower()](args)

def export_fastq(args):
    sys.argv = [sys.argv[0] + ' export fastq'] + args.exporter_args
    from dxpy.scripts import dx_reads_to_fastq
    dx_reads_to_fastq.main()

def export_sam(args):
    sys.argv = [sys.argv[0] + ' export sam'] + args.exporter_args
    from dxpy.scripts import dx_mappings_to_sam
    dx_mappings_to_sam.main()

def export_csv(args):
    sys.argv = [sys.argv[0] + ' export csv'] + args.exporter_args
    from dxpy.scripts import dx_gtable_to_csv
    dx_gtable_to_csv.main()

def export_tsv(args):
    sys.argv = [sys.argv[0] + ' export tsv'] + args.exporter_args
    from dxpy.scripts import dx_gtable_to_tsv
    dx_gtable_to_tsv.main()

def export_vcf(args):
    sys.argv = [sys.argv[0] + ' export vcf'] + args.exporter_args
    from dxpy.scripts import dx_variants_to_vcf
    dx_variants_to_vcf.main()

exporters = {
    "tsv": export_tsv,
    "csv": export_csv,
    "fastq": export_fastq,
    "sam": export_sam,
    "vcf": export_vcf,
}

def export(args):
    if args.format.lower() not in exporters:
        parser.exit(1, fill('Unsupported format: \"' + args.format + '\".  For a list of supported formats, run "dx help export"') + '\n')
    exporters[args.format.lower()](args)

def find_jobs(args):
    get_output_flag(args)
    if not args.origin_jobs and not args.all_jobs:
        args.trees = True
    if args.origin_jobs and args.parent is not None and args.parent != 'none':
        return
    project = dxpy.WORKSPACE_ID
    origin = None
    more_results = False
    include_io = (args.verbose and args.json) or args.show_outputs
    id_desc = None
    need_to_requery = args.trees and (
        args.state is not None or
        args.name is not None or
        args.created_after is not None or
        args.created_before is not None)

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
            project, none, none = try_call(resolve_existing_path,
                                           args.project, 'project')
        if args.user is not None and args.user != 'self' and not args.user.startswith('user-'):
            args.user = 'user-' + args.user.lower()
        if args.all_projects:
            project = None
    print_launched_by = (args.user is None) or args.verbose
    query = {'launched_by': args.user,
             'executable': args.executable,
             'project': project,
             'state': args.state,
             'origin_job': origin,
             'parent_job': "none" if args.origin_jobs else args.parent,
             'describe': {"io": include_io and not need_to_requery},
             'created_after': args.created_after,
             'created_before': args.created_before,
             'name': args.name,
             'name_mode': 'glob'}
    json_output = []                        # for args.json
    output_ids = []                         # for args.brief

    try:
        if args.all_jobs:
            if args.id:
                if args.json:
                    json_output.append(id_desc)
                elif args.brief:
                    output_ids.append(args.id)
                else:
                    print format_tree({}, get_find_jobs_string(id_desc, has_children=False, single_result=True, show_outputs=args.show_outputs))
            else:   
                i = 0
                for job_result in dxpy.find_jobs(**query):
                    i += 1
                    if (i > args.num_results):
                        more_results = True
                        break
                    if args.json:
                        json_output.append(job_result['describe'])
                    elif args.brief:
                        output_ids.append(job_result['id'])
                    else:
                        print format_tree({}, get_find_jobs_string(job_result['describe'], has_children=False, single_result=True, show_outputs=args.show_outputs))
        else:
            origin_jobs = []  # List of origin job IDs (ordered by search results)
            job_descs = {}    # job ID -> job desc
            job_children = collections.OrderedDict() # job ID -> list of child job IDs

            for job_result in dxpy.find_jobs(**query):
                if args.origin_jobs:
                    # Guaranteed that all results are origin jobs already
                    if len(origin_jobs) == args.num_results:
                        more_results = True
                        break
                    job_descs[job_result['id']] = job_result['describe']
                    origin_jobs.append(job_result['id'])

                if args.trees:
                    if not need_to_requery:
                        parent = job_result['describe']['parentJob']
                        if parent is None and len(origin_jobs) == args.num_results:
                            # Found N + 1 origin jobs
                            more_results = True
                            break
                        elif parent is None:
                            # Found <=N origin jobs
                            origin_jobs.append(job_result['id'])

                        if len(origin_jobs) != args.num_results or job_result['describe']['originJob'] in origin_jobs:
                            # Cache it if we might need it (in one of
                            # the N trees, or haven't found all N
                            # origin jobs yet)
                            job_descs[job_result['id']] = job_result['describe']

                            if parent is not None:
                                if parent in job_children:
                                    job_children[parent].append(job_result['id'])
                                else:
                                    job_children[parent] = [job_result['id']]
                    else:
                        # Will need to requery to get the whole tree,
                        # so don't bother caching anything.

                        # NOTE: these MIGHT NOT be ordered in order of
                        # creation of origin jobs
                        origin_job = job_result['describe']['originJob']
                        if origin_job not in origin_jobs:
                            if len(origin_jobs) == args.num_results:
                                more_results = True
                                break
                            origin_jobs.append(origin_job)

            if args.origin_jobs:
                output_ids = origin_jobs # for args.brief output
                for origin_job in origin_jobs:
                    if args.json:
                        json_output.append(job_descs[origin_job])
                    elif not args.brief:
                        print format_tree({}, get_find_jobs_string(job_descs[origin_job], has_children=False, show_outputs=args.show_outputs))
            else: # args.trees

                def process_children(parent_job, parent_hash=None):
                    has_children = parent_job in job_children
                    if args.json:
                        json_output.append(job_descs[parent_job])
                    elif args.brief:
                        output_ids.append(parent_job)
                    else:
                        parent_string = get_find_jobs_string(job_descs[parent_job],
                                                             has_children=has_children,
                                                             show_outputs=args.show_outputs)
                        parent_hash[parent_string] = collections.OrderedDict()
                    if has_children:
                        for child_job in job_children[parent_job]:
                            process_children(child_job,
                                             parent_hash[parent_string] if parent_hash is not None else None)

                def print_keys(hash_of_keys):
                    for key in hash_of_keys:
                        print key
                        print_keys(hash_of_keys[key])

                if need_to_requery:
                    # TODO: Following bit attempts to use a threadpool
                    # to parallelize the find jobs requests, but it
                    # does not seem to be any faster.
                    #
                    # def iterate_sub_find_jobs_requests(origin_jobs):
                    #     for origin_job in origin_jobs:
                    #         yield dxpy.find_jobs, [], {'origin_job': origin_job, 'describe': {'io': include_io}}
                    #
                    # sub_find_jobs_responses = dxpy.utils.response_iterator(
                    #     request_iterator=iterate_sub_find_jobs_requests(origin_jobs),
                    #     worker_pool=dxpy.utils.get_futures_threadpool(max_workers=8),
                    #     max_active_tasks=12)
                    #
                    # for find_jobs_response in sub_find_jobs_responses:
                    #     for subjob in find_jobs_response:

                    # Get all the descs

                    for origin_job in origin_jobs:
                        for subjob in dxpy.find_jobs(origin_job=origin_job, describe={'io': include_io}):
                            job_descs[subjob['id']] = subjob['describe']

                            parent = subjob['describe']['parentJob']
                            if parent is not None:
                                if parent in job_children:
                                    job_children[parent].append(subjob['id'])
                                else:
                                    job_children[parent] = [subjob['id']]
                    # Re-sort origin_jobs by their created time
                    origin_jobs.sort(key=lambda jobid: -job_descs[jobid]['created'])

                for origin_job in origin_jobs:
                    job_strings = collections.OrderedDict() # for summary or verbose
                    process_children(origin_job, job_strings if not args.json and not args.brief else None)

                    if not args.json and not args.brief:
                        if get_delimiter() is None:
                            for origin_job in job_strings:
                                print format_tree(job_strings[origin_job], root=origin_job)
                        else:
                            print_keys(job_strings)

        if args.json:
            print json.dumps(json_output, indent=4)
        elif args.brief:
            print "\n".join(output_ids)
        elif more_results and get_delimiter() is None:
            print fill("* More results not shown; use -n to increase number of results or --created-before to show older results", subsequent_indent='  ')
    except:
        err_exit()

def find_data(args):
    get_output_flag(args)
    try_call(process_properties_args, args)
    if args.all_projects:
        args.project = None
        args.folder = None
        args.recurse = True
    elif args.project is None:
        args.project = dxpy.WORKSPACE_ID
    else:
        if get_last_pos_of_char(':', args.project) == -1:
            args.project = args.project + ':'
        args.project, none, none = try_call(resolve_existing_path,
                                            args.project, 'project')
    try:
        results = list(dxpy.find_data_objects(classname=args.classname,
                                              state=args.state,
                                              visibility=args.visibility,
                                              properties=args.properties,
                                              name=args.name,
                                              name_mode='glob',
                                              typename=args.type,
                                              tag=args.tag, link=args.link,
                                              project=args.project,
                                              folder=args.folder,
                                              recurse=(args.recurse if not args.recurse else None),
                                              modified_after=args.mod_after,
                                              modified_before=args.mod_before,
                                              created_after=args.created_after,
                                              created_before=args.created_before,
                                              describe=(not args.brief)))
        if args.json:
            print json.dumps(results, indent=4)
            return
        if args.brief:
            for result in results:
                print result['project'] + ':' + result['id']
        else:
            for result in results:
                if args.verbose:
                    print ""
                    print_data_obj_desc(result["describe"])
                else:
                    print_ls_l_desc(result["describe"], include_folder=True, include_project=args.all_projects)
    except:
        err_exit()

def find_projects(args):
    get_output_flag(args)
    try:
        results = list(dxpy.find_projects(name=args.name, name_mode='glob',
                                          level=('VIEW' if args.public else args.level),
                                          describe=(not args.brief),
                                          explicit_perms=(not args.public if not args.public else None),
                                          public=(args.public if args.public else None)))
        if args.json:
            print json.dumps(results, indent=4)
            return
        if args.brief:
            for result in results:
                print result['id']
            return
        if args.summary or args.verbose:
            for result in results:
                cached_project_names[result['describe']['name']] = result['id']
                print result["id"] + DELIMITER(" : ") + result['describe']['name'] + DELIMITER(' (') + result["level"] + DELIMITER(')')
        print ""
        return map(lambda result: result["id"], results)
    except:
        err_exit()

def find_apps(args):
    def maybe_x(result):
        return DNANEXUS_X() if result['describe']['billTo'] == 'org-dnanexus' else ' '

    get_output_flag(args)
    try:
        results = list(dxpy.find_apps(name=args.name, name_mode='glob', category=args.category,
                                      all_versions=args.all,
                                      published=(not args.unpublished),
                                      billed_to=args.billed_to,
                                      created_by=args.creator,
                                      developer=args.developer,
                                      created_after=args.created_after,
                                      created_before=args.created_before,
                                      modified_after=args.mod_after,
                                      modified_before=args.mod_before,
                                      describe={"fields": {"name": True, "installed": args.installed,
                                                           "title": args.summary or args.verbose,
                                                           "version": args.summary or args.verbose,
                                                           "published": args.verbose,
                                                           "billTo": args.summary or args.verbose}}))

        if args.installed:
            results = [result for result in results if result['describe']['installed']]

        if args.brief:
            results = [{"id": result['id']} for result in results]

        if args.summary or args.verbose:
            results.sort(key = lambda result: result['describe']['name'])

        if args.json:
            print json.dumps(results, indent=4)
            return
        if args.brief:
            for result in results:
                print result['id']
        elif args.summary:
            for result in results:
                print maybe_x(result) + DELIMITER(" ") + result['describe'].get('title', result['describe']['name']) + DELIMITER(' (') + result["describe"]["name"] + DELIMITER("), v") + result["describe"]["version"]
        else:
            for result in results:
                print maybe_x(result) + DELIMITER(" ") + result["id"] + DELIMITER(" ") + result['describe'].get('title', result['describe']['name']) + DELIMITER(' (') + result["describe"]["name"] + DELIMITER('), v') + result['describe']['version'] + DELIMITER(" (") + ("published" if result["describe"].get("published", 0) > 0 else "unpublished") + DELIMITER(")")
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
            project, folderpath, entity_results = resolve_existing_path(path,
                                                                       expected='entity',
                                                                       allow_mult=True,
                                                                       all_mult=args.all)
        except:
            project, folderpath, entity_results = None, None, None

        if entity_results is None:
            print fill('Could not resolve \"' + args.path + '\" to a name or ID')
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
                except BaseException as details:
                    print fill(unicode(details))

    if args.wait:
        for handler in handlers:
            handler._wait_on_close()

    if had_error:
        parser.exit(1)

def wait(args):
    had_error = False
    for path in args.path:
        if is_job_id(path):
            dxjob = dxpy.DXJob(path)
            print "Waiting for " + path + " to finish running..."
            try_call(dxjob.wait_on_done)
            print "Done"
        else:
            # Attempt to resolve name
            try:
                project, folderpath, entity_result = resolve_existing_path(path, expected='entity')
            except:
                project, folderpath, entity_result = None, None, None
            
            if entity_result is None:
                print fill('Could not resolve ' + path + ' to a data object')
                had_error = True
            else:
                handler = dxpy.get_handler(entity_result['id'], project=project)
                print "Waiting for " + path + " to close..."
                try_call(handler._wait_on_close)
                print "Done"

    if had_error:
        parser.exit(1)

def build(args):
    from dxpy.scripts import dx_build_app
    sys.argv = ['dx build'] + sys.argv[2:]
    dx_build_app.main()

def install(args):
    app_desc = get_app_from_path(args.app)
    if app_desc is None:
        parser.exit(1, 'Could not find the app\n')
    else:
        try:
            dxpy.DXHTTPRequest('/' + app_desc['id'] + '/install', {})
            print 'Installed the ' + app_desc['name'] + ' app'
        except:
            err_exit()

def uninstall(args):
    app_desc = get_app_from_path(args.app)
    if app_desc is None:
        parser.exit(1, 'Could not find the app\n')
    else:
        try:
            dxpy.DXHTTPRequest('/' + app_desc['id'] + '/uninstall', {})
            print 'Uninstalled the ' + app_desc['name'] + ' app'
        except:
            err_exit()

def get_exec_or_workflow_handler(path, alias):
    handler = None
    if alias is None:
        app_desc = get_app_from_path(path)
        try:
            # Look for applets and workflows
            project, folderpath, entity_results = resolve_existing_path(path,
                                                                        expected='entity',
                                                                        ask_to_resolve=False,
                                                                        expected_classes=['applet', 'record'])
            def is_applet(i):
                return (i['describe']['class'] == 'applet')
            def is_workflow(i):
                return ('pipeline' in i['describe']['types'])
            if entity_results is not None:
                entity_results = [i for i in entity_results if is_applet(i) or is_workflow(i)]
                if len(entity_results) == 0:
                    entity_results = None
        except:
            if app_desc is None:
                err_exit()
            else:
                project, folderpath, entity_results = None, None, None

        if entity_results is not None and len(entity_results) == 1 and app_desc is None:
            handler = dxpy.get_handler(entity_results[0]['id'], project=entity_results[0]['describe']['project'])
        elif entity_results is None and app_desc is not None:
            handler = dxpy.DXApp(dxid=app_desc['id'])
        elif entity_results is not None:
            if not sys.stdout.isatty():
                parser.exit(1, 'Found multiple executables with the path ' + path + '\n')
            print 'Found multiple executables with the path ' + path
            choice_descriptions = [get_ls_l_desc(r['describe']) for r in entity_results]
            if app_desc is not None:
                choice_descriptions.append('app-' + app_desc['name'] + ', version ' + app_desc['version'])
            choice = try_call(pick, choice_descriptions)
            if choice < len(entity_results):
                handler = dxpy.get_handler(entity_results[choice]['id'], project=entity_results[choice]['describe']['project'])
            else:
                handler = dxpy.DXApp(dxid=app_desc['id'])
                desc = app_desc
        else:
            parser.exit(1, "No matches found for " + path + '\n')
    else:
        if path.startswith('app-'):
            path = path[4:]
        handler = dxpy.DXApp(name=path, alias=alias)
    return handler

def run_one(args, executable, dest_proj, dest_path, preset_inputs=None, input_name_prefix=None,
            is_the_only_job=True):
    exec_inputs = ExecutableInputs(executable, input_name_prefix=input_name_prefix)

    exec_inputs.update(args.input_from_clone, strip_prefix=False)

    if preset_inputs is not None:
        exec_inputs.update(preset_inputs, strip_prefix=False)

    try_call(exec_inputs.update_from_args, args)

    input_json = exec_inputs.inputs

    if not args.brief:
        print ''
        print 'Using input JSON:'
        print json.dumps(input_json, indent=4)
        print ''

    # Ask for confirmation if a tty and if input was not given as a
    # single JSON.
    if args.confirm and sys.stdout.isatty():
        try:
            value = raw_input('Confirm running the applet/app with this input [Y/n]: ')
        except KeyboardInterrupt:
            value = 'n'
        if value != '' and not value.lower().startswith('y'):
            parser.exit(0)

    if not args.brief:
        print fill("Calling " + executable.get_id() + " with output destination " + dest_proj + ":" + dest_path, subsequent_indent='  ') + '\n'
    try:
        dxjob = executable.run(input_json, project=dest_proj, folder=dest_path, name=args.name,
                               details=args.details, delay_workspace_destruction=args.delay_workspace_destruction,
                               instance_type=args.instance_type)
        if not args.brief:
            print "Job ID: " + dxjob.get_id()
        else:
            print dxjob.get_id()
        sys.stdout.flush()

        if args.wait and is_the_only_job:
            dxjob.wait_on_done()
        elif args.confirm and sys.stdin.isatty() and not args.watch:
            answer = raw_input("Watch launched job now? [Y/n] ")
            if len(answer) == 0 or answer.lower()[0] == 'y':
                args.watch = True

        if args.watch and is_the_only_job:
            watch_args = parser.parse_args(['watch', dxjob.get_id()])
            print ''
            print 'Job Log'
            print '-------'
            watch(watch_args)
    except:
        err_exit()

    return dxjob

def print_run_help(executable="", alias=None):
    if executable == "":
        parser_map['run'].print_help()
    else:
        exec_help = 'usage: dx run ' + executable + ('' if alias is None else ' --alias ' + alias)
        handler = get_exec_or_workflow_handler(executable, alias)
        try:
            exec_desc = handler.describe()
        except:
            err_exit()

        if isinstance(handler, dxpy.bindings.DXRecord):
            exec_help += ' [-iSTAGE_NUM.INPUT_NAME=VALUE ...]\n\n'
            exec_help += "Workflow: " + exec_desc['name'] + "\n\n"

            workflow = handler.get_details()
            if workflow.get('version') not in range(2, 6):
                parser.exit(1, "Unrecognized workflow version {v} in {w}\n".format(v=workflow.get('version', '<none>'), w=handler))

            exec_help += fill("To run this workflow, specify values for all required inputs to each stage which are not yet bound (shown without square brackets and the value \"<unbound>\").  To specify an input, use the stage's number and input name, e.g.") + '\n\n'
            exec_help += '  -i0.input_name=3\n\n'
            exec_help += fill('gives the value 3 to stage 0 for an input called "input_name".') + '\n\n'

            exec_help += fill("Not all inputs and outputs are shown for each stage.  For a list of all inputs and outputs for each stage and additional help, run") + '\n\n'
            exec_help += '  dx run EXECUTABLE -h\n\n'
            exec_help += fill("where EXECUTABLE is the stage's app(let) name or ID, along with any --version flags given.") + '\n'

            for k in range(len(workflow['stages'])):
                workflow['stages'][k].setdefault('key', str(k))
                for i in workflow['stages'][k].get('inputs', {}).keys():
                    if workflow['stages'][k]['inputs'][i] == "":
                        del workflow['stages'][k]['inputs'][i]

            for stage in workflow['stages']:
                exec_id = stage['app']['id'] if 'id' in stage['app'] else stage['app']
                if workflow.get('version') < 3:
                    exec_desc = stage['app']
                else:
                    if dxpy.is_dxlink(exec_id):
                        exec_id = exec_id['$dnanexus_link']
                    if exec_id.startswith('app-'):
                        exec_desc = get_app_from_path(exec_id)
                    else:
                        exec_desc = dxpy.get_handler(exec_id).describe()

                input_spec = exec_desc.get('inputSpec')
                output_spec = exec_desc.get('outputSpec')

                exec_help += '\nStage ' + unicode(stage['key']) + ': '
                if exec_desc['class'] == 'app':
                    exec_help += exec_desc['name'] + ' --version ' + exec_desc['version']
                elif exec_desc['class'] == 'applet':
                    exec_help += exec_desc['name'] + ' (' + exec_id + ')'

                def render_workflow_val(val):
                    if isinstance(val, dict) and 'connectedTo' in val:
                        for stage in workflow['stages']:
                            if stage['id'] == val['connectedTo']['stage']:
                                key = stage['key']
                                break
                        return '<Stage ' + unicode(key) + ' output:' + val['connectedTo']['output'] + '>'
                    else:
                        return json.dumps(val)

                # Go over required inputs
                if input_spec is not None:
                    exec_help += '\n  Inputs: '
                    if len(input_spec) == 0:
                        exec_help += '<none>'
                    else:
                        classes_to_ignore_by_default = [
                            'int', 'array:int',
                            'double', 'array:double',
                            'boolean', 'array:boolean',
                            'string', 'array:string',
                            'float', 'array:float',
                            'hash']

                        # ALWAYS show bound and required inputs
                        # IGNORE those that are in the things to ignore and are optional and unbound
                        for param in input_spec:
                            if (stage['inputs'].get(param['name']) is None or stage['inputs'][param['name']] == param.get('default', None)) and \
                                    param['class'] in classes_to_ignore_by_default and \
                                    (param.get('optional', False) or 'default' in param):
                                continue
                            exec_help += '\n    ' + ('[' if param.get('optional', False) or 'default' in param else '') + param['name'] + (']' if param.get('optional', False) or 'default' in param else '') + '='
                            if stage['inputs'].get(param['name']) is None:
                                if 'default' in param:
                                    exec_help += '<' + json.dumps(param['default']) + ' by default>'
                                exec_help += '<unbound>'
                            else:
                                if param['class'].startswith('array'):
                                    # array input
                                    rendered_vals = [render_workflow_val(val) for val in stage['inputs'][param['name']]]
                                    exec_help += '[' + ", ".join(rendered_vals) + ']'
                                else:
                                    # non-array input
                                    exec_help += render_workflow_val(stage['inputs'][param['name']])

                # Also list outputs
                if output_spec is not None:
                    exec_help += '\n  Outputs: '
                    if len(output_spec) == 0:
                        exec_help += "<none>"
                    else:
                        for param in output_spec:
                            exec_help += '\n    ' + param['name']

                exec_help += "\n"
        else:
            exec_help += ' [-iINPUT_NAME=VALUE ...]\n\n'

            if isinstance(handler, dxpy.bindings.DXApp):
                exec_help += BOLD("App: ")
                exec_details = exec_desc['details']
            else:
                exec_help += BOLD("Applet: ")
                exec_details = handler.get_details()
            advanced_inputs = exec_details.get("advancedInputs", [])
            exec_help += exec_desc.get('title', exec_desc['name']) + '\n\n'
            summary = exec_desc.get('summary', '')
            if summary != '':
                exec_help += fill(summary) + "\n\n"

            # Contact URL here
            if isinstance(handler, dxpy.bindings.DXApp):
                exec_help += "See the app page for more information:\n  https://platform.dnanexus.com/app/" + exec_desc['name'] +"\n\n"

            exec_help += BOLD("Inputs:")
            advanced_inputs_help = "Advanced Inputs:"
            if 'inputSpec' in exec_desc:
                if len(exec_desc['inputSpec']) == 0:
                    exec_help += " <none>\n"
                else:
                    for group, params in group_array_by_field(exec_desc['inputSpec']).iteritems():
                        if group is not None:
                            exec_help += "\n " + BOLD(group)
                        for param in params:
                            param_string = "\n  "
                            param_string += UNDERLINE(param.get('label', param['name'])) + ": "
                            param_string += get_io_desc(param, app_help_version=True) + "\n"
                            helpstring = param.get('help', '')

                            stanzas = []

                            if 'choices' in param:
                                stanzas.append(format_choices_or_suggestions('Choices:', param['choices'], param['class']))

                            if helpstring != '':
                                stanzas.append(fill(helpstring, initial_indent='        ', subsequent_indent='        '))

                            if param.get('suggestions'):
                                stanzas.append(format_choices_or_suggestions('Suggestions:', param['suggestions'], param['class']))

                            param_string += "\n\n".join(stanzas) + "\n"

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
            if 'outputSpec' in exec_desc:
                if len(exec_desc['outputSpec']) == 0:
                    exec_help += " <none>\n"
                else:
                    for param in exec_desc['outputSpec']:
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

        if sys.stdout.isatty():
            if tty_rows <= exec_help.count("\n"):
                try:
                    pipe = os.popen('less -RS', 'w')
                    pipe.write(exec_help.encode('utf-8'))
                    pipe.close()
                    parser.exit(0)
                except:
                    pass
        sys.stdout.write(exec_help + '\n')

    parser.exit(0)

def print_run_input_help():
    print 'Help: Specifying input for dx run\n'
    print fill('There are several ways to specify inputs.  In decreasing order of precedence, they are:')
    print '''
  1) inputs given in the interactive mode
  2) inputs listed individually with the -i/--input command line argument
  3) JSON given in --input-json
  4) JSON given in --input-json-file
  5) any inputs set in a workflow (if running a workflow).
'''
    print 'SPECIFYING INPUTS BY NAME\n\n' + fill('Use the -i/--input flag to specify each input field by ' + BOLD() + 'name' + ENDC() + ' and ' + BOLD() + 'value' + ENDC() + '.', initial_indent='  ', subsequent_indent='  ')
    print '''
    Syntax :  -i<input name>=<input value>
    Example:  dx run myApp -inum=34 -istr=ABC -igtables=reads1 -igtables=reads2
'''
    print fill('The example above runs an app called "myApp" with 3 inputs called num (class int), str (class string), and gtables (class array:gtable).  (For this method to work, the app must have an input spec so inputs can be interpreted correctly.)  The same input field can be used multiple times if the input class is an array.', initial_indent='  ', subsequent_indent='  ')
    print '\n' + fill(BOLD() + 'Job-based object references' + ENDC() + ' can also be provided using the <job id>:<output name> syntax:', initial_indent='  ', subsequent_indent='  ')
    print '''
    Syntax :  -i<input name>=<job id>:<output name>
    Example:  dx run mapper -ireads=job-B0fbxvGY00j9jqGQvj8Q0001:reads
'''
    print fill('When executing ' + BOLD() + 'workflows' + ENDC() + ', stage inputs can be specified using the <stage key>.<input name>=<value> syntax:', initial_indent='  ', subsequent_indent='  ')
    print '''
    Syntax :  -i<stage key>.<input name>=<input value>
    Example:  dx run my_workflow -i1.reads="My reads file"

SPECIFYING JSON INPUT
'''
    print fill('JSON input can be used directly using the -j/--input-json or -f/--input-json-file flags.  When running an ' + BOLD() + 'app' + ENDC() + ' or ' + BOLD() + 'applet' + ENDC() + ', the keys should be the input field names for the app or applet.  When running a ' + BOLD() + 'workflow' + ENDC() + ', the keys should be the input field names for each stage, prefixed by the stage key and a period, e.g. "1.reads" for the "reads" input of stage "1".', initial_indent='  ', subsequent_indent='  ') + '\n'
    parser.exit(0)

def run(args):
    if args.help:
        print_run_help(args.executable, args.alias)

    if args.clone is None and args.executable == "":
        parser_map['run'].print_help()
        parser.exit(2, fill("Error: Either the executable must be specified, or --clone must be used to indicate a job to clone") + "\n")

    args.input_from_clone = {}

    clone_desc = None
    if args.clone is not None:
        # Resolve job ID or name
        if is_job_id(args.clone):
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
                        job_name_or_id = args.clone[colon_pos + 1:]
                        if is_job_id(job_name_or_id):
                            clone_desc = dxpy.api.job_describe(job_name_or_id)
                        else:
                            iterators.append(dxpy.find_jobs(name=job_name_or_id,
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
                                                       get_find_jobs_string(result["describe"],
                                                                            has_children=False,
                                                                            single_result=True)))
                if result_choice == "none found":
                    parser.exit(1, "dx run --clone: No matching job found. Please use a valid job name or ID.\n")
                elif result_choice == "none picked":
                    parser.exit(1)
                else:
                    clone_desc = dxpy.api.job_describe(result_choice["id"])

        if args.executable == "":
            args.executable = clone_desc.get("applet", clone_desc.get("app", ""))
        if args.folder is None:
            args.folder = clone_desc["project"] + ":" + clone_desc["folder"]
        if args.name is None:
            import re
            match_obj = re.search("\(re-run\)$", clone_desc["name"])
            if match_obj is None:
                args.name = clone_desc["name"] + " (re-run)"
            else:
                args.name = clone_desc["name"]
        args.input_from_clone = clone_desc["runInput"]
        args.details = {"clonedFrom": {"id": clone_desc["id"],
                                       "executable": clone_desc.get("applet", clone_desc.get("app", "")),
                                       "project": clone_desc["project"],
                                       "folder": clone_desc["folder"],
                                       "name": clone_desc["name"],
                                       "runInput": clone_desc["runInput"]
                                       }
                        }

    get_output_flag(args)
    handler = get_exec_or_workflow_handler(args.executable, args.alias)

    if args.project is not None:
        if args.folder is not None and not args.clone:
            err_exit("Options --project and --folder/--destination cannot be specified together")
        args.folder = args.project + ":/"

    if args.folder is None:
        dest_proj = dxpy.WORKSPACE_ID
        if dest_proj is None:
            parser.exit(1, 'Unable to find project to run the app in. Please run "dx select" to set the working project, or use --folder=project:path\n')
        dest_path = os.environ.get('DX_CLI_WD', '/').decode('utf-8')
    else:
        dest_proj, dest_path, none = try_call(resolve_existing_path,
                                              args.folder,
                                              expected='folder')

    if args.instance_type and args.instance_type.strip().startswith('{'):
        try:
            args.instance_type = json.loads(args.instance_type)
        except ValueError:
            err_exit("Error while parsing JSON value for --instance-type",
                     expected_exceptions=default_expected_exceptions + (ValueError,))

    if isinstance(handler, dxpy.bindings.DXRecord): # Identified as a workflow in get_exec_or_workflow_handler()
        if clone_desc is not None:
            parser.exit(1, fill("Cannot run a workflow and clone a job; only apps or applets can be run with cloned options") + "\n")
        if not args.brief:
            print "Executing workflow", handler
        workflow = handler.get_details()
        if workflow.get('version') not in range(2, 6):
            parser.exit(1, "Unrecognized workflow version {v} in {w}\n".format(v=workflow['version'], w=handler))
        launched_jobs = {stage['id']: None for stage in workflow['stages']}
        requested_job_name = args.name

        for k in range(len(workflow['stages'])):
            workflow['stages'][k].setdefault('key', str(k))
            for i in workflow['stages'][k].get('inputs', {}).keys():
                if workflow['stages'][k]['inputs'][i] == "":
                    del workflow['stages'][k]['inputs'][i]

        for stage in workflow['stages']:
            if not args.brief:
                print "Processing stage", stage['key'], "(id", stage['id'] + ")"
            inputs_from_stage = {k: stage_to_job_refs(v, launched_jobs) for k, v in stage['inputs'].iteritems() if v is not None}

            exec_id = stage['app']['id'] if 'id' in stage['app'] else stage['app']
            if dxpy.is_dxlink(exec_id):
                exec_id = exec_id['$dnanexus_link']
            if exec_id.startswith('app-'):
                exec_id = get_app_from_path(exec_id)['id']

            executable = dxpy.get_handler(exec_id)
            if requested_job_name is None:
                # TODO: name and describe caching in dxobject
                # TODO: does apiserver append entry point name to parent/subjobs if job name is given? (or should we use :main here?)
                #args.name = "{wf}.{key}:{exec}".format(wf=workflow['name'], key=stage['key'], exec=executable.name)
                pass
            launched_jobs[stage['id']] = run_one(args, executable, dest_proj, dest_path,
                                                 preset_inputs=inputs_from_stage,
                                                 input_name_prefix=str(stage['key'])+".",
                                                 is_the_only_job=False)
        if args.wait:
            for stage in workflow['stages']:
                launched_jobs[stage['id']].wait_on_done()
        elif args.confirm and sys.stdin.isatty() and not args.watch:
            answer = raw_input("Watch launched jobs now? [Y/n] ")
            if len(answer) == 0 or answer.lower()[0] == 'y':
                args.watch = True

        if args.watch:
            for stage in workflow['stages']:
                watch_args = parser.parse_args(['watch', launched_jobs[stage['id']].get_id()])
                print ''
                print 'Job Log'
                print '-------'
                watch(watch_args)
    else:
        run_one(args, handler, dest_proj, dest_path)

def terminate(args):
    for jobid in args.jobid:
        try:
            dxpy.api.job_terminate(jobid)
        except:
            err_exit()

def shell(orig_args):
    if orig_args.filename is not None:
        try:
            with open(orig_args.filename, 'r') as script:
                for line in script:
                    args = parser.parse_args(shlex.split(line))
                    set_cli_colors(args)
                    args.func(args)
            exit(0)
        except:
            err_exit()
    elif not sys.stdin.isatty():
        for line in sys.stdin.read().splitlines():
            if len(line) > 0:
                args = parser.parse_args(shlex.split(line))
                set_cli_colors(args)
                args.func(args)
        exit(0)

    if state['interactive']:
        return
    state['interactive'] = True

    # WARNING: Following two lines may not be platform-independent and
    # should be made so.
    try:
        import rlcompleter
        readline.parse_and_bind("tab: complete")

        readline.set_completer_delims("")

        readline.set_completer(DXCLICompleter().complete)
    except:
        pass

    while True:
        # Reset the completer once we're done grabbing input
        try:
            if readline.get_completer() is None:
                readline.set_completer(DXCLICompleter().complete)
                readline.clear_history()
                readline.read_history_file(os.path.expanduser('~/.dnanexus_config/.dx_history'))
        except:
            pass
        try:
            prompt = '> '
            pwd_str = get_pwd()
            if pwd_str is not None:
                prompt = pwd_str + prompt
            cmd = raw_input(prompt)
        except EOFError:
            print ""
            exit(0)
        except KeyboardInterrupt:
            print ""
            continue
        if cmd == '':
            continue
        try:
            args = parser.parse_args(shlex.split(cmd))
            set_cli_colors(args)
            set_delim(args)
            args.func(args)
        except StopIteration:
            exit(0)
        except BaseException as details:
            if unicode(details) != '1' and unicode(details) != '0':
                print unicode(details) + '\n'

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
            args.format = BLUE() + "{job_name} ({job})" + ENDC() + " {level_color}{level}" + ENDC() + " {msg}"
        else:
            args.format = BLUE() + "{job_name}" + ENDC() + " {level_color}{level}" + ENDC() + " {msg}"
        if args.timestamps:
            args.format = "{timestamp} " + args.format

        def msg_callback(message):
            message['timestamp'] = str(datetime.datetime.fromtimestamp(message.get('timestamp', 0)/1000))
            message['level_color'] = level_colors.get(message.get('level', ''), '')
            message['job_name'] = log_client.seen_jobs[message['job']]['name'] if message['job'] in log_client.seen_jobs else message['job']
            print args.format.format(**message)

    from dxpy.utils.job_log_client import DXJobLogStreamClient

    input_params = {"numRecentMessages": args.num_recent_messages,
                    "recurseJobs": args.tree}
    
    if args.levels:
        input_params['levels'] = args.levels

    log_client = DXJobLogStreamClient(args.jobid, input_params=input_params, msg_callback=msg_callback,
                                      msg_output_format=args.format, print_job_info=args.job_info)

    # Note: currently, the client is synchronous and blocks until the socket is closed.
    # If this changes, some refactoring may be needed below
    try:
        if not args.quiet:
            print >>sys.stderr, "Watching job %s%s. Press Ctrl+C to stop." % (args.jobid, (" and sub-jobs" if args.tree else ""))
        log_client.connect()
    except Exception as details:
        parser.exit(3, fill(unicode(details)) + '\n')
#    except KeyboardInterrupt:
#        sys.exit(1)
#        print "Watching job %s%s. Press Ctrl+C to stop." % (args.jobid, (" and sub-jobs" if args.tree else ""))

def upgrade(args):
    if len(args.args) == 0:
        try:
            greeting = dxpy.api.system_greet({'client': 'dxclient', 'version': dxpy.TOOLKIT_VERSION}, auth=None)
            if greeting['update']['available']:
                recommended_version = greeting['update']['version']
            else:
                err_exit("Your SDK is up to date.", code=0)
        except (dxpy.DXAPIError, requests.ConnectionError, requests.HTTPError, requests.Timeout, httplib.HTTPException) as e:
            print e
            recommended_version = "current"
        print "Upgrading to", recommended_version
        args.args = [recommended_version]

    try:
        cmd = os.path.join(os.environ['DNANEXUS_HOME'], 'build', 'upgrade.sh')
        args.args.insert(0, cmd)
        os.execv(cmd, args.args)
    except:
        err_exit()

def print_help(args):
    if args.command_or_category is None:
        parser_help.print_help()
    elif args.command_or_category in parser_categories:
        print 'dx ' + args.command_or_category + ': ' + parser_categories[args.command_or_category]['desc'].lstrip()
        print '\nCommands:\n'
        for cmd in parser_categories[args.command_or_category]['cmds']:
            print '  ' + cmd[0] + ' '*(18-len(cmd[0])) + fill(cmd[1], width_adjustment=-20, subsequent_indent=' '*20)
    elif args.command_or_category not in parser_map:
        parser.exit(1, 'Unrecognized command: ' + args.command_or_category + '\n')
    elif args.command_or_category == 'export' and args.subcommand is not None:
        if args.subcommand not in exporters:
            parser.exit(1, 'Unsupported format for dx export: ' + args.subcommand + '\n')
        new_args = argparse.Namespace()
        setattr(new_args, 'exporter_args', ['-h'])
        exporters[args.subcommand](new_args)
    elif args.command_or_category == 'import' and args.subcommand is not None:
        if args.subcommand not in importers:
            parser.exit(1, 'Unsupported format for dx import: ' + args.subcommand + '\n')
        new_args = argparse.Namespace()
        setattr(new_args, 'importer_args', ['-h'])
        importers[args.subcommand](new_args)
    elif args.command_or_category == 'run':
        if args.subcommand is None:
            parser_map[args.command_or_category].print_help()
        else:
            print_run_help(args.subcommand)
    elif args.subcommand is None:
        parser_map[args.command_or_category].print_help()
    elif (args.command_or_category + ' ' + args.subcommand) not in parser_map:
        parser.exit(1, 'Unrecognized command and subcommand combination: ' + args.command_or_category + ' ' + args.subcommand + '\n')
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
        set_api(protocol='https', host='stagingapi.dnanexus.com', port='443', write=(not state['interactive'] or namespace.save))

class SetProdEnv(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):        
        setattr(namespace, 'host', 'prodauth.dnanexus.com')
        setattr(namespace, 'port', '443')
        setattr(namespace, 'protocol', 'https')
        setattr(namespace, 'prod', True)
        set_api(protocol='https', host='prodapi.dnanexus.com', port='443', write=(not state['interactive'] or namespace.save))

class SetPreprodEnv(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):        
        setattr(namespace, 'host', 'preprodauth.dnanexus.com')
        setattr(namespace, 'port', '443')
        setattr(namespace, 'protocol', 'https')
        setattr(namespace, 'preprod', True)
        set_api(protocol='https', host='preprodapi.dnanexus.com', port='443', write=(not state['interactive'] or namespace.save))

class DXArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_help(sys.stderr)
        self.exit(2, '%s: error: %s\n' % (self.prog, message))

def register_subparser(subparser, subparsers_action=None, categories=('other', )):
    if subparsers_action is None:
        subparsers_action = subparsers
    if isinstance(categories, basestring):
        categories = (categories, )
    if subparsers_action == subparsers:
        name = subparsers_action._choices_actions[-1].dest
    else:
        name = subparsers._choices_actions[-1].dest + ' ' + subparsers_action._choices_actions[-1].dest
    _help = subparsers_action._choices_actions[-1].help
    parser_map[name] = subparser
    parser_categories['all']['cmds'].append((name, _help))
    for category in categories:
        parser_categories[category]['cmds'].append((name, _help))


parser = DXArgumentParser(description=DNANEXUS_LOGO() + ' Command-Line Client, API v%s, client %s' % (dxpy.API_VERSION, dxpy.TOOLKIT_VERSION) + '\n\n' + fill('dx is a command-line client for interacting with the DNAnexus platform.  You can log in, navigate, upload, organize and share your data, launch analyses, and more.  For a quick tour of what the tool can do, see') + '\n\n  https://wiki.dnanexus.com/Command-Line-Client/Quickstart\n\n' + fill('For a breakdown of dx commands by category, run \"dx help\".'),
                          formatter_class=argparse.RawTextHelpFormatter,
                          parents=[env_args],
                          usage='%(prog)s [-h] [--version] command ...')
parser.add_argument('--version', action='version', version='dx %s' % (dxpy.TOOLKIT_VERSION,))

subparsers = parser.add_subparsers(help=argparse.SUPPRESS, dest='command')
subparsers.metavar = 'command'

parser_login = subparsers.add_parser('login', help='Log in (interactively or with an existing API token)', description='Log in interactively and acquire credentials.  Use "--token" to log in with an existing API token.', prog='dx login',
                                     parents=[env_args])
parser_login.add_argument('--token', help='Authentication token to use')
parser_login.add_argument('--host', help='Log into the given auth server host (port must also be given)')
parser_login.add_argument('--port', type=int, help='Log into the given auth server port (host must also be given)')
parser_login.add_argument('--protocol', help='Use the given protocol to contact auth server (by default, the correct protocol is guessed based on --port)')
parser_login.add_argument('--noprojects', dest='projects', help='Do not print available projects', action='store_false')
parser_login.add_argument('--save', help='Save token and other environment variables for future sessions', action='store_true')
parser_login.add_argument('--timeout', help='Timeout for this login token', default='30d')
parser_login.add_argument('--staging', nargs=0, help=argparse.SUPPRESS, action=SetStagingEnv)
parser_login.add_argument('--prod', nargs=0, help=argparse.SUPPRESS, action=SetProdEnv)
parser_login.add_argument('--preprod', nargs=0, help=argparse.SUPPRESS, action=SetPreprodEnv)
parser_login.set_defaults(staging=False, prod=False, func=login)
register_subparser(parser_login, categories='session')

parser_logout = subparsers.add_parser('logout',
                                      help='Log out and remove credentials',
                                      description='Log out and remove credentials',
                                      prog='dx logout',
                                      parents=[env_args])
parser_logout.add_argument('--host', help='Log out of the given auth server host (port must also be given)')
parser_logout.add_argument('--port', type=int, help='Log out of the given auth server port (host must also be given)')
parser_logout.set_defaults(func=logout)
register_subparser(parser_logout, categories='session')

parser_shell = subparsers.add_parser('sh', help='dx shell interpreter',
                                     description='When run with no arguments, this command launches an interactive shell.  Otherwise, it will load the filename provided and interpret each nonempty line as a command to execute.  In both cases, the "dx" is expected to be omitted from the command or line.',
                                     prog='dx sh',
                                     parents=[env_args])
parser_shell.add_argument('filename', help='File of dx commands to execute', nargs='?', default=None)
parser_shell.set_defaults(func=shell)
register_subparser(parser_shell, categories='session')

parser_exit = subparsers.add_parser('exit', help='Exit out of the interactive shell', description='Exit out of the interactive shell', prog='dx exit')
parser_exit.set_defaults(func=exit_shell)
register_subparser(parser_exit, categories='session')

parser_env = subparsers.add_parser('env', help='Print all environment variables in use',
                                   description=fill('Prints all environment variables in use as they have been resolved from environment variables and configuration files.  For more details, see') + '\n\nhttps://wiki.dnanexus.com/Command-Line-Client/Environment-Variables',
                                   formatter_class=argparse.RawTextHelpFormatter, prog='dx env',
                                   parents=[env_args])
parser_env.add_argument('--bash', help=fill('Prints a list of bash commands to export the environment variables', width_adjustment=-14), action='store_true')
parser_env.add_argument('--dx-flags', help=fill('Prints the dx options to override the environment variables', width_adjustment=-14), action='store_true')
parser_env.set_defaults(func=env)
register_subparser(parser_env, categories='session')

parser_setenv = subparsers.add_parser('setenv',
                                      help='Sets environment variables for the session',
                                      description='Sets environment variables for communication with the API server')
parser_setenv.add_argument('--noprojects', dest='projects', help='Do not print available projects', action='store_false')
parser_setenv.add_argument('--save', help='Save settings for future sessions.  Only one set of settings can be saved at a time.  Always set to true if login is run in a non-interactive session', action='store_true')
parser_setenv.add_argument('--current', help='Do not prompt for new values and just save current settings for future sessions.  Overrides --save to be true.', action='store_true')
parser_setenv.set_defaults(func=setenv)
register_subparser(parser_setenv, categories='other')

parser_clearenv = subparsers.add_parser('clearenv', help='Clears all environment variables set by dx', 
                                        description='Clears all environment variables set by dx.  More specifically, it removes local state stored in ~/.dnanexus_config/environment.  Does not affect the environment variables currently set in your shell.', prog='dx clearenv')
parser_clearenv.add_argument('--reset', help='Reset dx environment variables to empty values. Use this to avoid interference between multiple dx sessions when using shell environment variables.', action='store_true')
parser_clearenv.set_defaults(func=clearenv, interactive=True)
register_subparser(parser_clearenv, categories='session')

parser_invite = subparsers.add_parser('invite',
                                      help='Invite another user to a project or make it public',
                                      description='Invite a DNAnexus entity to a project.  Use "PUBLIC" as the invitee and "VIEW" as the level to make the project public.  If the invitee is not recognized as a DNAnexus ID or is not "PUBLIC", it will be treated as a username, i.e. "dx invite alice : VIEW" is equivalent to inviting the user with user ID "user-alice" to view your current default project.',
                                      prog='dx invite',
                                      parents=[env_args])
parser_invite.add_argument('invitee', help='Entity to invite')
parser_invite.add_argument('project', help='Project to invite the invitee to', default=':', nargs='?')
parser_invite.add_argument('level', help='Permissions level the new member should have', choices=['VIEW', 'CONTRIBUTE', 'ADMINISTER'], default='VIEW', nargs='?')
parser_invite.set_defaults(func=invite)
# parser_invite.completer = TODO
register_subparser(parser_invite, categories='other')

parser_uninvite = subparsers.add_parser('uninvite',
                                        help='Revoke others\' permissions on a project you administer',
                                        description='Revoke others\' permissions on a project you administer.  Use "PUBLIC" as the entity to make the project no longer public.  If the entity is not recognized as a DNAnexus ID or is not "PUBLIC", it will be treated as a username, i.e. "dx uninvite alice :" is equivalent to revoking the permissions of the user with user ID "user-alice" to your current default project.',
                                        prog='dx uninvite',
                                        parents=[env_args])
parser_uninvite.add_argument('entity', help='Entity to uninvite')
parser_uninvite.add_argument('project', help='Project to revoke permissions from')
parser_uninvite.set_defaults(func=uninvite)
register_subparser(parser_uninvite, categories='other')

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
ls_path_action = parser_ls.add_argument('path', help='Folder (possibly in another project) to list the contents of, default is the current directory in the current project.  Syntax: projectID:/folder/path', nargs='?', default='.')
ls_path_action.completer = DXPathCompleter()
parser_ls.set_defaults(func=ls)
register_subparser(parser_ls, categories='fs')

parser_tree = subparsers.add_parser('tree', help='List folders and objects in a tree',
                                    description='List folders and objects in a tree',
                                    parents=[no_color_arg, env_args],
                                    prog='dx tree')
parser_tree.add_argument('-a', '--all', help='show hidden files', action='store_true')
parser_tree.add_argument('-l', '--long', help='use a long listing format', action='store_true')
tree_path_action = parser_tree.add_argument('path', help='Folder (possibly in another project) to list the contents of, default is the current directory in the current project.  Syntax: projectID:/folder/path', nargs='?', default='.')
tree_path_action.completer = DXPathCompleter(expected='folder')
parser_tree.set_defaults(func=tree)
register_subparser(parser_tree, categories='fs')

parser_pwd = subparsers.add_parser('pwd', help='Print current working directory',
                                   description='Print current working directory',
                                   prog='dx pwd',
                                   parents=[env_args])
parser_pwd.set_defaults(func=pwd)
register_subparser(parser_pwd, categories='fs')

parser_select = subparsers.add_parser('select', help='List and select a project to switch to',
                                      description='Interactively list and select a project to switch to.  By default, only lists projects for which you have at least CONTRIBUTE permissions.  Use --public to see the list of public projects.',
                                      prog='dx select',
                                      parents=[env_args])
select_project_action = parser_select.add_argument('project', help='Name or ID of a project to switch to; if not provided a list will be provided for you', nargs='?', default=None)
select_project_action.completer = DXPathCompleter(expected='project', include_current_proj=False)
parser_select.add_argument('--name', help='Name of the project (wildcard patterns supported)')
parser_select.add_argument('--level', choices=['LIST', 'VIEW', 'CONTRIBUTE', 'ADMINISTER'], help='Minimum level of permissions expected', default='CONTRIBUTE')
parser_select.add_argument('--public', help='Include ONLY public projects (will automatically set --level to VIEW)', action='store_true')
parser_select.set_defaults(func=select, save=False)
register_subparser(parser_select, categories='fs')

parser_cd = subparsers.add_parser('cd', help='Change the current working directory',
                                  description='Change the current working directory', prog='dx cd',
                                  parents=[env_args])
cd_path_action = parser_cd.add_argument('path', help='Folder (possibly in another project) to which to change the current working directory, default is \"/\" in the current project', nargs='?', default='/')
cd_path_action.completer = DXPathCompleter(expected='folder')
parser_cd.set_defaults(func=cd)
register_subparser(parser_cd, categories='fs')

parser_cp = subparsers.add_parser('cp', help='Copy objects and/or folders between different projects',
                                  formatter_class=argparse.RawTextHelpFormatter,
                                  description=fill('Copy objects and/or folders between different projects.  Folders will automatically be copied recursively.  To specify which project to use as a source or destination, prepend the path or ID of the object/folder with the project ID or name and a colon.') + '''

EXAMPLES

  ''' + fill('The first example copies a gtable in a project called "FirstProj" to the current directory of the current project.  The second example copies the object named "reads" in the current directory to the folder /folder/path in the project with ID "project-B0VK6F6gpqG6z7JGkbqQ000Q", and finally renaming it to "newname".', width_adjustment=-2, subsequent_indent='  ') + '''

  $ dx cp FirstProj:gtable-B0XBQFygpqGK8ZPjbk0Q000q .
  $ dx cp reads project-B0VK6F6gpqG6z7JGkbqQ000Q:/folder/path/newname
''',
                                  prog='dx cp',
                                  parents=[env_args, all_arg])
cp_sources_action = parser_cp.add_argument('sources', help='Objects and/or folder names to copy', metavar='source', nargs='+')
cp_sources_action.completer = DXPathCompleter()
parser_cp.add_argument('destination', help=fill('Folder into which to copy the sources or new pathname (if only one source is provided).  Must be in a different project/container than all source paths.', width_adjustment=-15))
parser_cp.set_defaults(func=cp)
register_subparser(parser_cp, categories='fs')

parser_mv = subparsers.add_parser('mv', help='Move or rename objects and/or folders inside a project',
                                  formatter_class=argparse.RawTextHelpFormatter, 
                                  description=fill('Move or rename data objects and/or folders inside a single project.  To copy data between different projects, use \'dx cp\' instead.'),
                                  prog='dx mv',
                                  parents=[env_args, all_arg])
mv_sources_action = parser_mv.add_argument('sources', help='Objects and/or folder names to move', metavar='source', nargs='+')
mv_sources_action.completer = DXPathCompleter()
parser_mv.add_argument('destination', help=fill('Folder into which to move the sources or new pathname (if only one source is provided).  Must be in the same project/container as all source paths.', width_adjustment=-15))
parser_mv.set_defaults(func=mv)
register_subparser(parser_mv, categories='fs')

parser_mkdir = subparsers.add_parser('mkdir', help='Create a new folder',
                                     description='Create a new folder', prog='dx mkdir',
                                     parents=[env_args])
parser_mkdir.add_argument('-p', '--parents', help='no error if existing, create parent directories as needed', action='store_true')
mkdir_paths_action = parser_mkdir.add_argument('paths', help='Paths to folders to create', metavar='path', nargs='+')
mkdir_paths_action.completer = DXPathCompleter(expected='folder')
parser_mkdir.set_defaults(func=mkdir)
register_subparser(parser_mkdir, categories='fs')

parser_rmdir = subparsers.add_parser('rmdir', help='Remove a folder',
                                     description='Remove a folder', prog='dx rmdir',
                                     parents=[env_args])
rmdir_paths_action = parser_rmdir.add_argument('paths', help='Paths to folders to remove', metavar='path', nargs='+')
rmdir_paths_action.completer = DXPathCompleter(expected='folder')
parser_rmdir.set_defaults(func=rmdir)
register_subparser(parser_rmdir, categories='fs')

parser_rm = subparsers.add_parser('rm', help='Remove data objects and folders',
                                  description='Remove data objects and folders.', prog='dx rm',
                                  parents=[env_args, all_arg])
rm_paths_action = parser_rm.add_argument('paths', help='Paths to remove', metavar='path', nargs='+')
rm_paths_action.completer = DXPathCompleter()
parser_rm.add_argument('-r', '--recursive', help='Recurse into a directory', action='store_true')
parser_rm.set_defaults(func=rm)
register_subparser(parser_rm, categories='fs')

# data

parser_describe = subparsers.add_parser('describe', help='Describe a remote object',
                                        description=fill('Describe a DNAnexus entity.  Use this command to describe data objects by name or ID, jobs, apps, users, organizations, etc.  If using the "--json" flag, it will thrown an error if more than one match is found (but if you would like a JSON array of the describe hashes of all matches, then provide the "--multi" flag).  Otherwise, it will always display all results it finds.') + '\n\nNOTES:\n\n- ' + fill('The project found in the path is used as a HINT when you are using an object ID; you may still get a result if you have access to a copy of the object in some other project, but if it exists in the specified project, its description will be returned.') + '\n\n- ' + fill('When describing apps or applets, options marked as advanced inputs will be hidden unless --verbose is provided'),
                                        formatter_class=argparse.RawTextHelpFormatter,
                                        parents=[json_arg, no_color_arg, delim_arg, env_args],
                                        prog='dx describe')
parser_describe.add_argument('--details', help='Include details of data objects', action='store_true')
parser_describe.add_argument('--verbose', help='Include all possible metadata', action='store_true')
parser_describe.add_argument('--name', help='Only print the matching names, one per line', action='store_true')
parser_describe.add_argument('--multi', help=fill('If the flag --json is also provided, then returns a JSON array of describe hashes of all matching results', width_adjustment=-24), action='store_true')
describe_path_action = parser_describe.add_argument('path', help=fill('Object ID or path to an object (possibly in another project) to describe.', width_adjustment=-24))
describe_path_action.completer = DXPathCompleter()
parser_describe.set_defaults(func=describe)
register_subparser(parser_describe, categories=('data', 'metadata'))

parser_upload = subparsers.add_parser('upload', help='Upload file(s) or directory',
                                      description='Upload local file(s) or directory.  If "-" is provided, stdin will be used instead.  By default, the filename will be used as its new name.  If --path/--destination is provided with a path ending in a slash, the filename will be used, and the folder path will be used as a destination.  If it does not end in a slash, then it will be used as the final name.',
                                      parents=[parser_dataobject_args, stdout_args, env_args],
                                      prog="dx upload")
upload_filename_action = parser_upload.add_argument('filename', nargs='+',
                                                    help='Local file or directory to upload ("-" indicates stdin input); provide multiple times to upload multiple files or directories')
#upload_filename_action.completer = LocalCompleter()
parser_upload.add_argument('-o', '--output', help=argparse.SUPPRESS) # deprecated; equivalent to --path/--destination
parser_upload.add_argument('--path', '--destination', help=fill('DNAnexus path to upload file(s) to (default uses current project and folder if not provided)', width_adjustment=-24), nargs='?')
parser_upload.add_argument('-r', '--recursive', help='Upload directories recursively', action='store_true')
parser_upload.add_argument('--wait', help='Wait until the file has finished closing', action='store_true')
parser_upload.add_argument('--no-progress', help='Do not show a progress bar', dest='show_progress', action='store_false', default=sys.stderr.isatty())
parser_upload.set_defaults(func=upload, mute=False)
register_subparser(parser_upload, categories='data')

parser_download = subparsers.add_parser('download', help='Download file(s)',
                                        description='Download the contents of a file object or multiple objects.  Use "-o -" to direct the output to stdout.',
                                        prog='dx download',
                                        parents=[env_args])
parser_download.add_argument('path', help='Data object ID or name, or folder to download', nargs='+').completer = DXPathCompleter(classes=['file'])
parser_download.add_argument('-o', '--output', help='Local filename or directory to be used ("-" indicates stdout output); if not supplied or a directory is given, the object\'s name on the platform will be used, along with any applicable extensions')
parser_download.add_argument('-f', '--overwrite', help='Overwrite the local file if necessary', action='store_true')
parser_download.add_argument('-r', '--recursive', help='Download folders recursively', action='store_true')
parser_download.add_argument('--no-progress', help='Do not show a progress bar', dest='show_progress', action='store_false', default=sys.stderr.isatty())
parser_download.set_defaults(func=download)
register_subparser(parser_download, categories='data')

parser_cat = subparsers.add_parser('cat', help='Print file(s) to stdout', prog='dx cat',
                                   parents=[env_args])
cat_path_action = parser_cat.add_argument('path', help='File ID or name(s) to print to stdout', nargs='+')
cat_path_action.completer = DXPathCompleter(classes=['file'])
parser_cat.set_defaults(func=cat)
register_subparser(parser_cat, categories='data')

parser_head = subparsers.add_parser('head',
                                    help='Print part of a file or gtable',
                                    description='Print the first part of a file or a gtable.  By default, prints the first 10 lines or rows, respectively.  Additional query parameters can be provided in the case of gtables.  The output for gtables is formatted for human-readability; to print rows in a machine-readable format, see "dx export tsv".',
                                    parents=[no_color_arg, env_args],
                                    prog='dx head')
parser_head.add_argument('-n', '--lines', type=int, metavar='N', help='Print the first N lines or rows (default 10)', default=10)
head_gtable_args = parser_head.add_argument_group(title='GTable-specific options')
head_gtable_args.add_argument('-w', '--max-col-width', type=int, help='Maximum width of each column to display', default=32)
head_gtable_args.add_argument('--starting', type=int, help='Specify starting row ID', default=0)
head_gtable_args.add_argument('--gri', nargs=3, metavar=('CHR', 'LO', 'HI'), help='Specify chromosome name, low coordinate, and high coordinate for Genomic Range Index')
head_gtable_args.add_argument('--gri-mode', help='Specify the mode of the GRI query (\'overlap\' or \'enclose\'; default \'overlap\')', default="overlap")
head_gtable_args.add_argument('--gri-name', help='Override the default name of the Genomic Range Index (default: "gri"))', default="gri")
head_path_action = parser_head.add_argument('path', help='File or gtable ID or name to access')
head_path_action.completer = DXPathCompleter(classes=['file', 'gtable'])
parser_head.set_defaults(func=head)
register_subparser(parser_head, categories='data')

parser_import = subparsers.add_parser('import',
                                      help='Import (convert and upload) a local table or genomic file',
                                      description=fill('Import a local file to the DNAnexus platform as a GenomicTable.') + '\n\n' + fill('For more details on how to import from a particular format, run ') + '\n  $ dx help import <format>' + '\n\nSupported formats:\n\n  ' + '\n  '.join(sorted(importers)),
                                      formatter_class=argparse.RawTextHelpFormatter,
                                      prog='dx import',
                                      parents=[env_args])
parser_import.add_argument('format', help='Format to import from')
import_args_action = parser_import.add_argument('importer_args', help=fill('Arguments passed to the importer', width_adjustment=-24), nargs=argparse.REMAINDER)
#import_args_action.completer = LocalCompleter()
parser_import.set_defaults(func=dximport)
register_subparser(parser_import, categories='data')

parser_export = subparsers.add_parser('export',
                                      help='Export (download and convert) a gtable into a local file',
                                      description=fill('Export a GenomicTable into a local file with a particular file format.') + '\n\n' + fill('For more details on how to convert into a particular format, run ') + '\n  $ dx help export <format>' + '\n\nSupported formats:\n\n  ' + '\n  '.join(sorted(exporters)),
                                      formatter_class=argparse.RawTextHelpFormatter,
                                      prog='dx export',
                                      parents=[env_args])
parser_export.add_argument('format', help='Format to export to')
parser_export.add_argument('exporter_args', help=fill('Arguments passed to the exporter', width_adjustment=-24), nargs=argparse.REMAINDER)
parser_export.set_defaults(func=export)
register_subparser(parser_export, categories='data')

from dxpy.scripts.dx_build_app import parser as build_parser
build_parser.prog = 'dx build'
build_parser.set_defaults(mode="applet")

parser_build = subparsers.add_parser('build', help='Upload and build a new applet/app',
                                     description='Build an applet or app object from a local source directory.  You can use dx-app-wizard to generate a skeleton directory with the necessary files.',
                                     prog='dx build',
                                     add_help=False,
                                     parents=[build_parser, env_args]
)
parser_build.set_defaults(func=build)
#parser_build.completer = LocalCompleter()
register_subparser(parser_build, categories='exec')

parser_install = subparsers.add_parser('install', help='Install an app',
                                       description='Install an app by name.  To see a list of apps you can install, hit <TAB> twice after "dx install" or run "' + BOLD() + 'dx find apps' + ENDC() + '" to see a list of available apps.', prog='dx install',
                                       parents=[env_args])
install_app_action = parser_install.add_argument('app', help='ID or name of app to install')
install_app_action.completer = DXAppCompleter(installed=False)
parser_install.set_defaults(func=install)
register_subparser(parser_install, categories='exec')

parser_uninstall = subparsers.add_parser('uninstall', help='Uninstall an app',
                                         description='Uninstall an app by name.', prog='dx uninstall',
                                         parents=[env_args])
uninstall_app_action = parser_uninstall.add_argument('app', help='ID or name of app to uninstall')
uninstall_app_action.completer = DXAppCompleter(installed=True)
parser_uninstall.set_defaults(func=uninstall)
register_subparser(parser_uninstall, categories='exec')

parser_run = subparsers.add_parser('run', help='Run an applet, app, or workflow', add_help=False,
                                   description=(fill('Run an applet, app, or workflow.  To see a list of executables you can run, hit <TAB> twice after "dx run" or run "' + BOLD() + 'dx find apps' + ENDC() + '" to see a list of available apps.') + '\n\n' + fill('If any inputs are required but not specified, an interactive mode for selecting inputs will be launched.  Inputs can be set in multiple ways.  Run "dx run --input-help" for more details.')),
                                   prog='dx run',
                                   formatter_class=argparse.RawTextHelpFormatter,
                                   parents=[stdout_args, env_args])
run_executable_action = parser_run.add_argument('executable', help=fill('Name or ID of an applet, app, or workflow to run; must be provided if --clone is not set', width_adjustment=-24), nargs="?", default="")
run_executable_action.completer = MultiCompleter([DXAppCompleter(),
                                       DXPathCompleter(classes=['applet']),
                                       DXPathCompleter(classes=['record'], typespec='pipeline')])
parser_run.add_argument('-h', '--help', help='show this help message and exit', nargs=0, action=runHelp)
parser_run.add_argument('--clone', help=fill('Job ID or name from which to use as default options (will use the exact same executable ID, destination project and folder, job input, and a similar name unless explicitly overridden by command-line arguments)', width_adjustment=-24))
parser_run.add_argument('--alias', '--version', '--tag', dest='alias',
                        help=fill('Tag or version of the app to run (default: \"default\" if an app)', width_adjustment=-24))
parser_run.add_argument('--destination', '--folder', metavar='PATH', dest='folder', help=fill('The full project:folder path in which to output the results.  By default, the current working directory will be used.', width_adjustment=-24))
parser_run.add_argument('--project', metavar='PROJECT', help=fill('Project name or ID in which to run the executable. This can also be specified together with the output folder in --destination.', width_adjustment=-24))
parser_run.add_argument('--name', help=fill('Name for the job (default is the app or applet name)', width_adjustment=-24))
parser_run.add_argument('--delay-workspace-destruction', help=fill('Whether to keep the job\'s temporary workspace around for debugging purposes for 3 days after it succeeds or fails', width_adjustment=-24), action='store_true')
parser_run.add_argument('-y', '--yes', dest='confirm', help='Do not ask for confirmation', action='store_false')
parser_run.add_argument('--wait', help='Wait until the job is done before returning', action='store_true')
parser_run.add_argument('--watch', help="Watch the job after launching it", action='store_true')
parser_run.add_argument('--input-help', help=fill('Print help and examples for how to specify inputs', width_adjustment=-24), action=runInputHelp, nargs=0)
parser_run.add_argument('-i', '--input', help=fill('An input to be added using "<input name>[:<input class>]=<input value>"', width_adjustment=-24), action='append')
parser_run.add_argument('-j', '--input-json', help=fill('Input JSON string (keys=input field names, values=input field values)', width_adjustment=-24))
parser_run.add_argument('-f', '--input-json-file', dest='filename', help=fill('Load input JSON from FILENAME ("-" to use stdin)'))
instance_type_action = parser_run.add_argument('--instance-type',
                                               help=fill('Specify instance type for all jobs this executable will run, or a JSON string mapping function names to instance types, e.g. \'{"main": "dx_m1.large", ...}\'. Available instance types:', width_adjustment=-24)
                                                    + '\n' + format_table(InstanceTypesCompleter.instance_types.values(),
                                                                          column_names=InstanceTypesCompleter.instance_types.values()[0]._fields))
instance_type_action.completer = InstanceTypesCompleter()
parser_run.set_defaults(func=run, verbose=False, help=False, details=None)
register_subparser(parser_run, categories='exec')

parser_watch = subparsers.add_parser('watch', help='Watch logs of a job and its subjobs', prog='dx watch',
                                     description='Monitors logging output from a running job',
                                     parents=[env_args, no_color_arg])
parser_watch.add_argument('jobid', help='ID of the job to watch')
# .completer = TODO
parser_watch.add_argument('-n', '--num-recent-messages', help='Number of recent messages to get', type=int, default=1024*256)
parser_watch.add_argument('--tree', help='Include the entire job tree', action='store_true')
parser_watch.add_argument('-l', '--levels', nargs='*', choices=["EMERG", "ALERT", "CRITICAL", "ERROR", "WARNING", "NOTICE", "INFO", "DEBUG", "STDERR", "STDOUT"])
parser_watch.add_argument('--get-stdout', help='Extract stdout only from this job', action='store_true')
parser_watch.add_argument('--get-stderr', help='Extract stderr only from this job', action='store_true')
parser_watch.add_argument('--get-streams', help='Extract only stdout and stderr from this job', action='store_true')
parser_watch.add_argument('--no-timestamps', help='Omit timestamps from messages', action='store_false', dest='timestamps')
parser_watch.add_argument('--job-ids', help='Print job ID in each message', action='store_true')
parser_watch.add_argument('--no-job-info', help='Omit job info and status updates', action='store_false', dest='job_info')
parser_watch.add_argument('-q', '--quiet', help='Do not print extra info messages', action='store_true')
parser_watch.add_argument('-f', '--format', help='Message format. Available fields: job, level, msg, date')
parser_watch.set_defaults(func=watch)
register_subparser(parser_watch, categories='exec')

parser_terminate = subparsers.add_parser('terminate', help='Terminate job(s)',
                                         description='Terminate a job or jobs that have not yet finished',
                                         prog='dx terminate',
                                         parents=[env_args])
parser_terminate.add_argument('jobid', help='ID of the job to terminate', nargs='+')
parser_terminate.set_defaults(func=terminate)
parser_map['terminate'] = parser_terminate
parser_categories['all']['cmds'].append((subparsers._choices_actions[-1].dest, subparsers._choices_actions[-1].help))
parser_categories['exec']['cmds'].append((subparsers._choices_actions[-1].dest, subparsers._choices_actions[-1].help))

parser_rmproject = subparsers.add_parser('rmproject', help='Delete a project',
                                         description='Delete projects and all their associated data',
                                         prog='dx rmproject',
                                         parents=[env_args])
parser_rmproject.add_argument('projects', help='Projects to remove', metavar='project', nargs='+').completer = DXPathCompleter(expected='project', include_current_proj=True)
parser_rmproject.add_argument('-y', '--yes', dest='confirm', help='Do not ask for confirmation', action='store_false')
parser_rmproject.set_defaults(func=rmproject)
register_subparser(parser_rmproject, categories='fs')

parser_new = subparsers.add_parser('new', help='Create a new project or data object',
                                   description='Use this command with one of the available subcommands (classes) to create a new project or data object from scratch.  Not all data types are supported.  See \'dx upload\' for files, \'dx build\' for applets, and \'dx import\' for importing special file formats (e.g. csv, fastq) into GenomicTables.', prog="dx new")
subparsers_new = parser_new.add_subparsers(parser_class=DXArgumentParser)
subparsers_new.metavar = 'class'
register_subparser(parser_new, categories='data')

parser_new_project = subparsers_new.add_parser('project', help='Create a new project',
                                               description='Create a new project',
                                               parents=[stdout_args, env_args],
                                               prog='dx new project')
parser_new_project.add_argument('name', help='Name of the new project', nargs='?')
parser_new_project.add_argument('-s', '--select', help='Select the new project as current after creating', action='store_true')
parser_new_project.set_defaults(func=new_project)
register_subparser(parser_new_project, subparsers_action=subparsers_new, categories='fs')

parser_new_record = subparsers_new.add_parser('record', help='Create a new record',
                                              description='Create a new record',
                                              parents=[parser_dataobject_args, parser_single_dataobject_output_args, stdout_args, env_args],
                                              formatter_class=argparse.RawTextHelpFormatter,
                                              prog='dx new record')
parser_new_record.add_argument('--init', help='Path to record from which to initialize all metadata').completer = DXPathCompleter(classes=['record'])
parser_new_record.set_defaults(func=new_record)
register_subparser(parser_new_record, subparsers_action=subparsers_new, categories='fs')

parser_new_gtable = subparsers_new.add_parser('gtable', help='Create a new gtable',
                                              description='Create a new gtable from scratch.  See \'dx import\' for importing special file formats (e.g. csv, fastq) into GenomicTables.',
                                              parents=[parser_dataobject_args, parser_single_dataobject_output_args, stdout_args, env_args],
                                              formatter_class=argparse.RawTextHelpFormatter,
                                              prog='dx new gtable')
parser_new_gtable.add_argument('--columns', help=fill('Comma-separated list of column names to use, e.g. "col1,col2,col3"; columns with non-string types can be specified using "name:type" syntax, e.g. "col1:int,col2:boolean".  If not given, the first line of the file will be used to infer column names.', width_adjustment=-24), required=True)
new_gtable_indices_args = parser_new_gtable.add_mutually_exclusive_group()
new_gtable_indices_args.add_argument('--gri', nargs=3, metavar=('CHR', 'LO', 'HI'), help=fill('Specify column names to be used as chromosome, lo, and hi columns for a genomic range index (name will be set to "gri"); will also add the type "gri"', width_adjustment=-24))
new_gtable_indices_args.add_argument('--indices', help='JSON for specifying any other indices')
parser_new_gtable.set_defaults(func=new_gtable)
#parser_new_gtable.completer = DXPathCompleter(classes=['gtable'])
register_subparser(parser_new_gtable, subparsers_action=subparsers_new, categories='fs')

parser_get_details = subparsers.add_parser('get_details', help='Get details of a data object', description='Get the JSON details of a data object.', prog="dx get_details",
                                           parents=[env_args])
parser_get_details.add_argument('path', help='ID or path to data object to get details for').completer = DXPathCompleter()
parser_get_details.set_defaults(func=get_details)
register_subparser(parser_get_details, categories='metadata')

parser_set_details = subparsers.add_parser('set_details', help='Set details on a data object', description='Set the JSON details of a data object.', prog="dx set_details",
                                           parents=[env_args, all_arg])
parser_set_details.add_argument('path', help='ID or path to data object to modify').completer = DXPathCompleter()
parser_set_details.add_argument('details', help='JSON to store as details')
parser_set_details.set_defaults(func=set_details)
register_subparser(parser_set_details, categories='metadata')

parser_set_visibility = subparsers.add_parser('set_visibility', help='Set visibility on a data object', description='Set visibility on a data object.', prog='dx set_visibility',
                                              parents=[env_args, all_arg])
parser_set_visibility.add_argument('path', help='ID or path to data object to modify').completer = DXPathCompleter()
parser_set_visibility.add_argument('visibility', choices=['hidden', 'visible'], help='Visibility that the object should have')
parser_set_visibility.set_defaults(func=set_visibility)
register_subparser(parser_set_visibility, categories='metadata')

parser_add_types = subparsers.add_parser('add_types', help='Add types to a data object', description='Add types to a data object.  See https://wiki.dnanexus.com/pages/Types/ for a list of DNAnexus types.',
                                         prog='dx add_types',
                                         parents=[env_args, all_arg])
parser_add_types.add_argument('path', help='ID or path to data object to modify').completer = DXPathCompleter()
parser_add_types.add_argument('types', nargs='+', metavar='type', help='Types to add')
parser_add_types.set_defaults(func=add_types)
register_subparser(parser_add_types, categories='metadata')

parser_remove_types = subparsers.add_parser('remove_types', help='Remove types from a data object', description='Remove types from a data object.  See https://wiki.dnanexus.com/pages/Types/ for a list of DNAnexus types.', prog='dx remove_types',
                                            parents=[env_args, all_arg])
parser_remove_types.add_argument('path', help='ID or path to data object to modify').completer = DXPathCompleter()
parser_remove_types.add_argument('types', nargs='+', metavar='type', help='Types to remove')
parser_remove_types.set_defaults(func=remove_types)
register_subparser(parser_remove_types, categories='metadata')

parser_tag = subparsers.add_parser('tag', help='Tag a data object', description='Tag a data object.  Note that a project context must be either set or specified.', prog='dx tag',
                                   parents=[env_args, all_arg])
parser_tag.add_argument('path', help='Path to data object to modify').completer = DXPathCompleter()
parser_tag.add_argument('tags', nargs='+', metavar='tag', help='Tags to add')
parser_tag.set_defaults(func=add_tags)
register_subparser(parser_tag, categories='metadata')

parser_untag = subparsers.add_parser('untag', help='Untag a data object', description='Untag a data object.  Note that a project context must be either set or specified.', prog='dx untag',
                                     parents=[env_args, all_arg])
parser_untag.add_argument('path', help='Path to data object to modify').completer = DXPathCompleter()
parser_untag.add_argument('tags', nargs='+', metavar='tag', help='Tags to remove')
parser_untag.set_defaults(func=remove_tags)
register_subparser(parser_untag, categories='metadata')

parser_rename = subparsers.add_parser('rename',
                                      help='Rename a project or data object',
                                      description='Rename a project or data object.  To rename folders, use \'dx mv\' instead.  Note that a project context must be either set or specified to rename a data object.  To specify a project or a project context, append a colon character ":" after the project ID or name.',
                                      prog='dx rename',
                                      parents=[env_args, all_arg])
parser_rename.add_argument('path', help='Path to project or data object to rename').completer = DXPathCompleter(include_current_proj=True)
parser_rename.add_argument('name', help='New name')
parser_rename.set_defaults(func=rename)
register_subparser(parser_rename, categories='metadata')

parser_set_properties = subparsers.add_parser('set_properties', help='Set properties of a data object',
                                              description='Set properties of a data object.  Note that a project context must be either set or specified.', prog='dx set_properties',
                                              parents=[env_args, all_arg])
parser_set_properties.add_argument('path', help='Path to data object to modify').completer = DXPathCompleter()
parser_set_properties.add_argument('properties', nargs='+', metavar='propertyname=value', help='Key-value pairs of property names and their new values')
parser_set_properties.set_defaults(func=set_properties)
register_subparser(parser_set_properties, categories='metadata')

parser_unset_properties = subparsers.add_parser('unset_properties', help='Unset properties of a data object',
                                                description='Unset properties of a data object.  Note that a project context must be either set or specified.',
                                                prog='dx unset_properties',
                                                parents=[env_args, all_arg])
parser_unset_properties.add_argument('path', help='Data object to modify').completer = DXPathCompleter()
parser_unset_properties.add_argument('properties', nargs='+', metavar='propertyname', help='Property names to unset')
parser_unset_properties.set_defaults(func=unset_properties)
register_subparser(parser_unset_properties, categories='metadata')

parser_close = subparsers.add_parser('close', help='Close data object(s)', description='Close a remote data object or set of objects.', prog='dx close',
                                     parents=[env_args, all_arg])
parser_close.add_argument('path', help='Path to a data object to close', nargs='+').completer = DXPathCompleter()
parser_close.add_argument('--wait', help='Wait for the object(s) to close', action='store_true')
parser_close.set_defaults(func=close)
register_subparser(parser_close, categories=('data', 'metadata'))

parser_wait = subparsers.add_parser('wait', help='Wait for data object(s) to close or job(s) to finish', description='Polls the state of specified data object(s) or job(s) until they are all in the desired state.  Waits until the "closed" state for a data object, and for any terminal state for a job ("terminated", "failed", or "done").  Exits with a non-zero code if a job reaches a terminal state that is not "done".',
                                    prog='dx wait',
                                    parents=[env_args])
parser_wait.add_argument('path', help='Path to a data object or job ID to wait for', nargs='+').completer = DXPathCompleter()
parser_wait.set_defaults(func=wait)
register_subparser(parser_wait, categories=('data', 'metadata', 'exec'))

parser_get = subparsers.add_parser('get', help='Download records, applets, and apps',
                                   description='Download the contents of some types of data (records, applets, and files).  For gtables, see "dx export".  Downloading an applet will only download the source.  (Any bundled dependencies must be downloaded separately.)  Use "-o -" to direct the output to stdout.',
                                   prog='dx get',
                                   parents=[env_args])
parser_get.add_argument('path', help='Data object ID or name to access').completer = DXPathCompleter(classes=['file', 'record', 'applet'])
parser_get.add_argument('-o', '--output', help='local filename to be saved ("-" indicates stdout output); if not supplied, the object\'s name on the platform will be used, along with any applicable extensions')
parser_get.add_argument('--no-ext', help='If -o is not provided, do not add an extension to the filename', action='store_true')
parser_get.add_argument('-f', '--overwrite', help='Overwrite the local file if necessary', action='store_true')
parser_get.set_defaults(func=get)
register_subparser(parser_get, categories='data')

parser_find = subparsers.add_parser('find', help='Search functionality over various DNAnexus entities',
                                    description='Search functionality over various DNAnexus entities.',
                                    prog='dx find')
subparsers_find = parser_find.add_subparsers(parser_class=DXArgumentParser)
subparsers_find.metavar = 'category'
register_subparser(parser_find, categories=())

parser_find_apps = subparsers_find.add_parser('apps', help='List available apps',
                                              description='Finds apps with the given search parameters.  Use --category to restrict by a category.  Common categories include: "Alignment", "Annotation", "Debugging", "Export", "Import", "RNA-Seq", "Reports", "Statistics", "Variation calling".',
                                              parents=[stdout_args, json_arg, delim_arg, env_args],
                                              prog='dx find apps')
parser_find_apps.add_argument('--name', help='Name of the app')
parser_find_apps.add_argument('--category', help='Category of the app')
parser_find_apps.add_argument('-a', '--all', help='Whether to return all versions of the app', action='store_true')
parser_find_apps.add_argument('--unpublished', help='Whether to return unpublished apps as well', action='store_true')
parser_find_apps.add_argument('--installed', help='Whether to restrict the list to installed apps only', action='store_true')
parser_find_apps.add_argument('--billed-to', help='User or organization responsible for the app')
parser_find_apps.add_argument('--creator', help='Creator of the app version')
parser_find_apps.add_argument('--developer', help='Developer of the app')
parser_find_apps.add_argument('--created-after', help='Integer timestamp after which the app version was created (negative number means ms in the past, or use suffix s, m, h, d, w, M, y)')
parser_find_apps.add_argument('--created-before', help='Integer timestamp before which the app version was created (negative number means ms in the past, or use suffix s, m, h, d, w, M, y)')
parser_find_apps.add_argument('--mod-after', help='Integer timestamp after which the app was last modified (negative number means ms in the past, or use suffix s, m, h, d, w, M, y)')
parser_find_apps.add_argument('--mod-before', help='Integer timestamp before which the app was last modified (negative number means ms in the past, or use suffix s, m, h, d, w, M, y)')
parser_find_apps.set_defaults(func=find_apps)
register_subparser(parser_find_apps, subparsers_action=subparsers_find, categories='exec')

parser_find_jobs = subparsers_find.add_parser('jobs', help='List jobs in your project', description=fill('Finds jobs with the given search parameters.  By default, output is formatted to show the last several job trees that you\'ve run in the current project.') + '''

EXAMPLES

  ''' + fill('The following will show the full job tree containing the job ID given (it does not have to be the origin job).', subsequent_indent='  ') + '''

  $ dx find jobs --id job-B13f83KgpqG0PB8P0xkQ000X

  ''' + fill('The following will find all jobs that start with the string "bwa"', subsequent_indent='  ') + '''

  $ dx find jobs --name bwa*
''',
                                              parents=[stdout_args, json_arg, no_color_arg, delim_arg, env_args],
                                              formatter_class=argparse.RawTextHelpFormatter,
                                              prog='dx find jobs')
parser_find_jobs.add_argument('--id', help=fill('Show only the job tree or job containing this job ID', width_adjustment=-24))
parser_find_jobs.add_argument('--name', help=fill('Restrict the search by job name (accepts wildcards "*" and "?")', width_adjustment=-24))
parser_find_jobs.add_argument('--user', help=fill('Username who launched the job (use "self" to ask for your own jobs)', width_adjustment=-24))
parser_find_jobs.add_argument('--project', help=fill('Project context (output project), default is current project if set', width_adjustment=-24))
parser_find_jobs.add_argument('--all-projects', '--allprojects', help=fill('Extend search to all projects', width_adjustment=-24), action='store_true')
parser_find_jobs.add_argument('--app', '--applet', '--executable', dest='executable', help=fill('Applet or App ID that job is running', width_adjustment=-24))
parser_find_jobs.add_argument('--state', help=fill('State of the job, e.g. \"done\", \"failed\"', width_adjustment=-24))
parser_find_jobs.add_argument('--origin', help=fill('Job ID of the top-level (user-initiated) job', width_adjustment=-24)) # Redundant but might as well
parser_find_jobs.add_argument('--parent', help=fill('Job ID of the parent job; implies --all-jobs', width_adjustment=-24))
parser_find_jobs.add_argument('--created-after', help=fill('Integer timestamp after which the job was last created (negative number means ms in the past, or use suffix s, m, h, d, w, M, y)', width_adjustment=-24))
parser_find_jobs.add_argument('--created-before', help=fill('Integer timestamp before which the job was last created (negative number means ms in the past, or use suffix s, m, h, d, w, M, y)', width_adjustment=-24))
parser_find_jobs.add_argument('-n', '--num-results', metavar='N', type=int, help=fill('Max number of results (trees or jobs, as according to the search mode) to return (default 10)', width_adjustment=-24), default=10)
parser_find_jobs.add_argument('-o', '--show-outputs', help=fill('Show job outputs in results', width_adjustment=-24), action='store_true')
parser_find_jobs_search_gp = parser_find_jobs.add_argument_group('Search mode')
parser_find_jobs_search = parser_find_jobs_search_gp.add_mutually_exclusive_group()
parser_find_jobs_search.add_argument('--trees', help=fill('Show entire job trees for all matching results (default)', width_adjustment=-24), action='store_true')
parser_find_jobs_search.add_argument('--origin-jobs', help=fill('Search and display only top-level origin jobs', width_adjustment=-24), action='store_true')
parser_find_jobs_search.add_argument('--all-jobs', help=fill('Search for jobs at all depths matching the query (no tree structure shown)', width_adjustment=-24), action='store_true')
parser_find_jobs.set_defaults(func=find_jobs)
parser_find_jobs.completer = DXPathCompleter(expected='project')
register_subparser(parser_find_jobs, subparsers_action=subparsers_find, categories='exec')

parser_find_data = subparsers_find.add_parser('data', help='Find data objects',
                                              description='Finds data objects with the given search parameters.  By default, restricts the search to the current project if set.  To search over all projects (excludes public projects), use --all-projects (overrides --project, --folder, --norecurse).',
                                              parents=[stdout_args, json_arg, no_color_arg, delim_arg, env_args], prog='dx find data')
parser_find_data.add_argument('--class', dest='classname', choices=['record', 'file', 'gtable', 'applet'], help='Data object class')
parser_find_data.add_argument('--state', choices=['open', 'closing', 'closed', 'any'], help='State of the object')
parser_find_data.add_argument('--visibility', choices=['hidden', 'visible', 'either'], default='visible', help='Whether the object is hidden or not')
parser_find_data.add_argument('--name', help='Name of the object')
parser_find_data.add_argument('--property', dest='properties', metavar='KEY=VALUE', help='Key-value pair of a property; repeat as necessary, e.g. "--property key1=val1 --property key2=val2"', action='append')
parser_find_data.add_argument('--type', help='Type of the data object')
parser_find_data.add_argument('--tag', help='Tag of the data object')
parser_find_data.add_argument('--link', help='Object ID that the data object links to')
parser_find_data.add_argument('--all-projects', '--allprojects', help='Extend search to all projects (excluding public projects)', action='store_true')
parser_find_data.add_argument('--project', help='Project with which to restrict the results')
parser_find_data.add_argument('--folder', help='Folder path with which to restrict the results (\'--project\' must be used in this case)').completer = DXPathCompleter(expected='folder')
parser_find_data.add_argument('--norecurse', dest='recurse', help='Do not recurse into subfolders', action='store_false')
parser_find_data.add_argument('--mod-after', help='Integer timestamp after which the object was last modified (negative number means ms in the past, or use suffix s, m, h, d, w, M, y)')
parser_find_data.add_argument('--mod-before', help='Integer timestamp before which the object was last modified (negative number means ms in the past, or use suffix s, m, h, d, w, M, y)')
parser_find_data.add_argument('--created-after', help='Integer timestamp after which the object was created (negative number means ms in the past, or use suffix s, m, h, d, w, M, y)')
parser_find_data.add_argument('--created-before', help='Integer timestamp before which the object was created (negative number means ms in the past, or use suffix s, m, h, d, w, M, y)')
parser_find_data.set_defaults(func=find_data)
register_subparser(parser_find_data, subparsers_action=subparsers_find, categories=('data', 'metadata'))

parser_find_projects = subparsers_find.add_parser('projects', help='Find projects',
                                                  description='Finds projects with the given search parameters.  Use the --public flag to list all public projects.',
                                                  parents=[stdout_args, json_arg, delim_arg, env_args],
                                                  prog='dx find projects')
parser_find_projects.add_argument('--name', help='Name of the project')
parser_find_projects.add_argument('--level', choices=['LIST', 'VIEW', 'CONTRIBUTE', 'ADMINISTER'], help='Minimum level of permissions expected')
parser_find_projects.add_argument('--public', help='Include ONLY public projects (will automatically set --level to VIEW)', action='store_true')
parser_find_projects.set_defaults(func=find_projects)
register_subparser(parser_find_projects, subparsers_action=subparsers_find, categories='data')

parser_api = subparsers.add_parser('api', help='Call an API method',
                                   formatter_class=argparse.RawTextHelpFormatter,
                                   description=fill('Call an API method directly.  The JSON response from the API server will be returned if successful.  No name resolution is performed; DNAnexus IDs must always be provided.  The API specification can be found at') + '''

https://wiki.dnanexus.com/API-Specification-v1.0.0/Introduction

EXAMPLE

  In the following example, a project's description is changed.

  $ dx api project-B0VK6F6gpqG6z7JGkbqQ000Q update '{"description": "desc"}'
  {
      "id": "project-B0VK6F6gpqG6z7JGkbqQ000Q"
  }

''',
                                   prog='dx api',
                                   parents=[env_args])
parser_api.add_argument('resource', help=fill('One of \"system\", a class name (e.g. \"record\"), or an entity ID such as \"record-xxxx\".  Use "app-name/1.0.0" to refer to version "1.0.0" of the app named "name".', width_adjustment=-17))
parser_api.add_argument('method', help=fill('Method name for the resource as documented by the API specification', width_adjustment=-17))
parser_api.add_argument('input_json', nargs='?', default="{}", help='JSON input for the method (if not given, \"{}\" is used)')
parser_api.add_argument('--input', help=fill('Load JSON input from FILENAME ("-" to use stdin)', width_adjustment=-17))
parser_api.set_defaults(func=api)
# parser_api.completer = TODO
register_subparser(parser_api)

parser_upgrade = subparsers.add_parser('upgrade', help='Upgrade dx-toolkit (the DNAnexus SDK and this program)', description='Upgrades dx-toolkit (the DNAnexus SDK and this program) to the latest recommended version, or to a specified version and platform.')
parser_upgrade.add_argument('args', nargs='*')
parser_upgrade.set_defaults(func=upgrade)
register_subparser(parser_upgrade)


parser_make_download_url = subparsers.add_parser('make_download_url', help='Create a file download link for sharing', description='Creates a pre-authenticated link that can be used to download a file without logging in.')
parser_make_download_url.add_argument('path', help='Data object ID or name to access').completer = DXPathCompleter(classes=['file'])
parser_make_download_url.add_argument('--duration', help='Time for which the URL will remain valid (in seconds, or use suffix s, m, h, d, w, M, y). Default: 1 day')
parser_make_download_url.add_argument('--filename', help='Name that the server will instruct the client to save the file as')
parser_make_download_url.set_defaults(func=make_download_url)
register_subparser(parser_make_download_url)

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
#                                         DXPathCompleter(classes=['applet']),
#                                         DXPathCompleter(classes=['record'], typespec='pipeline')])
parser_map['help'] = parser_help # TODO: a special help completer
parser_map['help run'] = parser_help
for category in parser_categories:
    parser_categories[category]['cmds'].append(('help', subparsers._choices_actions[-1].help))
parser_categories['all']['cmds'].sort()

def main():
    # Bash argument completer hook
    if '_ARGCOMPLETE' in os.environ:
        import argcomplete
        argcomplete.autocomplete(parser, always_complete_options=False, output_stream=sys.stdout if '_DX_ARC_DEBUG' in os.environ else None)

    if len(args_list) > 0:
        args = parser.parse_args(args_list)
        dxpy.USER_AGENT += " {prog}-{command}".format(prog=parser.prog, command=getattr(args, 'command', ''))
        set_cli_colors(args)
        set_delim(args)
        set_env_from_args(args)
        args.func(args)
        # Flush buffered data in stdout before interpreter shutdown to ignore broken pipes
        try:
            sys.stdout.flush()
        except IOError as e:
            if e.errno == errno.EPIPE:
                if dxpy._DEBUG:
                    print >>sys.stderr, "Broken pipe"
            else:
                raise
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == '__main__':
    main()
