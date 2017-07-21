#!/usr/bin/env python
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

from __future__ import (print_function, unicode_literals)

import sys, os
import json
import argparse
import re
from collections import OrderedDict
import dxpy
from dxpy.templating.utils import (print_intro, get_name, get_version, get_metadata, Completer, get_ordinal_str,
                                   prompt_for_var, prompt_for_yn, use_completer, get_language, language_options,
                                   get_pattern, get_timeout, fill_in_name_and_ver, clean, create_files_from_templates)
from dxpy.utils.printing import fill, BOLD, UNDERLINE, DNANEXUS_LOGO, ENDC
from dxpy.app_categories import APP_CATEGORIES
from dxpy.utils.completer import InstanceTypesCompleter
from dxpy.utils.pretty_print import format_table
from dxpy.compat import wrap_stdio_in_codecs
wrap_stdio_in_codecs()

try:
    import colorama
    colorama.init()
except:
    pass

IO_NAME_PATTERN = re.compile('^[a-zA-Z_][0-9a-zA-Z_]*$')

API_VERSION = '1.0.0'

parser = argparse.ArgumentParser(description="Create a source code directory for a DNAnexus app.  You will be prompted for various metadata for the app as well as for its input and output specifications.")
parser.add_argument('--json-file', help='Use the metadata and IO spec found in the given file')
parser.add_argument('--language', help='Programming language of your app')
parser.add_argument('--template',
                    choices=["basic", "parallelized", "scatter-process-gather"], default='basic',
                    help='Execution pattern of your app')
parser.add_argument('name', help='Name of your app', nargs='?')
args = parser.parse_args()

if args.json_file is not None and not os.path.exists(args.json_file):
    parser.error('File not found: ' + args.json_file)

def main(**kwargs):
    """
    Entry point for dx-app-wizard.
    Note that this function is not meant to be used as a subroutine in your program.
    """
    manifest = []

    print_intro(API_VERSION)

    if args.json_file is not None:
        with open(args.json_file, 'r') as json_file:
            app_json = json.loads(json_file.read())
            # Re-confirm the name
            name = get_name(default=args.name or app_json.get('name'))
            app_json['name'] = name
            version = get_version(default=app_json.get('version'))
            app_json['version'] = version
        try:
            os.mkdir(app_json['name'])
        except:
            sys.stderr.write(fill('''Unable to create a directory for %s, please check that it is a valid app name and the working directory exists and is writable.''' % app_json['name']) + '\n')
            sys.exit(1)
    else:
        ##################
        # BASIC METADATA #
        ##################

        name = get_name(default=args.name)

        try:
            os.mkdir(name)
        except:
            sys.stderr.write(fill('''Unable to create a directory for %s, please check that it is a valid app name and the working directory exists and is writable.''' % name) + '\n')
            sys.exit(1)

        title, summary = get_metadata(API_VERSION)

        version = get_version()

        app_json = OrderedDict()
        app_json["name"] = name

        app_json["title"] = title or name
        app_json['summary'] = summary or name

        app_json["dxapi"] = API_VERSION
        app_json["version"] = version

        ############
        # IO SPECS #
        ############

        class_completer = Completer(['int', 'float', 'string', 'boolean', 'hash',
                                     'array:int', 'array:float', 'array:string', 'array:boolean',
                                     'record', 'file', 'applet',
                                     'array:record', 'array:file', 'array:applet'])
        bool_completer = Completer(['true', 'false'])

        print('')
        print(BOLD() + 'Input Specification' + ENDC())
        print('')

        input_spec = True
        input_names = []
        printed_classes = False

        if input_spec:
            app_json['inputSpec'] = []
            print(fill('You will now be prompted for each input parameter to your app.  Each parameter should have a unique name that uses only the underscore "_" and alphanumeric characters, and does not start with a number.'))

            while True:
                print('')
                ordinal = get_ordinal_str(len(app_json['inputSpec']) + 1)
                input_name = prompt_for_var(ordinal + ' input name (<ENTER> to finish)', allow_empty=True)
                if input_name == '':
                    break
                if input_name in input_names:
                    print(fill('Error: Cannot use the same input parameter name twice.  Please choose again.'))
                    continue
                if not IO_NAME_PATTERN.match(input_name):
                    print(fill('Error: Parameter names may use only underscore "_", ASCII letters, and digits; and may not start with a digit.  Please choose again.'))
                    continue
                input_names.append(input_name)

                input_label = prompt_for_var('Label (optional human-readable name)', '')

                use_completer(class_completer)
                if not printed_classes:
                    print('Your input parameter must be of one of the following classes:')
                    print('''applet         array:file     array:record   file           int
array:applet   array:float    array:string   float          record
array:boolean  array:int      boolean        hash           string
''')
                    printed_classes = True

                while True:
                    input_class = prompt_for_var('Choose a class (<TAB> twice for choices)')
                    if input_class in class_completer.choices:
                        break
                    else:
                        print(fill('Not a recognized class; please choose again.'))

                use_completer()

                optional = prompt_for_yn('This is an optional parameter')

                default_val = None
                if optional and input_class in ['int', 'float', 'string', 'boolean']:
                    default_val = prompt_for_yn('A default value should be provided')
                    if default_val:
                        while True:
                            if input_class == 'boolean':
                                use_completer(bool_completer)
                                default_val = prompt_for_var('  Default value', choices=['true', 'false'])
                                use_completer()
                            elif input_class == 'string':
                                default_val = prompt_for_var('  Default value', allow_empty=True)
                            else:
                                default_val = prompt_for_var('  Default value')

                            try:
                                if input_class == 'boolean':
                                    default_val = (default_val == 'true')
                                elif input_class == 'int':
                                    default_val = int(default_val)
                                elif input_class == 'float':
                                    default_val = float(default_val)
                                break
                            except:
                                print('Not a valid default value for the given class ' + input_class)
                    else:
                        default_val = None

                # Fill in the input parameter's JSON
                parameter_json = OrderedDict()

                parameter_json["name"] = input_name
                if input_label != '':
                    parameter_json['label'] = input_label
                parameter_json["class"] = input_class
                parameter_json["optional"] = optional
                if default_val is not None:
                    parameter_json['default'] = default_val

                # Fill in patterns and blank help string
                if input_class == 'file' or input_class == 'array:file':
                    parameter_json["patterns"] = ["*"]
                parameter_json["help"] = ""

                app_json['inputSpec'].append(parameter_json)

        print('')
        print(BOLD() + 'Output Specification' + ENDC())
        print('')

        output_spec = True
        output_names = []
        if output_spec:
            app_json['outputSpec'] = []
            print(fill('You will now be prompted for each output parameter of your app.  Each parameter should have a unique name that uses only the underscore "_" and alphanumeric characters, and does not start with a number.'))

            while True:
                print('')
                ordinal = get_ordinal_str(len(app_json['outputSpec']) + 1)
                output_name = prompt_for_var(ordinal + ' output name (<ENTER> to finish)', allow_empty=True)
                if output_name == '':
                    break
                if output_name in output_names:
                    print(fill('Error: Cannot use the same output parameter name twice.  Please choose again.'))
                    continue
                if not IO_NAME_PATTERN.match(output_name):
                    print(fill('Error: Parameter names may use only underscore "_", ASCII letters, and digits; and may not start with a digit.  Please choose again.'))
                    continue
                output_names.append(output_name)

                output_label = prompt_for_var('Label (optional human-readable name)', '')

                use_completer(class_completer)
                if not printed_classes:
                    print('Your output parameter must be of one of the following classes:')
                    print('''applet         array:file     array:record   file           int
array:applet   array:float    array:string   float          record
array:boolean  array:int      boolean        hash           string''')
                    printed_classes = True
                while True:
                    output_class = prompt_for_var('Choose a class (<TAB> twice for choices)')
                    if output_class in class_completer.choices:
                        break
                    else:
                        print(fill('Not a recognized class; please choose again.'))

                use_completer()

                # Fill in the output parameter's JSON
                parameter_json = OrderedDict()
                parameter_json["name"] = output_name
                if output_label != '':
                    parameter_json['label'] = output_label
                parameter_json["class"] = output_class

                # Fill in patterns and blank help string
                if output_class == 'file' or output_class == 'array:file':
                    parameter_json["patterns"] = ["*"]
                parameter_json["help"] = ""

                app_json['outputSpec'].append(parameter_json)

    required_file_input_names = []
    optional_file_input_names = []
    required_file_array_input_names = []
    optional_file_array_input_names = []
    file_output_names = []

    if 'inputSpec' in app_json:
        for param in app_json['inputSpec']:
            may_be_missing = param['optional'] and "default" not in param
            if param['class'] == 'file':
                param_list = optional_file_input_names if may_be_missing else required_file_input_names
            elif param['class'] == 'array:file':
                param_list = optional_file_array_input_names if may_be_missing else required_file_array_input_names
            else:
                param_list = None
            if param_list is not None:
                param_list.append(param['name'])

    if 'outputSpec' in app_json:
        file_output_names = [param['name'] for param in app_json['outputSpec'] if param['class'] == 'file']

    ##################
    # TIMEOUT POLICY #
    ##################

    print('')
    print(BOLD() + 'Timeout Policy' + ENDC())
    app_json["runSpec"] = OrderedDict({})
    app_json['runSpec'].setdefault('timeoutPolicy', {})

    timeout, timeout_units = get_timeout(default=app_json['runSpec']['timeoutPolicy'].get('*'))

    app_json['runSpec']['timeoutPolicy'].setdefault('*', {})
    app_json['runSpec']['timeoutPolicy']['*'].setdefault(timeout_units, timeout)

    ########################
    # LANGUAGE AND PATTERN #
    ########################

    print('')
    print(BOLD() + 'Template Options' + ENDC())

    # Prompt for programming language if not specified

    language = args.language if args.language is not None else get_language()

    interpreter = language_options[language].get_interpreter()
    app_json["runSpec"]["interpreter"] = interpreter

    # Prompt the execution pattern iff the args.pattern is provided and invalid

    template_dir = os.path.join(os.path.dirname(dxpy.__file__), 'templating', 'templates', language_options[language].get_path())
    if not os.path.isdir(os.path.join(template_dir, args.template)):
        print(fill('The execution pattern "' + args.template + '" is not available for your programming language.'))
        pattern = get_pattern(template_dir)
    else:
        pattern = args.template
    template_dir = os.path.join(template_dir, pattern)

    with open(os.path.join(template_dir, 'dxapp.json'), 'r') as template_app_json_file:
        file_text = fill_in_name_and_ver(template_app_json_file.read(), name, version)
        template_app_json = json.loads(file_text)
        for key in template_app_json['runSpec']:
            app_json['runSpec'][key] = template_app_json['runSpec'][key]

    if (language == args.language) and (pattern == args.template):
        print('All template options are supplied in the arguments.')

    ##########################
    # APP ACCESS PERMISSIONS #
    ##########################

    print('')
    print(BOLD('Access Permissions'))
    print(fill('''If you request these extra permissions for your app, users will see this fact when launching your app, and certain other restrictions will apply. For more information, see ''' +
    BOLD('https://wiki.dnanexus.com/App-Permissions') + '.'))

    print('')
    print(fill(UNDERLINE('Access to the Internet') + ' (other than accessing the DNAnexus API).'))
    if prompt_for_yn("Will this app need access to the Internet?", default=False):
        app_json.setdefault('access', {})
        app_json['access']['network'] = ['*']
        print(fill('App has full access to the Internet. To narrow access to specific sites, edit the ' +
                   UNDERLINE('access.network') + ' field of dxapp.json once we generate the app.'))

    print('')
    print(fill(UNDERLINE('Direct access to the parent project') + '''. This is not needed if your app specifies outputs,
    which will be copied into the project after it's done running.'''))
    if prompt_for_yn("Will this app need access to the parent project?", default=False):
        app_json.setdefault('access', {})
        app_json['access']['project'] = 'CONTRIBUTE'
        print(fill('App has CONTRIBUTE access to the parent project. To change the access level or request access to ' +
                   'other projects, edit the ' + UNDERLINE('access.project') + ' and ' + UNDERLINE('access.allProjects') +
                   ' fields of dxapp.json once we generate the app.'))

    #######################
    # SYSTEM REQUIREMENTS #
    #######################

    print('')
    print(BOLD('System Requirements'))
    print('')
    print(BOLD('Common instance types:'))
    print(format_table(InstanceTypesCompleter.preferred_instance_types.values(),
                       column_names=list(InstanceTypesCompleter.instance_types.values())[0]._fields))
    print(fill(BOLD('Default instance type:') + ' The instance type you select here will apply to all entry points in ' +
               'your app unless you override it. See ' +
               BOLD('https://wiki.dnanexus.com/API-Specification-v1.0.0/Instance-Types') + ' for more information.'))
    use_completer(InstanceTypesCompleter())
    instance_type = prompt_for_var('Choose an instance type for your app',
                                   default=InstanceTypesCompleter.default_instance_type.Name,
                                   choices=list(InstanceTypesCompleter.instance_types))
    app_json['runSpec'].setdefault('systemRequirements', {})
    app_json['runSpec']['systemRequirements'].setdefault('*', {})
    app_json['runSpec']['systemRequirements']['*']['instanceType'] = instance_type

    ######################
    # HARDCODED DEFAULTS #
    ######################

    # Default of no other authorizedUsers
    # app_json['authorizedUsers'] = []

    # print('\n' + BOLD('Linux version: '))
    app_json['runSpec']['distribution'] = 'Ubuntu'

    if any(instance_type.startswith(prefix) for prefix in ('mem1_hdd2', 'mem2_hdd2', 'mem3_hdd2')):
        print(fill('Your app will run on Ubuntu 12.04. To use Ubuntu 14.04, select from the list of common instance ' +
                   'types above.'))
        app_json['runSpec']['release'] = '12.04'
    else:
        app_json['runSpec']['release'] = '14.04'
        print(fill('Your app has been configured to run on Ubuntu 14.04. To use Ubuntu 12.04, edit the ' +
                   BOLD('runSpec.release') + ' field of your dxapp.json.'))

    #################
    # WRITING FILES #
    #################

    print('')
    print(BOLD() + '*** Generating ' + DNANEXUS_LOGO() + BOLD() + ' App Template... ***' + ENDC())

    with open(os.path.join(name, 'dxapp.json'), 'w') as prog_file:
        prog_file.write(clean(json.dumps(app_json, indent=2)) + '\n')
    manifest.append(os.path.join(name, 'dxapp.json'))

    print('')
    print(fill('''Your app specification has been written to the
dxapp.json file. You can specify more app options by editing this file
directly (see https://wiki.dnanexus.com/Developer-Portal for complete
documentation).''' + ('''  Note that without an input and output specification,
your app can only be built as an APPLET on the system.  To publish it to
the DNAnexus community, you must first specify your inputs and outputs.
''' if not ('inputSpec' in app_json and 'outputSpec' in app_json) else "")))
    print('')

    for subdir in 'src', 'test', 'resources':
        try:
            os.mkdir(os.path.join(name, subdir))
            manifest.append(os.path.join(name, subdir, ''))
        except:
            sys.stderr.write("Unable to create subdirectory %s/%s" % (name, subdir))
            sys.exit(1)

    entry_points = ['main']

    if pattern == 'parallelized':
        entry_points = ['main', 'process', 'postprocess']
    elif pattern == 'scatter-process-gather':
        entry_points = ['main', 'scatter', 'map', 'process', 'postprocess']

    manifest += create_files_from_templates(template_dir, app_json, language,
                                            required_file_input_names, optional_file_input_names,
                                            required_file_array_input_names, optional_file_array_input_names,
                                            file_output_names, pattern,
                                            description='<!-- Insert a description of your app here -->',
                                            entry_points=entry_points)

    print("Created files:")
    for filename in sorted(manifest):
        print("\t", filename)
    print("\n" + fill('''App directory created!  See
https://wiki.dnanexus.com/Developer-Portal for tutorials on how to modify these files,
or run "dx build {n}" or "dx build --create-app {n}" while logged in with dx.'''.format(n=name)) + "\n")
    print(fill('''Running the DNAnexus build utility will create an executable on the DNAnexus platform.  Any files found in the ''' +
            BOLD() + 'resources' + ENDC() +
            ''' directory will be uploaded so that they will be present in the root directory when the executable is run.'''))

if __name__ == '__main__':
    main()
