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
Miscellaneous utility classes and functions for the dx-app-wizard command-line tool
'''

from __future__ import print_function, unicode_literals, division, absolute_import

import os, sys, shutil, subprocess, re, json, platform
import stat

from ..utils.printing import (BOLD, DNANEXUS_LOGO, ENDC, fill)
from ..cli import prompt_for_yn
from ..compat import input, open
# Import pyreadline3 on Windows with Python >= 3.5
if platform.system() == 'Windows' and  sys.version_info >= (3, 5):
    import pyreadline3 as readline
else:
    try:
        # Import gnureadline if installed for macOS
        import gnureadline as readline
    except ImportError as e:
        import readline

from . import python
from . import bash

language_options = {
    "Python": python,
    "bash": bash
}

completer_state = {
    "available": False
}

try:
    import rlcompleter
    readline.parse_and_bind("tab: complete")
    readline.set_completer_delims("")
    completer_state['available'] = True
except ImportError:
    print('NOTE: readline module not available.  Install for tab-completion.')

class Completer():
    def __init__(self, choices):
        self.matches = None
        self.choices = choices

    def complete(self, text, state):
        if state == 0:
            self.matches = filter(lambda choice: choice.startswith(text),
                                  self.choices)

        if self.matches is not None and state < len(self.matches):
            return self.matches[state]
        else:
            return None

def clean(s):
    return "\n".join(line.rstrip() for line in s.split("\n"))

def use_completer(completer=None):
    if completer_state['available'] and completer is not None:
        readline.set_completer(completer.complete)

# Expect default to be a default string value
# Expect choices to be a list of strings
def prompt_for_var(prompt_str, default=None, allow_empty=False, choices=None):
    prompt = prompt_str
    if default is not None:
        prompt += ' [' + default + ']: '
    else:
        prompt += ': '
    while True:
        try:
            value = input(prompt)
        except KeyboardInterrupt:
            print('')
            exit(1)
        except EOFError:
            print('')
            exit(1)
        if value != '':
            if choices is not None and value not in choices:
                print('Error: unrecognized response, expected one of ' + json.dumps(choices))
            else:
                return value
        elif default is not None:
            return default
        elif allow_empty:
            return value

def print_intro(api_version):
    print(DNANEXUS_LOGO() + ' App Wizard, API v' + api_version)
    print('')

    print(BOLD() + 'Basic Metadata' + ENDC())
    print('')
    print(fill('''Please enter basic metadata fields that will be used to
describe your app.  Optional fields are denoted by options with square
brackets.  At the end of this wizard, the files necessary for building your
app will be generated from the answers you provide.'''))
    print('')

def get_name(default=None):
    print(fill('The ' + BOLD() + 'name' + ENDC() + ' of your app must be unique on the DNAnexus platform.  After creating your app for the first time, you will be able to publish new versions using the same app name.  App names are restricted to alphanumeric characters (a-z, A-Z, 0-9), and the characters ".", "_", and "-".'))
    name_pattern = re.compile('^[a-zA-Z0-9._-]+$')
    while True:
        name = prompt_for_var('App Name', default)
        if name_pattern.match(name) is None:
            print(fill('The name of your app must match /^[a-zA-Z0-9._-]+$/'))
        else:
            if os.path.exists(name):
                if os.path.isdir(name):
                    remove_dir = prompt_for_yn('The directory %s already exists.  Would you like to remove all of its contents and create a new directory in its place?' % name)
                    if remove_dir:
                        shutil.rmtree(name)
                        print(fill('Replacing all contents of directory %s...' % name))
                    else:
                        print('')
                        continue
                else:
                    print(fill('A file named %s already exists.  Please choose another name or rename your file' % name))
                    continue
            break
    return name

def get_metadata(api_version):
    print('')
    print(fill('The ' + BOLD() + 'title' + ENDC() + ', if provided, is what is shown as the name of your app on the website.  It can be any valid UTF-8 string.'))
    title = prompt_for_var('Title', '')

    print('')
    print(fill('The ' + BOLD() + 'summary' + ENDC() + ' of your app is a short phrase or one-line description of what your app does.  It can be any UTF-8 human-readable string.'))
    summary = prompt_for_var('Summary', '')

    return title, summary

def get_version(default=None):
    if default is None:
        default = '0.0.1'
    print('')
    print(fill('You can publish multiple versions of your app, and the ' + BOLD() + 'version' + ENDC() + ' of your app is a string with which to tag a particular version.  We encourage the use of Semantic Versioning for labeling your apps (see http://semver.org/ for more details).'))
    version = prompt_for_var('Version', default)
    return version

def get_timeout(default=None):
    # Max timeout is 30 days
    max_timeout = {'m': 30 * 24 * 60, 'h': 30 * 24, 'd': 30}
    units = {'m': 'minutes', 'h': 'hours', 'd': 'days'}
    time_pattern = re.compile('^[1-9]\d*[mhd]$')

    def timeout_dict_to_str(d):
        # Used to convert app_json inputs:
        # {'hours': 48} -> '48h'
        return str(d.values()[0]) + d.keys()[0][0]

    if default is None:
        default = '48h'
    else:
        default = timeout_dict_to_str(default)
    print('')
    print(fill('Set a ' + BOLD() + 'timeout policy' + ENDC() + ' for your app. Any single entry point of the app that runs longer than the specified timeout will fail with a TimeoutExceeded error. Enter an int greater than 0 with a single-letter suffix (m=minutes, h=hours, d=days) (e.g. "48h").'))
    while True:
        timeout = prompt_for_var('Timeout policy', default)
        if not time_pattern.match(timeout):
            print(fill('Error: enter an int with a single-letter suffix (m=minutes, h=hours, d=days)'))
        elif int(timeout[:-1]) > max_timeout[timeout[-1]]:
            print(fill('Error: max allowed timeout is 30 days'))
        else:
            break
    return int(timeout[:-1]), units[timeout[-1]]

def get_ordinal_str(num):
    return str(num) + ('th' if 11 <= num % 100 <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(num % 10, 'th'))

def get_language():
    #language_choices = language_options.keys()
    language_choices = ["Python", "bash"]
    use_completer(Completer(language_choices))
    print('')
    print(fill('You can write your app in any ' + BOLD() + 'programming language' + ENDC() + ', but we provide templates for the following supported languages' + ENDC() + ": " + ', '.join(language_choices)))
    language = prompt_for_var('Programming language', choices=language_choices)
    use_completer()
    return language

def get_pattern(template_dir):
    pattern_choices = []
    print('')
    print(fill('The following common ' + BOLD() + 'execution patterns' + ENDC() + ' are currently available for your programming language:'))

    pattern_choices.append('basic')
    print(' ' + BOLD() + 'basic' + ENDC())
    print(fill('Your app will run on a single machine from beginning to end.', initial_indent='   ', subsequent_indent='   '))

    if os.path.isdir(os.path.join(template_dir, 'parallelized')):
        pattern_choices.append('parallelized')
        print(' ' + BOLD() + 'parallelized' + ENDC())
        print(fill('Your app will subdivide a large chunk of work into multiple pieces that can be processed in parallel and independently of each other, followed by a final stage that will merge and process the results as necessary.', initial_indent='   ', subsequent_indent='   '))

    if os.path.isdir(os.path.join(template_dir, 'scatter-process-gather')):
        pattern_choices.append('scatter-process-gather')
        print(' ' + BOLD() + 'scatter-process-gather' + ENDC())
        print(fill('Similar to ' + BOLD() + 'parallelized' + ENDC() + ' but with the addition of a "scatter" entry point.  This allows you to break out the execution for splitting up the input, or you can call a separate app/applet to perform the splitting.',
                   initial_indent='   ',
                   subsequent_indent='   '))

    if len(pattern_choices) == 1:
        print('Automatically using the execution pattern "basic"')
        return 'basic'

    use_completer(Completer(pattern_choices))
    pattern = prompt_for_var('Execution pattern', 'basic', choices=pattern_choices)
    use_completer()
    return pattern

def fill_in_name_and_ver(template_string, name, version):
    '''
    TODO: Rename this?
    '''
    return template_string.replace('DX_APP_WIZARD_NAME', name).replace('DX_APP_WIZARD_VERSION', version)

def format_io_spec_to_markdown(io_spec):
    io_spec = dict(io_spec)
    if 'label' not in io_spec:
        io_spec['label'] = io_spec['name']
    if 'help' not in io_spec:
        io_spec['help'] = ''
    else:
        io_spec['help'] = ' ' + io_spec['help']
    return '* **{label}** ``{name}``: ``{class}``{help}'.format(**io_spec)

def create_files_from_templates(template_dir, app_json, language,
                                required_file_input_names, optional_file_input_names,
                                required_file_array_input_names, optional_file_array_input_names,
                                file_output_names,
                                pattern, pattern_suffix='',
                                parallelized_input='', parallelized_output='', description='',
                                entry_points=()):
    manifest = []
    name = app_json['name']
    title = app_json.get('title', name)
    summary = app_json.get('summary', '')

    version = app_json['version']
    pattern_suffix += '.'

    # List all files in template_dir (other than dxapp.json) and add
    # those (after passing it through fill_in_name_and_ver).  For the
    # code.* in src,

    def chmod_755(file):
        try:
            os.chmod(file,
                     stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                     stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH |
                     stat.S_IXOTH)
        except OSError as e:
            print("Unable to change file {} mode: {}".format(file, e))

    def use_template_file(path):
        '''
        :param path: relative path from template_dir
        '''
        with open(os.path.join(template_dir, path), 'r') as template_file:
            file_text = fill_in_name_and_ver(template_file.read(), name, version)
            filled_template_filename = os.path.join(name, path)
            with open(filled_template_filename, 'w') as filled_template_file:
                filled_template_file.write(file_text)
            if filled_template_filename.endswith('.py') or filled_template_filename.endswith('.sh'):
                chmod_755(filled_template_filename)
            manifest.append(filled_template_filename)

    for template_filename in os.listdir(template_dir):
        if template_filename in ['src', 'test', 'dxapp.json'] or template_filename.endswith('~'):
            continue
        use_template_file(template_filename)

    if os.path.exists(os.path.join(template_dir, 'test')):
        for template_filename in os.listdir(os.path.join(template_dir, 'test')):
            if any(template_filename.endswith(ext) for ext in ('~', '.pyc', '.pyo', '__pycache__')):
                continue
            use_template_file(os.path.join('test', template_filename))
    for template_filename in os.listdir(os.path.join(template_dir, 'src')):
        if template_filename.endswith('~'):
            continue
        elif template_filename.startswith('code'):
            if template_filename.startswith('code' + pattern_suffix):
                with open(os.path.join(template_dir, 'src', template_filename), 'r') as code_template_file:
                    code_file_text = fill_in_name_and_ver(code_template_file.read(), name, version)

                    if "outputSpec" in app_json:
                        dummy_output_hash = {output["name"]: None for output in app_json["outputSpec"]}
                    else:
                        dummy_output_hash = {}

                    input_sig_str, init_inputs_str, dl_files_str, ul_files_str, outputs_str = \
                        language_options[language].get_strings(app_json,
                                                               required_file_input_names,
                                                               optional_file_input_names,
                                                               required_file_array_input_names,
                                                               optional_file_array_input_names,
                                                               file_output_names,
                                                               dummy_output_hash)

                    code_file_text = code_file_text.replace('DX_APP_WIZARD_INPUT_SIGNATURE', input_sig_str)
                    code_file_text = code_file_text.replace('DX_APP_WIZARD_INITIALIZE_INPUT', init_inputs_str)
                    code_file_text = code_file_text.replace('DX_APP_WIZARD_DOWNLOAD_ANY_FILES', dl_files_str)
                    code_file_text = code_file_text.replace('DX_APP_WIZARD_UPLOAD_ANY_FILES', ul_files_str)
                    code_file_text = code_file_text.replace('DX_APP_WIZARD_OUTPUT', outputs_str)
                    code_file_text = code_file_text.replace('DX_APP_WIZARD_PARALLELIZED_INPUT', parallelized_input)
                    code_file_text = code_file_text.replace('DX_APP_WIZARD_PARALLELIZED_OUTPUT', parallelized_output)

                    filled_code_filename = os.path.join(name, 'src', template_filename.replace('code' + pattern_suffix, name + '.'))
                    with open(filled_code_filename, 'w') as filled_code_file:
                        filled_code_file.write(code_file_text)
                    if filled_code_filename.endswith('.sh') or filled_code_filename.endswith('.py'):
                        chmod_755(filled_code_filename)
                    manifest.append(filled_code_filename)
        else:
            use_template_file(os.path.join('src', template_filename))

    # Readme file

    readme_template = '''<!-- dx-header -->
# {app_title} (DNAnexus Platform App)

{summary}

This is the source code for an app that runs on the DNAnexus Platform.
For more information about how to run or modify it, see
https://documentation.dnanexus.com/.
<!-- /dx-header -->

{description}

<!--
TODO: This app directory was automatically generated by dx-app-wizard;
please edit this Readme.md file to include essential documentation about
your app that would be helpful to users. (Also see the
Readme.developer.md.) Once you're done, you can remove these TODO
comments.

For more info, see https://documentation.dnanexus.com/developer.
-->
'''
    with open(os.path.join(name, 'Readme.md'), 'w') as readme_file:
        readme_file.write(readme_template.format(app_title=title, summary=summary, description=description))
    manifest.append(os.path.join(name, 'Readme.md'))

    # Developer readme

    developer_readme_template = '''# {app_name} Developer Readme

<!--
TODO: Please edit this Readme.developer.md file to include information
for developers or advanced users, for example:

* Information about app internals and implementation details
* How to report bugs or contribute to development
-->

## Running this app with additional computational resources

This app has the following entry points:

{entry_points_list}

{instance_type_override_message}

    {{
      systemRequirements: {{
        {entry_points_hash}
      }},
      [...]
    }}

See <a
href="https://documentation.dnanexus.com/developer/api/running-analyses/io-and-run-specifications#run-specification">Run
Specification</a> in the API documentation for more information about the
available instance types.
'''

    entry_points_list = '\n'.join(['* {0}'.format(entry_point) for entry_point in entry_points])

    if len(entry_points) > 1:
        instance_type_override_message = '''When running this app, you can override the instance type to be used for each
entry point by providing the ``systemRequirements`` field to
```/applet-XXXX/run``` or ```/app-XXXX/run```, as follows:'''
    else:
        instance_type_override_message = '''When running this app, you can override the instance type to be used by
providing the ``systemRequirements`` field to ```/applet-XXXX/run``` or
```/app-XXXX/run```, as follows:'''

    entry_points_hash = ",\n        ".join(['"{entry_point}": {{"instanceType": "mem2_hdd2_x2"}}'.format(entry_point=entry_point) for entry_point in entry_points])

    with open(os.path.join(name, 'Readme.developer.md'), 'w') as developer_readme_file:
        developer_readme_file.write(developer_readme_template.format(
                app_name=name,
                entry_points_list=entry_points_list,
                entry_points_hash=entry_points_hash,
                instance_type_override_message=instance_type_override_message
                ))
    manifest.append(os.path.join(name, 'Readme.developer.md'))

    return manifest
