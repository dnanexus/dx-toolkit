# Miscellaneous utility classes and functions for the dx-app-wizard
# command-line tool

from dxpy.utils.printing import *
import os, shutil, subprocess, re
import json

from . import python
from . import cpp
from . import bash

language_options = {
    "Python": python,
    "C++": cpp,
    "bash": bash
}

completer_state = {
    "available": False
}

try:
    import readline
    import rlcompleter
    readline.parse_and_bind("tab: complete")
    readline.set_completer_delims("")
    completer_state['available'] = True
except ImportError:
    print 'NOTE: readline module not available.  Install for tab-completion.'

class Completer():
    def __init__(self, choices):
        self.matches = None
        self.choices = choices

    def __call__(self, text, state):
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
    if completer_state['available']:
        readline.set_completer(completer)

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
            value = raw_input(prompt)
        except KeyboardInterrupt:
            print ''
            exit(1)
        except EOFError:
            print ''
            exit(1)
        if value != '':
            if choices is not None and value not in choices:
                print 'Error: unrecognized response, expected one of ' + json.dumps(choices)
            else:
                return value
        elif default is not None:
            return default
        elif allow_empty:
            return value

def prompt_for_yn(prompt_str, default=None):
    if default == True:
        prompt = prompt_str + ' [Y/n]: '
    elif default == False:
        prompt = prompt_str + ' [y/N]: '
    else:
        prompt = prompt_str + ' [y/n]: '

    while True:
        try:
            value = raw_input(prompt)
        except KeyboardInterrupt:
            print ''
            exit(1)
        except EOFError:
            print ''
            exit(1)
        if value != '':
            if value.lower()[0] == 'y':
                return True
            elif value.lower()[0] == 'n':
                return False
            else:
                print 'Error: unrecognized response'
        elif default is not None:
            return default

def print_intro(api_version):
    print DNANEXUS_LOGO() + ' App Wizard, API v' + api_version
    print ''

    print BOLD() + 'Basic Metadata' + ENDC()
    print ''
    print fill('''Please enter basic metadata fields that will be used to
describe your app.  Optional fields are denoted by options with square
brackets.  At the end of this wizard, the files necessary for building your
app will be generated from the answers you provide.''')
    print ''

def get_name():
    print fill('The ' + BOLD() + 'name' + ENDC() + ' of your app must be unique on the DNAnexus platform.  After creating your app for the first time, you will be able to publish new versions using the same app name.  App names are restricted to alphanumeric characters (a-z, A-Z, 0-9), and the characters ".", "_", and "-".')
    name_pattern = re.compile('^[a-zA-Z0-9._-]+$')
    while True:
        name = prompt_for_var('App Name')
        if name_pattern.match(name) is None:
            print fill('The name of your app must match /^[a-zA-Z0-9._-]+$/')
        else:
            if os.path.exists(name):
                if os.path.isdir(name):
                    remove_dir = prompt_for_yn('The directory %s already exists.  Would you like to remove all of its contents and create a new directory in its place?' % name)
                    if remove_dir:
                        shutil.rmtree(name)
                        print fill('Replacing all contents of directory %s...' % name)
                    else:
                        print ''
                        continue
                else:
                    print fill('A file named %s already exists.  Please choose another name or rename your file')
                    continue
            break
    return name

def get_metadata(api_version):
    print ''
    print fill('The ' + BOLD() + 'title' + ENDC() + ', if provided, is what is shown as the name of your app on the website.  It can be any valid UTF-8 string.')
    title = prompt_for_var('Title', '')

    print ''
    print fill('The ' + BOLD() + 'summary' + ENDC() + ' of your app is a short phrase or one-line description of what your app does.  It can be any UTF-8 human-readable string.')
    summary = prompt_for_var('Summary', '')

    print ''
    print fill('The ' + BOLD() + 'description' + ENDC() + ' of your app is a longer piece of text describing your app.  It can be any UTF-8 human-readable string, and it will be interpreted using Markdown (see http://daringfireball.net/projects/markdown/syntax/ for more details).')
    description = prompt_for_var('Description', '')

    print ''
    print fill('The ' + BOLD() + 'API version' + ENDC() + ' of your app indicates which version of the DNAnexus API your app complies to.  Automatically setting it to the latest version: ' + api_version)

    print ''
    print fill('You can publish multiple versions of your app, and the ' + BOLD() + 'version' + ENDC() + ' of your app is a string with which to tag a particular version.  We encourage the use of Semantic Versioning for labeling your apps (see http://semver.org/ for more details).')
    version = prompt_for_var('Version', '0.0.1')

    return title, summary, description, version

def get_language():
    #language_choices = language_options.keys()
    language_choices = ["Python", "C++", "bash"]
    use_completer(Completer(language_choices))
    print ''
    print fill('The ' + BOLD() + 'programming language' + ENDC() + ' of your app indicates which code skeleton we will provide.') + '\n' + fill('Currently supported languages: ' + ', '.join(language_choices))
    language = prompt_for_var('Programming language', 'Python', language_choices)
    use_completer()
    return language

def fill_in_name_and_ver(template_string, name, version):
    '''
    TODO: Rename this?
    '''
    return template_string.replace('DX_APP_WIZARD_NAME', name).replace('DX_APP_WIZARD_VERSION', version)

def create_files_from_templates(template_dir, app_json, language, file_input_names):
    manifest = []
    name = app_json['name']
    version = app_json['version']

    # List all files in template_dir (other than dxapp.json) and add
    # those (after passing it through fill_in_name_and_ver).  For the
    # code.* in src,

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
                subprocess.call(["chmod", "+x", filled_template_filename])
            manifest.append(filled_template_filename)

    for template_filename in os.listdir(template_dir):
        if template_filename in ['src', 'test', 'dxapp.json'] or template_filename.endswith('~'):
            continue
        use_template_file(template_filename)

    if os.path.exists(os.path.join(template_dir, 'test')):
        for template_filename in os.listdir(os.path.join(template_dir, 'test')):
            if template_filename.endswith('~'):
                continue
            use_template_file(os.path.join('test', template_filename))

    for template_filename in os.listdir(os.path.join(template_dir, 'src')):
        if template_filename.endswith('~'):
            continue
        elif template_filename.startswith('code'):
            # TODO: DO SOMETHING SPECIAL
            with open(os.path.join(template_dir, 'src', template_filename), 'r') as code_template_file:
                code_file_text = fill_in_name_and_ver(code_template_file.read(), name, version)

                if "outputSpec" in app_json:
                    dummy_output_hash = {output["name"]: None for output in app_json["outputSpec"]}
                else:
                    dummy_output_hash = {}

                inputs_str, files_str, outputs_str = language_options[language].get_strings(app_json,
                                                                                            file_input_names,
                                                                                            dummy_output_hash)

                code_file_text = code_file_text.replace('DX_APP_WIZARD_INPUT', inputs_str).replace('DX_APP_WIZARD_FILE_INPUT', files_str).replace('DX_APP_WIZARD_OUTPUT', outputs_str)

                filled_code_filename = os.path.join(name, 'src', template_filename.replace('code', name))
                with open(filled_code_filename, 'w') as filled_code_file:
                    filled_code_file.write(code_file_text)
                subprocess.call(["chmod", "+x", os.path.join(filled_code_filename)])
                manifest.append(filled_code_filename)
        else:
            use_template_file(os.path.join('src', template_filename))

    readme_template = '''# {app_name} (DNAnexus platform app)
This app directory was generated by dx-app-wizard. Please edit this Readme file to include essential documentation about your app.

For more info, see http://wiki.dnanexus.com/Building-Your-First-DNAnexus-App.
'''

    with open(os.path.join(name, 'Readme.md'), 'w') as readme_file:
        readme_file.write(readme_template.format(app_name=name, app_version=version))
    manifest.append(os.path.join(name, 'Readme.md'))

    return manifest
