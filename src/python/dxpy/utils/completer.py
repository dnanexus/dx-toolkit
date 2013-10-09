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
This file contains utility functions for interactive scripts such as
dx for tab-completion, resolving naming conflicts, etc.
'''

import sys

from argcomplete import warn
from collections import namedtuple, OrderedDict
import dxpy
from dxpy.utils.resolver import (get_first_pos_of_char, get_last_pos_of_char, clean_folder_path, resolve_path,
                                 split_unescaped, ResolutionError)
from dxpy.utils.printing import fill

def startswith(text):
    return (lambda string: string.startswith(text))

def escape_name(text):
    return escape_colon(text).replace('/', '\\/')

def escape_colon(text):
    return text.replace(':', '\\:')

def unescape_colon(text):
    return text.replace('\\:', ':')

def join_path(project, path, name):
    project = escape_colon(project)
    path = escape_colon(path)
    name = escape_name(name)
    if not path.startswith('/'):
        path = '/' + path
    if not path.endswith('/'):
        path = path + '/'
    return project+":"+path+name

def split_path(path):
    colon_pos = get_first_pos_of_char(':', path)
    slash_pos = get_last_pos_of_char('/', path)
    project, path, name = path[:colon_pos], path[colon_pos+1:slash_pos], path[slash_pos+1:]
    if path == '':
        path = '/'
    project = unescape_colon(project)
    path = unescape_colon(path)
    name = unescape_colon(name).replace('\\/', '/')
    return project, path, name

# def completion_escaper(match):
#     if match.group(0) == ":":
#         return "\\\\:"
#     elif match.group(0) == "/":
#         return "\\\\/"
#     elif match.group(0) == "*":
#         return "\\\\\\\\*"
#     elif match.group(0) == "?":
#         return "\\\\\\\\\?"
#     else:
#         return "\\" + match.group(0)

# def escape_completion_name_str(string):
#     return re.sub("([#\?\*: ;&`\"'/!$\(\)\{\[<>|~])", completion_escaper, string.replace('\\', '\\\\\\\\'))

#def escape_completion_name_str(string):
#    return string.replace('\\', '\\\\\\\\').replace(' ', '\ ').replace(':', '\\\\:').replace('/', '\\\\/').replace('*', '\\\\\\\\*').replace('?', '\\\\\\\\?').replace('(', '\\(').replace(')', '\\)')

# def unescape_completion_name_str(string):
#     return string.replace('\\)', ')').replace('\\(', '(').replace('\\\\\\\\?', '?').replace('\\\\\\\\*', '*').replace('\\\\/', '/').replace('\\\\:', ':').replace('\ ', ' ').replace('\\\\\\\\', '\\')

def get_folder_matches(text, delim_pos, dxproj, folderpath):
    '''
    :param text: String to be tab-completed; still in escaped form
    :type text: string
    :param delim_pos: index of last unescaped "/" in text
    :type delim_pos: int
    :param dxproj: DXProject handler to use
    :type dxproj: DXProject
    :param folderpath: Unescaped path in which to search for folder matches
    :type folderpath: string
    :returns: List of matches
    :rtype: list of strings

    Members of the returned list are guaranteed to start with *text*
    and be in escaped form for consumption by the command-line.
    '''
    try:
        folder_names = map(lambda folder_name:
                               folder_name[folder_name.rfind('/') + 1:],
                           dxproj.list_folder(folder=folderpath,
                                              only='folders')['folders'])
        if text != '' and delim_pos != len(text) - 1:
            folder_names += ['.', '..']
        prefix = text[:delim_pos + 1]
        return [prefix + f + '/' for f in folder_names if (prefix + f + '/').startswith(text)]
    except:
        return []

def get_data_matches(text, delim_pos, dxproj, folderpath, classname=None,
                     typespec=None):
    '''
    :param text: String to be tab-completed; still in escaped form
    :type text: string
    :param delim_pos: index of last unescaped "/" or ":" in text
    :type delim_pos: int
    :param dxproj: DXProject handler to use
    :type dxproj: DXProject
    :param folderpath: Unescaped path in which to search for data object matches
    :type folderpath: string
    :param classname: Data object class by which to restrict the search (None for no restriction on class)
    :type classname: string
    :returns: List of matches
    :rtype: list of strings

    Members of the returned list are guaranteed to start with *text*
    and be in escaped form for consumption by the command-line.
    '''

    #unescaped_text = unescape_completion_name_str(text[delim_pos + 1:])
    unescaped_text = text[delim_pos + 1:]

    try:
        results = list(dxpy.find_data_objects(project=dxproj.get_id(),
                                              folder=folderpath,
                                              name=unescaped_text + "*",
                                              name_mode="glob",
                                              recurse=False,
                                              visibility='either' if text != '' and delim_pos != len(text) - 1 else 'visible',
                                              classname=classname,
                                              limit=100,
                                              describe=True,
                                              typename=typespec))
        prefix = '' if text == '' else text[:delim_pos + 1]
        return [prefix + escape_name(result['describe']['name']) for result in results]
    except:
        return []

def path_completer(text, expected=None, classes=None, perm_level=None,
                   include_current_proj=False, typespec=None):
    '''
    :param text: String to tab-complete to a path matching the syntax project-name:folder/entity_or_folder_name
    :type text: string
    :param expected: "folder", "entity", "project", or None (no restriction) as to the types of answers to look for
    :type expected: string
    :param classes: if expected="entity", the possible data object classes that are acceptable
    :type classes: list of strings
    :param perm_level: the minimum permissions level required, e.g. "VIEW" or "CONTRIBUTE"
    :type perm_level: string
    :param include_current_proj: Indicate whether the current project's name should be a potential result
    :type include_current_proj: boolean

    Returns a list of matches to the text and restricted by the
    requested parameters.
    '''

    colon_pos = get_last_pos_of_char(':', text)
    slash_pos = get_last_pos_of_char('/', text)
    delim_pos = max(colon_pos, slash_pos)

    # First get projects if necessary
    matches = []
    if expected == 'project' and colon_pos > 0 and colon_pos == len(text) - 1:
        if dxpy.find_one_project(zero_ok=True, name=text[:colon_pos]) is not None:
            return [text + " "]

    if colon_pos < 0 and slash_pos < 0:
        # Might be tab-completing a project, but don't ever include
        # whatever's set as dxpy.WORKSPACE_ID unless expected == "project"
        # Also, don't bother if text=="" and expected is NOT "project"
        # Also, add space if expected == "project"
        if text != "" or expected == 'project':
            results = dxpy.find_projects(describe=True, level=perm_level)
            if not include_current_proj:
                results = [r for r in results if r['id'] != dxpy.WORKSPACE_ID]
            matches += [escape_colon(r['describe']['name'])+':' for r in results if r['describe']['name'].startswith(text)]

    if expected == 'project':
        return matches

    # Attempt to tab-complete to a folder or data object name
    if colon_pos < 0 and slash_pos >= 0:
        # Not tab-completing a project, and the project is unambiguous
        # (use dxpy.WORKSPACE_ID)
        if dxpy.WORKSPACE_ID is not None:
            # try-catch block in case dxpy.WORKSPACE_ID is garbage
            try:
                dxproj = dxpy.get_handler(dxpy.WORKSPACE_ID)
                folderpath, entity_name = clean_folder_path(text)
                matches += get_folder_matches(text, slash_pos, dxproj, folderpath)
                if expected != 'folder':
                    if classes is not None:
                        for classname in classes:
                            matches += get_data_matches(text, slash_pos, dxproj,
                                                        folderpath, classname,
                                                        typespec)
                    else:
                        matches += get_data_matches(text, slash_pos, dxproj,
                                                    folderpath, typespec=typespec)
            except:
                pass
    else:
        # project is given by a path, but attempt to resolve to an
        # object or folder anyway
        try:
            proj_ids, folderpath, entity_name = resolve_path(text, multi_projects=True)
        except ResolutionError as details:
            sys.stderr.write("\n" + fill(unicode(details)))
            return matches
        for proj in proj_ids:
            # protects against dxpy.WORKSPACE_ID being garbage
            try:
                dxproj = dxpy.get_handler(proj)
                matches += get_folder_matches(text, delim_pos, dxproj, folderpath)
                if expected != 'folder':
                    if classes is not None:
                        for classname in classes:
                            matches += get_data_matches(text, delim_pos, dxproj,
                                                        folderpath, classname,
                                                        typespec)
                    else:
                        matches += get_data_matches(text, delim_pos, dxproj,
                                                    folderpath, typespec=typespec)
            except:
                pass
    return matches

class DXPathCompleter():
    '''
    This class can be used as a tab-completer with the modules
    readline and rlcompleter.  Note that to tab-complete data object
    names with spaces, the delimiters set for the completer must not
    include spaces.
    '''
    def __init__(self, expected=None, classes=None, typespec=None, include_current_proj=False):
        self.matches = []
        self.expected = expected
        self.classes = classes
        self.typespec = typespec
        self.include_current_proj = include_current_proj

    def _populate_matches(self, prefix):
        self.matches = path_completer(prefix, self.expected, self.classes,
                                      typespec=self.typespec,
                                      include_current_proj=self.include_current_proj)

    def get_matches(self, line, point, prefix, suffix):
        self._populate_matches(prefix)
        return self.matches

    def complete(self, text, state):
        if state == 0:
            self._populate_matches(text)
        if state < len(self.matches):
            return self.matches[state]
        else:
            return None

class DXAppCompleter():
    def __init__(self, installed=None):
        self.matches = []
        self.installed = installed

    def _populate_matches(self, prefix):
        try:
            name_query = None
            if len(prefix) > 0:
                if prefix.startswith("app-") and len(prefix) > 4:
                    name_query = prefix[4:] + "*"
                elif len(prefix) > 4 or not "app-".startswith(prefix):
                    name_query = prefix + "*"
            appnames = [result['describe']['name'] for result in dxpy.find_apps(name=name_query, name_mode="glob", describe={"fields": {"name": True, "installed": (self.installed is not None)}}) if self.installed is None or (self.installed == result['describe']['installed'])]
        except:
            # This is for (temporary) backwards-compatibility
            appnames = [result['describe']['name'] for result in dxpy.find_apps(describe=True) if self.installed is None or (self.installed == result['describe']['installed'])]
        self.matches = [name for name in appnames if name.startswith(prefix)]
        if prefix != '' and prefix.startswith('app-'[:len(prefix)]):
            appnames_with_prefix = [('app-' + name) for name in appnames]
            self.matches += [name for name in appnames_with_prefix if name.startswith(prefix)]

    def get_matches(self, line, point, prefix, suffix):
        # This implementation is reliant on bash behavior that ':' is
        # treated as a word separator for determining prefix
        if get_last_pos_of_char(' ', line) != point - 1:
            prefix = split_unescaped(' ', line[:point])[-1]
        # Can immediately return if there are disallowed characters of
        # app names in the prefix
        if ':' in prefix:
            return []
        self._populate_matches(prefix)
        return self.matches

    def complete(self, text, state):
        if state == 0:
            self._populate_matches(text)

        if state < len(self.matches):
            return self.matches[state]
        else:
            return None

class LocalCompleter():
    def __init__(self):
        self.matches = []

    def _populate_matches(self, prefix):
        import subprocess, shlex

        lexer = shlex.shlex(prefix, posix=True)
        tomatch = lexer.get_token()
        tomatch = '' if tomatch is None else tomatch
        file_completions = subprocess.Popen('bash -c "compgen -f -o filenames -- \'' + tomatch + '\'"',
                                            shell=True,
                                            stdout=subprocess.PIPE).stdout.read().splitlines()
        self.matches = subprocess.Popen('bash -c "compgen -d -o filenames -S / -- \'' + tomatch + '\'"',
                                        shell=True,
                                        stdout=subprocess.PIPE).stdout.read().splitlines()
        self.matches = [match.replace(" ", "\ ") for match in self.matches if match != '']
        self.matches += [(completion.replace(" ", "\ ") + " ") for completion in file_completions if (completion != '' and completion + "/" not in self.matches)]

    def get_matches(self, line, point, prefix, suffix):
        self._populate_matches(prefix)
        return self.matches

    def complete(self, text, state):
        if state == 0:
            self._populate_matches(text)

        if state < len(self.matches):
            return self.matches[state].replace(' ', '\ ')
        else:
            return None

class NoneCompleter():
    def get_matches(self, line, point, prefix, suffix):
        return []

    def complete(self, text, state):
        return None

class ListCompleter():
    def __init__(self, completions):
        self.completions = completions
        self.matches = []

    def _populate_matches(self, prefix):
        self.matches = [ans for ans in self.completions if ans.startswith(prefix)]

    def get_matches(self, line, point, prefix, suffix):
        # This implementation is reliant on bash behavior that ':' is
        # treated as a word separator for determining prefix
        if get_last_pos_of_char(' ', line) != point - 1:
            prefix = split_unescaped(' ', line[:point])[-1]
        self._populate_matches(prefix)
        if prefix.rfind(':') != -1:
            for i in range(len(self.matches)):
                self.matches[i] = self.matches[i][self.matches[i].rfind(':') + 1:]

        return self.matches

    def complete(self, text, state):
        if state == 0:
            self._populate_matches(text)

        if state < len(self.matches):
            return self.matches[state]
        else:
            return None

class MultiCompleter():
    def __init__(self, completers):
        self.completers = completers
        self.matches = []

    def _populate_matches(self, prefix):
        self.matches = []
        for completer in self.completers:
            self.matches += completer.get_matches('', 0, prefix, '')
        return self.matches

    def get_matches(self, line, point, prefix, suffix):
        # This implementation assumes the get_matches will handle any
        # special word separation
        self.matches = []
        for completer in self.completers:
            self.matches += completer.get_matches(line, point, prefix, suffix)
        return self.matches

    def complete(self, text, state):
        if state == 0:
            self._populate_matches(text)

        if state < len(self.matches):
            return self.matches[state]
        else:
            return None

class InstanceTypesCompleter():
    InstanceTypeSpec = namedtuple('InstanceTypeSpec', ('Name', 'Memory_GB', 'CPU_Cores', 'Scratch_Space_GB'))
    instance_types = OrderedDict()
    for i in (InstanceTypeSpec('dx_m1.medium', 3.75, 1, 420),
              InstanceTypeSpec('dx_m1.large', 7.5, 2, 840),
              InstanceTypeSpec('dx_m1.xlarge', 15, 4, 1680),
              InstanceTypeSpec('dx_c1.xlarge', 7, 8, 1680),
              InstanceTypeSpec('dx_m2.xlarge', 17.1, 2, 420),
              InstanceTypeSpec('dx_m2.2xlarge', 34.2, 4, 850),
              InstanceTypeSpec('dx_m2.4xlarge', 68.4, 8, 1680),
              InstanceTypeSpec('dx_cc2.8xlarge', 60.5, 32, 3360),
              InstanceTypeSpec('dx_cg1.4xlarge', 22.5, 16, 1680),
              InstanceTypeSpec('dx_cr1.8xlarge', 244, 32, 240)):
        instance_types[i.Name] = i
    instance_type_names = instance_types.keys()

    def complete(self, text, state):
        try:
            return self.instance_type_names[state]
        except IndexError:
            return None
