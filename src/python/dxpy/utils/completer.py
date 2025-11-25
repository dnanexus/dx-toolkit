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
This file contains utility functions for interactive scripts such as
dx for tab-completion, resolving naming conflicts, etc.
'''

from __future__ import print_function, unicode_literals, division, absolute_import
import sys

from argcomplete import warn
from collections import namedtuple, OrderedDict
import dxpy
from .resolver import (get_first_pos_of_char, get_last_pos_of_char, clean_folder_path, resolve_path,
                       split_unescaped, ResolutionError)
from .printing import fill

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
        folders = dxproj.list_folder(folder=folderpath, only='folders')['folders']
        folder_names = [name[name.rfind('/') + 1:] for name in folders]
        if text != '' and delim_pos != len(text) - 1:
            folder_names += ['.', '..']
        prefix = text[:delim_pos + 1]
        return [prefix + f + '/' for f in folder_names if (prefix + f + '/').startswith(text)]
    except:
        return []

def get_data_matches(text, delim_pos, dxproj, folderpath, classname=None,
                     typespec=None, visibility=None):
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
    :param visibility: Visibility to constrain the results to; default is "visible" for empty strings, "either" for nonempty
    :type visibility: string
    :returns: List of matches
    :rtype: list of strings

    Members of the returned list are guaranteed to start with *text*
    and be in escaped form for consumption by the command-line.
    '''
    #unescaped_text = unescape_completion_name_str(text[delim_pos + 1:])
    unescaped_text = text[delim_pos + 1:]

    if visibility is None:
        if text != '' and delim_pos != len(text) - 1:
            visibility = "either"
        else:
            visibility = "visible"

    try:
        results = list(dxpy.find_data_objects(project=dxproj.get_id(),
                                              folder=folderpath,
                                              name=unescaped_text + "*",
                                              name_mode="glob",
                                              recurse=False,
                                              visibility=visibility,
                                              classname=classname,
                                              limit=100,
                                              describe=dict(fields=dict(name=True)),
                                              typename=typespec))
        prefix = '' if text == '' else text[:delim_pos + 1]
        return [prefix + escape_name(result['describe']['name']) for result in results]
    except:
        return []

def path_completer(text, expected=None, classes=None, perm_level=None,
                   include_current_proj=False, typespec=None, visibility=None):
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
    :param visibility: Visibility with which to restrict the completion (one of "either", "visible", or "hidden") (default behavior is dependent on *text*)

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
                                                        folderpath, classname=classname,
                                                        typespec=typespec,
                                                        visibility=visibility)
                    else:
                        matches += get_data_matches(text, slash_pos, dxproj,
                                                    folderpath, typespec=typespec,
                                                    visibility=visibility)
            except:
                pass
    else:
        # project is given by a path, but attempt to resolve to an
        # object or folder anyway
        try:
            proj_ids, folderpath, entity_name = resolve_path(text, multi_projects=True)
        except ResolutionError as details:
            sys.stderr.write("\n" + fill(str(details)))
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
                                                        folderpath, classname=classname,
                                                        typespec=typespec, visibility=visibility)
                    else:
                        matches += get_data_matches(text, delim_pos, dxproj,
                                                    folderpath, typespec=typespec,
                                                    visibility=visibility)
            except:
                pass
    return matches

class DXPathCompleter():
    '''
    This class can be used as a tab-completer with the readline module
    Note that to tab-complete data object names with spaces, the delimiters
    set for the completer must not include spaces.
    '''
    def __init__(self, expected=None, classes=None, typespec=None, include_current_proj=False,
                 visibility=None):
        self.matches = []
        self.expected = expected
        self.classes = classes
        self.typespec = typespec
        self.include_current_proj = include_current_proj
        self.visibility = visibility

    def _populate_matches(self, prefix):
        self.matches = path_completer(prefix, self.expected, self.classes,
                                      typespec=self.typespec,
                                      include_current_proj=self.include_current_proj,
                                      visibility=self.visibility)

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
        from argcomplete.completers import FilesCompleter
        completer = FilesCompleter()
        self.matches = completer(prefix)

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
    InstanceTypeSpec = namedtuple('InstanceTypeSpec', ('Name', 'CPU_Cores', 'Memory_GiB', 'Storage_GB'))
    GpuInstanceTypeSpec = namedtuple('GpuInstanceTypeSpec', ('Name', 'CPU_Cores', 'Memory_GiB', 'Storage_GB', 'GPU', 'GPU_Memory_GiB'))
    FpgaInstanceTypeSpec = namedtuple('FpgaInstanceTypeSpec', ('Name', 'CPU_Cores', 'Memory_GiB', 'Storage_GB', 'FPGA'))

    # AWS
    aws_preferred_instance_types = OrderedDict()
    for i in (InstanceTypeSpec('mem1_ssd1_v2_x2', 2, 4.0, 50),
              InstanceTypeSpec('mem1_ssd1_v2_x4', 4, 8.0, 100),
              InstanceTypeSpec('mem1_ssd1_v2_x8', 8, 16.0, 200),
              InstanceTypeSpec('mem1_ssd1_v2_x16', 16, 32.0, 400),
              InstanceTypeSpec('mem1_ssd1_v2_x36', 36, 72.0, 900),
              InstanceTypeSpec('mem1_ssd1_v2_x72', 72, 144.0, 1800),

              InstanceTypeSpec('mem1_ssd2_v3_x2', 2, 4.0, 118),
              InstanceTypeSpec('mem1_ssd2_v3_x4', 4, 8.0, 237),
              InstanceTypeSpec('mem1_ssd2_v3_x8', 8, 16.0, 474),
              InstanceTypeSpec('mem1_ssd2_v3_x16', 16, 32.0, 950),
              InstanceTypeSpec('mem1_ssd2_v3_x32', 32, 64.0, 1900),
              InstanceTypeSpec('mem1_ssd2_v3_x48', 48, 96.0, 2850),
              InstanceTypeSpec('mem1_ssd2_v3_x64', 64, 128.0, 3800),
              InstanceTypeSpec('mem1_ssd2_v3_x96', 96, 192.0, 5700),
              InstanceTypeSpec('mem1_ssd2_v3_x128', 128, 256.0, 7600),

              InstanceTypeSpec('mem1_ssd2_v2_x2', 2, 4.0, 160),
              InstanceTypeSpec('mem1_ssd2_v2_x4', 4, 8.0, 320),
              InstanceTypeSpec('mem1_ssd2_v2_x8', 8, 16.0, 640),
              InstanceTypeSpec('mem1_ssd2_v2_x16', 16, 32.0, 1280),
              InstanceTypeSpec('mem1_ssd2_v2_x36', 36, 72.0, 2880),
              InstanceTypeSpec('mem1_ssd2_v2_x72', 72, 144.0, 5760),

              InstanceTypeSpec('mem2_ssd1_v2_x2', 2, 8.0, 75),
              InstanceTypeSpec('mem2_ssd1_v2_x4', 4, 16.0, 150),
              InstanceTypeSpec('mem2_ssd1_v2_x8', 8, 32.0, 300),
              InstanceTypeSpec('mem2_ssd1_v2_x16', 16, 64.0, 600),
              InstanceTypeSpec('mem2_ssd1_v2_x32', 32, 128.0, 1200),
              InstanceTypeSpec('mem2_ssd1_v2_x48', 48, 144.0, 1800),
              InstanceTypeSpec('mem2_ssd1_v2_x64', 64, 256.0, 2400),
              InstanceTypeSpec('mem2_ssd1_v2_x96', 96, 384.0, 3600),

              InstanceTypeSpec('mem2_ssd2_v3_x2', 2, 8.0, 118),
              InstanceTypeSpec('mem2_ssd2_v3_x4', 4, 16.0, 237),
              InstanceTypeSpec('mem2_ssd2_v3_x8', 8, 32.0, 474),
              InstanceTypeSpec('mem2_ssd2_v3_x16', 16, 64.0, 950),
              InstanceTypeSpec('mem2_ssd2_v3_x32', 32, 128.0, 1900),
              InstanceTypeSpec('mem2_ssd2_v3_x48', 48, 192.0, 2850),
              InstanceTypeSpec('mem2_ssd2_v3_x64', 64, 256.0, 3800),
              InstanceTypeSpec('mem2_ssd2_v3_x96', 96, 384.0, 5700),
              InstanceTypeSpec('mem2_ssd2_v3_x128', 128, 512.0, 7600),

              InstanceTypeSpec('mem2_ssd2_v2_x2', 2, 8.0, 160),
              InstanceTypeSpec('mem2_ssd2_v2_x4', 4, 16.0, 320),
              InstanceTypeSpec('mem2_ssd2_v2_x8', 8, 32.0, 640),
              InstanceTypeSpec('mem2_ssd2_v2_x16', 16, 64.0, 1280),
              InstanceTypeSpec('mem2_ssd2_v2_x32', 32, 128.0, 2560),
              InstanceTypeSpec('mem2_ssd2_v2_x48', 48, 192.0, 3840),
              InstanceTypeSpec('mem2_ssd2_v2_x64', 64, 256.0, 5120),
              InstanceTypeSpec('mem2_ssd2_v2_x96', 96, 384.0, 7480),

              InstanceTypeSpec('mem3_ssd1_v2_x2', 2, 16.0, 75),
              InstanceTypeSpec('mem3_ssd1_v2_x4', 4, 32.0, 150),
              InstanceTypeSpec('mem3_ssd1_v2_x8', 8, 64.0, 300),
              InstanceTypeSpec('mem3_ssd1_v2_x16', 16, 128.0, 600),
              InstanceTypeSpec('mem3_ssd1_v2_x32', 32, 256.0, 1200),
              InstanceTypeSpec('mem3_ssd1_v2_x48', 48, 384.0, 1800),
              InstanceTypeSpec('mem3_ssd1_v2_x64', 64, 512.0, 3200),
              InstanceTypeSpec('mem3_ssd1_v2_x96', 96, 768.0, 3600),

              InstanceTypeSpec('mem3_ssd2_v3_x2', 2, 16.0, 118),
              InstanceTypeSpec('mem3_ssd2_v3_x4', 4, 32.0, 237),
              InstanceTypeSpec('mem3_ssd2_v3_x8', 8, 64.0, 474),
              InstanceTypeSpec('mem3_ssd2_v3_x16', 16, 128.0, 950),
              InstanceTypeSpec('mem3_ssd2_v3_x32', 32, 256.0, 1900),
              InstanceTypeSpec('mem3_ssd2_v3_x48', 48, 384.0, 2850),
              InstanceTypeSpec('mem3_ssd2_v3_x64', 64, 512.0, 3800),
              InstanceTypeSpec('mem3_ssd2_v3_x96', 96, 768.0, 5700),
              InstanceTypeSpec('mem3_ssd2_v3_x128', 128, 1024.0, 7600),

              InstanceTypeSpec('mem3_ssd2_v2_x2', 2, 15.25, 475),
              InstanceTypeSpec('mem3_ssd2_v2_x4', 4, 30.5, 950),
              InstanceTypeSpec('mem3_ssd2_v2_x8', 8, 61.0, 1900),
              InstanceTypeSpec('mem3_ssd2_v2_x16', 16, 122.0, 3800),
              InstanceTypeSpec('mem3_ssd2_v2_x32', 32, 244.0, 7600),
              InstanceTypeSpec('mem3_ssd2_v2_x64', 64, 488.0, 15200),

              InstanceTypeSpec('mem3_ssd3_x2', 2, 16.0, 1250),
              InstanceTypeSpec('mem3_ssd3_x4', 4, 32.0, 2500),
              InstanceTypeSpec('mem3_ssd3_x8', 8, 64.0, 5000),
              InstanceTypeSpec('mem3_ssd3_x12', 12, 96.0, 7500),
              InstanceTypeSpec('mem3_ssd3_x24', 24, 192.0, 15000),
              InstanceTypeSpec('mem3_ssd3_x48', 48, 384.0, 30000),
              InstanceTypeSpec('mem3_ssd3_x96', 96, 768.0, 60000),

              InstanceTypeSpec('mem4_ssd1_x128', 128, 1952.0, 3840)):
        aws_preferred_instance_types[i.Name] = i

    # Azure
    azure_preferred_instance_types = OrderedDict()
    for i in (InstanceTypeSpec('azure:mem1_ssd1_x2', 2, 3.9, 32),
              InstanceTypeSpec('azure:mem1_ssd1_x4', 4, 7.8, 64),
              InstanceTypeSpec('azure:mem1_ssd1_x8', 8, 15.7, 128),
              InstanceTypeSpec('azure:mem1_ssd1_x16', 16, 31.4, 256),

              InstanceTypeSpec('azure:mem2_ssd1_x1', 1, 3.5, 128),
              InstanceTypeSpec('azure:mem2_ssd1_x2', 2, 7.0, 128),
              InstanceTypeSpec('azure:mem2_ssd1_x4', 4, 14.0, 128),
              InstanceTypeSpec('azure:mem2_ssd1_x8', 8, 28.0, 256),
              InstanceTypeSpec('azure:mem2_ssd1_x16', 16, 56.0, 512),

              InstanceTypeSpec('azure:mem3_ssd1_x2', 2, 14.0, 128),
              InstanceTypeSpec('azure:mem3_ssd1_x4', 4, 28.0, 128),
              InstanceTypeSpec('azure:mem3_ssd1_x8', 8, 56.0, 256),
              InstanceTypeSpec('azure:mem3_ssd1_x16', 16, 112.0, 512),
              InstanceTypeSpec('azure:mem3_ssd1_x20', 20, 140.0, 640),

              InstanceTypeSpec('azure:mem4_ssd1_x2', 2, 28.0, 128),
              InstanceTypeSpec('azure:mem4_ssd1_x4', 4, 56.0, 128),
              InstanceTypeSpec('azure:mem4_ssd1_x8', 8, 112.0, 256),
              InstanceTypeSpec('azure:mem4_ssd1_x16', 16, 224.0, 512),
              InstanceTypeSpec('azure:mem4_ssd1_x32', 32, 448.0, 1024),

              InstanceTypeSpec('azure:mem5_ssd2_x64', 64, 1792.0, 8192),
              InstanceTypeSpec('azure:mem5_ssd2_x128', 128, 3892.0, 16384)):
        azure_preferred_instance_types[i.Name] = i

    gpu_instance_types = OrderedDict()
    for i in (GpuInstanceTypeSpec('mem2_ssd1_gpu_x16', 16, 64.0, 225, '1 NVIDIA T4', 16.0),
              GpuInstanceTypeSpec('mem2_ssd1_gpu_x32', 32, 128.0, 900, '1 NVIDIA T4', 16.0),
              GpuInstanceTypeSpec('mem2_ssd1_gpu1_x32', 32, 128.0, 900, '1 NVIDIA T4', 16.0),
              GpuInstanceTypeSpec('mem2_ssd1_gpu_x64', 64, 256.0, 900, '1 NVIDIA T4', 16.0),
              GpuInstanceTypeSpec('mem2_ssd1_gpu1_x64', 64, 256.0, 900, '1 NVIDIA T4', 16.0),
              GpuInstanceTypeSpec('mem2_ssd1_gpu_x48', 48, 192.0, 900, '4 NVIDIA T4', 64.0),
              GpuInstanceTypeSpec('mem2_ssd1_gpu4_x48', 48, 192.0, 900, '4 NVIDIA T4', 64.0),

              GpuInstanceTypeSpec('mem2_ssd2_gpu1_x4', 4, 16.0, 250, '1 NVIDIA A10G', 24.0),
              GpuInstanceTypeSpec('mem2_ssd2_gpu1_x8', 8, 32.0, 450, '1 NVIDIA A10G', 24.0),
              GpuInstanceTypeSpec('mem2_ssd2_gpu1_x16', 16, 64.0, 600, '1 NVIDIA A10G', 24.0),
              GpuInstanceTypeSpec('mem2_ssd2_gpu1_x32', 32, 128.0, 900, '1 NVIDIA A10G', 24.0),
              GpuInstanceTypeSpec('mem2_ssd2_gpu1_x64', 64, 256.0, 1900, '1 NVIDIA A10G', 24.0),
              GpuInstanceTypeSpec('mem2_ssd2_gpu4_x48', 48, 192.0, 3800, '4 NVIDIA A10G', 96.0),
              GpuInstanceTypeSpec('mem2_ssd2_gpu4_x96', 96, 384.0, 3800, '4 NVIDIA A10G', 96.0),
              GpuInstanceTypeSpec('mem2_ssd2_gpu8_x192', 192, 768.0, 7600, '8 NVIDIA A10G', 192.0),

              GpuInstanceTypeSpec('mem2_ssd2_gpu1_v2_x4', 4, 16.0, 250, '1 NVIDIA L4', 24.0),
              GpuInstanceTypeSpec('mem2_ssd2_gpu1_v2_x8', 8, 32.0, 450, '1 NVIDIA L4', 24.0),
              GpuInstanceTypeSpec('mem2_ssd2_gpu1_v2_x16', 16, 64.0, 600, '1 NVIDIA L4', 24.0),
              GpuInstanceTypeSpec('mem2_ssd2_gpu1_v2_x32', 32, 128.0, 900, '1 NVIDIA L4', 24.0),
              GpuInstanceTypeSpec('mem2_ssd2_gpu1_v2_x64', 64, 256.0, 1880, '1 NVIDIA L4', 24.0),
              GpuInstanceTypeSpec('mem2_ssd2_gpu4_v2_x48', 48, 192.0, 3760, '4 NVIDIA L4', 96.0),
              GpuInstanceTypeSpec('mem2_ssd2_gpu4_v2_x96', 96, 384.0, 3760, '4 NVIDIA L4', 96.0),
              GpuInstanceTypeSpec('mem2_ssd2_gpu8_v2_x192', 192, 768.0, 7520, '8 NVIDIA L4', 192.0),
              GpuInstanceTypeSpec('mem3_ssd1_gpu1_x16', 16, 128.0, 600, '1 NVIDIA L4', 24.0),
              GpuInstanceTypeSpec('mem3_ssd1_gpu1_x32', 32, 256.0, 900, '1 NVIDIA L4', 24.0),

              GpuInstanceTypeSpec('mem3_ssd1_gpu_x8', 8, 61.0, 160, '1 NVIDIA V100', 16.0),
              GpuInstanceTypeSpec('mem3_ssd1_gpu_x32', 32, 244.0, 640, '4 NVIDIA V100', 64.0),
              GpuInstanceTypeSpec('mem3_ssd1_gpu_x64', 64, 488.0, 1280, '8 NVIDIA V100', 128.0),
              GpuInstanceTypeSpec('azure:mem3_ssd2_gpu4_x64', 64, 488.0, 2048, '4 NVIDIA V100', 64.0)):
        gpu_instance_types[i.Name] = i

    fpga_instance_types = OrderedDict()
    for i in (FpgaInstanceTypeSpec('mem3_ssd2_fpga1_x24', 24, 256.0, 940, 1),
              FpgaInstanceTypeSpec('mem3_ssd2_fpga2_x48', 48, 512.0, 1880, 2),
              FpgaInstanceTypeSpec('mem3_ssd2_fpga8_x192', 192, 2048.0, 7520, 8)):
        fpga_instance_types[i.Name] = i

    aws_other_instance_types = OrderedDict()
    for i in (InstanceTypeSpec('mem1_ssd1_x2', 2, 3.8, 32),
              InstanceTypeSpec('mem1_ssd1_x4', 4, 7.5, 80),
              InstanceTypeSpec('mem1_ssd1_x8', 8, 15.0, 160),
              InstanceTypeSpec('mem1_ssd1_x16', 16, 30.0, 320),
              InstanceTypeSpec('mem1_ssd1_x32', 32, 60.0, 640),
              InstanceTypeSpec('mem1_ssd1_x36', 36, 72.0, 900),

              InstanceTypeSpec('mem1_ssd2_x2', 2, 3.8, 160),
              InstanceTypeSpec('mem1_ssd2_x4', 4, 7.5, 320),
              InstanceTypeSpec('mem1_ssd2_x8', 8, 15.0, 640),
              InstanceTypeSpec('mem1_ssd2_x16', 16, 30.0, 1280),
              InstanceTypeSpec('mem1_ssd2_x36', 36, 60.0, 2880),

              InstanceTypeSpec('mem2_ssd1_x2', 2, 7.5, 32),
              InstanceTypeSpec('mem2_ssd1_x4', 4, 15.0, 80),
              InstanceTypeSpec('mem2_ssd1_x8', 8, 30.0, 160),

              InstanceTypeSpec('mem2_ssd2_x2', 2, 8.0, 160),
              InstanceTypeSpec('mem2_ssd2_x4', 4, 16.0, 320),
              InstanceTypeSpec('mem2_ssd2_x8', 8, 32.0, 1280),
              InstanceTypeSpec('mem2_ssd2_x16', 16, 64.0, 2560),
              InstanceTypeSpec('mem2_ssd2_x40', 40, 160.0, 3200),
              InstanceTypeSpec('mem2_ssd2_x64', 64, 256.0, 5120),

              InstanceTypeSpec('mem3_ssd1_x2', 2, 15.0, 32),
              InstanceTypeSpec('mem3_ssd1_x4', 4, 30.5, 80),
              InstanceTypeSpec('mem3_ssd1_x8', 8, 61.0, 160),
              InstanceTypeSpec('mem3_ssd1_x16', 16, 122.0, 320),
              InstanceTypeSpec('mem3_ssd1_x32', 32, 244.0, 640),

              InstanceTypeSpec('mem3_ssd2_x4', 4, 30.5, 800),
              InstanceTypeSpec('mem3_ssd2_x8', 8, 61.0, 1600),
              InstanceTypeSpec('mem3_ssd2_x16', 16, 122.0, 3200),
              InstanceTypeSpec('mem3_ssd2_x32', 32, 244.0, 6400),

              InstanceTypeSpec('mem1_hdd1_x2', 2, 3.75, 200),
              InstanceTypeSpec('mem1_hdd1_x4', 4, 7.5, 400),
              InstanceTypeSpec('mem1_hdd1_x8', 8, 15.0, 800),
              InstanceTypeSpec('mem1_hdd1_x16', 16, 30.0, 1600),
              InstanceTypeSpec('mem1_hdd1_x36', 36, 60.0, 3200),

              InstanceTypeSpec('mem1_hdd1_v2_x2', 2, 4.0, 200),
              InstanceTypeSpec('mem1_hdd1_v2_x4', 4, 8.0, 400),
              InstanceTypeSpec('mem1_hdd1_v2_x8', 8, 16.0, 800),
              InstanceTypeSpec('mem1_hdd1_v2_x16', 16, 32.0, 1600),
              InstanceTypeSpec('mem1_hdd1_v2_x36', 36, 72.0, 3600),
              InstanceTypeSpec('mem1_hdd1_v2_x72', 72, 144.0, 7200),
              InstanceTypeSpec('mem1_hdd1_v2_x96', 96, 192.0, 9600),

              InstanceTypeSpec('mem1_hdd2_x1', 1, 1.7, 160),
              InstanceTypeSpec('mem1_hdd2_x8', 8, 7.0, 1680),
              InstanceTypeSpec('mem1_hdd2_x32', 32, 60.5, 3360),

              InstanceTypeSpec('mem2_hdd2_x1', 1, 3.8, 410),
              InstanceTypeSpec('mem2_hdd2_x2', 2, 7.5, 840),
              InstanceTypeSpec('mem2_hdd2_x4', 4, 15.0, 1680),

              InstanceTypeSpec('mem2_hdd2_v2_x2', 2, 8.0, 1000),
              InstanceTypeSpec('mem2_hdd2_v2_x4', 4, 16.0, 2000),

              InstanceTypeSpec('mem3_hdd2_x2', 2, 17.1, 420),
              InstanceTypeSpec('mem3_hdd2_x4', 4, 34.2, 850),
              InstanceTypeSpec('mem3_hdd2_x8', 8, 68.4, 1680),

              InstanceTypeSpec('mem3_hdd2_v2_x2', 2, 16.0, 500),
              InstanceTypeSpec('mem3_hdd2_v2_x4', 4, 32.0, 1000),
              InstanceTypeSpec('mem3_hdd2_v2_x8', 8, 64.0, 2000)):
        aws_other_instance_types[i.Name] = i

    default_instance_type = aws_preferred_instance_types['mem1_ssd1_v2_x4']

    standard_instance_types = OrderedDict()
    standard_instance_types.update(aws_preferred_instance_types)
    standard_instance_types.update(azure_preferred_instance_types)
    standard_instance_types.update(aws_other_instance_types)

    instance_types = OrderedDict()
    instance_types.update(standard_instance_types)
    instance_types.update(gpu_instance_types)
    instance_types.update(fpga_instance_types)

    instance_type_names = instance_types.keys()

    def complete(self, text, state):
        try:
            return self.instance_type_names[state]
        except IndexError:
            return None

    def __call__(self, prefix, parsed_args, **kwargs):
        return [name for name in self.instance_type_names if name.startswith(prefix)]
