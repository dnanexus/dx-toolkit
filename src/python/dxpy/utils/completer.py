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

import os
import dxpy
from dxpy.utils.resolver import *

def startswith(text):
    return (lambda string: string.startswith(text))

def escape_completion_name_str(string):
    return string.replace('\\', '\\\\\\\\').replace(' ', '\ ').replace(':', '\\\\:').replace('/', '\\\\/').replace('*', '\\\\\\\\*').replace('?', '\\\\\\\\?').replace('(', '\\(').replace(')', '\\)')

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
        return filter(startswith(text),
                      map(lambda folder_name:
                              text[:delim_pos + 1] + \
                              escape_completion_name_str(folder_name) + \
                              '/',
                          folder_names + (['.', '..'] if text != '' and delim_pos != len(text) - 1 else [])))
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

    try:
        results = list(dxpy.find_data_objects(project=dxproj.get_id(),
                                              folder=folderpath,
                                              recurse=False,
                                              visibility='either' if text != '' and delim_pos != len(text) - 1 else 'visible',
                                              classname=classname,
                                              limit=100,
                                              describe=True,
                                              typename=typespec))
        names = map(lambda result: result['describe']['name'], results)
        return filter(startswith(text),
                      map(lambda name:
                              ('' if text == '' else text[:delim_pos + 1]) + escape_completion_name_str(name),
                          names))
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
    if colon_pos < 0 and slash_pos < 0:
        # Might be tab-completing a project, but don't ever include
        # whatever's set as dxpy.WORKSPACE_ID unless expected == "project"
        # Also, don't bother if text=="" and expected is NOT "project"
        if text != "" or expected == 'project':
            results = filter(lambda result: result['id'] != dxpy.WORKSPACE_ID or include_current_proj,
                             list(dxpy.find_projects(describe=True, level=perm_level)))
            matches += filter(startswith(text),
                              [(escape_completion_name_str(result['describe']['name']) + ':') for result in results])

    if expected == 'project':
        return matches

    # Attempt to tab-complete to a folder or data object name
    if colon_pos < 0 and slash_pos >= 0:
        # Not tab-completing a project, and the project is unambiguous
        # (use dxpy.WORKSPACE_ID)
        if dxpy.WORKSPACE_ID is not None:
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
    else:
        # project is ambiguous, but attempt to resolve to an object or folder
        proj_ids, folderpath, entity_name = resolve_path(text, multi_projects=True)
        for proj in proj_ids:
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
        # This implementation is reliant on bash behavior that ':' is
        # treated as a word separator for determining prefix
        if get_last_pos_of_char(' ', line) != point - 1:
            prefix = split_unescaped(' ', line[:point])[-1]
        self.matches = path_completer(prefix, self.expected, self.classes,
                                      typespec=self.typespec,
                                      include_current_proj=self.include_current_proj)
        if get_last_pos_of_char(':', prefix) != -1:
            for i in range(len(self.matches)):
                self.matches[i] = self.matches[i][get_last_pos_of_char(':', self.matches[i]) + 1:]
                
        return self.matches

    def __call__(self, text, state):
        if state == 0:
            self.matches = path_completer(text, self.expected, self.classes,
                                          typespec=self.typespec,
                                          include_current_proj=self.include_current_proj)

        if state < len(self.matches):
            return self.matches[state]
        else:
            return None

class DXAppCompleter():
    def __init__(self, installed=None):
        self.matches = []
        self.installed = installed

    def _populate_matches(self, prefix):
        appnames = [result['describe']['name'] for result in dxpy.find_apps(describe=True) if self.installed is None or (self.installed == result['describe']['installed'])]
        self.matches = [name for name in appnames if name.startswith(prefix)]
        if prefix != '':
            appnames_with_prefix = [('app-' + name) for name in appnames]
            self.matches += [name for name in appnames_with_prefix if name.startswith(prefix)]

    def get_matches(self, line, point, prefix, suffix):
        self._populate_matches(prefix)
        return self.matches

    def __call__(self, text, state):
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
        self.matches = [match for match in [path.replace(' ', '\ ') for path in os.listdir(os.getcwdu())] if match.startswith(prefix)]

    def get_matches(self, line, point, prefix, suffix):
        self._populate_matches(prefix)
        return self.matches

    def __call__(self, text, state):
        if state == 0:
            self._populate_matches(text)

        if state < len(self.matches):
            return self.matches[state].replace(' ', '\ ')
        else:
            return None

class NoneCompleter():
    def get_matches(self, line, point, prefix, suffix):
        return []

    def __call__(self, text, state):
        return None

class ListCompleter():
    def __init__(self, completions):
        self.completions = completions
        self.matches = []

    def _populate_matches(self, prefix):
        self.matches = [ans for ans in self.completions if ans.startswith(prefix)]

    def get_matches(self, line, point, prefix, suffix):
        self._populate_matches(prefix)
        return self.matches

    def __call__(self, text, state):
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
            completer._populate_matches(prefix)
            self.matches += completer.matches

    def get_matches(self, line, point, prefix, suffix):
        self._populate_matches(prefix)
        return self.matches

    def __call__(self, text, state):
        if state == 0:
            self._populate_matches(text)

        if state < len(self.matches):
            return self.matches[state]
        else:
            return None
