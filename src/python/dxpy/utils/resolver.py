# Copyright (C) 2013-2014 DNAnexus, Inc.
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
This file contains all the utilities needed for escaping and parsing
names in the syntax of

    project-ID-or-name:folder/path/to/filename

For more details, see external documentation [TODO: Put link here].
'''

from __future__ import (print_function, unicode_literals)

import os, sys, json, re

import dxpy
from .describe import get_ls_l_desc
from ..exceptions import DXError
from ..compat import str, input
from ..cli import INTERACTIVE_CLI

def pick(choices, default=None, str_choices=None, prompt=None, allow_mult=False, more_choices=False):
    '''
    :param choices: Strings between which the user will make a choice
    :type choices: list of strings
    :param default: Number the index to be used as the default
    :type default: int or None
    :param str_choices: Strings to be used as aliases for the choices; must be of the same length as choices and each string must be unique
    :type str_choices: list of strings
    :param prompt: A custom prompt to be used
    :type prompt: string
    :param allow_mult: Whether "*" is a valid option to select all choices
    :type allow_mult: boolean
    :param more_choices: Whether "m" is a valid option to ask for more options
    :type more_choices: boolean
    :returns: The user's choice, i.e. one of a numbered index of choices (e.g. 0 for the first item), "*" (only if allow_mult is True), or "m" (only if more_results is True)
    :rtype: int or string
    :raises: :exc:`EOFError` to signify quitting the process

    At most one of allow_mult and more_choices should be set to True.
    '''
    for i in range(len(choices)):
        prefix = str(i) + ') '
        lines = choices[i].split("\n")
        joiner = "\n" + " " * len(prefix)
        print(prefix + joiner.join(lines))
    if more_choices:
        print('m) More options not shown...')
    print('')
    if prompt is None:
        prompt = 'Pick a numbered choice'
        if allow_mult:
            prompt += ' or "*" for all'
        elif more_choices:
            prompt += ' or "m" for more options'
        if default is not None:
            prompt += ' [' + str(default) + ']'
        prompt += ': '
    while True:
        try:
            value = input(prompt)
        except KeyboardInterrupt:
            print('')
            raise
        except EOFError:
            print('')
            raise
        if default is not None and value == '':
            return default
        if allow_mult and value == '*':
            return value
        if more_choices and value == 'm':
            return value
        try:
            choice = str_choices.index(value)
            return choice
        except:
            pass
        try:
            choice = int(value)
            if choice not in range(len(choices)):
                raise IndexError()
            return choice
        except Exception:
            print('Not a valid selection')

def paginate_and_pick(generator, render_fn=str, filter_fn=None, page_len=10, **pick_opts):
    any_results = False
    while True:
        results = []
        while len(results) < page_len:
            try:
                if filter_fn is None:
                    results.append(next(generator))
                else:
                    possible_next = next(generator)
                    if filter_fn(possible_next):
                        results.append(possible_next)
                any_results = True
            except StopIteration:
                break
        if not any_results:
            return "none found"
        elif len(results) == 0:
            return "none picked"

        try:
            choice = pick([render_fn(result) for result in results],
                          more_choices=(len(results) == page_len),
                          **pick_opts)
        except KeyboardInterrupt:
            return "none picked"
        if choice == 'm':
            continue
        else:
            return results[choice]

# The following caches project names to project IDs because they are
# unlikely to change.
cached_project_names = {}
# Possible cache for the future of project ID->folderpath->object name->ID
# cached_project_paths = {}

class ResolutionError(DXError):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

data_obj_pattern = re.compile('^(record|gtable|applet|file|workflow)-[0-9A-Za-z]{24}$')
hash_pattern = re.compile('^(record|gtable|app|applet|workflow|job|analysis|project|container|file)-[0-9A-Za-z]{24}$')
nohash_pattern = re.compile('^(user|org|app|team)-')

def is_hashid(string):
    return hash_pattern.match(string) is not None

def is_data_obj_id(string):
    return data_obj_pattern.match(string) is not None

def is_container_id(string):
    return is_hashid(string) and (string.startswith('project-') or string.startswith('container-'))

def is_analysis_id(string):
    return is_hashid(string) and string.startswith('analysis-')

def is_job_id(string):
    return is_hashid(string) and string.startswith('job-')

def is_localjob_id(thing):
    return (os.environ.get('DX_JOB_ID') is None and thing.startswith('localjob-'))

def is_nohash_id(string):
    return nohash_pattern.match(string) is not None

def is_glob_pattern(string):
    return (get_last_pos_of_char('*', string) >= 0) or (get_last_pos_of_char('?', string) >= 0)

# Special characters in bash to be escaped: #?*: ;&`"'/!$({[<>|~
def escaper(match):
    return "\\" + match.group(0)

def escape_folder_str(string):
    return re.sub("([#\?\*: ;&`\"'!$\(\)\{\[<>|~])", escaper, string.replace('\\', '\\\\'))

def escape_name_str(string):
    return re.sub("([#\?\*: ;&`\"'/!$\(\)\{\[<>|~])", escaper, string.replace('\\', '\\\\'))

def unescaper(match):
    return match.group(0)[1]

def unescape_folder_str(string):
    return re.sub("(\\\[#\?*: ;&`\"'!$\(\){[<>|~])", unescaper, string).replace('\\\\', '\\')

def unescape_name_str(string):
    return re.sub("(\\\[#\?*: ;&`\"'/!$\(\){[<>|~])", unescaper, string).replace('\\\\', '\\')

def get_last_pos_of_char(char, string):
    '''
    :param char: The character to find
    :type char: string
    :param string: The string in which to search for *char*
    :type string: string
    :returns: Index in *string* where *char* last appears (unescaped by a preceding "\\"), -1 if not found
    :rtype: int

    Finds the last occurrence of *char* in *string* in which *char* is
    not present as an escaped character.

    '''
    pos = len(string)
    while pos > 0:
        pos = string[:pos].rfind(char)
        if pos == -1:
            return -1
        num_backslashes = 0
        test_index = pos - 1
        while test_index >= 0 and string[test_index] == '\\':
            num_backslashes += 1
            test_index -= 1
        if num_backslashes % 2 == 0:
            return pos
    return -1

def get_first_pos_of_char(char, string):
    '''
    :param char: The character to find
    :type char: string
    :param string: The string in which to search for *char*
    :type string: string
    :returns: Index in *string* where *char* last appears (unescaped by a preceding "\\"), -1 if not found
    :rtype: int

    Finds the first occurrence of *char* in *string* in which *char* is
    not present as an escaped character.

    '''
    first_pos = -1
    pos = len(string)
    while pos > 0:
        pos = string[:pos].rfind(char)
        if pos == -1:
            return first_pos
        num_backslashes = 0
        test_index = pos - 1
        while test_index >= 0 and string[test_index] == '\\':
            num_backslashes += 1
            test_index -= 1
        if num_backslashes % 2 == 0:
            first_pos = pos
    return first_pos

def split_unescaped(char, string, include_empty_strings=False):
    '''
    :param char: The character on which to split the string
    :type char: string
    :param string: The string to split
    :type string: string
    :returns: List of substrings of *string*
    :rtype: list of strings

    Splits *string* whenever *char* appears without an odd number of
    backslashes ('\\') preceding it, discarding any empty string
    elements.

    '''
    words = []
    pos = len(string)
    lastpos = pos
    while pos >= 0:
        pos = get_last_pos_of_char(char, string[:lastpos])
        if pos >= 0:
            if pos + 1 != lastpos or include_empty_strings:
                words.append(string[pos + 1: lastpos])
            lastpos = pos
    if lastpos != 0 or include_empty_strings:
        words.append(string[:lastpos])
    words.reverse()
    return words

def clean_folder_path(path, expected=None):
    '''
    :param path: A folder path to sanitize and parse
    :type path: string
    :param expected: Whether a folder ("folder"), a data object ("entity"), or either (None) is expected
    :type expected: string or None
    :returns: *folderpath*, *name*

    Unescape and parse *path* as a folder path to possibly an entity
    name.  Consecutive unescaped forward slashes "/" are collapsed to
    a single forward slash.  If *expected* is "folder", *name* is
    always returned as None.  Otherwise, the string to the right of
    the last unescaped "/" is considered a possible data object name
    and returned as such.

    '''
    folders = split_unescaped('/', path)

    if len(folders) == 0:
        return '/', None

    if expected == 'folder' or folders[-1] == '.' or folders[-1] == '..' or get_last_pos_of_char('/', path) == len(path) - 1:
        entity_name = None
    else:
        entity_name = unescape_name_str(folders[-1])
        folders = folders[:-1]

    sanitized_folders = []

    for folder in folders:
        if folder == '.':
            pass
        elif folder == '..':
            if len(sanitized_folders) > 0:
                sanitized_folders.pop()
        else:
            sanitized_folders.append(unescape_folder_str(folder))

    if len(sanitized_folders) == 0:
        newpath = '/'
    else:
        newpath = ""
        for folder in sanitized_folders:
            newpath += '/' + folder

    return newpath, entity_name

def resolve_container_id_or_name(raw_string, is_error=False, unescape=True, multi=False):
    '''
    :param raw_string: A potential project or container ID or name
    :type raw_string: string
    :param is_error: Whether to raise an exception if the project or container ID cannot be resolved
    :type is_error: boolean
    :param unescape: Whether to unescaping the string is required (TODO: External link to section on escaping characters.)
    :type unescape: boolean
    :returns: Project or container ID if found or else None
    :rtype: string or None
    :raises: :exc:`ResolutionError` if *is_error* is True and the project or container could not be resolved

    Attempt to resolve *raw_string* to a project or container ID.

    '''
    if unescape:
        string = unescape_name_str(raw_string)
    if is_container_id(string):
        return ([string] if multi else string)

    if string in cached_project_names:
        return ([cached_project_names[string]] if multi else cached_project_names[string])

    try:
        results = list(dxpy.find_projects(name=string, describe=True, level='VIEW'))
    except Exception as details:
        raise ResolutionError(str(details))

    if len(results) == 1:
        cached_project_names[string] = results[0]['id']
        return ([results[0]['id']] if multi else results[0]['id'])
    elif len(results) == 0:
        if is_error:
            raise ResolutionError('Could not find a project named "' + string + '"')
        return ([] if multi else None)
    elif not multi:
        if INTERACTIVE_CLI:
            print('Found multiple projects with name "' + string + '"')
            choice = pick(['{id} ({level})'.format(id=result['id'], level=result['level'])
                           for result in results])
            return results[choice]['id']
        else:
            raise ResolutionError('Found multiple projects with name "' + string + '"; please use a project ID to specify the desired project')
    else:
        # len(results) > 1 and multi
        return [result['id'] for result in results]

def resolve_path(path, expected=None, expected_classes=None, multi_projects=False, allow_empty_string=True):
    '''
    :param path: A path to a data object to attempt to resolve
    :type path: string
    :param expected: one of the following: "folder", "entity", or None to indicate whether the expected path is a folder, a data object, or either
    :type expected: string or None
    :param expected_classes: a list of DNAnexus data object classes (if any) by which the search can be filtered
    :type expected_classes: list of strings or None
    :returns: A tuple of 3 values: container_ID, folderpath, entity_name
    :rtype: string, string, string
    :raises: exc:`ResolutionError` if 1) a colon is provided but no project can be resolved, or 2) *expected* was set to "folder" but no project can be resolved from which to establish context
    :param allow_empty_string: If false, a ResolutionError will be raised if *path* is an empty string. Use this when resolving the empty string could result in unexpected behavior.
    :type allow_empty_string: boolean

    Attempts to resolve *path* to a project or container ID, a folder
    path, and a data object or folder name.  This method will NOT
    raise an exception if the specified folder or object does not
    exist.  This method is primarily for parsing purposes.

    '''

    if '_DX_FUSE' in os.environ:
        from xattr import xattr
        path = xattr(path)['project'] + ":" + xattr(path)['id']

    if path == '' and not allow_empty_string:
        raise ResolutionError('Cannot parse ""; expected the path to be a non-empty string')
    try:
        possible_hash = json.loads(path)
        if isinstance(possible_hash, dict) and '$dnanexus_link' in possible_hash:
            if isinstance(possible_hash['$dnanexus_link'], basestring):
                path = possible_hash['$dnanexus_link']
            elif isinstance(possible_hash['$dnanexus_link'], dict) and isinstance(possible_hash['$dnanexus_link'].get('project', None), basestring) and isinstance(possible_hash['$dnanexus_link'].get('id', None), basestring):
                path = possible_hash['$dnanexus_link']['project'] + ':' + possible_hash['$dnanexus_link']['id']
    except:
        pass

    # Easy case: ":"
    if path == ':':
        if dxpy.WORKSPACE_ID is None:
            raise ResolutionError('Cannot parse ":"; expected a project name or ID to the left of a colon or for a current project to be set')
        return ([dxpy.WORKSPACE_ID] if multi_projects else dxpy.WORKSPACE_ID), '/', None
    # Second easy case: empty string
    if path == '':
        if dxpy.WORKSPACE_ID is None:
            raise ResolutionError('Expected a project name or ID to the left of a colon or for a current project to be set')
        return ([dxpy.WORKSPACE_ID] if multi_projects else dxpy.WORKSPACE_ID), os.environ.get('DX_CLI_WD', '/'), None
    # Third easy case: hash ID
    if is_container_id(path):
        return ([path] if multi_projects else path), '/', None
    elif is_hashid(path):
        return ([dxpy.WORKSPACE_ID] if multi_projects else dxpy.WORKSPACE_ID), None, path

    # using a numerical sentinel value to indicate that it hasn't been
    # set in case dxpy.WORKSPACE_ID is actually None
    project = 0
    folderpath = None
    entity_name = None
    wd = None

    # Test for multiple colons
    last_colon = get_last_pos_of_char(':', path)
    if last_colon >= 0:
        last_last_colon = get_last_pos_of_char(':', path[:last_colon])
        if last_last_colon >= 0:
            raise ResolutionError('Cannot parse "' + path + '" as a path; at most one unescaped colon can be present')

    substrings = split_unescaped(':', path)

    if len(substrings) == 2:
        # One of the following:
        # 1) job-id:fieldname
        # 2) project-name-or-id:folderpath/to/possible/entity
        if is_job_id(substrings[0]):
            return ([substrings[0]] if multi_projects else substrings[0]), None, substrings[1]

        if multi_projects:
            project_ids = resolve_container_id_or_name(substrings[0], is_error=True, multi=True)
        else:
            project = resolve_container_id_or_name(substrings[0], is_error=True)
        wd = '/'
    elif get_last_pos_of_char(':', path) >= 0:
        # :folderpath/to/possible/entity OR project-name-or-id:
        # Colon is either at the beginning or at the end
        wd = '/'
        if path.startswith(':'):
            if dxpy.WORKSPACE_ID is None:
                raise ResolutionError('Cannot parse "' + path + '" as a path; expected a project name or ID to the left of a colon or for a current project to be set')
            project = dxpy.WORKSPACE_ID
        else:
            # One nonempty string to the left of a colon
            project = resolve_container_id_or_name(substrings[0], is_error=True)
            folderpath = '/'
    else:
        # One nonempty string, no colon present, do NOT interpret as
        # project
        project = dxpy.WORKSPACE_ID
        if expected == 'folder' and project is None:
            raise ResolutionError('a project context was expected for a path, but a current project is not set, nor was one provided in the path (preceding a colon) in "' + path + '"')
        wd = dxpy.config.get('DX_CLI_WD', u'/')

    # Determine folderpath and entity_name if necessary
    if folderpath is None:
        folderpath = substrings[-1]
        folderpath, entity_name = clean_folder_path(('' if folderpath.startswith('/') else wd + '/') + folderpath, expected)

    if multi_projects:
        return (project_ids if project == 0 else [project]), folderpath, entity_name
    else:
        return project, folderpath, entity_name

def resolve_job_ref(job_id, name, describe={}):
    try:
        job_desc = dxpy.api.job_describe(job_id)
    except Exception as details:
        raise ResolutionError(str(details))
    project = job_desc['project']
    describe['project'] = project
    if job_desc['state'] != 'done':
        raise ResolutionError('the job ' + job_id + ' is ' + job_desc['state'] + ', and it must be in the done state for its outputs to be accessed')

    index = None
    if '.' in name:
        try:
            actual_name, str_index = name.rsplit('.', 1)
            index = int(str_index)
            name = actual_name
        except ValueError:
            pass

    output_field = job_desc['output'].get(name, None)
    if index is not None:
        if not isinstance(output_field, list):
            raise ResolutionError('Found "' + name + '" as an output field name of ' + job_id +
                                  ', but it is not an array and cannot be indexed')
        if index < 0 or index >= len(output_field):
            raise ResolutionError('Found "' + name + '" as an output field name of ' + job_id +
                                  ', but the specified index ' + str_index + ' is out of range')
        output_field = output_field[index]
    results = []
    if output_field is not None:
        if isinstance(output_field, list):
            if len(output_field) > 0:
                if not isinstance(output_field[0], dict) or '$dnanexus_link' not in output_field[0]:
                    raise ResolutionError('Found "' + name + '" as an output field name of ' + job_id + ', but it is an array of non-data objects')
                ids = [link['$dnanexus_link'] for link in output_field]
                try:
                    results = [{"id": out_id,
                                "describe": dxpy.DXHTTPRequest('/' + out_id + '/describe', describe)} for out_id in ids]
                except Exception as details:
                    raise ResolutionError(str(details))
            else:
                raise ResolutionError('Found "' + name + '" as an output field name of ' + job_id + ', but it is an empty array')
        elif isinstance(output_field, dict) and '$dnanexus_link' in output_field:
            obj_id = output_field['$dnanexus_link']
            try:
                results = [{"id": obj_id, "describe": dxpy.DXHTTPRequest('/' + obj_id + '/describe', describe)}]
            except Exception as details:
                raise ResolutionError(str(details))
        else:
            raise ResolutionError('Found "' + name + '" as an output field name of ' + job_id + ', but it is not of a data object class')
    else:
        raise ResolutionError('Could not find "' + name + '" as an output field name of ' + job_id + '; available fields are: ' + ', '.join(job_desc['output'].keys()))

    return results

def resolve_existing_path(path, expected=None, ask_to_resolve=True, expected_classes=None, allow_mult=False, describe={}, all_mult=False, allow_empty_string=True,
                          visibility="either"):
    '''
    :param ask_to_resolve: Whether picking may be necessary (if true, a list is returned; if false, only one result is returned)
    :type ask_to_resolve: boolean
    :param allow_mult: Whether to allow the user to select multiple results from the same path
    :type allow_mult: boolean
    :param describe: Input hash to describe call for the results
    :type describe: dict
    :param all_mult: Whether to return all matching results without prompting (only applicable if allow_mult == True)
    :type all_mult: boolean
    :returns: A LIST of results when ask_to_resolve is False or allow_mult is True
    :raises: :exc:`ResolutionError` if the request path was invalid, or a single result was requested and input is not a TTY
    :param allow_empty_string: If false, a ResolutionError will be raised if *path* is an empty string. Use this when resolving the empty string could result in unexpected behavior.
    :type allow_empty_string: boolean
    :param visibility: The visibility expected ("either", "hidden", or "visible")
    :type visibility: string

    Returns either a list of results or a single result (depending on
    how many is expected; if only one, then an interactive picking of
    a choice will be initiated if input is a tty, or else throw an error).

    TODO: Always treats the path as a glob pattern.

    Output is of the form {"id": id, "describe": describe hash} a list
    of those

    TODO: Allow arbitrary flags for the describe hash.

    NOTE: if expected_classes is provided and conflicts with the class
    of the hash ID, it will return None for all fields.
    '''
    project, folderpath, entity_name = resolve_path(path, expected, allow_empty_string=allow_empty_string)

    if entity_name is None:
        # Definitely a folder (or project)
        # TODO: find a good way to check if folder exists and expected=folder
        return project, folderpath, entity_name
    elif is_hashid(entity_name):
        found_valid_class = True
        if expected_classes is not None:
            found_valid_class = False
            for klass in expected_classes:
                if entity_name.startswith(klass):
                    found_valid_class = True
        if not found_valid_class:
            return None, None, None

        if 'project' not in describe:
            if project != dxpy.WORKSPACE_ID:
                describe['project'] = project
            elif dxpy.WORKSPACE_ID is not None:
                describe['project'] = dxpy.WORKSPACE_ID
        try:
            desc = dxpy.DXHTTPRequest('/' + entity_name + '/describe', describe)
        except Exception as details:
            if 'project' in describe:
                # Now try it without the hint
                del describe['project']
                try:
                    desc = dxpy.DXHTTPRequest('/' + entity_name + '/describe', describe)
                except Exception as details:
                    raise ResolutionError(str(details))
            else:
                raise ResolutionError(str(details))
        result = {"id": entity_name, "describe": desc}
        if ask_to_resolve and not allow_mult:
            return project, folderpath, result
        else:
            return project, folderpath, [result]
    elif project is None:
        raise ResolutionError('Could not resolve "' + path + '" to a project context.  Please either set a default project using dx select or cd, or add a colon (":") after your project ID or name')
    else:
        msg = 'Object of name ' + str(entity_name) + ' could not be resolved in folder ' + str(folderpath) + ' of project ID ' + str(project)
        # Probably an object
        if is_job_id(project):
            # The following will raise if no results could be found
            results = resolve_job_ref(project, entity_name, describe=describe)

            # If results able to resolve without error, project will be
            # incorporated into results assuming results were found.
            project = None
        else:
            try:
                results = list(dxpy.find_data_objects(project=project,
                                                      folder=folderpath,
                                                      name=entity_name,
                                                      name_mode='glob',
                                                      recurse=False,
                                                      describe=describe,
                                                      visibility=visibility))
            except Exception as details:
                raise ResolutionError(str(details))
        if len(results) == 0:
            # Could not find it as a data object.  If anything, it's a
            # folder.
            if '/' in entity_name:
                # Then there's no way it's supposed to be a folder
                raise ResolutionError(msg)

            # This is the only possibility left.  Leave the
            # error-checking for later.  Note that folderpath does
            possible_folder = folderpath + '/' + entity_name
            possible_folder, _skip = clean_folder_path(possible_folder, 'folder')

            # Check that the folder specified actually exists, and raise error if it doesn't
            if not check_folder_exists(project, folderpath, entity_name):
                raise ResolutionError('Unable to resolve "' + entity_name +
                                      '" to a data object or folder name in \'' + folderpath + "'")
            return project, possible_folder, None

        # Caller wants ALL results; just return the whole thing
        if not ask_to_resolve:
            return project, None, results

        if len(results) > 1:
            if allow_mult and (all_mult or is_glob_pattern(entity_name)):
                return project, None, results
            if INTERACTIVE_CLI:
                print('The given path "' + path + '" resolves to the following data objects:')
                choice = pick([get_ls_l_desc(result['describe']) for result in results],
                              allow_mult=allow_mult)
                if allow_mult and choice == '*':
                    return project, None, results
                else:
                    return project, None, ([results[choice]] if allow_mult else results[choice])
            else:
                raise ResolutionError('The given path "' + path + '" resolves to ' + str(len(results)) + ' data objects')
        elif len(results) == 1:
            return project, None, ([results[0]] if allow_mult else results[0])


def check_folder_exists(project, path, folder_name):
    '''
    :param project: project id
    :type project: string
    :param path: path to where we should look for the folder in question
    :type path: string
    :param folder_name: name of the folder in question
    :type folder_name: string
    :returns: A boolean True or False whether the folder exists at the specified path
    :type: boolean
    :raises: :exc:'ResolutionError' if dxpy.api.container_list_folder raises an exception

    This function returns a boolean value that indicates whether a folder of the
    specified name exists at the specified path

    Note: this function will NOT work on the root folder case, i.e. '/'
    '''
    if folder_name is None or path is None:
        return False
    try:
        folder_list = dxpy.api.container_list_folder(project, {"folder": path, "only": "folders"})
    except dxpy.exceptions.DXAPIError as e:
        if e.name == 'ResourceNotFound':
            raise ResolutionError(str(e.msg))
        else:
            raise e
    target_folder = path + '/' + folder_name
    # sanitize input if necessary
    target_folder, _skip = clean_folder_path(target_folder, 'folder')

    # Check that folder name exists in return from list folder API call
    return target_folder in folder_list['folders']

def get_app_from_path(path):
    '''
    :param path: A string to attempt to resolve to an app object
    :type path: string
    :returns: The describe hash of the app object if found, or None otherwise
    :rtype: dict or None

    This method parses a string that is expected to perhaps refer to
    an app object.  If found, its describe hash will be returned.  For
    more information on the contents of this hash, see the API
    documentation. [TODO: external link here]

    '''
    alias = None
    if not path.startswith('app-'):
        path = 'app-' + path
    if '/' in path:
        alias = path[path.find('/') + 1:]
        path = path[:path.find('/')]
    try:
        if alias is None:
            desc = dxpy.DXHTTPRequest('/' + path + '/describe', {})
        else:
            desc = dxpy.DXHTTPRequest('/' + path + '/' + alias + '/describe', {})
        return desc
    except dxpy.DXAPIError:
        return None

def resolve_app(path):
    '''
    :param path: A string which is supposed to identify an app
    :type path: string
    :returns: The describe hash of the app object
    :raises: :exc:`ResolutionError` if it cannot be found

    *path* is expected to have one of the following forms:

    - hash ID, e.g. "app-B8GZ8bQ0xky1PKY6FjGQ000J"
    - named ID, e.g. "app-myapp"
    - named ID with alias (version or tag), e.g. "app-myapp/1.2.0"
    '''
    desc = get_app_from_path(path)
    if desc is None:
        raise ResolutionError('The given path "' + path + '" could not be resolved to an accessible app')
    else:
        return desc

def get_exec_handler(path, alias=None):
    handler = None
    def get_handler_from_desc(desc):
        if desc['class'] == 'applet':
            return dxpy.DXApplet(desc['id'], project=desc['project'])
        elif desc['class'] == 'app':
            return dxpy.DXApp(dxid=desc['id'])
        else:
            return dxpy.DXWorkflow(desc['id'], project=desc['project'])

    if alias is None:
        app_desc = get_app_from_path(path)
        try:
            # Look for applets and workflows
            _project, _folderpath, entity_results = resolve_existing_path(path,
                                                                          expected='entity',
                                                                          ask_to_resolve=False,
                                                                          expected_classes=['applet', 'record', 'workflow'],
                                                                          visibility="visible")
            def is_applet_or_workflow(i):
                return (i['describe']['class'] in ['applet', 'workflow'])
            if entity_results is not None:
                entity_results = [i for i in entity_results if is_applet_or_workflow(i)]
                if len(entity_results) == 0:
                    entity_results = None
        except ResolutionError:
            if app_desc is None:
                raise
            else:
                entity_results = None

        if entity_results is not None and len(entity_results) == 1 and app_desc is None:
            handler = get_handler_from_desc(entity_results[0]['describe'])
        elif entity_results is None and app_desc is not None:
            handler = get_handler_from_desc(app_desc)
        elif entity_results is not None:
            if not INTERACTIVE_CLI:
                raise ResolutionError('Found multiple executables with the path ' + path)
            print('Found multiple executables with the path ' + path)
            choice_descriptions = [get_ls_l_desc(r['describe']) for r in entity_results]
            if app_desc is not None:
                choice_descriptions.append('app-' + app_desc['name'] + ', version ' + app_desc['version'])
            choice = pick(choice_descriptions)
            if choice < len(entity_results):
                # all applet/workflow choices show up before the app,
                # of which there is always at most one possible choice
                handler = get_handler_from_desc(entity_results[choice]['describe'])
            else:
                handler = get_handler_from_desc(app_desc)
        else:
            raise ResolutionError("No matches found for " + path)
    else:
        if path.startswith('app-'):
            path = path[4:]
        handler = dxpy.DXApp(name=path, alias=alias)
    return handler

def resolve_to_objects_or_project(path, all_matching_results=False):
    '''
    :param path: Path to resolve
    :type path: string
    :param all_matching_results: Whether to return a list of all matching results
    :type all_matching_results: boolean

    A thin wrapper over :meth:`resolve_existing_path` which throws an
    error if the path does not look like a project and doesn't match a
    data object path.

    Returns either a list of results or a single result (depending on
    how many is expected; if only one, then an interactive picking of
    a choice will be initiated if input is a tty, or else throw an error).
    '''
    # Attempt to resolve name
    project, folderpath, entity_results = resolve_existing_path(path,
                                                                expected='entity',
                                                                allow_mult=True,
                                                                all_mult=all_matching_results)
    if entity_results is None and not is_container_id(path):
        if folderpath != None and folderpath != '/':
            raise ResolutionError('Could not resolve "' + path + \
                                  '''" to an existing data object or to only a project;
                                  if you were attempting to refer to a project by name,
                                  please append a colon ":" to indicate that it is a project.''')
    return project, folderpath, entity_results
