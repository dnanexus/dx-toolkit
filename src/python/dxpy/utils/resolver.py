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
This file contains all the utilities needed for escaping and parsing
names in the syntax of

    project-ID-or-name:folder/path/to/filename

For more details, see external documentation [TODO: Put link here].
'''

from __future__ import print_function, unicode_literals, division, absolute_import

import os, sys, json, re

import dxpy
from .describe import get_ls_l_desc
from ..exceptions import DXError
from ..compat import str, input, basestring
from ..cli import try_call, INTERACTIVE_CLI

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

data_obj_pattern = re.compile('^(record|gtable|applet|file|workflow|database)-[0-9A-Za-z]{24}$')
hash_pattern = re.compile('^(record|gtable|app|applet|workflow|globalworkflow|job|analysis|project|container|file|database)-[0-9A-Za-z]{24}$')
nohash_pattern = re.compile('^(user|org|app|globalworkflow|team)-')
jbor_pattern = re.compile('^(job|analysis)-[0-9A-Za-z]{24}:[a-zA-Z_][0-9a-zA-Z_]*$')

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


def is_jbor_str(string):
    return jbor_pattern.match(string) is not None


def is_project_explicit(path):
    """
    Returns True if the specified path explicitly specifies a project.
    """
    # This method encodes our rules for deciding when a path shows an explicit
    # affinity to a particular project. This is a stronger notion than just
    # saying that the path resolves to an object in that project.
    #
    # For an explanation of the rules, see the unit tests
    # (test_dxpy.TestResolver.test_is_project_explicit).
    #
    # Note, this method need not validate that the path can otherwise be
    # resolved; it can assume this as a precondition.
    path = _maybe_convert_stringified_dxlink(path)
    return not is_hashid(path)


def object_exists_in_project(obj_id, proj_id):
    '''
    :param obj_id: object ID
    :type obj_id: str
    :param proj_id: project ID
    :type proj_id: str

    Returns True if the specified data object can be found in the specified
    project.
    '''
    if obj_id is None:
        raise ValueError("Expected obj_id to be a string")
    if proj_id is None:
        raise ValueError("Expected proj_id to be a string")
    if not is_container_id(proj_id):
        raise ValueError('Expected %r to be a container ID' % (proj_id,))
    return try_call(dxpy.DXHTTPRequest, '/' + obj_id + '/describe', {'project': proj_id})['project'] == proj_id


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
        entity_name = unescape_name_str(folders.pop())

    sanitized_folders = []

    for folder in folders:
        if folder == '.':
            pass
        elif folder == '..':
            if len(sanitized_folders) > 0:
                sanitized_folders.pop()
        else:
            sanitized_folders.append(unescape_folder_str(folder))

    return ('/' + '/'.join(sanitized_folders)), entity_name


def resolve_container_id_or_name(raw_string, is_error=False, multi=False):
    '''
    :param raw_string: A potential project or container ID or name
    :type raw_string: string
    :param is_error: Whether to raise an exception if the project or
            container ID cannot be resolved
    :type is_error: boolean
    :returns: Project or container ID if found or else None
    :rtype: string or None
    :raises: :exc:`ResolutionError` if *is_error* is True and the
            project or container could not be resolved

    Unescapes and attempts to resolve *raw_string* to a project or
    container ID.

    '''
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


def _maybe_convert_stringified_dxlink(path):
    try:
        possible_hash = json.loads(path)
        if isinstance(possible_hash, dict) and '$dnanexus_link' in possible_hash:
            if isinstance(possible_hash['$dnanexus_link'], basestring):
                return possible_hash['$dnanexus_link']
            elif (isinstance(possible_hash['$dnanexus_link'], dict) and
                  isinstance(possible_hash['$dnanexus_link'].get('project', None), basestring) and
                  isinstance(possible_hash['$dnanexus_link'].get('id', None), basestring)):
                return possible_hash['$dnanexus_link']['project'] + ':' + possible_hash['$dnanexus_link']['id']
    except:
        pass
    return path


def resolve_path(path, expected=None, multi_projects=False, allow_empty_string=True):
    '''
    :param path: A path to a data object to attempt to resolve
    :type path: string
    :param expected: one of the following: "folder", "entity", or None
            to indicate whether the expected path is a folder, a data
            object, or either
    :type expected: string or None
    :returns: A tuple of 3 values: container_ID, folderpath, entity_name
    :rtype: string, string, string
    :raises: exc:`ResolutionError` if the project cannot be resolved by
            name or the path is malformed
    :param allow_empty_string: If false, a ResolutionError will be
            raised if *path* is an empty string. Use this when resolving
            the empty string could result in unexpected behavior.
    :type allow_empty_string: boolean

    Attempts to resolve *path* to a project or container ID, a folder
    path, and a data object or folder name.  This method will NOT
    raise an exception if the specified folder or object does not
    exist.  This method is primarily for parsing purposes.

    Returns one of the following:

      (project, folder, maybe_name)
      where
        project is a container ID (non-null)
        folder is a folder path
        maybe_name is a string if the path could represent a folder or an object, or
        maybe_name is None if the path could only represent a folder

    OR

      (maybe_project, None, object_id)
      where
        maybe_project is a container ID or None
        object_id is a dataobject, app, or execution (specified by ID, not name)

    OR

      (job_id, None, output_name)
      where
        job_id and output_name are both non-null

    '''
    # TODO: callers that intend to obtain a data object probably won't be happy
    # with an app or execution ID. Callers should probably have to specify
    # whether they are okay with getting an execution ID or not.

    # TODO: callers that are looking for a place to write data, rather than
    # read it, probably won't be happy with receiving an object ID, or a
    # JBOR. Callers should probably specify whether they are looking for an
    # "LHS" expression or not.

    if '_DX_FUSE' in os.environ:
        from xattr import xattr
        path = xattr(path)['project'] + ":" + xattr(path)['id']

    if path == '' and not allow_empty_string:
        raise ResolutionError('Cannot parse ""; expected the path to be a non-empty string')
    path = _maybe_convert_stringified_dxlink(path)

    # Easy case: ":"
    if path == ':':
        if dxpy.WORKSPACE_ID is None:
            raise ResolutionError("Cannot resolve \":\": expected a project name or ID "
                                  "to the left of the colon, or for a current project to be set")
        return ([dxpy.WORKSPACE_ID] if multi_projects else dxpy.WORKSPACE_ID), '/', None
    # Second easy case: empty string
    if path == '':
        if dxpy.WORKSPACE_ID is None:
            raise ResolutionError('Expected a project name or ID to the left of a colon, '
                                  'or for a current project to be set')
        return ([dxpy.WORKSPACE_ID] if multi_projects else dxpy.WORKSPACE_ID), dxpy.config.get('DX_CLI_WD', '/'), None
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
    wd = dxpy.config.get('DX_CLI_WD', u'/')

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
                raise ResolutionError('Cannot resolve "%s": expected a project name or ID to the left of the '
                                      'colon, or for a current project to be set' % (path,))
            project = dxpy.WORKSPACE_ID
        else:
            # One nonempty string to the left of a colon
            project = resolve_container_id_or_name(substrings[0], is_error=True)
            folderpath = '/'
    else:
        # One nonempty string, no colon present, do NOT interpret as
        # project
        project = dxpy.WORKSPACE_ID
        if project is None:
            raise ResolutionError('Cannot resolve "%s": expected the path to be qualified with a project name or ID, '
                                  'and a colon; or for a current project to be set' % (path,))

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


def _check_resolution_needed(path, project, folderpath, entity_name, expected_classes=None, describe=True,
                             enclose_in_list=False):
    """
    :param path: Path to the object that required resolution; propagated from
                 command-line
    :type path: string
    :param project: The potential project the entity belongs to
    :type project: string
    :param folderpath: Path to the entity
    :type folderpath: string
    :param entity_name: The name of the entity
    :type entity_name: string
    :param expected_classes: A list of expected classes the entity is allowed
                             to belong to if it is an ID (e.g. "record",
                             "file", "job"); if None, then entity_name may be
                             any data object class
    :type expected_classes: list or None
    :param describe: Dictionary of inputs to the describe API call; if
                     no describe input is provided (default value True), then
                     an empty mapping is passed to the describe API method
    :type describe: dict or True
    :param enclose_in_list: Whether the describe output is to be in the form
                            of a list (if False, the last return value is a
                            dictionary; if True, the last return value is a
                            list of one dictionary); it will only have an
                            effect if entity_name is a DX ID and is described
    :type enclose_in_list: boolean
    :returns: Whether or not the entity needs to be resolved with a more
              general resolution method, the project, the folderpath, and the
              entity name
    :rtype: tuple of 4 elements
    :raises: ResolutionError if the entity fails to be described

    Attempts to resolve the entity to a folder or an object, and describes
    the entity iff it is a DX ID of an expected class in the list
    expected_classes.
    Otherwise, determines whether or not more general resolution may be able
    to resolve the entity.

    If a more general resolution method is needed, then the return values will
    look like:
    (True, <project>, <folderpath>, <entity_name>)

    If the entity is a DX ID, but is not one of the supplied expected
    classes, then the return values will look like:
    (False, None, None, None)

    If the entity can be successfully described, then the return values will
    look like:
    <desc_output> ::= {"id": entity_name, "describe": {...}}
    <desc_or_desc_list> ::= <desc_output> || [<desc_output>]
    (False, <project>, <folderpath>, <desc_or_desc_list>)

    If the entity may be a folder, then the return values will look like:
    (False, <project>, <folderpath>, None)

    TODO: Allow arbitrary flags for the describe mapping.
    """
    if entity_name is None:
        # Definitely a folder (or project)
        # TODO: find a good way to check if folder exists and expected=folder
        return False, project, folderpath, None
    elif is_hashid(entity_name):

        found_valid_class = True
        if expected_classes is not None:
            found_valid_class = False
            for klass in expected_classes:
                if entity_name.startswith(klass):
                    found_valid_class = True
        if not found_valid_class:
            return False, None, None, None

        if describe is True:
            describe = {}

        # entity is an ID of a valid class, try to describe it
        if 'project' not in describe:
            if project != dxpy.WORKSPACE_ID:
                describe['project'] = project
            elif dxpy.WORKSPACE_ID is not None:
                describe['project'] = dxpy.WORKSPACE_ID
        try:
            desc = dxpy.DXHTTPRequest('/' + entity_name + '/describe', describe)
            desc = dxpy.append_underlying_workflow_describe(desc)
        except Exception as details:
            if 'project' in describe:
                # Now try it without the hint
                del describe['project']
                try:
                    desc = dxpy.DXHTTPRequest('/' + entity_name + '/describe', describe)
                except Exception as details2:
                    raise ResolutionError(str(details2))
            else:
                raise ResolutionError(str(details))
        result = {"id": entity_name, "describe": desc}
        if enclose_in_list:
            return False, project, folderpath, [result]
        else:
            return False, project, folderpath, result

    else:
        # Need to resolve later
        return True, project, folderpath, entity_name


def _resolve_folder(project, parent_folder, folder_name):
    """
    :param project: The project that the folder belongs to
    :type project: string
    :param parent_folder: Full path to the parent folder that contains
                         folder_name
    :type parent_folder: string
    :param folder_name: Name of the folder
    :type folder_name: string
    :returns: The path to folder_name, if it exists, in the form of
              "<parent_folder>/<folder_name>"
    :rtype: string
    :raises: ResolutionError if folder_name is not a folder, or if
             folder_name points to a folder that does not exist

    Attempts to resolve folder_name at location parent_folder in project.
    """
    if '/' in folder_name:
        # Then there's no way it's supposed to be a folder
        raise ResolutionError('Object of name ' + str(folder_name) + ' could not be resolved in folder ' +
                              str(parent_folder) + ' of project ID ' + str(project))
    possible_folder, _skip = clean_folder_path(parent_folder + '/' + folder_name, 'folder')

    if not check_folder_exists(project, parent_folder, folder_name):
        raise ResolutionError('Unable to resolve "' + folder_name +
                              '" to a data object or folder name in \'' + parent_folder + "'")
    return possible_folder


def _validate_resolution_output_length(path, entity_name, results, allow_mult=False, all_mult=False,
                                       ask_to_resolve=True):
    """
    :param path: Path to the object that required resolution; propagated from
                 command-line
    :type path: string
    :param entity_name: Name of the object
    :type entity_name: string
    :param results: Result of resolution; non-empty list of object
                    specifications (each specification is a dictionary with
                    keys "project" and "id")
    :type results: list of dictionaries
    :param allow_mult: If True, it is okay to choose from multiple results
                       of a single resolved object, or return all results
                       found; if False, raise an error if multiple results
                       are found
    :type allow_mult: boolean
    :param all_mult: If True, return all results if multiple results are
                     found for a single resolved object; if False, user needs
                     to choose a single result if multiple are found; the value
                     of all_mult only has an effect if allow_mult is True)
    :type all_mult: boolean
    :param ask_to_resolve: Whether picking may be necessary (if True, a
                           list is returned; if False, only one result
                           is returned); if specified as True, then all
                           results will be returned, regardless of the
                           values of allow_mult and all_mult
    :type ask_to_resolve: boolean
    :returns: The results of resolving entity_name, expected to be of the
              following form:
              <resolved_object>  # If only one result is present or the user
                                 # is able to select from multiple
              OR
              [<resolved_object>, ...]  # If multiple results are present and
                                        # it is allowed
              where <resolved_object> is of the following form:
              {"project": <project_id>, "id": <object_id>}
    :rtype: dict or list of dicts
    :raises: ValueError if results is empty
    :raises: ResolutionError if too many results are found and the user is
             not in interactive mode and cannot select one

    Precondition: results must be a nonempty list

    Validates length of results.

    If there are multiple results found and the user is in interactive mode,
    then the user will be prompted to select a single result to be returned.
    """
    if len(results) == 0:
        raise ValueError("'results' must be nonempty.")

    # Caller wants ALL results, so return the entire results list
    # At this point, do not care about the values of allow_mult or all_mult
    if not ask_to_resolve:
        return results

    if len(results) > 1:
        # The other way the caller can specify it wants all results is by setting
        # allow_mult to be True and allowing all_mult to be True (or if the object name is a glob pattern)
        if allow_mult and (all_mult or is_glob_pattern(entity_name)):
            return results
        if INTERACTIVE_CLI:
            print('The given path "' + path + '" resolves to the following data objects:')
            if any(['describe' not in result for result in results]):
                # findDataObject API call must be made to get 'describe' mappings
                project, folderpath, entity_name = resolve_path(path, expected='entity')
                results = _resolve_global_entity(project, folderpath, entity_name)
            choice = pick([get_ls_l_desc(result['describe']) for result in results],
                          allow_mult=allow_mult)
            if allow_mult and choice == '*':
                return results
            else:
                return [results[choice]] if allow_mult else results[choice]
        else:
            raise ResolutionError('The given path "' + path + '" resolves to ' +
                                  str(len(results)) + ' data objects')
    else:
        return [results[0]] if allow_mult else results[0]


def _resolve_global_entity(project_or_job_id, folderpath, entity_name, describe=True, visibility="either"):
    """
    :param project_or_job_id: The project ID to which the entity belongs
                              (then the entity is an existing data object),
                              or the job ID to which the entity belongs
                              (then the entity is a job-based object
                              reference to an object that may not exist yet)
    :type project_or_job_id: string
    :param folderpath: Full path to the object (parsed from command line)
    :type folderpath: string
    :param entity_name: Name of the object
    :type entity_name: string
    :param describe: Input mapping used to describe the job's project if
                     project_or_job_id is a job ID, or True if the input
                     mapping is to be empty
    :type describe: dict or True
    :param visibility: The expected visibility of the entity ("either",
                       "hidden", or "visible"); to be used in resolution
    :type visibility: string
    :returns: The results obtained from attempting to resolve the entity;
              the expected format of the return value is described below
    :rtype: list
    :raises: ResolutionError if dxpy.find_data_objects throws an error

    If project_or_job_id is a job ID, then return value will be like:
        [{"id": ..., "describe": {...}}, ...]

    Otherwise, the return value will be like:
        [{"id": ..., "project": ..., "describe": {...}}, ...]
    Note that if the entity is successfully resolved, then the "describe"
    key will be in the dictionary if and only if a nonempty describe
    mapping was provided.

    TODO: Inspect entity_name and conditionally treat it as a "glob" pattern.

    TODO: Callers should specify exactly what fields they want, and then
    hopefully we can avoid having a default set of fields that may be very
    expensive
    """
    if is_job_id(project_or_job_id):
        if describe is True:
            describe = {}
        # The following function call will raise a ResolutionError if no results
        # could be found.
        # If the call is successful, then the project will be incorporated into the
        # "describe" mapping of the returned dictionaries.
        return resolve_job_ref(project_or_job_id, entity_name, describe=describe)
    else:
        try:
            return list(dxpy.find_data_objects(project=project_or_job_id,
                                               folder=folderpath,
                                               name=entity_name,
                                               name_mode='glob',
                                               recurse=False,
                                               describe=describe,
                                               visibility=visibility))
        except Exception as details:
            raise ResolutionError(str(details))


def _format_resolution_output(path, project, folderpath, entity_name, result):
    """
    :param path: Path to the object that required resolution; propagated from
                 command-line
    :type path: string
    :param project: The potential project the entity belongs to
    :type project: string
    :param folderpath: Path to the entity
    :type folderpath: string
    :param entity_name: The name of the entity
    :type entity_name: string
    :param result: The result of resolving entity_name
    :type result: list of dictionaries
    :returns: The validated resolution output
    :rtype: dictionary

    Formats the output from the resolution of entity_name based on the number
    of resolved entities.

    If no results are found and entity_name can be resolved to a folder, then
    the return value will look like:
    {"project": <project>, "folder": <folder>, "name": None}

    If exactly one result is found, then the return value will look like:
    {"project": <project>, "folder": <folder>, "name": {"id": <id>,
                                                        "project": <project>}}
    OR
    {"project": None, "folder": <folder>, "name": {"id": <id>,
                                                   "project": <project>}}

    Else, the return value will look like:
    {"project": None, "folder": None, "name": None}
    """
    try:
        if len(result) == 0:
            folder = _resolve_folder(project, folderpath, entity_name)
            return {"project": project, "folder": folder, "name": None}
        else:
            validated_results = _validate_resolution_output_length(path, entity_name, result)
            return {"project": None if is_job_id(project) else project,
                    "folder": None, "name": validated_results}
    except ResolutionError:
        return {"project": None, "folder": None, "name": None}


def resolve_multiple_existing_paths(paths):
    """
    :param paths: A list of paths to items that need to be resolved
    :type paths: list
    :returns: A dictionary mapping a specified path to either its resolved
              object or Nones, if the object could not be resolved
    :rtype: dict

    For each input given in paths, attempts to resolve the path, and returns
    the resolved object in a dictionary.

    The return value will look like:
    {<path1>: <resolved_object1>, <path2>: <resolved_object2>,...}

    If entity_id is a DX ID that can be described,
        <resolved_object*> ::= {"project": None,
                                "folder": None,
                                "name": {"id": <id>,
                                         "describe": <describe_output>}}

    Else if a general resolution (or search) method will be used to resolve
    the entity,
        <resolved_object*> ::= {"project": <project>,
                                "folder": None,
                                "name": {"project": <project>,
                                         "id": <resolved_id>}}

    Else if <project> is a job ID,
        <resolved_object*> ::= {"project": None,
                                "folder": None,
                                "name": {"project": <project>,
                                         "id": <resolved_id>}}

    Else if the path refers to a folder instead of a data object,
        <resolved_object*> ::= {"project": <project>,
                                "folder": <folder>,
                                "name": None}

    Else if description or resolution fails,
        <resolved_object*> ::= {"project": None, "folder": None, "name": None}
    """
    done_objects = {}  # Return value
    to_resolve_in_batch_paths = []  # Paths to resolve
    to_resolve_in_batch_inputs = []  # Project, folderpath, and entity name
    for path in paths:
        project, folderpath, entity_name = resolve_path(path, expected='entity')
        try:
            must_resolve, project, folderpath, entity_name = _check_resolution_needed(
                path, project, folderpath, entity_name)
        except:
            must_resolve = False

        if must_resolve:
            if is_glob_pattern(entity_name):
                # TODO: Must call findDataObjects because resolveDataObjects does not support glob patterns
                try:
                    find_results = _resolve_global_entity(project, folderpath, entity_name)
                    done_objects[path] = _format_resolution_output(path, project, folderpath, entity_name,
                                                                   find_results)
                except ResolutionError:
                    # Catches any ResolutionError thrown by _resolve_global_entity
                    done_objects[path] = {"project": None, "folder": None, "name": None}
            else:
                # Prepare batch call for resolveDataObjects
                to_resolve_in_batch_paths.append(path)
                to_resolve_in_batch_inputs.append({"project": project, "folder": folderpath, "name": entity_name})
        else:
            # No need to resolve
            done_objects[path] = {"project": project, "folder": folderpath, "name": entity_name}

    # Call resolveDataObjects
    resolution_results = dxpy.resolve_data_objects(to_resolve_in_batch_inputs)
    for path, inputs, result in zip(to_resolve_in_batch_paths, to_resolve_in_batch_inputs,
                                    resolution_results):
        done_objects[path] = _format_resolution_output(path, inputs["project"], inputs["folder"], inputs["name"],
                                                       result)
    return done_objects


def resolve_existing_path(path, expected=None, ask_to_resolve=True, expected_classes=None, allow_mult=False,
                          describe=True, all_mult=False, allow_empty_string=True, visibility="either"):
    '''
    :param expected: one of the following: "folder", "entity", or None to indicate
                     whether the expected path is a folder, a data object, or either
    :type expected: string or None
    :param ask_to_resolve: Whether picking may be necessary (if true, a list is returned; if false, only one result is returned)
    :type ask_to_resolve: boolean
    :param expected_classes: A list of expected classes the entity is allowed
                             to belong to if it is an ID (e.g. "record",
                             "file", "job"); if None, then entity_name may be
                             any data object class
    :type expected_classes: list or None
    :param allow_mult: Whether to allow the user to select multiple results from the same path
    :type allow_mult: boolean
    :param describe: Input hash to describe call for the results, or True if no describe input
                     is to be provided
    :type describe: dict or True
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

    TODO: Callers should specify exactly what fields they want, and then
    hopefully we can avoid having a default set of fields that may be very
    expensive

    NOTE: if expected_classes is provided and conflicts with the class
    of the hash ID, it will return None for all fields.
    '''
    project, folderpath, entity_name = resolve_path(path, expected=expected, allow_empty_string=allow_empty_string)
    must_resolve, project, folderpath, entity_name = _check_resolution_needed(path,
                                                                              project,
                                                                              folderpath,
                                                                              entity_name,
                                                                              expected_classes=expected_classes,
                                                                              describe=describe,
                                                                              enclose_in_list=(not ask_to_resolve or
                                                                                               allow_mult))

    if must_resolve:
        results = _resolve_global_entity(project, folderpath, entity_name, describe=describe, visibility=visibility)
        if len(results) == 0:
            # Could not resolve entity, so it is probably a folder
            folder = _resolve_folder(project, folderpath, entity_name)
            return project, folder, None
        else:
            validated_results = _validate_resolution_output_length(path,
                                                                   entity_name,
                                                                   results,
                                                                   allow_mult=allow_mult,
                                                                   all_mult=all_mult,
                                                                   ask_to_resolve=ask_to_resolve)
            if is_job_id(project):
                return None, None, validated_results
            return project, None, validated_results
    return project, folderpath, entity_name


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
        return dxpy.api.app_describe(path, alias=alias)
    except dxpy.DXAPIError:
        return None

def get_global_workflow_from_path(path):
    '''
    :param path: A string to attempt to resolve to a global workflow object
    :type path: string
    :returns: The describe hash of the global workflow object if found, or None otherwise
    :rtype: dict or None

    This method parses a string that is expected to perhaps refer to
    a global workflow object.  If found, its describe hash will be returned.
    For more information on the contents of this hash, see the API
    documentation. [TODO: external link here]

    '''
    alias = None
    if not path.startswith('globalworkflow-'):
        path = 'globalworkflow-' + path
    if '/' in path:
        alias = path[path.find('/') + 1:]
        path = path[:path.find('/')]

    try:
        return dxpy.api.global_workflow_describe(path, alias=alias)
    except dxpy.DXAPIError:
        return None

def get_global_exec_from_path(path):
    if path.startswith('app-'):
        return get_app_from_path(path)
    elif path.startswith('globalworkflow-'):
        return get_global_workflow_from_path(path)

    # If the path doesn't include a prefix, we must try describing
    # as an app and, if that fails, as a global workflow
    desc = get_app_from_path(path)
    if not desc:
        desc = get_global_workflow_from_path(path)
    return desc

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

def resolve_global_workflow(path):
    '''
    :param path: A string which is supposed to identify a global workflow
    :type path: string
    :returns: The describe hash of the global workflow object
    :raises: :exc:`ResolutionError` if it cannot be found

    *path* is expected to have one of the following forms:

    - hash ID, e.g. "globalworkflow-F85Z6bQ0xku1PKY6FjGQ011J"
    - named ID, e.g. "globalworkflow-myworkflow"
    - named ID with alias (version or tag), e.g. "globalworkflow-myworkflow/1.2.0"
    '''
    desc = get_global_workflow_from_path(path)
    if desc is None:
        raise ResolutionError('The given path "' + path + '" could not be resolved to an accessible global workflow')
    else:
        return desc

def resolve_global_executable(path, is_version_required=False):
    """
    :param path: A string which is supposed to identify a global executable (app or workflow)
    :type path: string
    :param is_version_required: If set to True, the path has to specify a specific version/alias, e.g. "myapp/1.0.0"
    :type is_version_required: boolean
    :returns: The describe hash of the global executable object (app or workflow)
    :raises: :exc:`ResolutionError` if it cannot be found

    *path* is expected to have one of the following forms:

    - hash ID, e.g. "globalworkflow-F85Z6bQ0xku1PKY6FjGQ011J", "app-FBZ3f200yfzkKYyp9JkFVQ97"
    - named ID, e.g. "app-myapp", "globalworkflow-myworkflow"
    - named ID with alias (version or tag), e.g. "myapp/1.2.0", "myworkflow/1.2.0"
    - named ID with prefix and with alias (version or tag), e.g. "app-myapp/1.2.0", "globalworkflow-myworkflow/1.2.0"
    """
    if not is_hashid(path) and is_version_required and "/" not in path:
        raise ResolutionError('Version is required, e.g. "myexec/1.0.0"'.format())

    # First, check if the prefix is provided, then we don't have to resolve the name
    if path.startswith('app-'):
        return resolve_app(path)
    elif path.startswith('globalworkflow-'):
        return resolve_global_workflow(path)

    # If the path doesn't include a prefix, we must try describing
    # as an app and, if that fails, as a global workflow
    desc = get_app_from_path(path)
    if not desc:
        desc = get_global_workflow_from_path(path)
    if desc is None:
        raise ResolutionError(
            'The given path "' + path + '" could not be resolved to an accessible global executable (app or workflow)')
    return desc

def get_exec_handler(path, alias=None):
    handler = None
    def get_handler_from_desc(desc):
        if desc['class'] == 'applet':
            return dxpy.DXApplet(desc['id'], project=desc['project'])
        elif desc['class'] == 'app':
            return dxpy.DXApp(dxid=desc['id'])
        elif desc['class'] == 'workflow':
            return dxpy.DXWorkflow(desc['id'], project=desc['project'])
        elif desc['class'] == 'globalworkflow':
            return dxpy.DXGlobalWorkflow(dxid=desc['id'])
        else:
            raise DXError('The executable class {} is not supported'.format(desc['class']))

    # First attempt to resolve a global executable: app or global workflow
    global_exec_desc = get_global_exec_from_path(path)

    if alias is None:
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
            if global_exec_desc is None:
                raise
            else:
                entity_results = None

        if entity_results is not None and len(entity_results) == 1 and global_exec_desc is None:
            handler = get_handler_from_desc(entity_results[0]['describe'])
        elif entity_results is None and global_exec_desc is not None:
            handler = get_handler_from_desc(global_exec_desc)
        elif entity_results is not None:
            if not INTERACTIVE_CLI:
                raise ResolutionError('Found multiple executables with the path ' + path)
            print('Found multiple executables with the path ' + path)
            choice_descriptions = [get_ls_l_desc(r['describe']) for r in entity_results]
            if global_exec_desc is not None:
                choice_descriptions.append(
                    '{prefix}-{name}, version {version}'.format(
                        prefix=global_exec_desc['class'],
                        name=global_exec_desc['name'],
                        version=global_exec_desc['version']))
            choice = pick(choice_descriptions)
            if choice < len(entity_results):
                # all applet/workflow choices show up before the global app/workflow,
                # of which there is always at most one possible choice
                handler = get_handler_from_desc(entity_results[choice]['describe'])
            else:
                handler = get_handler_from_desc(global_exec_desc)
        else:
            raise ResolutionError("No matches found for " + path)
    else:
        handler = get_handler_from_desc(global_exec_desc)
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

# Generic function to parse an input key-value pair of the form '-ikey=val'
# e.g. returns ("key", "val") in the example above
def parse_input_keyval(keyeqval):
    try:
        first_eq_pos = get_first_pos_of_char('=', keyeqval)
        if first_eq_pos == -1:
            raise
        name = split_unescaped('=', keyeqval)[0]
        value = keyeqval[first_eq_pos + 1:]
        return (name, value)
    except:
        raise DXCLIError('An input was found that did not conform to the syntax: -i<input name>=<input value>')
