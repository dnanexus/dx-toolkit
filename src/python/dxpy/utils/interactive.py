'''
This file contains utility functions for interactive scripts such as
dx for tab-completion, resolving naming conflicts, etc.
'''

import sys, os
import dxpy
from dxpy.utils.resolver import *

def grab_contents_of_folder(completer, folder):
    # Only refresh lists if the current folder is not the one
    try:
        resp = completer.dxproj.list_folder(folder=folder, describe={},
                                            includeHidden=True)
        completer.folders = ['../', './'] + map(lambda folder: escape_folder_str(os.path.basename(folder)) + '/', resp['folders'])
        completer.names = map(lambda result: escape_name_str(result['describe']['name']) + ' ', resp['objects'])

        # Caching is postponed
        # if cached_project_paths is not None:
        #     if proj_id not in cached_project_paths:
        #         cached_project_paths[proj_id] = {}
        #     cached_project_paths[proj_id][folder] = {}
        # for obj in resp['objects']:
        #     obj_name = obj['describe']['name']
        #     if cached_project_paths is not None:
        #         if obj_name not in cached_project_paths[proj_id][folder]:
        #             cached_project_paths[proj_id][folder][obj_name] = [obj['id']]
        #         else:
        #             cached_project_paths[proj_id][folder][obj_name].append(obj['id'])
    except:
        pass

def get_project_and_folder_content_matches(completer, words):
    start_pos = 0
    if completer.last_slash_pos is not None:
        start_pos = completer.last_slash_pos + 1
    elif completer.colon_pos is not None:
        # project already filled in
        start_pos = completer.colon_pos + 1
    if len(words) > 0:
        true_prefix = words[-1][start_pos:]
        done_prefix = ' '.join(words[:-1]) + ' ' + words[-1][:start_pos]
    else:
        true_prefix = ''
        done_prefix = ''
    completer.matches = filter(lambda folder: folder.startswith(true_prefix) and (true_prefix != '' or folder not in ['../', './']), completer.folders) + \
        filter(lambda name: name.startswith(true_prefix), completer.names) + \
        filter(lambda proj_name: proj_name.startswith(true_prefix), completer.project_names)
    completer.matches = map(lambda match: done_prefix + match, completer.matches)

def basename(string):
    pos = get_last_pos_of_char('/', string)
    if pos >= 0:
        return unescape_name_str(string[pos+1:])
    else:
        return unescape_name_str(string)

def obj_completer(completer, text):
    completer.dxproj = None

    # First get projects if necessary
    colon_pos = -1
    project = None
    if len(words) > 0:
        colon_pos = get_last_pos_of_char(':', text)
#        print 'colon index: ' + str(colon_pos)
    if colon_pos < 0:
        # Might be tab-completing a project
        results = list(dxpy.find_projects(describe=True))
        completer.project_names = map(lambda result: escape_name_str(result['describe']['name']) + ':', results)
        if dxpy.WORKSPACE_ID is not None:
            completer.dxproj = dxpy.get_handler(dxpy.WORKSPACE_ID)
    else:
        project = unescape_name_str(words[-1][:colon_pos])
        if is_container_id(project):
            completer.dxproj = dxpy.get_handler(project)
        else:
            results = list(dxpy.find_projects(name=project))
            if len(results) == 1:
                completer.dxproj = dxpy.DXProject(results[0]['id'])
                completer.project_names = []
            elif len(results) > 1:
                print 'Found ' + str(len(results)) + ' projects with name ' + project + ':'
                for result in results:
                    print result['id']
                # TODO: Should this even be an error?
                raise ResolutionError('More than one project found with the same name')
            else:
                raise ResolutionError('Could not resolve project name')

    # Then get path to current folder if we have some project to look in
    if completer.dxproj is not None:
        path = os.environ.get('DX_CLI_WD', '/')
        last_slash_pos = -1
        if len(words) > 0:
            last_slash_pos = get_last_pos_of_char('/', words[-1])
        if last_slash_pos >= 0:
            if colon_pos >= 0:
                path += '/' + unescape_folder_str(words[-1][colon_pos + 1 : last_slash_pos])
            else:
                path += '/' + unescape_folder_str(words[-1][:last_slash_pos])
#        print 'looking in path ' + path
        # Then update lists as necessary
        grab_contents_of_folder(completer, resolve_path(path))

    # TODO: Then look for app names???

    completer.colon_pos = colon_pos
    completer.last_slash_pos = last_slash_pos

class DXObjectCompleter():
    '''
    This class can be used as a tab-completer with the modules
    readline and rlcompleter.  Note that to tab-complete data object
    names with spaces, the delimiters set for the completer must not
    include spaces.
    '''
    def __init__(self, classes=None):
        self.dxproj = None
        self.project_names = None
        self.folders = None
        self.names = None
        self.colon_pos = -1
        self.last_slash_pos = -1
        self.matches = None
        self.classes = classes

    def __call__(self, text, state):
        if state == 0:
            obj_completer(self, text)
            # Then find matches
            get_project_and_folder_content_matches(self, words)

        if self.matches is not None and state < len(self.matches):
            return self.matches[state]
        else:
            return None

def pick(choices, default=None, str_choices=None, prompt=None):
    '''
    :param choices: Strings between which the user will make a choice
    :type choices: list of strings
    :param default: Number the index to be used as the default
    :type default: int or None
    :param str_choices: Strings to be used as aliases for the choices; must be of the same length as choices and each string must be unique
    :type str_choices: list of strings
    :param prompt: A custom prompt to be used
    :type prompt: string
    :returns: The user's choice as a numbered index of choices (e.g. 0 for the first item)
    :rtype: int
    :raises: :exc:`EOFError` to signify quitting the process
    '''
    for i in range(len(choices)):
        print str(i) + ') ' + choices[i]
    print ''
    if prompt is None:
        if default is not None:
            prompt = 'Pick a numbered choice [' + str(default) + ']: '
        else:
            prompt = 'Pick a numbered choice: '
    while True:
        try: 
            value = raw_input(prompt)
        except KeyboardInterrupt:
            print ''
            continue
        except EOFError:
            print ''
            raise EOFError()
        if default is not None and value == '':
            return default
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
        except BaseException as details:
            print str(details)
            print 'Not a valid selection'
