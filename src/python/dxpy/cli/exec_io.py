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
Functions and classes used when launching platform executables from the CLI.
'''

from __future__ import print_function, unicode_literals, division, absolute_import

# TODO: refactor all dx run helper functions here

import os, sys, json, collections, pipes
from ..bindings.dxworkflow import DXWorkflow

import dxpy
from . import INTERACTIVE_CLI
from ..exceptions import DXCLIError, DXError
from ..utils.printing import (RED, GREEN, WHITE, BOLD, ENDC, UNDERLINE, fill)
from ..utils.describe import (get_find_executions_string, get_ls_l_desc, get_ls_l_desc_fields, parse_typespec)
from ..utils.resolver import (parse_input_keyval, is_hashid, is_job_id, is_localjob_id, paginate_and_pick, pick,
                              resolve_existing_path, resolve_multiple_existing_paths, split_unescaped, is_analysis_id)
from ..utils import OrderedDefaultdict
from ..compat import input, str, shlex, basestring, USING_PYTHON2

####################
# -i Input Parsing #
####################

def parse_bool(string):
    if len(string) > 0:
        if 'true'.startswith(string.lower()) or string == '1':
            return True
        elif 'false'.startswith(string.lower()) or string == '0':
            return False
    raise DXCLIError('Could not resolve \"' + string +  '\" to a boolean')

def parse_obj(string, klass):
    if string == '':
        raise DXCLIError('Error: Nonempty string cannot be resolved')
    project, path, entity_result = resolve_existing_path(string)
    if entity_result is None:
        raise DXCLIError('Could not resolve \"' + string + '\" to a name or ID')
    if not entity_result['describe']['class'] == klass:
        raise DXCLIError('Error: The given object is of class ' + entity_result['describe']['class'] + ' but an object of class ' + klass + ' was expected.')
    if is_hashid(string):
        return {'$dnanexus_link': entity_result['id']}
    else:
        return {'$dnanexus_link': {"project": entity_result['describe']['project'],
                                   "id": entity_result['id']}}

dx_data_classes = ['record', 'gtable', 'file', 'applet', 'workflow']

parse_input = {'boolean': parse_bool,
               'string': (lambda string: string),
               'float': (lambda string: float(string)),
               'int': (lambda string: int(string)),
               'hash': (lambda string: json.loads(string)),
               'record': (lambda string: parse_obj(string, 'record')),
               'gtable': (lambda string: parse_obj(string, 'gtable')),
               'file': (lambda string: parse_obj(string, 'file')),
               'applet': (lambda string: parse_obj(string, 'applet')),
               'workflow': (lambda string: parse_obj(string, 'workflow')),
               'job': (lambda string: {'$dnanexus_link': string}),
               'app': (lambda string: {'$dnanexus_link': string})}

def _construct_jbor(job_id, field_name_and_maybe_index):
    '''
    :param job_id: Job ID
    :type job_id: string
    :param field_name_and_maybe_index: Field name, plus possibly ".N" where N is an array index
    :type field_name_and_maybe_index: string
    :returns: dict of JBOR
    '''
    link = {"$dnanexus_link": {"job": job_id}}
    if '.' in field_name_and_maybe_index:
        split_by_dot = field_name_and_maybe_index.rsplit('.', 1)
        link["$dnanexus_link"]["field"] = split_by_dot[0]
        link["$dnanexus_link"]["index"] = int(split_by_dot[1])
    else:
        link["$dnanexus_link"]["field"] = field_name_and_maybe_index
    return link

def parse_input_or_jbor(in_class, value):
    val_substrings = split_unescaped(':', value)
    if len(val_substrings) == 2 and (is_job_id(val_substrings[0]) or is_localjob_id(val_substrings[0])):
        return _construct_jbor(val_substrings[0], val_substrings[1])
    else:
        if in_class.startswith('array:'):
            in_class = in_class[6:]
        return parse_input[in_class](value)

#################################
# Interactive Run Input Methods #
#################################

def print_param_help(param_desc):
    print(fill(UNDERLINE() + param_desc.get('label', param_desc['name']) + ':' + ENDC() + ' ' + (param_desc['help'] if 'help' in param_desc else '<no extra help available>'), initial_indent='  ', subsequent_indent='  '))

def interactive_help(in_class, param_desc, prompt):
    is_array = param_desc['class'].startswith("array:")
    print_param_help(param_desc)
    print()
    array_help_str = ', or <ENTER> to finish the list of inputs'
    if in_class in dx_data_classes:
        # Class is some sort of data object
        if dxpy.WORKSPACE_ID is not None:
            proj_name = None
            try:
                proj_name = dxpy.api.project_describe(dxpy.WORKSPACE_ID)['name']
            except:
                pass
            if proj_name is not None:
                print('Your current working directory is ' + proj_name + ':' + dxpy.config.get('DX_CLI_WD', '/'))
        while True:
            print('Pick an option to find input data:')
            try:
                opt_num = pick(['List and choose from available data in the current project',
                                'List and choose from available data in the DNAnexus Reference Genomes project',
                                'Select another project to list and choose available data',
                                'Select an output from a previously-run job (current project only)',
                                'Return to original prompt (specify an ID or path directly)'])
            except KeyboardInterrupt:
                opt_num = 4
            if opt_num == 0:
                query_project = dxpy.WORKSPACE_ID
            elif opt_num == 1:
                query_project = dxpy.find_one_project(name="Reference Genome Files", public=True, billed_to="org-dnanexus", level="VIEW")['id']
            elif opt_num == 2:
                project_generator = dxpy.find_projects(level='VIEW', describe=True, explicit_perms=True)
                print('\nProjects to choose from:')
                query_project = paginate_and_pick(project_generator, (lambda result: result['describe']['name']))['id']
            if opt_num in range(3):
                result_generator = dxpy.find_data_objects(classname=in_class,
                                                          typename=param_desc.get('type'),
                                                          describe=dict(fields=get_ls_l_desc_fields()),
                                                          project=query_project)
                print('\nAvailable data:')
                result_choice = paginate_and_pick(result_generator,
                                                  (lambda result: get_ls_l_desc(result['describe'])))
                if result_choice == 'none found':
                    print('No compatible data found')
                    continue
                elif result_choice == 'none picked':
                    continue
                else:
                    return [result_choice['project'] + ':' + result_choice['id']]
            elif opt_num == 3:
                # Select from previous jobs in current project
                result_generator = dxpy.find_jobs(project=dxpy.WORKSPACE_ID,
                                                  describe=True,
                                                  parent_job="none")
                print()
                print('Previously-run jobs to choose from:')
                result_choice = paginate_and_pick(result_generator,
                                                  (lambda result: get_find_executions_string(result['describe'],
                                                                                             has_children=False,
                                                                                             single_result=True)),
                                                  filter_fn=(lambda result: result['describe']['state'] not in ['unresponsive', 'terminating', 'terminated', 'failed']))
                if result_choice == 'none found':
                    print('No jobs found')
                    continue
                elif result_choice == 'none picked':
                    continue
                else:
                    if 'output' in result_choice['describe'] and result_choice['describe']['output'] != None:
                        keys = result_choice['describe']['output'].keys()
                    else:
                        exec_handler = dxpy.get_handler(result_choice.get('app', result_choice['applet']))
                        exec_desc = exec_handler.describe()
                        if 'outputSpec' not in exec_desc:
                            # This if block will either continue, return, or raise
                            print('No output spec found for the executable')
                            try:
                                field = input('Output field to use (^C or <ENTER> to cancel): ')
                                if field == '':
                                    continue
                                else:
                                    return [result_choice['id'] + ':' + field]
                            except KeyboardInterrupt:
                                continue
                        else:
                            keys = exec_desc['outputSpec'].keys()
                    if len(keys) > 1:
                        print('\nOutput fields to choose from:')
                        field_choice = pick(keys)
                        return [result_choice['id'] + ':' + keys[field_choice]]
                    elif len(keys) == 1:
                        print('Using the only output field: ' + keys[0])
                        return [result_choice['id'] + ':' + keys[0]]
                    else:
                        print('No available output fields')
            else:
                print(fill('Enter an ID or path (<TAB> twice for compatible ' + in_class + 's in current directory)' + (array_help_str if is_array else '')))
                return shlex.split(input(prompt))
    else:
        if in_class == 'boolean':
            if is_array:
                print(fill('Enter "true", "false"' + array_help_str))
            else:
                print(fill('Enter "true" or "false"'))
        elif in_class == 'string' and is_array:
                print(fill('Enter a nonempty string' + array_help_str))
        elif (in_class == 'float' or in_class == 'int') and is_array:
            print(fill('Enter a number' + array_help_str))
        elif in_class == 'hash':
            print(fill('Enter a quoted JSON hash'))
        result = input(prompt)
        if in_class == 'string':
            return [result]
        else:
            return shlex.split(result)

def get_input_array(param_desc):
    in_class = param_desc['class']
    if in_class.startswith("array:"):
        in_class = in_class[6:]
    typespec = param_desc.get('type', None)
    input_array = []
    print('\nInput:   ' + fill(UNDERLINE() + param_desc.get('label', param_desc['name']) + ENDC() + ' (' + param_desc['name'] + ')'))
    print('Class:   ' + param_desc['class'])
    if 'type' in param_desc:
        print('Type(s): ' + parse_typespec(param_desc['type']))
    print()

    prompt = "Enter {_class} values, one at a time (^D or <ENTER> to finish, {hint}'" + WHITE() + BOLD() + '?' + ENDC() + "' for more options)"
    hint = ''
    if in_class in dx_data_classes:
        hint = '<TAB> twice for compatible ' + in_class + 's in current directory, '
    elif 'suggestions' in param_desc:
        hint = '<TAB> twice for suggestions, '
    elif 'choices' in param_desc:
        hint = '<TAB> twice for choices, '
    prompt = prompt.format(_class=in_class, hint=hint)
    print(fill(prompt))

    try:
        import readline
        if in_class in dx_data_classes:
            from dxpy.utils.completer import DXPathCompleter
            readline.set_completer(DXPathCompleter(classes=[in_class],
                                                   typespec=typespec).complete)
        elif in_class == 'boolean':
            from dxpy.utils.completer import ListCompleter
            readline.set_completer(ListCompleter(completions=['true', 'false']).complete)
        elif 'suggestions' in param_desc:
            from dxpy.utils.completer import ListCompleter
            readline.set_completer(ListCompleter(completions=map(str, param_desc['suggestions'])).complete)
        elif 'choices' in param_desc:
            from dxpy.utils.completer import ListCompleter
            readline.set_completer(ListCompleter(completions=map(str, param_desc['choices'])).complete)
        else:
            from dxpy.utils.completer import NoneCompleter
            readline.set_completer(NoneCompleter().complete)
    except:
        pass
    try:
        while True:
            prompt = param_desc['name'] + '[' + str(len(input_array)) + "]: "
            user_input = input(prompt)
            if in_class == 'string':
                if user_input == '':
                    user_input = []
                else:
                    user_input = [user_input]
            else:
                user_input = shlex.split(user_input)
            while user_input == ['?']:
                user_input = interactive_help(in_class, param_desc, prompt)
            if len(user_input) > 1:
                print(fill('Error: more than one argument given.  Please quote your entire input or escape your whitespace with a backslash \'\\\'.'))
                continue
            elif len(user_input) == 0:
                return input_array
            try:
                input_array.append(parse_input_or_jbor(in_class, user_input[0]))
            except ValueError as details:
                print(fill('Error occurred when parsing for class ' + in_class + ': ' + str(details)))
                continue
            except TypeError as details:
                print(fill('Error occurred when parsing for class ' + in_class + ': ' + str(details)))
                continue
    except EOFError:
        return input_array

def format_choices_or_suggestions(header, items, obj_class, initial_indent=' ' * 8, subsequent_indent=' ' * 10):
    if obj_class.startswith('array:'):
        obj_class = obj_class[6:]

    def format_data_object_reference(item):
        if dxpy.is_dxlink(item):
            # Bare dxlink
            obj_id, proj_id = dxpy.get_dxlink_ids(item)
            return (proj_id + ":" if proj_id else '') + obj_id
        if dxpy.is_dxlink(item.get('value')):
            # value is set
            obj_id, proj_id = dxpy.get_dxlink_ids(item['value'])
            return (proj_id + ":" if proj_id else '') + obj_id + (' (%s)' % item['name'] if item.get('name') else '')
        if item.get('project') and item.get('path'):
            # project and folder path
            return item['project'] + ':' + item['path'] + "/" + obj_class + "-*" +  (' (%s)' % item['name'] if item.get('name') else '')
        return str(item)

    showing_data_objects = obj_class in dx_data_classes

    if showing_data_objects:
        return initial_indent + header + ''.join('\n' + subsequent_indent + format_data_object_reference(item) for item in items)
    else:
        # TODO: in interactive prompts the quotes here may be a bit
        # misleading. Perhaps it should be a separate mode to print
        # "interactive-ready" suggestions.
        return fill(header + ' ' + ', '.join([pipes.quote(str(item)) for item in items]),
                    initial_indent=initial_indent,
                    subsequent_indent=subsequent_indent)

def get_input_single(param_desc):
    in_class = param_desc['class']
    typespec = param_desc.get('type', None)
    print('\nInput:   ' + fill(UNDERLINE() + param_desc.get('label', param_desc['name']) + ENDC() + ' (' + param_desc['name'] + ')'))
    print('Class:   ' + param_desc['class'])
    if 'type' in param_desc:
        print('Type(s): ' + parse_typespec(param_desc['type']))
    if 'suggestions' in param_desc:
        print(format_choices_or_suggestions('Suggestions:', param_desc['suggestions'], param_desc['class'], initial_indent='', subsequent_indent='  '))
    if 'choices' in param_desc:
        print(format_choices_or_suggestions('Choices:', param_desc['choices'], param_desc['class'], initial_indent='', subsequent_indent='  '))
    print()

    prompt = "Enter {_class} {value} ({hint}'" + WHITE() + BOLD() + '?' + ENDC() + "' for more options)"
    hint = ''
    if in_class in dx_data_classes:
        hint = '<TAB> twice for compatible ' + in_class + 's in current directory, '
    elif 'suggestions' in param_desc:
        hint = '<TAB> twice for suggestions, '
    elif 'choices' in param_desc:
        hint = '<TAB> twice for choices, '
    prompt = prompt.format(_class=in_class,
                           value='ID or path' if in_class in dx_data_classes else 'value',
                           hint=hint)
    print(fill(prompt))

    try:
        import readline
        if in_class in dx_data_classes:
            from dxpy.utils.completer import DXPathCompleter
            readline.set_completer(DXPathCompleter(classes=[in_class],
                                                   typespec=typespec).complete)
        elif in_class == 'boolean':
            from dxpy.utils.completer import ListCompleter
            readline.set_completer(ListCompleter(completions=['true', 'false']).complete)
        elif 'suggestions' in param_desc:
            from dxpy.utils.completer import ListCompleter
            readline.set_completer(ListCompleter(completions=map(str, param_desc['suggestions'])).complete)
        elif 'choices' in param_desc:
            from dxpy.utils.completer import ListCompleter
            readline.set_completer(ListCompleter(completions=map(str, param_desc['choices'])).complete)
        else:
            from dxpy.utils.completer import NoneCompleter
            readline.set_completer(NoneCompleter().complete)
    except:
        pass
    try:
        while True:
            prompt = param_desc['name'] + ': '
            user_input = input(prompt)
            if in_class == 'string':
                if user_input == '':
                    user_input = []
                else:
                    user_input = [user_input]
            else:
                user_input = shlex.split(user_input)
            while user_input == ["?"]:
                user_input = interactive_help(in_class, param_desc, prompt)
            if len(user_input) > 1:
                print(fill('Error: more than one argument given.  Please quote your entire input or escape your whitespace with a backslash \'\\\'.'))
                continue
            elif len(user_input) == 0:
                user_input = ['']
            try:
                value = parse_input_or_jbor(in_class, user_input[0])
            except ValueError as details:
                print(fill('Error occurred when parsing for class ' + in_class + ': ' + str(details)))
                continue
            except TypeError as details:
                print(fill('Error occurred when parsing for class ' + in_class + ': ' + str(details)))
                continue
            if 'choices' in param_desc and value not in param_desc['choices']:
                print(fill(RED() + BOLD() + 'Warning:' + ENDC() + ' value "' + str(value) + '" for input ' + WHITE() +
                           BOLD() + param_desc['name'] + ENDC() + ' is not in the list of choices for that input'))
            return value
    except EOFError:
        raise DXCLIError('Unexpected end of input')

def get_optional_input_str(param_desc):
    return param_desc.get('label', param_desc['name']) + ' (' + param_desc['name'] + ')'

class ExecutableInputs(object):
    def __init__(self, executable=None, input_name_prefix=None, input_spec=None, active_region=None):
        """
        :param executable: Executable object handler
        :type executable: :class:`~dxpy.bindings.dxapplet.DXApplet`,
                          :class:`~dxpy.bindings.dxapp.DXApp`,
                          :class:`~dxpy.bindings.dxworkflow.DXWorkflow`,
                          :class:`~dxpy.bindings.dxglobalworkflow.DXGlobalWorkflow`
        :param input_name_prefix: A prefix set on an input field name
        :type input_name_prefix: string
        :param input_spec: Input specification
        :type input_spec: dict
        :param active_region: The region in which the executable is run, determined by the destination project context.
        :type active_region: string
        """
        self.executable = executable
        self.region = active_region

        self._desc = self.get_executable_description()

        self.required_inputs, self.optional_inputs, self.array_inputs = [], [], set()
        self.input_name_prefix = input_name_prefix
        self.inputs = OrderedDefaultdict(list)

        # List of tuples (input name, input value, input class, index), where input name and input value are
        # propagated from command-line (input class is propagated from the input spec, and may be None if no input
        # spec is provided). index is the order in which this particular input value is specified on the command-line
        # relative to other input values of the same name (as multiple input values may be specified for the same
        # input name). If input class is truthy, then the index is 0. Otherwise, if input name will have only a
        # single input value instead of a list of input values, then index is -1.
        self.requires_resolution = []

        # update input_spec passed to the constructor and initialize
        # self.input_spec, self.optional_inputs, self.required_inputs, self.array_inputs
        self.input_spec = collections.OrderedDict() if 'inputSpec' in self._desc or input_spec else None

        if input_spec is None:
            input_spec = self._desc.get('inputSpec', [])

        if input_spec is None and self._desc['class'] in ('workflow', 'globalworkflow'):
            # this is only the case if it's a workflow with an
            # inaccessible stage
            inaccessible_stages = [stage['id'] for stage in self._desc['stages'] if stage['accessible'] is False]
            raise DXCLIError('The workflow ' + self._desc['id'] + ' has the following inaccessible stage(s): ' + ', '.join(inaccessible_stages))

        # Workflow-level inputs (defined in "inputs")
        #  i. If the workflow has no "inputs"
        #   * The inputs can be passed to stages directly
        # ii. If the workflow has "inputs" (in a closed or open state)
        #   * Only inputs defined in inputs can be passed to the workflow,
        #     using workflow-level input names
        if self._accept_only_workflow_level_inputs():
            input_spec = self._desc.get('inputs', [])

        for spec_atom in input_spec:
            input_name = spec_atom['name']
            if spec_atom['class'].startswith('array:'):
                self.array_inputs.add(input_name)
            self.input_spec[input_name] = spec_atom
            if self._is_input_optional(spec_atom):
                self.optional_inputs.append(input_name)
            else:
                self.required_inputs.append(input_name)

    def _accept_only_workflow_level_inputs(self):
        return self._desc.get('inputs') is not None

    def get_executable_description(self):
        if self.executable is None:
            return {}
        elif isinstance(self.executable, dxpy.DXGlobalWorkflow):
            global_workflow_desc = self.executable.describe()
            return self.executable.append_underlying_workflow_desc(global_workflow_desc, self.region)
        else:
            return self.executable.describe()


    def update(self, new_inputs, strip_prefix=True):
        """
        Updates the inputs dictionary with the key/value pairs from new_inputs, overwriting existing keys.
        """
        if strip_prefix and self.input_name_prefix is not None:
            for i in new_inputs:
                if i.startswith(self.input_name_prefix):
                    self.inputs[i[len(self.input_name_prefix):]] = new_inputs[i]
        else:
            self.inputs.update(new_inputs)

    def _update_requires_resolution_inputs(self):
        """
        Updates self.inputs with resolved input values (the input values that were provided
        as paths to items that require resolutions, eg. folder or job/analyses ids)
        """
        input_paths = [quad[1] for quad in self.requires_resolution]
        results = resolve_multiple_existing_paths(input_paths)
        for input_name, input_value, input_class, input_index in self.requires_resolution:
            project = results[input_value]['project']
            folderpath = results[input_value]['folder']
            entity_result = results[input_value]['name']
            if input_class is None:
                if entity_result is not None:
                    if isinstance(entity_result, basestring):
                        # Case: -ifoo=job-012301230123012301230123
                        # Case: -ifoo=analysis-012301230123012301230123
                        assert(is_job_id(entity_result) or
                               (is_analysis_id(entity_result)))
                        input_value = entity_result
                    elif is_hashid(input_value):
                        input_value = {'$dnanexus_link': entity_result['id']}
                    elif 'describe' in entity_result:
                        # Then findDataObjects was called (returned describe hash)
                        input_value = {"$dnanexus_link": {"project": entity_result['describe']['project'],
                                                          "id": entity_result['id']}}
                    else:
                        # Then resolveDataObjects was called in a batch (no describe hash)
                        input_value = {"$dnanexus_link": {"project": entity_result['project'],
                                                          "id": entity_result['id']}}
                if input_index >= 0:
                    if self.inputs[input_name][input_index] is not None:
                        raise AssertionError("Expected 'self.inputs' to have saved a spot for 'input_value'.")
                    self.inputs[input_name][input_index] = input_value
                else:
                    if self.inputs[input_name] is not None:
                        raise AssertionError("Expected 'self.inputs' to have saved a spot for 'input_value'.")
                    self.inputs[input_name] = input_value
            else:
                msg = 'Value provided for input field "' + input_name + '" could not be parsed as ' + \
                      input_class + ': '
                if input_value == '':
                    raise DXCLIError(msg + 'empty string cannot be resolved')
                if entity_result is None:
                    raise DXCLIError(msg + 'could not resolve \"' + input_value + '\" to a name or ID')
                try:
                    dxpy.bindings.verify_string_dxid(entity_result['id'], input_class)
                except DXError as details:
                    raise DXCLIError(msg + str(details))
                if is_hashid(input_value):
                    input_value = {'$dnanexus_link': entity_result['id']}
                elif 'describe' in entity_result:
                    # Then findDataObjects was called (returned describe hash)
                    input_value = {'$dnanexus_link': {"project": entity_result['describe']['project'],
                                                      "id": entity_result['id']}}
                else:
                    # Then resolveDataObjects was called in a batch (no describe hash)
                    input_value = {"$dnanexus_link": {"project": entity_result['project'],
                                                      "id": entity_result['id']}}
                if input_index != -1:
                    # The class is an array, so append the resolved value
                    self.inputs[input_name].append(input_value)
                else:
                    self.inputs[input_name] = input_value

    def add(self, input_name, input_value):
        if self.input_name_prefix is not None:
            if input_name.startswith(self.input_name_prefix):
                input_name = input_name[len(self.input_name_prefix):]
            else: # Skip inputs that don't start with prefix
                return

        if ':' in input_name:
            input_class = input_name[input_name.find(':') + 1:]
            input_name = input_name[:input_name.find(':')]
        else:
            input_class = None

        if self.input_spec is not None:
            if input_name not in self.input_spec and self._desc.get('class') != 'workflow':
                raise DXCLIError('Input field called "' + input_name + '" was not found in the input spec')
            elif input_name in self.input_spec:
                input_class = self.input_spec[input_name]['class']

        if input_class is None:
            resolved_input_as_jbor = False
            try:
                # Resolve "job-xxxx:output-name" syntax into a canonical job ref
                job_id, field = split_unescaped(':', input_value)
            except:
                pass
            else:
                if is_job_id(job_id) or is_localjob_id(job_id):
                    input_value = _construct_jbor(job_id, field)
                    resolved_input_as_jbor = True

            if resolved_input_as_jbor:
                if isinstance(self.inputs[input_name], list):
                    self.inputs[input_name].append(input_value)
                else:
                    self.inputs[input_name] = input_value
            else:
                try:
                    parsed_input_value = json.loads(input_value, object_pairs_hook=collections.OrderedDict)
                    immediate_types = {collections.OrderedDict, list, int, float}
                    if USING_PYTHON2:
                        immediate_types.add(long) # noqa
                    if type(parsed_input_value) not in immediate_types:
                        raise Exception()
                except:
                    # Not recognized JSON (list or dict), so resolve it as a name
                    # Add to self.requires_resolution, and insert None as a placeholder in self.inputs;
                    # self.requires_resolution will also store the location of the corresponding placeholder
                    if isinstance(self.inputs[input_name], list):
                        self.requires_resolution.append((input_name, input_value, None, len(self.inputs[input_name])))
                        self.inputs[input_name].append(None)
                    else:
                        # If the input is to only have a single value, then the index will be -1
                        self.requires_resolution.append((input_name, input_value, None, -1))
                        self.inputs[input_name] = None
                else:
                    if isinstance(self.inputs[input_name], list):
                        self.inputs[input_name].append(parsed_input_value)
                    else:
                        self.inputs[input_name] = parsed_input_value
        else:
            # Input class is known.  Respect the "array" class.
            val_substrings = split_unescaped(':', input_value)
            try:
                if len(val_substrings) == 2 and (is_job_id(val_substrings[0]) or is_localjob_id(val_substrings[0])):
                    input_value = _construct_jbor(val_substrings[0], val_substrings[1])
                    if input_class.startswith('array:'):
                        self.inputs[input_name].append(input_value)
                    else:
                        self.inputs[input_name] = input_value
                else:
                    # TODO: Consolidate the following checks with exec_io.parse_input_or_jbor
                    # `parse_bool()` can throw DXCLIError, which will be
                    # propagated directly to the caller.
                    if input_class == 'boolean':
                        self.inputs[input_name] = parse_bool(input_value)
                    elif input_class == 'array:boolean':
                        self.inputs[input_name].append(parse_bool(input_value))
                    elif input_class == 'string':
                        self.inputs[input_name] = input_value
                    elif input_class == 'array:string':
                        self.inputs[input_name].append(input_value)
                    elif input_class == 'float':
                        self.inputs[input_name] = float(input_value)
                    elif input_class == 'array:float':
                        self.inputs[input_name].append(float(input_value))
                    elif input_class == 'int':
                        self.inputs[input_name] = int(input_value)
                    elif input_class == 'array:int':
                        self.inputs[input_name].append(int(input_value))
                    elif input_class == 'hash':
                        self.inputs[input_name] = json.loads(input_value)
                    elif input_class == 'array:hash':
                        self.inputs[input_name].append(json.loads(input_value))
                    elif input_class == 'job' or input_class == 'app':
                        self.inputs[input_name] = {'$dnanexus_link': input_value}
                    elif input_class == 'array:job' or input_class == 'array:app':
                        self.inputs[input_name].append({'$dnanexus_link': input_value})
                    else:
                        # Add to self.requires_resolution
                        if input_class.startswith('array:'):
                            # No placeholders needed; just pass in input_index that is not -1 to append a result
                            self.requires_resolution.append((input_name, input_value, input_class[6:], 0))
                        else:
                            # input_name is to only have a single value, set index to -1
                            self.requires_resolution.append((input_name, input_value, input_class, -1))
            except (ValueError, TypeError) as details:
                raise DXCLIError('Value provided for input field "' + input_name + '" could not be parsed as ' +
                                 input_class + ': ' + str(details))

    def init_completer(self):
        try:
            import readline
            import rlcompleter
            readline.parse_and_bind("tab: complete")

            readline.set_completer_delims("")

            readline.write_history_file(os.path.join(dxpy.config.get_user_conf_dir(), '.dx_history'))
            readline.clear_history()
            readline.set_completer()
        except:
            pass

    def uninit_completer(self):
        try:
            import readline
            readline.set_completer()
            readline.clear_history()
        except:
            pass

    def prompt_for_missing(self, confirm=True):
        # No-op if there is no input spec
        if self.input_spec is None:
            return

        # If running from the command-line (not in the shell), bring up the tab-completer
        self.init_completer()

        # Select input interactively
        no_prior_inputs = True if len(self.inputs) == 0 else False
        for i in self.required_inputs:
            if i not in self.inputs:
                if len(self.inputs) == 0:
                    print('Entering interactive mode for input selection.')
                self.inputs[i] = self.prompt_for_input(i)
        if no_prior_inputs and len(self.optional_inputs) > 0 and confirm:
            self.prompt_for_optional_inputs()

        self.uninit_completer()

    def prompt_for_input(self, input_name):
        if input_name in self.array_inputs:
            return get_input_array(self.input_spec[input_name])
        else:
            return get_input_single(self.input_spec[input_name])

    def prompt_for_optional_inputs(self):
        while True:
            print('\n' + fill('Select an optional parameter to set by its # (^D or <ENTER> to finish):') + '\n')
            for i in range(len(self.optional_inputs)):
                opt_str = ' [' + str(i) + '] ' + \
                    get_optional_input_str(self.input_spec[self.optional_inputs[i]])
                if self.optional_inputs[i] in self.inputs:
                    opt_str += ' [=' + GREEN()
                    opt_str += json.dumps(self.inputs[self.optional_inputs[i]])
                    opt_str += ENDC() + ']'
                elif 'default' in self.input_spec[self.optional_inputs[i]]:
                    opt_str += ' [default=' + json.dumps(self.input_spec[self.optional_inputs[i]]['default']) + ']'
                print(opt_str)
            print("")
            try:
                while True:
                    selected = input('Optional param #: ')
                    if selected == '':
                        return
                    try:
                        opt_num = int(selected)
                        if opt_num < 0 or opt_num >= len(self.optional_inputs):
                            raise ValueError('Error: Selection is out of range')
                        break
                    except ValueError as details:
                        print(str(details))
                        continue
            except EOFError:
                return
            try:
                self.inputs[self.optional_inputs[opt_num]] = self.prompt_for_input(self.optional_inputs[opt_num])
            except:
                pass

    def update_from_args(self, args, require_all_inputs=True):
        if args.filename is not None:
            try:
                if args.filename == "-":
                    data = sys.stdin.read()
                else:
                    with open(args.filename, 'r') as fd:
                        data = fd.read()
                self.update(json.loads(data, object_pairs_hook=collections.OrderedDict))
            except Exception as e:
                raise DXCLIError('Error while parsing input JSON file: %s' % str(e))

        if args.input_json is not None:
            try:
                self.update(json.loads(args.input_json, object_pairs_hook=collections.OrderedDict))
            except Exception as e:
                raise DXCLIError('Error while parsing input JSON: %s' % str(e))

        if args.input is not None:
            for keyeqval in args.input:
                name, value = parse_input_keyval(keyeqval)
                if '.' in name and self._accept_only_workflow_level_inputs():
                    raise DXCLIError('The input with a key '+ name + ' was passed to a stage but the workflow accepts inputs only on the workflow level')
                self.add(self.executable._get_input_name(name, region=args.region, describe_output=self._desc) if \
                         self._desc.get('class') in ('workflow', 'globalworkflow') else name, value)
            self._update_requires_resolution_inputs()

        if self.input_spec is None:
            for i in self.inputs:
                if type(self.inputs[i]) == list and len(self.inputs[i]) == 1:
                    self.inputs[i] = self.inputs[i][0]

        # For now, we do not handle prompting for workflow inputs nor
        # recognizing when not all inputs haven't been bound
        if require_all_inputs:
            if INTERACTIVE_CLI:
                self.prompt_for_missing(getattr(args, 'confirm', True))
            else:
                missing_required_inputs = set(self.required_inputs) - set(self.inputs.keys())
                if missing_required_inputs:
                    raise DXCLIError('Some inputs (%s) are missing, and interactive mode is not available' % (', '.join(missing_required_inputs)))

    def _is_input_optional(self, spec_atom):
        return spec_atom.get("optional") == True or 'default' in spec_atom
