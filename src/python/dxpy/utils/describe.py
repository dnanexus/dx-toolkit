# -*- coding: utf-8 -*-
#
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
This submodule contains helper functions for parsing and printing the
contents of describe hashes for various DNAnexus entities (projects,
containers, dataobjects, apps, and jobs).
'''

import datetime, time, json, math, sys

from dxpy.utils.printing import *

def JOB_STATES(state):
    if state == 'failed':
        return BOLD() + RED() + state + ENDC()
    elif state == 'done':
        return BOLD() + GREEN() + state + ENDC()
    elif state == 'running':
        return GREEN() + state + ENDC()
    else:
        return YELLOW() + state + ENDC()

def DATA_STATES(state):
    if state == 'open':
        return YELLOW() + state + ENDC()
    elif state == 'closing':
        return YELLOW() + state + ENDC()
    elif state == 'closed':
        return GREEN() + state + ENDC()
    else:
        return state

SIZE_LEVEL = ['bytes', 'KB', 'MB', 'GB', 'TB']

def get_size_str(size):
    if size == 0:
        magnitude = 0
        level = 0
    else:
        magnitude = math.floor(math.log(size, 10))
        level = int(min(math.floor(magnitude / 3), 4))
    return ('%d' if level == 0 else '%.2f') % (float(size) / 2**(level*10)) + ' ' + SIZE_LEVEL[level]

def parse_typespec(thing):
    if isinstance(thing, basestring):
        return thing
    elif '$and' in thing:
        return '(' + ' AND '.join(map(parse_typespec, thing['$and'])) + ')'
    elif '$or' in thing:
        return '(' + ' OR '.join(map(parse_typespec, thing['$or'])) + ')'
    else:
        return 'Type spec could not be parsed'

def get_io_desc(parameter, include_class=True, show_opt=True, app_help_version=False):
    desc = ""
    is_optional = False;
    if show_opt:
        if "default" in parameter or ("optional" in parameter and parameter["optional"]):
            is_optional = True
            desc += "["
    desc += ('-i' if app_help_version else '') + parameter["name"]
    include_parens = include_class or 'type' in parameter or 'default' in parameter
    if include_parens:
        desc += ("=" if app_help_version else " ") + "("
    is_first = True
    if include_class:
        desc += parameter["class"]
        is_first = False
    if "type" in parameter:
        if not is_first:
            desc += ", "
        else:
            is_first = False
        desc += "type " + parse_typespec(parameter["type"])
    if "default" in parameter:
        if not is_first:
            desc += ', '
        desc += 'default=' + json.dumps(parameter['default'])
    if include_parens:
        desc += ")"
    if show_opt and is_optional:
        desc += "]"
    return desc

def get_io_spec(spec, skip_fields=None):
    if skip_fields is None:
        skip_fields = []
    list_of_params = [get_io_desc(param) for param in spec if param["name"] not in skip_fields]
    if len(skip_fields) > 0:
        list_of_params.append("<advanced inputs hidden; use --verbose to see more>")

    if len(list_of_params) == 0:
        return '-'
    if get_delimiter() is not None:
        return ('\n' + get_delimiter()).join(list_of_params)
    else:
        return ('\n' + ' '*16).join([fill(param,
                                          subsequent_indent=' '*18,
                                          width_adjustment=-18) for param in list_of_params])

def is_job_ref(thing, reftype=dict):
    '''
    :param thing: something that might be a job-based object reference hash
    :param reftype: type that a job-based object reference would be (default is dict)
    '''
    return isinstance(thing, reftype) and \
        len(thing) == 2 and \
        'field' in thing and \
        'job' in thing and \
        isinstance(thing['field'], basestring) and \
        isinstance(thing['job'], basestring)

def io_val_to_str(val):
    if is_job_ref(val):
        # Job-based object references
        return val['job'] + ':' + val['field']
    elif isinstance(val, dict) and '$dnanexus_link' in val:
        # DNAnexus link
        if isinstance(val['$dnanexus_link'], basestring):
            # simple link
            return val['$dnanexus_link']
        elif 'project' in val['$dnanexus_link'] and 'id' in val['$dnanexus_link']:
            return val['$dnanexus_link']['project'] + ':' + val['$dnanexus_link']['id']
        else:
            return json.dumps(val)
    elif isinstance(val, list):
        if len(val) == 0:
            return '[]'
        else:
            return '[ ' + ', '.join([io_val_to_str(item) for item in val]) + ' ]'
    elif isinstance(val, dict):
        return '{ ' + ', '.join([key + ': ' + io_val_to_str(value) for key, value in val.iteritems()]) + ' }'
    else:
        return json.dumps(val)

def get_io_field(io_hash, defaults={}, delim='='):
    if io_hash is None:
        return '-'
    if len(io_hash) == 0 and len(defaults) == 0:
        return '-'
    if get_delimiter() is not None:
        return ('\n' + get_delimiter()).join([(key + delim + io_val_to_str(value)) for key, value in io_hash.items()] +
                                             [('[' + key + delim + io_val_to_str(value) + ']') for key, value in defaults.items()])
    else:
        return ('\n').join([fill(key + ' ' + delim + ' ' + io_val_to_str(value),
                                 initial_indent=' '*16,
                                 subsequent_indent=' '*17,
                                 break_long_words=False) for key, value in io_hash.items()] +
                           [fill('[' + key + ' ' + delim + ' ' + io_val_to_str(value) + ']',
                                 initial_indent=' '*16,
                                 subsequent_indent=' '*17,
                                 break_long_words=False) for key, value in defaults.items()])[16:]

def get_resolved_jbors(resolved_thing, orig_thing, resolved_jbors):
    if is_job_ref(orig_thing):
        resolved_jbors[orig_thing['job'] + ':' + orig_thing['field']] = resolved_thing
    elif isinstance(orig_thing, list):
        for i in range(len(orig_thing)):
            get_resolved_jbors(resolved_thing[i], orig_thing[i], resolved_jbors)
    elif isinstance(orig_thing, dict) and '$dnanexus_link' not in orig_thing:
        for key in orig_thing:
            get_resolved_jbors(resolved_thing[key], orig_thing[key], resolved_jbors)

def render_bundleddepends(thing):
    return [item["name"] + " (" + item["id"]["$dnanexus_link"] + ")" for item in thing]

def render_execdepends(thing):
    rendered = []
    for item in thing:
        if len(item) == 1:
            rendered.append(item['name'])
        elif 'package_manager' in item:
            if item['package_manager'] == 'apt':
                rendered.append(item['name'])
            else:
                rendered.append(item['package_manager'] + ":" + item['name'])
    return rendered

def print_field(label, value):
    if get_delimiter() is not None:
        sys.stdout.write(label + get_delimiter() + value + '\n')
    else:
        sys.stdout.write(label + " " * (16-len(label)) + fill(value, subsequent_indent=' '*16, width_adjustment=-16) + '\n')

def print_nofill_field(label, value):
    sys.stdout.write(label + DELIMITER(" " * (16-len(label))) + value + '\n')

def print_list_field(label, values):
    print_field(label, ('-' if len(values) == 0 else DELIMITER(', ').join(values)))

def print_json_field(label, json_value):
    print_field(label, json.dumps(json_value, ensure_ascii=False))

def print_project_desc(desc):
    recognized_fields = ['id', 'class', 'name', 'description', 'protected', 'restricted', 'created', 'modified', 'dataUsage', 'sponsoredDataUsage', 'tags', 'level', 'folders', 'objects', 'permissions', 'properties', 'appCaches', 'billTo']

    print_field("ID", desc["id"])
    print_field("Class", desc["class"])
    if "name" in desc:
        print_field("Name", desc["name"])
    if 'description' in desc:
        print_field("Description", desc["description"])
    if 'billTo' in desc:
        print_field("Billed to",  desc['billTo'][5 if desc['billTo'].startswith('user-') else 0:])
    if 'protected' in desc:
        print_json_field("Protected", desc["protected"])
    if 'restricted' in desc:
        print_json_field("Restricted", desc["restricted"])
    print_field("Created", datetime.datetime.fromtimestamp(desc['created']/1000).ctime())
    print_field("Last modified", datetime.datetime.fromtimestamp(desc['modified']/1000).ctime())
    print_field("Data usage", ('%.2f' % desc["dataUsage"]) + ' GB')
    if 'sponsoredDataUsage' in desc:
        print_field("Sponsored data", ('%.2f' % desc["sponsoredDataUsage"]) + ' GB')
    if "objects" in desc:
        print_field("# Files", str(desc["objects"]))
    if 'tags' in desc:
        print_list_field("Tags", desc["tags"])
    if "level" in desc:
        print_field("Access level", desc["level"])
    if "folders" in desc:
        print_list_field("Folders", desc["folders"])
    if "permissions" in desc:
        print_list_field("Permissions", [key[5 if key.startswith('user-') else 0:] + ':' + value for key, value in desc["permissions"].items()])
    if "properties" in desc:
        print_list_field("Properties", [key + '=' + value for key, value in desc["properties"].items()])
    if "appCaches" in desc:
        print_json_field("App caches", desc["appCaches"])

    for field in desc:
        if field not in recognized_fields:
            print_json_field(field, desc[field])

def print_app_desc(desc, verbose=False):
    recognized_fields = ['id', 'class', 'name', 'version', 'aliases', 'createdBy', 'created', 'modified', 'deleted', 'published', 'title', 'subtitle', 'description', 'categories', 'access', 'dxapi', 'inputSpec', 'outputSpec', 'runSpec', 'resources', 'billTo', 'installed', 'openSource', 'summary', 'applet', 'installs', 'billing', 'details', 'developerNotes']
    # NOTE: Hiding "billing" for now

    advanced_inputs = [] if verbose else desc["details"].get("advancedInputs")
    if "advancedInputs" in desc["details"]:
        del desc["details"]["advancedInputs"]

    print_field("ID", desc["id"])
    print_field("Class", desc["class"])
    if 'billTo' in desc:
        print_field("Billed to", desc['billTo'][5 if desc['billTo'].startswith('user-') else 0:])
    print_field("Name", desc["name"])
    print_field("Version", desc["version"])
    print_list_field("Aliases", desc["aliases"])
    print_field("Created by", desc["createdBy"][5 if desc['createdBy'].startswith('user-') else 0:])
    print_field("Created", datetime.datetime.fromtimestamp(desc['created']/1000).ctime())
    print_field("Last modified", datetime.datetime.fromtimestamp(desc['modified']/1000).ctime())
    print_field("Created from", desc["applet"])
    print_json_field('Installed', desc['installed'])
    print_json_field('Open source', desc['openSource'])
    print_json_field('Deleted', desc['deleted'])
    if not desc['deleted']:
        if 'published' not in desc or desc["published"] < 0:
            print_field("Published", "-")
        else:
            print_field("Published", datetime.datetime.fromtimestamp(desc['published']/1000).ctime())
        if "title" in desc and desc['title'] is not None:
            print_field("Title", desc["title"])
        if "subtitle" in desc and desc['subtitle'] is not None:
            print_field("Subtitle", desc["subtitle"])
        if 'summary' in desc and desc['summary'] is not None:
            print_field("Summary", desc['summary'])
        print_list_field("Categories", desc["categories"])
        if 'details' in desc:
            print_json_field("Details", desc["details"])
        print_json_field("Access", desc["access"])
        print_field("API version", desc["dxapi"])
        if 'inputSpec' in desc:
            print_nofill_field("Input Spec", get_io_spec(desc["inputSpec"], skip_fields=advanced_inputs))
            print_nofill_field("Output Spec", get_io_spec(desc["outputSpec"]))
            print_field("Interpreter", desc["runSpec"]["interpreter"])
            if "resources" in desc["runSpec"]:
                print_json_field("Resources", desc["runSpec"]["resources"])
            if "bundledDepends" in desc["runSpec"]:
                print_list_field("bundledDepends", render_bundleddepends(desc["runSpec"]["bundledDepends"]))
            if "execDepends" in desc["runSpec"]:
                print_list_field("execDepends", render_execdepends(desc["runSpec"]["execDepends"]))
            if "systemRequirements" in desc['runSpec']:
                print_json_field('Sys Requirements', desc['runSpec']['systemRequirements'])
        if 'resources' in desc:
            print_field("Resources", desc['resources'])
    if 'installs' in desc:
        print_field('# Installs', str(desc['installs']))

    for field in desc:
        if field not in recognized_fields:
            print_json_field(field, desc[field])

def get_col_str(col_desc):
    return col_desc['name'] + DELIMITER(" (") + col_desc['type'] + DELIMITER(")")

def print_data_obj_desc(desc, verbose=False):
    recognized_fields = ['id', 'class', 'project', 'folder', 'name', 'properties', 'tags', 'types', 'hidden', 'details', 'links', 'created', 'modified', 'state', 'title', 'subtitle', 'description', 'inputSpec', 'outputSpec', 'runSpec', 'summary', 'dxapi', 'access', 'createdBy', 'summary', 'sponsored', 'developerNotes']

    advanced_inputs = [] if verbose else desc["details"].get("advancedInputs") if "details" in desc else []

    print_field("ID", desc["id"])
    print_field("Class", desc["class"])
    if 'project' in desc:
        print_field("Project", desc['project'])
    if 'folder' in desc:
        print_field("Folder", desc["folder"])
    print_field("Name", desc["name"])
    if 'state' in desc:
        print_field("State", DATA_STATES(desc['state']))
    if 'hidden' in desc:
        print_field("Visibility", ("hidden" if desc["hidden"] else "visible"))
    if 'types' in desc:
        print_list_field("Types", desc['types'])
    if 'properties' in desc:
        print_list_field("Properties", map(lambda key: key + '=' + desc['properties'][key],
                                           desc['properties'].keys()))
    if 'tags' in desc:
        print_list_field("Tags", desc['tags'])
    if verbose and 'details' in desc:
        print_json_field("Details", desc["details"])
    if 'links' in desc:
        print_list_field("Outgoing links", desc['links'])
    print_field("Created", datetime.datetime.fromtimestamp(desc['created']/1000).ctime())
    if 'createdBy' in desc:
        print_field("Created by", desc['createdBy']['user'][5:])
    print_field("Last modified", datetime.datetime.fromtimestamp(desc['modified']/1000).ctime())
    if "title" in desc:
        print_field("Title", desc["title"])
    if "subtitle" in desc:
        print_field("Subtitle", desc["subtitle"])
    if 'summary' in desc:
        print_field("Summary", desc['summary'])
    if 'access' in desc:
        print_json_field("Access", desc["access"])
    if 'dxapi' in desc:
        print_field("API version", desc["dxapi"])
    if "inputSpec" in desc:
        print_nofill_field("Input Spec", get_io_spec(desc['inputSpec'], skip_fields=advanced_inputs))
    if "outputSpec" in desc:
        print_nofill_field("Output Spec", get_io_spec(desc['outputSpec']))
    if 'runSpec' in desc:
        print_field("Interpreter", desc["runSpec"]["interpreter"])
        if "resources" in desc['runSpec']:
            print_json_field("Resources", desc["runSpec"]["resources"])
        if "bundledDepends" in desc["runSpec"]:
            print_list_field("bundledDepends", render_bundleddepends(desc["runSpec"]["bundledDepends"]))
        if "execDepends" in desc["runSpec"]:
            print_list_field("execDepends", render_execdepends(desc["runSpec"]["execDepends"]))
        if "systemRequirements" in desc['runSpec']:
            print_json_field('Sys Requirements', desc['runSpec']['systemRequirements'])

    for field in desc:
        if field in recognized_fields:
            continue
        else:
            if field == "media":
                print_field("Media type", desc['media'])
            elif field == "size":
                if desc["class"] == "file" or desc["class"] == "gtable":
                    sponsored_str = ""
                    if 'sponsored' in desc and desc['sponsored']:
                        sponsored_str = ", sponsored by DNAnexus"
                    print_field("Size", get_size_str(desc['size']) + sponsored_str)
                else:
                    print_field("Size", str(desc['size']))
            elif field == "length":
                if desc["class"] == "gtable" or desc['class'] == 'table':
                    print_field("Size (rows)", str(desc['length']))
                else:
                    print_field("Length", str(desc['length']))
            elif field == "columns":
                if len(desc['columns']) > 0:
                    coldescs = "Columns" + DELIMITER(" " *(16-len("Columns"))) + get_col_str(desc["columns"][0])
                    for column in desc["columns"][1:]:
                        coldescs += '\n' + DELIMITER(" "*16) + get_col_str(column)
                    print coldescs
                else:
                    print_list_field("Columns", desc['columns'])
            else: # Unhandled prettifying
                print_json_field(field, desc[field])

def print_job_desc(desc):
    recognized_fields = ['id', 'class', 'project', 'workspace', 'app', 'state', 'parentJob', 'originJob',
                         'function', 'runInput', 'originalInput', 'input', 'output', 'folder', 'launchedBy', 'created',
                         'modified', 'failureReason', 'failureMessage', 'stdout', 'stderr', 'waitingOnChildren',
                         'dependsOn', 'resources', 'projectCache', 'applet',
                         'name', 'instanceType', 'systemRequirements', 'executableName', 'failureFrom', 'billTo',
                         'startedRunning', 'stoppedRunning', 'stateTransitions']

    print_field("ID", desc["id"])
    print_field("Class", desc["class"])
    if "name" in desc and desc['name'] is not None:
        print_field("Job name", desc['name'])
    if "executableName" in desc and desc['executableName'] is not None:
        print_field("Executable name", desc['executableName'])
    print_field("Project context", desc["project"])
    if 'billTo' in desc:
        print_field("Billed to",  desc['billTo'][5 if desc['billTo'].startswith('user-') else 0:])
    if 'workspace' in desc:
        print_field("Workspace", desc["workspace"])
    if 'projectCache' in desc:
        print_field('Cache workspace', desc['projectCache'])
        print_field('Resources', desc['resources'])
    if "app" in desc:
        print_field("App", desc["app"])
    elif "applet" in desc:
        print_field("Applet", desc["applet"])
    if "instanceType" in desc and desc['instanceType'] is not None:
        print_field("Instance Type", desc["instanceType"])
    print_field("State", JOB_STATES(desc["state"]))
    if desc["parentJob"] is None:
        print_field("Parent job", "-")
    else:
        print_field("Parent job", desc["parentJob"])
    print_field("Origin job", desc["originJob"])
    print_field("Function", desc["function"])
    if 'runInput' in desc:
        default_fields = {k: v for k, v in desc["originalInput"].iteritems() if k not in desc["runInput"]}
        print_nofill_field("Input", get_io_field(desc["runInput"], defaults=default_fields))
    else:
        print_nofill_field("Input", get_io_field(desc["originalInput"]))
    resolved_jbors = {}
    for k in desc["input"]:
        if desc["input"][k] != desc["originalInput"][k]:
            get_resolved_jbors(desc["input"][k], desc["originalInput"][k], resolved_jbors)
    if len(resolved_jbors) != 0:
        print_nofill_field("Resolved JBORs", get_io_field(resolved_jbors, delim=(GREEN() + '=>' + ENDC())))
    print_nofill_field("Output", get_io_field(desc["output"]))
    if 'folder' in desc:
        print_field('Output folder', desc['folder'])
    print_field("Launched by", desc["launchedBy"][5:])
    print_field("Created", datetime.datetime.fromtimestamp(desc['created']/1000).ctime())
    if 'startedRunning' in desc:
        if 'stoppedRunning' in desc:
            print_field("Started running", datetime.datetime.fromtimestamp(desc['startedRunning']/1000).ctime())
        else:
            print_field("Started running", "{t} (running for {rt})".format(t=datetime.datetime.fromtimestamp(desc['startedRunning']/1000).ctime(),
                rt=datetime.timedelta(milliseconds=int(time.time()*1000)-desc['startedRunning'])))
    if 'stoppedRunning' in desc:
        print_field("Stopped running", "{t} (Runtime: {rt})".format(
            t=datetime.datetime.fromtimestamp(desc['stoppedRunning']/1000).ctime(),
            rt=datetime.timedelta(milliseconds=desc['stoppedRunning']-desc['startedRunning'])))
    print_field("Last modified", datetime.datetime.fromtimestamp(desc['modified']/1000).ctime())
    if 'waitingOnChildren' in desc:
        print_list_field('Pending subjobs', desc['waitingOnChildren'])
    if 'dependsOn' in desc:
        print_list_field('Depends on', desc['dependsOn'])
    if "failureReason" in desc:
        print_field("Failure reason", desc["failureReason"])
    if "failureMessage" in desc:
        print_field("Failure message", desc["failureMessage"])
    if "failureFrom" in desc and desc['failureFrom'] is not None:
        print_field("Failure is from", desc['failureFrom']['id'])
    if "stdout" in desc:
        print_field("File of stdout", str(desc['stdout']))
    if 'stderr' in desc:
        print_field('File of stderr', str(desc['stderr']))
    if 'systemRequirements' in desc:
        print_json_field("Sys Requirements", desc['systemRequirements'])
    for field in desc:
        if field not in recognized_fields:
            print_json_field(field, desc[field])

def print_user_desc(desc):
    print_field("ID", desc["id"])
    print_field("Name", desc["first"] + " " + ((desc["middle"] + " ") if desc["middle"] != '' else '') + desc["last"])
    if "email" in desc:
        print_field("Email", desc["email"])
    if "appsInstalled" in desc:
        print_list_field("Apps installed", desc["appsInstalled"])

def print_generic_desc(desc):
    for field in desc:
        print_json_field(field, desc[field])

def print_desc(desc, verbose=False):
    '''
    :param desc: The describe hash of a DNAnexus entity
    :type desc: dict

    Depending on the class of the entity, this method will print a
    formatted and human-readable string containing the data in *desc*.
    '''
    if desc['class'] in ['project', 'workspace', 'container']:
        print_project_desc(desc)
    elif desc['class'] == 'app':
        print_app_desc(desc, verbose=verbose)
    elif desc['class'] == 'job':
        print_job_desc(desc)
    elif desc['class'] == 'user':
        print_user_desc(desc)
    elif desc['class'] in ['org', 'team']:
        print_generic_desc(desc)
    else:
        print_data_obj_desc(desc, verbose=verbose)

def get_ls_desc(desc, print_id=False):
    addendum = ' : ' + desc['id'] if print_id is True else ''
    if desc['class'] == 'applet' or (desc['class'] == 'record' and 'pipeline' in desc['types']):
        return BOLD() + GREEN() + desc['name'] + ENDC() + addendum
    else:
        return desc['name'] + addendum

def print_ls_desc(desc, **kwargs):
    print get_ls_desc(desc, **kwargs)

def get_ls_l_desc(desc, include_folder=False, include_project=False):
    if 'state' in desc:
        state_len = len(desc['state'])
        if desc['state'] != 'closed':
            state_str = YELLOW() + desc['state'] + ENDC()
        else:
            state_str = GREEN() + desc['state'] + ENDC()
    else:
        state_str = ''
        state_len = 0

    name_str = ''
    if include_folder:
        name_str += desc['folder'] + ('/' if desc['folder'] != '/' else '')

    name_str += desc['name']

    if desc['class'] == 'applet' or (desc['class'] == 'record' and 'pipeline' in desc['types']):
        name_str = BOLD() + GREEN() + name_str + ENDC()

    size_str = ''
    if 'size' in desc and desc['class'] == 'file':
        size_str = get_size_str(desc['size'])
    elif 'length' in desc:
        size_str = str(desc['length']) + ' rows'
    size_padding = ' '*(max(0, 8 - len(size_str)))

    return state_str + DELIMITER(' '*(8 - state_len)) + str(datetime.datetime.fromtimestamp(desc['modified']/1000)) + DELIMITER(' ') + size_str + DELIMITER(size_padding + ' ') + name_str + DELIMITER(' (') + ((desc['project'] + DELIMITER(':')) if include_project else '') + desc['id'] + DELIMITER(')')

def print_ls_l_desc(desc, **kwargs):
    print get_ls_l_desc(desc, **kwargs)

def get_find_jobs_string(jobdesc, has_children, single_result=False):
    '''
    :param jobdesc: hash of job describe output
    :param has_children: whether the job has subjobs to be printed
    :param single_result: whether the job is displayed as a single result or as part of a job tree
    '''
    is_origin_job = jobdesc['parentJob'] is None or single_result
    string = ("* " if is_origin_job and get_delimiter() is None else "")
    string += (BOLD() + BLUE() + (jobdesc['name'] if 'name' in jobdesc else "<no name>") + ENDC()) + DELIMITER(' (') + JOB_STATES(jobdesc['state']) + DELIMITER(') ') + jobdesc['id'] 
    string += DELIMITER('\n' + (u'│ ' if is_origin_job and has_children else ("  " if is_origin_job else "")))
    string += jobdesc['launchedBy'][5:] + DELIMITER(' ')
    string += str(datetime.datetime.fromtimestamp(jobdesc['created']/1000))
    if jobdesc['state'] in ['done', 'failed', 'terminated', 'waiting_on_output']:
        # TODO: Remove this check once all jobs are migrated to have these values
        if 'stoppedRunning' in jobdesc and 'startedRunning' in jobdesc:
            string += " (runtime {r})".format(r=str(datetime.timedelta(seconds=int(jobdesc['stoppedRunning']-jobdesc['startedRunning'])/1000)))
    elif jobdesc['state'] == 'running':
        string += " (running for {rt})".format(rt=datetime.timedelta(seconds=int(time.time()-jobdesc['startedRunning']/1000)))

    return string
