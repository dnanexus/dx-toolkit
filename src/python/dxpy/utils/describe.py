# -*- coding: utf-8 -*-
#
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
This submodule contains helper functions for parsing and printing the
contents of describe hashes for various DNAnexus entities (projects,
containers, dataobjects, apps, and jobs).
'''

from __future__ import print_function, unicode_literals, division, absolute_import

import datetime, time, json, math, sys, copy
import subprocess
from collections import defaultdict

import dxpy
from .printing import (RED, GREEN, BLUE, YELLOW, WHITE, BOLD, UNDERLINE, ENDC, DELIMITER, get_delimiter, fill)
from ..compat import basestring

def JOB_STATES(state):
    if state == 'failed':
        return BOLD() + RED() + state + ENDC()
    elif state == 'done':
        return BOLD() + GREEN() + state + ENDC()
    elif state in ['running', 'in_progress']:
        return GREEN() + state + ENDC()
    elif state == 'partially_failed':
        return RED() + state + ENDC()
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
    """
    Formats a byte size as a string.

    The returned string is no more than 9 characters long.
    """
    if size == 0:
        magnitude = 0
        level = 0
    else:
        magnitude = math.floor(math.log(size, 10))
        level = int(min(math.floor(magnitude // 3), 4))
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
    # For interactive help, format array:CLASS inputs as:
    #   -iNAME=CLASS [-iNAME=... [...]]   # If input is required (needs >=1 inputs)
    #   [-iNAME=CLASS [...]]              # If input is optional (needs >=0 inputs
    if app_help_version and parameter["class"].startswith("array"):
        scalar_parameter = parameter.copy()
        # Munge the parameter dict (strip off "array:" to turn it into a
        # scalar) and recurse
        scalar_parameter["class"] = scalar_parameter["class"][6:]
        if "default" in parameter or parameter.get("optional"):
            return "[" + get_io_desc(scalar_parameter, include_class=include_class, show_opt=False, app_help_version=app_help_version) + " [-i%s=... [...]]]" % (parameter["name"],)
        else:
            return get_io_desc(scalar_parameter, include_class=include_class, show_opt=False, app_help_version=app_help_version) + " [-i%s=... [...]]" % (parameter["name"],)

    desc = ""
    is_optional = False
    if show_opt:
        if "default" in parameter or parameter.get("optional"):
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
    filtered_spec = [param for param in spec if param["name"] not in skip_fields]
    groups = defaultdict(list)
    for param in filtered_spec:
        groups[param.get('group')].append(param)

    list_of_params = []
    for param in groups.get(None, []):
        list_of_params.append(get_io_desc(param))
    for group in groups:
        if group is None:
            continue
        list_of_params.append("{g}:".format(g=group))
        for param in groups[group]:
            list_of_params.append("    "+get_io_desc(param))

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
        ((len(thing) == 2 and \
              isinstance(thing.get('field'), basestring) and \
              isinstance(thing.get('job'), basestring)) or \
             (len(thing) == 1 and \
                  isinstance(thing.get('$dnanexus_link'), reftype) and \
                  isinstance(thing['$dnanexus_link'].get('field'), basestring) and \
                  isinstance(thing['$dnanexus_link'].get('job'), basestring)))

def get_job_from_jbor(thing):
    '''
    :returns: Job ID from a JBOR

    Assumes :func:`is_job_ref` evaluates to True
    '''
    if '$dnanexus_link' in thing:
        return thing['$dnanexus_link']['job']
    else:
        return thing['job']

def get_field_from_jbor(thing):
    '''
    :returns: Output field name from a JBOR

    Assumes :func:`is_job_ref` evaluates to True
    '''
    if '$dnanexus_link' in thing:
        return thing['$dnanexus_link']['field']
    else:
        return thing['field']

def get_index_from_jbor(thing):
    '''
    :returns: Array index of the JBOR if applicable; None otherwise

    Assumes :func:`is_job_ref` evaluates to True
    '''
    if '$dnanexus_link' in thing:
        return thing['$dnanexus_link'].get('index')
    else:
        return None

def is_metadata_ref(thing, reftype=dict):
    return isinstance(thing, reftype) and \
        len(thing) == 1 and \
        isinstance(thing.get('$dnanexus_link'), reftype) and \
        isinstance(thing['$dnanexus_link'].get('metadata'), basestring)

def jbor_to_str(val):
    ans = get_job_from_jbor(val) + ':' + get_field_from_jbor(val)
    index = get_index_from_jbor(val)
    if index is not None:
        ans += "." + str(index)
    return ans

def io_val_to_str(val):
    if is_job_ref(val):
        # Job-based object references
        return jbor_to_str(val)
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
        return '{ ' + ', '.join([key + ': ' + io_val_to_str(value) for key, value in val.items()]) + ' }'
    else:
        return json.dumps(val)

def job_output_to_str(job_output, prefix='\n', title="Output: ", title_len=None):
    if len(job_output) == 0:
        return prefix + title + "-"
    else:
        if title_len is None:
            title_len = len(title)
        return prefix + title + (prefix+' '*title_len).join([fill(key + ' = ' + io_val_to_str(value),
                                                                   subsequent_indent=' '*9,
                                                                   break_long_words=False) for key, value in job_output.items()])


def get_io_field(io_hash, defaults=None, delim='=', highlight_fields=()):

    def highlight_value(key, value):
        if key in highlight_fields:
            return YELLOW() + value + ENDC()
        else:
            return value

    if defaults is None:
        defaults = {}
    if io_hash is None:
        return '-'
    if len(io_hash) == 0 and len(defaults) == 0:
        return '-'
    if get_delimiter() is not None:
        return ('\n' + get_delimiter()).join([(key + delim + highlight_value(key, io_val_to_str(value))) for key, value in io_hash.items()] +
                                             [('[' + key + delim + io_val_to_str(value) + ']') for key, value in defaults.items()])
    else:
        lines = [fill(key + ' ' + delim + ' ' + highlight_value(key, io_val_to_str(value)),
                      initial_indent=' ' * FIELD_NAME_WIDTH,
                      subsequent_indent=' ' * (FIELD_NAME_WIDTH + 1),
                      break_long_words=False)
                 for key, value in io_hash.items()]
        lines.extend([fill('[' + key + ' ' + delim + ' ' + io_val_to_str(value) + ']',
                           initial_indent=' ' * FIELD_NAME_WIDTH,
                           subsequent_indent=' ' * (FIELD_NAME_WIDTH + 1),
                           break_long_words=False)
                      for key, value in defaults.items()])
        return '\n'.join(lines)[FIELD_NAME_WIDTH:]

def get_resolved_jbors(resolved_thing, orig_thing, resolved_jbors):
    if resolved_thing == orig_thing:
        return
    if is_job_ref(orig_thing):
        jbor_str = jbor_to_str(orig_thing)
        if jbor_str not in resolved_jbors:
            try:
                from dxpy.api import job_describe
                job_output = job_describe(get_job_from_jbor(orig_thing)).get('output')
                if job_output is not None:
                    field_value = job_output.get(get_field_from_jbor(orig_thing))
                    jbor_index = get_index_from_jbor(orig_thing)
                    if jbor_index is not None:
                        if isinstance(field_value, list):
                            resolved_jbors[jbor_str] = field_value[jbor_index]
                    else:
                        resolved_jbors[jbor_str] = field_value
            except:
                # Just don't report any resolved JBORs if there are
                # any problems
                pass
    elif isinstance(orig_thing, list):
        for i in range(len(orig_thing)):
            get_resolved_jbors(resolved_thing[i], orig_thing[i], resolved_jbors)
    elif isinstance(orig_thing, dict) and '$dnanexus_link' not in orig_thing:
        for key in orig_thing:
            get_resolved_jbors(resolved_thing[key], orig_thing[key], resolved_jbors)

def render_bundleddepends(thing):
    from ..bindings.search import find_one_data_object
    from ..exceptions import DXError
    bundles = []
    for item in thing:
        bundle_asset_record = dxpy.DXFile(item["id"]["$dnanexus_link"]).get_properties().get("AssetBundle")
        asset = None

        if bundle_asset_record:
            asset = dxpy.DXRecord(bundle_asset_record)

        if asset:
            try:
                bundles.append(asset.describe().get("name") + " (" + asset.get_id() + ")")
            except DXError:
                asset = None

        if not asset:
            bundles.append(item["name"] + " (" + item["id"]["$dnanexus_link"] + ")")

    return bundles

def render_execdepends(thing):
    rendered = []
    for item in thing:
        dep = copy.copy(item)
        dep.setdefault('package_manager', 'apt')
        dep['version'] = ' = '+dep['version'] if 'version' in dep else ''
        rendered.append("{package_manager}: {name}{version}".format(**dep))
    return rendered

def render_stage(title, stage, as_stage_of=None):
    lines_to_print = []

    if stage['name'] is not None:
        lines_to_print.append((title, "{name} ({id})".format(name=stage['name'], id=stage['id'])))
    else:
        lines_to_print.append((title, stage['id']))

    lines_to_print.append(('  Executable', stage['executable'] + \
                           (" (" + RED() + "inaccessible" + ENDC() + ")" \
                            if stage.get('accessible') is False else "")))

    if 'execution' in stage:
        is_cached_result = as_stage_of is not None and 'parentAnalysis' in stage['execution'] and \
                           stage['execution']['parentAnalysis'] != as_stage_of
        execution_id_str = stage['execution']['id']
        if is_cached_result:
            execution_id_str = "[" + execution_id_str + "]"

        if 'state' in stage['execution']:
            lines_to_print.append(('  Execution', execution_id_str + ' (' + JOB_STATES(stage['execution']['state']) + ')'))
        else:
            lines_to_print.append(('  Execution', execution_id_str))

        if is_cached_result:
            lines_to_print.append(('  Cached from', stage['execution']['parentAnalysis']))

    for line in lines_to_print:
        print_field(line[0], line[1])

def render_short_timestamp(timestamp):
    return str(datetime.datetime.fromtimestamp(timestamp//1000))

def render_timestamp(timestamp):
    return datetime.datetime.fromtimestamp(timestamp//1000).ctime()


FIELD_NAME_WIDTH = 20


def print_field(label, value):
    if get_delimiter() is not None:
        sys.stdout.write(label + get_delimiter() + value + '\n')
    else:
        sys.stdout.write(
            label + " " * (FIELD_NAME_WIDTH-len(label)) + fill(value,
                                                               subsequent_indent=' '*FIELD_NAME_WIDTH,
                                                               width_adjustment=-FIELD_NAME_WIDTH) +
            '\n')


def print_nofill_field(label, value):
    sys.stdout.write(label + DELIMITER(" " * (FIELD_NAME_WIDTH - len(label))) + value + '\n')


def print_list_field(label, values):
    print_field(label, ('-' if len(values) == 0 else DELIMITER(', ').join(values)))

def print_json_field(label, json_value):
    print_field(label, json.dumps(json_value, ensure_ascii=False))


def print_project_desc(desc, verbose=False):
    recognized_fields = [
        'id', 'class', 'name', 'summary', 'description', 'protected', 'restricted', 'created', 'modified',
        'dataUsage', 'sponsoredDataUsage', 'tags', 'level', 'folders', 'objects', 'permissions', 'properties',
        'appCaches', 'billTo', 'version', 'createdBy', 'totalSponsoredEgressBytes', 'consumedSponsoredEgressBytes',
        'containsPHI', 'region', 'storageCost', 'pendingTransfer', 'archivalState', 'atSpendingLimit',
        'archivalProgress',
        # Following are app container-specific
        'destroyAt', 'project', 'type', 'app', 'appName'
    ]

    # Basic metadata
    print_field("ID", desc["id"])
    print_field("Class", desc["class"])
    if "name" in desc:
        print_field("Name", desc["name"])
    if 'summary' in desc:
        print_field("Summary", desc["summary"])
    if 'description' in desc and (verbose or 'summary' not in desc):
        print_field("Description", desc['description'])
    if 'version' in desc and verbose:
        print_field("Version", str(desc['version']))

    # Ownership and permissions
    if 'billTo' in desc:
        print_field("Billed to",  desc['billTo'][5 if desc['billTo'].startswith('user-') else 0:])
    if 'pendingTransfer' in desc and (verbose or desc['pendingTransfer'] is not None):
        print_json_field('Pending transfer to', desc['pendingTransfer'])
    if "level" in desc:
        print_field("Access level", desc["level"])
    if 'region' in desc:
        print_field('Region', desc['region'])

    # Project settings
    if 'protected' in desc:
        print_json_field("Protected", desc["protected"])
    if 'restricted' in desc:
        print_json_field("Restricted", desc["restricted"])
    if 'containsPHI' in desc:
        print_json_field('Contains PHI', desc['containsPHI'])
    if 'archivalState' in desc and verbose:
        print_field('Archival state', desc['archivalState'])
    if 'archivalProgress' in desc and verbose:
        print_json_field('Archival progress', desc['archivalProgress'])

    # Usage
    print_field("Created", render_timestamp(desc['created']))
    if 'createdBy' in desc:
        print_field("Created by", desc['createdBy']['user'][desc['createdBy']['user'].find('-') + 1:])
    print_field("Last modified", render_timestamp(desc['modified']))
    print_field("Data usage", ('%.2f' % desc["dataUsage"]) + ' GB')
    if 'sponsoredDataUsage' in desc:
        print_field("Sponsored data", ('%.2f' % desc["sponsoredDataUsage"]) + ' GB')
    if 'storageCost' in desc:
        print_field("Storage cost", "$%.3f/month" % desc["storageCost"])
    if 'totalSponsoredEgressBytes' in desc or 'consumedSponsoredEgressBytes' in desc:
        total_egress_str = '%.2f GB' % (desc['totalSponsoredEgressBytes'] / 1073741824.,) \
                           if 'totalSponsoredEgressBytes' in desc else '??'
        consumed_egress_str = '%.2f GB' % (desc['consumedSponsoredEgressBytes'] / 1073741824.,) \
                              if 'consumedSponsoredEgressBytes' in desc else '??'
        print_field('Sponsored egress',
                    ('%s used of %s total' % (consumed_egress_str, total_egress_str)))
    if 'atSpendingLimit' in desc:
        print_json_field("At spending limit?", desc['atSpendingLimit'])

    # Misc metadata
    if "objects" in desc:
        print_field("# Files", str(desc["objects"]))
    if "folders" in desc:
        print_list_field("Folders", desc["folders"])
    if "permissions" in desc:
        print_list_field(
            "Permissions",
            [key[5 if key.startswith('user-') else 0:] + ':' + value for key, value in desc["permissions"].items()]
        )
    if 'tags' in desc:
        print_list_field("Tags", desc["tags"])
    if "properties" in desc:
        print_list_field("Properties", [key + '=' + value for key, value in desc["properties"].items()])

    if "appCaches" in desc:
        print_json_field("App caches", desc["appCaches"])

    # Container-specific
    if 'type' in desc:
        print_field("Container type", desc["type"])
    if 'project' in desc:
        print_field("Associated project", desc["project"])
    if 'destroyAt' in desc:
        print_field("To be destroyed", render_timestamp(desc['modified']))
    if 'app' in desc:
        print_field("Associated App ID", desc["app"])
    if 'appName' in desc:
        print_field("Associated App", desc["appName"])

    for field in desc:
        if field not in recognized_fields:
            print_json_field(field, desc[field])


def print_app_desc(desc, verbose=False):
    recognized_fields = ['id', 'class', 'name', 'version', 'aliases', 'createdBy', 'created', 'modified', 'deleted', 'published', 'title', 'subtitle', 'description', 'categories', 'access', 'dxapi', 'inputSpec', 'outputSpec', 'runSpec', 'resources', 'billTo', 'installed', 'openSource', 'summary', 'applet', 'installs', 'billing', 'details', 'developerNotes',
                         'authorizedUsers']
    print_field("ID", desc["id"])
    print_field("Class", desc["class"])
    if 'billTo' in desc:
        print_field("Billed to", desc['billTo'][5 if desc['billTo'].startswith('user-') else 0:])
    print_field("Name", desc["name"])
    print_field("Version", desc["version"])
    print_list_field("Aliases", desc["aliases"])
    print_field("Created by", desc["createdBy"][5 if desc['createdBy'].startswith('user-') else 0:])
    print_field("Created", render_timestamp(desc['created']))
    print_field("Last modified", render_timestamp(desc['modified']))
    print_field("Created from", desc["applet"])
    print_json_field('Installed', desc['installed'])
    print_json_field('Open source', desc['openSource'])
    print_json_field('Deleted', desc['deleted'])
    if not desc['deleted']:
        advanced_inputs = []
        details = desc["details"]
        if isinstance(details, dict) and "advancedInputs" in details:
            if not verbose:
                advanced_inputs = details["advancedInputs"]
            del details["advancedInputs"]

        if 'published' not in desc or desc["published"] < 0:
            print_field("Published", "-")
        else:
            print_field("Published", render_timestamp(desc['published']))
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
    if 'authorizedUsers' in desc:
        print_list_field('AuthorizedUsers', desc["authorizedUsers"])

    for field in desc:
        if field not in recognized_fields:
            print_json_field(field, desc[field])

def get_col_str(col_desc):
    return col_desc['name'] + DELIMITER(" (") + col_desc['type'] + DELIMITER(")")

def print_data_obj_desc(desc, verbose=False):
    recognized_fields = ['id', 'class', 'project', 'folder', 'name', 'properties', 'tags', 'types', 'hidden', 'details', 'links', 'created', 'modified', 'state', 'title', 'subtitle', 'description', 'inputSpec', 'outputSpec', 'runSpec', 'summary', 'dxapi', 'access', 'createdBy', 'summary', 'sponsored', 'developerNotes',
                         'stages', 'latestAnalysis', 'editVersion', 'outputFolder', 'initializedFrom']

    def get_advanced_inputs():
        details = desc.get("details")
        if not verbose and isinstance(details, dict):
            return details.get("advancedInputs", [])
        return []

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
        print_list_field("Properties", ['='.join([k, v]) for k, v in desc['properties'].items()])
    if 'tags' in desc:
        print_list_field("Tags", desc['tags'])
    if verbose and 'details' in desc:
        print_json_field("Details", desc["details"])
    if 'links' in desc:
        print_list_field("Outgoing links", desc['links'])
    print_field("Created", render_timestamp(desc['created']))
    if 'createdBy' in desc:
        print_field("Created by", desc['createdBy']['user'][5:])
        if 'job' in desc["createdBy"]:
            print_field(" via the job", desc['createdBy']['job'])
            if verbose and 'executable' in desc['createdBy']:
                print_field(" running", desc['createdBy']['executable'])
    print_field("Last modified", render_timestamp(desc['modified']))
    if "editVersion" in desc:
        print_field("Edit Version", str(desc['editVersion']))
    if "title" in desc:
        print_field("Title", desc["title"])
    if "subtitle" in desc:
        print_field("Subtitle", desc["subtitle"])
    if 'summary' in desc:
        print_field("Summary", desc['summary'])
    if 'description' in desc and verbose:
        print_field("Description", desc["description"])
    if 'outputFolder' in desc:
        print_field("Output Folder", desc["outputFolder"] if desc["outputFolder"] is not None else "-")
    if 'access' in desc:
        print_json_field("Access", desc["access"])
    if 'dxapi' in desc:
        print_field("API version", desc["dxapi"])
    if "inputSpec" in desc and desc['inputSpec'] is not None:
        print_nofill_field("Input Spec", get_io_spec(desc['inputSpec'], skip_fields=get_advanced_inputs()))
    if "outputSpec" in desc and desc['outputSpec'] is not None:
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
    if 'stages' in desc:
        for i, stage in enumerate(desc["stages"]):
            render_stage("Stage " + str(i), stage)
    if 'initializedFrom' in desc:
        print_field("initializedFrom", desc["initializedFrom"]["id"])
    if 'latestAnalysis' in desc and desc['latestAnalysis'] is not None:
        print_field("Last execution", desc["latestAnalysis"]["id"])
        print_field("  run at", render_timestamp(desc["latestAnalysis"]["created"]))
        print_field("  state", JOB_STATES(desc["latestAnalysis"]["state"]))

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
                        sponsored_str = DELIMITER(", ") + "sponsored by DNAnexus"
                    print_field("Size", get_size_str(desc['size']) + sponsored_str)
                else:
                    print_field("Size", str(desc['size']))
            elif field == "length":
                if desc["class"] == "gtable":
                    print_field("Size (rows)", str(desc['length']))
                else:
                    print_field("Length", str(desc['length']))
            elif field == "columns":
                if len(desc['columns']) > 0:
                    coldescs = "Columns" + DELIMITER(" " *(16-len("Columns"))) + get_col_str(desc["columns"][0])
                    for column in desc["columns"][1:]:
                        coldescs += '\n' + DELIMITER(" "*16) + get_col_str(column)
                    print(coldescs)
                else:
                    print_list_field("Columns", desc['columns'])
            else: # Unhandled prettifying
                print_json_field(field, desc[field])


def printable_ssh_host_key(ssh_host_key):
    try:
        keygen = subprocess.Popen(["ssh-keygen", "-lf", "/dev/stdin"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        (stdout, stderr) = keygen.communicate(ssh_host_key)
    except:
        return ssh_host_key.strip()
    else:
        return stdout.replace(" no comment", "").strip()


def print_execution_desc(desc):
    recognized_fields = ['id', 'class', 'project', 'workspace', 'region',
                         'app', 'applet', 'executable', 'workflow',
                         'state',
                         'rootExecution', 'parentAnalysis', 'parentJob', 'originJob', 'analysis', 'stage',
                         'function', 'runInput', 'originalInput', 'input', 'output', 'folder', 'launchedBy', 'created',
                         'modified', 'failureReason', 'failureMessage', 'stdout', 'stderr', 'waitingOnChildren',
                         'dependsOn', 'resources', 'projectCache', 'details', 'tags', 'properties',
                         'name', 'instanceType', 'systemRequirements', 'executableName', 'failureFrom', 'billTo',
                         'startedRunning', 'stoppedRunning', 'stateTransitions',
                         'delayWorkspaceDestruction', 'stages', 'totalPrice', 'isFree', 'invoiceMetadata',
                         'priority', 'sshHostKey']

    print_field("ID", desc["id"])
    print_field("Class", desc["class"])
    if "name" in desc and desc['name'] is not None:
        print_field("Job name", desc['name'])
    if "executableName" in desc and desc['executableName'] is not None:
        print_field("Executable name", desc['executableName'])
    print_field("Project context", desc["project"])
    if 'region' in desc:
        print_field("Region", desc["region"])
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
    elif "workflow" in desc:
        print_field("Workflow", desc["workflow"]["id"])
    if "instanceType" in desc and desc['instanceType'] is not None:
        print_field("Instance Type", desc["instanceType"])
    if "priority" in desc:
        print_field("Priority", desc["priority"])
    print_field("State", JOB_STATES(desc["state"]))
    if "rootExecution" in desc:
        print_field("Root execution", desc["rootExecution"])
    if "originJob" in desc:
        if desc["originJob"] is None:
            print_field("Origin job", "-")
        else:
            print_field("Origin job", desc["originJob"])
    if desc["parentJob"] is None:
        print_field("Parent job", "-")
    else:
        print_field("Parent job", desc["parentJob"])
    if "parentAnalysis" in desc:
        if desc["parentAnalysis"] is not None:
            print_field("Parent analysis", desc["parentAnalysis"])
    if "analysis" in desc and desc["analysis"] is not None:
        print_field("Analysis", desc["analysis"])
        print_field("Stage", desc["stage"])
    if "stages" in desc:
        for i, (stage, analysis_stage) in enumerate(zip(desc["workflow"]["stages"], desc["stages"])):
            stage['execution'] = analysis_stage['execution']
            render_stage("Stage " + str(i), stage, as_stage_of=desc["id"])
    if "function" in desc:
        print_field("Function", desc["function"])
    if 'runInput' in desc:
        default_fields = {k: v for k, v in desc["originalInput"].items() if k not in desc["runInput"]}
        print_nofill_field("Input", get_io_field(desc["runInput"], defaults=default_fields))
    else:
        print_nofill_field("Input", get_io_field(desc["originalInput"]))
    resolved_jbors = {}
    input_with_jbors = desc.get('runInput', desc['originalInput'])
    for k in desc["input"]:
        if k in input_with_jbors and desc["input"][k] != input_with_jbors[k]:
            get_resolved_jbors(desc["input"][k], input_with_jbors[k], resolved_jbors)
    if len(resolved_jbors) != 0:
        print_nofill_field("Resolved JBORs", get_io_field(resolved_jbors, delim=(GREEN() + '=>' + ENDC())))
    print_nofill_field("Output", get_io_field(desc["output"]))
    if 'folder' in desc:
        print_field('Output folder', desc['folder'])
    print_field("Launched by", desc["launchedBy"][5:])
    print_field("Created", render_timestamp(desc['created']))
    if 'startedRunning' in desc:
        if 'stoppedRunning' in desc:
            print_field("Started running", render_timestamp(desc['startedRunning']))
        else:
            print_field("Started running", "{t} (running for {rt})".format(t=render_timestamp(desc['startedRunning']),
                rt=datetime.timedelta(seconds=int(time.time())-desc['startedRunning']//1000)))
    if 'stoppedRunning' in desc:
        print_field("Stopped running", "{t} (Runtime: {rt})".format(
            t=render_timestamp(desc['stoppedRunning']),
            rt=datetime.timedelta(seconds=(desc['stoppedRunning']-desc['startedRunning'])//1000)))
    if desc.get('class') == 'analysis' and 'stateTransitions' in desc and desc['stateTransitions']:
        # Display finishing time of the analysis if available
        if desc['stateTransitions'][-1]['newState'] in ['done', 'failed', 'terminated']:
            print_field("Finished", "{t} (Wall-clock time: {wt})".format(
                t=render_timestamp(desc['stateTransitions'][-1]['setAt']),
                wt=datetime.timedelta(seconds=(desc['stateTransitions'][-1]['setAt']-desc['created'])//1000)))
    print_field("Last modified", render_timestamp(desc['modified']))
    if 'waitingOnChildren' in desc:
        print_list_field('Pending subjobs', desc['waitingOnChildren'])
    if 'dependsOn' in desc:
        print_list_field('Depends on', desc['dependsOn'])
    if "failureReason" in desc:
        print_field("Failure reason", desc["failureReason"])
    if "failureMessage" in desc:
        print_field("Failure message", desc["failureMessage"])
    if "failureFrom" in desc and desc['failureFrom'] is not None and desc['failureFrom']['id'] != desc['id']:
        print_field("Failure is from", desc['failureFrom']['id'])
    if 'systemRequirements' in desc:
        print_json_field("Sys Requirements", desc['systemRequirements'])
    if "tags" in desc:
        print_list_field("Tags", desc["tags"])
    if "properties" in desc:
        print_list_field("Properties", [key + '=' + value for key, value in desc["properties"].items()])
    if "details" in desc and "clonedFrom" in desc["details"]:
        cloned_hash = desc["details"]["clonedFrom"]
        if "id" in cloned_hash:
            print_field("Re-run of", cloned_hash["id"])
            print_field(" named", cloned_hash["name"])

            same_executable = cloned_hash["executable"] == desc.get("applet", desc.get("app", ""))
            print_field(" using", ("" if same_executable else YELLOW()) + \
                            cloned_hash["executable"] + \
                            (" (same)" if same_executable else ENDC()))
            same_project = cloned_hash["project"] == desc["project"]
            same_folder = cloned_hash["folder"] == desc["folder"] or not same_project
            print_field(" output folder", ("" if same_project else YELLOW()) + \
                            cloned_hash["project"] + \
                            ("" if same_project else ENDC()) + ":" + \
                            ("" if same_folder else YELLOW()) + \
                            cloned_hash["folder"] + \
                            (" (same)" if (same_project and same_folder) else "" if same_folder else ENDC()))
            different_inputs = []
            for item in cloned_hash["runInput"]:
                if cloned_hash["runInput"][item] != desc["runInput"][item]:
                    different_inputs.append(item)
            print_nofill_field(" input", get_io_field(cloned_hash["runInput"], highlight_fields=different_inputs))

            cloned_sys_reqs = cloned_hash.get("systemRequirements")
            if isinstance(cloned_sys_reqs, dict):
                if cloned_sys_reqs == desc.get('systemRequirements'):
                    print_nofill_field(" sys reqs", json.dumps(cloned_sys_reqs) + ' (same)')
                else:
                    print_nofill_field(" sys reqs", YELLOW() + json.dumps(cloned_sys_reqs) + ENDC())
    if not desc.get('isFree') and desc.get('totalPrice') is not None:
        print_field('Total Price', "$%.2f" % desc['totalPrice'])
    if desc.get('invoiceMetadata'):
        print_json_field("Invoice Metadata", desc['invoiceMetadata'])
    if desc.get('sshHostKey'):
        print_nofill_field("SSH Host Key", printable_ssh_host_key(desc['sshHostKey']))

    for field in desc:
        if field not in recognized_fields:
            print_json_field(field, desc[field])

def print_user_desc(desc):
    print_field("ID", desc["id"])
    print_field("Name", desc["first"] + " " + ((desc["middle"] + " ") if desc["middle"] != '' else '') + desc["last"])
    if "email" in desc:
        print_field("Email", desc["email"])

    bill_to_label = "Default bill to"
    if "billTo" in desc:
        print_field(bill_to_label, desc["billTo"])

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
        print_project_desc(desc, verbose=verbose)
    elif desc['class'] == 'app':
        print_app_desc(desc, verbose=verbose)
    elif desc['class'] in ['job', 'analysis']:
        print_execution_desc(desc)
    elif desc['class'] == 'user':
        print_user_desc(desc)
    elif desc['class'] in ['org', 'team']:
        print_generic_desc(desc)
    else:
        print_data_obj_desc(desc, verbose=verbose)

def get_ls_desc(desc, print_id=False):
    addendum = ' : ' + desc['id'] if print_id is True else ''
    if desc['class'] in ['applet', 'workflow']:
        return BOLD() + GREEN() + desc['name'] + ENDC() + addendum
    else:
        return desc['name'] + addendum


def print_ls_desc(desc, **kwargs):
    print(get_ls_desc(desc, **kwargs))


def get_ls_l_header():
    return (BOLD() +
            'State' + DELIMITER('   ') +
            'Last modified' + DELIMITER('       ') +
            'Size' + DELIMITER('      ') +
            'Name' + DELIMITER(' (') +
            'ID' + DELIMITER(')') +
            ENDC())


def print_ls_l_header():
    print(get_ls_l_header())


def get_ls_l_desc_fields():
    return {
        'id': True,
        'class': True,
        'folder': True,
        'length': True,
        'modified': True,
        'name': True,
        'project': True,
        'size': True,
        'state': True
    }


def get_ls_l_desc(desc, include_folder=False, include_project=False):
    """
    desc must have at least all the fields given by get_ls_l_desc_fields.
    """
    # If you make this method consume an additional field, you must add it to
    # get_ls_l_desc_fields above.
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

    if desc['class'] in ['applet', 'workflow']:
        name_str = BOLD() + GREEN() + name_str + ENDC()

    size_str = ''
    if 'size' in desc and desc['class'] == 'file':
        size_str = get_size_str(desc['size'])
    elif 'length' in desc:
        size_str = str(desc['length']) + ' rows'
    size_padding = ' ' * max(0, 9 - len(size_str))

    return (state_str +
            DELIMITER(' '*(8 - state_len)) + render_short_timestamp(desc['modified']) +
            DELIMITER(' ') + size_str +
            DELIMITER(size_padding + ' ') + name_str +
            DELIMITER(' (') + ((desc['project'] + DELIMITER(':')) if include_project else '') + desc['id'] +
            DELIMITER(')'))


def print_ls_l_desc(desc, **kwargs):
    print(get_ls_l_desc(desc, **kwargs))


def get_find_executions_string(desc, has_children, single_result=False, show_outputs=True,
                               is_cached_result=False):
    '''
    :param desc: hash of execution's describe output
    :param has_children: whether the execution has children to be printed
    :param single_result: whether the execution is displayed as a single result or as part of an execution tree
    :param is_cached_result: whether the execution should be formatted as a cached result
    '''
    is_not_subjob = desc['parentJob'] is None or desc['class'] == 'analysis' or single_result
    result = ("* " if is_not_subjob and get_delimiter() is None else "")
    canonical_execution_name = desc['executableName']
    if desc['class'] == 'job':
        canonical_execution_name += ":" + desc['function']
    execution_name = desc.get('name', '<no name>')

    # Format the name of the execution
    if is_cached_result:
        result += BOLD() + "[" + ENDC()
    result += BOLD() + BLUE()
    if desc['class'] == 'analysis':
        result += UNDERLINE()
    result += execution_name + ENDC()

    if execution_name != canonical_execution_name and execution_name+":main" != canonical_execution_name:
        result += ' (' + canonical_execution_name + ')'

    if is_cached_result:
        result += BOLD() + "]" + ENDC()

    # Format state
    result += DELIMITER(' (') + JOB_STATES(desc['state']) + DELIMITER(') ') + desc['id']

    # Add unicode pipe to child if necessary
    result += DELIMITER('\n' + (u'│ ' if is_not_subjob and has_children else ("  " if is_not_subjob else "")))
    result += desc['launchedBy'][5:] + DELIMITER(' ')
    result += render_short_timestamp(desc['created'])

    cached_and_runtime_strs = []

    if is_cached_result:
        cached_and_runtime_strs.append(YELLOW() + "cached" + ENDC())

    if desc['class'] == 'job':
        # Only print runtime if it ever started running
        if desc.get('startedRunning'):
            if desc['state'] in ['done', 'failed', 'terminated', 'waiting_on_output']:
                runtime = datetime.timedelta(seconds=int(desc['stoppedRunning']-desc['startedRunning'])//1000)
                cached_and_runtime_strs.append("runtime " + str(runtime))
            elif desc['state'] == 'running':
                seconds_running = max(int(time.time()-desc['startedRunning']//1000), 0)
                msg = "running for {rt}".format(rt=datetime.timedelta(seconds=seconds_running))
                cached_and_runtime_strs.append(msg)

    if cached_and_runtime_strs:
        result += " (" + ", ".join(cached_and_runtime_strs) + ")"

    if show_outputs:
        prefix = DELIMITER('\n' + (u'│ ' if is_not_subjob and has_children else ("  " if is_not_subjob else "")))
        if desc.get("output") != None:
            result += job_output_to_str(desc['output'], prefix=prefix)
        elif desc['state'] == 'failed' and 'failureReason' in desc:
            result += prefix + BOLD() + desc['failureReason'] + ENDC() + ": " + fill(desc.get('failureMessage', ''),
                                                                                     subsequent_indent=prefix.lstrip('\n'))

    return result
