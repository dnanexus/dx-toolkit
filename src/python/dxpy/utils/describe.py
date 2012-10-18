# coding: utf-8
'''
This submodule contains helper functions for parsing and printing the
contents of describe hashes for various DNAnexus entities (projects,
containers, dataobjects, apps, and jobs).
'''

import datetime, json, textwrap, math, sys

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
    return ('%d' if level == 0 else '%.2f') % (float(size) / 10**(level*3)) + ' ' + SIZE_LEVEL[level]

def parse_typespec(thing):
    if isinstance(thing, basestring):
        return thing
    elif '$and' in thing:
        return '(' + ' AND '.join(map(parse_typespec, thing['$and'])) + ')'
    elif '$or' in thing:
        return '(' + ' OR '.join(map(parse_typespec, thing['$or'])) + ')'
    else:
        return 'Type spec could not be parsed'

def get_io_desc(parameter, include_class=True, show_opt=True):
    desc = ""
    is_optional = False;
    if show_opt:
        if "default" in parameter or ("optional" in parameter and parameter["optional"]):
            is_optional = True
            desc += "["
    desc += parameter["name"]
    include_parens = include_class or 'type' in parameter or 'default' in parameter
    if include_parens:
        desc += " ("
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
        desc += json.dumps(parameter['default'])
    if include_parens:
        desc += ")"
    if show_opt and is_optional:
        desc += "]"
    return desc

def get_io_spec(spec):
    if len(spec) == 0:
        return '-'
    if get_delimiter() is not None:
        return ('\n' + get_delimiter()).join([get_io_desc(param) for param in spec])
    else:
        return ('\n' + ' '*16).join(map(lambda param:
                                            fill(get_io_desc(param),
                                                 subsequent_indent=' '*18,
                                                 width_adjustment=-18),
                                        spec))

def get_io_field(io_hash):
    if io_hash is None:
        return '-'
    if len(io_hash) == 0:
        return '-'
    if get_delimiter() is not None:
        return ('\n' + get_delimiter()).join([(key + '=' + json.dumps(value)) for key, value in io_hash.items()])
    else:
        return ('\n').join([fill(key + '=' + json.dumps(value),
                                 initial_indent=' '*16,
                                 subsequent_indent=' '*20) for key, value in io_hash.items()])[16:]

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
    recognized_fields = ['id', 'class', 'name', 'description', 'protected', 'restricted', 'created', 'modified', 'dataUsage', 'tags', 'level', 'folders', 'objects', 'permissions', 'properties', 'appCaches', 'billTo']

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

def print_app_desc(desc):
    recognized_fields = ['id', 'class', 'name', 'version', 'aliases', 'createdBy', 'created', 'modified', 'program', 'deleted', 'published', 'title', 'subtitle', 'description', 'categories', 'access', 'dxapi', 'inputSpec', 'outputSpec', 'runSpec', 'globalWorkspace', 'resources', 'billTo', 'installed', 'openSource', 'summary', 'applet', 'installs', 'billing', 'details']
    # NOTE: Hiding "billing" for now

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
    if "program" in desc:
        print_field("Created from", desc["program"])
    elif "applet" in desc:
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
        if "description" in desc and desc['description'] is not None:
            print_field("Description", desc["description"])
        print_list_field("Categories", desc["categories"])
        if 'details' in desc:
            print_json_field("Details", desc["details"])
        print_json_field("Access", desc["access"])
        print_field("API version", desc["dxapi"])
        if 'inputSpec' in desc:
            print_nofill_field("Input Spec", get_io_spec(desc["inputSpec"]))
            print_nofill_field("Output Spec", get_io_spec(desc["outputSpec"]))
            print_field("Interpreter", desc["runSpec"]["interpreter"])
            if "resources" in desc["runSpec"]:
                print_json_field("Resources", desc["runSpec"]["resources"])
            if "bundledDepends" in desc["runSpec"]:
                print_json_field("bundledDepends", desc["runSpec"]["bundledDepends"])
            if "execDepends" in desc["runSpec"]:
                print_json_field("execDepends", desc["runSpec"]["execDepends"])
            if "systemRequirements" in desc['runSpec']:
                print_json_field('Sys Requirements', desc['runSpec']['systemRequirements'])
        if 'resources' in desc:
            print_field("Resources", desc['resources'])
        elif 'globalWorkspace' in desc:
            print_field("GlobalWorkspace", desc["globalWorkspace"])
    if 'installs' in desc:
        print_field('# Installs', str(desc['installs']))

    for field in desc:
        if field not in recognized_fields:
            print_json_field(field, desc[field])

def get_col_str(col_desc):
    return col_desc['name'] + DELIMITER(" (") + col_desc['type'] + DELIMITER(")")

def print_data_obj_desc(desc):
    recognized_fields = ['id', 'class', 'project', 'folder', 'name', 'properties', 'tags', 'types', 'hidden', 'details', 'links', 'created', 'modified', 'state', 'title', 'subtitle', 'description', 'inputSpec', 'outputSpec', 'runSpec', 'summary', 'dxapi', 'access', 'createdBy', 'summary']
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
    if 'details' in desc:
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
    if "description" in desc:
        print_field("Description", desc["description"])
    if 'summary' in desc:
        print_field('Summary', desc['summary'])
    if 'access' in desc:
        print_json_field("Access", desc["access"])
    if 'dxapi' in desc:
        print_field("API version", desc["dxapi"])
    if "inputSpec" in desc:
        print_nofill_field("Input Spec", get_io_spec(desc['inputSpec']))
    if "outputSpec" in desc:
        print_nofill_field("Output Spec", get_io_spec(desc['outputSpec']))
    if 'runSpec' in desc:
        print_field("Interpreter", desc["runSpec"]["interpreter"])
        if "resources" in desc['runSpec']:
            print_json_field("Resources", desc["runSpec"]["resources"])
        if "bundledDepends" in desc['runSpec']:
            print_json_field("bundledDepends", desc["runSpec"]["bundledDepends"])
        if "execDepends" in desc['runSpec']:
            print_json_field("execDepends", desc["runSpec"]["execDepends"])
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
                    print_field("Size", get_size_str(desc['size']))
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
    recognized_fields = ['id', 'class', 'project', 'workspace', 'program', 'app', 'state', 'parentJob', 'originJob', 'function', 'runInput', 'originalInput', 'input', 'output', 'folder', 'launchedBy', 'created', 'modified', 'failureReason', 'failureMessage', 'stdout', 'stderr', 'waitingOnChildren', 'dependencies', 'projectWorkspace', 'globalWorkspace', 'resources', 'projectCache', 'applet', 'name', 'instanceType', 'systemRequirements', 'executableName', 'failureFrom']

    print_field("ID", desc["id"])
    print_field("Class", desc["class"])
    if "name" in desc and desc['name'] is not None:
        print_field("Job name", desc['name'])
    if "executableName" in desc and desc['executableName'] is not None:
        print_field("Executable name", desc['executableName'])
    print_field("Project context", desc["project"])
    if 'workspace' in desc:
        print_field("Workspace", desc["workspace"])
    if 'projectWorkspace' in desc:
        print_field('Cache workspace', desc['projectWorkspace'])
        print_field('GlobalWorkspace', desc['globalWorkspace'])
    elif 'projectCache' in desc:
        print_field('Cache workspace', desc['projectCache'])
        print_field('Resources', desc['resources'])
    if "program" in desc:
        print_field("Program", desc["program"])
    elif "app" in desc:
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
        print_nofill_field("Run Input", get_io_field(desc["runInput"]))
    if "originalInput" in desc:
        print_nofill_field("Original Input", get_io_field(desc["originalInput"]))
        print_nofill_field("Input", get_io_field(desc["input"]))
        print_nofill_field("Output", get_io_field(desc["output"]))
    if 'folder' in desc:
        print_field('Output folder', desc['folder'])
    print_field("Launched by", desc["launchedBy"][5:])
    print_field("Created", datetime.datetime.fromtimestamp(desc['created']/1000).ctime())
    print_field("Last modified", datetime.datetime.fromtimestamp(desc['modified']/1000).ctime())
    if 'waitingOnChildren' in desc:
        print_list_field('Pending subjobs', desc['waitingOnChildren'])
    if 'dependencies' in desc:
        print_list_field('Dependencies', desc['dependencies'])
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

def print_desc(desc):
    '''
    :param desc: The describe hash of a DNAnexus entity
    :type desc: dict

    Depending on the class of the entity, this method will print a
    formatted and human-readable string containing the data in *desc*.
    '''
    if desc['class'] in ['project', 'workspace', 'container']:
        print_project_desc(desc)
    elif desc['class'] == 'app':
        print_app_desc(desc)
    elif desc['class'] == 'job':
        print_job_desc(desc)
    elif desc['class'] == 'user':
        print_user_desc(desc)
    elif desc['class'] in ['org', 'team']:
        print_generic_desc(desc)
    else:
        print_data_obj_desc(desc)

def get_ls_desc(desc, print_id=False):
    addendum = ' : ' + desc['id'] if print_id is True else ''
    if desc['class'] == 'applet':
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

    size_str = ''
    if 'size' in desc and desc['class'] == 'file':
        size_str = get_size_str(desc['size'])
    elif 'length' in desc:
        size_str = str(desc['length']) + ' rows'
    size_padding = ' '*(max(0, 8 - len(size_str)))

    if desc['class'] == 'program':
        name_str = BOLD() + GREEN() + name_str + ENDC()

    return state_str + DELIMITER(' '*(8 - state_len)) + str(datetime.datetime.fromtimestamp(desc['modified']/1000)) + DELIMITER(' ') + size_str + DELIMITER(size_padding + ' ') + name_str + DELIMITER(' (') + ((desc['project'] + DELIMITER(':')) if include_project else '') + desc['id'] + DELIMITER(')')

def print_ls_l_desc(desc, **kwargs):
    print get_ls_l_desc(desc, **kwargs)

def get_find_jobs_string(jobdesc, has_children):
    is_origin_job = jobdesc['parentJob'] is None
    string = ("* " if is_origin_job and get_delimiter() is None else "")
    string += (BOLD() + BLUE() + (jobdesc['name'] if 'name' in jobdesc else "<no name>") + ENDC()) + DELIMITER(' (') + JOB_STATES(jobdesc['state']) + DELIMITER(') ') + jobdesc['id'] 
    string += DELIMITER('\n' + (u'│ ' if is_origin_job and has_children else ("  " if is_origin_job else "")))
    string += jobdesc['launchedBy'][5:] + DELIMITER(' ')
    string += str(datetime.datetime.fromtimestamp(jobdesc['created']/1000))
    if jobdesc['state'] == 'done':
        string += " .. {enddate} ({duration})".format(
            enddate=str(datetime.datetime.fromtimestamp(jobdesc['modified']/1000)),
            duration=str(datetime.timedelta(milliseconds=jobdesc['modified']-jobdesc['created'])))

    return string
