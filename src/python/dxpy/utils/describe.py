'''
This submodule contains helper functions for parsing and printing the
contents of describe hashes for various DNAnexus entities (projects,
containers, dataobjects, apps, and jobs).
'''

import datetime, json, textwrap, math

CYAN = '\033[36m'
BLUE = '\033[34m'
YELLOW = '\033[33m'
GREEN = '\033[32m'
RED = '\033[31m'
WHITE = '\033[37m'
UNDERLINE = '\033[4m'
BOLD = '\033[1m'
ENDC = '\033[0m'

JOB_STATES = {'failed': BOLD + RED + 'failed' + ENDC,
              'done': BOLD + GREEN + 'done' + ENDC,
              'running': GREEN + 'running' + ENDC,
              'idle': YELLOW + 'idle' + ENDC,
              'runnable': YELLOW + 'runnable' + ENDC,
              'waiting_on_inputs': YELLOW + 'waiting_on_inputs' + ENDC,
              'waiting_on_outputs': YELLOW + 'waiting_on_outputs' + ENDC
              }

DATA_STATES = {'open': YELLOW + 'open' + ENDC,
               'closing': YELLOW + 'open' + ENDC,
               'closed': GREEN + 'closed' + ENDC
               }

SIZE_LEVEL = ['bytes', 'KB', 'MB', 'GB', 'TB']

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
    return ('\n' + ' '*16).join(map(lambda param:
                                        textwrap.fill(get_io_desc(param),
                                                      subsequent_indent=' '*16,
                                                      width=64),
                                    spec))

def print_field(label, value):
    print label + " " * (16-len(label)) + textwrap.fill(value, subsequent_indent=' '*16, width=64)

def print_list_field(label, values):
    print_field(label, ('-' if len(values) == 0 else ', '.join(values)))

def print_json_field(label, json_value):
    print_field(label, json.dumps(json_value))

def print_project_desc(desc):
    recognized_fields = ['id', 'class', 'name', 'description', 'owner', 'protected', 'restricted', 'created', 'modified', 'dataUsage', 'tags', 'level', 'folders', 'objects', 'permissions', 'appWorkspaces', 'appCaches', 'billTo']

    print_field("ID", desc["id"])
    print_field("Class", desc["class"])
    if "name" in desc:
        print_field("Name", desc["name"])
    if 'description' in desc:
        print_field("Description", desc["description"])
    if 'owner' in desc:
        print_field("Owner", desc["owner"])
    elif 'billTo' in desc:
        print_field("Billed to", desc['billTo'])
    if 'protected' in desc:
        print "Protected\t" + json.dumps(desc["protected"])
    if 'restricted' in desc:
        print "Restricted\t" + json.dumps(desc["restricted"])
    print_field("Created", datetime.datetime.fromtimestamp(desc['created']/1000).ctime())
    print_field("Last modified", datetime.datetime.fromtimestamp(desc['modified']/1000).ctime())
    print_field("Data usage", ('%.2f' % desc["dataUsage"]) + ' GB')
    if 'tags' in desc:
        print_list_field("Tags", join(desc["tags"]))
    if "level" in desc:
        print_field("Access level", desc["level"])
    if "folders" in desc:
        print_list_field("Folders", desc["folders"])
    if "objects" in desc:
        print_field("# Files", str(desc["objects"]))
    if "permissions" in desc:
        print_field("Permissions", json.dumps(desc["permissions"]))
    if "appCaches" in desc:
        print_field("App caches", json.dumps(desc["appCaches"]))
    elif "appWorkspaces" in desc:
        print_field("App caches", json.dumps(desc["appWorkspaces"]))

    for field in desc:
        if field not in recognized_fields:
            print_json_field(field, desc[field])

def print_app_desc(desc):
    recognized_fields = ['id', 'class', 'owner', 'name', 'version', 'aliases', 'createdBy', 'created', 'modified', 'program', 'deleted', 'published', 'title', 'subtitle', 'description', 'categories', 'access', 'dxapi', 'inputSpec', 'outputSpec', 'runSpec', 'globalWorkspace', 'resources', 'billTo', 'installed', 'openSource', 'inputs', 'outputs', 'run', 'summary', 'applet']

    print_field("ID", desc["id"])
    print_field("Class", desc["class"])
    if 'owner' in desc:
        print_field("Owner", desc["owner"])
    elif 'billTo' in desc:
        print_field("Billed to", desc['billTo'])
    print_field("Name", desc["name"])
    print_field("Version", desc["version"])
    print_list_field("Aliases", desc["aliases"])
    print_field("Created by", desc["createdBy"])
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
        print_json_field("Access", desc["access"])
        print_field("API version", desc["dxapi"])
        if 'inputSpec' in desc:
            print "Input Spec      " + get_io_spec(desc["inputSpec"])
            print "Output Spec     " + get_io_spec(desc["outputSpec"])
            print_field("Interpreter", desc["runSpec"]["interpreter"])
            if "resources" in desc["runSpec"]:
                print_json_field("Resources", desc["runSpec"]["resources"])
            if "bundledDepends" in desc["runSpec"]:
                print_json_field("bundledDepends", desc["runSpec"]["bundledDepends"])
            if "execDepends" in desc["runSpec"]:
                print_json_field("execDepends", desc["runSpec"]["execDepends"])
        elif 'inputs' in desc:
            print "Input Spec      " + get_io_spec(desc['inputs'])
            print "Output Spec     " + get_io_spec(desc['outputs'])
            print_field("Interpreter", desc["run"]["interpreter"])
            if "resources" in desc["run"]:
                print_json_field("Resources", desc["run"]["resources"])
            if "bundledDepends" in desc["run"]:
                print_json_field("bundledDepends", desc["run"]["bundledDepends"])
            if "execDepends" in desc["run"]:
                print_json_field("execDepends", desc["run"]["execDepends"])
        if 'resources' in desc:
            print_field("Resources", desc['resources'])
        elif 'globalWorkspace' in desc:
            print_field("GlobalWorkspace", desc["globalWorkspace"])

    for field in desc:
        if field not in recognized_fields:
            print_json_field(field, desc[field])

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
        if desc['state'] in DATA_STATES:
            print_field("State", DATA_STATES[desc['state']])
        else:
            print_field("State", desc["state"])
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
        print_field("Created by", desc['createdBy']['user'])
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
        print "Input Spec\t" + get_io_spec(desc['inputSpec'])
    if "outputSpec" in desc:
        print "Output Spec\t" + get_io_spec(desc['outputSpec'])
    if 'runSpec' in desc:
        print_field("Interpreter", desc["runSpec"]["interpreter"])
        if "resources" in desc['runSpec']:
            print_json_field("Resources", desc["runSpec"]["resources"])
        if "bundledDepends" in desc['runSpec']:
            print_json_field("bundledDepends", desc["runSpec"]["bundledDepends"])
        if "execDepends" in desc['runSpec']:
            print_json_field("execDepends", desc["runSpec"]["execDepends"])

    for field in desc:
        if field in recognized_fields:
            continue
        else:
            if field == "media":
                print_field("Media type", desc['media'])
            elif field == "size":
                if desc["class"] == "file" or desc["class"] == "gtable":
                    if desc['size'] == 0:
                        magnitude = 0
                        level = 0
                    else:
                        magnitude = math.floor(math.log(desc['size'], 10))
                        level = int(min(math.floor(magnitude / 3), 4))
                    print_field("Size", ('%d' if level == 0 else '%.2f') % (float(desc['size']) / 10**(level*3)) + ' ' + SIZE_LEVEL[level])
                else:
                    print_field("Size", str(desc['size']))
            elif field == "length":
                if desc["class"] == "gtable" or desc['class'] == 'table':
                    print_field("Size (rows)", str(desc['length']))
                else:
                    print_field("Length", str(desc['length']))
            elif field == "columns":
                if len(desc['columns']) > 0:
                    coldescs = ""
                    for column in desc["columns"]:
                        coldescs += column["name"] + " (" + column["type"] + ")\n" + " "*16
                    print "Columns" + " " *(16-len("Columns")) + coldescs[:-17]
                else:
                    print_list_field("Columns", desc['columns'])
            else: # Unhandled prettifying
                print_json_field(field, desc[field])

def print_job_desc(desc):
    recognized_fields = ['id', 'class', 'project', 'workspace', 'program', 'app', 'state', 'parentJob', 'originJob', 'function', 'originalInput', 'input', 'output', 'folder', 'launchedBy', 'created', 'modified', 'failureReason', 'failureMessage', 'stdout', 'stderr', 'waitingOnChildren', 'projectWorkspace', 'globalWorkspace', 'resources', 'projectCache', 'applet']

    print_field("ID", desc["id"])
    print_field("Class", desc["class"])
    print_field("Project context", desc["project"])
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
    if desc['state'] in JOB_STATES:
        print_field("State", JOB_STATES[desc["state"]])
    else:
        print_field("State", desc['state'])
    if desc["parentJob"] is None:
        print_field("Parent job", "-")
    else:
        print_json_field("Parent job", desc["parentJob"])
    print_field("Origin job", desc["originJob"])
    print_field("Function", desc["function"])
    if "originalInput" in desc:
        print_json_field("Original Input", desc["originalInput"])
        print_json_field("Input", desc["input"])
        print_json_field("Output", desc["output"])
    if 'folder' in desc:
        print_field('Output folder', desc['folder'])
    print_field("Launched by", desc["launchedBy"])
    print_field("Created", datetime.datetime.fromtimestamp(desc['created']/1000).ctime())
    print_field("Last modified", datetime.datetime.fromtimestamp(desc['modified']/1000).ctime())
    if 'waitingOnChildren' in desc:
        print_list_field('Pending subjobs', desc['waitingOnChildren'])
    if "failureReason" in desc:
        print_field("Failure reason", desc["failureReason"])
    if "failureMessage" in desc:
        print_field("Failure message", desc["failureMessage"])
    if "stdout" in desc:
        print_field("File of stdout", str(desc['stdout']))
    if 'stderr' in desc:
        print_field('File of stderr', str(desc['stderr']))
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

def get_ls_l_desc(desc, include_folder=False):
    if 'state' in desc:
        state_len = len(desc['state'])
        if desc['state'] != 'closed':
            state_str = YELLOW + desc['state'] + ENDC
        else:
            state_str = GREEN + desc['state'] + ENDC
    else:
        state_str = ''
        state_len = 0

    if include_folder:
        name_str = desc['folder'] + ('/' if desc['folder'] != '/' else '') + desc['name']
    else:
        name_str = desc['name']

    if desc['class'] == 'program':
        name_str = BOLD + GREEN + name_str + ENDC

    return state_str + ' '*(8 - state_len) + str(datetime.datetime.fromtimestamp(desc['modified']/1000)) + '  ' + name_str + ' (' + desc['id'] + ')'

def print_ls_l_desc(desc, include_folder=False):
    print get_ls_l_desc(desc, include_folder)
