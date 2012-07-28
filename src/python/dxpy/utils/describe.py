'''
This submodule contains helper functions for parsing and printing the
contents of describe hashes for various DNAnexus entities (projects,
containers, dataobjects, apps, and jobs).
'''

import datetime, json, textwrap

CYAN = '\033[36m'
BLUE = '\033[34m'
YELLOW = '\033[33m'
GREEN = '\033[32m'
RED = '\033[31m'
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
        if isinstance(parameter["type"], dict):
            desc += "type satisfying " + json.dumps(parameter["type"])
        else:
            desc += "type " + parameter["type"]
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
    return '\n\t\t'.join(map(get_io_desc, spec))

def print_project_desc(desc):
    recognized_fields = ['id', 'class', 'name', 'description', 'owner', 'protected', 'restricted', 'created', 'modified', 'dataUsage', 'tags', 'level', 'folders', 'objects', 'permissions', 'appWorkspaces', 'billTo']

    print "ID\t\t" + desc["id"]
    print "Class\t\t" + desc["class"]
    if "name" in desc:
        print "Name\t\t" + desc["name"]
    if 'description' in desc:
        print textwrap.fill("Description\t" + desc["description"], subsequent_indent='\t\t', width=64)
    if 'owner' in desc:
        print "Owner\t\t" + desc["owner"]
    elif 'billTo' in desc:
        print "Billed to\t" + desc['billTo']
    if 'protected' in desc:
        print "Protected\t" + json.dumps(desc["protected"])
    if 'restricted' in desc:
        print "Restricted\t" + json.dumps(desc["restricted"])
    print "Created\t\t" + datetime.datetime.fromtimestamp(desc['created']/1000).ctime()
    print "Last modified\t" + datetime.datetime.fromtimestamp(desc['modified']/1000).ctime()
    print "Data usage\t" + str(desc["dataUsage"])
    if 'tags' in desc:
        print "Tags\t\t" + json.dumps(desc["tags"])
    if "level" in desc:
        print "Access level\t" + desc["level"]
    if "folders" in desc:
        print "Folders\t\t" + ', '.join(desc["folders"])
    if "objects" in desc:
        print "# Files\t\t" + str(desc["objects"])
    if "permissions" in desc:
        print "Permissions\t" + json.dumps(desc["permissions"])
    if "appWorkspaces" in desc:
        print "App workspaces\t" + json.dumps(desc["appWorkspaces"])

    for field in desc:
        if field not in recognized_fields:
            print field + '\t\t' + json.dumps(desc[field])

def print_app_desc(desc):
    recognized_fields = ['id', 'class', 'owner', 'name', 'version', 'aliases', 'createdBy', 'created', 'modified', 'program', 'deleted', 'published', 'title', 'subtitle', 'description', 'categories', 'access', 'dxapi', 'inputSpec', 'outputSpec', 'runSpec', 'globalWorkspace', 'installed', 'openSource', 'inputs', 'outputs', 'run', 'summary']

    print "ID\t\t" + desc["id"]
    print "Class\t\t" + desc["class"]
    print "Owner\t\t" + desc["owner"]
    print "Name\t\t" + desc["name"]
    print "Version\t\t" + desc["version"]
    print "Aliases\t\t" + ', '.join(desc["aliases"])
    print "Created by\t" + desc["createdBy"]
    print "Created\t\t" + datetime.datetime.fromtimestamp(desc['created']/1000).ctime()
    print "Last modified\t" + datetime.datetime.fromtimestamp(desc['modified']/1000).ctime()
    if "program" in desc:
        print "Created from\t" + desc["program"]
    if desc['installed'] == True:
        print 'Installed\ttrue'
    else:
        print 'Installed\tfalse'
    if desc['openSource'] == True:
        print 'Open source\ttrue'
    else:
        print 'Open source\tfalse'
    if desc["deleted"]:
        print "Deleted\t\ttrue"
    else:
        print "Deleted\t\tfalse"

        if 'published' not in desc or desc["published"] < 0:
            "Published\tN/A"
        else:
            "Published\t" + datetime.datetime.fromtimestamp(desc['published']/1000).ctime()

    if not desc["deleted"]:
        if "title" in desc:
            print "Title\t\t" + desc["title"]
        if "subtitle" in desc:
            print "Subtitle\t\t" + desc["subtitle"]
        if 'summary' in desc:
            print textwrap.fill("Summary\t\t" + desc['summary'], subsequent_indent='\t\t', width=64)
        if "description" in desc:
            print textwrap.fill("Description\t" + desc["description"], subsequent_indent='\t\t', width=64)
        print "Categories\t" + ', '.join(desc["categories"])
        print "Access\t\t" + json.dumps(desc["access"])
        print "API version\t" + desc["dxapi"]
        if 'inputSpec' in desc:
            print "Input Spec\t" + get_io_spec(desc["inputSpec"])
            print "Output Spec\t" + get_io_spec(desc["outputSpec"])
            print "Interpreter\t" + desc["runSpec"]["interpreter"]
            if "resources" in desc["runSpec"]:
                print "Resources\t" + json.dumps(desc["runSpec"]["resources"])
            if "bundledDepends" in desc["runSpec"]:
                print "bundledDepends\t" + textwrap.fill(json.dumps(desc["runSpec"]["bundledDepends"]), width=64, subsequent_indent='\t\t')
            if "execDepends" in desc["runSpec"]:
                print "execDepends\t" + json.dumps(desc["runSpec"]["execDepends"])
        elif 'inputs' in desc:
            print "Input Spec\t" + get_io_spec(desc['inputs'])
            print "Output Spec\t" + get_io_spec(desc['outputs'])
            print "Interpreter\t" + desc["run"]["interpreter"]
            if "resources" in desc["run"]:
                print "Resources\t" + json.dumps(desc["runSpec"]["resources"])
            if "bundledDepends" in desc["run"]:
                print "bundledDepends\t" + textwrap.fill(json.dumps(desc["run"]["bundledDepends"]), width=64, subsequent_indent='\t\t')
            if "execDepends" in desc["run"]:
                print "execDepends\t" + json.dumps(desc["run"]["execDepends"])
        print "GlobalWorkspace\t" + desc["globalWorkspace"]

    for field in desc:
        if field not in recognized_fields:
            print field + '\t\t' + json.dumps(desc[field])

def print_data_obj_desc(desc):
    recognized_fields = ['id', 'class', 'project', 'folder', 'name', 'properties', 'tags', 'types', 'hidden', 'details', 'links', 'created', 'modified', 'state', 'title', 'subtitle', 'description', 'inputSpec', 'outputSpec', 'runSpec', 'summary', 'dxapi', 'access', 'createdBy', 'summary']
    print "ID\t\t" + desc["id"]
    print "Class\t\t" + desc["class"]
    if 'project' in desc:
        print "Project\t\t" + desc["project"]
    if 'folder' in desc:
        print "Folder\t\t" + desc["folder"]
    print "Name\t\t" + desc["name"]
    if 'state' in desc:
        if desc['state'] in DATA_STATES:
            print "State\t\t" + DATA_STATES[desc['state']]
        else:
            print "State\t\t" + desc["state"]
    if 'hidden' in desc:
        print "Hidden\t\t" + json.dumps(desc["hidden"])
    if 'types' in desc:
        print "Types\t\t" + json.dumps(desc["types"])
    if 'properties' in desc:
        print "Properties\t" + json.dumps(desc["properties"])
    if 'tags' in desc:
        print "Tags\t\t" + json.dumps(desc["tags"])
    if 'details' in desc:
        print "Details\t\t" + json.dumps(desc["details"])
    if 'links' in desc:
        print "Outgoing links\t" + json.dumps(desc["links"])
    print "Created\t\t" + datetime.datetime.fromtimestamp(desc['created']/1000).ctime()
    if 'createdBy' in desc:
        print "Created by\t" + desc['createdBy']['user']
    print "Last modified\t" + datetime.datetime.fromtimestamp(desc['modified']/1000).ctime()
    if "title" in desc:
        print "Title\t\t" + desc["title"]
    if "subtitle" in desc:
        print "Subtitle\t\t" + desc["subtitle"]
    if 'summary' in desc:
        print textwrap.fill("Summary\t\t" + desc['summary'], subsequent_indent='\t\t', width=64)
    if "description" in desc:
        print textwrap.fill("Description\t" + desc["description"], subsequent_indent='\t\t', width=64)
    if 'summary' in desc:
        print textwrap.fill('Summary\t\t' + desc['summary'], subsequent_indent='\t\t', width=64)
    if 'access' in desc:
        print "Access\t\t" + json.dumps(desc["access"])
    if 'dxapi' in desc:
        print "API version\t" + desc["dxapi"]
    if "inputSpec" in desc:
        print "Input Spec\t" + get_io_spec(desc['inputSpec'])
    if "outputSpec" in desc:
        print "Output Spec\t" + get_io_spec(desc['outputSpec'])
    if 'runSpec' in desc:
        print "Interpreter\t" + desc["runSpec"]["interpreter"]
        if "resources" in desc['runSpec']:
            print "Resources\t" + json.dumps(desc["runSpec"]["resources"])
        if "bundledDepends" in desc['runSpec']:
            print "bundledDepends\t" + textwrap.fill(json.dumps(desc["runSpec"]["bundledDepends"]), width=64, subsequent_indent='\t\t')
        if "execDepends" in desc['runSpec']:
            print "execDepends\t" + json.dumps(desc["runSpec"]["execDepends"])

    for field in desc:
        if field in recognized_fields:
            continue
        else:
            if field == "media":
                print "Media type\t" + desc['media']
            elif field == "size":
                if desc["class"] == "file" or desc["class"] == "gtable":
                    print "Size (bytes)\t" + str(desc['size'])
                else:
                    print "Size\t\t" + str(desc['size'])
            elif field == "length":
                if desc["class"] == "gtable":
                    print "Size (rows)\t" + str(desc['length'])
                else:
                    print "Size\t\t" + str(desc['length'])
            elif field == "columns":
                coldescs = ""
                for column in desc["columns"]:
                    coldescs += "\t\t" + column["name"] + " (" + column["type"] + ")\n"
                print "Columns" + coldescs[:-1]
            else: # Unhandled prettifying
                print field + "\t\t" + json.dumps(desc[field])

def print_job_desc(desc):
    recognized_fields = ['id', 'class', 'project', 'workspace', 'program', 'app', 'state', 'parentJob', 'originJob', 'function', 'originalInput', 'input', 'output', 'folder', 'launchedBy', 'created', 'modified', 'failureReason', 'failureMessage', 'stdout', 'stderr', 'waitingOnChildren', 'projectWorkspace', 'globalWorkspace']

    print "ID\t\t" + desc["id"]
    print "Class\t\t" + desc["class"]
    print "Project context\t" + desc["project"]
    print "Workspace\t" + desc["workspace"]
    if 'projectWorkspace' in desc:
        print 'Cache workspace\t' + desc['projectWorkspace']
        print 'GlobalWorkspace\t' + desc['globalWorkspace']
    if "program" in desc:
        print "Program\t\t" + desc["program"]
    elif "app" in desc:
        print "App\t\t" + desc["app"]
    if desc['state'] in JOB_STATES:
        print "State\t\t" + JOB_STATES[desc["state"]]
    else:
        print "State\t\t" + desc['state']
    if desc["parentJob"] is None:
        print "Parent job\tNone"
    else:
        print "Parent job\t" + json.dumps(desc["parentJob"])
    print "Origin job\t" + desc["originJob"]
    print "Function\t" + desc["function"]
    if "originalInput" in desc:
        print "Original Input\t" + json.dumps(desc["originalInput"])
        print "Input\t\t" + json.dumps(desc["input"])
        print "Output\t\t" + json.dumps(desc["output"])
    if 'folder' in desc:
        print 'Output folder\t' + desc['folder']
    print "Launched by\t" + desc["launchedBy"]
    print "Created\t\t" + datetime.datetime.fromtimestamp(desc['created']/1000).ctime()
    print "Last modified\t" + datetime.datetime.fromtimestamp(desc['modified']/1000).ctime()
    if 'waitingOnChildren' in desc:
        if len(desc['waitingOnChildren']) == 0:
            print 'Pending subjobs\tNone'
        else:
            print 'Pending subjobs\t' + ', '.join(desc['waitingOnChildren'])
    if "failureReason" in desc:
        print "Failure reason\t" + desc["failureReason"]
    if "failureMessage" in desc:
        print textwrap.fill("Failure message\t" + desc["failureMessage"], subsequent_indent='\t\t', width=64)
    if "stdout" in desc:
        print "File of stdout\t" + str(desc['stdout'])
    if 'stderr' in desc:
        print 'File of stderr\t' + str(desc['stderr'])
    for field in desc:
        if field not in recognized_fields:
            print field + '\t\t' + json.dumps(desc[field])

def print_user_desc(desc):
    print "ID\t\t" + desc["id"]
    print "Name\t\t" + desc["first"] + " " + ((desc["middle"] + " ") if desc["middle"] != '' else '') + desc["last"]
    if "email" in desc:
        print "Email\t\t" + desc["email"]
    if "appsInstalled" in desc:
        if len(desc["appsInstalled"]) == 0:
            print "Apps installed\tNone"
        else:
            print "Apps installed\t" + textwrap.fill(', '.join(desc["appsInstalled"].keys()), width=64, subsequent_indent='\t\t')

def print_generic_desc(desc):
    for field in desc:
        print field + ('\t\t' if len(field) < 8 else '\t') + json.dumps(desc[field])

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

def get_ls_l_desc(desc):
    if desc['state'] != 'closed':
        state_str = YELLOW + desc['state'] + ENDC
    else:
        state_str = GREEN + desc['state'] + ENDC

    if desc['class'] == 'program':
        name_str = BOLD + GREEN + desc['name'] + ENDC
    else:
        name_str = desc['name']

    return state_str + ' '*(8-len(desc['state'])) + str(datetime.datetime.fromtimestamp(desc['modified']/1000)) + '  ' + name_str + ' (' + desc['id'] + ')'

def print_ls_l_desc(desc):
    print get_ls_l_desc(desc)
