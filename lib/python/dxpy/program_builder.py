'''
DNAnexus Program Builder Library

Contains methods used by the application builder to compile and deploy programs
and apps onto the platform.

You can specify the destination project in the following ways (with the earlier
ones taking precedence):

* Supply the 'project' argument to upload_resources or upload_program.
* Supply the 'project' attribute in your dxprogram.json.
* Set the DX_WORKSPACE_ID environment variable (when running in a job context).

'''

import os, sys, json, subprocess, tempfile, logging
import dxpy

class ProgramBuilderException(Exception):
    pass

def validate_program_spec(program_spec):
    if "name" not in program_spec:
        raise ProgramBuilderException("Program specification does not contain a name")

def validate_app_spec(app_spec):
    if "globalWorkspace" not in app_spec:
        raise ProgramBuilderException("App specification does not contain a globalWorkspace field")

def get_program_spec(src_dir):
    program_spec_file = os.path.join(src_dir, "dxprogram.json")
    with open(program_spec_file) as fh:
        program_spec = json.load(fh)

    validate_program_spec(program_spec)
    if 'project' not in program_spec:
        program_spec['project'] = dxpy.WORKSPACE_ID
    return program_spec

def get_app_spec(src_dir):
    app_spec_file = os.path.join(src_dir, "dxapp.json")
    with open(app_spec_file) as fh:
        app_spec = json.load(fh)

    validate_app_spec(app_spec)
    return app_spec

def build(src_dir):
    logging.debug("Building in " + src_dir)
    # TODO: use Gentoo or deb buildsystem
    config_script = os.path.join(src_dir, "configure")
    if os.path.isfile(config_script) and os.access(config_script, os.X_OK):
        subprocess.check_call([config_script])
    if os.path.isfile(os.path.join(src_dir, "Makefile")) \
        or os.path.isfile(os.path.join(src_dir, "makefile")) \
        or os.path.isfile(os.path.join(src_dir, "GNUmakefile")):
        subprocess.check_call(["make", "-C", src_dir, "-j8"])

def upload_resources(src_dir, project=None):
    program_spec = get_program_spec(src_dir)
    dest_project = project or program_spec['project']
    resources_dir = os.path.join(src_dir, "resources")
    if os.path.exists(resources_dir) and len(os.listdir(resources_dir)) > 0:
        logging.debug("Uploading in " + src_dir)

        with tempfile.NamedTemporaryFile(suffix=".tar.xz") as tar_fh:
            subprocess.check_call(['tar', '-C', resources_dir, '-cJf', tar_fh.name, '.'])
            if 'folder' in program_spec:
                try:
                    dxpy.DXProject(dest_project).new_folder(program_spec['folder'], parents=True)
                except dxpy.exceptions.DXAPIError:
                    pass # TODO: make this better
            target_folder = program_spec['folder'] if 'folder' in program_spec else '/'
            dx_resource_archive = dxpy.upload_local_file(tar_fh.name, wait_on_close=True, folder=target_folder, hidden=True)
            archive_link = dxpy.dxlink(dx_resource_archive.get_id())
            return [{'name': 'resources.tar.xz', 'id': archive_link}]
    else:
        return None

def upload_program(src_dir, uploaded_resources, check_name_collisions=True, overwrite=False, project=None):
    program_spec = get_program_spec(src_dir)

    dest_project = project or program_spec['project']

    if check_name_collisions:
        logging.debug("Searching for programs with name " + program_spec["name"])
        for result in dxpy.find_data_objects(classname="program", properties={"name": program_spec["name"]}, project=dest_project):
            if overwrite:
                logging.info("Deleting program %s" % (result['id']))
                # TODO: test me
                dxpy.DXProject(dest_project).remove_objects([result['id']])
            else:
                raise ProgramBuilderException("A program with name %s already exists (id %s) and the overwrite option was not given" % (program_spec["name"], result['id']))

    if "run" in program_spec and "file" in program_spec["run"]:
        # Avoid using run.file for now, it's not fully implemented
        #code_filename = os.path.join(src_dir, program_spec["run"]["file"])
        #f = dxpy.upload_local_file(code_filename, wait_on_close=True)
        #program_spec["run"]["file"] = f.get_id()
        # Put it into run.code instead
        with open(os.path.join(src_dir, program_spec["run"]["file"])) as code_fh:
            program_spec["run"]["code"] = code_fh.read()
            del program_spec["run"]["file"]

    if uploaded_resources is not None:
        program_spec["run"].setdefault("bundledDepends", [])
        program_spec["run"]["bundledDepends"].extend(uploaded_resources)

    program_id = dxpy.api.programNew(program_spec)["id"]

    properties = {"name": program_spec["name"]}
    if "title" in program_spec:
        properties["title"] = program_spec["title"]
    if "summary" in program_spec:
        properties["summary"] = program_spec["summary"]
    if "description" in program_spec:
        properties["description"] = program_spec["description"]

    dxpy.api.programSetProperties(program_id, {"project": dest_project, "properties": properties})

    if "categories" in program_spec:
        dxpy.DXProgram(program_id).add_tags(program_spec["categories"])

    return program_id

def create_app(program_id, src_dir, publish=False, set_default=False, owner=None, try_versions=None):
    app_spec = get_app_spec(src_dir)
    print >> sys.stderr, "Will create app with spec: ", app_spec

    program_desc = dxpy.DXProgram(program_id).describe(incl_properties=True)
    app_spec["program"] = program_id
    app_spec["name"] = program_desc["name"]

    if "title" in program_desc["properties"]:
        app_spec["title"] = program_desc["properties"]["title"]
    if "summary" in program_desc["properties"]:
        app_spec["summary"] = program_desc["properties"]["summary"]
    if "description" in program_desc["properties"]:
        app_spec["description"] = program_desc["properties"]["description"]

    if owner:
        app_spec["owner"] = owner
    if not try_versions:
        try_versions = [app_spec["version"]]

    for version in try_versions:
        try:
            app_spec['version'] = version
            app_id = dxpy.api.appNew(app_spec)["id"]
            break
        except dxpy.exceptions.DXAPIError as e:
            # TODO: detect this error more reliably
            if e.name == 'InvalidInput' and e.msg == 'Specified name and version conflict with an existing alias':
                # The version number was already taken, try the next alternative
                print >> sys.stderr, '%s %s already exists' % (app_spec["name"], version)
                continue
            raise e
    else:
        # All versions failed
        raise EnvironmentError('Could not create any of the requested versions: ' + ', '.join(try_versions))

    if "categories" in app_spec:
        dxpy.api.appAddCategories(app_id, input_params={'categories': app_spec["categories"]})

    if publish:
        dxpy.api.appPublish(app_id, input_params={'makeDefault': set_default})

    return app_id
