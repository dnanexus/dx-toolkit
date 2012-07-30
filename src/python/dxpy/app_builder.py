'''
DNAnexus Applet Builder Library

Contains methods used by the application builder to compile and deploy applets
and apps onto the platform.

You can specify the destination project in the following ways (with the earlier
ones taking precedence):

* Supply the 'project' argument to upload_resources or upload_applet.
* Supply the 'project' attribute in your dxapplet.json.
* Set the DX_WORKSPACE_ID environment variable (when running in a job context).

'''

import os, sys, json, subprocess, tempfile, logging, multiprocessing
import dxpy

NUM_CORES = multiprocessing.cpu_count()

class AppletBuilderException(Exception):
    pass

def validate_applet_spec(applet_spec):
    if "name" not in applet_spec:
        raise AppletBuilderException("Applet specification does not contain a name")

def validate_app_spec(app_spec):
    if "resources" not in app_spec:
        raise AppletBuilderException("App specification does not contain a resources field")

def get_applet_spec(src_dir):
    applet_spec_file = os.path.join(src_dir, "dxapp.json")
    with open(applet_spec_file) as fh:
        applet_spec = json.load(fh)

    validate_applet_spec(applet_spec)
    if 'project' not in applet_spec:
        applet_spec['project'] = dxpy.WORKSPACE_ID
    return applet_spec

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
        logging.debug("Running ./configure")
        subprocess.check_call([config_script])
    if os.path.isfile(os.path.join(src_dir, "Makefile")) \
        or os.path.isfile(os.path.join(src_dir, "makefile")) \
        or os.path.isfile(os.path.join(src_dir, "GNUmakefile")):
        logging.debug("Building with make -j%d" % (NUM_CORES,))
        subprocess.check_call(["make", "-C", src_dir, "-j" + str(NUM_CORES)])

def upload_resources(src_dir, project=None):
    applet_spec = get_applet_spec(src_dir)
    dest_project = project or applet_spec['project']
    resources_dir = os.path.join(src_dir, "resources")
    if os.path.exists(resources_dir) and len(os.listdir(resources_dir)) > 0:
        logging.debug("Uploading in " + src_dir)

        with tempfile.NamedTemporaryFile(suffix=".tar.xz") as tar_fh:
            subprocess.check_call(['tar', '-C', resources_dir, '-cJf', tar_fh.name, '.'])
            if 'folder' in applet_spec:
                try:
                    dxpy.DXProject(dest_project).new_folder(applet_spec['folder'], parents=True)
                except dxpy.exceptions.DXAPIError:
                    pass # TODO: make this better
            target_folder = applet_spec['folder'] if 'folder' in applet_spec else '/'
            dx_resource_archive = dxpy.upload_local_file(tar_fh.name, wait_on_close=True, folder=target_folder, hidden=True)
            archive_link = dxpy.dxlink(dx_resource_archive.get_id())
            return [{'name': 'resources.tar.xz', 'id': archive_link}]
    else:
        return None

def upload_applet(src_dir, uploaded_resources, check_name_collisions=True, overwrite=False, project=None):
    applet_spec = get_applet_spec(src_dir)

    dest_project = project or applet_spec['project']

    if 'description' not in applet_spec:
        readme_filename = None
        for filename in 'README.md', 'Readme.md', 'readme.md':
            if os.path.exists(os.path.join(src_dir, filename)):
                readme_filename = filename
                break
        if readme_filename is None:
            logging.warn("No description found")
        else:
            with open(os.path.join(src_dir, readme_filename)) as fh:
                applet_spec['description'] = fh.read()

    if check_name_collisions:
        logging.debug("Searching for applets with name " + applet_spec["name"])
        for result in dxpy.find_data_objects(classname="applet", properties={"name": applet_spec["name"]}, project=dest_project):
            if overwrite:
                logging.info("Deleting applet %s" % (result['id']))
                # TODO: test me
                dxpy.DXProject(dest_project).remove_objects([result['id']])
            else:
                raise AppletBuilderException("A applet with name %s already exists (id %s) and the overwrite option was not given" % (applet_spec["name"], result['id']))

    if "runSpec" in applet_spec and "file" in applet_spec["runSpec"]:
        # Avoid using runSpec.file for now, it's not fully implemented
        #code_filename = os.path.join(src_dir, applet_spec["runSpec"]["file"])
        #f = dxpy.upload_local_file(code_filename, wait_on_close=True)
        #applet_spec["runSpec"]["file"] = f.get_id()
        # Put it into runSpec.code instead
        with open(os.path.join(src_dir, applet_spec["runSpec"]["file"])) as code_fh:
            applet_spec["runSpec"]["code"] = code_fh.read()
            del applet_spec["runSpec"]["file"]

    if uploaded_resources is not None:
        applet_spec["runSpec"].setdefault("bundledDepends", [])
        applet_spec["runSpec"]["bundledDepends"].extend(uploaded_resources)

    applet_id = dxpy.api.appletNew(applet_spec)["id"]

    properties = {"name": applet_spec["name"]}
    if "title" in applet_spec:
        properties["title"] = applet_spec["title"]
    if "summary" in applet_spec:
        properties["summary"] = applet_spec["summary"]
    if "description" in applet_spec:
        properties["description"] = applet_spec["description"]

    dxpy.api.appletSetProperties(applet_id, {"project": dest_project, "properties": properties})

    if "categories" in applet_spec:
        dxpy.DXApplet(applet_id).add_tags(applet_spec["categories"])

    return applet_id

def create_app(applet_id, src_dir, publish=False, set_default=False, billTo=None, try_versions=None):
    app_spec = get_app_spec(src_dir)
    print >> sys.stderr, "Will create app with spec: ", app_spec

    applet_desc = dxpy.DXApplet(applet_id).describe(incl_properties=True)
    app_spec["applet"] = applet_id
    app_spec["name"] = applet_desc["name"]

    if "title" in applet_desc["properties"]:
        app_spec["title"] = applet_desc["properties"]["title"]
    if "summary" in applet_desc["properties"]:
        app_spec["summary"] = applet_desc["properties"]["summary"]
    if "description" in applet_desc["properties"]:
        app_spec["description"] = applet_desc["properties"]["description"]

    if billTo:
        app_spec["billTo"] = billTo
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
