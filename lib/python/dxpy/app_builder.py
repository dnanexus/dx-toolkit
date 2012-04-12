'''
DNAnexus App Builder Library

Contains methods used by the application builder to compile and deploy an app onto the platform.

'''

import os, sys, json, subprocess, tempfile, logging
import dxpy

class AppBuilderException(Exception):
    pass

def build(app_src_dir):
    logging.debug("Building in " + app_src_dir)
    # TODO: use Gentoo or deb buildsystem
    config_script = os.path.join(app_src_dir, "configure")
    if os.path.isfile(config_script) and os.access(config_script, os.X_OK):
        subprocess.check_call([config_script])
    if os.path.isfile(os.path.join(app_src_dir, "Makefile")) \
        or os.path.isfile(os.path.join(app_src_dir, "makefile")) \
        or os.path.isfile(os.path.join(app_src_dir, "GNUmakefile")):
        subprocess.check_call(["make", "-C", app_src_dir, "-j8"])

def upload_resources(app_src_dir):
    resources_dir = os.path.join(app_src_dir, "resources")
    if os.path.exists(resources_dir) and len(os.listdir(resources_dir)) > 0:
        logging.debug("Uploading in " + app_src_dir)

        with tempfile.NamedTemporaryFile(suffix=".tar.xz") as tar_fh:
            subprocess.check_call(['tar', '-C', resources_dir, '-cJf', tar_fh.name, '.'])
            dx_resource_archive = dxpy.upload_local_file(tar_fh.name, wait_on_close=True)
            #return [{'name': 'resources.tar.xz', 'id': {'$dnanexus_link': dx_resource_archive.get_id()}}]
            return [{'name': 'resources.tar.xz', 'id': dx_resource_archive.get_id()}]
    else:
        return None

def upload_app(app_src_dir, uploaded_resources, check_name_collisions=True, overwrite=False):
    with open(os.path.join(app_src_dir, "dxapp")) as fh:
        app_spec = json.load(fh)

    if "name" not in app_spec:
        raise AppBuilderException("App specification does not contain a name")

    if check_name_collisions:
        logging.debug("Searching for apps with name " + app_spec["name"])
        for app_id in dxpy.search(classname="app", properties={"name": app_spec["name"]}):
            if overwrite:
                logging.info("Deleting app %s" % (app_id))
                dxpy.api.appDestroy(app_id)
            else:
                raise AppBuilderException("An app with name %s already exists (id %s) and the overwrite option was not given" % (app_spec["name"], app_id))

    if "run" in app_spec and "file" in app_spec["run"]:
        code_filename = os.path.join(app_src_dir, app_spec["run"]["file"])
        f = dxpy.upload_local_file(code_filename, wait_on_close=True)
        app_spec["run"]["file"] = f.get_id()

    if uploaded_resources is not None:
        app_spec["bundledDepends"] = uploaded_resources

    app_id = dxpy.api.appNew(app_spec)["id"]

    dxpy.api.appSetProperties(app_id, {"name": app_spec["name"]})

    return app_id
