'''
DNAnexus Program Builder Library

Contains methods used by the application builder to compile and deploy programs onto the platform.

'''

import os, sys, json, subprocess, tempfile, logging
import dxpy

class ProgramBuilderException(Exception):
    pass

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

def upload_resources(src_dir):
    resources_dir = os.path.join(src_dir, "resources")
    if os.path.exists(resources_dir) and len(os.listdir(resources_dir)) > 0:
        logging.debug("Uploading in " + src_dir)

        with tempfile.NamedTemporaryFile(suffix=".tar.xz") as tar_fh:
            subprocess.check_call(['tar', '-C', resources_dir, '-cJf', tar_fh.name, '.'])
            dx_resource_archive = dxpy.upload_local_file(tar_fh.name, wait_on_close=True)
            archive_link = dxpy.dxlink(dx_resource_archive.get_id())
            return [{'name': 'resources.tar.xz', 'id': archive_link}]
    else:
        return None

def validateProgramSpec(program_spec):
    if "name" not in program_spec:
        raise ProgramBuilderException("Program specification does not contain a name")

def upload_program(src_dir, uploaded_resources, check_name_collisions=True, overwrite=False):
    with open(os.path.join(src_dir, "dxprogram")) as fh:
        program_spec = json.load(fh)

    validateProgramSpec(program_spec)

    program_spec['project'] = dxpy.WORKSPACE_ID

    if check_name_collisions:
        logging.debug("Searching for programs with name " + program_spec["name"])
        for result in dxpy.find_data_objects(classname="program", properties={"name": program_spec["name"]}, project=program_spec['project']):
            if overwrite:
                logging.info("Deleting program %s" % (result['id']))
                # TODO: test me
                dxpy.DXProject(program_spec['project']).remove_objects([result['id']])
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

    dxpy.api.programSetProperties(program_id, {"project": dxpy.WORKSPACE_ID, "properties": {"name": program_spec["name"]}})

    return program_id
