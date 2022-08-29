#!/usr/bin/env python

import os
import sys

from dxpy.nextflow.nextflow_templates import get_nextflow_dxapp
from dxpy.nextflow.nextflow_templates import get_nextflow_src
from dxpy.nextflow.nextflow_utils import get_template_dir
from dxpy.nextflow.nextflow_utils import write_exec
from dxpy.nextflow.nextflow_utils import write_dxapp
from dxpy.exceptions import err_exit
import dxpy
import json
import argparse
from distutils.dir_util import copy_tree
parser = argparse.ArgumentParser(description="Uploads a DNAnexus App.")


def build_pipeline_from_repository(repository, tag, profile="", github_creds=None, brief=False):
    """
    :param repository: URL to a Git repository
    :type repository: string
    :param tag: tag of a given Git repository. If it is not provided, the default branch is used.
    :type tag: string
    :param profile: Custom Nextflow profile, for more information visit https://www.nextflow.io/docs/latest/config.html#config-profiles
    :type profile: string
    :param brief: Level of verbosity
    :type brief: boolean
    :returns: ID of the created applet

    Runs the Nextflow Pipeline Importer app, which creates a Nextflow applet from a given Git repository.
    """
    # FIXME: is this already present somewhere?
    def create_dxlink(dx_object):
        try:
            if dxpy.is_dxlink(dx_object):
                return dx_object
            if ":" in dx_object:
                object_project, object_id = dx_object.split(":", 1)
            else:
                object_id = dx_object
                object_project = None
            if not dxpy.utils.resolver.is_hashid(object_id):
                object_project, _, object_id = dxpy.utils.resolver.resolve_existing_path(object_id, expected="entity", expected_classes=["file"], describe=False)
                object_id = object_id["id"]
            return dxpy.dxlink(object_id=object_id, project_id=object_project)
        except dxpy.utils.resolver.ResolutionError:
            err_exit("GitHub credentials ('{}') file could not be found!".format(dx_object))


    build_project_id = dxpy.WORKSPACE_ID
    if build_project_id is None:
        parser.error(
            "Can't create an applet without specifying a destination project; please use the -d/--destination flag to explicitly specify a project")

    input_hash = {
        "repository_url": repository,
    }
    if tag:
        input_hash["repository_tag"] = tag
    if profile:
        input_hash["config_profile"] = profile
    if github_creds:
        input_hash["github_credentials"] = create_dxlink(github_creds)

    print(build_project_id)
    nf_builder_job = dxpy.DXApp(name='nextflow_pipeline_importer').run(app_input=input_hash, project=build_project_id, name="Nextflow build of %s" % (repository), detach=True)

    if not brief:
        print("Started builder job %s" % (nf_builder_job.get_id(),))
    nf_builder_job.wait_on_done(interval=1)
    applet_id, _ = dxpy.get_dxlink_ids(nf_builder_job.describe()['output']['output_applet'])
    if not brief:
        print("Created Nextflow pipeline %s" % (applet_id))
    else:
        print(applet_id)
    return applet_id

def prepare_nextflow(resources_dir, profile):
    """
    :param resources_dir: Directory with all resources needed for the Nextflow pipeline. Usually directory with user's Nextflow files.
    :type resources_dir: str or Path
    :param profile: Custom NF profile, for more information visit https://www.nextflow.io/docs/latest/config.html#config-profiles
    :type profile: string

    Creates files necessary for creating an applet on the Platform, such as dxapp.json and a source file. These files are created in '.dx.nextflow' directory.
    """
    assert os.path.exists(resources_dir)
    inputs = []
    dxapp_dir = os.path.join(resources_dir, '.dx.nextflow')
    os.makedirs(dxapp_dir, exist_ok=True)
    if os.path.exists(f"{resources_dir}/nextflow_schema.json"):
        inputs = prepare_inputs(f"{resources_dir}/nextflow_schema.json")
    dxapp_content = get_nextflow_dxapp(inputs, resources_dir)
    exec_content = get_nextflow_src(inputs=inputs, profile=profile)
    copy_tree(get_template_dir(), dxapp_dir)
    write_dxapp(dxapp_dir, dxapp_content)
    write_exec(dxapp_dir, exec_content)
    return dxapp_dir


def prepare_inputs(schema_file):
    """
    :param schema_file: path to nextflow_schema.json file
    :type schema_file: str or Path
    :returns: DNAnexus datatype used in dxapp.json inputSpec field
    :rtype: string
    Creates DNAnexus inputs (inputSpec) from Nextflow inputs.
    """
    def get_default_input_value(key):
        input_items = {
            "hidden": False,
        }
        if key in input_items:
            return input_items[key]
        raise Exception("Default value for key {} is not given.".format(key))

    def get_dx_type(nf_type):
        types = {
            "string": "str",
            "integer": "int",
            "number": "float",
            "boolean": "boolean",
            "object": "hash"
            # TODO: add directory + file + path
        }
        if nf_type in types:
            return types[nf_type]
        # TODO: raise Exception after file+directory is implemented
        return "string"
        # raise Exception(f"type {nf_type} is not supported by DNAnexus")

    inputs = []
    with open(schema_file, "r") as fh:
        schema = json.load(fh)
    for d_key, d_schema in schema.get("definitions", {}).items():
        required_inputs = d_schema.get("required", [])
        for property_key, property in d_schema.get("properties", {}).items():
            dx_input = {}
            dx_input["name"] = property_key
            dx_input["title"] = dx_input['name']
            if "help_text" in property:
                dx_input["help"] = property.get('help_text')
            if "default" in property:
                dx_input["default"] = property.get("default")
            dx_input["hidden"] = property.get('hidden', get_default_input_value('hidden'))
            dx_input["class"] = get_dx_type(property_key)
            if property_key not in required_inputs:
                dx_input["optional"] = True
                dx_input["help"] = "(Optional) {}".format(dx_input["help"])
            inputs.append(dx_input)
    return inputs
