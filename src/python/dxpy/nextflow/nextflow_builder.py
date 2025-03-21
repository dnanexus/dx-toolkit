#!/usr/bin/env python3
import os
import dxpy
import json
import argparse
from glob import glob
import shutil
import tempfile
from functools import partial

from dxpy.nextflow.nextflow_templates import (get_nextflow_dxapp, get_nextflow_src)
from dxpy.nextflow.nextflow_utils import (get_template_dir, write_exec, write_dxapp, get_importer_name,
                                          create_readme, get_nested, get_allowed_extra_fields_mapping)
from dxpy.cli.exec_io import parse_obj
from dxpy.cli import try_call
from dxpy.utils.resolver import resolve_existing_path


parser = argparse.ArgumentParser(description="Uploads a DNAnexus App.")


def build_pipeline_with_npi(
        repository=None,
        tag=None,
        cache_docker=False,
        docker_secrets=None,
        nextflow_pipeline_params="",
        profile="",
        git_creds=None,
        brief=False,
        destination=None,
        extra_args=None
):
    """
    :param repository: URL to a Git repository
    :type repository: string
    :param tag: tag of a given Git repository. If it is not provided, the default branch is used.
    :type tag: string
    :param cache_docker: Pull a remote docker image and store it on the platform
    :type cache_docker: bool
    :param docker_secrets: Dx file id with the private docker registry credentials
    :type docker_secrets: string
    :param nextflow_pipeline_params: Custom Nextflow pipeline parameters
    :type nextflow_pipeline_params: string
    :param profile: Custom Nextflow profile, for more information visit https://www.nextflow.io/docs/latest/config.html#config-profiles
    :type profile: string
    :param brief: Level of verbosity
    :type brief: boolean
    :returns: ID of the created applet

    Runs the Nextflow Pipeline Importer app, which creates a Nextflow applet from a given Git repository.
    """

    def parse_extra_args(args):
        """
        :param args: extra args from command input
        :returns: overridable fields from extra_args
        """
        return {
            target_key: val
            for arg_path, target_key in get_allowed_extra_fields_mapping()
            if (val := get_nested(args, arg_path)) is not None
        }

    extra_args = extra_args or {}
    build_project_id = dxpy.WORKSPACE_ID
    build_folder = None
    input_hash = parse_extra_args(extra_args)
    input_hash["repository_url"] = repository

    # { NPI_input_name: (raw value, transformation function),...}
    input_updates = {
        "repository_tag": (tag, None),
        "config_profile": (profile, None),
        "cache_docker": (cache_docker, None),
        "nextflow_pipeline_params": (nextflow_pipeline_params, None),
        "docker_secrets": (docker_secrets, partial(parse_obj, klass="file")),
        "github_credentials": (git_creds, partial(parse_obj, klass="file")),
    }
    for key, (raw_value, transform) in input_updates.items():
        if raw_value:
            input_hash[key] = transform(raw_value) if transform else raw_value

    if destination:
        build_project_id, build_folder, _ = try_call(resolve_existing_path, destination, expected='folder')
    if build_project_id is None:
        parser.error(
            "Can't create an applet without specifying a destination project; please use the -d/--destination flag to explicitly specify a project")
    nf_builder_job = dxpy.DXApp(name=get_importer_name()).run(app_input=input_hash, project=build_project_id,
                                                              folder=build_folder,
                                                              name="Nextflow build of %s" % (repository), detach=True)

    if not brief:
        print("Started builder job %s" % (nf_builder_job.get_id(),))
    nf_builder_job.wait_on_done(interval=1)
    applet_id, _ = dxpy.get_dxlink_ids(nf_builder_job.describe(fields={"output": True})['output']['output_applet'])
    if not brief:
        print("Created Nextflow pipeline %s" % (applet_id))
    else:
        print(json.dumps(dict(id=applet_id)))
    return applet_id


def prepare_nextflow(
        resources_dir,
        profile,
        region,
        cache_docker=False,
        nextflow_pipeline_params=""
):
    """
    :param resources_dir: Directory with all resources needed for the Nextflow pipeline. Usually directory with user's Nextflow files.
    :type resources_dir: str or Path
    :param profile: Custom Nextflow profile. More profiles can be provided by using comma separated string (without whitespaces).
    :type profile: str
    :param region: The region in which the applet will be built.
    :type region: str
    :param cache_docker: Perform pipeline analysis and cache the detected docker images on the platform
    :type cache_docker: boolean
    :param nextflow_pipeline_params: Custom Nextflow pipeline parameters
    :type nextflow_pipeline_params: string
    :returns: Path to the created dxapp_dir
    :rtype: Path

    Creates files necessary for creating an applet on the Platform, such as dxapp.json and a source file. These files are created in '.dx.nextflow' directory.
    """
    assert os.path.exists(resources_dir)
    if not glob(os.path.join(resources_dir, "*.nf")):
        raise dxpy.app_builder.AppBuilderException(
            "Directory %s does not contain Nextflow file (*.nf): not a valid Nextflow directory" % resources_dir)
    dxapp_dir = tempfile.mkdtemp(prefix=".dx.nextflow")

    custom_inputs = prepare_custom_inputs(schema_file=os.path.join(resources_dir, "nextflow_schema.json"))
    dxapp_content = get_nextflow_dxapp(
        custom_inputs=custom_inputs,
        resources_dir=resources_dir,
        region=region,
        profile=profile,
        cache_docker=cache_docker,
        nextflow_pipeline_params=nextflow_pipeline_params
    )
    exec_content = get_nextflow_src(custom_inputs=custom_inputs, profile=profile, resources_dir=resources_dir)
    shutil.copytree(get_template_dir(), dxapp_dir, dirs_exist_ok=True)
    write_dxapp(dxapp_dir, dxapp_content)
    write_exec(dxapp_dir, exec_content)
    create_readme(resources_dir, dxapp_dir)
    return dxapp_dir


def prepare_custom_inputs(schema_file="./nextflow_schema.json"):
    """
    :param schema_file: path to nextflow_schema.json file
    :type schema_file: str or Path
    :returns: list of custom inputs defined with DNAnexus datatype 
    :rtype: list
    Creates custom input list from nextflow_schema.json that
    will be added in dxapp.json inputSpec field
    """

    def get_dx_type(nf_type, nf_format=None):
        types = {
            "string": "string",
            "integer": "int",
            "number": "float",
            "boolean": "boolean",
            "object": "hash"
        }
        str_types = {
            "file-path": "file",
            "directory-path": "string",  # So far we will stick with strings dx://...
            "path": "string"
        }
        if nf_type == "string" and nf_format in str_types:
            return str_types[nf_format]
        elif nf_type in types:
            return types[nf_type]
        raise Exception("type {} is not supported by DNAnexus".format(nf_type))

    inputs = []
    if not os.path.exists(schema_file):
        return inputs

    with open(schema_file, "r") as fh:
        schema = json.load(fh)
    defs_key = "definitions" if "definitions" in schema else "$defs" if "$defs" in schema else {}
    for d_key, d_schema in schema.get(defs_key).items():
        required_inputs = d_schema.get("required", [])
        for property_key, property in d_schema.get("properties", {}).items():
            dx_input = {}
            dx_input["name"] = property_key
            dx_input["title"] = dx_input['name']
            if "default" in property:
                dx_input["help"] = "Default value:{}\n".format(property.get("default", ""))
            if "help_text" in property:
                dx_input["help"] = dx_input.get("help", "") + property.get('help_text', "")
            dx_input["hidden"] = property.get('hidden', False)
            dx_input["class"] = get_dx_type(property.get("type"), property.get("format"))
            dx_input["optional"] = True
            if property_key not in required_inputs:
                dx_input["help"] = "(Nextflow pipeline optional) {}".format(dx_input.get("help", ""))
                inputs.append(dx_input)
            else:
                dx_input["help"] = "(Nextflow pipeline required) {}".format(dx_input.get("help", ""))
                inputs.insert(0, dx_input)

    return inputs
