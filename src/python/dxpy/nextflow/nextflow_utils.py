#!/usr/bin/env python3
from os import path, makedirs, listdir
import re
import errno
import dxpy
import json
import shutil
from dxpy.exceptions import ResourceNotFound
from dxpy.nextflow.collect_images import run_nextaur_collect, bundle_docker_images


def get_source_file_name():
    return "src/nextflow.sh"


def get_resources_dir_name(resources_dir):
    """
    :param resources_dir: Directory with all source files needed to build an applet. Can be an absolute or a relative path.
    :type resources_dir: str or Path
    :returns: The name of the folder
    :rtype: str
    """
    if resources_dir == None:
        return ''
    return path.basename(path.abspath(resources_dir))


def get_resources_subpath(resources_dir):
    return path.join("/home/dnanexus/", get_resources_dir_name(resources_dir))


def get_importer_name():
    return "nextflow_pipeline_importer"


def get_template_dir():
    return path.join(path.dirname(dxpy.__file__), 'templating', 'templates', 'nextflow')


def is_importer_job():
    try:
        with open("/home/dnanexus/dnanexus-job.json", "r") as f:
            job_info = json.load(f)
            return job_info.get("executableName") == get_importer_name()
    except Exception:
        return False


def write_exec(folder, content):
    exec_file = "{}/{}".format(folder, get_source_file_name())
    try:
        makedirs(path.dirname(path.abspath(exec_file)))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
        pass
    with open(exec_file, "w") as fh:
        fh.write(content)


def find_readme(dir):
    """
    Returns first readme (in alphabetical order) from a root of a given folder
    :param dir: Directory in which we search for readme files
    :type dir: str or Path
    :returns: List[str]
    """
    readme_pattern = re.compile(r"readme(\.(txt|md|rst|adoc|html|txt|asciidoc|org|text|textile|pod|wiki))?", re.IGNORECASE)
    file_list = [f for f in listdir(dir) if path.isfile(path.join(dir, f))]
    readme_files = [file for file in file_list if readme_pattern.match(file)]
    readme_files.sort()
    return readme_files[0] if readme_files else None


def create_readme(source_dir, destination_dir):
    """
    :param destination_dir: Directory where readme is going to be created
    :type destination_dir: str or Path
    :param source_dir: Directory from which readme is going to be copied
    :type source_dir: str or Path
    :returns: None
    """
    readme_file = find_readme(source_dir)

    if readme_file:
        source_path = path.join(source_dir, readme_file)
        destination_path = path.join(destination_dir, "Readme.md")
        shutil.copy2(source_path, destination_path)


def write_dxapp(folder, content):
    dxapp_file = "{}/dxapp.json".format(folder)
    with open(dxapp_file, "w") as dxapp:
        json.dump(content, dxapp)


def get_regional_options(region, resources_dir, profile, cache_docker, nextflow_pipeline_params):
    nextaur_asset, nextflow_asset, awscli_asset = get_nextflow_assets(region)
    regional_instance_type = get_instance_type(region)
    if cache_docker:
        image_refs = run_nextaur_collect(resources_dir, profile, nextflow_pipeline_params)
        image_bundled = bundle_docker_images(image_refs)
    else:
        image_bundled = {}
    regional_options = {
        region: {
            "systemRequirements": {
                "*": {
                    "instanceType": regional_instance_type
                }
            },
            "assetDepends": [
                {"id": nextaur_asset},
                {"id": nextflow_asset},
                {"id": awscli_asset}
            ],
            "bundledDepends": image_bundled
        }
    }
    return regional_options


def get_instance_type(region):
    instance_type = {
        "aws:ap-southeast-2": "mem2_ssd1_v2_x4",
        "aws:eu-central-1": "mem2_ssd1_v2_x4",
        "aws:us-east-1": "mem2_ssd1_v2_x4",
        "aws:me-south-1": "mem2_ssd1_v2_x4",
        "azure:westeurope": "azure:mem2_ssd1_x4",
        "azure:westus": "azure:mem2_ssd1_x4",
        "aws:eu-west-2-g": "mem2_ssd1_v2_x4"
    }.get(region)
    if not instance_type:
        raise dxpy.exceptions.ResourceNotFound("Instance type is not specified for region {}.".format(region))
    return instance_type


def get_nextflow_assets(region):
    nextflow_basepath = path.join(path.dirname(dxpy.__file__), 'nextflow')
    # The order of assets in the tuple is: nextaur, nextflow
    nextaur_assets = path.join(nextflow_basepath, "nextaur_assets.json")
    nextflow_assets = path.join(nextflow_basepath, "nextflow_assets.json")
    awscli_assets = path.join(nextflow_basepath, "awscli_assets.json")
    try:
        with open(nextaur_assets, 'r') as nextaur_f, open(nextflow_assets, 'r') as nextflow_f, open(awscli_assets, 'r') as awscli_f:
            nextaur = json.load(nextaur_f)[region]
            nextflow = json.load(nextflow_f)[region]
            awscli = json.load(awscli_f)[region]
        dxpy.describe(nextflow, fields={})  # existence check
        return nextaur, nextflow, awscli
    except ResourceNotFound:
        nextaur_assets = path.join(nextflow_basepath, "nextaur_assets.staging.json")
        nextflow_assets = path.join(nextflow_basepath, "nextflow_assets.staging.json")
        awscli_assets = path.join(nextflow_basepath, "awscli_assets.staging.json")

        with open(nextaur_assets, 'r') as nextaur_f, open(nextflow_assets, 'r') as nextflow_f, open(awscli_assets, 'r') as awscli_f:
            return json.load(nextaur_f)[region], json.load(nextflow_f)[region], json.load(awscli_f)[region]
