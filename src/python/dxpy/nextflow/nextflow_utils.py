#!/usr/bin/env python
from os import path, makedirs, listdir
import re
import errno
import dxpy
import json
import shutil
from dxpy.exceptions import ResourceNotFound


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


def create_readme(source_dir, destination_dir):
    """
    :param destination_dir: Directory where readme is going to be created
    :type destination_dir: str or Path
    :param source_dir: Directory from which readme is going to be copied
    :type source_dir: str or Path
    :returns: None
    """
    readme_pattern = re.compile(r"readme(\.(txt|md|rst|adoc|html|txt|asciidoc|org|text|textile|pod|wiki))?", re.IGNORECASE)
    destination_path = path.join(destination_dir, "Readme.md")
    file_list = [f for f in listdir(source_dir) if path.isfile(path.join(source_dir, f))]
    matching_files = [file for file in file_list if readme_pattern.match(file)]
    matching_files.sort()

    if matching_files:
        source_path = path.join(source_dir, matching_files[0])
        shutil.copy2(source_path, destination_path)


def write_dxapp(folder, content):
    dxapp_file = "{}/dxapp.json".format(folder)
    with open(dxapp_file, "w") as dxapp:
        json.dump(content, dxapp)


def get_regional_options(region):
    nextaur_asset, nextflow_asset = get_nextflow_assets(region)
    regional_instance_type = get_instance_type(region)
    regional_options = {
        region: {
            "systemRequirements": {
                "*": {
                    "instanceType": regional_instance_type
                }
            },
            "assetDepends": [
                {"id": nextaur_asset},
                {"id": nextflow_asset}
            ]
        }
    }
    return regional_options


def get_instance_type(region):
    instance_type = {
        "aws:ap-southeast-2": "mem2_ssd1_v2_x4",
        "aws:eu-central-1": "mem2_ssd1_v2_x4",
        "aws:us-east-1": "mem2_ssd1_v2_x4",
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
    try:
        with open(nextaur_assets, 'r') as nextaur_f, open(nextflow_assets, 'r') as nextflow_f:
            nextaur = json.load(nextaur_f)[region]
            nextflow = json.load(nextflow_f)[region]
        dxpy.describe(nextaur, fields={})
        return nextaur, nextflow
    except ResourceNotFound:
        nextaur_assets = path.join(nextflow_basepath, "nextaur_assets.staging.json")
        nextflow_assets = path.join(nextflow_basepath, "nextflow_assets.staging.json")

        with open(nextaur_assets, 'r') as nextaur_f, open(nextflow_assets, 'r') as nextflow_f:
            return json.load(nextaur_f)[region], json.load(nextflow_f)[region]
