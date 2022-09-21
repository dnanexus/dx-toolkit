#!/usr/bin/env python

import os
import dxpy
import json
from dxpy.exceptions import ResourceNotFound
from dxpy.scripts.dx_build_app import parse_destination

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
    return os.path.basename(os.path.abspath(resources_dir))

def get_resources_subpath(resources_dir):
    return "/home/dnanexus/" + get_resources_dir_name(resources_dir)

def get_importer_name():
    return "nextflow_pipeline_importer"

def get_template_dir():
    return os.path.join(os.path.dirname(dxpy.__file__), 'templating', 'templates', 'nextflow')

def get_destination_region(destination):
    """
    :param destination: The destination path for building the applet, as given by the --destination option to "dx build". Will be in the form [PROJECT_NAME_OR_ID:][/[FOLDER/][NAME]].
    :type destination: str
    :returns: The name of the region in which the applet will be built, e.g. 'aws:us-east-1'
    :rtype: str
    """
    if destination:
        dest_project_id, _, _ = parse_destination(destination)
    else:
        dest_project_id = dxpy.WORKSPACE_ID
    return dxpy.api.project_describe(dest_project_id, input_params={"fields": {"region": True}})["region"]

def write_exec(folder, content):
    exec_file = "{}/{}".format(folder, get_source_file_name())
    os.makedirs(os.path.dirname(os.path.abspath(exec_file)), exist_ok=True)
    with open(exec_file, "w") as exec:
        exec.write(content)

def write_dxapp(folder, content):
    dxapp_file = "{}/dxapp.json".format(folder)
    with open(dxapp_file, "w") as dxapp:
        json.dump(content, dxapp)

def F(region):
    nextflow_asset, nextaur_asset = get_nextflow_assets(region)
    regional_options = {
        region: {
            "systemRequirements": {
                "*": {
                    "instanceType": "mem1_ssd1_v2_x2"
                }
            },
            "assetDepends": [
                {"id": nextaur_asset},
                {"id": nextflow_asset}
            ]
        }
    }
    return regional_options

def get_nextflow_assets(region):
    prod_assets = {
        "aws:ap-southeast-2": ("record-GGbPk785py8Kz5bj4xJqBQ1x", "record-GGbPv285K56b6YXp4x0yx4Xg"),
        "aws:eu-central-1": ("record-GGbPk5j49jj85Jbg4vQVXy38", "record-GGbPpyj48GgbqqGk4x4FzYgP"),
        "aws:eu-west-2": ("record-GGbPkGjJ0pX15P8f4vgX0G0q", "record-GGbPqp0JkzF2XyxP50xKyGbp"),
        "aws:us-east-1": ("record-GGbP68Q0qg6877g84xQXqB0F", "record-GGbKvfQ0Yy4JPBbP4vfF5P3z"),
        "azure:westeurope": ("record-GGbPpQQBZGZP29Yg4xZpgxZP", "record-GGbPq00BXKVBBgk14x8pQj3K"),
        "azure:westus": ("record-GGbPpQj9vGQzFq8y4xG102Zz", "record-GGbPq089xQvJBjkG4vbPg0Kg"),
        "aws:eu-west-2-g": ("record-GGbPk72KX7VbgXp34y4z119x", "record-GGbPqXpKyyZzyfPg4yX55JXb")
    }
    stg_assets = {
        "aws:ap-southeast-2": ("record-GGbJz3j52fYPG2fp8Y1Q3vvG", "record-GGbVF5857Zj7PbQQ8FP1KKxX"),
        "aws:eu-central-1": ("record-GGbJz204G2gQ2bpj8BzyYQzj", "record-GGbVBb84ZBgV92b98Yz263QK"),
        "aws:eu-west-2": ("record-GGbJz10JpGq192b98Yz21pJQ", "record-GGbVBp8JXvXj0ZZJ8Y2KYjj7"),
        "aws:us-east-1": ("record-GGb8zb801pBqB2PF8XjBf7Px", "record-GG23y1j00vk1BJ5zKzykGXvY"),
        "azure:westeurope": ("record-GGbJyZ8BP1f3X0xP8GkgJP41", "record-GGbVFZjBQZzGv2p88F39ZJY3"),
        "azure:westus": ("record-GGbJy489qFGpB0468YFXxyX5", "record-GGbVG009zzFX6fg18YKxKXy7"),
        "aws:eu-west-2-g": ("record-GGbJyxpKkJJFB2PF8XjBpV00", "record-GGbVBv2KqxX767f98FXG6p0Q")
    }

    try:
        dxpy.describe(prod_assets[region][0], fields={})
        return prod_assets[region]
    except ResourceNotFound:
        return stg_assets[region]
