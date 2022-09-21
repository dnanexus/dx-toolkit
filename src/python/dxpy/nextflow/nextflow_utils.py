#!/usr/bin/env python

import os
import dxpy
import json
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
    return os.path.basename(os.path.abspath(resources_dir))

def get_resources_subpath(resources_dir):
    return "/home/dnanexus/" + get_resources_dir_name(resources_dir)

def get_importer_name():
    return "nextflow_pipeline_importer"

def get_template_dir():
    return os.path.join(os.path.dirname(dxpy.__file__), 'templating', 'templates', 'nextflow')

def write_exec(folder, content):
    exec_file = "{}/{}".format(folder, get_source_file_name())
    os.makedirs(os.path.dirname(os.path.abspath(exec_file)), exist_ok=True)
    with open(exec_file, "w") as exec:
        exec.write(content)

def write_dxapp(folder, content):
    dxapp_file = "{}/dxapp.json".format(folder)
    with open(dxapp_file, "w") as dxapp:
        json.dump(content, dxapp)

def get_regional_options():
    region = dxpy.DXProject().describe()["region"]
    nextaur_asset, nextflow_asset = get_nextflow_assets(region)
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
        "aws:ap-southeast-2": ("record-GGbJpPj59YgV92b98Yz216qZ", "record-GGbJz3j52fYPG2fp8Y1Q3vvG"),
        "aws:eu-central-1": ("record-GGbJpFj4YYB26fg18YKxFQq8", "record-GGbJz204G2gQ2bpj8BzyYQzj"),
        "aws:eu-west-2": ("record-GGbJp7jJ8F8V92b98Yz216qQ", "record-GGbJz10JpGq192b98Yz21pJQ"),
        "aws:us-east-1": ("record-GG0q3X00fVVZQP9G4kFF16zp", "record-GGb8zb801pBqB2PF8XjBf7Px"),
        "azure:westeurope": ("record-GGbJq6jBB8k26fg18YKxFQqf", "record-GGbJyZ8BP1f3X0xP8GkgJP41"),
        "azure:westus": ("record-GGbJxfQ9qK380ZZJ8Y2KXVFb", "record-GGbJy489qFGpB0468YFXxyX5"),
        "aws:eu-west-2-g": ("record-GGbJp92K0KbzG2fp8Y1Q2bkG", "record-GGbJyxpKkJJFB2PF8XjBpV00")
    }

    try:
        dxpy.describe(prod_assets[region][0], fields={})
        return prod_assets[region]
    except ResourceNotFound:
        return stg_assets[region]
