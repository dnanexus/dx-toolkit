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
    region=dxpy.DXProject().describe()["region"]
    nextaur_asset, nextflow_asset = get_nextflow_assets()
    regional_options = {
        region: {
            "systemRequirements": {
                "*": {
                    "instanceType": "mem1_ssd1_v2_x2"
                }
            },
            "assetDepends": [
                {"id": nextflow_asset},
                {"id": nextaur_asset}
            ]
        }
    }
    return regional_options

def get_nextflow_assets(region):
    prod_assets = {
        "aws:ap-southeast-2": ("record-xxxxxxxxxxxxxxxxxxxxxxxx", "record-yyyyyyyyyyyyyyyyyyyyyyyy"),
        "aws:eu-central-1": ("record-xxxxxxxxxxxxxxxxxxxxxxxx", "record-yyyyyyyyyyyyyyyyyyyyyyyy"),
        "aws:eu-west-2": ("record-xxxxxxxxxxxxxxxxxxxxxxxx", "record-yyyyyyyyyyyyyyyyyyyyyyyy"),
        "aws:us-east-1": ("record-xxxxxxxxxxxxxxxxxxxxxxxx", "record-yyyyyyyyyyyyyyyyyyyyyyyy"),
        "azure:westeurope": ("record-xxxxxxxxxxxxxxxxxxxxxxxx", "record-yyyyyyyyyyyyyyyyyyyyyyyy"),
        "azure:westus": ("record-xxxxxxxxxxxxxxxxxxxxxxxx", "record-yyyyyyyyyyyyyyyyyyyyyyyy"),
        "aws:eu-west-2-g": ("record-xxxxxxxxxxxxxxxxxxxxxxxx", "record-yyyyyyyyyyyyyyyyyyyyyyyy")
    }
    stg_assets = {
        "aws:ap-southeast-2": ("record-xxxxxxxxxxxxxxxxxxxxxxxx", "record-yyyyyyyyyyyyyyyyyyyyyyyy"),
        "aws:eu-central-1": ("record-xxxxxxxxxxxxxxxxxxxxxxxx", "record-yyyyyyyyyyyyyyyyyyyyyyyy"),
        "aws:eu-west-2": ("record-xxxxxxxxxxxxxxxxxxxxxxxx", "record-yyyyyyyyyyyyyyyyyyyyyyyy"),
        "aws:us-east-1": ("record-GG0q3X00fVVZQP9G4kFF16zp", "record-GG1P0xQ0F9gggxxjKzqV40vg"),
        "azure:westeurope": ("record-xxxxxxxxxxxxxxxxxxxxxxxx", "record-yyyyyyyyyyyyyyyyyyyyyyyy"),
        "azure:westus": ("record-xxxxxxxxxxxxxxxxxxxxxxxx", "record-yyyyyyyyyyyyyyyyyyyyyyyy"),
        "aws:eu-west-2-g": ("record-xxxxxxxxxxxxxxxxxxxxxxxx", "record-yyyyyyyyyyyyyyyyyyyyyyyy")
    }
    try:
        dxpy.describe(prod_assets[region], fields={})
        return prod_assets[region]
    except ResourceNotFound:
        return stg_assets[region]
