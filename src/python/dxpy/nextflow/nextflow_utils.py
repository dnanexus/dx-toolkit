#!/usr/bin/env python

import os, errno
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
    return os.path.join("/home/dnanexus/", get_resources_dir_name(resources_dir))

def get_importer_name():
    return "nextflow_pipeline_importer"

def get_template_dir():
    return os.path.join(os.path.dirname(dxpy.__file__), 'templating', 'templates', 'nextflow')

def write_exec(folder, content):
    exec_file = "{}/{}".format(folder, get_source_file_name())
    try:
        os.makedirs(os.path.dirname(os.path.abspath(exec_file)))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
        pass
    with open(exec_file, "w") as fh:
        fh.write(content)

def write_dxapp(folder, content):
    dxapp_file = "{}/dxapp.json".format(folder)
    with open(dxapp_file, "w") as dxapp:
        json.dump(content, dxapp)

def get_regional_options(region):
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
    # The order of assets in the tuple is: nextaur, nextflow
    prod_assets = {
        "aws:ap-southeast-2": ("record-GP5Bf385XGYQv71K5V25Q520",
                               "record-GKv2BV05Q46P03jk5pQvVyBp"),
        "aws:eu-central-1": ("record-GP5Bjqj4b633kF765jB330F2",
                             "record-GKv2B384q2YZFjVz3jFPj7Zq"),
        "aws:eu-west-2": ("record-GP5BgFjJXG2z2qk35G48f0vB",
                          "record-GKv2BK8Jp9k14y5Y0bbGkf65"),
        "aws:us-east-1": ("record-GP5BV700z3VxZ9xq5Qy3v962",
                          "record-GKv1xPQ06J10XzPb3fVBF8XB"),
        "azure:westeurope": ("record-GP5BZY0BKYf82PqX5kz6p875",
                             "record-GKv2FGjBPKPP9k0x5Kpzj1ZX"),
        "azure:westus": ("record-GP5BvZ890PK82PqX5kz6p94b",
                         "record-GKv2j109b91Kg31j5Kj7V0Qj"),
        "aws:eu-west-2-g": ("record-GP5Bp72KQ7b12Kx45vjfxF04",
                            "record-GKv2B2XKJ6xP03jk5pQvVyBQ")
    }
    stg_assets = {
        "aws:ap-southeast-2": (
            "record-GP5B00Q56Y24843ffZ0Y83Gb",
            "record-GKv18k85z6Qx78xb9kGkVB3j"),
        "aws:eu-central-1": (
            "record-GP5B2V046Z6J9kG5Vx49k0Kf",
            "record-GKv18Qj43XYfYk2YGGkbYFVK"),
        "aws:eu-west-2": (
            "record-GP5B1FQJ1pz6gB19bv9BYkPP",
            "record-GKv18b0Jz9YV9vqx9j9GGq05"),
        "aws:us-east-1": (
            "record-GP59VV80zXP4843ffZ0Y7fv2",
            "record-GKv0P7Q00Q82gyJjGG8y03xf"),
        "azure:westeurope": (
            "record-GP59y98BQxYqB75pQy66X9FV",
            "record-GKv19PjBJgXfVjgBGGB1Qjy2"),
        "azure:westus": (
            "record-GP5B6g89QjBFB75pQy66XVzP",
            "record-GKv19X098bVGV15j9gpy8q2z"),
        "aws:eu-west-2-g": (
            "record-GP5B3p2Kb446439821Qqj02X",
            "record-GKv1892K6Gkk0K53GFj415XZ")
    }

    try:
        dxpy.describe(prod_assets[region][0], fields={})
        return prod_assets[region]
    except ResourceNotFound:
        return stg_assets[region]

