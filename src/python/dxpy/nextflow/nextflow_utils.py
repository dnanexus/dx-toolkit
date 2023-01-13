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
        "aws:ap-southeast-2": ("record-GKv2g2j5qzK4vz5F3z2P4gQ6",
                               "record-GKv2BV05Q46P03jk5pQvVyBp"),
        "aws:eu-central-1": ("record-GKv2g3j4Jypj29zj3Z4J15gY",
                             "record-GKv2B384q2YZFjVz3jFPj7Zq"),
        "aws:eu-west-2": ("record-GKv2g00JZ0z6932k3v475q5k",
                          "record-GKv2BK8Jp9k14y5Y0bbGkf65"),
        "aws:us-east-1": ("record-GKv1yB80y8FKzbF8BJQvqyPJ",
                          "record-GKv1xPQ06J10XzPb3fVBF8XB"),
        "azure:westeurope": ("record-GKv2j88B6064FjVz3jFPj882",
                             "record-GKv2FGjBPKPP9k0x5Kpzj1ZX"),
        "azure:westus": ("record-GKv2gg09qZ70pyB33fPG318y",
                         "record-GKv2j109b91Kg31j5Kj7V0Qj"),
        "aws:eu-west-2-g": ("record-GKv2g8pK392Kg31j5Kj7V0Q2",
                            "record-GKv2B2XKJ6xP03jk5pQvVyBQ")
    }
    stg_assets = {
        "aws:ap-southeast-2": (
            "record-GKv1K6Q5z7z19vqx9j9GGq0G",
            "record-GKv18k85z6Qx78xb9kGkVB3j"),
        "aws:eu-central-1": (
            "record-GKv1K9Q4j2Q19vqx9j9GGq0K",
            "record-GKv18Qj43XYfYk2YGGkbYFVK"),
        "aws:eu-west-2": (
            "record-GKv1Jp0Jf425x9FK9jbqgQJP",
            "record-GKv18b0Jz9YV9vqx9j9GGq05"),
        "aws:us-east-1": (
            "record-GKv0G9Q0gV6bjFYy9j82Kgkj",
            "record-GKv0P7Q00Q82gyJjGG8y03xf"),
        "azure:westeurope": (
            "record-GKv1PGQBK7fx1z529j3fKVB8",
            "record-GKv19PjBJgXfVjgBGGB1Qjy2"),
        "azure:westus": (
            "record-GKv1QBQ9xGybQ9Y79k6Kp1Kj",
            "record-GKv19X098bVGV15j9gpy8q2z"),
        "aws:eu-west-2-g": (
            "record-GKv1Jv2KV7p9yvFp9gjY2ky6",
            "record-GKv1892K6Gkk0K53GFj415XZ")
    }

    try:
        dxpy.describe(prod_assets[region][0], fields={})
        return prod_assets[region]
    except ResourceNotFound:
        return stg_assets[region]

