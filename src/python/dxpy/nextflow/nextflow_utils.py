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
        "aws:ap-southeast-2": ("record-GK4zX70567q2Bx2x651zGJFP",
                               "record-GGbPv285K56b6YXp4x0yx4Xg"),
        "aws:eu-central-1": ("record-GK4zV6042BJGz6bb63V40K0b",
                             "record-GGbPpyj48GgbqqGk4x4FzYgP"),
        "aws:eu-west-2": ("record-GK4zVkjJJ55Q49Bj6Z177GKj",
                          "record-GGbPqp0JkzF2XyxP50xKyGbp"),
        "aws:us-east-1": ("record-GK4zKv00kz5p0B8p65yZ9VJZ",
                          "record-GJypP1j0PqVPQ3YkJgq1B8Bq"),
        "azure:westeurope": ("record-GK4zY4QB77pvP6g4640b41vk",
                             "record-GGbPq00BXKVBBgk14x8pQj3K"),
        "azure:westus": ("record-GK4zXy893q6yyZzf6QY8qPxB",
                         "record-GGbPq089xQvJBjkG4vbPg0Kg"),
        "aws:eu-west-2-g": ("record-GK4zX8BKKgQbYbBp6Y7q5P3Q",
                            "record-GGbPqXpKyyZzyfPg4yX55JXb")
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

