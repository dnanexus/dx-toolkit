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
            "record-GK4z9X85KXZggJ40PGXbzyqQ",
            "record-GJyfYx85FpVKxgxg4kQj67yB"),
        "aws:eu-central-1": (
            "record-GK4z90Q4vXYBG5zqPPy6FFk5",
            "record-GJyfYkj4XGF5YV9x4kk7XG0K"),
        "aws:eu-west-2": (
            "record-GK4z96jJ903jKV7PPFk47zbg",
            "record-GJyfY7jJ079q7gFV4pB3QkZ5"),
        "aws:us-east-1": (
            "record-GK4z5xQ0bf7BbQzQPJ4Zb7yB",
            "record-GJyfG8j0pVx67bq751qkgPfV"),
        "azure:westeurope": (
            "record-GK4zB00Bbvb7gJ40PGXbzyqX",
            "record-GJyfZ8QB80fbb8064pKz991Z"),
        "azure:westus": (
            "record-GK4zBxj9X8G8KV7PPFk47zf4",
            "record-GJyfZBQ9k78f7bq751qkgPg1"),
        "aws:eu-west-2-g": (
            "record-GK4z94pKb480qfxgPQ1B3gY7",
            "record-GJyfY8pKbPb3vfp34kx605BY")
    }

    try:
        dxpy.describe(prod_assets[region][0], fields={})
        return prod_assets[region]
    except ResourceNotFound:
        return stg_assets[region]
