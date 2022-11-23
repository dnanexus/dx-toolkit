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
    return "/home/dnanexus/" + get_resources_dir_name(resources_dir)

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
        "aws:ap-southeast-2": ("record-GGbPk785py8Kz5bj4xJqBQ1x", "record-GGbPv285K56b6YXp4x0yx4Xg"),
        "aws:eu-central-1": ("record-GGbPk5j49jj85Jbg4vQVXy38", "record-GGbPpyj48GgbqqGk4x4FzYgP"),
        "aws:eu-west-2": ("record-GGbPkGjJ0pX15P8f4vgX0G0q", "record-GGbPqp0JkzF2XyxP50xKyGbp"),
        "aws:us-east-1": ("record-GGbP68Q0qg6877g84xQXqB0F", "record-GGbKvfQ0Yy4JPBbP4vfF5P3z"),
        "azure:westeurope": ("record-GGbPpQQBZGZP29Yg4xZpgxZP", "record-GGbPq00BXKVBBgk14x8pQj3K"),
        "azure:westus": ("record-GGbPpQj9vGQzFq8y4xG102Zz", "record-GGbPq089xQvJBjkG4vbPg0Kg"),
        "aws:eu-west-2-g": ("record-GGbPk72KX7VbgXp34y4z119x", "record-GGbPqXpKyyZzyfPg4yX55JXb")
    }
    stg_assets = {
        "aws:ap-southeast-2": (
            "record-GJyfpF85VFv6BBp54pxQJ3zv",
            "record-GJyfYx85FpVKxgxg4kQj67yB"),
        "aws:eu-central-1": (
            "record-GJyfpB04kJjF7gFV4pB3QkZQ",
            "record-GJyfYkj4XGF5YV9x4kk7XG0K"),
        "aws:eu-west-2": (
            "record-GJyfkfQJK0PYf9fF4k5VgbFy",
            "record-GJyfY7jJ079q7gFV4pB3QkZ5"),
        "aws:us-east-1": (
            "record-GJyfgjQ0Q6vF7gFV4pB3QkZF",
            "record-GJyfG8j0pVx67bq751qkgPfV"),
        "azure:westeurope": (
            "record-GJyfpq0BZkFYvfp34kx605F1",
            "record-GJyfZ8QB80fbb8064pKz991Z"),
        "azure:westus": (
            "record-GJyfpx091jJYvfp34kx605F3",
            "record-GJyfZBQ9k78f7bq751qkgPg1"),
        "aws:eu-west-2-g": (
            "record-GJyfkpXKgjpf7bq751qkgPgG",
            "record-GJyfY8pKbPb3vfp34kx605BY")
    }

    try:
        dxpy.describe(prod_assets[region][0], fields={})
        return prod_assets[region]
    except ResourceNotFound:
        return stg_assets[region]

