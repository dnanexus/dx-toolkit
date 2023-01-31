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
    nextaur_assets = "./nextaur_assets.json"
    nextflow_assets = "./nextflow_assets.json"
    try:
        with open(nextaur_assets, 'w') as nextaur_f, open(nextflow_assets, 'w') as nextflow_f:
            nextaur, nextflow = json.load(nextaur_f)[region], json.load(nextflow_f)[region]
        dxpy.describe(nextaur, fields={})
        return nextaur, nextflow
    except ResourceNotFound:
        nextaur_assets = "./nextaur_assets.staging.json"
        nextflow_assets = "./nextflow_assets.staging.json"
        with open(nextaur_assets, 'w') as nextaur_f, open(nextflow_assets, 'w') as nextflow_f:
            return json.load(nextaur_f)[region], json.load(nextflow_f)[region]

