#!/usr/bin/env python

import os
import dxpy
import json

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
    region=dxpy.DXProject().describe(fields={})
    ...