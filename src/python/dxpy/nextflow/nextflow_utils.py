import os
import dxpy
import json

def get_source_file_name():
    return "nextflow.sh"

def get_resources_subpath():
    return "/home/dnanexus/nfp"

def get_template_dir():
    return os.path.join(os.path.dirname(dxpy.__file__), 'templating', 'templates', 'nextflow')

def write_exec(folder, content):
    exec_file = f"{folder}/{get_source_file_name()}"
    os.makedirs(os.path.dirname(os.path.abspath(exec_file)), exist_ok=True)
    with open(exec_file, "w") as exec:
        exec.write(content)


def write_dxapp(folder, content):
    dxapp_file = f"{folder}/dxapp.json"
    with open(dxapp_file, "w") as dxapp:
        json.dump(content, dxapp)