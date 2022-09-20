#!/usr/bin/env python

from dxpy.nextflow.nextflow_utils import (get_template_dir, get_source_file_name, get_resources_subpath, get_importer_name)
import json
import os


def get_nextflow_dxapp(custom_inputs=[], name=""):
    """
    :param custom_inputs: Custom inputs that will be used in the created Nextflow pipeline.
    :type custom_inputs: list

    Creates Nextflow dxapp.json from the Nextflow dxapp.json template
    """
    def is_importer_job():
        try:
            with open("/home/dnanexus/dnanexus-job.json", "r") as f:
                job_info = json.load(f)
                return job_info.get("executableName") == get_importer_name()
        except Exception:
            return False

    if custom_inputs is None:
        custom_inputs = []
    with open(os.path.join(str(get_template_dir()), 'dxapp.json'), 'r') as f:
        dxapp = json.load(f)
    dxapp["inputSpec"] = custom_inputs + dxapp["inputSpec"]
    dxapp["runSpec"]["file"] = get_source_file_name()

    # By default title and summary will be set to the pipeline name
    if name is None or name == "":
        name = "Nextflow pipeline"
    dxapp["name"] = name
    dxapp["title"] = name
    dxapp["summary"] = name
    if os.environ.get("DX_JOB_ID") is None or not is_importer_job():
        dxapp["details"] = {"repository": "local"}
    return dxapp


def get_nextflow_src(custom_inputs=[], profile=None, resources_dir=None):
    """
    :param custom_inputs: Custom inputs that will be used in created Nextflow params-file
    :type custom_inputs: list
    :param profile: Custom Nextflow profile to be used when running a Nextflow pipeline, for more information visit https://www.nextflow.io/docs/latest/config.html#config-profiles
    :type profile: string
    :param resources_dir: Directory with all source files needed to build an applet. Can be an absolute or a relative path.
    :type resources_dir: str or Path
    :returns: String containing the whole source file of an applet.
    :rtype: string

    Creates Nextflow source file from the Nextflow source file template
    """
    with open(os.path.join(str(get_template_dir()), get_source_file_name()), 'r') as f:
        src = f.read()

    required_runtime_params = ""
    override_config_params=""
    generate_runtime_config=""
    for i in custom_inputs:
        value = "${%s}" % (i['name'])
        if i.get("class") == "file":
            value = "dx://$(jq .[$dnanexus_link] -r <<< ${%s})" % i['name']
        elif i.get("class") == "string":
            value = '\\"' + value + '\\"'
# \\'"%s"\\'
        if i.get("optional", False):
            generate_runtime_config = generate_runtime_config + '''
            if [ -n "$%s" ]; then
                echo params.%s=%s >> nxf_runtime.config
            fi    
            '''% (i['name'], i['name'], value)
        #     override_config_params = override_config_params + '''
        #     if [ -n "$%s" ]; then
        #         sed -E -i 's|(\s+'"${%s}"'\s+=\s).*(\s*$)|\1%s\2|' nextflow.config
        #     fi
        #     ''' % (i['name'], i['name'], value)
        else:
            required_runtime_params += " --{} {}".format(i["name"], value)
    profile_arg = "-profile {}".format(profile) if profile else ""
    src = src.replace("@@GENERATE_RUNTIME_CONFIG@@", generate_runtime_config)
    src = src.replace("@@REQUIRED_RUNTIME_PARAMS@@", required_runtime_params)
    src = src.replace("@@PROFILE_ARG@@", profile_arg)
    src = src.replace("@@RESOURCES_SUBPATH@@",
                      get_resources_subpath(resources_dir))
    return src
