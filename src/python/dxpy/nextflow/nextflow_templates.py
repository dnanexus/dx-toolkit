#!/usr/bin/env python

from dxpy.nextflow.nextflow_utils import (get_template_dir, get_source_file_name, get_resources_subpath, get_importer_name)
import json
import os



def get_nextflow_dxapp(custom_inputs=None, name="Nextflow pipeline"):
    """
    :param custom_inputs: Custom inputs that will be used in the created Nextflow pipeline.
    :type custom_inputs: list

    Creates Nextflow dxapp.json from the Nextflow dxapp.json template
    """
    def is_importer_job():
        try:
            with open("/home/dnanexus/dnanexus-job.json", "r") as f:
                job_info=json.load(f)
                return job_info.get("executableName") == get_importer_name()
        except Exception:
            return False

    if custom_inputs is None:
        custom_inputs = []
    with open(os.path.join(str(get_template_dir()), 'dxapp.json'), 'r') as f:
        dxapp = json.load(f)
    dxapp["inputSpec"] = custom_inputs + dxapp["inputSpec"]
    dxapp["runSpec"]["file"] = get_source_file_name()
    dxapp["name"] = name
    dxapp["title"] = name
    dxapp["summary"] = name
    if os.environ.get("DX_JOB_ID") is None or not is_importer_job():
        dxapp["details"] = {"repository": "local"}
    return dxapp


def get_nextflow_src(inputs=None, profile=None):
    """
    :param inputs: Custom inputs that will be used in created Nextflow pipeline
    :type inputs: list
    :param profile: Custom Nextflow profile to be used when running a Nextflow pipeline, for more information visit https://www.nextflow.io/docs/latest/config.html#config-profiles
    :type profile: string
    :returns: String containing the whole source file of an applet.
    :rtype: string

    Creates Nextflow source file from the Nextflow source file template
    """
    if inputs is None:
        inputs = []
    with open(os.path.join(str(get_template_dir()), get_source_file_name()), 'r') as f:
        src = f.read()

    run_inputs = ""
    for i in inputs:
        value = "${%s}" % (i['name'])
        if i.get("class") == "file":
            value = "dx://$(jq .[$dnanexus_link] -r <<< ${%s})" % i['name']
        run_inputs = run_inputs + '''
        if [ -n "$%s" ]; then
            filtered_inputs+=(--%s="%s")
        fi
        ''' % (i['name'], i['name'], value)
    profile_arg = "-profile {}".format(profile) if profile else ""
    src = src.replace("@@RUN_INPUTS@@", run_inputs)
    src = src.replace("@@PROFILE_ARG@@", profile_arg)
    src = src.replace("@@RESOURCES_SUBPATH@@", get_resources_subpath())
    return src
