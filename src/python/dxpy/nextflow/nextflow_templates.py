#!/usr/bin/env python

from dxpy.nextflow.nextflow_utils import get_template_dir
from dxpy.nextflow.nextflow_utils import get_source_file_name
import json
import os



def get_nextflow_dxapp(custom_inputs=[], resources_directory="Nextflow pipeline"):
    """
    :param custom_inputs: Custom inputs that will be used in created Nextflow pipeline.
    :type custom_inputs: list

    Creates Nextflow dxapp.json from the Nextflow dxapp.json template
    """
    with open(os.path.join(str(get_template_dir()), 'dxapp.json'), 'r') as f:
        dxapp = json.load(f)
    dxapp["inputSpec"] = custom_inputs + dxapp["inputSpec"]
    dxapp["runSpec"]["file"] = get_source_file_name()
    dxapp["runSpec"]["name"] = resources_directory

    return dxapp


def get_nextflow_src(inputs=[], profile=None):
    """
    :param inputs: Custom inputs that will be used in created Nextflow pipeline
    :type inputs: list
    :param profile: Custom NF profile to be used when running Nextflow pipeline, for more information visit https://www.nextflow.io/docs/latest/config.html#config-profiles
    :type profile: string

    Creates Nextflow source file from the Nextflow source file template
    """
    with open(os.path.join(str(get_template_dir()), get_source_file_name()), 'r') as f:
        src = f.read()

    run_inputs = ""
    for i in inputs:
        #FIXME: not pushed, test
        run_inputs = run_inputs + f'''
        if [ -n "${i['name']}" ]; then
            filtered_inputs="${{filtered_inputs}} --{i['name']}=\"${i['name']}\""
        fi
        '''
    profile_arg = "-profile {}".format(profile) if profile else ""
    src = src.replace("@@RUN_INPUTS@@", run_inputs)
    src = src.replace("@@PROFILE_ARG@@", profile_arg)
    return src
