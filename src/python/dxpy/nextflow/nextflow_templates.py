#!/usr/bin/env python3

from .nextflow_utils import (get_template_dir, get_source_file_name, get_resources_subpath,
                             is_importer_job, get_regional_options, get_resources_dir_name)
import json
import os
from dxpy import TOOLKIT_VERSION
from dxpy.compat import USING_PYTHON2, sys_encoding


def get_nextflow_dxapp(
        custom_inputs=None,
        resources_dir="",
        region="aws:us-east-1",
        profile="",
        cache_docker=False,
        nextflow_pipeline_params=""
):
    """
    :param custom_inputs: Custom inputs that will be used in the created Nextflow pipeline.
    :type custom_inputs: list
    :param resources_dir: Directory with all resources needed for the Nextflow pipeline. Usually directory with user's Nextflow files.
    :type resources_dir: str or Path
    :param region: The name of the region in which the applet will be built.
    :type region: str
    :param profile: Custom Nextflow profile. More profiles can be provided by using comma separated string (without whitespaces).
    :type profile: str
    :param cache_docker: Perform pipeline analysis and cache the detected docker images on the platform
    :type cache_docker: boolean
    :param nextflow_pipeline_params: Custom Nextflow pipeline parameters
    :type nextflow_pipeline_params: string
    Creates Nextflow dxapp.json from the Nextflow dxapp.json template
    """

    if custom_inputs is None:
        custom_inputs = []
    with open(os.path.join(str(get_template_dir()), 'dxapp.json'), 'r') as f:
        dxapp = json.load(f)
    dxapp["inputSpec"] = custom_inputs + dxapp["inputSpec"]
    dxapp["runSpec"]["file"] = get_source_file_name()

    # By default, title and summary will be set to the pipeline name
    name = get_resources_dir_name(resources_dir)
    if name is None or name == "":
        name = "Nextflow pipeline"
    dxapp["name"] = name
    dxapp["title"] = name
    dxapp["summary"] = name
    dxapp["regionalOptions"] = get_regional_options(region, resources_dir, profile, cache_docker, nextflow_pipeline_params)

    # Record dxpy version used for this Nextflow build
    dxapp["details"]["dxpyBuildVersion"] = TOOLKIT_VERSION
    if os.environ.get("DX_JOB_ID") is None or not is_importer_job():
        dxapp["details"]["repository"] = "local"
    return dxapp


def get_nextflow_src(custom_inputs=None, profile=None, resources_dir=None):
    """
    :param custom_inputs: Custom inputs (as configured in nextflow_schema.json) that will be used in created runtime configuration and runtime params argument
    :type custom_inputs: list
    :param profile: Custom Nextflow profile to be used when running a Nextflow pipeline, for more information visit https://www.nextflow.io/docs/latest/config.html#config-profiles
    :type profile: string
    :param resources_dir: Directory with all source files needed to build an applet. Can be an absolute or a relative path.
    :type resources_dir: str or Path
    :returns: String containing the whole source file of an applet.
    :rtype: string

    Creates Nextflow source file from the Nextflow source file template
    """
    if custom_inputs is None:
        custom_inputs = []
    with open(os.path.join(str(get_template_dir()), get_source_file_name()), 'r') as f:
        src = f.read()

    exclude_input_download = ""
    applet_runtime_params = ""
    for i in custom_inputs:
        value = "${%s}" % (i['name'])
        if i.get("class") == "file":
            value = "dx://${DX_WORKSPACE_ID}:/$(echo ${%s} | jq .[$dnanexus_link] -r | xargs -I {} dx describe {} --json | jq -r .name)" % \
                    i['name']
            exclude_input_download += "--except {} ".format(i['name'])

        # applet_runtime_inputs variable is initialized in the nextflow.sh script template
        applet_runtime_params = applet_runtime_params + '''
        if [ -n "${}" ]; then
            applet_runtime_inputs+=(--{} "{}")
        fi
        '''.format(i['name'], i['name'], value)

    profile_arg = "-profile {}".format(profile) if profile else ""
    src = src.replace("@@APPLET_RUNTIME_PARAMS@@", applet_runtime_params)
    src = src.replace("@@PROFILE_ARG@@", profile_arg)
    src = src.replace("@@EXCLUDE_INPUT_DOWNLOAD@@", exclude_input_download)
    src = src.replace("@@DXPY_BUILD_VERSION@@", TOOLKIT_VERSION)
    if USING_PYTHON2:
        src = src.replace("@@RESOURCES_SUBPATH@@",
                          get_resources_subpath(resources_dir).encode(sys_encoding))
    else:
        src = src.replace("@@RESOURCES_SUBPATH@@",
                          get_resources_subpath(resources_dir))

    return src
