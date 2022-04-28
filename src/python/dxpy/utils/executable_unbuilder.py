# Copyright (C) 2014-2016 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

'''
App, Applet, and Workflow Unbuilder
+++++++++++++

Contains utility methods for "decompiling" an app, applet, or workflow
from the platform and generating an equivalent source directory.

'''

from __future__ import print_function, unicode_literals, division, absolute_import

import collections
import json
import os
import sys
import tarfile
import shutil

import dxpy
from .. import get_handler, download_dxfile
from ..compat import open
from ..exceptions import err_exit, DXError
from .pretty_print import flatten_json_array


def _recursive_cleanup(foo):
    """
    Aggressively cleans up things that look empty.
    """
    if isinstance(foo, dict):
        for (key, val) in list(foo.items()):
            if isinstance(val, dict):
                _recursive_cleanup(val)
            if val == "" or val == [] or val == {}:
                del foo[key]


def _write_json_file(filename, json_content):
    with open(filename, "w") as f:
        f.write(flatten_json_array(json.dumps(json_content, indent=2, separators=(',', ': ')), "patterns"))
        f.write('\n')


def _write_simple_file(filename, content):
    with open(filename, "w") as f:
        f.write(content)


def _dump_workflow(workflow_obj, describe_output={}):
    dxworkflow_json_keys = ['name', 'title', 'summary', 'dxapi', 'version',
                            'outputFolder']
    dxworkflow_json_stage_keys = ['id', 'name', 'executable', 'folder', 'input',
                                  'executionPolicy', 'systemRequirements']

    dxworkflow_json = collections.OrderedDict()
    for key in dxworkflow_json_keys:
        if key in describe_output and describe_output[key]:
            dxworkflow_json[key] = describe_output[key]

    for key in ('inputs', 'outputs'):
        if key in describe_output and describe_output[key] is not None:
            dxworkflow_json[key] = describe_output[key]
    stages = describe_output.get("stages", ())
    new_stages = []
    for stage in stages:
        new_stage = collections.OrderedDict()
        for key in dxworkflow_json_stage_keys:
            if key in stage and stage[key]:
                new_stage[key] = stage[key]
        new_stages.append(new_stage)
    dxworkflow_json["stages"] = new_stages

    # Create dxworkflow.json, Readme.md files
    _write_json_file("dxworkflow.json", dxworkflow_json)
    readme = describe_output.get("description", "")
    if readme:
        _write_simple_file("Readme.md", readme)


def _dump_app_or_applet(executable, omit_resources=False, describe_output={}):
    info = executable.get()

    if info["runSpec"]["interpreter"] == "bash":
        suffix = "sh"
    elif info["runSpec"]["interpreter"].startswith("python"):
        suffix = "py"
    else:
        print("Sorry, I don\'t know how to get executables with interpreter {}.\n".format(
            info["runSpec"]["interpreter"]), file=sys.stderr)
        sys.exit(1)

    # Entry point script
    script = "src/code.{}".format(suffix)
    os.mkdir("src")
    with open(script, "w") as f:
        f.write(info["runSpec"]["code"])

    def make_cluster_bootstrap_script_file(region, entry_point, code, suffix):
        """
        Writes the string `code` into a file at the relative path
        "src/<region>_<entry_point>_clusterBootstrap.<suffix>"
        """
        script_name = "src/{}_{}_clusterBootstrap.{}".format(region, entry_point, suffix)
        with open(script_name, "w") as f:
            f.write(code)
        return script_name
    
    # Get regions where the user's billTo are permitted
    try:
        bill_to = dxpy.api.user_describe(dxpy.whoami())['billTo']
        permitted_regions = set(dxpy.DXHTTPRequest('/' + bill_to + '/describe', {}).get("permittedRegions"))
    except DXError:
        print("Failed to get permitted regions where {} can perform billable activities.\n".format(bill_to), file=sys.stderr)
        sys.exit(1)    
    
    # when applet/app is built, the runSpec is initialized with fields "interpreter" and "bundledDependsByRegion"
    # even when we don't have any bundledDepends in dxapp.json
    enabled_regions = set(info["runSpec"]["bundledDependsByRegion"].keys())
    if not enabled_regions.issubset(permitted_regions):
        print("Region(s) {} are not among the permitted regions of {}. Resources from these regions will not be available.".format(
            ", ".join(enabled_regions.difference(permitted_regions)), bill_to), file=sys.stderr )
    # Update enabled regions
    enabled_regions.intersection_update(permitted_regions)
    
    # Start downloading when not omitting resources
    deps_downloaded = set()
    if not omit_resources:
        download_completed = False
        
        # Check if at least one region is enabled 
        if not enabled_regions:
            raise DXError(
                "Cannot download resources of the requested executable {} since it is not available in any of the billable regions. "
                "You can use the --omit-resources flag to skip downloading the resources. ".format(info["name"]))
        
        # Pick a source region. The current selected region is preferred
        try:
            current_region = dxpy.api.project_describe(dxpy.WORKSPACE_ID, input_params={"fields": {"region": True}})["region"]
        except:
            current_region = None

        if current_region in enabled_regions:
            source_region  = current_region
            print("Trying to download resources from the current region {}...".format(source_region), file=sys.stderr)
        else:
            source_region = list(enabled_regions)[0]
            print("Trying to download resources from one of the enabled region {}...".format(source_region), file=sys.stderr)

        # When an app(let) is built the following dependencies are added as bundledDepends:
        # 1. bundledDepends explicitly specified in the dxapp.json
        # 2. resources (contents of resources directory added as bundledDepends)
        # 3. assetDepends in the dxapp.json (with their record IDs translated into file IDs)
        #
        # To get the resources, we will tranverse the bundleDepends list in the source region and do the following:
        # - If an file ID refers to an AssetBundle tarball (a file with the "AssetBundle" property), 
        #   skip downloading the file, and keep this file ID as a bundledDepends in the final dxapp.json
        # - Otherwise, download the file and remove this ID from the bundledDepends list in the final dxapp.json

        def untar_strip_leading_slash(tarfname, path):
            with tarfile.open(tarfname) as t:
                for m in t.getmembers():
                    if m.name.startswith("/"):
                        m.name = m.name[1:]
                    t.extract(m, path)
            t.close()

        created_resources_directory = False
        # Download resources from the source region      
        for dep in info["runSpec"]["bundledDependsByRegion"][source_region]:
            try: 
                file_handle = get_handler(dep["id"])
                handler_id = file_handle.get_id()
                # if dep is not a file (record etc.), check the next dep
                if not isinstance(file_handle, dxpy.DXFile):
                    continue
                
                # check if the file is an asset dependency
                # if so, skip downloading
                if file_handle.get_properties().get("AssetBundle"):
                    continue

                # if the file is a bundled dependency, try downloading it
                if not created_resources_directory:
                    os.mkdir("resources")
                    created_resources_directory = True
                
                fname = "resources/{}.tar.gz" .format(handler_id)
                download_dxfile(handler_id, fname)
                print("Unpacking resource {}".format(dep.get("name")), file=sys.stderr)

                untar_strip_leading_slash(fname, "resources")
                os.unlink(fname)
                # add dep name to deps_downloaded set
                deps_downloaded.add(dep.get("name"))
                
            except DXError:
                print("Failed to download {} from region {}.".format(handler_id, source_region),
                        file=sys.stderr)
                # clean up deps already downloaded and quit downloading
                deps_downloaded.clear()
                shutil.rmtree("resources")
                break
        # if all deps have been checked without an error, mark downloading as completed
        else: # for loop finished with no break
            download_completed = True                    
    
        # Check if downloading is completed in one of the enabled regions
        # if so, files in deps_downloaded will not shown in dxapp.json
        # if not, deps_downloaded is an empty set. So ID of all deps will be in dxapp.json
        if not download_completed:
            print("Downloading resources from region {} failed. "
                "Please try downloading with their IDs in dxapp.json, "
                "or skip downloading resources entirely by using the --omit-resources flag.".format(source_region), file=sys.stderr)

    # TODO: if output directory is not the same as executable name we
    # should print a warning and/or offer to rewrite the "name"
    # field in the 'dxapp.json'
    dxapp_json = collections.OrderedDict()
    all_keys = executable._get_required_keys() + executable._get_optional_keys()
    for key in all_keys:
        if key in executable._get_describe_output_keys() and key in describe_output:
            dxapp_json[key] = describe_output[key]
        if key in info:
            dxapp_json[key] = info[key]
    if info.get("hidden", False):
        dxapp_json["hidden"] = True

    # TODO: inputSpec and outputSpec elements should have their keys
    # printed in a sensible (or at least consistent) order too

    # Un-inline code
    del dxapp_json["runSpec"]["code"]
    dxapp_json["runSpec"]["file"] = script

    # Ordering input/output spec keys
    ordered_spec_keys = ("name", "label", "help", "class", "type", "patterns", "optional", "default", "choices",
                         "suggestions", "group")
    for spec_key in "inputSpec", "outputSpec":
        if spec_key not in dxapp_json.keys():
            continue
        for i, spec in enumerate(dxapp_json[spec_key]):
            ordered_spec = collections.OrderedDict()
            # Adding keys, for which the ordering is defined
            for key in ordered_spec_keys:
                if key in spec.keys():
                    ordered_spec[key] = spec[key]
            # Adding the rest of the keys
            for key in spec.keys():
                if key not in ordered_spec_keys:
                    ordered_spec[key] = spec[key]
            dxapp_json[spec_key][i] = ordered_spec

    # Remove dx-toolkit from execDepends
    dx_toolkit = {"name": "dx-toolkit", "package_manager": "apt"}
    if dx_toolkit in dxapp_json["runSpec"].get("execDepends", ()):
        dxapp_json["runSpec"]["execDepends"].remove(dx_toolkit)

    # "dx build" parses the "regionalOptions" key from dxapp.json into the
    # "runSpec.systemRequirements" field of applet/new.
    # "dx get" should parse the "systemRequirementsByRegion" field from
    # the response of /app-x/get or /applet-x/get into the "regionalOptions"
    # key in dxapp.json.
    dxapp_json["regionalOptions"] = {}
    for region in enabled_regions:
        dxapp_json["regionalOptions"][region] = {}
        if "systemRequirementsByRegion" in dxapp_json['runSpec']:
            region_sys_reqs = dxapp_json['runSpec']['systemRequirementsByRegion'][region]

            # handle cluster bootstrap scripts if any are present
            for entry_point in region_sys_reqs:
                try:
                    bootstrap_script = region_sys_reqs[entry_point]['clusterSpec']['bootstrapScript']
                    filename = make_cluster_bootstrap_script_file(region,
                                                                  entry_point,
                                                                  bootstrap_script,
                                                                  suffix)
                    region_sys_reqs[entry_point]['clusterSpec']['bootstrapScript'] = filename
                except KeyError:
                    # either no "clusterSpec" or no "bootstrapScript" within "clusterSpec"
                    continue

            dxapp_json["regionalOptions"][region]["systemRequirements"]=region_sys_reqs

        region_depends = dxapp_json["runSpec"]["bundledDependsByRegion"][region]
        region_bundle_depends = [d for d in region_depends if d["name"] not in deps_downloaded]
        if region_bundle_depends:
            dxapp_json["regionalOptions"][region]["bundledDepends"]=region_bundle_depends
            
    # Remove "bundledDependsByRegion" and "bundledDepends" field from "runSpec".
    # assetDepends and bundledDepends data are stored in regionalOptions instead.
    dxapp_json["runSpec"].pop("bundledDependsByRegion", None)
    dxapp_json["runSpec"].pop("bundledDepends", None)
    # systemRequirementsByRegion data is stored in regionalOptions,
    # systemRequirements is ignored
    dxapp_json["runSpec"].pop("systemRequirementsByRegion",None)
    dxapp_json["runSpec"].pop("systemRequirements",None)

    # Cleanup of empty elements. Be careful not to let this step
    # introduce any semantic changes to the app specification. For
    # example, an empty input (output) spec is not equivalent to a
    # missing input (output) spec.
    if 'runSpec' in dxapp_json:
        _recursive_cleanup(dxapp_json['runSpec'])
    if 'access' in dxapp_json:
        _recursive_cleanup(dxapp_json['access'])
    for key in executable._get_cleanup_keys():
        if key in dxapp_json and not dxapp_json[key]:
            del dxapp_json[key]

    readme = info.get("description", "")
    devnotes = info.get("developerNotes", "")

    # Write dxapp.json, Readme.md, and Readme.developer.md
    _write_json_file("dxapp.json", dxapp_json)
    if readme:
        _write_simple_file("Readme.md", readme)
    if devnotes:
        _write_simple_file("Readme.developer.md", devnotes)

def dump_executable(executable, destination_directory, omit_resources=False, describe_output={}):
    """
    Reconstitutes an app, applet, or a workflow into a directory that would
    create a functionally identical executable if "dx build" were run on it.
    destination_directory will be the root source directory for the
    executable.

    :param executable: executable, i.e. app, applet, or workflow, to be dumped
    :type executable: DXExecutable (either of: DXApp, DXApplet, DXWorkflow, DXGlobalWorkflow)
    :param destination_directory: an existing, empty, and writable directory
    :type destination_directory: str
    :param omit_resources: if True, executable's resources will not be downloaded
    :type omit_resources: boolean
    :param describe_output: output of a describe API call on the executable
    :type describe_output: dictionary
    """
    try:
        old_cwd = os.getcwd()
        os.chdir(destination_directory)
        if isinstance(executable, dxpy.DXWorkflow):
            _dump_workflow(executable, describe_output)
        elif isinstance(executable, dxpy.DXGlobalWorkflow):
            # Add inputs, outputs, stages. These fields contain region-specific values
            # e.g. files or applets, that's why:
            # * if the workflow is global, we will unpack the underlying workflow
            #   from the region of the current project context
            # * if this is a regular, project-based workflow, we will just use
            #   its description (the describe_output that we already have)
            # Underlying workflows are workflows stored in resource containers
            # of the global workflow (one per each region the global workflow is
            # enabled in). #TODO: add a link to documentation.
            current_project = dxpy.WORKSPACE_ID
            if not current_project:
                raise DXError(
                    'A project needs to be selected to "dx get" a global workflow. You can use "dx select" to select a project')
            region = dxpy.api.project_describe(current_project,
                                               input_params={"fields": {"region": True}})["region"]
            describe_output = executable.append_underlying_workflow_desc(describe_output, region)
            _dump_workflow(executable, describe_output)
        else:
            _dump_app_or_applet(executable, omit_resources, describe_output)
    except:
        err_exit()
    finally:
        os.chdir(old_cwd)
