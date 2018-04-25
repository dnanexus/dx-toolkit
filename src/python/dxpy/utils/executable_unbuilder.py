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
        for (key, val) in foo.items():
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
    elif info["runSpec"]["interpreter"] == "python2.7":
        suffix = "py"
    else:
        print('Sorry, I don\'t know how to get executables with interpreter ' +
              info["runSpec"]["interpreter"] + '\n', file=sys.stderr)
        sys.exit(1)

    # Entry point script
    script = "src/code.%s" % (suffix,)
    os.mkdir("src")
    with open(script, "w") as f:
        f.write(info["runSpec"]["code"])

    def make_cluster_bootstrap_script_file(region, entry_point, code, suffix):
        """
        Writes the string `code` into a file at the relative path
        "src/<region>_<entry_point>_clusterBootstrap.<suffix>"
        """
        script_name = "src/%s_%s_clusterBootstrap.%s" % (region, entry_point, suffix)
        with open(script_name, "w") as f:
            f.write(code)
        return script_name

    # Get all the asset bundles
    asset_depends = []
    deps_to_remove = []

    # When an applet is built bundledDepends are added in the following order:
    # 1. bundledDepends explicitly specified in the dxapp.json
    # 2. resources (contents of resources directory added as bundledDepends)
    # 3. assetDepends (translated into bundledDepends)
    #
    # Therefore while translating bundledDepends to assetDepends, we are traversing the
    # list in reverse order and exiting when we can't find the "AssetBundle" property
    # with the tarball file.
    #
    # NOTE: If last item (and contiguous earlier items) of bundledDepends (#1 above) refers to an
    # AssetBundle tarball, those items will be converted to assetDepends.
    #
    # TODO: The bundledDepends should be annotated with another field called {"asset": true}
    # to distinguish it from non assets. It will be needed to annotate the bundleDepends,
    # when the wrapper record object is no more accessible.

    for dep in reversed(info["runSpec"]["bundledDepends"]):
        file_handle = get_handler(dep["id"])
        if isinstance(file_handle, dxpy.DXFile):
            asset_record_id = file_handle.get_properties().get("AssetBundle")
            asset_record = None
            if asset_record_id:
                asset_record = dxpy.DXRecord(asset_record_id)
                if asset_record:
                    try:
                        asset_depends.append({"name": asset_record.describe().get("name"),
                                              "project": asset_record.get_proj_id(),
                                              "folder": asset_record.describe().get("folder"),
                                              "version": asset_record.describe(fields={"properties": True}
                                                                               )["properties"]["version"]
                                              })
                        deps_to_remove.append(dep)
                    except DXError:
                        print("Describe failed on the assetDepends record object with ID - " +
                              asset_record_id + "\n", file=sys.stderr)
                        pass
            else:
                break
    # Reversing the order of the asset_depends[] so that original order is maintained
    asset_depends.reverse()
    # resources/ directory
    created_resources_directory = False
    if not omit_resources:
        for dep in info["runSpec"]["bundledDepends"]:
            if dep in deps_to_remove:
                continue
            handler = get_handler(dep["id"])
            if isinstance(handler, dxpy.DXFile):
                if not created_resources_directory:
                    os.mkdir("resources")
                    created_resources_directory = True
                handler_id = handler.get_id()
                fname = "resources/%s.tar.gz" % (handler_id)
                download_dxfile(handler_id, fname)
                print("Unpacking resources", file=sys.stderr)

                def untar_strip_leading_slash(tarfname, path):
                    t = tarfile.open(tarfname)
                    for m in t.getmembers():
                        if m.name.startswith("/"):
                            m.name = m.name[1:]
                        t.extract(m, path)
                    t.close()

                untar_strip_leading_slash(fname, "resources")
                os.unlink(fname)
                deps_to_remove.append(dep)

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

    # Remove resources from bundledDepends
    for dep in deps_to_remove:
        dxapp_json["runSpec"]["bundledDepends"].remove(dep)

    # Add assetDepends to dxapp.json
    if len(asset_depends) > 0:
        dxapp_json["runSpec"]["assetDepends"] = asset_depends

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

    # Remove "bundledDependsByRegion" field from "runSpec". This utility
    # will reconstruct the resources directory based on the
    # "bundledDepends" field, which should be equivalent to
    # "bundledDependsByRegion".
    dxapp_json["runSpec"].pop("bundledDependsByRegion", None)

    # "dx build" parses the "regionalOptions" key from dxapp.json into the
    # "runSpec.systemRequirements" field of applet/new.
    # "dx get" should parse the "systemRequirementsByRegion" field from
    # the response of /app-x/get or /applet-x/get into the "regionalOptions"
    # key in dxapp.json.
    if "systemRequirementsByRegion" in dxapp_json['runSpec']:
        dxapp_json["regionalOptions"] = {}
        for region in dxapp_json['runSpec']["systemRequirementsByRegion"]:
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

            dxapp_json["regionalOptions"][region] = \
                dict(systemRequirements=region_sys_reqs)

    # systemRequirementsByRegion data is stored in regionalOptions,
    # systemRequirements is ignored
    if 'systemRequirementsByRegion' in dxapp_json["runSpec"]:
        del dxapp_json["runSpec"]["systemRequirementsByRegion"]
    if 'systemRequirements' in dxapp_json["runSpec"]:
        del dxapp_json["runSpec"]["systemRequirements"]

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
