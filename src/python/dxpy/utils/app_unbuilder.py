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
App Unbuilder
+++++++++++++

Contains utility methods for "decompiling" an applet from the platform
and generating an equivalent applet source directory.

'''

from __future__ import print_function, unicode_literals, division, absolute_import

import collections
import json
import os
import subprocess
import sys

from .. import get_handler, download_dxfile
from ..compat import open
from ..exceptions import err_exit
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

def dump_executable(executable, destination_directory, omit_resources=False, describe_output=[]):
    """
    Reconstitutes executable into a directory that would create a
    functionally identical executable if "dx build" were run on it.
    destination_directory will be the root source directory for the
    applet.

    :param executable: executable, i.e. app or applet,  to be dumped
    :type executable: DXExecutable (only DXApp or DXApplet now)
    :param destination_directory: an existing, empty, and writable directory
    :type destination_directory: str
    """

    old_cwd = os.getcwd()
    os.chdir(destination_directory)

    try:
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

        # resources/ directory
        deps_to_remove = []
        created_resources_directory = False
        if not omit_resources:
            for dep in info["runSpec"]["bundledDepends"]:
                handler = get_handler(dep["id"])
                if handler.__class__.__name__ == "DXFile":
                    if not created_resources_directory:
                        os.mkdir("resources")
                        created_resources_directory = True
                    handler_id = handler.get_id()
                    fname = "resources/%s.tar.gz" % (handler_id)
                    download_dxfile(handler_id, fname)
                    print("Unpacking resources", file=sys.stderr)
                    subprocess.check_call(["tar", "-C", "resources", "-zxf", fname], shell=False)
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
        if dx_toolkit in dxapp_json["runSpec"]["execDepends"]:
            dxapp_json["runSpec"]["execDepends"].remove(dx_toolkit)

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
        with open("dxapp.json", "w") as f:
            f.write(flatten_json_array(json.dumps(dxapp_json, indent=2, separators=(',', ': ')), "patterns"))
            f.write('\n')
        if readme:
            with open("Readme.md", "w") as f:
                f.write(readme)
        if devnotes:
            with open("Readme.developer.md", "w") as f:
                f.write(devnotes)
    except:
        err_exit()
    finally:
        os.chdir(old_cwd)
