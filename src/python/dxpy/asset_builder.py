# Copyright (C) 2016 DNAnexus, Inc.
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

from __future__ import print_function, unicode_literals, division, absolute_import

import os
import sys
import subprocess
import tempfile
import shutil
import json

from .compat import open
from .exceptions import err_exit
from .utils import json_load_raise_on_duplicates
from .utils.resolver import is_container_id, resolve_path
from .cli import try_call
import dxpy

ASSET_BUILDER_PRECISE = "app-create_asset_precise"
ASSET_BUILDER_TRUSTY = "app-create_asset_trusty"
ASSET_BUILDER_XENIAL = "app-create_asset_xenial"
ASSET_BUILDER_XENIAL_V1 = "app-create_asset_xenial_v1"
ASSET_BUILDER_FOCAL = "app-create_asset_focal"



class AssetBuilderException(Exception):
    """
    This exception is raised by the methods in this module
    when asset building fails.
    """
    pass


def parse_asset_spec(src_dir):
    if not os.path.isdir(src_dir):
        err_exit(src_dir + " is not a valid directory.")
    if not os.path.exists(os.path.join(src_dir, "dxasset.json")):
        raise AssetBuilderException("'" + src_dir + "' is not a valid DNAnexus asset source directory." +
                                    " It does not contain a 'dxasset.json' file.")
    with open(os.path.join(src_dir, "dxasset.json")) as asset_desc:
        try:
            return json_load_raise_on_duplicates(asset_desc)
        except Exception as e:
            raise AssetBuilderException("Could not parse dxasset.json file as JSON: " + str(e.args))


def validate_conf(asset_conf):
    """
    Validates the contents of the conf file and makes sure that the required information
    is provided.
        {
            "name": "asset_library_name",
            "title": "A human readable name",
            "description": " A detailed description abput the asset",
            "version": "0.0.1",
            "runSpecVersion": "1",
            "release": "16.04",
            "distribution": "Ubuntu"
            "execDepends":
                        [
                            {"name": "samtools", "package_manager": "apt"},
                            {"name": "bamtools"},
                            {"name": "bio", "package_manager": "gem", "version": "1.4.3"},
                            {"name": "pysam","package_manager": "pip", "version": "0.7.4"},
                            {"name": "Bio::SeqIO", "package_manager": "cpan", "version": "1.006924"}
                        ]
        }
    """
    if 'name' not in asset_conf:
        raise AssetBuilderException('The asset configuration does not contain the required field "name".')

    # Validate runSpec
    if 'release' not in asset_conf or asset_conf['release'] not in ["20.04", "16.04", "14.04", "12.04"]:
        raise AssetBuilderException('The "release" field value should be either "20.04", "16.04", "14.04" (DEPRECATED), or "12.04" (DEPRECATED)')
    if 'runSpecVersion' in asset_conf:
        if asset_conf['runSpecVersion'] not in ["0", "1"]:
            raise AssetBuilderException('The "runSpecVersion" field should be either "0", or "1"')
        if (asset_conf['runSpecVersion'] == "1" and asset_conf['release'] != "16.04"):
            raise AssetBuilderException('The "runSpecVersion" field can only be "1" if "release" is "16.04"')
    else:
        asset_conf['runSpecVersion'] = "0"
    if 'distribution' in asset_conf:
        if asset_conf['distribution'] != "Ubuntu":
            raise AssetBuilderException('The distribution may only take the value "Ubuntu".')
    else:
        asset_conf['distribution'] = "Ubuntu"

    if 'version' not in asset_conf:
        raise AssetBuilderException('The asset configuration does not contain the required field "version". ')
    if 'title' not in asset_conf:
        raise AssetBuilderException('The asset configuration does not contain the required field "title". ')
    if 'description' not in asset_conf:
        raise AssetBuilderException('The asset configuration does not contain the required field "description".')



def dx_upload(file_name, dest_project, target_folder, json_out):
    try:
        maybe_progress_kwargs = {} if json_out else dict(show_progress=True)
        remote_file = dxpy.upload_local_file(file_name,
                                             project=dest_project,
                                             folder=target_folder,
                                             wait_on_close=True,
                                             **maybe_progress_kwargs)
        return remote_file
    except:
        print("Failed to upload the file " + file_name, file=sys.stderr)
        raise


def get_asset_make(src_dir, dest_folder, target_folder, json_out):
    if os.path.exists(os.path.join(src_dir, "Makefile")):
        return dx_upload(os.path.join(src_dir, "Makefile"), dest_folder, target_folder, json_out)
    elif os.path.exists(os.path.join(src_dir, "makefile")):
        return dx_upload(os.path.join(src_dir, "makefile"), dest_folder, target_folder, json_out)


def parse_destination(dest_str):
    """
    Parses dest_str, which is (roughly) of the form
    PROJECT:/FOLDER/NAME, and returns a tuple (project, folder, name)
    """
    # Interpret strings of form "project-XXXX" (no colon) as project. If
    # we pass these through to resolve_path they would get interpreted
    # as folder names...
    if is_container_id(dest_str):
        return (dest_str, None, None)

    # ...otherwise, defer to resolver.resolve_path. This handles the
    # following forms:
    #
    # /FOLDER/
    # /ENTITYNAME
    # /FOLDER/ENTITYNAME
    # [PROJECT]:
    # [PROJECT]:/FOLDER/
    # [PROJECT]:/ENTITYNAME
    # [PROJECT]:/FOLDER/ENTITYNAME
    return try_call(resolve_path, dest_str)


def get_asset_tarball(asset_name, src_dir, dest_project, dest_folder, json_out):
    """
    If the src_dir contains a "resources" directory its contents are archived and
    the archived file is uploaded to the platform
    """
    if os.path.isdir(os.path.join(src_dir, "resources")):
        temp_dir = tempfile.mkdtemp()
        try:
            resource_file = os.path.join(temp_dir, asset_name + "_resources.tar.gz")
            cmd = ["tar", "-czf", resource_file, "-C", os.path.join(src_dir, "resources"), "."]
            subprocess.check_call(cmd)
            file_id = dx_upload(resource_file, dest_project, dest_folder, json_out)
            return file_id
        finally:
            shutil.rmtree(temp_dir)


def build_asset(args):
    if args.src_dir is None:
        args.src_dir = os.getcwd()

    dest_project_name = None
    dest_folder_name = None
    dest_asset_name = None
    make_file = None
    asset_file = None
    conf_file = None

    try:
        asset_conf = parse_asset_spec(args.src_dir)
        validate_conf(asset_conf)
        asset_conf_file = os.path.join(args.src_dir, "dxasset.json")

        dxpy.api.system_whoami()
        dest_project_name, dest_folder_name, dest_asset_name = parse_destination(args.destination)
        if dest_project_name is None:
            raise AssetBuilderException("Can't build an asset without specifying a destination project; \
            please use the -d/--destination flag to explicitly specify a project")
        if dest_asset_name is None:
            dest_asset_name = asset_conf['name']

        # If dx build_asset is launched form a job, set json flag to True to avoid watching the job log
        if dxpy.JOB_ID:
            args.json = True

        if not args.json:
            print("Uploading input files for the AssetBuilder", file=sys.stderr)

        conf_file = dx_upload(asset_conf_file, dest_project_name, dest_folder_name, args.json)
        make_file = get_asset_make(args.src_dir, dest_project_name, dest_folder_name, args.json)
        asset_file = get_asset_tarball(asset_conf['name'], args.src_dir, dest_project_name,
                                       dest_folder_name, args.json)

        input_hash = {"conf_json": dxpy.dxlink(conf_file)}
        if asset_file:
            input_hash["custom_asset"] = dxpy.dxlink(asset_file)
        if make_file:
            input_hash["asset_makefile"] = dxpy.dxlink(make_file)

        builder_run_options = {
            "name": dest_asset_name,
            "input": input_hash
            }

        if args.priority is not None:
            builder_run_options["priority"] = args.priority

        # Add the default destination project to app run options, if it is not run from a job
        if not dxpy.JOB_ID:
            builder_run_options["project"] = dest_project_name
        if 'instanceType' in asset_conf:
            builder_run_options["systemRequirements"] = {"*": {"instanceType": asset_conf["instanceType"]}}
        if dest_folder_name:
            builder_run_options["folder"] = dest_folder_name
        if asset_conf['release'] == "12.04":
            app_run_result = dxpy.api.app_run(ASSET_BUILDER_PRECISE, input_params=builder_run_options)
        elif asset_conf['release'] == "14.04":
            app_run_result = dxpy.api.app_run(ASSET_BUILDER_TRUSTY, input_params=builder_run_options)
        elif asset_conf['release'] == "16.04" and asset_conf['runSpecVersion'] == '1':
            app_run_result = dxpy.api.app_run(ASSET_BUILDER_XENIAL_V1, input_params=builder_run_options)
        elif asset_conf['release'] == "16.04":
            app_run_result = dxpy.api.app_run(ASSET_BUILDER_XENIAL, input_params=builder_run_options)
        elif asset_conf['release'] == "20.04":
            app_run_result = dxpy.api.app_run(ASSET_BUILDER_FOCAL, input_params=builder_run_options)

        job_id = app_run_result["id"]

        if not args.json:
            print("\nStarted job '" + str(job_id) + "' to build the asset bundle.\n", file=sys.stderr)
            if args.watch:
                try:
                    subprocess.check_call(["dx", "watch", job_id])
                except subprocess.CalledProcessError as e:
                    if e.returncode == 3:
                        # Some kind of failure to build the asset. The reason
                        # for the failure is probably self-evident from the
                        # job log (and if it's not, the CalledProcessError
                        # is not informative anyway), so just propagate the
                        # return code without additional remarks.
                        sys.exit(3)
                    else:
                        raise e

        dxpy.DXJob(job_id).wait_on_done(interval=1)
        asset_id, _ = dxpy.get_dxlink_ids(dxpy.api.job_describe(job_id)['output']['asset_bundle'])

        if args.json:
            print(json.dumps({"id": asset_id}))
        else:
            print("\nAsset bundle '" + asset_id +
                  "' is built and can now be used in your app/applet's dxapp.json\n", file=sys.stderr)
    except Exception as de:
        print(de.__class__.__name__ + ": " + str(de), file=sys.stderr)
        sys.exit(1)
    finally:
        if conf_file:
            try:
                conf_file.remove()
            except:
                pass
        if make_file:
            try:
                make_file.remove()
            except:
                pass
        if asset_file:
            try:
                asset_file.remove()
            except:
                pass
