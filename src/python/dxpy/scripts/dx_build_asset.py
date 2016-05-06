#!/usr/bin/env python
#
# Copyright (C) 2013-2016 DNAnexus, Inc.
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

import logging
logging.basicConfig(level=logging.WARNING)
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.ERROR)

import os, sys, subprocess, argparse
import re

from ..compat import open, USING_PYTHON2, decode_command_line_args
from ..exceptions import err_exit
from ..utils.completer import LocalCompleter
from ..utils import json_load_raise_on_duplicates

decode_command_line_args()

parser = argparse.ArgumentParser(description="Builds a DNAnexus Asset.")

NAME_PATTERN = re.compile('^[a-zA-Z_][0-9a-zA-Z_]*$')

src_dir_action = parser.add_argument("src_dir", help="Asset source directory (default: current directory)", nargs='?')
src_dir_action.completer = LocalCompleter()

ASSET_BUILDER_APPLET_NAME_PRECISE = "create_asset_precise"
ASSET_BUILDER_APPLET_NAME_TRUSTY = "create_asset_trusty"

class AssetBuilderException(Exception):
    """
    This exception is raised by the methods in this module
    when asset building fails.
    """
    pass

def parse_asset_spec(src_dir):
    """
    Returns the parsed contents of dxasset.json.
    Raises either AssetBuilderException if this cannot be done.
    """
    if not os.path.isdir(src_dir):
        parser.error("%s is not a directory" % src_dir)
    if not os.path.exists(os.path.join(src_dir, "dxasset.json")):
        raise AssetBuilderException("Directory %s does not contain dxasset.json: not a valid DNAnexus asset source directory" % src_dir)
    with open(os.path.join(src_dir, "dxasset.json")) as asset_desc:
        try:
            return json_load_raise_on_duplicates(asset_desc)
        except Exception as e:
            raise AssetBuilderException("Could not parse dxasset.json file as JSON: " + e.message)

def validate_conf(asset_conf):
    """
    Validates the contents of the conf file and makes sure that the required information
    is provided.
    """
    if 'name' not in asset_conf:
        raise AssetBuilderException('The asset configuration does not contain the required field "name".')
    elif not NAME_PATTERN.match(asset_conf['name']):
        raise AssetBuilderException('The "name" filed in asset configuration may use only underscore "_", ASCII letters, and digits; and may not start with a digit.')
    if 'project' not in asset_conf:
        raise AssetBuilderException('The asset configuration does not contain the required field "project".')
    if 'ubuntuRelease' not in asset_conf:
        raise AssetBuilderException('The asset configuration does not contain the required field "ubuntuRelease". ')
    elif asset_conf['ubuntuRelease'] != '12.04' and asset_conf['ubuntuRelease'] != '14.04':
        raise AssetBuilderException('The "ubuntuRelease" field value should be either "12.04" or "14.04".')
    if 'version' not in asset_conf:
        raise AssetBuilderException('The asset configuration does not contain the required field "version". ')
    if 'title' not in asset_conf:
        raise AssetBuilderException('The asset configuration does not contain the required field "title". ')
    if 'description' not in asset_conf:
        raise AssetBuilderException('The asset configuration does not contain the required field "description".')

def dx_login(project_name):
    """
    Check if user has logged into the system using dx toolkit
    :return: boolean True is user has logged in else false

    """
    cmd = ["dx", "whoami"]
    try:
        cmd_out = subprocess.check_output(cmd)
    except subprocess.CalledProcessError as cp:
        raise AssetBuilderException("Failed to run the dx command: " + str(cmd))
    except OSError:
        raise AssetBuilderException("dx command is not found. Make sure that the dx toolkit is installed on this system.")

    try:
        cmd_out = subprocess.check_output(["dx", "select", project_name])
    except subprocess.CalledProcessError as cp:
        raise AssetBuilderException("Failed to select the project: " + project_name)
    except OSError:
        raise AssetBuilderException("dx command is not found. Make sure that the dx toolkit is installed on this system.")

def dx_upload(file_name):
    """ Uploads a file to the platform

    :return: string File id received after uploading a file to the platform
    """
    cmd = ["dx", "upload", file_name, "--brief"]
    try:
        cmd_out = subprocess.check_output(cmd)
        return cmd_out.strip()
    except subprocess.CalledProcessError as cp:
        raise AssetBuilderException("Failed to run the dx command: " + str(cmd))
    except OSError:
        raise AssetBuilderException("dx command is not found. Make sure that the dx toolkit is installed on this system.")

def gzip_asset(asset_name, asset_path):
    """ This method creates a asset_name.tar.gz file in the current directory containing
    all the files in the asset_path directory.
    :param asset_name: string, representing the asset name
    :param asset_path: string, representing the asset root path
    """
    cmd = ["tar", "-czf", asset_name, "-C", asset_path, "."]
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as cp:
        raise AssetBuilderException("Failed to run the command: " + str(cmd))
    except OSError:
        raise AssetBuilderException("Failed to run the tar command: " + str(cmd))

def get_asset_make(src_dir):
    """
    If the asset src_dir contains a make file it is uploaded to the platform and its id is returned
    """

    if os.path.exists(os.path.join(src_dir, "Makefile")):
        return dx_upload(os.path.join(src_dir, "Makefile"))
    elif os.path.exists(os.path.join(src_dir, "makefile")):
        return dx_upload(os.path.join(src_dir, "makefile"))
    else:
        return ""

def get_asset_tarball(asset_name, src_dir):
    """
    If the src_dir contains a "resources" directory its contents are archived and
    the archived file is uploaded to the platform
    """
    if os.path.isdir(os.path.join(src_dir, "resources")):
        resource_file = os.path.join(src_dir, asset_name + "_resources.tar.gz")
        gzip_asset(resource_file, os.path.join(src_dir, "resources"))
        file_id = dx_upload(resource_file)
        subprocess.check_call(["rm", "-f", resource_file])
        return file_id
    else:
        return ""

def dx_run_app(json_file_id, make_file_id, asset_file_id, app_name):
    """ Runs the applet that will bundle the libraries
    :return: Job ID
    """
    json_arg = "conf_json=" + json_file_id
    asset_arg = "custom_asset=" + asset_file_id
    make_arg = "asset_makefile=" + make_file_id
    if len(make_file_id) > 0 and len(asset_file_id) > 0:
        cmd = ["dx", "run", "--yes", "--brief", app_name, "-i", json_arg, "-i", make_arg, "-i", asset_arg]
    elif len(asset_file_id) > 0:
        cmd = ["dx", "run", "--yes", "--brief", app_name, "-i", json_arg, "-i", asset_arg]
    elif len(make_file_id) > 0:
        cmd = ["dx", "run", "--yes", "--brief", app_name, "-i", json_arg, "-i", make_arg]
    else:
        cmd = ["dx", "run", "--yes", "--brief", app_name, "-i", json_arg]
    try:
        job_out = subprocess.check_output(cmd)
        return job_out.strip()
    except subprocess.CalledProcessError as cp:
        raise AssetBuilderException("Failed to run the command: " + str(cmd) + cp.message)
    except OSError:
        raise AssetBuilderException("Failed to run the tar command: " + str(cmd))

def main(**kwargs):
    """
    Entry point for dx build_asset
    """
    if len(kwargs) == 0:
        args = parser.parse_args()
    else:
        args = parser.parse_args(**kwargs)

    if args.src_dir is None:
        args.src_dir = os.getcwd()
        if USING_PYTHON2:
            args.src_dir = args.src_dir.decode(sys.getfilesystemencoding())
    # check if user has logged in to the platform using dx toolkit
    try:
        asset_conf = parse_asset_spec(args.src_dir)
        validate_conf(asset_conf)
        asset_conf_file = os.path.join(args.src_dir, "dxasset.json")

        dx_login(asset_conf['project'])
        conf_file_id = dx_upload(asset_conf_file)
        make_file_id = get_asset_make(args.src_dir)
        asset_file_id = get_asset_tarball(asset_conf['name'], args.src_dir)

        # run the applet and pass the file ids
        if asset_conf['ubuntuRelease'] == "12.04":
            job_id = dx_run_app(conf_file_id, make_file_id, asset_file_id, ASSET_BUILDER_APPLET_NAME_PRECISE)
        elif asset_conf['ubuntuRelease'] == "14.04":
            job_id = dx_run_app(conf_file_id, make_file_id, asset_file_id, ASSET_BUILDER_APPLET_NAME_TRUSTY)

        print("AssetBuilder launched " + str(job_id)  + " on the platform to bundle your required libraries.")
    except Exception as de:
        print(de.__class__.__name__ + ": " + str(de), file=sys.stderr)
        sys.exit(1)
    except:
        err_exit(1)

if __name__ == '__main__':
    main()
