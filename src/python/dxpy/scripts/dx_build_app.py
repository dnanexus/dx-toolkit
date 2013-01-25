#!/usr/bin/env python
#
# Copyright (C) 2013 DNAnexus, Inc.
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

import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.ERROR)

import os, sys, json, subprocess, argparse
import shutil
import tempfile
from datetime import datetime
import dxpy, dxpy.app_builder

from dxpy.utils.resolver import resolve_path, is_container_id

parser = argparse.ArgumentParser(description="Uploads a DNAnexus App.")

# COMMON OPTIONS
parser.add_argument("src_dir", help="App or applet source directory (default: current directory)", nargs='?')

parser.set_defaults(mode="app")
parser.add_argument("--create-app", help=argparse.SUPPRESS, action="store_const", dest="mode", const="app")
parser.add_argument("--create-applet", help=argparse.SUPPRESS, action="store_const", dest="mode", const="applet")
# TODO: remove -p/--destination-project and deprecation message below.
parser.add_argument("-p", "--destination-project", help="(Deprecated) Insert the applet into the project with the specified project ID.", default=None)
parser.add_argument("-d", "--destination", help="Specifies the destination project, destination folder, and/or name for the applet, in the form [PROJECT_NAME_OR_ID:][/[FOLDER/][NAME]]. Overrides the project, folder, and name fields of the dxapp.json, if they were supplied.", default=None)

parser.set_defaults(use_temp_build_project=True)
parser.add_argument("--no-temp-build-project", help="When building an app, build its applet in the current project instead of a temporary project", action="store_false", dest="use_temp_build_project")

# --[no-]publish
parser.set_defaults(publish=False)
parser.add_argument("--publish", help="Publish the resulting app and make it the default.", action="store_true", dest="publish")
parser.add_argument("--no-publish", help=argparse.SUPPRESS, action="store_false", dest="publish")

# --[no-]remote
parser.set_defaults(remote=False)
parser.add_argument("--remote", help="Build the app remotely.", action="store_true", dest="remote")
parser.add_argument("--no-remote", help=argparse.SUPPRESS, action="store_false", dest="remote")

parser.add_argument("-f", "--overwrite", help="If creating an applet, remove existing applets of the same name from the destination project.", action="store_true", default=False)
parser.add_argument("-v", "--version", help="Override the version number supplied in the manifest.", default=None, dest="version_override", metavar='VERSION')
parser.add_argument("-b", "--bill-to", help="Entity (of the form user-NAME or org-ORGNAME) to bill for the app.", default=None, dest="bill_to", metavar='USER_OR_ORG')

# --[no-]version-autonumbering
parser.set_defaults(version_autonumbering=True)
parser.add_argument("--version-autonumbering", help=argparse.SUPPRESS, action="store_true", dest="version_autonumbering")
parser.add_argument("--no-version-autonumbering", help="Only attempt to create the version number supplied in the manifest (that is, do not try to create an autonumbered version such as 1.2.3+git.ab1b1c1d if 1.2.3 already exists and is published).", action="store_false", dest="version_autonumbering")
# --[no-]update
parser.set_defaults(update=True)
parser.add_argument("--update", help=argparse.SUPPRESS, action="store_true", dest="update")
parser.add_argument("--no-update", help="Never update an existing unpublished app in place.", action="store_false", dest="update")
# --[no-]dx-toolkit-autodep
parser.set_defaults(dx_toolkit_autodep="auto")
parser.add_argument("--dx-toolkit-legacy-git-autodep", help="Auto-insert a dx-toolkit dependency on the latest git version (to be built from source at runtime)", action="store_const", dest="dx_toolkit_autodep", const="git")
parser.add_argument("--dx-toolkit-stable-autodep", help="Auto-insert a dx-toolkit dependency on the dx-toolkit (stable) apt package", action="store_const", dest="dx_toolkit_autodep", const="stable")
parser.add_argument("--dx-toolkit-beta-autodep", help="Auto-insert a dx-toolkit dependency on the dx-toolkit-beta apt package", action="store_const", dest="dx_toolkit_autodep", const="beta")
parser.add_argument("--dx-toolkit-unstable-autodep", help="Auto-insert a dx-toolkit dependency on the dx-toolkit-unstable apt package", action="store_const", dest="dx_toolkit_autodep", const="unstable")
parser.add_argument("--dx-toolkit-autodep", help=argparse.SUPPRESS, action="store_const", dest="dx_toolkit_autodep", const="beta")
parser.add_argument("--no-dx-toolkit-autodep", help="Do not auto-insert the dx-toolkit dependency if it's absent from the runSpec. See the documentation for more details.", action="store_false", dest="dx_toolkit_autodep")

# --[no-]dry-run
#
# The --dry-run flag can be used to see the applet spec that would be
# provided to /applet/new, for debugging purposes. However, the output
# would deviate from that of a real run in the following ways:
#
# * Any bundled resources are NOT uploaded and are not reflected in the
#   app(let) spec.
# * No temporary project is created (if building an app) and the
#   "project" field is not set in the app spec.
parser.set_defaults(dry_run=False)
parser.add_argument("--dry-run", help="Do not create an app(let), only show the spec of the applet that would have been created.", action="store_true", dest="dry_run")
parser.add_argument("--no-dry-run", help=argparse.SUPPRESS, action="store_false", dest="dry_run")


def get_timestamp_version_suffix():
    return "+build." + datetime.today().strftime('%Y%m%d.%H%M')

def get_version_suffix(src_dir):
    # If anything goes wrong, fall back to the date-based suffix.
    try:
        if os.path.exists(os.path.join(src_dir, ".git")):
            abbrev_sha1 = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=src_dir).strip()[:7]
            return "+git." + abbrev_sha1
    except:
        pass
    return get_timestamp_version_suffix()

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
    return resolve_path(dest_str)

def _build_app_remote(src_dir, publish=False, dx_toolkit_autodep="auto"):
    temp_dir = tempfile.mkdtemp()

    # We have to resolve the correct dx-toolkit dependency type here and
    # explicitly pass it into the interior call of dx-build-app, because
    # within the execution environment of tarball_app_builder,
    # APISERVER_HOST is set to the address of the proxy (a 10.x.x.x
    # address) and doesn't give us any information about whether we are
    # talking to preprod.
    if dx_toolkit_autodep == "auto":
        # "auto" (the default) means dx-toolkit (stable) on preprod and prod, and
        # dx-toolkit-beta on all other systems.
        if dxpy.APISERVER_HOST == "preprodapi.dnanexus.com" or dxpy.APISERVER_HOST == "api.dnanexus.com":
            dx_toolkit_autodep_flag = "--dx-toolkit-stable-autodep"
        else:
            dx_toolkit_autodep_flag = "--dx-toolkit-beta-autodep"
    elif dx_toolkit_autodep == "git":
        dx_toolkit_autodep_flag = "--dx-toolkit-legacy-git-autodep"
    elif dx_toolkit_autodep == "stable":
        dx_toolkit_autodep_flag = "--dx-toolkit-stable-autodep"
    elif dx_toolkit_autodep == "beta":
        dx_toolkit_autodep_flag = "--dx-toolkit-beta-autodep"
    elif dx_toolkit_autodep == "unstable":
        dx_toolkit_autodep_flag = "--dx-toolkit-unstable-autodep"
    elif dx_toolkit_autodep == False:
        dx_toolkit_autodep_flag = "--no-dx-toolkit-autodep"

    extra_flags = " ".join([dx_toolkit_autodep_flag])

    try:
        # Resolve relative paths and symlinks here so we have something
        # reasonable to write in the job name below.
        src_dir = os.path.realpath(src_dir)

        app_tarball_file = os.path.join(temp_dir, "app_tarball.tar.gz")
        # TODO: figure out if we can use --exclude-vcs here (conditional
        # on presence of GNU tar). This might require propagating the
        # --version directly to the interior dx-build-app since in
        # general that can depend on the git metadata.
        subprocess.check_call(["tar", "-czf", app_tarball_file, "."], cwd=src_dir)

        build_project_id = dxpy.api.projectNew({"name": "dx-build-app --remote temporary project"})["id"]

        try:
            dxpy.set_workspace_id(build_project_id)
            remote_file_id = dxpy.upload_local_file(app_tarball_file, media_type="application/gzip",
                                                    wait_on_close=True, show_progress=True)
            app_run_result = dxpy.api.appRun(
                "app-tarball_app_builder",
                input_params={
                    "name": "Remote build of %s" % (os.path.basename(src_dir),),
                    "input": {
                        "input_file": dxpy.dxlink(remote_file_id),
                        "publish": publish,
                        "extra_flags": extra_flags
                        },
                    "project": build_project_id
                    }
                )
            job_id = app_run_result["id"]
            print "Started builder job %s" % (job_id,)
            subprocess.check_call(["dx", "watch", job_id])

        finally:
            dxpy.api.projectDestroy(build_project_id)
    finally:
        shutil.rmtree(temp_dir)
    return


def main(**kwargs):

    if len(kwargs) == 0:
        args = parser.parse_args()
    else:
        args = parser.parse_args(kwargs)

    if dxpy.AUTH_HELPER is None:
        parser.error('Authentication required to build an executable on the platform; please run "dx login" first')

    if args.src_dir is None:
        args.src_dir = os.getcwd()

    if not os.path.isdir(args.src_dir):
        parser.error("%s is not a directory" % args.src_dir)

    if not os.path.exists(os.path.join(args.src_dir, "dxapp.json")):
        parser.error("Directory %s does not contain dxapp.json: not a valid DNAnexus app source directory" % args.src_dir)

    if args.remote:
        # To enable these, the tarball builder app needs to learn how to
        # pass these options through to the interior call of
        # dx_build_app.
        if args.mode == 'applet':
            parser.error('--remote can only be used to create apps')
        if args.version_override:
            parser.error('--remote cannot be combined with --version')
        if args.bill_to:
            parser.error('--remote cannot be combined with --bill-to')
        if not args.version_autonumbering:
            parser.error('--remote cannot be combined with --no-version-autonumbering')
        if not args.update:
            parser.error('--remote cannot be combined with --no-update')
        if args.dry_run:
            parser.error('--remote cannot be combined with --dry-run')
        return _build_app_remote(args.src_dir, publish=args.publish, dx_toolkit_autodep=args.dx_toolkit_autodep)

    working_project = None
    using_temp_project = False
    override_folder = None
    override_applet_name = None

    if args.destination_project:
        if args.destination:
            # We'll clobber destination below.
            parser.error("Can't supply both --destination and --destination-project. Just use --destination.")
        args.destination = args.destination_project
        print "*"
        print "* The -p/--destination-project flag has been deprecated. Instead please use"
        print "* -d=project-XXXX or --destination=project-XXXX, which does the same and more."
        print "*"

    if args.mode == "applet" and args.destination:
        working_project, override_folder, override_applet_name = parse_destination(args.destination)
    elif args.mode == "app" and args.use_temp_build_project and not args.dry_run:
        # Create a temp project
        working_project = dxpy.api.projectNew({"name": "Temporary build project for dx-build-app"})["id"]
        print >> sys.stderr, "Created temporary project %s to build in" % (working_project,)
        using_temp_project = True

    if args.mode == "applet" and working_project is None and dxpy.WORKSPACE_ID is None:
        parser.error("Can't create an applet without specifying a destination project; please use the -d/--destination flag to explicitly specify a project")

    try:
        with open(os.path.join(args.src_dir, "dxapp.json")) as app_desc:
            try:
                app_json = json.load(app_desc)
            except:
                parser.error("Could not parse dxapp.json file as valid JSON")

        if "buildOptions" in app_json:
            if app_json["buildOptions"].get("dx_toolkit_autodep") == False:
                args.dx_toolkit_autodep = False
            del app_json["buildOptions"]

        dxpy.app_builder.build(args.src_dir)

        bundled_resources = dxpy.app_builder.upload_resources(args.src_dir, project=working_project) if not args.dry_run else []

        try:
            if args.dx_toolkit_autodep == "auto":
                # "auto" (the default) means dx-toolkit (stable) on preprod and prod,
                # and dx-toolkit-beta on all other systems.
                if dxpy.APISERVER_HOST == "preprodapi.dnanexus.com" or dxpy.APISERVER_HOST == "api.dnanexus.com":
                    args.dx_toolkit_autodep = "stable"
                else:
                    args.dx_toolkit_autodep = "beta"
            applet_id = dxpy.app_builder.upload_applet(args.src_dir, bundled_resources,
                                                       check_name_collisions=(args.mode == "applet"),
                                                       overwrite=args.overwrite and args.mode == "applet",
                                                       project=working_project,
                                                       override_folder = override_folder,
                                                       override_name = override_applet_name,
                                                       dx_toolkit_autodep=args.dx_toolkit_autodep,
                                                       dry_run=args.dry_run)
        except:
            # Avoid leaking any bundled_resources files we may have
            # created, if applet creation fails. Note that if
            # using_temp_project, the entire project gets destroyed at
            # the end, so we don't bother.
            if not using_temp_project:
                objects_to_delete = [dxpy.get_dxlink_ids(bundled_resource_obj['id'])[0] for bundled_resource_obj in bundled_resources]
                if objects_to_delete:
                    dxpy.api.projectRemoveObjects(dxpy.app_builder.get_destination_project(args.src_dir, project=working_project),
                                                  input_params={"objects": objects_to_delete})
            raise

        if args.dry_run:
            return

        print >> sys.stderr, "Created applet " + applet_id + " successfully"

        if args.mode == "app":
            if 'version' not in app_json:
                parser.error("dxapp.json contains no \"version\" field, but it is required to build an app")
            version = app_json['version']
            try_versions = [args.version_override or version]
            if not args.version_override and args.version_autonumbering:
                try_versions.append(version + get_version_suffix(args.src_dir))

            app_id = dxpy.app_builder.create_app(applet_id, args.src_dir,
                                                 publish=args.publish,
                                                 set_default=args.publish,
                                                 billTo=args.bill_to,
                                                 try_versions=try_versions,
                                                 try_update=args.update)

            app_describe = dxpy.api.appDescribe(app_id)
            print json.dumps(app_describe)

            if args.publish:
                print >> sys.stderr, "Uploaded and published app %s/%s (%s) successfully" % (app_describe["name"], app_describe["version"], app_id)
            else:
                print >> sys.stderr, "Uploaded app %s/%s (%s) successfully" % (app_describe["name"], app_describe["version"], app_id)
                print >> sys.stderr, "You can publish this app with:"
                print >> sys.stderr, "  dx api app-%s/%s publish \"{\\\"makeDefault\\\": true}\"" % (app_describe["name"], app_describe["version"])

        elif args.mode == "applet":
            pass
        else:
            raise ValueError("Unrecognized mode %r" % (args.mode,))

    except dxpy.app_builder.AppletBuilderException as e:
        # AppletBuilderException represents errors during applet
        # building that could reasonably have been anticipated.
        print "Error: %s" % (e.message,)
        sys.exit(1)

    finally:
        # Clean up after ourselves.
        if using_temp_project:
            dxpy.api.projectDestroy(working_project)

if __name__ == '__main__':
    main()
