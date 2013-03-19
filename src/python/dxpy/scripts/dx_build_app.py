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
import locale
import pipes
import shutil
import tempfile
import time
from datetime import datetime
import dxpy, dxpy.app_builder

# Use default locale (used for formatting numbers nicely)
locale.setlocale(locale.LC_ALL, '')

from dxpy.utils.resolver import resolve_path, is_container_id

parser = argparse.ArgumentParser(description="Uploads a DNAnexus App.")

# COMMON OPTIONS
parser.add_argument("src_dir", help="App or applet source directory (default: current directory)", nargs='?')

parser.set_defaults(mode="app")
parser.add_argument("--create-app", help=argparse.SUPPRESS, action="store_const", dest="mode", const="app")
parser.add_argument("--create-applet", help=argparse.SUPPRESS, action="store_const", dest="mode", const="applet")
parser.add_argument("-d", "--destination", help="Specifies the destination project, destination folder, and/or name for the applet, in the form [PROJECT_NAME_OR_ID:][/[FOLDER/][NAME]]. Overrides the project, folder, and name fields of the dxapp.json, if they were supplied.", default=None)

parser.set_defaults(use_temp_build_project=True)
parser.add_argument("--no-temp-build-project", help="When building an app, build its applet in the current project instead of a temporary project", action="store_false", dest="use_temp_build_project")

parser.set_defaults(parallel_build=True)
parser.add_argument("--parallel-build", help=argparse.SUPPRESS, action="store_true", dest="parallel_build")
parser.add_argument("--no-parallel-build", help="Build with make instead of make -jN.", action="store_false", dest="parallel_build")

# --[no-]publish
parser.set_defaults(publish=False)
parser.add_argument("--publish", help="Publish the resulting app and make it the default.", action="store_true", dest="publish")
parser.add_argument("--no-publish", help=argparse.SUPPRESS, action="store_false", dest="publish")

# --[no-]remote
parser.set_defaults(remote=False)
parser.add_argument("--remote", help="Build the app remotely.", action="store_true", dest="remote")
parser.add_argument("--no-remote", help=argparse.SUPPRESS, action="store_false", dest="remote")

parser.add_argument("-f", "--overwrite", help="Remove existing applet(s) of the same name in the destination folder.", action="store_true", default=False)
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

# TODO: remove this flag (once all calls to build_and_upload_locally are
# in process
#
# --[no-]build (undocumented): perform the ./configure && make step
parser.set_defaults(build_step=True)
parser.add_argument("--build-step", help=argparse.SUPPRESS, action="store_true", dest="build_step")
parser.add_argument("--no-build-step", help=argparse.SUPPRESS, action="store_false", dest="build_step")

# TODO: remove this flag (once all calls to build_and_upload_locally are
# in process
#
# --[no-]upload (undocumented): perform the actual upload
parser.set_defaults(upload_step=True)
parser.add_argument("--upload-step", help=argparse.SUPPRESS, action="store_true", dest="upload_step")
parser.add_argument("--no-upload-step", help=argparse.SUPPRESS, action="store_false", dest="upload_step")

# TODO: remove this flag (once all calls to build_and_upload_locally are
# in process
#
# --[no-]json (undocumented): dumps the JSON describe of the app or
# applet that was created
parser.set_defaults(json=False)
parser.add_argument("--json", help=argparse.SUPPRESS, action="store_true", dest="json")
parser.add_argument("--no-json", help=argparse.SUPPRESS, action="store_false", dest="json")

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

def _verify_app_source_dir(src_dir):
    if not os.path.isdir(src_dir):
        parser.error("%s is not a directory" % src_dir)
    if not os.path.exists(os.path.join(src_dir, "dxapp.json")):
        parser.error("Directory %s does not contain dxapp.json: not a valid DNAnexus app source directory" % src_dir)

def _build_app_remote(mode, src_dir, publish=False, destination_override=None,
                      version_override=None, bill_to_override=None, dx_toolkit_autodep="auto",
                      do_version_autonumbering=True, do_try_update=True, do_parallel_build=True):
    if mode == 'app':
        builder_app = 'app-tarball_app_builder'
    else:
        builder_app = 'app-tarball_applet_builder'

    temp_dir = tempfile.mkdtemp()

    # If dx_toolkit_autodep is "auto", We have to resolve the correct
    # dx-toolkit dependency type here and explicitly pass it into the
    # interior call of dx-build-app, because within the execution
    # environment of tarball_app(let)_builder, APISERVER_HOST is set to
    # the address of the proxy (a 10.x.x.x address) and doesn't give us
    # any information about whether we are talking to preprod.
    if dx_toolkit_autodep == "auto":
        # "auto" (the default) means dx-toolkit (stable) on preprod and prod, and
        # dx-toolkit-beta on all other systems.
        if dxpy.APISERVER_HOST == "preprodapi.dnanexus.com" or dxpy.APISERVER_HOST == "api.dnanexus.com":
            dx_toolkit_autodep = "stable"
        else:
            dx_toolkit_autodep = "beta"

    build_options = {'dx_toolkit_autodep': dx_toolkit_autodep}

    # These flags are basically passed through verbatim.
    if version_override:
        build_options['version_override'] = version_override
    if bill_to_override:
        build_options['bill_to_override'] = bill_to_override
    if not do_version_autonumbering:
        build_options['do_version_autonumbering'] = False
    if not do_try_update:
        build_options['do_try_update'] = False
    if not do_parallel_build:
        build_options['do_parallel_build'] = False

    using_temp_project_for_remote_build = False

    # If building an applet, run the builder app in the destination
    # project. If building an app, run the builder app in a temporary
    # project.
    dest_folder = None
    dest_applet_name = None
    if mode == "applet":
        # Translate the --destination flag as follows. If --destination
        # is PROJ:FOLDER/NAME,
        #
        # 1. Run the builder app in PROJ
        # 2. Make the output folder FOLDER
        # 3. Supply --destination=NAME to the interior call of dx-build-applet.
        build_project_id = dxpy.WORKSPACE_ID
        if destination_override:
            build_project_id, dest_folder, dest_applet_name = parse_destination(destination_override)
        if build_project_id is None:
            parser.error("Can't create an applet without specifying a destination project; please use the -d/--destination flag to explicitly specify a project")
        if dest_applet_name:
            build_options['destination_override'] = '/' + dest_applet_name

    elif mode == "app":
        using_temp_project_for_remote_build = True
        build_project_id = dxpy.api.project_new({"name": "dx-build-app --remote temporary project"})["id"]

    try:
        # Resolve relative paths and symlinks here so we have something
        # reasonable to write in the job name below.
        src_dir = os.path.realpath(src_dir)

        # Show the user some progress as the tarball is being generated.
        # Hopefully this will help them to understand when their tarball
        # is huge (e.g. the target directory already has a whole bunch
        # of binaries in it) and interrupt before uploading begins.
        app_tarball_file = os.path.join(temp_dir, "app_tarball.tar.gz")
        # TODO: figure out if we can use --exclude-vcs here (conditional
        # on presence of GNU tar). This might require propagating the
        # --version directly to the interior dx-build-app since in
        # general that can depend on the git metadata.
        tar_subprocess = subprocess.Popen(["tar", "-czf", "-", "."], cwd=src_dir, stdout=subprocess.PIPE)
        with open(app_tarball_file, 'w') as tar_output_file:
            total_num_bytes = 0
            last_console_update = 0
            start_time = time.time()
            printed_static_message = False
            # Pipe the output of tar into the output file, and
            while True:
                tar_exitcode = tar_subprocess.poll()
                data = tar_subprocess.stdout.read(4 * 1024 * 1024)
                if tar_exitcode is not None and len(data) == 0:
                    break
                tar_output_file.write(data)
                total_num_bytes += len(data)
                current_time = time.time()
                # Don't show status messages at all for very short tar
                # operations (< 1.0 sec)
                if current_time - last_console_update > 0.25 and current_time - start_time > 1.0:
                    if sys.stderr.isatty():
                        if last_console_update > 0:
                            sys.stderr.write("\r")
                        sys.stderr.write("Compressing target directory %s... (%s kb)" % (src_dir, locale.format("%d", (total_num_bytes / 1024,), grouping=True),))
                        sys.stderr.flush()
                        last_console_update = current_time
                    elif not printed_static_message:
                        # Print a message (once only) when stderr is not
                        # going to a live console
                        sys.stderr.write("Compressing target directory %s..." % (src_dir,))
                        printed_static_message = True

        if last_console_update > 0:
            sys.stderr.write("\n")
        if tar_exitcode != 0:
            raise Exception("tar exited with non-zero exit code " + str(tar_exitcode))

        dxpy.set_workspace_id(build_project_id)

        remote_file = dxpy.upload_local_file(app_tarball_file, media_type="application/gzip",
                                             wait_on_close=True, show_progress=True)

        try:
            input_hash = {
                "input_file": dxpy.dxlink(remote_file),
                "build_options": build_options
                }
            if mode == 'app':
                input_hash["publish"] = publish
            api_options = {
                "name": "Remote build of %s" % (os.path.basename(src_dir),),
                "input": input_hash,
                "project": build_project_id,
                }
            if dest_folder:
                api_options["folder"] = dest_folder
            app_run_result = dxpy.api.app_run(builder_app, input_params=api_options)
            job_id = app_run_result["id"]
            print "Started builder job %s" % (job_id,)
            subprocess.check_call(["dx", "watch", job_id])
        finally:
            if not using_temp_project_for_remote_build:
                dxpy.DXProject(build_project_id).remove_objects([remote_file.get_id()])
    finally:
        if using_temp_project_for_remote_build:
            dxpy.api.project_destroy(build_project_id)
        shutil.rmtree(temp_dir)

    return


def build_and_upload_locally(src_dir, mode, overwrite=False, publish=False, destination_override=None, version_override=None, bill_to_override=None, use_temp_build_project=True, do_parallel_build=True, do_version_autonumbering=True, do_try_update=True, dx_toolkit_autodep="auto", do_build_step=True, do_upload_step=True, dry_run=False, return_object_dump=False):

    _verify_app_source_dir(src_dir)

    working_project = None
    using_temp_project = False
    override_folder = None
    override_applet_name = None

    if mode == "applet" and destination_override:
        working_project, override_folder, override_applet_name = parse_destination(destination_override)
    elif mode == "app" and use_temp_build_project and not dry_run:
        # Create a temp project
        working_project = dxpy.api.project_new({"name": "Temporary build project for dx-build-app"})["id"]
        print >> sys.stderr, "Created temporary project %s to build in" % (working_project,)
        using_temp_project = True

    try:
        if mode == "applet" and working_project is None and dxpy.WORKSPACE_ID is None:
            parser.error("Can't create an applet without specifying a destination project; please use the -d/--destination flag to explicitly specify a project")

        with open(os.path.join(src_dir, "dxapp.json")) as app_desc:
            try:
                app_json = json.load(app_desc)
            except Exception as e:
                raise dxpy.app_builder.AppBuilderException("Could not parse dxapp.json file as JSON: " + e.message)

        if "buildOptions" in app_json:
            if app_json["buildOptions"].get("dx_toolkit_autodep") == False:
                dx_toolkit_autodep = False
            del app_json["buildOptions"]

        if do_build_step:
            dxpy.app_builder.build(src_dir, parallel_build=do_parallel_build)

        if not do_upload_step:
            return

        bundled_resources = dxpy.app_builder.upload_resources(src_dir, project=working_project) if not dry_run else []

        try:
            if dx_toolkit_autodep == "auto":
                # "auto" (the default) means dx-toolkit (stable) on preprod and prod,
                # and dx-toolkit-beta on all other systems.
                if dxpy.APISERVER_HOST == "preprodapi.dnanexus.com" or dxpy.APISERVER_HOST == "api.dnanexus.com":
                    dx_toolkit_autodep = "stable"
                else:
                    dx_toolkit_autodep = "beta"
            applet_id, applet_spec = dxpy.app_builder.upload_applet(
                src_dir,
                bundled_resources,
                check_name_collisions=(mode == "applet"),
                overwrite=overwrite and mode == "applet",
                project=working_project,
                override_folder=override_folder,
                override_name=override_applet_name,
                dx_toolkit_autodep=dx_toolkit_autodep,
                dry_run=dry_run)
        except:
            # Avoid leaking any bundled_resources files we may have
            # created, if applet creation fails. Note that if
            # using_temp_project, the entire project gets destroyed at
            # the end, so we don't bother.
            if not using_temp_project:
                objects_to_delete = [dxpy.get_dxlink_ids(bundled_resource_obj['id'])[0] for bundled_resource_obj in bundled_resources]
                if objects_to_delete:
                    dxpy.api.project_remove_objects(dxpy.app_builder.get_destination_project(src_dir, project=working_project),
                                                  input_params={"objects": objects_to_delete})
            raise

        if dry_run:
            return

        applet_name = applet_spec['name']

        print >> sys.stderr, "Created applet " + applet_id + " successfully"

        if mode == "app":
            if 'version' not in app_json:
                parser.error("dxapp.json contains no \"version\" field, but it is required to build an app")
            version = app_json['version']
            try_versions = [version_override or version]
            if not version_override and do_version_autonumbering:
                try_versions.append(version + get_version_suffix(src_dir))

            app_id = dxpy.app_builder.create_app(applet_id,
                                                 applet_name,
                                                 src_dir,
                                                 publish=publish,
                                                 set_default=publish,
                                                 billTo=bill_to_override,
                                                 try_versions=try_versions,
                                                 try_update=do_try_update)

            app_describe = dxpy.api.app_describe(app_id)

            if publish:
                print >> sys.stderr, "Uploaded and published app %s/%s (%s) successfully" % (app_describe["name"], app_describe["version"], app_id)
            else:
                print >> sys.stderr, "Uploaded app %s/%s (%s) successfully" % (app_describe["name"], app_describe["version"], app_id)
                print >> sys.stderr, "You can publish this app with:"
                print >> sys.stderr, "  dx api app-%s/%s publish \"{\\\"makeDefault\\\": true}\"" % (app_describe["name"], app_describe["version"])

            return app_describe if return_object_dump else None

        elif mode == "applet":
            return dxpy.api.applet_describe(applet_id) if return_object_dump else None
        else:
            raise dxpy.app_builder.AppBuilderException("Unrecognized mode %r" % (mode,))

    finally:
        # Clean up after ourselves.
        if using_temp_project:
            dxpy.api.project_destroy(working_project)


def main(**kwargs):

    if len(kwargs) == 0:
        args = parser.parse_args()
    else:
        args = parser.parse_args(**kwargs)

    if dxpy.AUTH_HELPER is None:
        parser.error('Authentication required to build an executable on the platform; please run "dx login" first')

    if args.src_dir is None:
        args.src_dir = os.getcwd()

    if args.mode == "app" and args.destination:
        parser.error("--destination cannot be used when creating an app (only an applet)")

    if not args.remote:
        # LOCAL BUILD

        try:
            output = build_and_upload_locally(
                args.src_dir,
                args.mode,
                overwrite=args.overwrite,
                publish=args.publish,
                destination_override=args.destination,
                version_override=args.version_override,
                bill_to_override=args.bill_to,
                use_temp_build_project=args.use_temp_build_project,
                do_parallel_build=args.parallel_build,
                do_version_autonumbering=args.version_autonumbering,
                do_try_update=args.update,
                dx_toolkit_autodep=args.dx_toolkit_autodep,
                do_build_step=args.build_step,
                do_upload_step=args.upload_step,
                dry_run=args.dry_run,
                return_object_dump=args.json
                )

            if output is not None:
                print json.dumps(output)
        except dxpy.app_builder.AppBuilderException as e:
            # AppBuilderException represents errors during app or applet building
            # that could reasonably have been anticipated by the user.
            print >> sys.stderr, "Error: %s" % (e.message,)
            sys.exit(3)

        return

    else:
        # REMOTE BUILD

        _verify_app_source_dir(args.src_dir)

        # The following flags might be useful in conjunction with
        # --remote. To enable these, we need to learn how to pass these
        # options through to the interior call of dx_build_app(let).
        if args.dry_run:
            parser.error('--remote cannot be combined with --dry-run')
        if args.overwrite:
            parser.error('--remote cannot be combined with --overwrite/-f')

        # The following flags are probably not useful in conjunction
        # with --remote.
        if not args.build_step:
            parser.error('--remote cannot be combined with --no-build-step')
        if not args.upload_step:
            parser.error('--remote cannot be combined with --no-upload-step')
        if args.json:
            parser.error('--remote cannot be combined with --json')
        if not args.use_temp_build_project:
            parser.error('--remote cannot be combined with --no-temp-build-project')

        more_kwargs = {}
        if args.version_override:
            more_kwargs['version_override'] = args.version_override
        if args.bill_to:
            more_kwargs['bill_to_override'] = args.bill_to
        if not args.version_autonumbering:
            more_kwargs['do_version_autonumbering'] = False
        if not args.update:
            more_kwargs['do_try_update'] = False
        if not args.parallel_build:
            more_kwargs['do_parallel_build'] = False

        return _build_app_remote(args.mode, args.src_dir, destination_override=args.destination, publish=args.publish, dx_toolkit_autodep=args.dx_toolkit_autodep, **more_kwargs)


if __name__ == '__main__':
    main()
