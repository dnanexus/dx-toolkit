#!/usr/bin/env python

import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.ERROR)

import os, sys, json, fileinput, re, subprocess, argparse
from datetime import datetime
import dxpy, dxpy.app_builder

parser = argparse.ArgumentParser(description="Uploads a DNAnexus App.")

# COMMON OPTIONS
parser.add_argument("src_dir", help="App or applet source directory (default: current directory)", nargs='?')

parser.set_defaults(mode="app")
parser.add_argument("--create-app", help=argparse.SUPPRESS, action="store_const", dest="mode", const="app")
parser.add_argument("--create-applet", help=argparse.SUPPRESS, action="store_const", dest="mode", const="applet")
parser.add_argument("-p", "--destination-project", help="Insert the applet into the project with the specified project ID.", default=None)

parser.set_defaults(use_temp_build_project=True)
parser.add_argument("--no-temp-build-project", help="When building an app, build its applet in the current project instead of a temporary project", action="store_false", dest="use_temp_build_project")

# --[no-]publish
parser.set_defaults(publish=False)
parser.add_argument("--publish", help="Publish the resulting app and make it the default.", action="store_true", dest="publish")
parser.add_argument("--no-publish", help=argparse.SUPPRESS, action="store_false", dest="publish")

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
# would deviate from a real run in the following ways:
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


def main(**kwargs):

    if len(kwargs) == 0:
        args = parser.parse_args()
    else:
        args = parser.parse_args(kwargs)

    if args.src_dir is None:
        args.src_dir = os.getcwd()

    if not os.path.isdir(args.src_dir):
        parser.error("%s is not a directory" % args.src_dir)

    if not os.path.exists(os.path.join(args.src_dir, "dxapp.json")):
        parser.error("Directory %s does not contain dxapp.json: not a valid DNAnexus app source directory" % args.src_dir)

    working_project = None
    using_temp_project = False
    if args.mode == "applet" and args.destination_project:
        # TODO: should -p be required when creating an applet?
        working_project = args.destination_project
    elif args.mode == "app" and args.use_temp_build_project and not args.dry_run:
        # Create a temp project
        working_project = dxpy.api.projectNew({"name": "Temporary build project for dx-build-app"})["id"]
        print >> sys.stderr, "Created temporary project %s to build in" % (working_project,)
        using_temp_project = True

    try:
        with open(os.path.join(args.src_dir, "dxapp.json")) as app_desc:
            app_json = json.load(app_desc)

        if "buildOptions" in app_json:
            if app_json["buildOptions"].get("dx_toolkit_autodep") == False:
                args.dx_toolkit_autodep = False
            del app_json["buildOptions"]

        dxpy.app_builder.build(args.src_dir)

        bundled_resources = dxpy.app_builder.upload_resources(args.src_dir, project=working_project) if not args.dry_run else []

        if args.dx_toolkit_autodep == "auto":
            # "auto" (the default) means dx-toolkit (stable) on preprod
            # and dx-toolkit-beta on all other systems.
            if dxpy.APISERVER_HOST == "preprodapi.dnanexus.com":
                args.dx_toolkit_autodep = "stable"
            else:
                args.dx_toolkit_autodep = "beta"

        applet_id = dxpy.app_builder.upload_applet(args.src_dir, bundled_resources,
                                                   check_name_collisions=(args.mode == "applet"),
                                                   overwrite=args.overwrite and args.mode == "applet",
                                                   project=working_project,
                                                   dx_toolkit_autodep=args.dx_toolkit_autodep,
                                                   dry_run=args.dry_run)

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
            print json.dumps(dxpy.api.appletDescribe(applet_id))
        else:
            raise ValueError("Unrecognized mode %r" % (args.mode,))

    finally:
        # Clean up after ourselves.
        if using_temp_project:
            dxpy.api.projectDestroy(working_project)

if __name__ == '__main__':
    main()
