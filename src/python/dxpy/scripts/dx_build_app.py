#!/usr/bin/env python

import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.ERROR)

import os, sys, json, fileinput, re, subprocess, argparse
from datetime import datetime
import dxpy, dxpy.app_builder

parser = argparse.ArgumentParser(prog="dx-build-app", description="Uploads a DNAnexus App.")

# COMMON OPTIONS
parser.add_argument("src_dir", help="App or applet source directory")

parser.set_defaults(mode="app")
parser.add_argument("--create-app", help=argparse.SUPPRESS, action="store_const", dest="mode", const="app")
parser.add_argument("--create-applet", help="Create an applet (default is to create an app).", action="store_const", dest="mode", const="applet")
parser.add_argument("-p", "--destination-project", help="Insert the applet into the project with the specified project ID.", default=None)

# --[no-]publish
parser.set_defaults(publish=False)
parser.add_argument("--publish", help="Publish the resulting app and make it the default.", action="store_true", dest="publish")
parser.add_argument("--no-publish", help=argparse.SUPPRESS, action="store_false", dest="publish")

parser.add_argument("-f", "--overwrite", help="Remove existing applets of the same name.", action="store_true", default=False)
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
parser.set_defaults(dx_toolkit_autodep=True)
parser.add_argument("--dx-toolkit-autodep", help=argparse.SUPPRESS, action="store_true", dest="dx_toolkit_autodep")
parser.add_argument("--no-dx-toolkit-autodep", help="Do not auto-insert the dx-toolkit dependency if it's absent from the runSpec. Only use this if you know what you are doing.", action="store_false", dest="dx_toolkit_autodep")


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

    if not os.path.isdir(args.src_dir):
        parser.error("%s is not a directory" % args.src_dir)

    if not os.path.exists(os.path.join(args.src_dir, "dxapp.json")):
        parser.error("Directory %s does not contain dxapp.json: not a valid DNAnexus app source directory" % args.src_dir)

    dxpy.app_builder.build(args.src_dir)
    bundled_resources = dxpy.app_builder.upload_resources(args.src_dir,
                                                          project=args.destination_project)

    applet_id = dxpy.app_builder.upload_applet(args.src_dir, bundled_resources,
                                               # For now, --create-app implies -f so the user
                                               # doesn't have to care about the existence of the
                                               # intermediate applet.
                                               overwrite=args.overwrite or args.mode == "app",
                                               project=args.destination_project,
                                               dx_toolkit_autodep = args.dx_toolkit_autodep)

    print >> sys.stderr, "Created applet " + applet_id + " successfully"

    if args.mode == "app":
        with open(os.path.join(args.src_dir, "dxapp.json")) as app_desc:
            app_json = json.load(app_desc)
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

if __name__ == '__main__':
    main()
