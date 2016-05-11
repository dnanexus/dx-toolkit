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

'''
This submodule contains the callables (and their helpers) that are called by
the org-based commands of the dx command-line client.
'''
from __future__ import print_function, unicode_literals, division, absolute_import

from ..compat import input
import dxpy
from . import try_call, prompt_for_yn, INTERACTIVE_CLI
from .parsers import process_find_by_property_args, process_phi_param
from ..exceptions import (DXCLIError, err_exit)
from dxpy.utils.printing import (fill, DELIMITER, format_find_results)
import json


def get_user_id(user_id_or_username):
    """Gets the user ID based on the value `user_id_or_username` specified on
    the command-line, being extra lenient and lowercasing the value in all
    cases.
    """
    user_id_or_username = user_id_or_username.lower()
    if not user_id_or_username.startswith("user-"):
        user_id = "user-" + user_id_or_username.lower()
    else:
        user_id = user_id_or_username
    return user_id


def get_org_invite_args(user_id, args):
    """
    Used by:
        - `dx new user`
        - `dx add member`

    PRECONDITION:
        - If /org-x/invite is being called in conjunction with /user/new, then
          `_validate_new_user_input()` has been called on `args`; otherwise,
          the parser must perform all the basic input validation.
    """
    org_invite_args = {"invitee": user_id}
    org_invite_args["level"] = args.level
    if "set_bill_to" in args and args.set_bill_to is True:
        # /org-x/invite is called in conjunction with /user/new.
        org_invite_args["allowBillableActivities"] = True
    else:
        org_invite_args["allowBillableActivities"] = args.allow_billable_activities
    org_invite_args["appAccess"] = args.app_access
    org_invite_args["projectAccess"] = args.project_access
    org_invite_args["suppressEmailNotification"] = args.no_email
    return org_invite_args


def add_membership(args):
    user_id = get_user_id(args.username_or_user_id)

    try:
        dxpy.api.org_find_members(args.org_id, {"id": [user_id]})["results"][0]
    except:
        pass
    else:
        raise DXCLIError("Cannot add a user who is already a member of the org. To update an existing member's permissions, use 'dx update member'")

    dxpy.api.org_invite(args.org_id, get_org_invite_args(user_id, args))

    if args.brief:
        print("org-" + args.org_id)
    else:
        print(fill("Invited {u} to {o}".format(u=user_id, o=args.org_id)))


def _get_org_remove_member_args(args):
    remove_member_args = {
        "user": get_user_id(args.username_or_user_id),
        "revokeProjectPermissions": args.revoke_project_permissions,
        "revokeAppPermissions": args.revoke_app_permissions}
    return remove_member_args


def remove_membership(args):
    user_id = get_user_id(args.username_or_user_id)

    try:
        dxpy.api.org_find_members(args.org_id, {"id": [user_id]})["results"][0]
    except IndexError:
        raise DXCLIError("Cannot remove a user who is not a member of the org")

    confirmed = not args.confirm
    if not confirmed:
        # Request interactive confirmation.
        print(fill("WARNING: About to remove {u} from {o}; project permissions will{rpp} be removed and app permissions will{rap} be removed".format(
            u=user_id, o=args.org_id,
            rpp="" if args.revoke_project_permissions else " not",
            rap="" if args.revoke_app_permissions else " not")))

        if prompt_for_yn("Please confirm"):
            confirmed = True

    if confirmed:
        result = dxpy.api.org_remove_member(args.org_id,
                                            _get_org_remove_member_args(args))
        if args.brief:
            print(result["id"])
        else:
            print(fill("Removed {u} from {o}".format(u=user_id, o=args.org_id)))
            print(fill("Removed {u} from the following projects:".format(u=user_id)))
            if len(result["projects"].keys()) != 0:
                for project_id in result["projects"].keys():
                    print("\t{p}".format(p=project_id))
            else:
                print("\tNone")
            print(fill("Removed {u} from the following apps:".format(u=user_id)))
            if len(result["apps"].keys()) != 0:
                for app_id in result["apps"].keys():
                    print("\t{a}".format(a=app_id))
            else:
                print("\tNone")
    else:
        print(fill("Aborting removal of {u} from {o}".format(u=user_id, o=args.org_id)))


def _get_org_set_member_access_args(args, current_level):
    user_id = get_user_id(args.username_or_user_id)
    org_set_member_access_input = {user_id: {}}

    if args.level is not None:
        org_set_member_access_input[user_id]["level"] = args.level
    else:
        org_set_member_access_input[user_id]["level"] = current_level

    admin_to_member = args.level == "MEMBER" and current_level == "ADMIN"

    if args.allow_billable_activities is not None:
        org_set_member_access_input[user_id]["allowBillableActivities"] = (True if args.allow_billable_activities == "true" else False)
    elif admin_to_member:
        org_set_member_access_input[user_id]["allowBillableActivities"] = False

    if args.app_access is not None:
        org_set_member_access_input[user_id]["appAccess"] = (True if args.app_access == "true" else False)
    elif admin_to_member:
        org_set_member_access_input[user_id]["appAccess"] = True

    if args.project_access is not None:
        org_set_member_access_input[user_id]["projectAccess"] = args.project_access
    elif admin_to_member:
        org_set_member_access_input[user_id]["projectAccess"] = "CONTRIBUTE"

    return org_set_member_access_input


def update_membership(args):
    user_id = get_user_id(args.username_or_user_id)

    try:
        member_access = dxpy.api.org_find_members(args.org_id, {"id": [user_id]})["results"][0]
    except IndexError:
        raise DXCLIError("Cannot update a user who is not a member of the org")

    current_level = member_access["level"]

    result = dxpy.api.org_set_member_access(args.org_id,
                                            _get_org_set_member_access_args(args,
                                                                            current_level))
    if args.brief:
        print(result["id"])
    else:
        print(fill("Updated membership of {u} in {o}".format(u=user_id, o=args.org_id)))


def _get_find_orgs_args(args):
    find_orgs_input = {"level": args.level}

    if args.with_billable_activities is not None:
        find_orgs_input["allowBillableActivities"] = args.with_billable_activities

    if not args.brief:
        find_orgs_input["describe"] = True

    return {"query": find_orgs_input}


def find_orgs(args):
    res_iter = dxpy.find_orgs(_get_find_orgs_args(args)["query"])

    if args.json:
        print(json.dumps(list(res_iter)))
    elif args.brief:
        for res in res_iter:
            print(res["id"])
    else:
        for res in res_iter:
            print("{o}{d1}{n}".format(
                o=res["id"],
                d1=(DELIMITER(args.delimiter) if args.delimiter else " : "),
                n=res["describe"]["name"]
            ))


def org_find_members(args):
    results = try_call(dxpy.org_find_members, org_id=args.org_id, level=args.level, describe=(not args.brief))
    format_find_results(args, results)


def new_org(args):
    if args.name is None and INTERACTIVE_CLI:
        args.name = input("Enter descriptive name for new org: ")

    if args.name is None:
        err_exit("No org name supplied and input is not interactive.")

    org_new_input = {"handle": args.handle, "name": args.name,
                     "policies": {"memberListVisibility": args.member_list_visibility,
                                  "restrictProjectTransfer": args.project_transfer_ability}}

    resp = try_call(dxpy.api.org_new, org_new_input)
    if args.brief:
        print(resp['id'])
    else:
        print('Created new org called "' + args.name + '" (' + resp['id'] + ')')


def _get_org_update_args(args):
    org_update_inputs = {}

    if args.name is not None:
        org_update_inputs["name"] = args.name

    if args.member_list_visibility is not None or args.project_transfer_ability is not None:
        org_update_inputs["policies"] = {}
    if args.member_list_visibility is not None:
        org_update_inputs["policies"]["memberListVisibility"] = args.member_list_visibility
    if args.project_transfer_ability is not None:
        org_update_inputs["policies"]["restrictProjectTransfer"] = args.project_transfer_ability

    return org_update_inputs


def update_org(args):
    org_update_inputs = _get_org_update_args(args)
    res = try_call(dxpy.api.org_update, args.org_id, org_update_inputs)
    if args.brief:
        print(res["id"])
    else:
        print(fill("Updated {o}".format(o=res["id"])))


def org_find_projects(args):
    try_call(process_find_by_property_args, args)
    try_call(process_phi_param, args)
    try:
        results = dxpy.org_find_projects(org_id=args.org_id, name=args.name, name_mode='glob',
                                         ids=args.ids, properties=args.properties, tags=args.tag,
                                         describe=(not args.brief),
                                         public=args.public,
                                         created_after=args.created_after,
                                         created_before=args.created_before,
                                         containsPHI=args.containsPHI)
    except:
        err_exit()
    format_find_results(args, results)


def org_find_apps(args):
    try:
        results = dxpy.org_find_apps(org_id=args.org_id,
                                     name=args.name,
                                     name_mode='glob',
                                     describe=(not args.brief),
                                     created_after=args.created_after,
                                     created_before=args.created_before)
    except:
        err_exit()
    format_find_results(args, results)
