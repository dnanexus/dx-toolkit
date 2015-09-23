# Copyright (C) 2013-2015 DNAnexus, Inc.
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
from __future__ import (print_function, unicode_literals)

import dxpy
from ..exceptions import DXCLIError
from dxpy.utils.printing import (fill)


def get_org_invite_args(args):
    """
    PRECONDITION:
        - If /org-x/invite is being called in conjunction with /user/new, then
          `_validate_new_user_input()` has been called on `args`; otherwise,
          the parser must perform all the basic input validation.
        - `args.username` is well-formed and valid (e.g. it does not start with
          "user-").
    """
    org_invite_args = {"invitee": "user-" + args.username}
    org_invite_args["level"] = args.level
    if "set_bill_to" in args and args.set_bill_to is True:
        # /org-x/invite is called in conjunction with /user/new.
        org_invite_args["createProjectsAndApps"] = True
    else:
        org_invite_args["createProjectsAndApps"] = args.allow_billable_activities
    org_invite_args["appAccess"] = args.app_access
    org_invite_args["projectAccess"] = args.project_access
    org_invite_args["suppressEmailNotification"] = args.no_email
    return org_invite_args


def add_membership(args):
    try:
        dxpy.api.org_get_member_access(args.org_id,
                                       {"user": "user-" + args.username})
    except:
        pass
    else:
        raise DXCLIError("Cannot add a user who is already a member of the org")

    dxpy.api.org_invite(args.org_id, get_org_invite_args(args))

    if args.brief:
        print("org-" + args.org_id)
    else:
        print(fill("Invited user-{u} to {o}".format(u=args.username,
                                                    o=args.org_id)))


def _get_org_remove_member_args(args):
    remove_member_args = {
        "user": "user-" + args.username,
        "revokeProjectPermissions": args.revoke_project_permissions,
        "revokeAppPermissions": args.revoke_app_permissions}
    return remove_member_args


def remove_membership(args):
    # Will throw ResourceNotFound of the specified user is not currently a
    # member of the org.
    dxpy.api.org_get_member_access(args.org_id,
                                   {"user": "user-" + args.username})

    result = dxpy.api.org_remove_member(args.org_id,
                                        _get_org_remove_member_args(args))
    if args.brief:
        print(result["id"])
    else:
        print(fill("Removed user-{u} from {o}. user-{u} has been removed from the following projects {p}. user-{u} has been removed from the following apps {a}.".format(
          u=args.username, o=args.org_id, p=result["projects"].keys(),
          a=result["apps"].keys())))


def _get_org_set_member_access_args(args):
    user_id = "user-" + args.username
    org_set_member_access_input = {user_id: {"level": args.level}}
    if args.allow_billable_activities is not None:
        org_set_member_access_input[user_id]["createProjectsAndApps"] = (True if args.allow_billable_activities == "true" else False)
    if args.app_access is not None:
        org_set_member_access_input[user_id]["appAccess"] = (True if args.app_access == "true" else False)
    if args.project_access is not None:
        org_set_member_access_input[user_id]["projectAccess"] = args.project_access
    return org_set_member_access_input


def update_membership(args):
    # Will throw ResourceNotFound of the specified user is not currently a
    # member of the org.
    dxpy.api.org_get_member_access(args.org_id,
                                   {"user": "user-" + args.username})
    result = dxpy.api.org_set_member_access(args.org_id,
                                            _get_org_set_member_access_args(args))
    if args.brief:
        print(result["id"])
    else:
        print(fill("Updated membership of user-{u} in {o}".format(
            u=args.username, o=args.org_id)))
