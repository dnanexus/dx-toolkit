#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

import os, sys, unittest, json, tempfile, subprocess, shutil, re, base64, random, time
import filecmp
import pipes
import stat
import hashlib
import collections
import string
from contextlib import contextmanager
import pexpect
import requests
import textwrap
import pytest
import gzip
import tarfile
from mock import patch

import dxpy
import dxpy.executable_builder
import dxpy.workflow_builder
from dxpy.scripts import dx_build_app
from dxpy_testutil import (DXTestCase, DXTestCaseBuildApps, DXTestCaseBuildWorkflows, check_output, temporary_project,
                           select_project, cd, override_environment, generate_unique_username_email,
                           without_project_context, without_auth, as_second_user, chdir, run, DXCalledProcessError)
import dxpy_testutil as testutil
from dxpy.exceptions import DXAPIError, DXSearchError, EXPECTED_ERR_EXIT_STATUS, HTTPError
from dxpy.compat import USING_PYTHON2, str, sys_encoding, open
from dxpy.utils.resolver import ResolutionError, _check_resolution_needed as check_resolution

if USING_PYTHON2:
    spawn_extra_args = {}
else:
    # Python 3 requires specifying the encoding
    spawn_extra_args = {"encoding": "utf-8"}


def create_file_in_project(fname, trg_proj_id, folder=None):
    data = "foo"
    if folder is None:
        dxfile = dxpy.upload_string(data, name=fname, project=trg_proj_id, wait_on_close=True)
    else:
        dxfile = dxpy.upload_string(data, name=fname, project=trg_proj_id, folder=folder, wait_on_close=True)
    return dxfile.get_id()


def create_project():
    project_name = "test_dx_cp_" + str(random.randint(0, 1000000)) + "_" + str(int(time.time() * 1000))
    return dxpy.api.project_new({'name': project_name})['id']


def rm_project(proj_id):
    dxpy.api.project_destroy(proj_id, {"terminateJobs": True})


def create_folder_in_project(proj_id, path):
    dxpy.api.project_new_folder(proj_id, {"folder": path})


def list_folder(proj_id, path):
    output = dxpy.api.project_list_folder(proj_id, {"folder": path})
    # Canonicalize to account for possibly different ordering
    output['folders'] = set(output['folders'])
    # (objects is a list of dicts-- which are not hashable-- so just
    # sort them to canonicalize instead of putting them in a set)
    output['objects'] = sorted(output['objects'])
    return output


# for some reason, python 3 insists that we run the command first,
# and then load the result into json. Doing this in one line causes
# an error.
def run_and_parse_json(cmd):
    output = run(cmd)
    return json.loads(output)


@unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                     'skipping tests that require presence of test user and org')
class TestDXClientMembership(DXTestCase):

    def _add_user(self, user_id, level="ADMIN"):
        dxpy.api.org_invite(self.org_id,
                            {"invitee": user_id, "level": level})

    def _remove_user(self, user_id):
        dxpy.api.org_remove_member(self.org_id, {"user": user_id})

        with self.assertRaises(IndexError):
            self._org_find_members(user_id)

    def _org_find_members(self, user_id):
        return dxpy.api.org_find_members(self.org_id, {"id": [user_id]})["results"][0]

    def setUp(self):
        self.username = "bob"
        self.user_id = "user-" + self.username

        # ADMIN: Alice.
        self.org_id = "org-piratelabs"

        super(TestDXClientMembership, self).setUp()

    def tearDown(self):
        self._remove_user(self.user_id)
        super(TestDXClientMembership, self).tearDown()

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_ORG_ADD_MEMBER"])
    def test_add_membership_default(self):
        cmd = "dx add member {o} {u} --level {l}"

        run(cmd.format(o=self.org_id, u=self.username, l="ADMIN"))
        exp_membership = {"id": self.user_id,
                          "level": "ADMIN",
                          "allowBillableActivities": True,
                          "appAccess": True,
                          "projectAccess": "ADMINISTER"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        self._remove_user(self.user_id)

        run(cmd.format(o=self.org_id, u=self.username, l="MEMBER"))
        exp_membership = {"id": self.user_id,
                          "level": "MEMBER",
                          "allowBillableActivities": False,
                          "appAccess": True,
                          "projectAccess": "CONTRIBUTE"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

    def test_add_membership_with_options(self):
        cmd = "dx add member {o} {u} --level {l}"

        run("{cmd} --no-app-access --project-access NONE".format(
            cmd=cmd.format(o=self.org_id, u=self.username, l="ADMIN")))
        exp_membership = {"id": self.user_id,
                          "level": "ADMIN",
                          "allowBillableActivities": True,
                          "appAccess": True,
                          "projectAccess": "ADMINISTER"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        self._remove_user(self.user_id)

        run("{cmd} --allow-billable-activities --no-app-access --project-access NONE".format(
            cmd=cmd.format(o=self.org_id, u=self.username, l="MEMBER")))
        exp_membership = {"id": self.user_id,
                          "level": "MEMBER",
                          "allowBillableActivities": True,
                          "appAccess": False,
                          "projectAccess": "NONE"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

    def test_add_membership_negative(self):
        cmd = "dx add member"

        called_process_error_opts = [
            "",
            "some_username --level ADMIN",
            "org-foo --level ADMIN",
            "org-foo some_username",
        ]
        for invalid_opts in called_process_error_opts:
            with self.assertRaises(subprocess.CalledProcessError):
                run(" ".join([cmd, invalid_opts]))

        self._add_user(self.user_id)

        # Cannot add a user who is already a member of the org.
        with self.assertRaisesRegex(subprocess.CalledProcessError, "DXCLIError"):
            run(" ".join([cmd, self.org_id, self.username, "--level ADMIN"]))

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_ORG_REMOVE_MEMBER",
                                          "DNA_API_ORG_REMOVE_USER"])
    def test_remove_membership_default(self):
        self._add_user(self.user_id)

        exp_membership = {"id": self.user_id,
                          "level": "ADMIN",
                          "allowBillableActivities": True,
                          "appAccess": True,
                          "projectAccess": "ADMINISTER"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        run("dx remove member {o} {u} -y".format(o=self.org_id, u=self.username))
        with self.assertRaises(IndexError):
            self._org_find_members(self.user_id)

    def test_remove_membership_interactive_conf(self):
        self._add_user(self.user_id)

        exp_membership = {"id": self.user_id,
                          "level": "ADMIN",
                          "allowBillableActivities": True,
                          "appAccess": True,
                          "projectAccess": "ADMINISTER"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        dx_rm_member_int = pexpect.spawn("dx remove member {o} {u}".format(o=self.org_id, u=self.username),
                                         logfile=sys.stderr,
                                         **spawn_extra_args)
        dx_rm_member_int.expect("Please confirm")
        dx_rm_member_int.sendline("")
        dx_rm_member_int.expect("Please confirm")

        membership = self._org_find_members(self.user_id)
        self.assertDictContainsSubset(membership, exp_membership)

        dx_rm_member_int = pexpect.spawn("dx remove member {o} {u}".format(o=self.org_id, u=self.username),
                                         logfile=sys.stderr,
                                         **spawn_extra_args)
        dx_rm_member_int.expect("Please confirm")
        dx_rm_member_int.sendintr()

        membership = self._org_find_members(self.user_id)
        self.assertDictContainsSubset(membership, exp_membership)

        dx_rm_member_int = pexpect.spawn("dx remove member {o} {u}".format(o=self.org_id, u=self.username),
                                         logfile=sys.stderr,
                                         **spawn_extra_args)
        dx_rm_member_int.expect("Please confirm")
        dx_rm_member_int.sendline("n")
        dx_rm_member_int.expect("Aborting removal")

        membership = self._org_find_members(self.user_id)
        self.assertDictContainsSubset(membership, exp_membership)

        dx_rm_member_int = pexpect.spawn("dx remove member {o} {u}".format(o=self.org_id, u=self.username),
                                         **spawn_extra_args)
        dx_rm_member_int.logfile = sys.stdout
        dx_rm_member_int.expect("Please confirm")
        dx_rm_member_int.sendline("y")
        dx_rm_member_int.expect("Removed user-{u}".format(u=self.username))

    def test_remove_membership_interactive_conf_format(self):
        self._add_user(self.user_id)

        exp_membership = {"id": self.user_id,
                          "level": "ADMIN",
                          "allowBillableActivities": True,
                          "appAccess": True,
                          "projectAccess": "ADMINISTER"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        project_id_1 = "project-000000000000000000000001"
        prev_bill_to_1 = dxpy.api.project_describe(project_id_1, {"fields": {"billTo": True}})["billTo"]
        dxpy.api.project_update(project_id_1, {"billTo": self.org_id})
        project_permissions = dxpy.api.project_describe(project_id_1, {"fields": {"permissions": True}})["permissions"]
        self.assertEqual(project_permissions[self.user_id], "VIEW")

        project_id_2 = "project-000000000000000000000002"
        prev_bill_to_2 = dxpy.api.project_describe(project_id_2, {"fields": {"billTo": True}})["billTo"]
        dxpy.api.project_update(project_id_2, {"billTo": self.org_id})
        dxpy.api.project_invite(project_id_2, {"invitee": self.user_id, "level": "ADMINISTER"})
        project_permissions = dxpy.api.project_describe(project_id_2, {"fields": {"permissions": True}})["permissions"]
        self.assertEqual(project_permissions[self.user_id], "ADMINISTER")

        dx_rm_member_int = pexpect.spawn("dx remove member {o} {u}".format(o=self.org_id, u=self.username),
                                         **spawn_extra_args)
        dx_rm_member_int.logfile = sys.stdout
        dx_rm_member_int.expect("Please confirm")
        dx_rm_member_int.sendline("y")
        dx_rm_member_int.expect("Removed user-{u}".format(u=self.username))
        dx_rm_member_int.expect("Removed user-{u} from the following projects:".format(
            u=self.username))
        dx_rm_member_int.expect("\t" + project_id_1)
        dx_rm_member_int.expect("\t" + project_id_2)
        dx_rm_member_int.expect("Removed user-{u} from the following apps:".format(
            u=self.username))
        dx_rm_member_int.expect("None")

        dxpy.api.project_update(project_id_1, {"billTo": prev_bill_to_1})
        dxpy.api.project_update(project_id_2, {"billTo": prev_bill_to_2})

    def test_remove_membership_negative(self):
        cmd = "dx remove member"

        # Cannot remove a user who is not currently a member of the org.
        with self.assertRaisesRegex(subprocess.CalledProcessError,
                                    "DXCLIError"):
            run(" ".join([cmd, self.org_id, self.username]))

        called_process_error_opts = [
            "",
            "some_username",
            "org-foo",
        ]
        for invalid_opts in called_process_error_opts:
            with self.assertRaises(subprocess.CalledProcessError):
                run(" ".join([cmd, invalid_opts]))

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_ORG_UPDATE_USER_MEMBERSHIP",
                                          "DNA_API_ORG_CHANGE_USER_PERMISSIONS"])
    def test_update_membership_positive(self):
        # default test
        self._add_user(self.user_id)

        exp_membership = {"id": self.user_id,
                          "level": "ADMIN",
                          "allowBillableActivities": True,
                          "appAccess": True,
                          "projectAccess": "ADMINISTER"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        run(
            "dx update member {o} {u} --level MEMBER --allow-billable-activities false --project-access VIEW --app-access true".format(
                o=self.org_id, u=self.username))
        exp_membership = {"id": self.user_id,
                          "level": "MEMBER",
                          "allowBillableActivities": False,
                          "projectAccess": "VIEW",
                          "appAccess": True}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        run("dx update member {o} {u} --allow-billable-activities true --app-access false".format(
            o=self.org_id, u=self.username))
        exp_membership = {"id": self.user_id,
                          "level": "MEMBER",
                          "allowBillableActivities": True,
                          "projectAccess": "VIEW",
                          "appAccess": False}

        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

    def test_update_membership_to_member_without_membership_flags(self):
        cmd = "dx update member {o} {u} --level MEMBER".format(o=self.org_id, u=self.username)

        # ADMIN to MEMBER.
        self._add_user(self.user_id)
        exp = {"id": self.user_id,
               "level": "ADMIN",
               "allowBillableActivities": True,
               "projectAccess": "ADMINISTER",
               "appAccess": True}
        membership_response = self._org_find_members(self.user_id)
        self.assertEqual(membership_response, exp)

        run(cmd)
        exp = {"id": self.user_id,
               "level": "MEMBER",
               "allowBillableActivities": False,
               "projectAccess": "CONTRIBUTE",
               "appAccess": True}
        membership_response = self._org_find_members(self.user_id)
        self.assertEqual(membership_response, exp)

        # MEMBER to MEMBER.
        run(cmd + " --allow-billable-activities true")
        exp = {"id": self.user_id,
               "level": "MEMBER",
               "allowBillableActivities": True,
               "projectAccess": "CONTRIBUTE",
               "appAccess": True}
        membership_response = self._org_find_members(self.user_id)
        self.assertEqual(membership_response, exp)

        run(cmd)
        membership_response = self._org_find_members(self.user_id)
        self.assertDictContainsSubset(membership_response, exp)

    def test_update_membership_negative(self):
        cmd = "dx update member"

        # Cannot update the membership of a user who is not currently a member
        # of the org.
        with self.assertRaisesRegex(subprocess.CalledProcessError,
                                    "DXCLIError"):
            run(" ".join([cmd, self.org_id, self.username, "--level ADMIN"]))

        called_process_error_opts = [
            "",
            "some_username --level ADMIN",
            "org-foo --level ADMIN",
            "org-foo some_username --level NONE",
        ]

        for invalid_opts in called_process_error_opts:
            with self.assertRaises(subprocess.CalledProcessError):
                run(" ".join([cmd, invalid_opts]))

        # We expect the following to fail as an API call, as ADMIN doesn't
        # take options
        self._add_user(self.user_id)

        api_error_opts = [
            "{} {} --allow-billable-activities true".format(self.org_id,
                                                            self.username),
        ]

        for invalid_opt in api_error_opts:
            with self.assertRaisesRegex(subprocess.CalledProcessError,
                                        "InvalidInput"):
                run(' '.join([cmd, invalid_opt]))

    def test_add_update_remove_membership(self):
        cmd = "dx add member {o} {u} --level {l} --project-access UPLOAD"
        run(cmd.format(o=self.org_id, u=self.username, l="MEMBER"))
        exp_membership = {"id": self.user_id,
                          "level": "MEMBER",
                          "allowBillableActivities": False,
                          "appAccess": True,
                          "projectAccess": "UPLOAD"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        cmd = "dx update member {o} {u} --level MEMBER --allow-billable-activities true"
        run(cmd.format(o=self.org_id, u=self.username))
        exp_membership.update(allowBillableActivities=True)
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        cmd = "dx update member {o} {u} --level ADMIN"
        run(cmd.format(o=self.org_id, u=self.username))
        exp_membership = {"id": self.user_id,
                          "level": "ADMIN",
                          "allowBillableActivities": True,
                          "appAccess": True,
                          "projectAccess": "ADMINISTER"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        cmd = "dx update member {o} {u} --level MEMBER --allow-billable-activities true --project-access CONTRIBUTE --app-access false"
        run(cmd.format(o=self.org_id, u=self.username))
        exp_membership.update(level="MEMBER", projectAccess="CONTRIBUTE", appAccess=False)
        membership = self._org_find_members(self.user_id)
        self.assertDictContainsSubset(membership, exp_membership)

        cmd = "dx remove member {o} {u} -y"
        run(cmd.format(o=self.org_id, u=self.username))

        with self.assertRaises(IndexError):
            self._org_find_members(self.user_id)

    def test_add_update_remove_membership_with_user_id(self):
        # This is similar to `test_add_update_remove_membership()` above, but
        # it specifies user id instead of username as arg to `dx` command.

        cmd = "dx add member {o} {u} --level {l} --project-access UPLOAD"
        run(cmd.format(o=self.org_id, u=self.user_id, l="MEMBER"))
        exp_membership = {"id": self.user_id,
                          "level": "MEMBER",
                          "allowBillableActivities": False,
                          "appAccess": True,
                          "projectAccess": "UPLOAD"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        cmd = "dx update member {o} {u} --level MEMBER --allow-billable-activities true"
        run(cmd.format(o=self.org_id, u=self.user_id))
        exp_membership.update(allowBillableActivities=True)
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        cmd = "dx update member {o} {u} --level ADMIN"
        run(cmd.format(o=self.org_id, u=self.user_id))
        exp_membership = {"id": self.user_id,
                          "level": "ADMIN",
                          "allowBillableActivities": True,
                          "appAccess": True,
                          "projectAccess": "ADMINISTER"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        cmd = "dx update member {o} {u} --level MEMBER --allow-billable-activities true --project-access CONTRIBUTE --app-access false"
        run(cmd.format(o=self.org_id, u=self.user_id))
        exp_membership.update(level="MEMBER", projectAccess="CONTRIBUTE", appAccess=False)
        membership = self._org_find_members(self.user_id)
        self.assertDictContainsSubset(membership, exp_membership)

        cmd = "dx remove member {o} {u} -y"
        run(cmd.format(o=self.org_id, u=self.user_id))

        with self.assertRaises(IndexError):
            self._org_find_members(self.user_id)


class TestDXClientUpdateProject(DXTestCase):
    cmd = "dx update project {pid} --{item} {n}"

    def setUp(self):
        proj_name = u"Project_name" + str(time.time())
        self.project = dxpy.api.project_new({"name": proj_name})['id']
        dxpy.config["DX_PROJECT_CONTEXT_ID"] = self.project
        cd(self.project + ":/")
        dxpy.config.__init__(suppress_warning=True)
        if 'DX_CLI_WD' in dxpy.config:
            del dxpy.config['DX_CLI_WD']

    def tearDown(self):
        dxpy.api.project_destroy(self.project, {'terminateJobs': True})

    def project_describe(self, input_params):
        return dxpy.api.project_describe(self.project, input_params)

    def test_update_strings(self):
        update_items = {'name': 'NewProjectName' + str(time.time()),
                        'summary': 'This is a summary',
                        'description': 'This is a description'}

        # Update items one by one.
        for item in update_items:
            run(self.cmd.format(pid=self.project, item=item, n=pipes.quote(update_items[item])))
            describe_input = {}
            describe_input[item] = 'true'
            self.assertEqual(self.project_describe(describe_input)[item],
                             update_items[item])

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_PROJ_UPDATE_OPTIONS"])
    def test_update_multiple_items(self):
        # Test updating multiple items in a single api call
        update_items = {'name': 'NewProjectName' + str(time.time()),
                        'summary': 'This is new a summary',
                        'description': 'This is new a description',
                        'protected': 'false'}

        update_project_output = check_output(["dx", "update", "project", self.project, "--name",
                                              pipes.quote(update_items['name']), "--summary", update_items['summary'],
                                              "--description",
                                              update_items['description'], "--protected", update_items['protected']])
        update_project_json = json.loads(update_project_output);
        self.assertTrue("id" in update_project_json)
        self.assertEqual(self.project, update_project_json["id"])

        update_project_output = check_output(["dx", "update", "project", self.project, "--name",
                                              pipes.quote(update_items['name']), "--summary", update_items['summary'],
                                              "--description",
                                              update_items['description'], "--protected", update_items['protected'],
                                              "--brief"])
        self.assertEqual(self.project, update_project_output.rstrip("\n"))

        describe_input = {}
        for item in update_items:
            describe_input[item] = 'true'

        result = self.project_describe(describe_input)

        for item in update_items:
            if item == 'protected':
                self.assertFalse(result[item])
            else:
                self.assertEqual(result[item], update_items[item])

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_API_PROJ_RENAME_PROJECT"])
    def test_update_project_by_name(self):
        describe_input = {}
        describe_input['name'] = 'true'

        project_name = self.project_describe(describe_input)['name']
        new_name = 'Another Project Name' + str(time.time())

        run(self.cmd.format(pid=project_name, item='name', n=pipes.quote(new_name)))
        result = self.project_describe(describe_input)
        self.assertEqual(result['name'], new_name)

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_API_PROJ_ENABLE_PROTECTED_OPTION"])
    def test_update_booleans(self):
        update_items = {'protected': 'true',
                        'restricted': 'true'}

        for item in update_items:
            run(self.cmd.format(pid=self.project, item=item, n=update_items[item]))
            describe_input = {}
            describe_input[item] = 'true'
            self.assertTrue(self.project_describe(describe_input)[item])

    def test_bill_non_existent_user(self):
        # Test that the api returns an invalid input when giving a non existing user
        cmd = "dx update project {pid} --bill-to user-wronguser"

        with self.assertSubprocessFailure(stderr_text="InvalidInput"):
            run(cmd.format(pid=self.project))


@unittest.skipUnless(testutil.TEST_HTTP_PROXY,
                     'skipping HTTP Proxy support test that needs squid3')
class TestHTTPProxySupport(DXTestCase):
    def setUp(self):
        squid_wd = os.path.join(os.path.dirname(__file__), 'http_proxy')
        self.proxy_process = subprocess.Popen(['squid3', '-N', '-f', 'squid.conf'], cwd=squid_wd)
        time.sleep(1)

        print("Waiting for squid to come up...")
        t = 0
        while True:
            try:
                if requests.get("http://localhost:3129").status_code == requests.codes.bad_request:
                    if self.proxy_process.poll() is not None:
                        # Got a response on port 3129, but our proxy
                        # quit with an error, so it must be another
                        # process.
                        raise Exception("Tried launching squid, but port 3129 is already bound")
                    print("squid is up")
                    break
            except requests.exceptions.RequestException:
                pass
            time.sleep(0.5)
            t += 1
            if t > 16:
                raise Exception("Failed to launch Squid")

        self.proxy_env_no_auth = os.environ.copy()
        self.proxy_env_no_auth["HTTP_PROXY"] = "http://localhost:3129"
        self.proxy_env_no_auth["HTTPS_PROXY"] = "http://localhost:3129"

        self.proxy_env = os.environ.copy()
        self.proxy_env["HTTP_PROXY"] = "http://proxyuser:proxypassword@localhost:3129"
        self.proxy_env["HTTPS_PROXY"] = "http://proxyuser:proxypassword@localhost:3129"

    def test_proxy(self):
        run("dx find projects", env=self.proxy_env)
        with self.assertSubprocessFailure(stderr_regexp="407 Proxy Authentication Required"):
            run("dx find projects", env=self.proxy_env_no_auth)

    def tearDown(self):
        self.proxy_process.terminate()


class TestDXBuildWorkflow(DXTestCaseBuildWorkflows):

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create global workflows')
    def test_build_single_region_workflow(self):
        gwf_name = "gwf_{t}_single_region".format(t=int(time.time()))
        dxworkflow_json = dict(self.dxworkflow_spec, name=gwf_name)
        workflow_dir = self.write_workflow_directory(gwf_name,
                                                     json.dumps(dxworkflow_json),
                                                     readme_content="Workflow Readme Please")

        gwf = json.loads(run("dx build --create-globalworkflow --json " + workflow_dir))
        gwf_describe = json.loads(run("dx describe --json " + gwf["id"]))
        self.assertEqual(gwf_describe["class"], "globalworkflow")
        self.assertEqual(gwf_describe["id"], gwf_describe["id"])
        self.assertEqual(gwf_describe["version"], "0.0.1")
        self.assertEqual(gwf_describe["name"], gwf_name)
        self.assertFalse("published" in gwf_describe)
        self.assertIn("regionalOptions", gwf_describe)
        self.assertItemsEqual(list(gwf_describe["regionalOptions"].keys()), ["aws:us-east-1"])

        # We can also create a regular workflow from this dxworkflow.json
        wf = json.loads(run("dx build --json " + workflow_dir))
        wf_describe = json.loads(run("dx describe --json " + wf["id"]))
        self.assertEqual(wf_describe["class"], "workflow")

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create global workflows')
    def test_build_workflow_warnings(self):
        gwf_name = "Test_build_workflow_warnings".format(t=int(time.time()))
        dxworkflow_json = dict(self.dxworkflow_spec, name=gwf_name, version="foo")
        del dxworkflow_json['title']
        workflow_dir = self.write_workflow_directory("test_build_workflow_warnings",
                                                     json.dumps(dxworkflow_json))

        unexpected_warnings = ["missing a name",
                               "should be a short phrase not ending in a period"]
        expected_warnings = [
            "should be all lowercase",
            "does not match containing directory",
            "missing a title",
            "missing a summary",
            # "missing a description",
            "should be semver compliant"]
        try:
            # Expect "dx build" to succeed, exit with error code to
            # grab stderr.
            run("dx build --globalworkflow " + workflow_dir + " && exit 28")
        except subprocess.CalledProcessError as err:
            self.assertEqual(err.returncode, 28)
            for warning in unexpected_warnings:
                self.assertNotIn(warning, err.stderr)
            for warning in expected_warnings:
                self.assertIn(warning, err.stderr)

    def test_build_workflow_invalid_project_context(self):
        gwf_name = "invalid_project_context_{t}".format(t=int(time.time()))
        dxworkflow_json = dict(self.dxworkflow_spec, name=gwf_name)
        workflow_dir = self.write_workflow_directory(gwf_name,
                                                     json.dumps(dxworkflow_json))

        # Set the project context to a nonexistent project. This
        # should result in an error since currently the workflow
        # will be enabled in the region of the project context
        # (if regionalOptions or --region are not set)
        env = override_environment(DX_PROJECT_CONTEXT_ID='project-B00000000000000000000000')
        with self.assertRaisesRegexp(subprocess.CalledProcessError, "ResourceNotFound"):
            run("dx build --create-globalworkflow --json " + workflow_dir, env=env)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create global workflows')
    def test_build_workflow_with_bill_to(self):
        alice_id = "user-alice"
        org_id = "org-piratelabs"

        # --bill-to is not specified with dx build
        gwf_name = "globalworkflow_build_bill_to_user"
        dxworkflow_json = dict(self.dxworkflow_spec, name=gwf_name)
        workflow_dir = self.write_workflow_directory(gwf_name,
                                                     json.dumps(dxworkflow_json))
        new_gwf = json.loads(run("dx build --globalworkflow --json " + workflow_dir))
        self.assertEqual(new_gwf["billTo"], alice_id)

        # --bill-to is set to org with dx build
        gwf_name = "globalworkflow_build_bill_to_org"
        dxworkflow_json = dict(self.dxworkflow_spec, name=gwf_name)
        workflow_dir = self.write_workflow_directory(gwf_name,
                                                     json.dumps(dxworkflow_json))
        new_gwf = json.loads(run("dx build --globalworkflow --bill-to {} --json {}".format(org_id, workflow_dir)))
        self.assertEqual(new_gwf["billTo"], org_id)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create global workflows')
    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_WORKFLOW_REMOVE_AUTHORIZED_USERS_GLOBALWF",
                                          "DNA_CLI_WORKFLOW_LIST_AUTHORIZED_USERS_GLOBALWF",
                                          "DNA_CLI_WORKFLOW_ADD_AUTHORIZED_USERS_GLOBALWF"])
    def test_dx_add_list_remove_users_of_global_workflows(self):
        """
        This test is for some other dx subcommands, but it's in this
        test suite to take advantage of workflow-building methods.
        """
        # Only create if it's not available already (makes
        # local testing easier)
        try:
            workflow_desc = dxpy.api.global_workflow_describe("globalworkflow-test_dx_users", {})
            workflow_id = workflow_desc["id"]
            # reset users to empty list
            run("dx remove users globalworkflow-test_dx_users " + " ".join(workflow_desc["authorizedUsers"]))
        except:
            workflow_id = None
        if workflow_id is None:
            gwf_name = "wf_test_dx_users"
            dxworkflow_json = dict(self.dxworkflow_spec, name=gwf_name)
            workflow_dir = self.write_workflow_directory(gwf_name,
                                                         json.dumps(dxworkflow_json))
            workflow_id = json.loads(run("dx build --create-globalworkflow --json " + workflow_dir))['id']
        # don't use "globalworkflow-" prefix, duplicate and multiple members are fine
        run("dx add users wf_test_dx_users eve user-eve org-piratelabs")
        users = run("dx list users globalworkflow-wf_test_dx_users").strip().split("\n")
        self.assertEqual(len(users), 2)
        self.assertIn("user-eve", users)
        self.assertIn("org-piratelabs", users)
        run("dx remove users wf_test_dx_users eve org-piratelabs")
        # use version string
        run("dx list users globalworkflow-wf_test_dx_users/0.0.1")

        # bad paths and exit codes
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx add users nonexistentgwf user-eve')
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx list users globalworkflow-nonexistentgwf')
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx remove users globalworkflow-nonexistentgwf/1.0.0 user-eve')
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run('dx add users wf_test_dx_users org-nonexistentorg')
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run('dx add users wf_test_dx_users nonexistentuser')
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run('dx add users wf_test_dx_users piratelabs')

        # ResourceNotFound is not thrown when removing things
        run('dx remove users wf_test_dx_users org-nonexistentorg')
        run('dx remove users wf_test_dx_users nonexistentuser')
        run('dx remove users wf_test_dx_users piratelabs')

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_WORKFLOW_ADD_DEVELOPERS_GLOBALWF",
                                          "DNA_CLI_WORKFLOW_LIST_DEVELOPERS_GLOBALWF",
                                          "DNA_CLI_WORKFLOW_REMOVE_DEVELOPERS_GLOBALWF"])
    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create global workflows')
    def test_dx_add_list_remove_developers_of_global_workflows(self):
        '''
        This test is for some other dx subcommands, but it's in this
        test suite to take advantage of workflow-building methods.
        '''
        # Only create if it's not available already (makes
        # local testing easier)
        try:
            workflow_desc = dxpy.api.global_workflow_describe("globalworkflow-wf_test_dx_developers", {})
            workflow_id = workflow_desc["id"]
            my_userid = workflow_desc["createdBy"]
            developers = dxpy.api.global_workflow_list_developers("globalworkflow-wf_test_dx_developers", {})[
                "developers"]
            # reset developers to default list
            if len(developers) != 1:
                run("dx remove developers globalworkflow-wf_test_dx_developers " +
                    " ".join([dev for dev in developers if dev != my_userid]))
        except:
            workflow_id = None
        if workflow_id is None:
            gwf_name = "wf_test_dx_developers"
            dxworkflow_json = dict(self.dxworkflow_spec, name=gwf_name)
            workflow_dir = self.write_workflow_directory(gwf_name,
                                                         json.dumps(dxworkflow_json))
            workflow_desc = json.loads(run("dx build --create-globalworkflow --json " + workflow_dir))
            workflow_id = workflow_desc['id']
            my_userid = workflow_desc["createdBy"]
        developers = run("dx list developers globalworkflow-wf_test_dx_developers").strip()
        self.assertEqual(developers, my_userid)

        # use hash ID
        run("dx add developers " + workflow_id + " eve")
        developers = run("dx list developers globalworkflow-wf_test_dx_developers").strip().split("\n")
        self.assertEqual(len(developers), 2)
        self.assertIn(my_userid, developers)
        # don't use "globalworkflow-" prefix, duplicate, multiple, and non- members are fine
        run("dx remove developers wf_test_dx_developers PUBLIC eve user-eve org-piratelabs")
        developers = run("dx list developers globalworkflow-wf_test_dx_developers").strip()
        self.assertEqual(developers, my_userid)
        # use version string
        run("dx list developers globalworkflow-wf_test_dx_developers/0.0.1")

        # bad paths and exit codes
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx list developers globalworkflow-nonexistent')
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx remove developers globalworkflow-nonexistent/1.0.0 eve')
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run('dx add developers wf_test_dx_developers nonexistentuser')
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run('dx add developers wf_test_dx_developers piratelabs')

        # ResourceNotFound is not thrown when removing things
        run('dx remove developers wf_test_dx_developers org-nonexistentorg')
        run('dx remove developers wf_test_dx_developers nonexistentuser')
        run('dx remove developers wf_test_dx_developers piratelabs')

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_WORKFLOW_PUBLISH_GLOBALWF"])
    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create global workflows')
    def test_dx_publish_global_workflow(self):
        gwf_name = "dx_publish_wf"

        def _create_global_workflow(version):
            dxworkflow_json = dict(self.dxworkflow_spec, name=gwf_name, version=version)
            workflow_dir = self.write_workflow_directory(gwf_name, json.dumps(dxworkflow_json))
            desc = json.loads(run("dx build --globalworkflow {wf_dir} --json".format(wf_dir=workflow_dir)))
            return desc

        # create two versions
        _create_global_workflow("1.0.0")
        desc = _create_global_workflow("2.0.0")
        self.assertFalse("default" in desc["aliases"])

        # version must be explicitly specified
        with self.assertSubprocessFailure(stderr_regexp="Version is required", exit_code=3):
            run("dx publish {name}".format(name=gwf_name))

        run("dx publish {name}/{version}".format(name=gwf_name, version="2.0.0"))
        published_desc = json.loads(run("dx describe globalworkflow-{name}/{version} --json".format(name=gwf_name,
                                                                                                    version="2.0.0")))
        self.assertTrue("published" in published_desc)
        self.assertTrue("default" in published_desc["aliases"])

        with self.assertSubprocessFailure(stderr_regexp="already published", exit_code=3):
            run("dx publish {name}/{version}".format(name=gwf_name, version="2.0.0"))


class TestSparkClusterApps(DXTestCaseBuildApps):

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_build_and_get_cluster_app_bootstrap_script_inlined(self):
        app_name = "cluster_app"
        cluster_spec_with_bootstrap_aws = {"type": "spark",
                                           "version": "2.4.0",
                                           "initialInstanceCount": 5,
                                           "bootstrapScript": "clusterBootstrapAws.py"}
        cluster_spec_with_bootstrap_azure = cluster_spec_with_bootstrap_aws.copy()
        cluster_spec_with_bootstrap_azure['bootstrapScript'] = "clusterBootstrapAzure.py"
        cluster_spec_no_bootstrap = {"type": "spark",
                                     "version": "2.4.0",
                                     "initialInstanceCount": 10}
        bootstrap_code_aws = "def improper():\nprint 'oops'"  # syntax error
        bootstrap_code_azure = "import os\n"

        # cluster spec must be specified under "regionalOptions"
        non_regional_app_spec = dict(self.base_app_spec, name=app_name)
        non_regional_app_spec["runSpec"]["systemRequirements"] = dict(
            main=dict(instanceType="mem2_hdd2_x1", clusterSpec=cluster_spec_with_bootstrap_aws)
        )
        app_dir = self.write_app_directory(app_name, json.dumps(non_regional_app_spec), "code.py")
        with self.assertSubprocessFailure(
                stderr_regexp="clusterSpec.*must be specified.*under the \"regionalOptions\" field"):
            run("dx build " + app_dir)

        app_spec = dict(self.base_app_spec, name=app_name,
                        regionalOptions={
                            "aws:us-east-1": {
                                "systemRequirements": {
                                    "main": {
                                        "instanceType": "mem2_hdd2_x1",
                                        "clusterSpec": cluster_spec_with_bootstrap_aws
                                    },
                                    "cluster_2": {
                                        "instanceType": "mem2_hdd2_x4",
                                        "clusterSpec": cluster_spec_no_bootstrap
                                    },
                                    "cluster_3": {
                                        "instanceType": "mem2_hdd2_x1",
                                        "clusterSpec": cluster_spec_with_bootstrap_aws
                                    }
                                }
                            },
                            "azure:westus": {
                                "systemRequirements": {
                                    "main": {
                                        "instanceType": "azure:mem1_ssd1_x2",
                                        "clusterSpec": cluster_spec_with_bootstrap_azure
                                    }
                                }
                            }})
        del app_spec["runSpec"]["systemRequirements"]
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")
        self.write_app_directory(app_name, json.dumps(app_spec), "clusterBootstrapAws.py",
                                 code_content=bootstrap_code_aws)
        self.write_app_directory(app_name, json.dumps(app_spec), "clusterBootstrapAzure.py",
                                 code_content=bootstrap_code_azure)

        # confirm syntax checking
        with self.assertSubprocessFailure(stderr_regexp="Code in cluster bootstrapScript \\S+ has syntax errors"):
            run("dx build " + app_dir)
        # get rid of syntax error
        bootstrap_code_aws = "import sys\n"
        self.write_app_directory(app_name, json.dumps(app_spec), "clusterBootstrapAws.py",
                                 code_content=bootstrap_code_aws)

        def build_and_verify_bootstrap_script_inlined(app_dir):
            # build cluster app with multiple bootstrap scripts and regions
            # expect bootstrap scripts to be inlined in the app doc
            app_doc = json.loads(run("dx build --create-app --json " + app_dir))
            sys_reqs = app_doc["runSpec"]["systemRequirements"]
            self.assertEqual(sys_reqs["main"]["clusterSpec"]["bootstrapScript"], bootstrap_code_aws)
            self.assertEqual(sys_reqs["cluster_3"]["clusterSpec"]["bootstrapScript"], bootstrap_code_aws)
            self.assertFalse("bootstrapScript" in sys_reqs["cluster_2"]["clusterSpec"])
            self.assertEqual(app_doc["runSpec"]['systemRequirementsByRegion']["azure:westus"]["main"]["clusterSpec"][
                                 "bootstrapScript"], bootstrap_code_azure)
            return app_doc["id"]

        app_id = build_and_verify_bootstrap_script_inlined(app_dir)

        # get same cluster app
        # expect each bootstrap script to be in its own file referenced by the corresponding entry point
        with chdir(tempfile.mkdtemp()):
            run("dx get " + app_id)
            self.assertTrue(os.path.exists("cluster_app"))
            self.assertTrue(os.path.exists(os.path.join("cluster_app", "dxapp.json")))
            dxapp_json = json.loads(open(os.path.join("cluster_app", "dxapp.json")).read())
            aws_sys_reqs = dxapp_json["regionalOptions"]["aws:us-east-1"]["systemRequirements"]
            azure_sys_reqs = dxapp_json["regionalOptions"]["azure:westus"]["systemRequirements"]

            # bootstrap script names should now be: <region>_<entry-point>_clusterBootstrap.<lang>
            self.assertEqual(aws_sys_reqs["main"]["clusterSpec"]["bootstrapScript"],
                             "src/aws:us-east-1_main_clusterBootstrap.py")
            with open("cluster_app/src/aws:us-east-1_main_clusterBootstrap.py") as f:
                self.assertEqual(f.read(), bootstrap_code_aws)

            # this clusterSpec had no bootstrapScript
            self.assertFalse("bootstrapScript" in aws_sys_reqs["cluster_2"]["clusterSpec"])

            self.assertEqual(aws_sys_reqs["cluster_3"]["clusterSpec"]["bootstrapScript"],
                             "src/aws:us-east-1_cluster_3_clusterBootstrap.py")
            with open("cluster_app/src/aws:us-east-1_cluster_3_clusterBootstrap.py") as f:
                self.assertEqual(f.read(), bootstrap_code_aws)

            self.assertEqual(azure_sys_reqs["main"]["clusterSpec"]["bootstrapScript"],
                             "src/azure:westus_main_clusterBootstrap.py")
            with open("cluster_app/src/azure:westus_main_clusterBootstrap.py") as f:
                self.assertEqual(f.read(), bootstrap_code_azure)

            # now rebuild with the result of `dx get` and verify that we get the same result
            build_and_verify_bootstrap_script_inlined("cluster_app")


class TestDXBuildApp(DXTestCaseBuildApps):
    def run_and_assert_stderr_matches(self, cmd, stderr_regexp):
        with self.assertSubprocessFailure(stderr_regexp=stderr_regexp, exit_code=28):
            run(cmd + ' && exit 28')

    def test_help_without_security_context(self):
        env = override_environment(DX_SECURITY_CONTEXT=None, DX_APISERVER_HOST=None,
                                   DX_APISERVER_PORT=None, DX_APISERVER_PROTOCOL=None)
        run("dx build -h", env=env)

    def test_accepts_semver(self):
        self.assertTrue(dxpy.executable_builder.GLOBAL_EXEC_VERSION_RE.match('3.1.41') is not None)
        self.assertTrue(dxpy.executable_builder.GLOBAL_EXEC_VERSION_RE.match('3.1.41-rc.1') is not None)
        self.assertFalse(dxpy.executable_builder.GLOBAL_EXEC_VERSION_RE.match('3.1.41-rc.1.') is not None)
        self.assertFalse(dxpy.executable_builder.GLOBAL_EXEC_VERSION_RE.match('3.1.41-rc..1') is not None)
        self.assertTrue(dxpy.executable_builder.GLOBAL_EXEC_VERSION_RE.match('22.0.999+git.abcdef') is not None)
        self.assertFalse(dxpy.executable_builder.GLOBAL_EXEC_VERSION_RE.match('22.0.999+git.abcdef$') is not None)
        self.assertFalse(dxpy.executable_builder.GLOBAL_EXEC_VERSION_RE.match('22.0.999+git.abcdef.') is not None)
        self.assertTrue(dxpy.executable_builder.GLOBAL_EXEC_VERSION_RE.match('22.0.999-rc.1+git.abcdef') is not None)

    def test_version_suffixes(self):
        app_spec = dict(self.base_app_spec, name="test_versioning_åpp")
        app_dir = self.write_app_directory("test_versioning_app", json.dumps(app_spec), "code.py")
        self.assertTrue(dx_build_app._get_version_suffix(app_dir, '1.0.0').startswith('+build.'))
        self.assertTrue(dx_build_app._get_version_suffix(app_dir, '1.0.0+git.abcdef')
                        .startswith('.build.'))

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_APP_UPLOAD_BUILD_NEW_APPLET"])
    def test_build_applet(self):
        app_spec = dict(self.base_app_spec, name="minimal_applet")
        app_dir = self.write_app_directory("minimal_åpplet", json.dumps(app_spec), "code.py")
        new_applet = run_and_parse_json("dx build --json " + app_dir)
        applet_describe = dxpy.get_handler(new_applet["id"]).describe()
        self.assertEqual(applet_describe["class"], "applet")
        self.assertEqual(applet_describe["id"], applet_describe["id"])
        self.assertEqual(applet_describe["name"], "minimal_applet")

    def test_dx_build_applet_dxapp_json_created_with_makefile(self):
        app_name = "nodxapp_applet"
        app_dir = self.write_app_directory(app_name, None, "code.py")
        app_spec = dict(self.base_app_spec, name=app_name)
        makefile_str = "dxapp.json:\n\tcp temp_dxapp.json dxapp.json\n"
        with open(os.path.join(app_dir, 'temp_dxapp.json'), 'wb') as manifest:
            manifest.write(json.dumps(app_spec).encode())
        with open(os.path.join(app_dir, "Makefile"), 'w') as makefile:
            makefile.write(makefile_str)
        run("dx build " + app_dir)

    def test_dx_build_applet_no_app_linting(self):
        run("dx clearenv")

        # Case: Missing title, summary, description.
        app_spec = dict(self.base_app_spec,
                        name="dx_build_applet_missing_fields",
                        categories=["Annotation"])
        app_dir = self.write_app_directory("dx_build_applet_missing_fields", json.dumps(app_spec), "code.py")
        args = ['dx', 'build', app_dir]
        p = subprocess.Popen(args, stderr=subprocess.PIPE)
        out, err = p.communicate()
        self.assertFalse(err.decode("utf-8").startswith("WARNING"))

        # Case: Usage of period at end of summary.
        app_spec = dict(self.base_app_spec,
                        name="dx_build_applet_summary_without_period",
                        title="Title",
                        summary="Summary without period",
                        description="Description with period.",
                        categories=["Annotation"])
        app_dir = self.write_app_directory("dx_build_applet_summary_without_period", json.dumps(app_spec), "code.py")
        args = ['dx', 'build', app_dir]
        p = subprocess.Popen(args, stderr=subprocess.PIPE)
        out, err = p.communicate()
        self.assertFalse(err.decode("utf-8").startswith("WARNING"))

        # Case: Usage of unknown categories.
        unknown_category = "asdf1234"
        app_spec = dict(self.base_app_spec,
                        name="dx_build_applet_unknown_cat",
                        title="Title",
                        summary="Summary without period",
                        description="Description without period",
                        categories=[unknown_category])
        app_dir = self.write_app_directory("dx_build_applet_unknown_cat", json.dumps(app_spec), "code.py")
        args = ['dx', 'build', app_dir]
        p = subprocess.Popen(args, stderr=subprocess.PIPE)
        out, err = p.communicate()
        self.assertFalse(err.decode("utf-8").startswith("WARNING"))

    def test_build_applet_dry_run(self):
        app_spec = dict(self.base_app_spec, name="minimal_applet_dry_run")
        app_dir = self.write_app_directory("minimal_applet_dry_run", json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp='cannot be specified together', exit_code=2):
            run("dx build --dry-run " + app_dir + " --run -y --brief")
        run("dx build --dry-run " + app_dir)
        self.assertEqual(len(list(dxpy.find_data_objects(name="minimal_applet_dry_run"))), 0)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    def test_build_applet_and_run_immediately(self):
        app_spec = dict(self.base_app_spec, name="minimal_applet_to_run")
        app_dir = self.write_app_directory("minimal_åpplet_to_run", json.dumps(app_spec), "code.py")
        job_id = run("dx build " + app_dir + ' --run -y --brief').strip()
        job_desc = json.loads(run('dx describe --json ' + job_id))
        # default priority should be high for running after building
        # an applet
        self.assertEqual(job_desc['name'], 'minimal_applet_to_run')
        self.assertEqual(job_desc['priority'], 'high')

        # if priority is explicitly requested as normal, it should be
        # honored
        job_id = run("dx build -f " + app_dir + ' --run --priority normal -y --brief').strip()
        job_desc = json.loads(run('dx describe --json ' + job_id))
        self.assertEqual(job_desc['name'], 'minimal_applet_to_run')
        self.assertEqual(job_desc['priority'], 'normal')

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    def test_remote_build_applet_and_run_immediately(self):
        app_spec = dict(self.base_app_spec, name="minimal_remote_build_applet_to_run")
        app_dir = self.write_app_directory("minimal_remote_build_åpplet_to_run", json.dumps(app_spec),
                                           "code.py")
        job_name = ("remote_build_test_run_" + str(int(time.time() * 1000)) + "_" +
                    str(random.randint(0, 1000)))
        run("dx build --remote " + app_dir + " --run -y --name=" + job_name)
        resulting_jobs = list(dxpy.find_executions(name=job_name, project=self.project, return_handler=True))
        self.assertEqual(1, len(resulting_jobs))
        self.assertEqual('minimal_remote_build_applet_to_run',
                         resulting_jobs[0].describe()['executableName'])

    @unittest.skipUnless(testutil.TEST_RUN_JOBS and testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps and run jobs')
    def test_remote_build_app(self):
        app_spec = dict(self.base_app_spec, name="minimal_remote_build_app")
        app_dir = self.write_app_directory("minimal_remote_build_åpp", json.dumps(app_spec), "code.py")
        run("dx build --remote --app " + app_dir)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS and testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps and run jobs')
    def test_remote_build_app_trusty(self):
        app_spec = {
            "name": "minimal_remote_build_app_trusty",
            "dxapi": "1.0.0",
            # Use a package specific to trusty but not in precise as part of the execdepends to ensure it is installed properly
            "runSpec": {"file": "code.py", "interpreter": "python2.7",
                        "distribution": "Ubuntu", "release": "14.04",
                        "buildDepends": [{"name": "postgresql-9.3"}],
                        "systemRequirements": {"*": {"instanceType": "mem1_ssd1_x4"}}},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
        }
        app_dir = self.write_app_directory("minimal_remote_build_åpp_trusty", json.dumps(app_spec), "code.py")
        run("dx build --remote --app " + app_dir)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS and testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps and run jobs')
    def test_remote_build_applet(self):
        app_spec = dict(self.base_app_spec, name="minimal_remote_build_applet")
        app_dir = self.write_app_directory("minimal_remote_build_åpplet", json.dumps(app_spec), "code.py")
        run("dx build --remote " + app_dir)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS and testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps and run jobs')
    def test_remote_build_applet_trusty(self):
        app_spec = {
            "name": "minimal_remote_build_applet_trusty",
            "dxapi": "1.0.0",
            # Use a package specific to trusty but not in precise as part of the execdepends to ensure it is installed properly
            "runSpec": {"file": "code.py", "interpreter": "python2.7",
                        "distribution": "Ubuntu", "release": "14.04",
                        "buildDepends": [{"name": "postgresql-9.3"}],
                        "systemRequirements": {"*": {"instanceType": "mem1_ssd1_x4"}}},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
        }
        app_dir = self.write_app_directory("minimal_remote_build_åpplet_trusty", json.dumps(app_spec), "code.py")
        run("dx build --remote " + app_dir)

    def test_cannot_remote_build_multi_region_app(self):
        app_name = "asset_{t}_remote_multi_region_app".format(t=int(time.time()))
        app_spec = dict(self.base_app_spec, name=app_name)
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp='--region.*once for remote', exit_code=2):
            run("dx build --remote --app --region aws:us-east-1 --region azure:westus " + app_dir)

    def test_remote_build_app_and_run_immediately(self):
        app_spec = dict(self.base_app_spec, name="minimal_remote_build_app_to_run")
        app_dir = self.write_app_directory("minimal_remote_build_åpp_to_run", json.dumps(app_spec),
                                           "code.py")
        # Not supported yet
        with self.assertSubprocessFailure(stderr_regexp='cannot all be specified together', exit_code=2):
            run("dx build --remote --app " + app_dir + " --run --yes")

    def test_build_applet_warnings(self):
        app_spec = {
            "title": "title",
            "summary": "a summary sentence.",
            "description": "foo",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7",
                        "distribution": "Ubuntu", "release": "14.04"},
            "inputSpec": [{"name": "34", "class": "int"}],
            "outputSpec": [{"name": "92", "class": "string"}],
            "version": "1.0.0",
            "categories": ["foo", "Import", "Export"]
        }
        app_dir = self.write_app_directory("test_build_åpplet_warnings", json.dumps(app_spec), "code.py")
        with open(os.path.join(app_dir, 'Readme.md'), 'w') as readme:
            readme.write('a readme file')
        applet_expected_warnings = ["missing a name",
                                    'input 0 has illegal name',
                                    'output 0 has illegal name']
        applet_unexpected_warnings = ["should be all lowercase",
                                      "does not match containing directory",
                                      "missing a title",
                                      "missing a summary",
                                      "should be a short phrase not ending in a period",
                                      "missing a description",
                                      '"description" field shadows file',
                                      '"description" field should be written in complete sentences',
                                      'unrecognized category',
                                      'should end in "Importer"',
                                      'should end in "Exporter"',
                                      "should be semver compliant"]
        try:
            run("dx build " + app_dir)
            self.fail("dx build invocation should have failed because of bad IO spec")
        except subprocess.CalledProcessError as err:
            for warning in applet_expected_warnings:
                self.assertIn(warning, err.stderr)
            for warning in applet_unexpected_warnings:
                self.assertNotIn(warning, err.stderr)

        # some more errors
        app_spec = {
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py"}
        }
        app_dir = self.write_app_directory("test_build_second_åpplet_warnings", json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp='interpreter field was not present'):
            run("dx build " + app_dir)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_build_app_warnings(self):
        app_spec = dict(self.base_app_spec, name="Foo", version="foo")
        app_dir = self.write_app_directory("test_build_app_warnings", json.dumps(app_spec), "code.py")
        app_unexpected_warnings = ["missing a name",
                                   "should be a short phrase not ending in a period",
                                   '"description" field shadows file',
                                   '"description" field should be written in complete sentences',
                                   'unrecognized category',
                                   'should end in "Importer"',
                                   'should end in "Exporter"',
                                   'input 0 has illegal name',
                                   'output 0 has illegal name']
        app_expected_warnings = ["should be all lowercase",
                                 "does not match containing directory",
                                 "missing a title",
                                 "missing a summary",
                                 "missing a description",
                                 "should be semver compliant"]
        try:
            # Expect "dx build" to succeed, exit with error code to
            # grab stderr.
            run("dx build --app " + app_dir + " && exit 28")
        except subprocess.CalledProcessError as err:
            self.assertEqual(err.returncode, 28)
            for warning in app_unexpected_warnings:
                self.assertNotIn(warning, err.stderr)
            for warning in app_expected_warnings:
                self.assertIn(warning, err.stderr)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create apps')
    def test_build_app_suggestions(self):
        app_spec = {
            "name": "test_build_app_suggestions",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7",
                        "distribution": "Ubuntu", "release": "14.04"},
            "inputSpec": [{"name": "testname", "class": "file", "suggestions": []}],
            "outputSpec": [],
            "version": "0.0.1"
        }

        # check if project exists
        app_spec["inputSpec"][0]["suggestions"] = [
            {"name": "somename", "project": "project-0000000000000000000000NA", "path": "/"}]
        app_dir = self.write_app_directory("test_build_app_suggestions", json.dumps(app_spec), "code.py")
        res = run("dx build --app " + app_dir, also_return_stderr=True)
        self.assertIn('Suggested project {name} does not exist'.
                      format(name=app_spec["inputSpec"][0]["suggestions"][0]["project"]), res[1])

        # check path
        app_spec["inputSpec"][0]["suggestions"] = [{"name": "somename", "project": self.project,
                                                    "path": "/some_invalid_path"}]
        app_dir = self.write_app_directory("test_build_app_suggestions", json.dumps(app_spec), "code.py")
        res = run("dx build --app " + app_dir, also_return_stderr=True)
        self.assertIn('Folder {path} could not be found in project'.
                      format(path=app_spec["inputSpec"][0]["suggestions"][0]["path"]), res[1])

        # check for $dnanexus_link
        app_spec["inputSpec"][0]["suggestions"] = [
            {"name": "somename", "$dnanexus_link": "file-0000000000000000000000NA"}]
        app_dir = self.write_app_directory("test_build_app_suggestions", json.dumps(app_spec), "code.py")
        try:
            run("dx build --app " + app_dir)
        except subprocess.CalledProcessError as err:
            self.assertIn('Suggested object {name} could not be found'.format
                          (name=app_spec["inputSpec"][0]["suggestions"][0]["$dnanexus_link"]), err.stderr)

        # check for value and $dnanexus_link in it
        app_spec["inputSpec"][0]["suggestions"] = [{"name": "somename",
                                                    "value": {"$dnanexus_link": "file-0000000000000000000000NA"}}]
        app_dir = self.write_app_directory("test_build_app_suggestions", json.dumps(app_spec), "code.py")
        try:
            run("dx build --app " + app_dir)
        except subprocess.CalledProcessError as err:
            self.assertIn('Suggested object {name} could not be found'.format
                          (name=app_spec["inputSpec"][0]["suggestions"][0]['value']["$dnanexus_link"]), err.stderr)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create apps')
    def test_build_app_suggestions_success(self):
        app_spec = {"name": "test_build_app_suggestions",
                    "dxapi": "1.0.0",
                    "runSpec": {"file": "code.py", "interpreter": "python2.7",
                                "distribution": "Ubuntu", "release": "14.04"},
                    "inputSpec": [{"name": "testname", "class": "file", "suggestions": []}],
                    "outputSpec": [], "version": "0.0.1"}

        # check when project not public and we publish app, also check app build with a valid suggestion
        app_spec["inputSpec"][0]["suggestions"] = [{"name": "somename", "project": self.project, "path": "/"}]
        app_dir = self.write_app_directory("test_build_app_suggestions", json.dumps(app_spec), "code.py")
        result = run("dx build --app --publish " + app_dir, also_return_stderr=True)
        if len(result) == 2:
            self.assertIn('NOT PUBLIC!'.format(name=app_spec['name']), result[1])
        app_id = json.loads(result[0])['id']
        app = dxpy.describe(app_id)
        self.assertEqual(app['name'], app_spec['name'])

    def test_build_applet_with_no_dxapp_json(self):
        app_dir = self.write_app_directory("åpplet_with_no_dxapp_json", None, "code.py")
        with self.assertSubprocessFailure(stderr_regexp='does not contain dxapp\.json', exit_code=3):
            run("dx build " + app_dir)

    def test_build_applet_with_malformed_dxapp_json(self):
        app_dir = self.write_app_directory("åpplet_with_malformed_dxapp_json", "{", "code.py")
        with self.assertSubprocessFailure(stderr_regexp='Could not parse dxapp\.json file', exit_code=3):
            run("dx build " + app_dir)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_API_APP_DESCRIBE"])
    def test_build_single_region_app_without_regional_options(self):
        # Backwards-compatible.
        app_name = "app_{t}_single_region".format(t=int(time.time()))
        app_spec = dict(self.base_app_spec, name=app_name)
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")
        new_app = json.loads(run("dx build --create-app --json " + app_dir))
        app_describe = json.loads(run("dx describe --json " + new_app["id"]))
        self.assertEqual(app_describe["class"], "app")
        self.assertEqual(app_describe["id"], app_describe["id"])
        self.assertEqual(app_describe["version"], "1.0.0")
        self.assertEqual(app_describe["name"], app_name)
        self.assertFalse("published" in app_describe)
        self.assertIn("regionalOptions", app_describe)
        self.assertItemsEqual(list(app_describe["regionalOptions"].keys()), ["aws:us-east-1"])

        self.assertTrue(os.path.exists(os.path.join(app_dir, 'code.py')))
        self.assertFalse(os.path.exists(os.path.join(app_dir, 'code.pyc')))

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_build_app_with_regional_options(self):
        app_name = "app_regional_options"
        app_spec = dict(self.base_app_spec, name=app_name, regionalOptions={"aws:us-east-1": {}})
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")
        new_app = json.loads(run("dx build --create-app --json " + app_dir))
        app_describe = json.loads(run("dx describe --json " + new_app["id"]))
        self.assertEqual(app_describe["class"], "app")
        self.assertEqual(app_describe["id"], new_app["id"])
        self.assertEqual(app_describe["version"], "1.0.0")
        self.assertEqual(app_describe["name"], app_name)
        self.assertFalse("published" in app_describe)
        self.assertIn("regionalOptions", app_describe)
        self.assertItemsEqual(list(app_describe["regionalOptions"].keys()), list(app_spec["regionalOptions"].keys()))

        self.assertTrue(os.path.exists(os.path.join(app_dir, 'code.py')))
        self.assertFalse(os.path.exists(os.path.join(app_dir, 'code.pyc')))

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_build_app_with_resources(self):
        region = "aws:us-east-1"
        with temporary_project(region=region) as tmp_project:
            file_id = create_file_in_project("abc", tmp_project.get_id())
            app_name = "app_resources"
            app_spec = dict(self.base_app_spec, name=app_name,
                            resources=tmp_project.get_id())
            app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")
            new_app = json.loads(run("dx build --create-app --json " + app_dir))
            self.assertIn("regionalOptions", new_app)
            self.assertIn(region, new_app["regionalOptions"])
            app_container = new_app["regionalOptions"][region]["resources"]
            container_content = dxpy.api.container_list_folder(app_container, {"folder": "/"})
            self.assertIn(file_id, [item["id"] for item in container_content["objects"]])

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV and testutil.TEST_AZURE,
                         'skipping test that would create apps')
    def test_build_multi_region_app(self):
        app_name = "multi_region_app"
        app_spec = dict(self.base_app_spec, name=app_name,
                        regionalOptions={"aws:us-east-1": {},
                                         "azure:westus": {}})
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")

        app_id = json.loads(run("dx build --create-app --json " + app_dir))["id"]
        app_desc_res = json.loads(run("dx describe --json " + app_id))
        self.assertEqual(app_desc_res["class"], "app")
        self.assertEqual(app_desc_res["id"], app_id)
        self.assertEqual(app_desc_res["version"], "1.0.0")
        self.assertEqual(app_desc_res["name"], app_name)
        self.assertFalse("published" in app_desc_res)
        self.assertIn("regionalOptions", app_desc_res)
        self.assertItemsEqual(list(app_desc_res["regionalOptions"].keys()), list(app_spec["regionalOptions"].keys()))

        self.assertTrue(os.path.exists(os.path.join(app_dir, 'code.py')))
        self.assertFalse(os.path.exists(os.path.join(app_dir, 'code.pyc')))

    def create_asset(self, tarball_name, proj):
        asset_archive = dxpy.upload_string("foo", name=tarball_name, project=proj.get_id())
        asset_archive.wait_on_close()
        asset = dxpy.new_dxrecord(
            project=proj.get_id(),
            details={"archiveFileId": {"$dnanexus_link": asset_archive.get_id()}},
            properties={"version": "0.0.1"}
        )
        asset.close()
        return asset.get_id()

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV and testutil.TEST_AZURE,
                         'skipping test that would create apps')
    def test_build_multi_region_app_with_regional_options(self):
        app_name = "multi_region_app_with_regional_options"

        with temporary_project(region="aws:us-east-1") as aws_proj:
            with temporary_project(region="azure:westus") as azure_proj:
                aws_bundled_dep = dxpy.upload_string("foo", project=aws_proj.get_id())
                aws_bundled_dep.close()
                azure_bundled_dep = dxpy.upload_string("foo", project=azure_proj.get_id())
                azure_bundled_dep.close()

                aws_asset = self.create_asset("aws_asset.tar.gz", aws_proj)
                azure_asset = self.create_asset("azure_asset.tar.gz", azure_proj)

                aws_sys_reqs = dict(main=dict(instanceType="mem2_hdd2_x1"))
                azure_sys_reqs = dict(main=dict(instanceType="azure:mem2_ssd1_x1"))

                aws_file_id = create_file_in_project("aws_file", aws_proj.get_id())
                azure_file_id_a = create_file_in_project("azure_a", azure_proj.get_id())
                azure_file_id_b = create_file_in_project("azure_b", azure_proj.get_id())
                app_spec = dict(
                    self.base_app_spec,
                    name=app_name,
                    regionalOptions={
                        "aws:us-east-1": dict(
                            systemRequirements=aws_sys_reqs,
                            bundledDepends=[{"name": "aws.tar.gz",
                                             "id": {"$dnanexus_link": aws_bundled_dep.get_id()}}],
                            assetDepends=[{"id": aws_asset}],
                            resources=aws_proj.get_id()
                        ),
                        "azure:westus": dict(
                            systemRequirements=azure_sys_reqs,
                            bundledDepends=[{"name": "azure.tar.gz",
                                             "id": {"$dnanexus_link": azure_bundled_dep.get_id()}}],
                            assetDepends=[{"id": azure_asset}],
                            resources=[azure_file_id_a, azure_file_id_b]
                        )
                    }
                )
                app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")
                app_id = json.loads(run("dx build --create-app --json " + app_dir))["id"]

                app_desc_res = dxpy.api.app_describe(app_id)
                self.assertEqual(app_desc_res["class"], "app")
                self.assertEqual(app_desc_res["id"], app_id)
                self.assertEqual(app_desc_res["version"], "1.0.0")
                self.assertEqual(app_desc_res["name"], app_name)

                self.assertIn("regionalOptions", app_desc_res)
                regional_options = app_desc_res["regionalOptions"]
                self.assertItemsEqual(list(regional_options.keys()), list(app_spec["regionalOptions"].keys()))

                aws_applet = regional_options["aws:us-east-1"]["applet"]
                self.assertEqual(dxpy.api.applet_describe(aws_applet)["runSpec"]["systemRequirements"],
                                 aws_sys_reqs)

                azure_applet = regional_options["azure:westus"]["applet"]
                self.assertEqual(dxpy.api.applet_describe(azure_applet)["runSpec"]["systemRequirements"],
                                 azure_sys_reqs)

                # Given an asset ID, returns the bundledDepends spec that the
                # inclusion of that asset would have generated
                def get_asset_spec(asset_id):
                    tarball_id = dxpy.DXRecord(asset_id).describe(
                        fields={'details'})["details"]["archiveFileId"]["$dnanexus_link"]
                    tarball_name = dxpy.DXFile(tarball_id).describe()["name"]
                    return {"name": tarball_name, "id": {"$dnanexus_link": tarball_id}}

                # Make sure the bundledDepends are the same as what we put
                # in: explicit bundledDepends first, then assets
                self.assertEqual(
                    app_desc_res["runSpec"]["bundledDependsByRegion"],
                    {region: opts["bundledDepends"] + [get_asset_spec(opts["assetDepends"][0]["id"])]
                     for region, opts in app_spec["regionalOptions"].items()}
                )

                # Make sure additional resources were cloned to the app containers
                # in the specified regions
                aws_container = regional_options["aws:us-east-1"]["resources"]
                aws_obj_id_list = dxpy.api.container_list_folder(aws_container, {"folder": "/"})
                self.assertIn(aws_file_id, [item["id"] for item in aws_obj_id_list["objects"]])

                azure_container = regional_options["azure:westus"]["resources"]
                azure_container_list = dxpy.api.container_list_folder(azure_container, {"folder": "/"})
                azure_obj_id_list = [item["id"] for item in azure_container_list["objects"]]
                self.assertIn(azure_file_id_a, azure_obj_id_list)
                self.assertIn(azure_file_id_b, azure_obj_id_list)

    def test_build_applets_using_multi_region_dxapp_json(self):
        app_name = "applet_{t}_multi_region_dxapp_json_with_regional_system_requirements".format(t=int(time.time()))

        aws_us_east_system_requirements = dict(main=dict(instanceType="mem2_hdd2_x1"))
        azure_westus_system_requirements = dict(main=dict(instanceType="azure:mem2_ssd1_x1"))
        # regionalOptions will be accepted but only the region in which the applet is
        # actually built will be read (and returned in describe output in
        # systemRequirementsByRegion), other regions' configs will be ignored
        app_spec = dict(self.base_app_spec, name=app_name,
                        regionalOptions={"aws:us-east-1": dict(systemRequirements=aws_us_east_system_requirements),
                                         "azure:westus": dict(systemRequirements=azure_westus_system_requirements)})
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")

        for region in ("aws:us-east-1", "azure:westus"):
            with temporary_project(region=region, select=True):
                applet_id = run_and_parse_json("dx build --json " + app_dir)["id"]
                applet_desc = dxpy.api.applet_describe(applet_id)
                bundled_depends_by_region = applet_desc["runSpec"]["bundledDependsByRegion"]
                self.assertEqual(list(bundled_depends_by_region.keys()), [region])
                sysreq_by_region = applet_desc["runSpec"]["systemRequirementsByRegion"]
                self.assertEqual(list(sysreq_by_region.keys()), [region])

    def test_cannot_build_applet_with_mismatching_regional_options(self):
        app_name = "applet_{t}_with_regional_system_requirements".format(t=int(time.time()))
        aws_us_east_system_requirements = dict(main=dict(instanceType="mem2_hdd2_x1"))
        app_spec = dict(self.base_app_spec, name=app_name,
                        regionalOptions={"aws:us-east-1": dict(systemRequirements=aws_us_east_system_requirements)})
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")

        with temporary_project(region="azure:westus", select=True):
            with self.assertRaisesRegex(DXCalledProcessError, "do not contain this region"):
                run("dx build --json " + app_dir)

    def test_build_multi_region_app_without_regional_options(self):
        app_name = "asset_{t}_multi_region_app".format(t=int(time.time()))
        app_spec = dict(self.base_app_spec, name=app_name)
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")

        cmd = "dx build --create-app --region aws:us-east-1 --region azure:westus --json {app_dir}".format(
            app_dir=app_dir)
        app_new_res = run_and_parse_json(cmd)
        app_desc_res = json.loads(run("dx describe --json " + app_new_res["id"]))
        self.assertEqual(app_desc_res["class"], "app")
        self.assertEqual(app_desc_res["id"], app_desc_res["id"])
        self.assertEqual(app_desc_res["version"], "1.0.0")
        self.assertEqual(app_desc_res["name"], app_name)
        self.assertFalse("published" in app_desc_res)
        self.assertIn("regionalOptions", app_desc_res)
        self.assertItemsEqual(list(app_desc_res["regionalOptions"].keys()), ["aws:us-east-1", "azure:westus"])
        self.assertTrue(os.path.exists(os.path.join(app_dir, 'code.py')))
        self.assertFalse(os.path.exists(os.path.join(app_dir, 'code.pyc')))

    def test_build_multi_region_app_with_resources_failure(self):
        error_message = "dxapp.json cannot contain a top-level \"resources\" field "
        error_message += "when the \"regionalOptions\" field is used or when "
        error_message += "the app is enabled in multiple regions"
        region_0 = "aws:us-east-1"
        region_1 = "azure:westus"
        with temporary_project(region=region_0) as tmp_project:
            # Build an app with top-level "resources" and enable it in multiple regions
            # with --region
            app_name = "asset_{t}_multi_region_app_resources".format(t=int(time.time()))
            app_spec = dict(self.base_app_spec, name=app_name,
                            resources=tmp_project.get_id())
            app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")
            cmd = "dx build --create-app --json --region {} --region {} {}".format(
                region_0, region_1, app_dir)
            with self.assertRaisesRegex(DXCalledProcessError, error_message):
                run(cmd)

            # Build an app where both top-level "resources" and "regionalOptions" are set
            app_name = "asset_{t}_multi_region_app_resources".format(t=int(time.time()))
            app_spec_resources_regionalOpts = dict(self.base_app_spec, name=app_name,
                                                   resources=tmp_project.get_id(),
                                                   regionalOptions={region_0: dict(resources=tmp_project.get_id())})
            app_dir = self.write_app_directory(app_name,
                                               json.dumps(app_spec_resources_regionalOpts),
                                               "code.py")
            with self.assertRaisesRegex(DXCalledProcessError, error_message):
                run("dx build --create-app --json " + app_dir)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV and testutil.TEST_AZURE,
                         'skipping test that would create apps')
    def test_update_multi_region_app(self):
        app_name = "asset_{t}_multi_region_app".format(t=int(time.time()))
        app_spec = dict(self.base_app_spec, name=app_name, regionalOptions={"aws:us-east-1": {}, "azure:westus": {}})
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")

        app_new_res = json.loads(run("dx build --create-app --json " + app_dir))
        app_desc_res = json.loads(run("dx describe --json " + app_new_res["id"]))
        self.assertIn("regionalOptions", app_desc_res)

        # The underlying applets of the newly created multi-region app.
        aws_applet = app_desc_res["regionalOptions"]["aws:us-east-1"]["applet"]
        azure_applet = app_desc_res["regionalOptions"]["azure:westus"]["applet"]

        # Update the multi-region app.
        app_new_res = json.loads(run("dx build --create-app --json " + app_dir))

        app_desc_res = json.loads(run("dx describe --json " + app_new_res["id"]))
        self.assertFalse("published" in app_desc_res)
        self.assertIn("regionalOptions", app_desc_res)

        new_aws_applet = app_desc_res["regionalOptions"]["aws:us-east-1"]["applet"]
        new_azure_applet = app_desc_res["regionalOptions"]["azure:westus"]["applet"]
        self.assertNotEqual(new_aws_applet, aws_applet)
        self.assertNotEqual(new_aws_applet, azure_applet)
        self.assertNotEqual(new_azure_applet, azure_applet)
        self.assertNotEqual(new_azure_applet, aws_applet)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV and testutil.TEST_AZURE,
                         'skipping test that would create apps')
    def test_build_multi_region_app_regional_options_empty(self):
        app_name = "asset_{t}_multi_region_app".format(t=int(time.time()))

        app_spec = dict(self.base_app_spec, name=app_name, regionalOptions={})
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")

        with self.assertSubprocessFailure(stderr_regexp="regionalOptions", exit_code=3):
            run("dx build --create-app --json " + app_dir)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV and testutil.TEST_AZURE,
                         'skipping test that would create apps')
    def test_build_multi_region_app_regions_do_not_match(self):
        # Regions specified in dxapp.json do not match the ones specified on
        # command-line.
        app_name = "asset_{t}_multi_region_app".format(t=int(time.time()))
        app_spec = dict(self.base_app_spec, name=app_name, regionalOptions={"aws:us-east-1": {}})
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")

        with self.assertSubprocessFailure(stderr_regexp="regionalOptions", exit_code=3):
            run("dx build --create-app --region azure:westus --json " + app_dir)

        app_name = "asset_{t}_multi_region_app".format(t=int(time.time()))
        app_spec = dict(self.base_app_spec, name=app_name,
                        regionalOptions={"azure:westus": {},
                                         "aws:us-east-1": {}})
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")

        with self.assertSubprocessFailure(stderr_regexp="regionalOptions", exit_code=3):
            run("dx build --create-app --region azure:westus --region aws:us-east-2 --json " + app_dir)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV and testutil.TEST_AZURE,
                         'skipping test that would create apps')
    def test_build_multi_region_app_with_malformed_regional_options(self):
        app_name = "multi_region_app_with_malformed_regional_options"

        with temporary_project(region="aws:us-east-1") as aws_proj:
            with temporary_project(region="azure:westus") as azure_proj:
                aws_bundled_dep = dxpy.upload_string("foo", project=aws_proj.get_id())
                aws_bundled_dep.close()
                azure_bundled_dep = dxpy.upload_string("foo", project=azure_proj.get_id())
                azure_bundled_dep.close()

                aws_asset = self.create_asset("aws_asset.tar.gz", aws_proj)
                azure_asset = self.create_asset("azure_asset.tar.gz", azure_proj)

                aws_sys_reqs = dict(main=dict(instanceType="mem2_hdd2_x1"))
                azure_sys_reqs = dict(main=dict(instanceType="azure:mem2_ssd1_x1"))

                app_spec = dict(
                    self.base_app_spec,
                    name=app_name,
                    title="title",
                    summary="summary",
                    description="description.",
                    regionalOptions={
                        "aws:us-east-1": dict(
                            systemRequirements=aws_sys_reqs,
                            bundledDepends=[{"name": "aws.tar.gz",
                                             "id": {"$dnanexus_link": aws_bundled_dep.get_id()}}],
                            assetDepends=[{"id": aws_asset}]
                        ),
                        "azure:westus": dict(
                            systemRequirements=azure_sys_reqs,
                            bundledDepends=[{"name": "azure.tar.gz",
                                             "id": {"$dnanexus_link": azure_bundled_dep.get_id()}}],
                            assetDepends=[{"id": azure_asset}]
                        )
                    }
                )

                # Build an app spec that looks like {region-A: {foo: ..., bar:
                # ...}, region-B: {foo: ...}} and make sure it is rejected
                for region_to_remove, key_to_remove in (("azure:westus", "systemRequirements"),
                                                        ("aws:us-east-1", "bundledDepends"),
                                                        ("azure:westus", "assetDepends")):
                    # Remove one of the regionalOptions keys from one of the
                    # regions
                    app_spec_without_regional_option = dict(app_spec)
                    app_spec_without_regional_option['regionalOptions'] = dict(app_spec['regionalOptions'])
                    app_spec_without_regional_option['regionalOptions'][region_to_remove] = (
                        dict(app_spec['regionalOptions'][region_to_remove]))
                    del app_spec_without_regional_option['regionalOptions'][region_to_remove][key_to_remove]

                    app_dir = self.write_app_directory(app_name,
                                                       json.dumps(app_spec_without_regional_option),
                                                       "code.py")
                    with self.assertSubprocessFailure(
                            stderr_regexp=key_to_remove + " was given for [-a-z0-9:]+ but not for " +
                                          region_to_remove):
                        run("dx build --create-app --json " + app_dir)

                # Build an app spec where some of the keys in the
                # regionalOptions also appear in the runSpec, and make sure it
                # is rejected
                for key_to_add in ("systemRequirements", "bundledDepends", "assetDepends"):
                    app_spec_with_redundant_option = dict(app_spec)
                    app_spec_with_redundant_option['runSpec'] = dict(app_spec['runSpec'])
                    app_spec_with_redundant_option['runSpec'][key_to_add] = (
                        app_spec['regionalOptions']['aws:us-east-1'][key_to_add])

                    app_dir = self.write_app_directory(app_name,
                                                       json.dumps(app_spec_with_redundant_option),
                                                       "code.py")
                    with self.assertSubprocessFailure(
                            stderr_regexp=key_to_add + " cannot be given in both runSpec and in regional options"):
                        run("dx build --create-app --json " + app_dir)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV and testutil.TEST_AZURE,
                         'skipping test that would create apps')
    def test_build_multi_region_app_requires_temporary_projects(self):
        # Attempt to build app without creating temporary projects.
        base_cmd = "dx build --create-app --no-temp-build-project --json {app_dir}"

        app_name = "asset_{t}_multi_region_app".format(t=int(time.time() * 1000))
        app_spec = dict(self.base_app_spec, name=app_name,
                        # This is a multi-region app.
                        regionalOptions={"aws:us-east-1": {},
                                         "azure:westus": {}})
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")

        with self.assertSubprocessFailure(stderr_regexp="--no-temp-build-project.*multi-region"):
            run(base_cmd.format(app_dir=app_dir))

        app_name = "asset_{t}_multi_region_app".format(t=int(time.time() * 1000))
        # This is a single-region app.
        app_spec = dict(self.base_app_spec, name=app_name, regionalOptions={"aws:us-east-1": {}})
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")

        app_new_res = json.loads(run(base_cmd.format(app_dir=app_dir)))
        app_desc_res = json.loads(run("dx describe --json " + app_new_res["id"]))
        self.assertEqual(app_desc_res["class"], "app")
        self.assertEqual(app_desc_res["id"], app_desc_res["id"])
        self.assertEqual(app_desc_res["version"], "1.0.0")
        self.assertEqual(app_desc_res["name"], app_name)
        self.assertFalse("published" in app_desc_res)
        self.assertIn("regionalOptions", app_desc_res)
        self.assertItemsEqual(list(app_desc_res["regionalOptions"].keys()), list(app_spec["regionalOptions"].keys()))

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_build_app_and_update_code(self):
        app_spec = dict(self.base_app_spec, name="update_app_code")
        app_dir = self.write_app_directory("update_app_code", json.dumps(app_spec), "code.py", code_content="'v1'\n")
        json.loads(run("dx build --create-app --json " + app_dir))

        with chdir(tempfile.mkdtemp()):
            run("dx get app-update_app_code")
            self.assertEqual(open(os.path.join("update_app_code", "src", "code.py")).read(), "'v1'\n")

        shutil.rmtree(app_dir)

        # Change the content of the app entry point (keeping everything else
        # the same)
        app_dir = self.write_app_directory("update_app_code", json.dumps(app_spec), "code.py", code_content="'v2'\n")
        json.loads(run("dx build --create-app --json " + app_dir))

        with chdir(tempfile.mkdtemp()):
            run("dx get app-update_app_code")
            self.assertEqual(open(os.path.join("update_app_code", "src", "code.py")).read(), "'v2'\n")

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create apps')
    def test_build_app_and_pretend_to_update_devs(self):
        app_spec = dict(self.base_app_spec, name="test_build_app_and_pretend_to_update_devs",
                        developers=['user-dnanexus'])
        app_dir = self.write_app_directory("test_build_app_and_pretend_to_update_devs",
                                           json.dumps(app_spec), "code.py")

        # Without --yes, the build will succeed except that it will skip
        # the developer update
        self.run_and_assert_stderr_matches('dx build --create-app --json ' + app_dir,
                                           'skipping requested change to the developer list')
        app_developers = dxpy.api.app_list_developers('app-test_build_app_and_pretend_to_update_devs')['developers']
        self.assertEqual(len(app_developers), 1)  # the id of the user we are calling as

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create apps')
    def test_build_app_and_update_devs(self):
        app_spec = dict(self.base_app_spec, name="test_build_app_and_update_devs")
        app_dir = self.write_app_directory("test_build_app_and_update_devs", json.dumps(app_spec),
                                           "code.py")

        my_userid = dxpy.whoami()

        run('dx build --create-app --json ' + app_dir)
        app_developers = dxpy.api.app_list_developers('app-test_build_app_and_update_devs')['developers']
        self.assertEqual(app_developers, [my_userid])

        # Add a developer
        app_spec['developers'] = [my_userid, 'user-eve']
        self.write_app_directory("test_build_app_and_update_devs", json.dumps(app_spec), "code.py")
        self.run_and_assert_stderr_matches('dx build --create-app --yes --json ' + app_dir,
                                           'the following developers will be added: user-eve')
        app_developers = dxpy.api.app_list_developers('app-test_build_app_and_update_devs')['developers']
        self.assertEqual(set(app_developers), set([my_userid, 'user-eve']))

        # Add and remove a developer
        app_spec['developers'] = [my_userid, 'user-bob']
        self.write_app_directory("test_build_app_and_update_devs", json.dumps(app_spec), "code.py")
        self.run_and_assert_stderr_matches(
            'dx build --create-app --yes --json ' + app_dir,
            'the following developers will be added: user-bob; and ' \
            + 'the following developers will be removed: user-eve'
        )
        app_developers = dxpy.api.app_list_developers('app-test_build_app_and_update_devs')['developers']
        self.assertEqual(set(app_developers), set([my_userid, 'user-bob']))

        # Remove a developer
        app_spec['developers'] = [my_userid]
        self.write_app_directory("test_build_app_and_update_devs", json.dumps(app_spec), "code.py")
        self.run_and_assert_stderr_matches('dx build --create-app --yes --json ' + app_dir,
                                           'the following developers will be removed: ' +
                                           'user-bob')
        app_developers = dxpy.api.app_list_developers('app-test_build_app_and_update_devs')['developers']
        self.assertEqual(app_developers, [my_userid])

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_build_app_with_region(self):
        app_spec = dict(self.base_app_spec, name="minimal_app_regions")
        app_dir = self.write_app_directory("minimal_app_regions", json.dumps(app_spec), "code.py")
        new_app = json.loads(run("dx build --create-app --region aws:us-east-1 --json " + app_dir))
        app_describe = json.loads(run("dx describe --json " + new_app["id"]))
        self.assertEqual(app_describe["region"], "aws:us-east-1")

        with self.assertRaisesRegex(subprocess.CalledProcessError, "InvalidInput"):
            run("dx build --create-app --region aws:not-a-region --json " + app_dir)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_API_APP_CREATE"])
    def test_build_app_with_bill_to(self):
        alice_id = "user-alice"
        org_id = "org-piratelabs"

        # --bill-to is not specified with dx build
        app_name = "app_build_local_bill_to_user"
        app_spec = dict(self.base_app_spec,
                        name=app_name,
                        regionalOptions={"aws:us-east-1": {}, "azure:westus": {}})
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")
        new_app = json.loads(run("dx build --app --json " + app_dir))
        self.assertEqual(new_app["billTo"], alice_id)

        # --bill-to is specified, but the billTo entity (org-piratelabs) does not have azure:westus
        # in their "permittedRegions".
        app_name = "app_build_local_bill_to_org_fails"
        app_spec = dict(self.base_app_spec,
                        name=app_name,
                        regionalOptions={"aws:us-east-1": {}, "azure:westus": {}})
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")
        with self.assertRaisesRegex(DXCalledProcessError, "PermissionDenied"):
            run("dx build --app --bill-to {} --json {}".format(org_id, app_dir))

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_invalid_project_context(self):
        app_spec = dict(self.base_app_spec, name="invalid_project_context")
        app_dir = self.write_app_directory("invalid_project_context", json.dumps(app_spec), "code.py")
        # Set the project context to a nonexistent project. This
        # shouldn't have any effect since building an app is supposed to
        # be hygienic.
        env = override_environment(DX_PROJECT_CONTEXT_ID='project-B00000000000000000000000')
        run("dx build --create-app --json " + app_dir, env=env)

    def test_invalid_execdepends(self):
        app_spec = {
            "name": "invalid_execdepends",
            "dxapi": "1.0.0",
            "runSpec": {
                "file": "code.py",
                "interpreter": "python2.7",
                "distribution": "Ubuntu", "release": "14.04",
                "execDepends": {"name": "oops"}
            },
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
        }
        app_dir = self.write_app_directory("invalid_execdepends", json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp="Expected runSpec\.execDepends to"):
            run("dx build --json " + app_dir)

    def test_invalid_authorized_users(self):
        app_spec = dict(self.base_app_spec, name="invalid_authorized_users", authorizedUsers="PUBLIC")
        app_dir = self.write_app_directory("invalid_authorized_users", json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp='Expected authorizedUsers to be a list of strings'):
            run("dx build --json " + app_dir)

        app_spec["authorizedUsers"] = ["foo"]
        app_dir = self.write_app_directory("invalid_authorized_users_2", json.dumps(app_spec),
                                           "code.py")
        with self.assertSubprocessFailure(stderr_regexp='contains an entry which is not'):
            run("dx build --json " + app_dir)

    def test_duplicate_keys_in_spec(self):
        app_spec = dict(self.base_app_spec, name="test_duplicate_keys_in_spec")
        spec = json.dumps(app_spec).replace('"file": "code.py"', '"file": "code.py", "file": "code.py"')
        app_dir = self.write_app_directory("duplicate_keys_in_spec", spec, "code.py")
        with self.assertSubprocessFailure(stderr_regexp="duplicate key: "):
            run("dx build --json " + app_dir)

    def test_deps_without_network_access(self):
        app_spec = dict(self.base_app_spec, name="test_deps_without_network_access",
                        runSpec={"execDepends": [{"name": "ddd", "package_manager": "pip"}],
                                 "file": "code.py",
                                 "interpreter": "python2.7",
                                 "distribution": "Ubuntu",
                                 "release": "14.04"})
        app_dir = self.write_app_directory("deps_without_network_access", json.dumps(app_spec),
                                           "code.py")

        with self.assertSubprocessFailure(stderr_regexp=("runSpec.execDepends specifies non-APT " +
                                                         "dependencies, but no network access spec " +
                                                         "is given")):
            run("dx build --json " + app_dir)

    def test_overwrite_applet(self):
        app_spec = dict(self.base_app_spec, name="applet_overwriting")
        app_dir = self.write_app_directory("applet_overwriting", json.dumps(app_spec), "code.py")
        applet_id = json.loads(run("dx build --json " + app_dir))["id"]
        # Verify that we can succeed by writing to a different folder.
        run("dx mkdir subfolder")
        run("dx build --destination=subfolder/applet_overwriting " + app_dir)
        with self.assertSubprocessFailure():
            run("dx build " + app_dir)
        run("dx build -f " + app_dir)
        # Verify that the original app was deleted by the previous
        # dx build -f
        with self.assertSubprocessFailure(exit_code=3):
            run("dx describe " + applet_id)

    def test_overwrite_multiple_applets(self):
        app_spec = dict(self.base_app_spec, name="applet_overwriting")
        app_dir = self.write_app_directory("applet_overwriting", json.dumps(app_spec), "code.py")

        # Create two applets in different directories, but with the same name
        run("dx mkdir subfolder1")
        app_1_spec = run_and_parse_json("dx build --json --destination=subfolder1/applet_overwriting " + app_dir)
        run("dx mkdir subfolder2")
        app_2_spec = run_and_parse_json("dx build --json --destination=subfolder2/applet_overwriting " + app_dir)

        # Move the applet in subfolder2 to subfolder1
        run("dx mv subfolder2/applet_overwriting subfolder1")

        # Verify that subfolder1 has two applets with the same name
        desc_1 = json.loads(run("dx describe --json " + app_1_spec["id"]))
        desc_2 = json.loads(run("dx describe --json " + app_2_spec["id"]))
        self.assertEqual(desc_1["name"], desc_2["name"])
        self.assertEqual(desc_1["folder"], desc_2["folder"])

        # Creating a new applet with the same name should fail
        with self.assertSubprocessFailure():
            run("dx build --destination=subfolder1/applet_overwriting " + app_dir)

        # Creating a new applet with the same name with overwrite should succeed
        app_final_spec = run_and_parse_json("dx build -f --json --destination=subfolder1/applet_overwriting " + app_dir)

        # Verify that the original applets were deleted by dx build -f
        with self.assertSubprocessFailure(exit_code=3):
            run("dx describe " + app_1_spec["id"])
        with self.assertSubprocessFailure(exit_code=3):
            run("dx describe " + app_2_spec["id"])

        # Verify that the newly created applet exists
        run("dx describe " + app_final_spec["id"])

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_update_app_categories(self):
        app1_spec = dict(self.base_app_spec, name="update_app_categories", categories=["A"])
        app2_spec = dict(self.base_app_spec, name="update_app_categories", categories=["B"])
        app_dir = self.write_app_directory("update_app_categories", json.dumps(app1_spec), "code.py")
        app_id = json.loads(run("dx build --create-app --json " + app_dir))['id']
        self.assertEqual(json.loads(run("dx api " + app_id + " listCategories"))["categories"], ['A'])
        shutil.rmtree(app_dir)
        self.write_app_directory("update_app_categories", json.dumps(app2_spec), "code.py")
        run("dx build --create-app --json " + app_dir)
        self.assertEqual(json.loads(run("dx api " + app_id + " listCategories"))["categories"], ['B'])

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create apps')
    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_API_APP_LIST_AUTHORIZED_USERS", "DNA_API_APP_ADD_AUTHORIZED_USER"])
    def test_update_app_authorized_users(self):
        app0_spec = dict(self.base_app_spec, name="update_app_authorized_users")
        app1_spec = dict(self.base_app_spec, name="update_app_authorized_users", authorizedUsers=[])
        app2_spec = dict(self.base_app_spec, name="update_app_authorized_users", authorizedUsers=["user-eve"])
        app_dir = self.write_app_directory("update_app_authorized_users", json.dumps(app0_spec),
                                           "code.py")
        app_id = json.loads(run("dx build --create-app --json " + app_dir))['id']
        self.assertEqual(json.loads(run("dx api " + app_id +
                                        " listAuthorizedUsers"))["authorizedUsers"], [])
        shutil.rmtree(app_dir)
        self.write_app_directory("update_app_authorized_users", json.dumps(app1_spec), "code.py")
        run("dx build --create-app --json " + app_dir)
        self.assertEqual(json.loads(run("dx api " + app_id +
                                        " listAuthorizedUsers"))["authorizedUsers"], [])
        shutil.rmtree(app_dir)
        self.write_app_directory("update_app_authorized_users", json.dumps(app2_spec), "code.py")
        run("dx build --create-app --yes --json " + app_dir)
        self.assertEqual(json.loads(run("dx api " + app_id +
                                        " listAuthorizedUsers"))["authorizedUsers"], ["user-eve"])

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_APP_ADD_AUTHORIZED_USERS_APP",
                                          "DNA_CLI_APP_LIST_AUTHORIZED_USERS_APP",
                                          "DNA_CLI_APP_REMOVE_AUTHORIZED_USERS_APP"])
    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_dx_add_list_remove_users_of_apps(self):
        '''
        This test is for some other dx subcommands, but it's in this
        test suite to take advantage of app-building methods.
        '''
        # Only create the app if it's not available already (makes
        # local testing easier)
        try:
            app_desc = dxpy.api.app_describe("app-test_dx_users", {})
            app_id = app_desc["id"]
            # reset users to empty list
            run("dx remove users app-test_dx_users " + " ".join(app_desc["authorizedUsers"]))
        except:
            app_id = None
        if app_id is None:
            app_spec = dict(self.base_app_spec, name="test_dx_users", version="0.0.1")
            app_dir = self.write_app_directory("test_dx_users", json.dumps(app_spec), "code.py")
            app_id = json.loads(run("dx build --create-app --json " + app_dir))['id']
        # don't use "app-" prefix, duplicate and multiple members are fine
        run("dx add users test_dx_users eve user-eve org-piratelabs")
        users = run("dx list users app-test_dx_users").strip().split("\n")
        self.assertEqual(len(users), 2)
        self.assertIn("user-eve", users)
        self.assertIn("org-piratelabs", users)
        run("dx remove users test_dx_users eve org-piratelabs")
        # use version string
        users = run("dx list users app-test_dx_users/0.0.1").strip()

        # bad paths and exit codes
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx add users nonexistentapp user-eve')
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx list users app-nonexistentapp')
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx remove users app-nonexistentapp/1.0.0 user-eve')
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run('dx add users test_dx_users org-nonexistentorg')
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run('dx add users test_dx_users nonexistentuser')
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run('dx add users test_dx_users piratelabs')

        # ResourceNotFound is not thrown when removing things
        run('dx remove users test_dx_users org-nonexistentorg')
        run('dx remove users test_dx_users nonexistentuser')
        run('dx remove users test_dx_users piratelabs')

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_APP_ADD_DEVELOPERS_APP",
                                          "DNA_CLI_APP_LIST_DEVELOPERS_APP",
                                          "DNA_CLI_APP_REMOVE_DEVELOPERS_APP",
                                          "DNA_API_APP_ADD_DEVELOPER"])
    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_dx_add_list_remove_developers_of_apps(self):
        '''
        This test is for some other dx subcommands, but it's in this
        test suite to take advantage of app-building methods.
        '''
        # Only create the app if it's not available already (makes
        # local testing easier)
        try:
            app_desc = dxpy.api.app_describe("app-test_dx_developers", {})
            app_id = app_desc["id"]
            my_userid = app_desc["createdBy"]
            developers = dxpy.api.app_list_developers("app-test_dx_developers", {})["developers"]
            # reset developers to default list
            if len(developers) != 1:
                run("dx remove developers app-test_dx_developers " +
                    " ".join([dev for dev in developers if dev != my_userid]))
        except:
            app_id = None
        if app_id is None:
            app_spec = dict(self.base_app_spec, name="test_dx_developers", version="0.0.1")
            app_dir = self.write_app_directory("test_dx_developers", json.dumps(app_spec), "code.py")
            app_desc = json.loads(run("dx build --create-app --json " + app_dir))
            app_id = app_desc['id']
            my_userid = app_desc["createdBy"]
        developers = run("dx list developers app-test_dx_developers").strip()
        self.assertEqual(developers, my_userid)

        # use hash ID
        run("dx add developers " + app_id + " eve")
        developers = run("dx list developers app-test_dx_developers").strip().split("\n")
        self.assertEqual(len(developers), 2)
        self.assertIn(my_userid, developers)
        # don't use "app-" prefix, duplicate, multiple, and non- members are fine
        run("dx remove developers test_dx_developers PUBLIC eve user-eve org-piratelabs")
        developers = run("dx list developers app-test_dx_developers").strip()
        self.assertEqual(developers, my_userid)
        # use version string
        run("dx list developers app-test_dx_developers/0.0.1")

        # bad paths and exit codes
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx add developers nonexistentapp eve')
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx list developers app-nonexistentapp')
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx remove developers app-nonexistentapp/1.0.0 eve')
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run('dx add developers test_dx_developers nonexistentuser')
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run('dx add developers test_dx_developers piratelabs')

        # ResourceNotFound is not thrown when removing things
        run('dx remove developers test_dx_developers org-nonexistentorg')
        run('dx remove developers test_dx_developers nonexistentuser')
        run('dx remove developers test_dx_developers piratelabs')

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_build_app_autonumbering(self):
        app_spec = dict(self.base_app_spec, name="build_app_autonumbering")
        app_dir = self.write_app_directory("build_app_autonumbering", json.dumps(app_spec), "code.py")
        run("dx build --create-app --json --publish " + app_dir)
        with self.assertSubprocessFailure(stderr_regexp="Could not create"):
            print(run("dx build --create-app --json --no-version-autonumbering " + app_dir))
        run("dx build --create-app --json " + app_dir)  # Creates autonumbered version

    def test_build_failure(self):
        app_spec = dict(self.base_app_spec, name="build_failure")
        app_dir = self.write_app_directory("build_failure", json.dumps(app_spec), "code.py")
        with open(os.path.join(app_dir, 'Makefile'), 'w') as makefile:
            makefile.write("all:\n\texit 7")
        with self.assertSubprocessFailure(stderr_regexp="make -j[0-9]+ in target directory failed with exit code"):
            run("dx build " + app_dir)
        # Somewhat indirect test of --no-parallel-build
        with self.assertSubprocessFailure(stderr_regexp="make in target directory failed with exit code"):
            run("dx build --no-parallel-build " + app_dir)

    def test_syntax_checks(self):
        app_spec = dict(self.base_app_spec, name="syntax_checks")
        app_dir = self.write_app_directory("syntax_checks",
                                           json.dumps(app_spec),
                                           code_filename="code.py",
                                           code_content="def improper():\nprint 'oops'")
        with self.assertSubprocessFailure(stderr_regexp="Entry point file \\S+ has syntax errors"):
            run("dx build " + app_dir)
        run("dx build --no-check-syntax " + app_dir)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping test that would run jobs')
    def test_build_and_run_applet_remote(self):
        app_spec = {
            "name": "build_applet_remote",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7", "distribution": "Ubuntu", "release": "14.04"},
            "inputSpec": [
                {"name": "in1", "class": "int"},
            ],
            "outputSpec": [
                {"name": "out1", "class": "int"}
            ],
            "version": "1.0.0"
        }
        app_code = """import dxpy
@dxpy.entry_point("main")
def main(in1):
    return {"out1": in1 + 1}
"""
        app_dir = self.write_app_directory(
            'build_applet_remote', json.dumps(app_spec), code_filename='code.py', code_content=app_code)
        remote_build_output = run('dx build --remote ' + app_dir).strip().split('\n')[-1]
        # TODO: it would be nice to have the output of dx build --remote
        # more machine readable (perhaps when --json is specified)
        build_job_id = re.search('job-[A-Za-z0-9]{24}', remote_build_output).group(0)
        build_job_describe = json.loads(run('dx describe --json ' + build_job_id))
        applet_id = build_job_describe['output']['output_applet']['$dnanexus_link']
        invocation_job_id = run('dx run --brief --yes ' + applet_id + ' -iin1=8675309').strip()
        run('dx wait ' + invocation_job_id)
        invocation_job_describe = json.loads(run('dx describe --json ' + invocation_job_id))
        self.assertEqual(invocation_job_describe['output']['out1'], 8675310)

    def test_applet_help(self):
        app_spec = {
            "name": "applet_help",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7", "distribution": "Ubuntu", "release": "14.04"},
            "inputSpec": [
                {"name": "reads", "class": "array:file", "type": "LetterReads", "label": "Reads",
                 "help": "One or more Reads table objects."},
                {"name": "required", "class": "file", "label": "Required", "help": "Another parameter"},
                {"name": "optional", "class": "file", "label": "Optional",
                 "help": "Optional parameter", "optional": True}
            ],
            "outputSpec": [
                {"name": "mappings", "class": "file", "type": "LetterMappings", "label": "Mappings",
                 "help": "The mapped reads."}
            ],
            "version": "1.0.0"
        }
        app_dir = self.write_app_directory("åpplet_help", json.dumps(app_spec),
                                           code_filename="code.py", code_content="")
        applet_id = run_and_parse_json("dx build --json " + app_dir)["id"]
        applet_help = run("dx run " + applet_id + " -h")
        self.assertTrue("Reads: -ireads=(file, type LetterReads) [-ireads=... [...]]" in applet_help)
        self.assertTrue("Required: -irequired=(file)" in applet_help)
        self.assertTrue("Optional: [-ioptional=(file)]" in applet_help)
        self.assertTrue("Mappings: mappings (file, type LetterMappings)" in applet_help)

    def test_upload_resources(self):
        run("dx mkdir /subfolder")
        cd("/subfolder")
        app_spec = dict(self.base_app_spec, name="upload_resources")
        app_dir = self.write_app_directory("upload_app_resources", json.dumps(app_spec), "code.py")
        os.mkdir(os.path.join(app_dir, 'resources'))
        with open(os.path.join(app_dir, 'resources', 'test.txt'), 'w') as resources_file:
            resources_file.write('test\n')
        new_applet = run_and_parse_json("dx build --json " + app_dir)
        applet_describe = dxpy.get_handler(new_applet["id"]).describe()
        resources_file = applet_describe['runSpec']['bundledDepends'][0]['id']['$dnanexus_link']
        resources_file_describe = json.loads(run("dx describe --json " + resources_file))
        # Verify that the bundled depends appear in the same folder.
        self.assertEqual(resources_file_describe['folder'], '/subfolder')

    def _build_check_resources(self, app_dir, args="", extract_resources=True):
        """
        Builds an app given the arguments and either:
            - downloads and extracts the tarball to a temp directory, returning the path of
              the temp directory (when extract_resources is True), or
            - returns the ID of the resource bundle (when extract_resources is False)
        """
        # create applet and get the resource_file id
        new_applet = run_and_parse_json("dx build -f --json " + args + " " + app_dir)
        applet_describe = dxpy.api.applet_describe(new_applet["id"])
        resources_file = applet_describe['runSpec']['bundledDepends'][0]['id']['$dnanexus_link']
        id1 = dxpy.api.file_describe(resources_file)['id']

        if not extract_resources:
            return id1

        # download resources tar and extract it to a temp. directory.
        # note that if the resource directory also contains a file of the name
        # './res.tar.gz', things could get ugly, but this is just a helper for
        # a test, so we don't have to worry about this pathological case
        res_temp_dir = tempfile.mkdtemp()
        dxpy.download_dxfile(id1, os.path.join(res_temp_dir, 'res.tar.gz'))
        subprocess.check_call(['tar', '-zxf', os.path.join(res_temp_dir, 'res.tar.gz'), '-C', res_temp_dir])

        # remove the original tar file
        os.remove(os.path.join(res_temp_dir, 'res.tar.gz'))

        # return the temp directory to perform tests specific to the configuration
        return res_temp_dir

    def test_upload_resources_symlink(self):
        app_spec = dict(self.base_app_spec, name="upload_resources_symlink")
        test_symlink_dir = "upload_resources_symlink"
        os.mkdir(os.path.join(self.temp_file_path, test_symlink_dir))
        app_dir = self.write_app_directory(os.path.join(self.temp_file_path, test_symlink_dir, 'app'),
                                           json.dumps(app_spec), "code.py")
        os.mkdir(os.path.join(app_dir, 'resources'))

        with open(os.path.join(app_dir, 'resources', 'test_file2.txt'), 'w') as resources_file2:
            resources_file2.write('test_file2\n')

        if 'symbolic_link' in os.listdir(os.path.join(app_dir, 'resources')):
            os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))

        # ==== Case 1 ====

        # == Links to local files are kept
        os.symlink(os.path.join(os.curdir, 'test_file2.txt'), os.path.join(app_dir, 'resources', 'symbolic_link'))

        # build app
        res_temp_dir = self._build_check_resources(app_dir)
        # Test symbolic_link is soft link
        self.assertTrue(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))

        shutil.rmtree(res_temp_dir)

        # == Links to local directories are kept when using relative build directory

        curdir = os.getcwd()
        os.chdir(os.path.join(app_dir, os.pardir))

        # build app in the current wd
        res_temp_dir = self._build_check_resources('app')

        # Test symbolic_link is a soft link
        self.assertTrue(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))

        shutil.rmtree(res_temp_dir)
        os.chdir(curdir)

        # == Links to local directories are kept
        os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))
        os.mkdir(os.path.join(app_dir, 'resources', 'local_dir'))
        os.symlink(os.path.join(os.curdir, 'local_dir'), os.path.join(app_dir, 'resources', 'symbolic_link'))

        # build app
        res_temp_dir = self._build_check_resources(app_dir)
        # Test symbolic_link is soft link
        self.assertTrue(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))

        shutil.rmtree(res_temp_dir)

        # ==== Case 2 ====

        # == Links to remote files are dereferenced
        with open(os.path.join(app_dir, os.pardir, 'test_file_outside.txt'), 'w') as file1:
            file1.write('test_file_outside\n')
        os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))
        os.symlink(os.path.join(os.pardir, os.pardir, 'test_file_outside.txt'),
                   os.path.join(app_dir, 'resources', 'symbolic_link'))

        # create applet and get the resource_file id
        res_temp_dir = self._build_check_resources(app_dir)

        # Test: symbolic_link exists, is NOT a link, and has the same content
        #       as app_dir/test_file_outside.txt
        self.assertTrue(os.path.exists(os.path.join(res_temp_dir, 'symbolic_link')))
        self.assertFalse(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))
        self.assertTrue(filecmp.cmp(os.path.join(res_temp_dir, 'symbolic_link'),
                                    os.path.join(app_dir, os.pardir, 'test_file_outside.txt')))

        shutil.rmtree(res_temp_dir)

        # == Links to remote files are NOT dereferenced with --force-symlink

        # create applet and get the resource_file id
        res_temp_dir = self._build_check_resources(app_dir, "--force-symlinks")

        # Test: symbolic_link is a symlink
        self.assertTrue(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))

        shutil.rmtree(res_temp_dir)

        # ==== Case 3 ====

        # == Broken remote links result in build error
        # NOTE: we just removed the test_file_outside.txt, breaking symbolic_link
        os.remove(os.path.join(app_dir, os.pardir, 'test_file_outside.txt'))

        with self.assertSubprocessFailure(stderr_regexp="Broken symlink"):
            run("dx build -f " + app_dir)

        # == Broken remote links are NOT dereferenced with --force-symlink

        # create applet and get the resource_file id
        res_temp_dir = self._build_check_resources(app_dir, "--force-symlinks")

        # Test: symbolic_link is a symlink
        self.assertTrue(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))

        shutil.rmtree(res_temp_dir)

        # ==== Case 4 ====

        # == Links to remote directories causes an AssertionError
        os.mkdir(os.path.join(app_dir, 'test_outside_dir'))
        os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))
        os.symlink(os.path.join(os.pardir, 'test_outside_dir'),
                   os.path.join(app_dir, 'resources', 'symbolic_link'))

        with self.assertSubprocessFailure(
                stderr_regexp="Cannot include symlinks to directories outside of the resource directory"):
            run("dx build -f " + app_dir)

        # == Links to remote directories are NOT dereferenced with --force-symlink

        # create applet and get the resource_file id
        res_temp_dir = self._build_check_resources(app_dir, "--force-symlinks")

        # Test: symbolic_link is a symlink
        self.assertTrue(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))

        shutil.rmtree(res_temp_dir)

        # ==== Case 5 ====

        # == Links to local links (regardless of eventual destination) are kept
        os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))

        with open(os.path.join(app_dir, os.pardir, 'remote_file'), 'w') as file1:
            file1.write('remote file outside\n')

        os.symlink(os.path.join(os.pardir, os.pardir, 'remote_file'), os.path.join(app_dir, 'resources', 'remote_link'))
        os.symlink(os.path.join(os.curdir, 'remote_link'), os.path.join(app_dir, 'resources', 'symbolic_link'))

        # create applet and get the resource_file id
        res_temp_dir = self._build_check_resources(app_dir)

        # Test: symbolic_link is a symlink
        self.assertTrue(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))

        shutil.rmtree(res_temp_dir)

        # ==== Case 6 ====

        # == Links to remote links (which are links to a file) are dereferenced
        os.remove(os.path.join(app_dir, 'resources', 'remote_link'))
        os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))
        os.mkdir(os.path.join(app_dir, os.pardir, 'lib'))
        shutil.move(os.path.join(app_dir, os.pardir, 'remote_file'), os.path.join(app_dir, os.pardir, 'lib'))
        os.symlink(os.path.join(app_dir, os.pardir, 'lib', 'remote_file'),
                   os.path.join(app_dir, os.pardir, 'outside_link'))
        os.symlink(os.path.join(os.pardir, os.pardir, 'outside_link'),
                   os.path.join(app_dir, 'resources', 'symbolic_link'))

        # create applet and get the resource_file id
        res_temp_dir = self._build_check_resources(app_dir)

        # Test: symbolic_link is NOT a symlink and is the same file as app_dir/../lib/remote_file
        self.assertFalse(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))
        self.assertTrue(filecmp.cmp(os.path.join(res_temp_dir, 'symbolic_link'),
                                    os.path.join(app_dir, os.pardir, 'lib', 'remote_file')))

        shutil.rmtree(res_temp_dir)

        # == Links to remote links (which are links to a file) are kept using --force-symlink
        res_temp_dir = self._build_check_resources(app_dir, "--force-symlinks")

        # Test: symbolic_link is a symlink
        self.assertTrue(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))

        shutil.rmtree(res_temp_dir)

        # ==== Case 7 ====

        # == Absolute links to files are ALWAYS dereferenced, regardless of destination
        os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))
        os.symlink(os.path.join(app_dir, 'resources', 'test_file2.txt'),
                   os.path.join(app_dir, 'resources', 'symbolic_link'))

        # create applet and get the resource_file id
        res_temp_dir = self._build_check_resources(app_dir)

        # Test: symbolic_link is NOT a symlink and is the same file as test_file2.txt
        self.assertFalse(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))
        self.assertTrue(filecmp.cmp(os.path.join(res_temp_dir, 'symbolic_link'),
                                    os.path.join(app_dir, 'resources', 'test_file2.txt')))

        shutil.rmtree(res_temp_dir)

        # == Absolute links to files are kept using --force-symlink
        res_temp_dir = self._build_check_resources(app_dir, "--force-symlinks")

        # Test: symbolic_link is a symlink
        self.assertTrue(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))

        shutil.rmtree(res_temp_dir)

        # == Absolute links to directories cause an error, regardless of destination
        os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))
        os.symlink(os.path.join(app_dir, 'resources', 'local_dir'), os.path.join(app_dir, 'resources', 'symbolic_link'))

        with self.assertSubprocessFailure(
                stderr_regexp="Cannot include symlinks to directories outside of the resource directory"):
            run("dx build -f " + app_dir)

        # == Absolute links to directories are keps when --force-symlinks is used
        res_temp_dir = self._build_check_resources(app_dir, "--force-symlinks")

        # Test: symbolic_link is a symlink
        self.assertTrue(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))

        # ==== Case 8 ====

        # == Relative link to a file that extends outside the resource path is dereferenced
        os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))
        os.symlink(os.path.join(os.pardir, 'resources', 'test_file2.txt'),
                   os.path.join(app_dir, 'resources', 'symbolic_link'))

        # create applet and get the resource_file id
        res_temp_dir = self._build_check_resources(app_dir)

        # Test: symbolic_link is NOT a symlink and is the same file as test_file2.txt
        self.assertFalse(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))
        self.assertTrue(filecmp.cmp(os.path.join(res_temp_dir, 'symbolic_link'),
                                    os.path.join(app_dir, 'resources', 'test_file2.txt')))

        shutil.rmtree(res_temp_dir)

        # == Relative link to a file that extends outside resource path is kept using --force-symlinks
        res_temp_dir = self._build_check_resources(app_dir, "--force-symlinks")

        # Test: symbolic_link is a symlink
        self.assertTrue(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))

        shutil.rmtree(res_temp_dir)

        # == Relative link to directory extending outside resource path causes error
        os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))
        os.symlink(os.path.join(os.pardir, 'resources', 'local_dir'),
                   os.path.join(app_dir, 'resources', 'symbolic_link'))

        with self.assertSubprocessFailure(
                stderr_regexp="Cannot include symlinks to directories outside of the resource directory"):
            run("dx build -f " + app_dir)

        # == Relative link to directory extending outside resource path are kept when --force-symlinks is used
        res_temp_dir = self._build_check_resources(app_dir, "--force-symlinks")

        # Test: symbolic_link is a symlink
        self.assertTrue(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))

        shutil.rmtree(res_temp_dir)

    def test_upload_resources_permissions(self):
        app_spec = dict(self.base_app_spec, name="upload_resources_permissions")
        test_perms_dir = "upload_resources_permissions"
        os.mkdir(os.path.join(self.temp_file_path, test_perms_dir))
        app_dir = self.write_app_directory(os.path.join(self.temp_file_path, test_perms_dir, 'app'),
                                           json.dumps(app_spec), "code.py")
        os.mkdir(os.path.join(app_dir, 'resources'))

        with open(os.path.join(app_dir, 'resources', 'test_644.txt'), 'w') as rf:
            rf.write('test_permissions: 644\n')
        with open(os.path.join(app_dir, 'resources', 'test_660.txt'), 'w') as rf:
            rf.write('test_permissions: 660\n')
        with open(os.path.join(app_dir, 'resources', 'test_400.txt'), 'w') as rf:
            rf.write('test_permissions: 400\n')
        with open(os.path.join(app_dir, 'resources', 'test_755.txt'), 'w') as rf:
            rf.write('test_permissions: 755\n')
        with open(os.path.join(app_dir, 'resources', 'test_770.txt'), 'w') as rf:
            rf.write('test_permissions: 770\n')
        with open(os.path.join(app_dir, 'resources', 'test_670.txt'), 'w') as rf:
            rf.write('test_permissions: 670\n')

        # Now, set the permissions alluded to above
        os.chmod(os.path.join(app_dir, 'resources', 'test_644.txt'),
                 stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
        os.chmod(os.path.join(app_dir, 'resources', 'test_660.txt'),
                 stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP)
        os.chmod(os.path.join(app_dir, 'resources', 'test_400.txt'),
                 stat.S_IRUSR)
        os.chmod(os.path.join(app_dir, 'resources', 'test_755.txt'),
                 stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
        os.chmod(os.path.join(app_dir, 'resources', 'test_770.txt'),
                 stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP)
        os.chmod(os.path.join(app_dir, 'resources', 'test_670.txt'),
                 stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP)

        # build app
        res_temp_dir = self._build_check_resources(app_dir)

        # TODO: commented out tests below possibly failing due to weird umask
        # settings?

        # Test file permissions (all will have stat.S_IFREG as well)
        # 644 => 644
        self.assertEqual(os.stat(os.path.join(res_temp_dir, "test_644.txt")).st_mode,
                         stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
        # 660 => 664
        # self.assertEqual(os.stat(os.path.join(res_temp_dir, "test_660.txt")).st_mode,
        #    stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)
        # 400 => 444
        self.assertEqual(os.stat(os.path.join(res_temp_dir, "test_400.txt")).st_mode,
                         stat.S_IFREG | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        # 755 => 755
        # self.assertEqual(os.stat(os.path.join(res_temp_dir, "test_755.txt")).st_mode,
        #    stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
        # 770 => 775
        # self.assertEqual(os.stat(os.path.join(res_temp_dir, "test_770.txt")).st_mode,
        #    stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
        # 670 => 674
        # self.assertEqual(os.stat(os.path.join(res_temp_dir, "test_670.txt")).st_mode,
        #    stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | stat.S_IROTH)

        shutil.rmtree(res_temp_dir)

        # If we make a permission change that does NOT affect the tar, we should re-use the resource bundle
        id1 = self._build_check_resources(app_dir, extract_resources=False)

        # change the 400 => 444
        os.chmod(os.path.join(app_dir, 'resources', 'test_400.txt'),
                 stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

        id2 = self._build_check_resources(app_dir, extract_resources=False)

        self.assertEqual(id1, id2)

        # If we make a permission change that DOES affect the tar, we should rebuild

        # change 400 => 500 (will result in perms 555)
        os.chmod(os.path.join(app_dir, 'resources', 'test_400.txt'),
                 stat.S_IRUSR | stat.S_IXUSR)

        id3 = self._build_check_resources(app_dir, extract_resources=False)

        self.assertNotEqual(id1, id3)

    # This test uses an applet name that include unicode characters. Therefore,
    # we need to encode to byte everytime we use os filesystem operations.
    def test_upload_resources_advanced(self):
        app_spec = dict(self.base_app_spec, name="upload_resources_advanced")
        app_dir = self.write_app_directory("upload_åpp_resources_advanced", json.dumps(app_spec), "code.py")
        os.mkdir(os.path.join(app_dir, 'resources'))

        with open(os.path.join(app_dir, 'test_file1.txt'), 'w') as file1:
            file1.write('test_file1\n')  # Not in resources folder, so will not affect checksum
        with open(os.path.join(app_dir, 'resources', 'test_file2.txt'), 'w') as resources_file2:
            resources_file2.write('test_file2\n')

        # Create symbolic link to test_file1.txt
        if 'symbolic_link' in os.listdir(os.path.join(app_dir, 'resources')):
            os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))
        os.symlink(os.path.join(app_dir, 'test_file1.txt'),
                   os.path.join(app_dir, 'resources', 'symbolic_link'))

        id1 = self._build_check_resources(app_dir, "--force-symlinks", False)

        # Remove test_file1.txt, even though symbolic_link points to it. Removal itself will not affect checksum
        os.remove(os.path.join(app_dir, 'test_file1.txt'))

        id2 = self._build_check_resources(app_dir, "--force-symlinks", False)

        self.assertEqual(id1, id2)  # No upload happened

        # Make symbolic_link point to test_file2.txt, giving it a different modification time
        os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))
        os.symlink(os.path.join(app_dir, 'resources', 'test_file2.txt'),
                   os.path.join(app_dir, 'resources', 'symbolic_link'))

        id3 = self._build_check_resources(app_dir, "--force-symlinks", False)

        self.assertNotEqual(id2, id3)  # Upload should have happened

        # Force upload even with no changes
        id4 = self._build_check_resources(app_dir, "--force-symlinks --ensure-upload", False)

        self.assertNotEqual(id3, id4)  # Upload should have happened

        # Also, the new bundle should not have a checksum property
        # (and thus be eligible for future loads)
        resources_file_describe = json.loads(run("dx describe --json " + id4))
        self.assertNotIn('resource_bundle_checksum', resources_file_describe['properties'])

        # Test the behavior without --force-symlinks

        # First, let's clean up the old resources directory
        shutil.rmtree(os.path.join(app_dir, 'resources'))
        os.mkdir(os.path.join(app_dir, 'resources'))

        # create a couple files both inside and outside the directory
        with open(os.path.join(app_dir, 'outside_file1.txt'), 'w') as fn:
            fn.write('test_file1\n')
        with open(os.path.join(app_dir, 'outside_file2.txt'), 'w') as fn:
            fn.write('test_file2\n')
        with open(os.path.join(app_dir, 'resources', 'inside_file1.txt'), 'w') as fn:
            fn.write('test_file1\n')
        with open(os.path.join(app_dir, 'resources', 'inside_file2.txt'), 'w') as fn:
            fn.write('test_file2\n')

        os.symlink(os.path.join(app_dir, 'outside_file1.txt'),
                   os.path.join(app_dir, 'outside_link'))

        # First, testing dereferencing of links
        # Create a link to be dereferenced
        if 'symbolic_link' in os.listdir(os.path.join(app_dir, 'resources')):
            os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))

        # NOTE: we're going to have to use a link to a link in order to avoid modifying the directory mtime
        os.symlink(os.path.join(app_dir, 'outside_link'),
                   os.path.join(app_dir, 'resources', 'symbolic_link'))

        idr1 = self._build_check_resources(app_dir, "", False)

        # Update the link target; modify target mtime
        with open(os.path.join(app_dir, 'outside_file1.txt'), 'w') as fn:
            fn.write('test_file1 Update!\n')

        idr2 = self._build_check_resources(app_dir, "", False)

        self.assertNotEqual(idr1, idr2)  # Upload should happen

        # Change link destination; target mtime change
        os.remove(os.path.join(app_dir, 'outside_link'))
        os.symlink(os.path.join(app_dir, 'outside_file2.txt'),
                   os.path.join(app_dir, 'outside_link'))

        idr3 = self._build_check_resources(app_dir, "", False)

        # New Upload should happen
        self.assertNotEqual(idr2, idr3)

        # Add another link the the chain, but eventual destination doesn't change
        os.remove(os.path.join(app_dir, 'outside_link'))
        os.symlink(os.path.join(app_dir, 'outside_file2.txt'),
                   os.path.join(app_dir, 'outside_link_1'))
        os.symlink(os.path.join(app_dir, 'outside_link_1'),
                   os.path.join(app_dir, 'outside_link'))

        idr4 = self._build_check_resources(app_dir, "", False)

        # New Upload should NOT happen - filename change
        self.assertEqual(idr3, idr4)

        # However, if we ensure upload, we need to upload!
        idr5 = self._build_check_resources(app_dir, "--ensure-upload", False)

        # New Upload should happen
        self.assertNotEqual(idr4, idr5)

        # NOTE: for non-dereferenced links, almost any change will result in an mtime change
        # for the directory or a file within.  It's virtually impossible to make a local
        # symlink change and NOT create a new tarball, so we won't test that

    def test_archive_in_another_project(self):
        app_spec = dict(self.base_app_spec, name="archive_in_another_project")
        app_dir = self.write_app_directory("archive_in_another_project", json.dumps(app_spec), "code.py")

        with temporary_project("Temporary working project", select=True) as temp_project:
            orig_applet = run_and_parse_json("dx build --json -d {p}: {app_dir}".format(
                p=self.project, app_dir=app_dir))["id"]
            new_applet = run_and_parse_json("dx build --json --archive -d {p}: {app_dir}".format(
                p=self.project, app_dir=app_dir))["id"]
            self.assertEqual(dxpy.DXApplet(orig_applet).describe(incl_properties=True)["properties"]["replacedWith"],
                             new_applet)

    def test_categories_propagated_to_tags(self):
        app_spec = dict(self.base_app_spec, name="update_app_categories", categories=["Import"], tags=["mytag"])
        app_dir = self.write_app_directory("categories_propagated_to_tags", json.dumps(app_spec), "code.py")
        applet_id = json.loads(run("dx build --json -d categories1 " + app_dir))["id"]
        self.assertEqual(set(dxpy.DXApplet(applet_id).describe()["tags"]),
                         set(["mytag", "Import"]))

        app_spec2 = dict(self.base_app_spec, name="update_app_categories", categories=["Import"])
        app_dir2 = self.write_app_directory("categories_propagated_to_tags", json.dumps(app_spec2), "code.py")
        applet_id2 = json.loads(run("dx build --json -d categories2 " + app_dir2))["id"]
        self.assertEqual(set(dxpy.DXApplet(applet_id2).describe()["tags"]),
                         set(["Import"]))

    def test_bundled_depends_reuse(self):
        app_spec = dict(self.base_app_spec, name="bundled_depends_reuse")
        app_dir = self.write_app_directory("bundled_depends_reuse", json.dumps(app_spec), "code.py")
        os.mkdir(os.path.join(app_dir, 'resources'))
        with open(os.path.join(app_dir, 'resources', 'foo.txt'), 'w') as file_in_resources:
            file_in_resources.write('foo\n')

        first_applet = run_and_parse_json("dx build --json -d {p}:applet1 {app_dir}".format(
            p=self.project, app_dir=app_dir))["id"]
        second_applet = run_and_parse_json("dx build --json -d {p}:applet2 {app_dir}".format(
            p=self.project, app_dir=app_dir))["id"]

        # The second applet should reuse the bundle from the first.

        # touch foo.txt
        os.utime(os.path.join(app_dir, 'resources', 'foo.txt'), None)

        # But the third applet should not share with the first two,
        # because the resources have been touched in between.

        third_applet = json.loads(run("dx build --json -d {p}:applet3 {app_dir}".format(
            p=self.project, app_dir=app_dir)))["id"]

        self.assertEqual(
            dxpy.DXApplet(first_applet).describe()['runSpec']['bundledDepends'][0]['id']['$dnanexus_link'],
            dxpy.DXApplet(second_applet).describe()['runSpec']['bundledDepends'][0]['id']['$dnanexus_link']
        )
        self.assertNotEqual(
            dxpy.DXApplet(first_applet).describe()['runSpec']['bundledDepends'][0]['id']['$dnanexus_link'],
            dxpy.DXApplet(third_applet).describe()['runSpec']['bundledDepends'][0]['id']['$dnanexus_link']
        )

    def test_bundled_depends_reuse_with_force(self):
        app_spec = dict(self.base_app_spec, name="bundled_depends_reuse_with_force")
        app_dir = self.write_app_directory("bundled_depends_reuse_with_force", json.dumps(app_spec), "code.py")
        os.mkdir(os.path.join(app_dir, 'resources'))
        with open(os.path.join(app_dir, 'resources', 'foo.txt'), 'w') as file_in_resources:
            file_in_resources.write('foo\n')

        # For this to work, "dx build" must not remove the first applet
        # until after the second applet has been built, since otherwise
        # the first applet's bundled depends will be garbage collected
        first_applet = json.loads(run("dx build --json -d {p}:applet1 {app_dir}".format(
            p=self.project, app_dir=app_dir)))["id"]
        first_bundled_resources = \
            dxpy.DXApplet(first_applet).describe()['runSpec']['bundledDepends'][0]['id']['$dnanexus_link']
        second_applet = json.loads(run("dx build --json -f -d {p}:applet1 {app_dir}".format(
            p=self.project, app_dir=app_dir)))["id"]
        second_bundled_resources = \
            dxpy.DXApplet(second_applet).describe()['runSpec']['bundledDepends'][0]['id']['$dnanexus_link']
        # Verify that the resources are shared...
        self.assertEqual(first_bundled_resources, second_bundled_resources)
        # ...and that the first applet has been removed
        with self.assertSubprocessFailure(exit_code=3):
            run("dx describe " + first_applet)

    @unittest.skipUnless(testutil.TEST_ENV, 'skipping test that would clobber your local environment')
    def test_build_without_context(self):
        app_spec = dict(self.base_app_spec, name="applet_without_context")
        app_dir = self.write_app_directory("applet_without_context", json.dumps(app_spec), "code.py")

        # Without project context, cannot create new object without
        # project qualified path
        with without_project_context():
            with self.assertSubprocessFailure(stderr_regexp='expected the path to be qualified with a project',
                                              exit_code=3):
                run("dx build --json --destination foo " + app_dir)
            # Can create object with explicit project qualifier
            applet_describe = json.loads(run("dx build --json --destination " + self.project + ":foo " + app_dir))
            self.assertEqual(applet_describe["name"], "foo")

    def test_asset_depends_using_name(self):
        # upload a tar.gz file and mark it hidden
        asset_name = "test-asset.tar.gz"
        asset_file = dxpy.upload_string("xxyyzz", project=self.project, hidden=True, wait_on_close=True,
                                        name=asset_name)

        # create a record with details to the hidden asset
        create_folder_in_project(self.project, "/record_subfolder")
        record_name = "asset-record"
        record_details = {"archiveFileId": {"$dnanexus_link": asset_file.get_id()}}
        record_properties = {"version": "0.0.1"}
        dxpy.new_dxrecord(project=self.project, folder="/record_subfolder", types=["AssetBundle"],
                          details=record_details, name=record_name, properties=record_properties, close=True)

        # failure: asset will not be searched in folders recursively
        with self.assertSubprocessFailure(stderr_regexp="No asset bundle was found", exit_code=3):
            app_spec = dict(self.base_app_spec, name="asset_depends",
                            runSpec={
                                "assetDepends": [{"name": record_name, "version": "0.0.1", "project": self.project}],
                                "file": "code.py", "distribution": "Ubuntu", "release": "14.04",
                                "interpreter": "python2.7"})
            app_dir = self.write_app_directory("asset_depends", json.dumps(app_spec), "code.py")
            asset_applet = json.loads(run("dx build --json {app_dir}".format(app_dir=app_dir)))["id"]
            run("dx build --json {app_dir}".format(app_dir=app_dir))

        # success: asset found
        app_spec = dict(self.base_app_spec, name="asset_depends",
                        runSpec={"assetDepends": [{"name": record_name, "version": "0.0.1", "project": self.project,
                                                   "folder": "/record_subfolder"}],
                                 "file": "code.py", "distribution": "Ubuntu", "release": "14.04",
                                 "interpreter": "python2.7"})
        app_dir = self.write_app_directory("asset_depends", json.dumps(app_spec), "code.py")
        asset_applet = json.loads(run("dx build --json {app_dir}".format(app_dir=app_dir)))["id"]

        self.assertEqual(
            dxpy.DXApplet(asset_applet).describe()['runSpec']['bundledDepends'][0],
            {'id': {'$dnanexus_link': asset_file.get_id()}, 'name': asset_name}
        )

        # failure: multiple assets with the same name
        dxpy.new_dxrecord(project=self.project, folder="/record_subfolder", types=["AssetBundle"],
                          details=record_details, name=record_name, properties=record_properties, close=True)
        with self.assertSubprocessFailure(stderr_regexp="Found more than one asset record that matches", exit_code=3):
            app_spec = dict(self.base_app_spec, name="asset_depends_fail",
                            runSpec={"assetDepends": [{"name": record_name, "version": "0.0.1", "project": self.project,
                                                       "folder": "/record_subfolder"}],
                                     "file": "code.py", "distribution": "Ubuntu", "release": "14.04",
                                     "interpreter": "python2.7"})
            app_dir = self.write_app_directory("asset_depends_fail", json.dumps(app_spec), "code.py")
            asset_applet = json.loads(run("dx build --json {app_dir}".format(app_dir=app_dir)))["id"]
            run("dx build --json {app_dir}".format(app_dir=app_dir))

    def test_asset_depends_using_id(self):
        # upload a tar.gz file and mark it hidden
        asset_name = "test-asset.tar.gz"
        asset_file = dxpy.upload_string("xxyyzz", project=self.project, hidden=True, wait_on_close=True,
                                        name=asset_name)

        # create a record with details to the hidden asset
        record_name = "asset-record"
        record_details = {"archiveFileId": {"$dnanexus_link": asset_file.get_id()}}
        record_properties = {"version": "0.0.1"}
        record = dxpy.new_dxrecord(project=self.project, types=["AssetBundle"], details=record_details,
                                   name=record_name, properties=record_properties, close=True)

        app_spec = dict(self.base_app_spec, name="asset_depends",
                        runSpec={"assetDepends": [{"id": record.get_id()}],
                                 "file": "code.py", "distribution": "Ubuntu", "release": "14.04",
                                 "interpreter": "python2.7"})
        app_dir = self.write_app_directory("asset_depends", json.dumps(app_spec), "code.py")
        asset_applet = run_and_parse_json("dx build --json {app_dir}".format(app_dir=app_dir))["id"]
        self.assertEqual(
            dxpy.DXApplet(asset_applet).describe()['runSpec']['bundledDepends'][0],
            {'id': {'$dnanexus_link': asset_file.get_id()}, 'name': asset_name}
        )

    def test_asset_depends_failure(self):
        # upload a tar.gz file and mark it hidden
        asset_name = "test-asset.tar.gz"
        asset_file = dxpy.upload_string("xxyyzz", project=self.project, hidden=True, wait_on_close=True,
                                        name=asset_name)

        # create a record with details to the hidden asset
        record_name = "asset-record"
        record_details = {"archiveFileId": {"$dnanexus_link": asset_file.get_id()}}
        record_properties = {"version": "0.0.1"}
        dxpy.new_dxrecord(project=self.project, types=["AssetBundle"], details=record_details, name=record_name,
                          properties=record_properties, close=True)

        app_spec = dict(self.base_app_spec, name="asset_depends",
                        runSpec={"assetDepends": [{"name": record_name, "version": "0.1.1", "project": self.project}],
                                 "file": "code.py", "distribution": "Ubuntu", "release": "14.04",
                                 "interpreter": "python2.7"})
        app_dir = self.write_app_directory("asset_depends", json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp="No asset bundle was found", exit_code=3):
            run("dx build --json {app_dir}".format(app_dir=app_dir))

    def test_asset_depends_malform_details(self):
        # upload a tar.gz file and mark it hidden
        asset_name = "test-asset.tar.gz"
        asset_file = dxpy.upload_string("xxyyzz", project=self.project, hidden=True, wait_on_close=True,
                                        name=asset_name)

        # create a record with details to the hidden asset
        record_name = "asset-record"
        record_details = {"wrongField": {"$dnanexus_link": asset_file.get_id()}}
        record_properties = {"version": "0.0.1"}
        dxpy.new_dxrecord(project=self.project, types=["AssetBundle"], details=record_details, name=record_name,
                          properties=record_properties, close=True)

        app_spec = dict(self.base_app_spec, name="asset_depends",
                        runSpec={"assetDepends": [{"name": record_name, "version": "0.0.1", "project": self.project}],
                                 "file": "code.py", "distribution": "Ubuntu", "release": "14.04",
                                 "interpreter": "python2.7"})
        app_dir = self.write_app_directory("asset_depends", json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp="The required field 'archiveFileId'", exit_code=3):
            run("dx build --json {app_dir}".format(app_dir=app_dir))

    def test_asset_depends_clone(self):
        # create an asset in this project
        asset_name = "test-asset.tar.gz"
        asset_file = dxpy.upload_string("xxyyzz", project=self.project, hidden=True, wait_on_close=True,
                                        name=asset_name)

        # create a record with details to the hidden asset
        record_name = "asset-record"
        record_details = {"archiveFileId": {"$dnanexus_link": asset_file.get_id()}}
        record_properties = {"version": "0.0.1"}
        record = dxpy.new_dxrecord(project=self.project, types=["AssetBundle"], details=record_details,
                                   name=record_name, properties=record_properties, close=True)

        # create an applet with assetDepends in a different project
        with temporary_project('test_select_project', select=True):
            app_spec = dict(self.base_app_spec, name="asset_depends",
                            runSpec={"assetDepends": [{"id": record.get_id()}],
                                     "file": "code.py", "distribution": "Ubuntu", "release": "14.04",
                                     "interpreter": "python2.7"})
            app_dir = self.write_app_directory("asset_depends", json.dumps(app_spec), "code.py")
            run("dx build --json {app_dir}".format(app_dir=app_dir))
            temp_record_id = run("dx ls {asset} --brief".format(asset=record_name)).strip()
            self.assertEqual(temp_record_id, record.get_id())

    def test_dry_run_does_not_clone_asset_depends(self):
        # create an asset in this project
        asset_name = "test-asset.tar.gz"
        asset_file = dxpy.upload_string("xxyyzz", project=self.project, hidden=True, wait_on_close=True,
                                        name=asset_name)

        # create a record with details to the hidden asset
        record_name = "asset-record"
        record_details = {"archiveFileId": {"$dnanexus_link": asset_file.get_id()}}
        record_properties = {"version": "0.0.1"}
        record = dxpy.new_dxrecord(project=self.project, types=["AssetBundle"], details=record_details,
                                   name=record_name, properties=record_properties, close=True)

        # Build the applet with --dry-run: the "assetDepends" should not be
        # cloned.
        with temporary_project('does_not_clone_asset', select=True) as p:
            app_name = "asset_depends_not_cloned"
            app_spec = dict(self.base_app_spec,
                            name=app_name,
                            runSpec={"file": "code.py",
                                     "interpreter": "python2.7", "distribution": "Ubuntu", "release": "14.04",
                                     "assetDepends": [{"id": record.get_id()}]})
            app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")
            run("dx build --dry-run {app_dir}".format(app_dir=app_dir))
            self.assertIsNone(dxpy.find_one_data_object(project=p.get_id(),
                                                        name=record_name,
                                                        zero_ok=True))

    def test_asset_depends_clone_app(self):
        # upload a tar.gz file and mark it hidden
        asset_name = "test-asset.tar.gz"
        asset_file = dxpy.upload_string("xxyyzz", project=self.project, hidden=True, wait_on_close=True,
                                        name=asset_name)

        # create a record with details to the hidden asset
        record_name = "asset-record"
        record_details = {"archiveFileId": {"$dnanexus_link": asset_file.get_id()}}
        record_properties = {"version": "0.0.1"}
        dxpy.new_dxrecord(project=self.project, types=["AssetBundle"], details=record_details, name=record_name,
                          properties=record_properties, close=True)

        app_spec = dict(self.base_app_spec, name="asset_depends",
                        runSpec={"assetDepends": [{"name": record_name, "version": "0.0.1", "project": self.project}],
                                 "file": "code.py", "distribution": "Ubuntu", "release": "14.04",
                                 "interpreter": "python2.7"})
        app_dir = self.write_app_directory("asset_depends", json.dumps(app_spec), "code.py")
        asset_applet = run_and_parse_json("dx build --json {app_dir}".format(app_dir=app_dir))["id"]

        # clone the applet to a different project and test that the hidden file is also cloned
        with temporary_project('test_select_project', select=True) as temp_project:
            dxpy.DXApplet(asset_applet, project=self.project).clone(temp_project.get_id())
            # check that asset_file is also cloned to this project
            temp_asset_fid = run("dx ls {asset} --brief".format(asset=asset_name)).strip()
            self.assertEqual(temp_asset_fid, asset_file.get_id())

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create app')
    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_APP_PUBLISH"])
    def test_dx_publish_app(self):
        app_name = "dx_publish_app"

        def _create_app(version):
            app_spec = dict(self.base_app_spec, name=app_name, version=version)
            app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")
            desc = json.loads(run("dx build --app {app_dir} --json".format(app_dir=app_dir)))
            return desc

        desc = _create_app("1.0.0")
        self.assertFalse("published" in desc)
        run("dx publish {name}/{alias}".format(name=app_name, alias="default"))
        published_desc = json.loads(run("dx describe {name} --json".format(name=app_name)))
        self.assertTrue("published" in published_desc)

        # with --no-default flag
        _create_app("2.0.0")
        run("dx publish {name}/{version} --no-default".format(name=app_name,
                                                              version="2.0.0"))
        published_desc = json.loads(run("dx describe app-{name}/{version} --json".format(name=app_name,
                                                                                         version="2.0.0")))
        self.assertTrue("published" in published_desc)
        self.assertFalse("default" in published_desc["aliases"])

        # using the ID
        desc = _create_app("3.0.0")
        run("dx publish {}".format(desc['id']))
        published_desc = json.loads(run("dx describe {} --json".format(desc['id'])))
        self.assertTrue("published" in published_desc)

        with self.assertSubprocessFailure(stderr_regexp="InvalidState",
                                          exit_code=3):
            run("dx publish {name}/{version}".format(name=app_name, version="2.0.0"))


class TestDXGetWorkflows(DXTestCaseBuildWorkflows):

    def test_get_workflow(self):
        workflow_id = run("dx new workflow get_workflow --brief").strip()
        applet_01_id = dxpy.api.applet_new({"name": "myapplet_01",
                                            "project": self.project,
                                            "dxapi": "1.0.0",
                                            "inputSpec": [{"name": "my_number_in_01", "class": "int"}],
                                            "outputSpec": [{"name": "my_number_out_01", "class": "int"}],
                                            "runSpec": {"interpreter": "bash", "code": "exit 0",
                                                        "distribution": "Ubuntu", "release": "14.04"}})['id']
        applet_02_id = dxpy.api.applet_new({"name": "myapplet_02",
                                            "project": self.project,
                                            "dxapi": "1.0.0",
                                            "inputSpec": [{"name": "my_number_in_02", "class": "int"}],
                                            "outputSpec": [{"name": "my_number_out_02", "class": "int"}],
                                            "runSpec": {"interpreter": "bash", "code": "exit 0",
                                                        "distribution": "Ubuntu", "release": "14.04"}})['id']
        stage_01_name = "Stage 1 name"
        stage_01_id = dxpy.api.workflow_add_stage(workflow_id,
                                                  {"editVersion": 0, "executable": applet_01_id,
                                                   "name": stage_01_name})['stage']
        bound_input = {"my_number_in_02": {
            "$dnanexus_link": {"stage": stage_01_id, "outputField": "my_number_out_01"}}}
        stage_02_name = "Stage 2 name"
        stage_02_id = dxpy.api.workflow_add_stage(workflow_id,
                                                  {"editVersion": 1, "executable": applet_02_id,
                                                   "input": bound_input, "name": stage_02_name})['stage']

        output_workflow_spec = {
            "name": "get_workflow",
            "title": "get_workflow",
            "stages": [{
                "id": stage_01_id,
                "name": stage_01_name,
                "executable": applet_01_id
            }, {
                "id": stage_02_id,
                "name": stage_02_name,
                "executable": applet_02_id,
                "input": bound_input
            }]
        }
        with chdir(tempfile.mkdtemp()):
            run("dx get {workflow_id}".format(workflow_id=workflow_id))
            self.assertTrue(os.path.exists(os.path.join("get_workflow", "dxworkflow.json")))

            workflow_metadata = open(os.path.join("get_workflow", "dxworkflow.json")).read()
            output_json = json.loads(workflow_metadata, object_pairs_hook=collections.OrderedDict)
            self.assertEqual(output_workflow_spec, output_json)

            # Target workflow does not exist
            with self.assertSubprocessFailure(stderr_regexp='Unable to resolve', exit_code=3):
                run("dx get path_does_not_exist")

            # -o dest (dest does not exist yet)
            run("dx get -o dest get_workflow")
            self.assertTrue(os.path.exists("dest"))
            self.assertTrue(os.path.exists(os.path.join("dest", "dxworkflow.json")))

            # -o -
            with self.assertSubprocessFailure(stderr_regexp='cannot be dumped to stdout', exit_code=3):
                run("dx get -o - " + workflow_id)

            # -o dir (such that dir/workflow_name is empty)
            os.mkdir('destdir')
            os.mkdir(os.path.join('destdir', 'get_workflow'))
            run("dx get -o destdir get_workflow")  # Also tests getting by name
            self.assertTrue(os.path.exists(os.path.join("destdir", "get_workflow", "dxworkflow.json")))

            # -o dir (such that dir/workflow_name is not empty)
            os.mkdir('destdir_nonempty')
            os.mkdir(os.path.join('destdir_nonempty', 'get_workflow'))
            with open(os.path.join('destdir_nonempty', 'get_workflow', 'myfile'), 'w') as f:
                f.write('content')
            get_workflow_error = 'path "destdir_nonempty/get_workflow" already exists'
            with self.assertSubprocessFailure(stderr_regexp=get_workflow_error, exit_code=3):
                run("dx get -o destdir_nonempty get_workflow")

            # -o dir (such that dir/workflow_name is a file)
            os.mkdir('destdir_withfile')
            with open(os.path.join('destdir_withfile', 'get_workflow'), 'w') as f:
                f.write('content')
            with self.assertSubprocessFailure(stderr_regexp='already exists', exit_code=3):
                run("dx get -o destdir_withfile get_workflow")

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_DATA_OBJ_DOWNLOAD_EXECUTABLE"])
    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create global workflows')
    def test_get_global_workflow(self):
        gwf_name = "test_get_global_workflow"
        dxworkflow_json = dict(self.dxworkflow_spec, name=gwf_name, version="0.0.7")
        workflow_dir = self.write_workflow_directory(gwf_name,
                                                     json.dumps(dxworkflow_json),
                                                     readme_content="Workflow Get and Readme Please")
        built_workflow = json.loads(run("dx build --globalworkflow {} --json".format(workflow_dir)))

        identifiers = [built_workflow["id"],
                       "globalworkflow-" + gwf_name,
                       # TODO: enable once dx get can be used with non-prefixed app & global workflow name
                       # TODO: (which works for dx describe)
                       # gwf_name,
                       # gwf_name + "/0.0.7"
                       ]
        for identifier in identifiers:
            with chdir(tempfile.mkdtemp()):
                run("dx get {wfidentifier}".format(wfidentifier=identifier))
                self.assertTrue(os.path.exists(os.path.join(gwf_name, "dxworkflow.json")))
                workflow_metadata = open(os.path.join(gwf_name, "dxworkflow.json")).read()
                output_json = json.loads(workflow_metadata, object_pairs_hook=collections.OrderedDict)
                self.assertEqual(dxworkflow_json, output_json)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create global workflows')
    def test_dx_cross_get_and_build_workflows(self):
        def _build(name, atype):
            dxworkflow_json = dict(self.dxworkflow_spec, name=name)
            workflow_dir = self.write_workflow_directory(name,
                                                         json.dumps(dxworkflow_json),
                                                         readme_content="Workflow Get and Readme Please")
            built_workflow = json.loads(run("dx build --{} {} --json".format(atype, workflow_dir)))
            self.assertEqual(built_workflow["name"], name)

        def _get_and_build(name, atype):
            with chdir(tempfile.mkdtemp()):
                run("dx get {}".format('globalworkflow-' + name if atype == 'globalworkflow' else name))
                dxworkflow_json = json.loads(open(os.path.join(name, "dxworkflow.json")).read())
                self.assertIn("name", dxworkflow_json)

                # we need to stick in 'version' to dxworkflow.json to build an global workflow
                dxworkflow_json["version"] = "1.0.0"
                with open(os.path.join(name, "dxworkflow.json"), 'w') as dxapp_json_file_2:
                    dxapp_json_file_2.write(json.dumps(dxworkflow_json, ensure_ascii=False))

                # build global workflow from the new source dir, created with 'dx get'
                new_gwf = json.loads(run("dx build --json --globalworkflow {}".format(name)))
                self.assertEqual(new_gwf["class"], "globalworkflow")
                self.assertEqual(new_gwf["name"], name)

                # build regular workflow from the new source dir, created with 'dx get'
                new_workflow = json.loads(run("dx build --json {}".format(name)))
                self.assertEqual(new_workflow["class"], "workflow")
                self.assertEqual(new_workflow["name"], name)

        _build('globalworkflow_cycle', 'globalworkflow')
        _get_and_build('globalworkflow_cycle', 'globalworkflow')

        _build('workflow_cycle', 'workflow')
        _get_and_build('workflow_cycle', 'workflow')


class TestDXGetAppsAndApplets(DXTestCaseBuildApps):

    def test_get_applet(self):
        # TODO: not sure why self.assertEqual doesn't consider
        # assertEqual to pass unless the strings here are unicode strings
        app_spec = {
            "name": "get_applet",
            "dxapi": "1.0.0",
            "runSpec": {
                "file": "code.py",
                "interpreter": "python2.7",
                "distribution": "Ubuntu",
                "release": "14.04"},
            "inputSpec": [{
                "name": "in1",
                "help": "A help for in1 input param",
                "optional": False,
                "class": "file",
                "label": "A label for in1 input param",
                "patterns": ["*.bam", "*.babam", "*.pab\"abam"]
            }, {
                "name": "reads_type",
                "choices": ["single-end", "paired-end"],
                "default": "paired-end",
                "class": "string",
                "group": "Advanced Options"
            }],
            "outputSpec": [{
                "name": "out1",
                "class": "file",
                "patterns": ["*.bam"]
            }],
            "description": "Description\n",
            "developerNotes": "Developer notes\n",
            "types": ["Foo"],
            "tags": ["bar"],
            "properties": {"sample_id": "123456"},
            "details": {"key1": "value1"},
            "ignoreReuse": False
        }
        # description and developerNotes should be un-inlined back to files
        output_app_spec = dict((k, v) for (k, v) in list(app_spec.items()) if k not in ('description',
                                                                                        'developerNotes'))
        output_app_spec["runSpec"] = {"file": "src/code.py", "interpreter": "python2.7",
                                      "distribution": "Ubuntu", "release": "14.04", "version": "0"}

        output_app_spec["regionalOptions"] = {"aws:us-east-1": {"systemRequirements": {}}}

        app_dir = self.write_app_directory("get_åpplet", json.dumps(app_spec), "code.py",
                                           code_content="import os\n")
        os.mkdir(os.path.join(app_dir, "resources"))
        with open(os.path.join(app_dir, "resources", "resources_file"), 'w') as f:
            f.write('content\n')
        new_applet_id = run_and_parse_json("dx build --json " + app_dir)["id"]
        with chdir(tempfile.mkdtemp()):
            run("dx get " + new_applet_id)
            self.assertTrue(os.path.exists("get_applet"))
            self.assertTrue(os.path.exists(os.path.join("get_applet", "dxapp.json")))

            applet_metadata = open(os.path.join("get_applet", "dxapp.json")).read()

            # Checking inputSpec/outputSpec patterns arrays were flattened
            self.assertTrue(applet_metadata.find('"patterns": ["*.bam", "*.babam", "*.pab\\"abam"]') >= 0)
            self.assertTrue(applet_metadata.find('"patterns": ["*.bam"]') >= 0)

            # Checking inputSpec keys ordering
            self.assertTrue(
                applet_metadata.find('"name": "in1"') < applet_metadata.find('"label": "A label for in1 input param"'))
            self.assertTrue(applet_metadata.find('"label": "A label for in1 input param"') < applet_metadata.find(
                '"help": "A help for in1 input param"'))
            self.assertTrue(
                applet_metadata.find('"help": "A help for in1 input param"') < applet_metadata.find('"class": "file"'))
            self.assertTrue(applet_metadata.find('"class": "file"') < applet_metadata.find(
                '"patterns": ["*.bam", "*.babam", "*.pab\\"abam"]'))
            self.assertTrue(
                applet_metadata.find('"patterns": ["*.bam", "*.babam", "*.pab\\"abam"]') < applet_metadata.find(
                    '"optional": false'))

            self.assertTrue(applet_metadata.find('"name": "reads_type"') < applet_metadata.find('"class": "string"'))
            self.assertTrue(applet_metadata.find('"class": "string"') < applet_metadata.find('"default": "paired-end"'))
            self.assertTrue(applet_metadata.find('"default": "paired-end"') < applet_metadata.find('"choices": ['))
            self.assertTrue(applet_metadata.find('"choices": [') < applet_metadata.find('"group": "Advanced Options"'))

            # Checking outputSpec keys ordering
            output_spec_index = applet_metadata.find('"outputSpec"')
            self.assertTrue(
                applet_metadata.find('"name": "out1"', output_spec_index) < applet_metadata.find('"class": "file"',
                                                                                                 output_spec_index))
            self.assertTrue(applet_metadata.find('"class": "file"', output_spec_index) < applet_metadata.find(
                '"patterns": ["*.bam"]', output_spec_index))

            output_json = json.loads(applet_metadata)
            self.assertEqual(output_app_spec, output_json)
            self.assertNotIn("bundledDepends", output_json["runSpec"])
            self.assertNotIn("systemRequirementsByRegion", output_json["runSpec"])

            self.assertIn("regionalOptions", output_json)

            self.assertNotIn("description", output_json)
            self.assertNotIn("developerNotes", output_json)

            self.assertEqual("Description\n", open(os.path.join("get_applet", "Readme.md")).read())
            self.assertEqual("Developer notes\n",
                             open(os.path.join("get_applet", "Readme.developer.md")).read())
            self.assertEqual("import os\n", open(os.path.join("get_applet", "src", "code.py")).read())

            self.assertEqual("content\n",
                             open(os.path.join("get_applet", "resources", "resources_file")).read())

            # Target applet does not exist
            with self.assertSubprocessFailure(stderr_regexp='Unable to resolve', exit_code=3):
                run("dx get path_does_not_exist")

            # -o dest (dest does not exist yet)
            run("dx get -o dest get_applet")
            self.assertTrue(os.path.exists("dest"))
            self.assertTrue(os.path.exists(os.path.join("dest", "dxapp.json")))

            # -o -
            with self.assertSubprocessFailure(stderr_regexp='cannot be dumped to stdout', exit_code=3):
                run("dx get -o - " + new_applet_id)

            # -o dir (such that dir/applet_name is empty)
            os.mkdir('destdir')
            os.mkdir(os.path.join('destdir', 'get_applet'))
            run("dx get -o destdir get_applet")  # Also tests getting by name
            self.assertTrue(os.path.exists(os.path.join("destdir", "get_applet", "dxapp.json")))

            # -o dir (such that dir/applet_name is not empty)
            os.mkdir('destdir_nonempty')
            os.mkdir(os.path.join('destdir_nonempty', 'get_applet'))
            with open(os.path.join('destdir_nonempty', 'get_applet', 'myfile'), 'w') as f:
                f.write('content')
            get_applet_error = 'path "destdir_nonempty/get_applet" already exists'
            with self.assertSubprocessFailure(stderr_regexp=get_applet_error, exit_code=3):
                run("dx get -o destdir_nonempty get_applet")

            # -o dir (such that dir/applet_name is a file)
            os.mkdir('destdir_withfile')
            with open(os.path.join('destdir_withfile', 'get_applet'), 'w') as f:
                f.write('content')
            with self.assertSubprocessFailure(stderr_regexp='already exists', exit_code=3):
                run("dx get -o destdir_withfile get_applet")

            # -o dir --overwrite (such that dir/applet_name is a file)
            os.mkdir('destdir_withfile_force')
            with open(os.path.join('destdir_withfile_force', 'get_applet'), 'w') as f:
                f.write('content')
            run("dx get --overwrite -o destdir_withfile_force get_applet")
            self.assertTrue(os.path.exists(os.path.join("destdir_withfile_force", "get_applet",
                                                        "dxapp.json")))

            # -o file
            with open('destfile', 'w') as f:
                f.write('content')
            with self.assertSubprocessFailure(stderr_regexp='already exists', exit_code=3):
                run("dx get -o destfile get_applet")

            # -o file --overwrite
            run("dx get --overwrite -o destfile get_applet")
            self.assertTrue(os.path.exists("destfile"))
            self.assertTrue(os.path.exists(os.path.join("destfile", "dxapp.json")))

    def test_get_applet_omit_resources(self):
        # TODO: not sure why self.assertEqual doesn't consider
        # assertEqual to pass unless the strings here are unicode strings
        app_spec = {
            "name": "get_applet",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7", "distribution": "Ubuntu", "release": "14.04"},
            "inputSpec": [{"name": "in1", "class": "file"}],
            "outputSpec": [{"name": "out1", "class": "file"}],
            "description": "Description\n",
            "developerNotes": "Developer notes\n",
            "types": ["Foo"],
            "tags": ["bar"],
            "properties": {"sample_id": "123456"},
            "details": {"key1": "value1"},
        }
        # description and developerNotes should be un-inlined back to files
        output_app_spec = dict((k, v) for (k, v) in list(app_spec.items()) if k not in ('description',
                                                                                        'developerNotes'))
        output_app_spec["runSpec"] = {"file": "src/code.py", "interpreter": "python2.7",
                                      "distribution": "Ubuntu", "release": "14.04", "version": "0"}

        app_dir = self.write_app_directory("get_åpplet", json.dumps(app_spec), "code.py",
                                           code_content="import os\n")
        os.mkdir(os.path.join(app_dir, "resources"))
        with open(os.path.join(app_dir, "resources", "resources_file"), 'w') as f:
            f.write('content\n')
        new_applet_id = run_and_parse_json("dx build --json " + app_dir)["id"]
        with chdir(tempfile.mkdtemp()):
            run("dx get --omit-resources " + new_applet_id)
            self.assertFalse(os.path.exists(os.path.join("get_applet", "resources")))

            with open(os.path.join("get_applet", "dxapp.json")) as f2:
                output_json = json.load(f2)
            self.assertIn("bundledDepends", output_json["runSpec"])
            seenResources = False
            for bd in output_json["runSpec"]["bundledDepends"]:
                if bd["name"] == "resources.tar.gz":
                    seenResources = True
                    break
            self.assertTrue(seenResources)

    def test_get_applet_field_cleanup(self):
        # TODO: not sure why self.assertEqual doesn't consider
        # assertEqual to pass unless the strings here are unicode strings

        # When retrieving the applet, we'll get back an empty list for
        # types, tags, etc. Those should not be written back to the
        # dxapp.json so as not to pollute it.
        app_spec = dict(self.base_applet_spec, name="get_applet_field_cleanup")
        output_app_spec = app_spec.copy()
        output_app_spec["runSpec"] = {"file": "src/code.py", "interpreter": "python2.7",
                                      "distribution": "Ubuntu", "release": "14.04", "version": "0"}
        output_app_spec["regionalOptions"] = {u'aws:us-east-1': {u'systemRequirements': {}}}

        app_dir = self.write_app_directory("get_åpplet_field_cleanup", json.dumps(app_spec), "code.py",
                                           code_content="import os\n")
        os.mkdir(os.path.join(app_dir, "resources"))
        with open(os.path.join(app_dir, "resources", "resources_file"), 'w') as f:
            f.write('content\n')
        new_applet_id = run_and_parse_json("dx build --json " + app_dir)["id"]
        with chdir(tempfile.mkdtemp()):
            run("dx get " + new_applet_id)
            self.assertTrue(os.path.exists("get_applet_field_cleanup"))
            self.assertTrue(os.path.exists(os.path.join("get_applet_field_cleanup", "dxapp.json")))
            output_json = json.load(open(os.path.join("get_applet_field_cleanup", "dxapp.json")))
            self.assertEqual(output_app_spec, output_json)
            self.assertFalse(os.path.exists(os.path.join("get_applet", "Readme.md")))
            self.assertFalse(os.path.exists(os.path.join("get_applet", "Readme.developer.md")))

    def test_get_applet_on_windows(self):
        # This test is to verify that "dx get applet" works correctly on windows,
        # making sure the resource directory is downloaded.
        app_spec = dict(self.base_applet_spec, name="get_applet_windows")
        output_app_spec = app_spec.copy()
        output_app_spec["runSpec"] = {"file": "src/code.py", "interpreter": "python2.7",
                                      "distribution": "Ubuntu", "release": "14.04", "version": "0"}
        output_app_spec["regionalOptions"] = {u'aws:us-east-1': {u'systemRequirements': {}}}

        app_dir = self.write_app_directory("get_åpplet_windows", json.dumps(app_spec), "code.py",
                                           code_content="import os\n")
        os.mkdir(os.path.join(app_dir, "resources"))
        with open(os.path.join(app_dir, "resources", "resources_file"), 'w') as f:
            f.write('content\n')
        new_applet_id = json.loads(run("dx build --json " + app_dir))["id"]
        with chdir(tempfile.mkdtemp()):
            run("dx get " + new_applet_id)
            self.assertTrue(os.path.exists("get_applet_windows"))
            self.assertTrue(os.path.exists(os.path.join("get_applet_windows", "dxapp.json")))
            output_json = json.load(open(os.path.join("get_applet_windows", "dxapp.json")))
            self.assertEqual(output_app_spec, output_json)
            self.assertFalse(os.path.exists(os.path.join("get_applet_windows", "Readme.md")))
            self.assertFalse(os.path.exists(os.path.join("get_applet_windows", "Readme.developer.md")))
            self.assertEqual("import os\n", open(os.path.join("get_applet_windows", "src", "code.py")).read())
            self.assertEqual("content\n",
                             open(os.path.join("get_applet_windows", "resources", "resources_file")).read())

    def make_app(self, name, open_source=True, published=True, authorized_users=[], regional_options=None):
        if regional_options is None:
            regional_options = {"aws:us-east-1": {"systemRequirements": {}},
                                "azure:westus": {"systemRequirements": {}}}
        elif not isinstance(regional_options, dict):
            raise ValueError("'regional_options' should be None or a dict")

        app_spec = {
            "name": name,
            "title": "Sir",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7",
                        "distribution": "Ubuntu", "release": "14.04", "version": "0"},
            "inputSpec": [{"name": "in1", "class": "file"}],
            "outputSpec": [{"name": "out1", "class": "file"}],
            "description": "Description\n",
            "developerNotes": "Developer notes\n",
            "authorizedUsers": authorized_users,
            "openSource": open_source,
            "version": "0.0.1",
            "regionalOptions": regional_options}

        # description and developerNotes should be un-inlined back to files
        output_app_spec = dict((k, v)
                               for (k, v) in app_spec.iteritems()
                               if k not in ('description', 'developerNotes'))
        output_app_spec["runSpec"] = {"file": "src/code.py", "interpreter": "python2.7",
                                      "distribution": "Ubuntu", "release": "14.04", "version": "0"}

        app_dir = self.write_app_directory(name,
                                           json.dumps(app_spec),
                                           "code.py",
                                           code_content="import os\n")
        os.mkdir(os.path.join(app_dir, "resources"))
        with open(os.path.join(app_dir, "resources", "resources_file"), 'w') as f:
            f.write('content\n')
        if published:
            build_cmd = "dx build --create-app --json --publish "
        else:
            build_cmd = "dx build --create-app --json "

        app_json = json.loads(run(build_cmd + app_dir))
        app_id = app_json["id"]
        app_describe = dxpy.api.app_describe(app_id)
        self.assertEqual(app_describe["class"], "app")
        self.assertEqual(app_describe["version"], "0.0.1")
        self.assertEqual(app_describe["name"], name)
        if published:
            self.assertTrue("published" in app_describe)
        else:
            self.assertFalse("published" in app_describe)

        self.assertTrue(os.path.exists(os.path.join(app_dir, 'code.py')))
        self.assertFalse(os.path.exists(os.path.join(app_dir, 'code.pyc')))
        return [app_id, output_app_spec]

    def assert_app_get_initialized(self, name, app_spec):
        self.assertTrue(os.path.exists(name))
        self.assertTrue(os.path.exists(os.path.join(name,
                                                    "dxapp.json")))
        output_json = json.load(open(os.path.join(name,
                                                  "dxapp.json")),
                                object_pairs_hook=collections.OrderedDict)

        black_list = ['published']

        if not app_spec['openSource']:
            black_list.append('openSource')

        if not app_spec['authorizedUsers']:
            black_list.append('authorizedUsers')

        filtered_app_spec = dict((k, v)
                                 for (k, v) in app_spec.iteritems()
                                 if k not in black_list)

        self.assertNotIn("description", output_json)
        self.assertNotIn("developerNotes", output_json)

        self.assertNotIn("systemRequirements", output_json["runSpec"])
        self.assertNotIn("systemRequirementsByRegion", output_json["runSpec"])

        self.assertDictSubsetOf(filtered_app_spec, output_json)

        self.assertFileContentsEqualsString([name, "src",
                                             "code.py"],
                                            "import os\n")

        self.assertFileContentsEqualsString([name,
                                             "Readme.md"],
                                            "Description\n")

        self.assertFileContentsEqualsString([name, "Readme.developer.md"],
                                            "Developer notes\n")

        self.assertFileContentsEqualsString([name, "resources", "resources_file"],
                                            "content\n")

    def _test_cant_get_app(self, name, open_source, published, authorized_users):
        [app_id, output_app_spec] = self.make_app(name,
                                                  open_source,
                                                  published,
                                                  authorized_users)

        with chdir(tempfile.mkdtemp()):
            # -o -
            with self.assertSubprocessFailure(stderr_regexp='cannot be dumped to stdout', exit_code=3):
                run("dx get -o - " + app_id)

            # Target app does not exist
            with self.assertSubprocessFailure(stderr_regexp='Unable to resolve', exit_code=3):
                run("dx get path_does_not_exist")

    def _test_get_app(self, name, open_source, published, authorized_users):
        second = json.loads(os.environ['DXTEST_SECOND_USER'])
        second_user_id = second['user']
        [app_id, output_app_spec] = self.make_app(name,
                                                  open_source,
                                                  published,
                                                  authorized_users)

        with chdir(tempfile.mkdtemp()):
            run("dx get {}".format(app_id))
            self.assert_app_get_initialized(name, output_app_spec)

        # Second test app is openSource && published, second user is an authorized user, should succeed
        with chdir(tempfile.mkdtemp()):
            with without_project_context():
                if second_user_id in authorized_users and open_source and published:
                    run('dx get {}'.format(app_id), env=as_second_user())
                    self.assert_app_get_initialized(name, output_app_spec)
                else:
                    with self.assertSubprocessFailure(stderr_regexp='code 401', exit_code=3):
                        run('dx get {}'.format(app_id), env=as_second_user())

    @unittest.skipUnless(testutil.TEST_ENV, 'skipping test that would clobber your local environment')
    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create apps')
    @unittest.skipUnless(testutil.TEST_MULTIPLE_USERS, 'skipping test that would require another user')
    def test_get_app(self):
        second = json.loads(os.environ['DXTEST_SECOND_USER'])
        second_user_id = second['user']
        authorized_users = [second_user_id]

        self._test_cant_get_app("get_app_failure", True, True, authorized_users)
        self._test_get_app("get_app_open_source_published", True, True, authorized_users)
        self._test_get_app("get_app_open_source", True, False, authorized_users)
        self._test_get_app("get_app_published", False, True, authorized_users)
        self._test_get_app("get_app", False, False, authorized_users)

        self._test_get_app("get_app_open_source_published_no_authusers", True, True, [])
        self._test_get_app("get_app_published_no_authusers", False, True, [])

    @unittest.skipUnless(testutil.TEST_ENV, 'skipping test that would clobber your local environment')
    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create apps')
    def test_dx_cross_get_and_build_app_and_applet(self):
        sysreq_spec = {"aws:us-east-1": {"systemRequirements": {"*": {"instanceType": "mem1_ssd1_x8"}}}}

        def _build(name, atype):
            app_spec = dict(self.base_app_spec, name=name, regionalOptions=sysreq_spec)
            app_dir = self.write_app_directory(name, json.dumps(app_spec), "code.py")
            atype = '--app' if atype == 'app' else ''
            run("dx build {} {}".format(atype, app_dir))

        def _get_and_build(name, atype):
            with chdir(tempfile.mkdtemp()):
                run("dx get {}".format('app-' + name if atype == 'app' else name))
                dxapp_json = json.loads(open(os.path.join(name, "dxapp.json")).read())
                self.assertNotIn("systemRequirements", dxapp_json['runSpec'])
                self.assertNotIn("systemRequirementsByRegion", dxapp_json['runSpec'])
                self.assertEqual(dxapp_json["regionalOptions"], sysreq_spec)

                # we need to stick in 'version' to dxapp,json to build an app
                dxapp_json["version"] = "1.0.0"
                with open(os.path.join(name, "dxapp.json"), 'w') as dxapp_json_file_2:
                    dxapp_json_file_2.write(json.dumps(dxapp_json, ensure_ascii=False))

                # build app from the new source dir, created with 'dx get'
                app_desc = json.loads(run("dx build --json --app {}".format(name)))
                self.assertEqual(app_desc["class"], "app")
                self.assertEqual(app_desc["name"], name)

                # build applet from the new source dir, created with 'dx get'
                applet_desc = json.loads(run("dx build --json -a {}".format(name)))
                self.assertEqual(applet_desc["class"], "applet")
                self.assertEqual(applet_desc["name"], name)

        _build('app_cycle', 'app')
        _get_and_build('app_cycle', 'app')

        _build('applet_cycle', 'applet')
        _get_and_build('applet_cycle', 'applet')

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create apps')
    def test_get_app_by_name(self):
        [app_id, output_app_spec] = self.make_app("cool_app_name", False, False, [])

        with chdir(tempfile.mkdtemp()):
            run("dx get app-cool_app_name")
            self.assert_app_get_initialized("cool_app_name", output_app_spec)

        with chdir(tempfile.mkdtemp()):
            run("dx get app-cool_app_name/0.0.1")
            self.assert_app_get_initialized("cool_app_name", output_app_spec)

        with chdir(tempfile.mkdtemp()):
            with self.assertSubprocessFailure(stderr_regexp="Could not find an app", exit_code=3):
                run("dx get app-not_so_cool_app_name")

            with self.assertSubprocessFailure(stderr_regexp="Could not find an app", exit_code=3):
                run("dx get app-cool_app_name/1.0.0")

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create apps')
    def test_get_app_omit_resources(self):
        app_name = "omit_resources"
        app_spec = {
            "name": app_name,
            "title": "Sir",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7",
                        "distribution": "Ubuntu", "release": "14.04"},
            "inputSpec": [{"name": "in1", "class": "file"}],
            "outputSpec": [{"name": "out1", "class": "file"}],
            "description": "Description\n",
            "developerNotes": "Developer notes\n",
            "openSource": True,
            "version": "0.0.1"
        }
        # description and developerNotes should be un-inlined back to files
        output_app_spec = dict((k, v)
                               for (k, v) in app_spec.iteritems()
                               if k not in ('description', 'developerNotes'))
        output_app_spec["runSpec"] = {"file": "src/code.py", "interpreter": "python2.7",
                                      "distribution": "Ubuntu", "release": "14.04"}

        app_dir = self.write_app_directory(app_name,
                                           json.dumps(app_spec),
                                           "code.py",
                                           code_content="import os\n")
        os.mkdir(os.path.join(app_dir, "resources"))
        with open(os.path.join(app_dir, "resources", "resources_file"), 'w') as f:
            f.write('content\n')
        new_app_json = json.loads(run("dx build --create-app --json " + app_dir))
        new_app_id = new_app_json["id"]
        # app_describe = json.loads(run("dx describe --json " + new_app_json["id"]))
        app_describe = dxpy.api.app_describe(new_app_json["id"])

        self.assertEqual(app_describe["class"], "app")
        self.assertEqual(app_describe["version"], "0.0.1")
        self.assertEqual(app_describe["name"], app_name)
        self.assertFalse("published" in app_describe)
        self.assertTrue(os.path.exists(os.path.join(app_dir, 'code.py')))
        self.assertFalse(os.path.exists(os.path.join(app_dir, 'code.pyc')))

        with chdir(tempfile.mkdtemp()):
            run("dx get --omit-resources " + new_app_id)
            self.assertFalse(os.path.exists(os.path.join(app_name, "resources")))

            output_json = json.load(open(os.path.join(app_name, "dxapp.json")))
            self.assertTrue("bundledDepends" in output_json["runSpec"])
            seenResources = False
            for bd in output_json["runSpec"]["bundledDepends"]:
                if bd["name"] == "resources.tar.gz":
                    seenResources = True
                    break
            self.assertTrue(seenResources)

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_APP_LIST_AVAILABLE_APPS",
                                          "DNA_CLI_APP_INSTALL_APP",
                                          "DNA_CLI_APP_UNINSTALL_APP",
                                          "DNA_API_APP_INSTALL",
                                          "DNA_API_APP_UNINSTALL"])
    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV and testutil.TEST_MULTIPLE_USERS,
                         'skipping test that would create apps and another user')
    def test_uninstall_app(self):
        second_user_id = json.loads(os.environ['DXTEST_SECOND_USER'])['user']
        authorized_users = [second_user_id]
        name = 'uninstall_test'
        app_spec = {
            "name": name,
            "title": name,
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7",
                        "distribution": "Ubuntu", "release": "14.04"},
            "inputSpec": [],
            "outputSpec": [],
            "description": "Description\n",
            "developerNotes": "Developer notes\n",
            "authorizedUsers": authorized_users,
            "openSource": True,
            "version": "0.0.1"
        }
        app_dir = self.write_app_directory(name, json.dumps(app_spec), "code.py", code_content="import os\n")
        build_cmd = "dx build --create-app --json --publish "
        app_json = json.loads(run(build_cmd + app_dir))
        self.assertEqual(app_json['name'], name)
        # Install and check uninstall by name
        run("dx install " + name, env=as_second_user())
        output = json.loads(run("dx find apps --installed --json", env=as_second_user()))
        self.assertIn(name, [x['describe']['name'] for x in output])
        dxpy.api.app_remove_authorized_users(app_json['id'],
                                             input_params={'authorizedUsers': list(authorized_users)})
        output = dxpy.api.app_describe(app_json['id'])
        self.assertNotIn(second_user_id, output['authorizedUsers'])
        dxpy.api.project_invite(self.project, input_params={'invitee': second_user_id, 'level': 'VIEW'})
        user_data = json.loads(run('dx describe --json ' + second_user_id, env=as_second_user()))
        self.assertIn(name, user_data['appsInstalled'])
        run("dx uninstall %s" % name, env=as_second_user())
        output = json.loads(run("dx find apps --installed --json", env=as_second_user()))
        self.assertNotIn(name, [x['describe']['name'] for x in output])
        user_data = json.loads(run('dx describe --json ' + second_user_id, env=as_second_user()))
        self.assertNotIn(name, user_data['appsInstalled'])
        # Install and check uninstall by ID
        dxpy.api.app_add_authorized_users(app_json['id'],
                                          input_params={'authorizedUsers': list(authorized_users)})
        run("dx install " + name, env=as_second_user())
        dxpy.api.app_remove_authorized_users(app_json['id'],
                                             input_params={'authorizedUsers': list(authorized_users)})
        user_data = json.loads(run('dx describe --json ' + second_user_id, env=as_second_user()))
        self.assertIn(name, user_data['appsInstalled'])
        run("dx uninstall %s" % app_json['id'], env=as_second_user())
        user_data = json.loads(run('dx describe --json ' + second_user_id, env=as_second_user()))
        self.assertNotIn(name, user_data['appsInstalled'])
        # Check for App not found
        app_unknown_name = ''.join(random.choice(string.ascii_lowercase) for _ in range(12))
        with self.assertSubprocessFailure(stderr_regexp='Could not find the app', exit_code=3):
            run("dx uninstall %s" % app_unknown_name, env=as_second_user())
        pass

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV and testutil.TEST_AZURE,
                         'skipping test that would create apps')
    def test_get_preserves_system_requirements(self):
        app_name = "app_{t}_multi_region_app_with_regional_system_requirements".format(t=int(time.time()))

        aws_us_east_system_requirements = dict(main=dict(instanceType="mem2_hdd2_x1"))
        azure_westus_system_requirements = dict(main=dict(instanceType="azure:mem2_ssd1_x1"))
        regional_options = {"aws:us-east-1": dict(systemRequirements=aws_us_east_system_requirements),
                            "azure:westus": dict(systemRequirements=azure_westus_system_requirements)}

        app_id, _ = self.make_app(app_name, regional_options=regional_options)

        with chdir(tempfile.mkdtemp()):
            run("dx get {app_id}".format(app_id=app_id))
            path_to_dxapp_json = "./{app_name}/dxapp.json".format(app_name=app_name)
            with open(path_to_dxapp_json, "r") as fh:
                app_spec = json.load(fh)
                self.assertEqual(app_spec["regionalOptions"], regional_options)


class TestDXBuildReportHtml(unittest.TestCase):
    js = "console.log('javascript');"
    css = "body {background-color: green;}"

    def setUp(self):
        self.temp_file_path = tempfile.mkdtemp()
        self.gif_base64 = "R0lGODdhAQABAIAAAAQCBAAAACwAAAAAAQABAAACAkQBADs="
        gif_file = open("{}/img.gif".format(self.temp_file_path), "wb")
        gif_file.write(base64.b64decode(self.gif_base64))
        gif_file.close()
        wiki_logo = "http://upload.wikimedia.org/wikipedia/en/thumb/8/80/Wikipedia-logo-v2.svg/200px-Wikipedia-logo-v2.svg.png"
        script_file = open("{}/index.js".format(self.temp_file_path), "w")
        script_file.write(self.js)
        script_file.close()
        css_file = open("{}/index.css".format(self.temp_file_path), "w")
        css_file.write(self.css)
        css_file.close()
        html_file = open("{}/index.html".format(self.temp_file_path), "w")
        html = "<html><head><link rel='stylesheet' href='index.css' type='text/css'/><script src='index.js'></script></head><body><a href='/'/><a href='/' target='_new'/><img src='img.gif'/><img src='{}'/></body></html>".format(
            wiki_logo)
        html_file.write(html)
        html_file.close()

        self.proj_id = dxpy.api.project_new({'name': 'TestDXBuildReportHtml Project'})['id']
        os.environ['DX_PROJECT_CONTEXT_ID'] = self.proj_id

    def tearDown(self):
        shutil.rmtree(self.temp_file_path)
        dxpy.api.project_destroy(self.proj_id, {'terminateJobs': True})

    def test_local_file(self):
        run("dx-build-report-html {d}/index.html --local {d}/out.html".format(d=self.temp_file_path))
        out_path = "{}/out.html".format(self.temp_file_path)
        self.assertTrue(os.path.exists(out_path))
        f = open(out_path, "r")
        html = f.read()
        f.close()
        self.assertTrue(re.search(self.gif_base64, html))
        self.assertEqual(len(re.split("src=\"data:image", html)), 3)
        self.assertEqual(len(re.split("<img", html)), 3)
        self.assertTrue(re.search("target=\"_top\"", html))
        self.assertTrue(re.search("target=\"_new\"", html))
        self.assertTrue(re.search("<style", html))
        self.assertTrue(re.search(re.escape(self.css), html))
        self.assertFalse(re.search("<link", html))
        self.assertFalse(re.search("index.css", html))
        self.assertTrue(re.search(re.escape(self.js), html))
        self.assertFalse(re.search("index.js", html))

    def test_image_only(self):
        run("dx-build-report-html {d}/img.gif --local {d}/gif.html".format(d=self.temp_file_path))
        out_path = "{}/gif.html".format(self.temp_file_path)
        self.assertTrue(os.path.exists(out_path))
        f = open(out_path, "r")
        html = f.read()
        f.close()
        self.assertTrue(re.search("<img src=\"data:", html))

    def test_remote_file(self):
        report = json.loads(
            run("dx-build-report-html {d}/index.html --remote /html_report -w 47 -g 63".format(d=self.temp_file_path)))
        fileId = report["fileIds"][0]
        desc = json.loads(run("dx describe {record} --details --json".format(record=report["recordId"])))
        self.assertEqual(desc["types"], ["Report", "HTMLReport"])
        self.assertEqual(desc["name"], "html_report")
        self.assertEqual(desc["details"]["files"][0]["$dnanexus_link"], fileId)
        self.assertEqual(desc["details"]["width"], "47")
        self.assertEqual(desc["details"]["height"], "63")
        desc = json.loads(run("dx describe {file} --details --json".format(file=fileId)))
        self.assertTrue(desc["hidden"])
        self.assertEqual(desc["name"], "index.html")
        run("dx rm {record} {file}".format(record=report["recordId"], file=fileId))


@unittest.skipUnless(testutil.TEST_TCSH, 'skipping tests that require tcsh to be installed')
class TestTcshEnvironment(unittest.TestCase):
    def test_tcsh_dash_c(self):
        # tcsh -c doesn't set $_, or provide any other way for us to determine the source directory, so
        # "source environment" only works from DNANEXUS_HOME
        run(
            'cd $DNANEXUS_HOME && env - HOME=$HOME PATH=/usr/local/bin:/usr/bin:/bin tcsh -c "source /etc/csh.cshrc && source /etc/csh.login && source $DNANEXUS_HOME/environment && dx --help"')
        run(
            'cd $DNANEXUS_HOME && env - HOME=$HOME PATH=/usr/local/bin:/usr/bin:/bin tcsh -c "source /etc/csh.cshrc && source /etc/csh.login && source $DNANEXUS_HOME/environment.csh && dx --help"')

    def test_tcsh_source_environment(self):
        tcsh = pexpect.spawn("env - HOME=$HOME PATH=/usr/local/bin:/usr/bin:/bin tcsh",
                             **spawn_extra_args)
        tcsh.logfile = sys.stdout
        tcsh.setwinsize(20, 90)
        tcsh.sendline("source /etc/csh.cshrc")
        tcsh.sendline("source /etc/csh.login")
        tcsh.sendline("dx")
        tcsh.expect("Command not found")
        tcsh.sendline("source ../../../environment")
        tcsh.sendline("dx")
        tcsh.expect("dx is a command-line client")


class TestDXScripts(DXTestCase):
    def test_minimal_invocation(self):
        # For dxpy scripts that have no other tests, these dummy calls
        # ensure that the coverage report is aware of them (instead of
        # excluding them altogether from the report, which artificially
        # inflates our %covered).
        #
        # This is a hack and obviously it would be preferable to figure
        # out why the coverage generator sometimes likes to include
        # these files and sometimes likes to exclude them.
        run('dx-build-applet -h')


class TestDXCp(DXTestCase):
    @classmethod
    def setUpClass(cls):
        # setup two projects
        cls.proj_id1 = create_project()
        cls.proj_id2 = create_project()
        cls.counter = 1

    @classmethod
    def tearDownClass(cls):
        rm_project(cls.proj_id1)
        rm_project(cls.proj_id2)

    @classmethod
    def gen_uniq_fname(cls):
        cls.counter += 1
        return "file_{}".format(cls.counter)

    # Make sure a folder (path) has the same contents in the two projects.
    # Note: the contents of the folders are not listed recursively.
    def verify_folders_are_equal(self, path):
        listing_proj1 = list_folder(self.proj_id1, path)
        listing_proj2 = list_folder(self.proj_id2, path)
        self.assertEqual(listing_proj1, listing_proj2)

    def verify_file_ids_are_equal(self, path1, path2=None):
        if path2 is None:
            path2 = path1
        listing_proj1 = run("dx ls {proj}:/{path} --brief".format(proj=self.proj_id1, path=path1).strip())
        listing_proj2 = run("dx ls {proj}:/{path} --brief".format(proj=self.proj_id2, path=path2).strip())
        self.assertEqual(listing_proj1, listing_proj2)

    # create new file with the same name in the target
    #    dx cp  proj-1111:/file-1111   proj-2222:/
    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_API_PROJ_VIEW_DATA_IN_FOLDER"])
    def test_file_with_same_name(self):
        create_folder_in_project(self.proj_id1, "/earthsea")
        create_folder_in_project(self.proj_id2, "/earthsea")
        file_id = create_file_in_project(self.gen_uniq_fname(), self.proj_id1, folder="/earthsea")
        run("dx cp {p1}:/earthsea/{f} {p2}:/earthsea/".format(f=file_id, p1=self.proj_id1, p2=self.proj_id2))
        self.verify_folders_are_equal("/earthsea")

    # copy and rename
    #   dx cp  proj-1111:/file-1111   proj-2222:/file-2222
    def test_cp_rename(self):
        basename = self.gen_uniq_fname()
        create_file_in_project(basename, self.proj_id1)
        run("dx cp {p1}:/{f1} {p2}:/{f2}".format(f1=basename, f2="AAA.txt",
                                                 p1=self.proj_id1, p2=self.proj_id2))
        self.verify_file_ids_are_equal(basename, path2="AAA.txt")

    # multiple arguments
    #   dx cp  proj-1111:/file-1111 proj-2222:/file-2222 proj-3333:/
    def test_multiple_args(self):
        fname1 = self.gen_uniq_fname()
        fname2 = self.gen_uniq_fname()
        fname3 = self.gen_uniq_fname()
        create_file_in_project(fname1, self.proj_id1)
        create_file_in_project(fname2, self.proj_id1)
        create_file_in_project(fname3, self.proj_id1)
        run("dx cp {p1}:/{f1} {p1}:/{f2} {p1}:/{f3} {p2}:/".
            format(f1=fname1, f2=fname2, f3=fname3, p1=self.proj_id1, p2=self.proj_id2))
        self.verify_file_ids_are_equal(fname1)
        self.verify_file_ids_are_equal(fname2)
        self.verify_file_ids_are_equal(fname3)

    # copy an entire directory
    def test_cp_dir(self):
        create_folder_in_project(self.proj_id1, "/foo")
        run("dx cp {p1}:/foo {p2}:/".format(p1=self.proj_id1, p2=self.proj_id2))
        self.verify_folders_are_equal("/foo")

    # Weird error code:
    #   This part makes sense:
    #     'InvalidState: If cloned, a folder would conflict with the route of an existing folder.'
    #   This does not:
    #     'Successfully cloned from project: None, code 422'
    #
    def test_copy_empty_folder_on_existing_folder(self):
        create_folder_in_project(self.proj_id1, "/bar")
        create_folder_in_project(self.proj_id2, "/bar")
        with self.assertSubprocessFailure(stderr_regexp='If cloned, a folder would conflict', exit_code=3):
            run("dx cp {p1}:/bar {p2}:/".format(p1=self.proj_id1, p2=self.proj_id2))
        self.verify_folders_are_equal("/bar")

    def test_copy_folder_on_existing_folder(self):
        create_folder_in_project(self.proj_id1, "/baz")
        create_file_in_project(self.gen_uniq_fname(), self.proj_id1, folder="/baz")
        run("dx cp {p1}:/baz {p2}:/".format(p1=self.proj_id1, p2=self.proj_id2))
        with self.assertSubprocessFailure(stderr_regexp='If cloned, a folder would conflict', exit_code=3):
            run("dx cp {p1}:/baz {p2}:/".format(p1=self.proj_id1, p2=self.proj_id2))
        self.verify_folders_are_equal("/baz")

    # PTFM-13569: This used to give a weird error message, like so:
    # dx cp project-BV80zyQ0Ffb7fj64v03fffqX:/foo/XX.txt  project-BV80vzQ0P9vk785K1GgvfZKv:/foo/XX.txt
    # The following objects already existed in the destination container and were not copied:
    #   [
    #   "
    #   f
    #   l
    #   ...
    def test_copy_overwrite(self):
        fname1 = self.gen_uniq_fname()
        file_id1 = create_file_in_project(fname1, self.proj_id1)
        run("dx cp {p1}:/{f} {p2}:/{f}".format(p1=self.proj_id1, f=fname1, p2=self.proj_id2))
        output = run("dx cp {p1}:/{f} {p2}:/{f}".format(p1=self.proj_id1,
                                                        f=fname1, p2=self.proj_id2))
        self.assertIn("destination", output)
        self.assertIn("already existed", output)
        self.assertIn(file_id1, output)

    # 'dx cp' used to give a confusing error message when source file is not found.
    # Check that this has been fixed
    def test_error_msg_for_nonexistent_folder(self):
        fname1 = self.gen_uniq_fname()
        create_file_in_project(fname1, self.proj_id1)

        # The file {proj_id1}:/{f} exists, however, {proj_id1}/{f} does not
        expected_err_msg = "ResolutionError: The specified folder could not be found in {p}".format(p=self.project)
        with self.assertSubprocessFailure(stderr_regexp=expected_err_msg, exit_code=3):
            run("dx cp {p1}/{f} {p2}:/".format(p1=self.proj_id1, f=fname1, p2=self.proj_id2))

        with self.assertSubprocessFailure(stderr_regexp="The destination folder does not exist",
                                          exit_code=3):
            run("dx cp {p1}:/{f} {p2}:/xxx/yyy/z.txt".format(p1=self.proj_id1, f=fname1, p2=self.proj_id2))

        with self.assertSubprocessFailure(
                stderr_regexp="source path and the destination path resolved to the same project",
                exit_code=3):
            run("dx cp {p1}:/{f} {p1}:/".format(p1=self.proj_id1, f=fname1))

    @unittest.skip("PTFM-11906 This doesn't work yet.")
    def test_file_in_other_project(self):
        ''' Copy a file-id, where the file is not located in the default project-id.

        Main idea: create projects A and B. Create a file in A, and copy it to project B,
        -without- specifying a source project.

        This could work, with some enhancements to the 'dx cp' implementation.
        '''
        file_id = create_file_in_project(self.gen_uniq_fname(), self.proj_id1)
        run('dx cp ' + file_id + ' ' + self.proj_id2)

    @unittest.skipUnless(testutil.TEST_ENV,
                         'skipping test that would clobber your local environment')
    # This will start working, once PTFM-11906 is addressed. The issue is
    # that you must specify a project when copying a file. In theory this
    # can be addressed, because the project can be found, given the file-id.
    def test_no_env(self):
        ''' Try to copy a file when the context is empty.
        '''
        # create a file in the current project
        #  -- how do we get the current project id?
        file_id = create_file_in_project(self.gen_uniq_fname(), self.project)

        # Copy the file to a new project.
        # This does not currently work, because the context is not set.
        proj_id = create_project()
        with self.assertSubprocessFailure(stderr_regexp='project must be specified or a current project set',
                                          exit_code=3), without_project_context():
            run('dx cp ' + file_id + ' ' + proj_id)

        # cleanup
        rm_project(proj_id)


class TestDXLs(DXTestCase):
    def test_regular_output(self):
        dxpy.new_dxrecord(project=self.project, name="foo", close=True)
        o = run("dx ls")
        self.assertEqual(o.strip(), "foo")

    def test_long_output(self):
        rec = dxpy.new_dxrecord(project=self.project, name="foo", close=True)
        o = run("dx ls -l")
        #                             state    modified                              name      id
        self.assertRegex(o, r"closed\s+\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+foo \(" + rec.get_id() + "\)")


class TestDXTree(DXTestCase):

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_PROJ_LIST_FOLDERS_OBJECTS_TREE"])
    def test_regular_output(self):
        dxpy.new_dxrecord(project=self.project, name="foo", close=True)
        o = run("dx tree")
        self.assertEqual(o.strip(), '.\n└── foo')

    def test_tree(self):
        rec = dxpy.new_dxrecord(project=self.project, name="foo", close=True)
        o = run("dx tree -l")
        self.assertRegex(o.strip(),
                         r".\n└── closed\s+\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+foo \(" + rec.get_id() + "\)")


class TestDXGenerateBatchInputs(DXTestCase):
    # More advanced corner cases of generateBatchInputs API calls performed in API unit tests
    def test_example_matches(self):
        # Upload test files to the project
        files = "RP10B_S1_R1_001.fastq.gz RP10B_S1_R2_001.fastq.gz RP10T_S5_R1_001.fastq.gz RP10T_S5_R2_001.fastq.gz RP15B_S4_R1_002.fastq.gz RP15B_S4_R2_002.fastq.gz RP15T_S8_R1_002.fastq.gz RP15T_S8_R2_002.fastq.gz SRR123_1.gz SRR223_2.gz SRR2223_2.gz SRR1_1.gz SRR1_1.gz"
        run("touch {}".format(files))
        run("dx upload --brief {}".format(files))

        # Test for basic working TSV and stderr output
        readpair_test_stderr = run(
            "dx generate_batch_inputs -ipair1='RP(.*)_R1_(.*).fastq.gz' -ipair2='RP(.*)_R2_(.*).fastq.gz' 2>&1")
        expected_readpair_test_stderr = """
        Found 4 valid batch IDs matching desired pattern.
        Created batch file dx_batch.0000.tsv

        CREATED 1 batch files each with at most 500 batch IDs.
        """
        self.assertEqual(readpair_test_stderr.strip(), textwrap.dedent(expected_readpair_test_stderr).strip())

        expected_readpair_test_tsv_cut = '''
        10B_S1\tRP10B_S1_R1_001.fastq.gz\tRP10B_S1_R2_001.fastq.gz
        10T_S5\tRP10T_S5_R1_001.fastq.gz\tRP10T_S5_R2_001.fastq.gz
        15B_S4\tRP15B_S4_R1_002.fastq.gz\tRP15B_S4_R2_002.fastq.gz
        15T_S8\tRP15T_S8_R1_002.fastq.gz\tRP15T_S8_R2_002.fastq.gz
        batch ID\tpair1\tpair2
        '''

        readpair_test_tsv_cut = run("cut -f1-3 dx_batch.0000.tsv | sort")
        self.assertEqual(readpair_test_tsv_cut.strip(), textwrap.dedent(expected_readpair_test_tsv_cut).strip())

        try:
            cornercase_test_stderr = run(
                "dx generate_batch_inputs -ipair1='SRR1(.*)_1.gz' -ipair2='SRR2(.*)_2.gz' 2>&1")
            raise Exception("Expected test to return non-zero exit code, but it did not.")
        except Exception as e:
            cornercase_test_stderr = str(e.output)

        expected_cornercase_test_stderr = """
        Found 1 valid batch IDs matching desired pattern.
        Created batch file dx_batch.0000.tsv

        ERROR processing batch ID matching pattern "223"
            Mismatched set of input names.
                Required input names: pair1, pair2
                Matched input names: pair2

        ERROR processing batch ID matching pattern ""
            Mismatched set of input names.
                Required input names: pair1, pair2
                Matched input names: pair1
        Input pair1 is associated with a file name that matches multiple IDs:
        """
        self.assertTrue(cornercase_test_stderr.startswith(textwrap.dedent(expected_cornercase_test_stderr).strip()))


class TestDXRun(DXTestCase):
    @unittest.skipUnless(testutil.TEST_WITH_SMOKETEST_APP,
                         'skipping test that requires the smoketest app')
    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_API_APP_RUN"])
    def test_dx_run_app(self):
        app_name = "app-dnanexus_smoke_test"
        run("dx run {} -isubjobs=1 --yes --wait --watch".format(app_name))

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that runs jobs')
    def test_dx_run_specific_project(self):
        with temporary_project('tmp_project_for_run', select=True) as temp_project:
            dxpy.api.applet_new({
                "project": temp_project.get_id(),
                "name": "myapplet",
                "dxapi": "1.0.0",
                "inputSpec": [{"name": "number", "class": "int"}],
                "outputSpec": [{"name": "number", "class": "int"}],
                "runSpec": {"interpreter": "bash",
                            "distribution": "Ubuntu",
                            "release": "16.04",
                            "code": "dx-jobutil-add-output number 32"}
            })
            run("dx run myapplet -inumber=5 --project %s" % temp_project.name)


class TestDXUpdateApp(DXTestCaseBuildApps):
    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that creates apps')
    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_API_APP_UPDATE"])
    def test_update_app(self):
        # Build and publish app with initial version
        app_spec = {
            "name": "test_app_update",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7",
                        "distribution": "Ubuntu", "release": "14.04"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "0.0.1"}
        app_dir = self.write_app_directory("test_app_update", json.dumps(app_spec), "code.py")
        result = run("dx build --app --publish " + app_dir, also_return_stderr=True)
        app_id = json.loads(result[0])['id']
        app = dxpy.describe(app_id)
        self.assertEqual(app['name'], app_spec['name'])
        self.assertEqual(app['version'], "0.0.1")

        # Rebuild and publish app with new version
        app_spec_2 = {
            "name": "test_app_update",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7",
                        "distribution": "Ubuntu", "release": "14.04"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "0.0.2"}
        app_dir_2 = self.write_app_directory("test_app_update_2", json.dumps(app_spec_2), "code.py")
        result_2 = run("dx build --app --publish " + app_dir_2, also_return_stderr=True)
        app_id_2 = json.loads(result_2[0])['id']
        app_2 = dxpy.describe(app_id_2)
        self.assertEqual(app_2['name'], app_spec_2['name'])
        self.assertEqual(app_2['version'], "0.0.2")


if __name__ == '__main__':
    if 'DXTEST_FULL' not in os.environ:
        sys.stderr.write(
            'WARNING: env var DXTEST_FULL is not set; tests that create apps or run jobs will not be run\n')
    unittest.main()
