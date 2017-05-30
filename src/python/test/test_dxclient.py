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

import os, sys, unittest, json, tempfile, subprocess, csv, shutil, re, base64, random, time
import filecmp
import pipes
import stat
import hashlib
import collections
import string
from contextlib import contextmanager
import pexpect
import requests

import dxpy
from dxpy.scripts import dx_build_app
from dxpy_testutil import (DXTestCase, DXTestCaseBuildApps, DXTestCaseBuildWorkflows, check_output, temporary_project,
                           select_project, cd, override_environment, generate_unique_username_email,
                           without_project_context, without_auth, as_second_user, chdir, run, DXCalledProcessError)
import dxpy_testutil as testutil
from dxpy.exceptions import DXAPIError, DXSearchError, EXPECTED_ERR_EXIT_STATUS, HTTPError
from dxpy.compat import str, sys_encoding, open
from dxpy.utils.resolver import ResolutionError, _check_resolution_needed as check_resolution


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


class TestDXTestUtils(DXTestCase):
    def test_temporary_project(self):
        with temporary_project('test_temporary_project', select=True):
            self.assertEquals('test_temporary_project:/', run('dx pwd').strip())

    def test_select_project(self):
        test_dirname = '/test_folder'
        with temporary_project('test_select_project') as temp_project:
            test_projectid = temp_project.get_id()
            run('dx mkdir -p {project}:{dirname}'.format(project=test_projectid, dirname=test_dirname))
            with select_project(test_projectid):
                # This would fail if the project context hadn't been
                # successfully changed by select_project
                run('dx cd {dirname}'.format(dirname=test_dirname))

    @unittest.skipUnless(testutil.TEST_ENV, 'skipping test that would clobber your local environment')
    def test_without_project_context(self):
        self.assertIn('DX_PROJECT_CONTEXT_ID', run('dx env --bash'))
        with without_project_context():
            self.assertNotIn('DX_PROJECT_CONTEXT_ID', run('dx env --bash'))
        self.assertIn('DX_PROJECT_CONTEXT_ID', run('dx env --bash'))

    @unittest.skipUnless(testutil.TEST_ENV, 'skipping test that would clobber your local environment')
    def test_without_auth(self):
        self.assertIn('DX_SECURITY_CONTEXT', run('dx env --bash'))
        with without_auth():
            self.assertNotIn('DX_SECURITY_CONTEXT', run('dx env --bash'))
        self.assertIn('DX_SECURITY_CONTEXT', run('dx env --bash'))

    @unittest.skipUnless(testutil.TEST_MULTIPLE_USERS, 'skipping test that would require multiple users')
    def test_as_second_user(self):
        default_user = run('dx whoami').strip()
        second_user = run('dx whoami', env=as_second_user()).strip()
        expected_user = json.loads(os.environ['DXTEST_SECOND_USER'])['user'].split('-')[1]

        self.assertEqual(expected_user, second_user)
        self.assertNotEqual(default_user, second_user)


class TestDXRemove(DXTestCase):
    def test_remove_objects(self):
        dxpy.new_dxrecord(name="my record")
        dxpy.find_one_data_object(name="my record", project=self.project, zero_ok=False)
        run("dx rm 'my record'")
        self.assertEqual(dxpy.find_one_data_object(name="my record", project=self.project, zero_ok=True), None)

    def test_remove_nonexistent_object(self):
        with self.assertSubprocessFailure(exit_code=3):
            run("dx rm nonexistent")

    def test_remove_folders(self):
        folder_name = "/test_folder"
        record_name = "test_folder"
        record_name2 = "test_folder2"

        # Throw error on non-existent folder
        with self.assertSubprocessFailure(exit_code=3):
            run("dx rm -r {f}".format(f=folder_name))

        # make folder and file of the same name, confirm that file is deleted with regular rm call
        create_folder_in_project(self.project, folder_name)
        self.assertIn(folder_name, list_folder(self.project, "/")['folders'])
        run("dx new record {f}".format(f=record_name))
        self.assertEquals(record_name,
                          dxpy.find_one_data_object(classname="record",
                                                    describe=True,
                                                    project=self.project)['describe']['name'])
        # -r flag shouldn't matter, object will take precedence over folder
        run("dx rm -r {f}".format(f=record_name))
        with self.assertRaises(DXSearchError):
            dxpy.find_one_data_object(classname="record", describe=True, project=self.project)
        # if no -r flag provided, should throw error since it's a folder
        with self.assertSubprocessFailure(exit_code=3):
            run("dx rm {f}".format(f=record_name))
        # finally remove the folder
        run("dx rm -r {f}".format(f=record_name))
        self.assertNotIn(folder_name, list_folder(self.project, "/")['folders'])

        # make a record and then try to delete that record along with a non-existent record
        run("dx new record {f}".format(f=record_name))
        self.assertEquals(record_name,
                          dxpy.find_one_data_object(classname="record",
                                                    describe=True,
                                                    project=self.project)['describe']['name'])
        with self.assertSubprocessFailure(exit_code=3):
            run("dx rm {f} {f2}".format(f=record_name, f2=record_name2))


class TestApiDebugOutput(DXTestCase):
    def test_dx_debug_shows_request_id(self):
        (stdout, stderr) = run("_DX_DEBUG=1 dx ls", also_return_stderr=True)
        self.assertRegexpMatches(stderr, "POST \d{13}-\d{1,6} http",
                                 msg="stderr does not appear to contain request ID")

    def test_dx_debug_shows_timestamp(self):
        timestamp_regex = "\[\d{1,15}\.\d{0,8}\]"

        (stdout, stderr) = run("_DX_DEBUG=1 dx ls", also_return_stderr=True)
        self.assertRegexpMatches(stderr, timestamp_regex, msg="Debug log does not contain a timestamp")
        (stdout, stderr) = run("_DX_DEBUG=2 dx ls", also_return_stderr=True)
        self.assertRegexpMatches(stderr, timestamp_regex, msg="Debug log does not contain a timestamp")

    def test_dx_debug_shows_request_response(self):
        (stdout, stderr) = run("_DX_DEBUG=1 dx new project --brief dx_debug_test", also_return_stderr=True)
        proj_id = stdout.strip()
        self.assertIn("/project/new", stderr)
        self.assertIn("{u'name': u'dx_debug_test'}", stderr)
        self.assertIn(proj_id[-4:], stderr)  # repr can ellipsize the output

        (stdout, stderr) = run("_DX_DEBUG=2 dx new project --brief dx_debug_test", also_return_stderr=True)
        proj_id = stdout.strip()
        self.assertIn("/project/new", stderr)
        self.assertIn('{"name": "dx_debug_test"}', stderr)
        self.assertIn('{"id": "' + proj_id + '"}', stderr)

        (stdout, stderr) = run("_DX_DEBUG=3 dx new project --brief dx_debug_test", also_return_stderr=True)
        proj_id = stdout.strip()
        self.assertIn("/project/new", stderr)
        self.assertIn('{\n  "name": "dx_debug_test"\n}', stderr)
        self.assertIn('{\n    "id": "' + proj_id + '"\n  }', stderr)

    def test_upload_binary_data(self):
        # Really a test that the _DX_DEBUG output doesn't barf on binary data
        with chdir(tempfile.mkdtemp()):
            with open('binary', 'wb') as f:
                f.write(b'\xee\xee\xee\xef')
            (stdout, stderr) = run('_DX_DEBUG=1 dx upload binary', also_return_stderr=True)
            self.assertIn("'\\xee\\xee\\xee\\xef'", stderr)
            (stdout, stderr) = run('_DX_DEBUG=2 dx upload binary', also_return_stderr=True)
            self.assertIn("<file data of length 4>", stderr)
            (stdout, stderr) = run('_DX_DEBUG=3 dx upload binary', also_return_stderr=True)
            self.assertIn("<file data of length 4>", stderr)


class TestDXClient(DXTestCase):
    def test_dx_version(self):
        version = run("dx --version")
        self.assertIn("dx", version)

    def test_dx(self):
        with self.assertRaises(subprocess.CalledProcessError):
            run("dx")
        run("dx help")

    def test_head(self):
        dxpy.upload_string("abcd\n", project=self.project, name="foo", wait_on_close=True)
        self.assertEqual("abcd\n", run("dx head foo"))

    def test_path_resolution_doesnt_crash(self):
        # TODO: add some assertions
        run("dx find jobs --project :")
        run("dx find executions --project :")
        run("dx find analyses --project :")
        run("dx find data --project :")

    def test_windows_pager(self):
        with self.assertRaises(DXCalledProcessError):
            original_path = os.environ['PATH']
            try:
                path_items = original_path.split(";")
                new_path = ""
                # Remove gnu tools from Path
                for i in path_items:
                    if "MinGW" not in i:
                        new_path += i + ";"

                os.environ['PATH'] = new_path
                check_output("dx")
            except DXCalledProcessError as e:
                self.assertNotIn("'less' is not recognized", e.output)
                raise e
            finally:
                os.environ['PATH'] = original_path

    def test_get_unicode_url(self):
        with self.assertSubprocessFailure(stderr_regexp="ResourceNotFound", exit_code=3):
            run("dx api project-эксперимент describe")

    def test_dx_env(self):
        run("dx env")
        run("dx env --bash")
        run("dx env --dx-flags")

    def test_dx_api(self):
        with testutil.TemporaryFile() as fd:
            fd.write("{}")
            fd.flush()
            fd.close()
            run("dx api {p} describe --input {fn}".format(p=self.project, fn=fd.name))

    @unittest.skipUnless(testutil.TEST_NO_RATE_LIMITS,
                         'skipping tests that need rate limits to be disabled')
    def test_dx_invite(self):
        for query in ("Ψ", "alice.nonexistent", "alice.nonexistent {p}", "user-alice.nonexistent {p}",
                      "alice.nonexistent@example.com {p}", "alice.nonexistent : VIEW"):
            with self.assertSubprocessFailure(stderr_regexp="ResourceNotFound", exit_code=3):
                run(("dx invite "+query).format(p=self.project))
        with self.assertSubprocessFailure(stderr_regexp="invalid choice", exit_code=2):
            run(("dx invite alice.nonexistent : ПРОСМОТР").format(p=self.project))

    @unittest.skipUnless(testutil.TEST_NO_RATE_LIMITS,
                         'skipping tests that need rate limits to be disabled')
    def test_dx_uninvite(self):
        for query in ("Ψ", "alice.nonexistent", "alice.nonexistent {p}", "user-alice.nonexistent {p}",
                      "alice.nonexistent@example.com {p}"):
            with self.assertSubprocessFailure(stderr_regexp="ResourceNotFound", exit_code=3):
                run(("dx uninvite "+query).format(p=self.project))

    def test_dx_add_rm_types(self):
        run("dx new record Ψ")
        run("dx add_types Ψ abc xyz")
        with self.assertSubprocessFailure(stderr_text="be an array of valid strings for a type name",
                                          exit_code=3):
            run("dx add_types Ψ ΨΨ")
        run("dx remove_types Ψ abc xyz")
        run("dx remove_types Ψ abc xyz")
        with self.assertSubprocessFailure(stderr_regexp="Unable to resolve", exit_code=3):
            run("dx remove_types ΨΨ Ψ")

    def test_dx_set_details(self):
        record_id = run("dx new record Ψ1 --brief").strip()
        run("dx set_details Ψ1 '{\"foo\": \"bar\"}'")
        dxrecord = dxpy.DXRecord(record_id)
        details = dxrecord.get_details()
        self.assertEqual({"foo": "bar"}, details, msg="dx set_details with valid JSON string input failed.")

    def test_dx_set_details_with_file(self):
        # Create temporary JSON file with valid JSON.
        with tempfile.NamedTemporaryFile() as tmp_file, tempfile.NamedTemporaryFile() as tmp_invalid_file:
            tmp_file.write('{\"foo\": \"bar\"}')
            tmp_file.flush()

            # Test -f with valid JSON file.
            record_id = run("dx new record Ψ2 --brief").strip()
            run("dx set_details Ψ2 -f " + pipes.quote(tmp_file.name))
            dxrecord = dxpy.DXRecord(record_id)
            details = dxrecord.get_details()
            self.assertEqual({"foo": "bar"}, details, msg="dx set_details -f with valid JSON input file failed.")

            # Test --details-file with valid JSON file.
            record_id = run("dx new record Ψ3 --brief").strip()
            run("dx set_details Ψ3 --details-file " + pipes.quote(tmp_file.name))
            dxrecord = dxpy.DXRecord(record_id)
            details = dxrecord.get_details()
            self.assertEqual({"foo": "bar"}, details,
                             msg="dx set_details --details-file with valid JSON input file failed.")

            # Create temporary JSON file with invalid JSON.
            tmp_invalid_file.write('{\"foo\": \"bar\"')
            tmp_invalid_file.flush()

            # Test above with invalid JSON file.
            record_id = run("dx new record Ψ4 --brief").strip()
            with self.assertSubprocessFailure(stderr_regexp="JSON", exit_code=3):
                run("dx set_details Ψ4 -f " + pipes.quote(tmp_invalid_file.name))

            # Test command with (-f or --details-file) and CL JSON.
            with self.assertSubprocessFailure(stderr_regexp="Error: Cannot provide both -f/--details-file and details",
                                              exit_code=3):
                run("dx set_details Ψ4 '{ \"foo\":\"bar\" }' -f " + pipes.quote(tmp_file.name))

            # Test piping JSON from STDIN.
            record_id = run("dx new record Ψ5 --brief").strip()
            run("cat " + pipes.quote(tmp_file.name) + " | dx set_details Ψ5 -f -")
            dxrecord = dxpy.DXRecord(record_id)
            details = dxrecord.get_details()
            self.assertEqual({"foo": "bar"}, details, msg="dx set_details -f - with valid JSON input failed.")

    def test_dx_shell(self):
        shell = pexpect.spawn("bash")
        shell.logfile = sys.stdout
        shell.sendline("dx sh")
        shell.expect(">")
        shell.sendline("Ψ 'Ψ Ψ'")
        shell.expect("invalid choice: Ψ".encode(sys_encoding))
        shell.expect(">")
        shell.sendline("env")
        shell.expect("Current user")
        shell.sendline("help all")
        shell.expect("Commands:")
        shell.sendline("exit")
        shell.sendline("echo find projects | dx sh")
        shell.expect("project-")

    def test_dx_get_record(self):
        with chdir(tempfile.mkdtemp()):
            run("dx new record -o :foo --verbose")
            run("dx get :foo")
            self.assertTrue(os.path.exists('foo.json'))
            run("dx get --no-ext :foo")
            self.assertTrue(os.path.exists('foo'))
            run("diff -q foo foo.json")

    def test_dx_object_tagging(self):
        the_tags = ["Σ1=n", "helloo0", "ωω"]
        # tag
        record_id = run("dx new record Ψ --brief").strip()
        run("dx tag Ψ " + " ".join(the_tags))
        mytags = dxpy.describe(record_id)['tags']
        for tag in the_tags:
            self.assertIn(tag, mytags)
        # untag
        run("dx untag Ψ " + " ".join(the_tags[:2]))
        mytags = dxpy.describe(record_id)['tags']
        for tag in the_tags[:2]:
            self.assertNotIn(tag, mytags)
        self.assertIn(the_tags[2], mytags)

        # -a flag
        second_record_id = run("dx new record Ψ --brief").strip()
        self.assertNotEqual(record_id, second_record_id)
        run("dx tag -a Ψ " + " ".join(the_tags))
        mytags = dxpy.describe(record_id)['tags']
        for tag in the_tags:
            self.assertIn(tag, mytags)
        second_tags = dxpy.describe(second_record_id)['tags']
        for tag in the_tags:
            self.assertIn(tag, second_tags)

        run("dx untag -a Ψ " + " ".join(the_tags))
        mytags = dxpy.describe(record_id)['tags']
        self.assertEqual(len(mytags), 0)
        second_tags = dxpy.describe(second_record_id)['tags']
        self.assertEqual(len(second_tags), 0)

        # nonexistent name
        with self.assertSubprocessFailure(stderr_regexp='Unable to resolve', exit_code=3):
            run("dx tag nonexistent atag")
        with self.assertSubprocessFailure(stderr_regexp='Unable to resolve', exit_code=3):
            run("dx untag nonexistent atag")

    def test_dx_project_tagging(self):
        the_tags = ["$my.tag", "secoиdtag", "тhird тagggg"]
        # tag
        run("dx tag : \\" + the_tags[0] + " " + the_tags[1] + " '" + the_tags[2] + "'")
        mytags = dxpy.describe(self.project)['tags']
        for tag in the_tags:
            self.assertIn(tag, mytags)
        # untag
        run("dx untag : \\" + the_tags[0] + " '" + the_tags[2] + "'")
        mytags = dxpy.describe(self.project)['tags']
        self.assertIn(the_tags[1], mytags)
        for tag in [the_tags[0], the_tags[2]]:
            self.assertNotIn(tag, mytags)

        # nonexistent name
        with self.assertSubprocessFailure(stderr_regexp='Could not find a project named', exit_code=3):
            run("dx tag nonexistent: atag")
        with self.assertSubprocessFailure(stderr_regexp='Could not find a project named', exit_code=3):
            run("dx untag nonexistent: atag")

    def test_dx_object_properties(self):
        property_names = ["Σ_1^n", "helloo0", "ωω"]
        property_values = ["n", "world z", "ω()"]
        # set_properties
        record_id = run("dx new record Ψ --brief").strip()
        run("dx set_properties Ψ " +
            " ".join(["'" + prop[0] + "'='" + prop[1] + "'" for prop in zip(property_names,
                                                                            property_values)]))
        my_properties = dxpy.api.record_describe(record_id, {"properties": True})['properties']
        for (name, value) in zip(property_names, property_values):
            self.assertIn(name, my_properties)
            self.assertEqual(value, my_properties[name])
        # unset_properties
        run("dx unset_properties Ψ '" + "' '".join(property_names[:2]) + "'")
        my_properties = dxpy.api.record_describe(record_id, {"properties": True})['properties']
        for name in property_names[:2]:
            self.assertNotIn(name, my_properties)
        self.assertIn(property_names[2], my_properties)
        self.assertEqual(property_values[2], my_properties[property_names[2]])

        # -a flag
        second_record_id = run("dx new record Ψ --brief").strip()
        self.assertNotEqual(record_id, second_record_id)
        run("dx set_properties -a Ψ " +
            " ".join(["'" + prop[0] + "'='" + prop[1] + "'" for prop in zip(property_names,
                                                                            property_values)]))
        my_properties = dxpy.api.record_describe(record_id, {"properties": True})['properties']
        for (name, value) in zip(property_names, property_values):
            self.assertIn(name, my_properties)
            self.assertEqual(value, my_properties[name])
        second_properties = dxpy.api.record_describe(second_record_id,
                                                     {"properties": True})['properties']
        for (name, value) in zip(property_names, property_values):
            self.assertIn(name, my_properties)
            self.assertEqual(value, my_properties[name])

        run("dx unset_properties -a Ψ '" + "' '".join(property_names) + "'")
        my_properties = dxpy.api.record_describe(record_id, {"properties": True})['properties']
        self.assertEqual(len(my_properties), 0)
        second_properties = dxpy.api.record_describe(second_record_id,
                                                     {"properties": True})['properties']
        self.assertEqual(len(second_properties), 0)

        # nonexistent name
        with self.assertSubprocessFailure(stderr_regexp='Unable to resolve', exit_code=3):
            run("dx set_properties nonexistent key=value")
        with self.assertSubprocessFailure(stderr_regexp='Unable to resolve', exit_code=3):
            run("dx unset_properties nonexistent key")

        # Errors parsing --property value
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties -a Ψ ''")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties -a Ψ foo=bar=baz")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties -a Ψ =foo=bar=")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties -a Ψ foo")
        # Property keys must be nonempty
        with self.assertSubprocessFailure(stderr_regexp='nonempty strings', exit_code=3):
            run("dx set_properties -a Ψ =bar")
        # Empty string values should be okay
        run("dx set_properties -a Ψ bar=")

        my_properties = dxpy.api.record_describe(record_id, {"properties": True})['properties']
        self.assertEqual(my_properties["bar"], "")

    def test_dx_project_properties(self):
        property_names = ["$my.prop", "secoиdprop", "тhird prop"]
        property_values = ["$hello.world", "Σ2,n", "stuff"]
        # set_properties
        run("dx set_properties : " +
            " ".join(["'" + prop[0] + "'='" + prop[1] + "'" for prop in zip(property_names,
                                                                            property_values)]))
        my_properties = dxpy.api.project_describe(self.project, {"properties": True})['properties']
        for (name, value) in zip(property_names, property_values):
            self.assertIn(name, my_properties)
            self.assertEqual(value, my_properties[name])
        # unset_properties
        run("dx unset_properties : '" + property_names[0] + "' '" + property_names[2] + "'")
        my_properties = dxpy.api.project_describe(self.project, {"properties": True})['properties']
        self.assertIn(property_names[1], my_properties)
        self.assertEqual(property_values[1], my_properties[property_names[1]])
        for name in [property_names[0], property_names[2]]:
            self.assertNotIn(name, my_properties)

        # nonexistent name
        with self.assertSubprocessFailure(stderr_regexp='Could not find a project named', exit_code=3):
            run("dx set_properties nonexistent: key=value")
        with self.assertSubprocessFailure(stderr_regexp='Could not find a project named', exit_code=3):
            run("dx unset_properties nonexistent: key")

        # Errors parsing --property value
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties : ''")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties : foo=bar=baz")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties : =foo=bar=")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties : foo")
        # Property keys must be nonempty
        with self.assertSubprocessFailure(stderr_regexp='nonempty strings', exit_code=3):
            run("dx set_properties : =bar")
        # Empty string values should be okay
        run("dx set_properties : bar=")

        my_properties = dxpy.api.project_describe(self.project, {"properties": True})['properties']
        self.assertEqual(my_properties["bar"], "")

    def test_dx_describe_project(self):
        # Look for field name, some number of spaces, and then the value
        field_regexp = lambda fieldname, value: \
            "(^|\n)" + re.escape(fieldname) + " +" + re.escape(value) + "(\n|$)"

        desc_output = run("dx describe :").strip()
        self.assertRegex(desc_output, field_regexp("ID", self.project))
        self.assertRegex(desc_output, field_regexp("Name", "dxclient_test_pröject"))
        self.assertRegex(desc_output, field_regexp("Region", "aws:us-east-1"))
        self.assertRegex(desc_output, field_regexp("Contains PHI", "false"))
        self.assertNotRegex(desc_output, field_regexp("Archival state", "null"))
        self.assertNotRegex(desc_output, field_regexp("Archival progress", "null"))
        self.assertRegex(desc_output, field_regexp("Data usage", "0.00 GB"))
        self.assertRegex(desc_output,
                         field_regexp("Storage cost", "$0.000/month"),
                         "No storage cost shown, does this account have billing information supplied?")
        self.assertRegex(desc_output, field_regexp("Sponsored egress", "0.00 GB used of 0.00 GB total"))
        self.assertRegex(desc_output, field_regexp("At spending limit?", "false"))
        self.assertRegex(desc_output, field_regexp("Properties", "-"))

        desc_output = run("dx describe --verbose :").strip()
        self.assertRegex(desc_output, field_regexp("Archival state", "live"))
        self.assertRegex(desc_output, field_regexp("Archival progress", "null"))

    def test_dx_remove_project_by_name(self):
        # TODO: this test makes no use of the DXTestCase-provided
        # project.
        project_name = ("test_dx_remove_project_by_name_" + str(random.randint(0, 1000000)) + "_" +
                        str(int(time.time() * 1000)))
        project_id = run("dx new project {name} --brief".format(name=project_name)).strip()
        self.assertEqual(run("dx find projects --brief --name {name}".format(name=project_name)).strip(),
                         project_id)
        run("dx rmproject -y {name}".format(name=project_name))
        self.assertEqual(run("dx find projects --brief --name {name}".format(name=project_name)), "")

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that requires presence of test user')
    def test_dx_project_invite_without_email(self):
        user_id = 'user-bob'
        with temporary_project() as unique_project:
            project_id = unique_project.get_id()

            # Check that user is not already invited to project
            project_members = dxpy.api.project_describe(project_id, {'fields': {'permissions': True}})['permissions']
            self.assertNotIn(user_id, project_members.keys())

            # Test --no-email flag
            res = run("dx invite {user} {project} VIEW --no-email".format(user=user_id, project=project_id)).strip()
            exp = "Invited {user} to {project} (accepted)".format(user=user_id, project=project_id)
            self.assertEqual(res, exp)

            # Confirm user in project
            conf = dxpy.api.project_describe(project_id, {'fields': {'permissions': True}})['permissions']
            self.assertEqual(conf[user_id], 'VIEW')

    def test_dx_cp(self):
        project_name = "test_dx_cp_" + str(random.randint(0, 1000000)) + "_" + str(int(time.time() * 1000))
        dest_project_id = run("dx new project {name} --brief".format(name=project_name)).strip()
        try:
            record_id = run("dx new record --brief --details '{\"hello\": 1}'").strip()
            run("dx close --wait {r}".format(r=record_id))
            self.assertEqual(run("dx ls --brief {p}".format(p=dest_project_id)), "")
            run("dx cp {r} {p}".format(r=record_id, p=dest_project_id))
            self.assertEqual(run("dx ls --brief {p}".format(p=dest_project_id)).strip(), record_id)
        finally:
            run("dx rmproject -y {p}".format(p=dest_project_id))

    def test_dx_mkdir(self):
        with self.assertRaises(subprocess.CalledProcessError):
            run("dx mkdir mkdirtest/b/c")
        run("dx mkdir -p mkdirtest/b/c")
        run("dx mkdir -p mkdirtest/b/c")
        run("dx rm -r mkdirtest")

    @unittest.skip('PTFM-16383 Disable flaky test')
    def test_dxpy_session_isolation(self):
        for var in 'DX_PROJECT_CONTEXT_ID', 'DX_PROJECT_CONTEXT_NAME', 'DX_CLI_WD':
            if var in os.environ:
                del os.environ[var]
        shell1 = pexpect.spawn("bash")
        shell2 = pexpect.spawn("bash")
        shell1.logfile = shell2.logfile = sys.stdout
        shell1.setwinsize(20, 90)
        shell2.setwinsize(20, 90)

        def expect_dx_env_cwd(shell, wd):
            shell.expect(self.project)
            shell.expect(wd)
            shell.expect([">", "#", "$"]) # prompt

        shell1.sendline("dx select "+self.project)
        shell1.sendline("dx mkdir /sessiontest1")
        shell1.sendline("dx cd /sessiontest1")
        shell1.sendline("dx env")
        expect_dx_env_cwd(shell1, "sessiontest1")

        shell2.sendline("dx select "+self.project)
        shell2.sendline("dx mkdir /sessiontest2")
        shell2.sendline("dx cd /sessiontest2")
        shell2.sendline("dx env")
        expect_dx_env_cwd(shell2, "sessiontest2")
        shell2.sendline("bash -c 'dx env'")
        expect_dx_env_cwd(shell2, "sessiontest2")

        shell1.sendline("dx env")
        expect_dx_env_cwd(shell1, "sessiontest1")
        # Grandchild subprocess inherits session
        try:
            shell1.sendline("bash -c 'dx env'")
            expect_dx_env_cwd(shell1, "sessiontest1")
        except:
            print("*** TODO: FIXME: Unable to verify that grandchild subprocess inherited session")

    def test_dx_ssh_config_revoke(self):
        original_ssh_public_key = None

        user_id = dxpy.whoami()
        original_ssh_public_key = dxpy.api.user_describe(user_id).get("sshPublicKey")
        wd = tempfile.mkdtemp()
        os.mkdir(os.path.join(wd, ".dnanexus_config"))

        def revoke_ssh_public_key(args=["ssh_config", "--revoke"]):
            dx_ssh_config_revoke = pexpect.spawn("dx", args=args)
            dx_ssh_config_revoke.expect("revoked")

        def set_ssh_public_key():
            dx_ssh_config = pexpect.spawn("dx ssh_config", env=override_environment(HOME=wd))
            dx_ssh_config.logfile = sys.stdout
            dx_ssh_config.expect("Select an SSH key pair")
            dx_ssh_config.sendline("0")
            dx_ssh_config.expect("Enter passphrase")
            dx_ssh_config.sendline()
            dx_ssh_config.expect("again")
            dx_ssh_config.sendline()
            dx_ssh_config.expect("Your account has been configured for use with SSH")

        def assert_same_ssh_pub_key():
            self.assertTrue(os.path.exists(os.path.join(wd, ".dnanexus_config/ssh_id")))

            with open(os.path.join(wd, ".dnanexus_config/ssh_id.pub")) as fh:
                self.assertEquals(fh.read(), dxpy.api.user_describe(user_id).get('sshPublicKey'))

        try:
            # public key exists
            set_ssh_public_key()
            assert_same_ssh_pub_key()
            revoke_ssh_public_key()
            self.assertNotIn("sshPublicKey", dxpy.api.user_describe(user_id))

            # public key does not exist
            revoke_ssh_public_key()
            self.assertNotIn("sshPublicKey", dxpy.api.user_describe(user_id))

            # random input after '--revoke'
            revoke_ssh_public_key(args=["ssh_config", '--revoke', 'asdf'])
            self.assertNotIn("sshPublicKey", dxpy.api.user_describe(user_id))

        finally:
            if original_ssh_public_key:
                dxpy.api.user_update(user_id, {"sshPublicKey": original_ssh_public_key})

    def test_dx_ssh_config(self):
        original_ssh_public_key = None
        try:
            user_id = dxpy.whoami()
            original_ssh_public_key = dxpy.api.user_describe(user_id).get('sshPublicKey')
            wd = tempfile.mkdtemp()

            def get_dx_ssh_config():
                dx_ssh_config = pexpect.spawn("dx ssh_config", env=override_environment(HOME=wd))
                dx_ssh_config.logfile = sys.stdout
                dx_ssh_config.setwinsize(20, 90)
                return dx_ssh_config

            def read_back_pub_key():
                self.assertTrue(os.path.exists(os.path.join(wd, ".dnanexus_config/ssh_id")))

                with open(os.path.join(wd, ".dnanexus_config/ssh_id.pub")) as fh:
                    self.assertEqual(fh.read(), dxpy.api.user_describe(user_id).get('sshPublicKey'))

            dx_ssh_config = get_dx_ssh_config()
            dx_ssh_config.expect("The DNAnexus configuration directory")
            dx_ssh_config.expect("does not exist")

            os.mkdir(os.path.join(wd, ".dnanexus_config"))

            dx_ssh_config = get_dx_ssh_config()
            dx_ssh_config.expect("Select an SSH key pair")
            dx_ssh_config.sendline("1")
            dx_ssh_config.expect("Enter the location of your SSH key")
            dx_ssh_config.sendline("нет ключа")
            dx_ssh_config.expect("Unable to find")

            dx_ssh_config = get_dx_ssh_config()
            dx_ssh_config.expect("Select an SSH key pair")
            dx_ssh_config.sendline("0")
            dx_ssh_config.expect("Enter passphrase")
            dx_ssh_config.sendline()
            dx_ssh_config.expect("again")
            dx_ssh_config.sendline()
            dx_ssh_config.expect("Your account has been configured for use with SSH")
            read_back_pub_key()

            dx_ssh_config = get_dx_ssh_config()
            dx_ssh_config.expect("Select an SSH key pair")
            dx_ssh_config.expect("already configured")
            dx_ssh_config.sendline("0")
            dx_ssh_config.expect("Your account has been configured for use with SSH")
            read_back_pub_key()

            dx_ssh_config = get_dx_ssh_config()
            dx_ssh_config.expect("Select an SSH key pair")
            dx_ssh_config.expect("already configured")
            dx_ssh_config.sendline("1")
            dx_ssh_config.expect("Generate a new SSH key pair")
            dx_ssh_config.sendline("0")
            dx_ssh_config.expect("Enter passphrase")
            dx_ssh_config.sendline()
            dx_ssh_config.expect("again")
            dx_ssh_config.sendline()
            dx_ssh_config.expect("Your account has been configured for use with SSH")
            read_back_pub_key()

            # Ensure that private key upload is rejected
            with open(os.path.join(wd, ".dnanexus_config", "ssh_id")) as private_key:
                with self.assertRaisesRegexp(DXAPIError,
                                             'Tried to put a private key in the sshPublicKey field'):
                    dxpy.api.user_update(user_id, {"sshPublicKey": private_key.read()})
        finally:
            if original_ssh_public_key:
                dxpy.api.user_update(user_id, {"sshPublicKey": original_ssh_public_key})

    @contextmanager
    def configure_ssh(self, use_alternate_config_dir=False):
        original_ssh_public_key = None
        try:
            config_subdir = "dnanexus_config_alternate" if use_alternate_config_dir else ".dnanexus_config"
            user_id = dxpy.whoami()
            original_ssh_public_key = dxpy.api.user_describe(user_id).get('sshPublicKey')
            wd = tempfile.mkdtemp()
            config_dir = os.path.join(wd, config_subdir)
            os.mkdir(config_dir)
            if use_alternate_config_dir:
                os.environ["DX_USER_CONF_DIR"] = config_dir

            dx_ssh_config = pexpect.spawn("dx ssh_config", env=override_environment(HOME=wd))
            dx_ssh_config.logfile = sys.stdout
            dx_ssh_config.setwinsize(20, 90)
            dx_ssh_config.expect("Select an SSH key pair")
            dx_ssh_config.sendline("0")
            dx_ssh_config.expect("Enter passphrase")
            dx_ssh_config.sendline()
            dx_ssh_config.expect("again")
            dx_ssh_config.sendline()
            dx_ssh_config.expect("Your account has been configured for use with SSH")
            yield wd
        finally:
            if original_ssh_public_key:
                dxpy.api.user_update(user_id, {"sshPublicKey": original_ssh_public_key})

    def _test_dx_ssh(self, project, instance_type):
        dxpy.config["DX_PROJECT_CONTEXT_ID"] = project
        for use_alternate_config_dir in [False, True]:
            with self.configure_ssh(use_alternate_config_dir=use_alternate_config_dir) as wd:
                sleep_applet = dxpy.api.applet_new(dict(name="sleep",
                                                        runSpec={"code": "sleep 1200",
                                                                 "interpreter": "bash",
                                                                 "execDepends": [{"name": "dx-toolkit"}],
                                                                 "systemRequirements": {"*": {"instanceType": instance_type}}},
                                                        inputSpec=[], outputSpec=[],
                                                        dxapi="1.0.0", version="1.0.0",
                                                        project=project))["id"]

                dx = pexpect.spawn("dx run {} --yes --ssh".format(sleep_applet),
                                   env=override_environment(HOME=wd))
                dx.logfile = sys.stdout
                dx.setwinsize(20, 90)
                dx.expect("Waiting for job")
                dx.expect("Resolving job hostname and SSH host key", timeout=1200)

                # Wait for the line displayed between the first and second MOTDs,
                # since we only care about checking the second set of MOTD lines.
                # Example of the dividing line:
                # dnanexus@job-BP90K3Q0X2v81PXXPZj005Zj.dnanex.us (10.0.0.200) - byobu
                dx.expect(["dnanexus.io \(10.0.0.200\) - byobu",
                           "dnanex.us \(10.0.0.200\) - byobu"], timeout=120)
                dx.expect("This is the DNAnexus Execution Environment", timeout=600)
                # Check for job name (e.g. "Job: sleep")
                dx.expect("Job: \x1b\[1msleep", timeout=5)
                # \xf6 is ö
                dx.expect("Project: dxclient_test_pr\xf6ject".encode(sys_encoding))
                dx.expect("The job is running in terminal 1.", timeout=5)
                # Check for terminal prompt and verify we're in the container
                job_id = dxpy.find_jobs(name="sleep", project=project).next()['id']
                dx.expect(("dnanexus@%s" % job_id), timeout=10)

                expected_history_filename = os.path.join(
                        os.environ.get("DX_USER_CONF_DIR", os.path.join(wd, ".dnanexus_config")), ".dx_history")
                self.assertTrue(os.path.isfile(expected_history_filename))

                # Make sure the job can be connected to using 'dx ssh <job id>'
                dx2 = pexpect.spawn("dx ssh " + job_id, env=override_environment(HOME=wd))
                dx2.logfile = sys.stdout
                dx2.setwinsize(20, 90)
                dx2.expect("Waiting for job")
                dx2.expect("Resolving job hostname and SSH host key", timeout=1200)
                dx2.expect(("dnanexus@%s" % job_id), timeout=10)
                dx2.sendline("whoami")
                dx2.expect("dnanexus", timeout=10)
                # Exit SSH session and terminate job
                dx2.sendline("exit")
                dx2.expect("bash running")
                dx2.sendcontrol("c") # CTRL-c
                dx2.expect("[exited]")
                dx2.expect("dnanexus@job", timeout=10)
                dx2.sendline("exit")
                dx2.expect("still running. Terminate now?")
                dx2.sendline("y")
                dx2.expect("Terminated job", timeout=60)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, "Skipping test that would run jobs")
    def test_dx_ssh(self):
        self._test_dx_ssh(self.project, "mem2_hdd2_x1")

    @unittest.skipUnless(testutil.TEST_RUN_JOBS and testutil.TEST_AZURE, "Skipping test that would run jobs in Azure")
    def test_dx_ssh_azure(self):
        azure_project = dxpy.api.project_new({"name": "test_dx_ssh_azure", "region": testutil.TEST_AZURE})['id']
        try:
            self._test_dx_ssh(azure_project, "azure:mem2_ssd1_x1")
        finally:
            dxpy.api.project_destroy(azure_project, {"terminateJobs": True})

    def _test_dx_ssh_proxy(self, project, instance_type):
        proxy_host = "localhost"
        proxy_port = "3129"
        proxy_addr = "http://{h}:{p}".format(h=proxy_host, p=proxy_port)

        port_check = run("netstat -plant 2>/dev/null|grep 3129||echo available")[:-1]
        if port_check != 'available':
            raise Exception("Cannot launch squid, because port 3129 is already bound")

        def launch_squid():
            squid_wd = os.path.join(os.path.dirname(__file__), 'http_proxy')
            self.proxy_process = subprocess.Popen(['squid3', '-N', '-f', 'squid_noauth.conf'],
                                                  cwd=squid_wd)
            print("Waiting for squid to come up...")
            t = 0
            while True:
                try:
                    if requests.get(proxy_addr).status_code == requests.codes.bad_request:
                        print("squid is up")
                        break
                except requests.exceptions.RequestException:
                    pass
                time.sleep(0.5)
                t += 1
                if t > 16:
                    raise Exception("Failed to launch Squid")

        dxpy.config["DX_PROJECT_CONTEXT_ID"] = project
        with self.configure_ssh() as wd:
            launch_squid()
            applet_json = dict(name="sleep",
                               runSpec={"code": "sleep 6000",
                                        "interpreter": "bash",
                                        "execDepends": [{"name": "dx-toolkit"}],
                                        "systemRequirements": {"*": {"instanceType": instance_type}}},
                               inputSpec=[], outputSpec=[],
                               dxapi="1.0.0", version="1.0.0",
                               project=project)
            sleep_applet = dxpy.api.applet_new(applet_json)["id"]

            # Test incorrect arguments i.e. --ssh is missing
            with self.assertSubprocessFailure(stderr_regexp="DXCLIError", exit_code=3):
                run("dx run {a} --yes --ssh-proxy {h}:{p} --debug-on All".format(a=sleep_applet,
                                                                                 h=proxy_host,
                                                                                 p=proxy_port),
                    env=override_environment(HOME=wd))

            # Create job using the proxy
            dx = pexpect.spawn("dx run {a} --yes --ssh --ssh-proxy {h}:{p} --debug-on All".
                               format(a=sleep_applet,
                                      h=proxy_host,
                                      p=proxy_port),
                               env=override_environment(HOME=wd))
            dx.logfile = sys.stdout
            dx.setwinsize(20, 90)
            dx.expect("The job is running in terminal 1.", timeout=1200)
            # Check for terminal prompt and verify we're in the container
            job_id = dxpy.find_jobs(name="sleep", project=project).next()['id']
            job_ssh_port = dxpy.DXJob(job_id).describe().get('sshPort', 22)
            dx.expect(("dnanexus@%s" % job_id), timeout=10)
            # Cache default ssh command for refactoring
            ssh_proxy_command = "dx ssh --ssh-proxy {h}:{p} {id}".format(h=proxy_host,
                                                                         p=proxy_port,
                                                                         id=job_id)
            # Make sure the job can be connected to using 'dx ssh <job id>'
            dx2 = pexpect.spawn(ssh_proxy_command, env=override_environment(HOME=wd))
            dx2.expect(("dnanexus@%s" % job_id), timeout=10)
            dx2.sendline("exit")
            dx2.expect("bash running", timeout=10)
            dx2.sendcontrol("c")  # CTRL-c
            dx2.expect("[exited]")
            dx2.expect("dnanexus@job", timeout=10)
            # Test proxy connection from worker side
            squid_address = run("netstat -plant 2>/dev/null|grep squid3|grep :{p}|awk '{{print $4}}'".format(p=job_ssh_port))
            squid_port = squid_address.split(':')[1][:-1]
            dx2.sendline("netstat -plant 2>/dev/null|grep :{p}|awk '{{print $6}}'".format(p=squid_port))
            dx2.expect("ESTABLISHED", timeout=60)
            # Make sure ssh-proxy fails without proxy running
            self.proxy_process.kill()
            with self.assertSubprocessFailure(stderr_regexp="DXCLIError", exit_code=3):
                run(ssh_proxy_command, env=override_environment(HOME=wd))
            # Test invalid proxy address
            with self.assertSubprocessFailure(stderr_regexp="DXCLIError", exit_code=3):
                run("dx ssh --ssh-proxy {h}:{p} {id}".format(h='999.999.9.9',
                                                             p='9999',
                                                             id=job_id),
                    env=override_environment(HOME=wd))

            run("dx terminate " + job_id, env=override_environment(HOME=wd))
            # Verify job termination
            running = 'terminating'
            t = 0
            while running == 'terminating':
                running = run("dx find jobs --id {id}|grep 'terminated'||echo 'terminating'"
                              .format(id=job_id))[:-1]
                time.sleep(1)
                t += 1
                if t > 16:
                    raise Exception("Failed to terminate job")

            launch_squid()
            # Test sshing into a terminated job while proxy is running
            with self.assertSubprocessFailure(stderr_regexp=("%s is in a terminal state" % job_id), exit_code=1):
                run(ssh_proxy_command, env=override_environment(HOME=wd))

            # Test sshing into a non existentant job through proxy
            with self.assertSubprocessFailure(stderr_regexp="ResourceNotFound", exit_code=3):
                bad_id = 'job-000000000000000000000000'
                run("dx ssh --ssh-proxy {h}:{p} {id}".format(h=proxy_host,
                                                             p=proxy_port,
                                                             id=bad_id),
                    env=override_environment(HOME=wd))
            self.proxy_process.kill()

    @unittest.skipIf(sys.platform.startswith("win"), "pexpect is not supported")
    @unittest.skipUnless(testutil.TEST_RUN_JOBS, "Skipping test that would run jobs")
    @unittest.skipUnless(testutil.TEST_HTTP_PROXY,
                         'skipping HTTP Proxy support test that needs squid3')
    def test_dx_ssh_proxy(self):
        self._test_dx_ssh_proxy(self.project, "mem2_hdd2_x1")

    @unittest.skipIf(sys.platform.startswith("win"), "pexpect is not supported")
    @unittest.skipUnless(testutil.TEST_RUN_JOBS, "Skipping test that would run jobs")
    @unittest.skipUnless(testutil.TEST_HTTP_PROXY,
                         'skipping HTTP Proxy support test that needs squid3')
    @unittest.skipUnless(testutil.TEST_AZURE, 'skipping test that runs on Azure')
    def test_dx_ssh_proxy_azure(self):
        azure_project = dxpy.api.project_new({"name": "test_dx_ssh_azure", "region": testutil.TEST_AZURE})['id']
        try:
            self._test_dx_ssh_proxy(azure_project, "azure:mem2_ssd1_x1")
        finally:
            dxpy.api.project_destroy(azure_project, {"terminateJobs": True})

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, "Skipping test that would run jobs")
    def test_dx_run_debug_on(self):
        with self.configure_ssh() as wd:
            crash_applet = dxpy.api.applet_new(dict(name="crash",
                                                    runSpec={"code": "exit 5", "interpreter": "bash",
                                                             "execDepends": [{"name": "dx-toolkit"}]},
                                                    inputSpec=[], outputSpec=[],
                                                    dxapi="1.0.0", version="1.0.0",
                                                    project=self.project))["id"]

            job_id = run("dx run {} --yes --brief --debug-on AppInternalError".format(crash_applet),
                         env=override_environment(HOME=wd)).strip()
            elapsed = 0
            while True:
                job_desc = dxpy.describe(job_id)
                if job_desc["state"] == "debug_hold":
                    break
                time.sleep(1)
                elapsed += 1
                if elapsed > 1200:
                    raise Exception("Timeout while waiting for job to enter debug hold")

            dx = pexpect.spawn("dx ssh " + job_id, env=override_environment(HOME=wd))
            dx.logfile = sys.stdout
            dx.setwinsize(20, 90)
            dx.expect("dnanexus@", timeout=1200)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, "Skipping test that would run jobs")
    def test_dx_run_debug_on_all(self):
        with self.configure_ssh() as wd:
            crash_applet = dxpy.api.applet_new(dict(name="crash",
                                                runSpec={"code": "exit 5", "interpreter": "bash",
                                                         "execDepends": [{"name": "dx-toolkit"}]},
                                                inputSpec=[], outputSpec=[],
                                                dxapi="1.0.0", version="1.0.0",
                                                project=self.project))["id"]

            job_id = run("dx run {} --yes --brief --debug-on All".format(crash_applet),
                         env=override_environment(HOME=wd)).strip()
            job_desc = dxpy.describe(job_id)
            self.assertEqual(job_desc["debug"]['debugOn'], ['AppError', 'AppInternalError', 'ExecutionError'])


    @unittest.skipUnless(testutil.TEST_DX_LOGIN,
                         'This test requires authserver to run, requires dx login to select the right authserver, ' +
                         'and may result in temporary account lockout. TODO: update test instrumentation to allow ' +
                         'it to run')
    def test_dx_login(self):
        wd = tempfile.mkdtemp()
        username = dxpy.user_info()['username']

        def get_dx_login(opts=""):
            dx_login = pexpect.spawn("dx login" + opts, env=override_environment(HOME=wd))
            dx_login.logfile = sys.stdout
            dx_login.setwinsize(20, 90)
            return dx_login

        dx_login = get_dx_login(" --token BAD_TOKEN")
        dx_login.expect("The token could not be found")
        dx_login.close()
        self.assertEqual(dx_login.exitstatus, 1)

        dx_login = get_dx_login(" --auth-token BAD_TOKEN")
        dx_login.expect("The token could not be found")
        dx_login.close()
        self.assertEqual(dx_login.exitstatus, 1)

        dx_login = get_dx_login()
        dx_login.expect("Acquiring credentials")
        dx_login.expect("Username")
        dx_login.sendline(username)
        dx_login.expect("Password: ")
        dx_login.sendline("wrong passwörd")
        dx_login.expect("Incorrect username and/or password")
        dx_login.expect("Username")
        dx_login.sendline()
        dx_login.expect("Password: ")
        dx_login.sendline("wrong passwörd")
        dx_login.expect("Incorrect username and/or password")
        dx_login.expect("Username")
        dx_login.sendline()
        dx_login.expect("Password: ")
        dx_login.sendline("wrong passwörd")
        dx_login.expect("dx: Incorrect username and/or password")
        dx_login.close()
        self.assertEqual(dx_login.exitstatus, EXPECTED_ERR_EXIT_STATUS)

    def test_dx_with_bad_job_id_env(self):
        env = override_environment(DX_JOB_ID="foobar")
        run("dx env", env=env)

    @unittest.skipUnless(testutil.TEST_WITH_AUTHSERVER,
                         'skipping tests that require a running authserver')
    def test_dx_http_request_handles_auth_errors(self):
        # The JSON content cannot be processed.
        with self.assertRaises(HTTPError):
            dxpy.DXHTTPRequest(dxpy.get_auth_server_name() + "/oauth2/token",
                               {"grant_type": "authorization_code",
                                "redirect_uri": "/",
                                "client_id": "apiserver"},
                               prepend_srv=False,
                               max_retries=0)

    def test_dx_api_error_msg(self):
        error_regex = "Request Time=\d{1,15}\.\d{0,8}, Request ID=\d{13}-\d{1,6}"
        with self.assertSubprocessFailure(stderr_regexp=error_regex, exit_code=3):
            run("dx api file-InvalidFileID describe")


class TestDXNewRecord(DXTestCase):
    def test_new_record_basic(self):
        run("dx new record -o :foo --verbose")
        record_id = run("dx new record -o :foo2 --brief --visibility hidden --property foo=bar " +
                        "--property baz=quux --tag onetag --tag twotag --type foo --type bar " +
                        "--details '{\"hello\": \"world\"}'").strip()
        self.assertEqual(record_id, run("dx ls :foo2 --brief").strip())
        self.assertEqual({"hello": "world"}, json.loads(run("dx get -o - :foo2")))

        second_record_id = run("dx new record :somenewfolder/foo --parents --brief").strip()
        self.assertEqual(second_record_id, run("dx ls :somenewfolder/foo --brief").strip())

        # describe
        run("dx describe {record}".format(record=record_id))
        desc = json.loads(run("dx describe {record} --details --json".format(record=record_id)))
        self.assertEqual(desc['tags'], ['onetag', 'twotag'])
        self.assertEqual(desc['types'], ['foo', 'bar'])
        self.assertEqual(desc['properties'], {"foo": "bar", "baz": "quux"})
        self.assertEqual(desc['details'], {"hello": "world"})
        self.assertEqual(desc['hidden'], True)

        desc = json.loads(run("dx describe {record} --json".format(record=second_record_id)))
        self.assertEqual(desc['folder'], '/somenewfolder')

        run("dx rm :foo")
        run("dx rm :foo2")
        run("dx rm -r :somenewfolder")

    def test_dx_new_record_with_close(self):
        record_id = run("dx new record --close --brief").strip()
        self.assertEqual("closed", dxpy.describe(record_id)['state'])

        second_record_id = run("dx new record --brief").strip()
        self.assertEqual("open", dxpy.describe(second_record_id)['state'])

    @unittest.skipUnless(testutil.TEST_ENV, 'skipping test that would clobber your local environment')
    def test_new_record_without_context(self):
        # Without project context, cannot create new object without
        # project qualified path
        with without_project_context():
            with self.assertSubprocessFailure(stderr_regexp='expected the path to be qualified with a project',
                                              exit_code=3):
                run("dx new record foo")
            # Can create object with explicit project qualifier
            record_id = run("dx new record --brief " + self.project + ":foo").strip()
            self.assertEqual(dxpy.DXRecord(record_id).name, "foo")


class TestDXWhoami(DXTestCase):
    def test_dx_whoami_name(self):
        whoami_output = run("dx whoami").strip()
        self.assertEqual(whoami_output, dxpy.api.user_describe(dxpy.whoami())['handle'])
    def test_dx_whoami_id(self):
        whoami_output = run("dx whoami --id").strip()
        self.assertEqual(whoami_output, dxpy.whoami())


class TestDXRmdir(DXTestCase):
    def test_dx_rmdir(self):
        dxpy.api.project_new_folder(self.project, {"folder": "/mydirectory"})
        self.assertIn("/mydirectory", list_folder(self.project, "/")['folders'])
        run("dx rmdir mydirectory")
        self.assertNotIn("/mydirectory", list_folder(self.project, "/")['folders'])


class TestDXMv(DXTestCase):
    def test_dx_mv(self):
        dxpy.new_dxrecord(name="a")
        dxpy.find_one_data_object(name="a", project=self.project, zero_ok=False)
        run("dx mv a b")
        dxpy.find_one_data_object(name="b", project=self.project, zero_ok=False)
        self.assertEqual(dxpy.find_one_data_object(name="a", project=self.project, zero_ok=True), None)


class TestDXRename(DXTestCase):
    def test_rename(self):
        my_record = dxpy.new_dxrecord(name="my record").get_id()
        self.assertEquals(dxpy.describe(my_record)["name"], "my record")
        run("dx rename 'my record' 'my record 2'")
        self.assertEquals(dxpy.describe(my_record)["name"], "my record 2")


class TestDXClientUploadDownload(DXTestCase):
    def test_dx_download_recursive_overwrite(self):
        wd = "foodir"
        if os.path.exists("{wd}".format(wd=wd)):
            run("rm -rf {}".format(wd=wd))
        os.mkdir(wd)
        with open(os.path.join(wd, "file.txt"), 'w') as fd:
            fd.write("foo")
        run("dx upload -r {wd}".format(wd=wd))
        tree1 = check_output("find {wd}".format(wd=wd), shell=True)
        # download the directory again with an overwrite (-f) flag
        run("dx download -r -f {wd}".format(wd=wd))
        tree2 = check_output("find {wd}".format(wd=wd), shell=True)
        self.assertEqual(tree1, tree2)
        run("rm -rf {wd}".format(wd=wd))

    def test_dx_upload_download(self):
        with self.assertSubprocessFailure(stderr_regexp='expected the path to be a non-empty string',
                                          exit_code=3):
            run('dx download ""')
        wd = tempfile.mkdtemp()
        os.mkdir(os.path.join(wd, "a"))
        os.mkdir(os.path.join(wd, "a", "б"))
        os.mkdir(os.path.join(wd, "a", "б", "c"))
        with testutil.TemporaryFile(dir=os.path.join(wd, "a", "б")) as fd:
            fd.write("0123456789ABCDEF"*64)
            fd.flush()
            fd.close()
            with self.assertSubprocessFailure(stderr_regexp='is a directory but the -r/--recursive option was not given', exit_code=3):
                run("dx upload "+wd)
            run("dx upload -r "+wd)
            run('dx wait "{f}"'.format(f=os.path.join(os.path.basename(wd), "a", "б",
                                                      os.path.basename(fd.name))))
            with self.assertSubprocessFailure(stderr_regexp='is a folder but the -r/--recursive option was not given', exit_code=1):
                run("dx download "+os.path.basename(wd))
            old_dir = os.getcwd()
            with chdir(tempfile.mkdtemp()):
                run("dx download -r "+os.path.basename(wd))

                tree1 = check_output("cd {wd}; find .".format(wd=wd), shell=True)
                tree2 = check_output("cd {wd}; find .".format(wd=os.path.basename(wd)), shell=True)
                self.assertEqual(tree1, tree2)

            with chdir(tempfile.mkdtemp()):
                os.mkdir('t')
                run("dx download -r -o t "+os.path.basename(wd))
                tree1 = check_output("cd {wd}; find .".format(wd=wd), shell=True)
                tree2 = check_output("cd {wd}; find .".format(wd=os.path.join("t",
                                                                              os.path.basename(wd))),
                                     shell=True)
                self.assertEqual(tree1, tree2)

                os.mkdir('t2')
                run("dx download -o t2 "+os.path.join(os.path.basename(wd), "a", "б",
                                                      os.path.basename(fd.name)))
                self.assertEqual(os.stat(os.path.join("t2", os.path.basename(fd.name))).st_size,
                                 len("0123456789ABCDEF"*64))

            with chdir(tempfile.mkdtemp()), temporary_project('dx download test proj') as other_project:
                run("dx mkdir /super/")
                run("dx mv '{}' /super/".format(os.path.basename(wd)))

                # Specify an absolute path in another project
                with select_project(other_project):
                    run("dx download -r '{proj}:/super/{path}'".format(proj=self.project, path=os.path.basename(wd)))

                    tree1 = check_output("cd {wd} && find .".format(wd=wd), shell=True)
                    tree2 = check_output("cd {wd} && find .".format(wd=os.path.basename(wd)), shell=True)
                    self.assertEqual(tree1, tree2)

                # Now specify a relative path in the same project
                with chdir(tempfile.mkdtemp()), select_project(self.project):
                    run("dx download -r super/{path}/".format(path=os.path.basename(wd)))

                    tree3 = check_output("cd {wd} && find .".format(wd=os.path.basename(wd)), shell=True)
                    self.assertEqual(tree1, tree3)

            with self.assertSubprocessFailure(stderr_regexp="paths are both file and folder names", exit_code=1):
                cmd = "dx cd {d}; dx mkdir {f}; dx download -r {f}*"
                run(cmd.format(d=os.path.join("/super", os.path.basename(wd), "a", "б"),
                               f=os.path.basename(fd.name)))

    @unittest.skipUnless(testutil.TEST_WITH_AUTHSERVER,
                         'skipping tests that require a running authserver')
    def test_dx_upload_with_upload_perm(self):
        with temporary_project('test proj with UPLOAD perms', reclaim_permissions=True) as temp_project:
            data = {"scope": {"projects": {"*": "UPLOAD"}}}
            upload_only_auth_token = dxpy.DXHTTPRequest(dxpy.get_auth_server_name() + '/system/newAuthToken', data,
                                                        prepend_srv=False, always_retry=True)
            token_callable = dxpy.DXHTTPOAuth2({"auth_token": upload_only_auth_token["access_token"],
                                                "auth_token_type": upload_only_auth_token["token_type"],
                                                "auth_token_signature": upload_only_auth_token["token_signature"]})
            testdir = tempfile.mkdtemp()
            try:
                # Filename provided with path
                with open(os.path.join(testdir, 'myfilename'), 'w') as f:
                    f.write('foo')
                remote_file = dxpy.upload_local_file(filename=os.path.join(testdir, 'myfilename'),
                                                     project=temp_project.get_id(), folder='/', auth=token_callable)
                self.assertEqual(remote_file.name, 'myfilename')
                # Filename provided with file handle
                remote_file2 = dxpy.upload_local_file(file=open(os.path.join(testdir, 'myfilename')),
                                                      project=temp_project.get_id(), folder='/', auth=token_callable)
                self.assertEqual(remote_file2.name, 'myfilename')
            finally:
                shutil.rmtree(testdir)

    @unittest.skipUnless(testutil.TEST_ENV,
                         'skipping test that would clobber your local environment')
    def test_dx_download_no_env(self):
        testdir = tempfile.mkdtemp()
        with testutil.TemporaryFile(dir=testdir) as fd:
            fd.write("foo")
            fd.flush()
            fd.close()
            file_id = run("dx upload " + fd.name + " --brief --wait").strip()
            self.assertTrue(file_id.startswith('file-'))

            # download file
            output_path = os.path.join(testdir, 'output')
            with without_project_context():
                run('dx download ' + file_id + ' -o ' + output_path)
            run('cmp ' + output_path + ' ' + fd.name)

    @unittest.skipUnless(testutil.TEST_ENV, 'skipping test that would clobber your local environment')
    def test_dx_upload_no_env(self):
        # Without project context, cannot upload to a
        # non-project-qualified destination
        with without_project_context():
            with self.assertSubprocessFailure(stderr_regexp='expected the path to be qualified with a project',
                                              exit_code=3):
                run("dx upload --path foo /dev/null")
            # Can upload to a path specified with explicit project qualifier
            file_id = run("dx upload --brief --path " + self.project + ":foo /dev/null").strip()
            self.assertEqual(dxpy.DXFile(file_id).name, "foo")

    def test_dx_make_download_url(self):
        testdir = tempfile.mkdtemp()
        output_testdir = tempfile.mkdtemp()
        with testutil.TemporaryFile(dir=testdir) as fd:
            fd.write("foo")
            fd.flush()
            fd.close()
            file_id = run("dx upload " + fd.name + " --brief --wait").strip()
            self.assertTrue(file_id.startswith('file-'))

            # download file
            download_url = run("dx make_download_url " + file_id).strip()
            run("wget -P " + output_testdir + " " + download_url)
            run('cmp ' + os.path.join(output_testdir, os.path.basename(fd.name)) + ' ' + fd.name)

            # download file with a different name
            download_url = run("dx make_download_url " + file_id + " --filename foo")
            run("wget -P " + output_testdir + " " + download_url)
            run('cmp ' + os.path.join(output_testdir, "foo") + ' ' + fd.name)


    def test_dx_make_download_url_project_affinity(self):
        # Ensure that URLs created with make_download_url never have project
        # affinity. In particular, ensures that download URLs created in a job
        # (when the workspace is set to a container) continue to work after the
        # job has terminated
        with temporary_project("make_download_url test 2") as temp_project_2:
            with temporary_project("make_download_url test 1", select=True) as temp_project_1:
                fh = dxpy.upload_string("foo", project=temp_project_1.get_id(), wait_on_close=True)
                # file now appears in both projects
                temp_project_1.clone(temp_project_2.get_id(), objects=[fh.get_id()])
                download_url = run("dx make_download_url " + fh.get_id()).strip()
                run("wget -O /dev/null " + download_url)
            # Even after project 1 is destroyed, the download URL should still work
            run("wget -O /dev/null " + download_url)

    @unittest.skipUnless(testutil.TEST_ENV,
                         'skipping test that would clobber your local environment')
    def test_dx_download_when_current_project_inaccessible(self):
        with testutil.TemporaryFile() as fd:
            with temporary_project("test_dx_accessible_project", select=True) as p_accessible:
                expected_content = '1234'
                fd.write(expected_content)
                fd.flush()
                fd.close()
                tmp_filename = os.path.basename(fd.name)
                listing = run("dx upload --wait {filepath} --path {project}:{filename}".format(
                              filepath=fd.name, project=p_accessible.get_id(), filename=tmp_filename))
                self.assertIn(p_accessible.get_id(), listing)
                self.assertIn(os.path.basename(fd.name), listing)

                # Create another project, select it, and remove it to loose access to it
                p_inaccessible_name = ("test_dx_inaccessible_project" + str(random.randint(0, 1000000)) + "_" +
                                       str(int(time.time() * 1000)))
                p_inaccessible_id = run("dx new project {name} --brief --select"
                                        .format(name=p_inaccessible_name)).strip()
                with select_project(p_inaccessible_id):
                    self.assertEqual(run("dx find projects --brief --name {name}"
                                         .format(name=p_inaccessible_name)).strip(), p_inaccessible_id)
                    run("dx rmproject -y {name} -q".format(name=p_inaccessible_name))
                    self.assertEqual(run("dx find projects --brief --name {name}"
                                         .format(name=p_inaccessible_name)).strip(), "")
                    current_project_env_var = dxpy.config.get('DX_PROJECT_CONTEXT_ID', None)
                    self.assertEqual(p_inaccessible_id, current_project_env_var)
                    # Successfully download file from the accessible project
                    run("dx download {project}:{filename}"
                        .format(project=p_accessible.name, filename=tmp_filename)).strip()
                    result_content = run("dx head {project}:{filename}"
                                         .format(project=p_accessible.name, filename=tmp_filename)).strip()
                    self.assertEqual(expected_content, result_content)

    def test_dx_upload_mult_paths(self):
        testdir = tempfile.mkdtemp()
        os.mkdir(os.path.join(testdir, 'a'))
        with testutil.TemporaryFile(dir=testdir) as fd:
            fd.write("root-file")
            fd.flush()
            fd.close()
            with testutil.TemporaryFile(dir=os.path.join(testdir, "a")) as fd2:
                fd2.write("a-file")
                fd2.flush()
                fd2.close()

                run(("dx upload -r {rootfile} {testdir} " +
                     "--wait").format(testdir=os.path.join(testdir, 'a'), rootfile=fd.name))
                listing = run("dx ls").split("\n")
                self.assertIn("a/", listing)
                self.assertIn(os.path.basename(fd.name), listing)
                listing = run("dx ls a").split("\n")
                self.assertIn(os.path.basename(fd2.name), listing)

    def test_dx_upload_mult_paths_with_dest(self):
        testdir = tempfile.mkdtemp()
        os.mkdir(os.path.join(testdir, 'a'))
        with testutil.TemporaryFile(dir=testdir) as fd:
            fd.write("root-file")
            fd.flush()
            fd.close()
            with testutil.TemporaryFile(dir=os.path.join(testdir, "a")) as fd2:
                fd2.write("a-file")
                fd2.flush()
                fd2.close()

                run("dx mkdir /destdir")
                run(("dx upload -r {rootfile} {testdir} --destination /destdir " +
                     "--wait").format(testdir=os.path.join(testdir, 'a'), rootfile=fd.name))
                listing = run("dx ls /destdir/").split("\n")
                self.assertIn("a/", listing)
                self.assertIn(os.path.basename(fd.name), listing)
                listing = run("dx ls /destdir/a").split("\n")
                self.assertIn(os.path.basename(fd2.name), listing)

    def test_dx_upload_empty_file(self):
        with testutil.TemporaryFile() as fd:
            fd.close()
            self.assertEqual(0, os.path.getsize(fd.name))
            with temporary_project("test_dx_upload_empty_file default", select=True) as p:
                listing = run("dx upload --wait {}".format(fd.name))
                self.assertIn(p.get_id(), listing)
                self.assertIn(os.path.basename(fd.name), listing)
                self.assertIn("0 bytes", listing)

    @unittest.skipUnless(testutil.TEST_AZURE, "Skipping test that would upload file to Azure")
    def test_dx_upload_empty_file_azure(self):
        with testutil.TemporaryFile() as fd:
            fd.close()
            self.assertEqual(0, os.path.getsize(fd.name))
            with temporary_project("test_dx_upload_empty_file azure", select=True, region="azure:westus") as p:
                listing = run("dx upload --wait {}".format(fd.name))
                self.assertIn(p.get_id(), listing)
                self.assertIn(os.path.basename(fd.name), listing)
                self.assertIn("0 bytes", listing)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, "Skipping test that would run jobs")
    def test_dx_download_by_job_id_and_output_field(self):
        test_project_name = 'PTFM-13437'
        test_file_name = 'test_file_01'
        expected_result = 'asdf1234...'
        with temporary_project(test_project_name, select=True) as temp_project:
            temp_project_id = temp_project.get_id()

            # Create and run minimal applet to generate output file.
            code_str = """import dxpy
@dxpy.entry_point('main')
def main():
    test_file_01 = dxpy.upload_string('{exp_res}', name='{filename}')
    output = {{}}
    output['{filename}'] = dxpy.dxlink(test_file_01)
    return output
dxpy.run()
"""
            code_str = code_str.format(exp_res=expected_result, filename=test_file_name)
            app_spec = {"name": "test_applet_dx_download_by_jbor",
                        "project": temp_project_id,
                        "dxapi": "1.0.0",
                        "inputSpec": [],
                        "outputSpec": [{"name": test_file_name, "class": "file"}],
                        "runSpec": {"code": code_str, "interpreter": "python2.7"},
                        "version": "1.0.0"}
            applet_id = dxpy.api.applet_new(app_spec)['id']
            applet = dxpy.DXApplet(applet_id)
            job = applet.run({}, project=temp_project_id)
            job.wait_on_done()
            job_id = job.get_id()

            # Case: Correctly specify "<job_id>:<output_field>"; save to file.
            with chdir(tempfile.mkdtemp()):
                run("dx download " + job_id + ":" + test_file_name)
                with open(test_file_name) as fh:
                    result = fh.read()
                    self.assertEqual(expected_result, result)

            # Case: Correctly specify file id; print to stdout.
            test_file_id = dxpy.DXFile(job.describe()['output'][test_file_name]).get_id()
            result = run("dx download " + test_file_id + " -o -").strip()
            self.assertEqual(expected_result, result)

            # Case: Correctly specify file name; print to stdout.
            result = run("dx download " + test_file_name + " -o -").strip()
            self.assertEqual(expected_result, result)

            # Case: Correctly specify "<job_id>:<output_field>"; print to stdout.
            result = run("dx download " + job_id + ":" + test_file_name + " -o -").strip()
            self.assertEqual(expected_result, result)

            # Case: File does not exist.
            with self.assertSubprocessFailure(stderr_regexp="Unable to resolve", exit_code=3):
                run("dx download foo -o -")

            # Case: Invalid output field name when specifying <job_id>:<output_field>.
            with self.assertSubprocessFailure(stderr_regexp="Could not find", exit_code=3):
                run("dx download " + job_id + ":foo -o -")

    # In a directory structure like:
    # ROOT/
    #      X.txt
    #      A/
    #      B/
    # Make sure that files/subdirs are not downloaded twice. This checks that we fixed
    # PTFM-14106.
    def test_dx_download_root_recursive(self):
        data = "ABCD"

        def gen_file(fname, proj_id):
            dxfile = dxpy.upload_string(data, name=fname, project=proj_id, wait_on_close=True)
            return dxfile

        # Download the project recursively, with command [cmd_string].
        # Compare the downloaded directory against the first download
        # structure.
        def test_download_cmd(org_dir, cmd_string):
            testdir = tempfile.mkdtemp()
            with chdir(testdir):
                run(cmd_string)
                run("diff -Naur {} {}".format(org_dir, testdir))
            shutil.rmtree(testdir)

        with temporary_project('test_proj', select=True) as temp_project:
            proj_id = temp_project.get_id()
            gen_file("X.txt", proj_id)
            dxpy.api.project_new_folder(proj_id, {"folder": "/A"})
            dxpy.api.project_new_folder(proj_id, {"folder": "/B"})

            # Create an entire copy of the project directory structure,
            # which will be compared to all other downloads.
            orig_dir = tempfile.mkdtemp()
            with chdir(orig_dir):
                run("dx download -r {}:/".format(proj_id))

            test_download_cmd(orig_dir, "dx download -r /")
            test_download_cmd(orig_dir, "dx download -r {}:/*".format(proj_id))
            test_download_cmd(orig_dir, "dx download -r *")

            shutil.rmtree(orig_dir)

    # Test download to stdout
    def test_download_to_stdout(self):
        data = "ABCD"

        def gen_file(fname, proj_id):
            dxfile = dxpy.upload_string(data, name=fname, project=proj_id, wait_on_close=True)
            return dxfile

        with temporary_project('test_proj', select=True) as temp_project:
            proj_id = temp_project.get_id()
            gen_file("X.txt", proj_id)
            buf = run("dx download -o - X.txt")
            self.assertEqual(buf, data)

    def test_dx_download_resume_and_checksum(self):
        def assert_md5_checksum(filename, hasher):
            with open(filename, "rb") as fh:
                self.assertEqual(hashlib.md5(fh.read()).hexdigest(), hasher.hexdigest())

        def truncate(filename, size):
            with open(filename, "rb+") as fh:
                fh.seek(size)
                fh.truncate()

        # Manually upload 2 parts
        part1, part2 = b"0123456789ABCDEF"*1024*64*5, b"0"
        dxfile = dxpy.new_dxfile(name="test")
        dxfile.upload_part(part1, index=1)
        dxfile.upload_part(part2, index=2)
        dxfile.close(block=True)

        wd = tempfile.mkdtemp()
        run("cd {wd}; dx download test; ls -la".format(wd=wd))
        assert_md5_checksum(os.path.join(wd, "test"), hashlib.md5(part1 + part2))
        truncate(os.path.join(wd, "test"), 1024*1024*5)
        run("cd {wd}; dx download -f test".format(wd=wd))
        assert_md5_checksum(os.path.join(wd, "test"), hashlib.md5(part1 + part2))
        truncate(os.path.join(wd, "test"), 1024*1024*5 - 1)
        run("cd {wd}; dx download -f test".format(wd=wd))
        assert_md5_checksum(os.path.join(wd, "test"), hashlib.md5(part1 + part2))
        truncate(os.path.join(wd, "test"), 1)
        run("cd {wd}; dx download -f test".format(wd=wd))
        assert_md5_checksum(os.path.join(wd, "test"), hashlib.md5(part1 + part2))
        run("cd {wd}; rm test; touch test".format(wd=wd))
        run("cd {wd}; dx download -f test".format(wd=wd))
        assert_md5_checksum(os.path.join(wd, "test"), hashlib.md5(part1 + part2))


class TestDXClientDownloadDataEgressBilling(DXTestCase):
    def gen_file(self, fname, data, proj_id):
        return dxpy.upload_string(data, name=fname, project=proj_id, wait_on_close=True)

    def get_billed_project(self):
        with open(self.temp_file_fd.name, "r") as fd:
            return fd.read()

    # Clean testing state prior to running a download test.
    #
    # We need to remove the local file before downloading. The file
    # has already been downloaded, and the 'dx download' code will
    # skip re-downloading, causing test failure.
    def prologue(self, file1, file2):
        with open(self.temp_file_fd.name, "w") as fd:
            fd.truncate()
        for filename in [file1, file2]:
            if os.path.exists(filename):
                os.remove(filename)

    def setUp(self):
        self.temp_file_fd = tempfile.NamedTemporaryFile(delete=False)
        # set output file to verify api call is called with correct project
        os.environ['_DX_DUMP_BILLED_PROJECT'] = self.temp_file_fd.name

    def tearDown(self):
        del os.environ['_DX_DUMP_BILLED_PROJECT']
        self.temp_file_fd.close()
        os.remove(self.temp_file_fd.name)

    @unittest.skipUnless(testutil.TEST_ENV,
                         'skipping test that would clobber your local environment')
    def test_dx_cat_project_context(self):
        proj1_name = 'test_proj1'
        proj2_name = 'test_proj2'

        with temporary_project(proj1_name, select=True) as proj, \
                temporary_project(proj2_name) as proj2, \
                chdir(tempfile.mkdtemp()):
            data1 = 'ABCD'
            file1_name = "file1"
            file1_id = self.gen_file(file1_name, data1, proj.get_id()).get_id()

            data2 = '1234'
            file2_name = "file2"
            file2_id = self.gen_file(file2_name, data2, proj2.get_id()).get_id()

            # Success: project from context contains file specified by ID
            buf = run("dx download -o - {f}".format(f=file1_id))
            self.assertEqual(buf, data1)
            # Project context alone, when combined with file by ID, is
            # not sufficient to indicate user's intent to use that
            # project
            self.assertEqual(self.get_billed_project(), "")

            # Success: project from context contains file specified by dxlink
            buf = run("dx download -o - '{{\"$dnanexus_link\": \"{f}\"}}'".format(f=file1_id))
            self.assertEqual(buf, data1)
            self.assertEqual(self.get_billed_project(), "")

            # Success: project from context contains file specified by name
            buf = run("dx download -o - {f}".format(f=file1_name))
            self.assertEqual(buf, data1)
            self.assertEqual(self.get_billed_project(), proj.get_id())

            # Success: project specified by context does not contains file specified by ID
            buf = run("dx download -o - {f}".format(f=file2_id))
            self.assertEqual(buf, data2)
            self.assertEqual(self.get_billed_project(), "")

            # Success: project specified by context does not contains file specified by dxlink
            buf = run("dx download -o - '{{\"$dnanexus_link\": \"{f}\"}}'".format(f=file2_id))
            self.assertEqual(buf, data2)
            self.assertEqual(self.get_billed_project(), "")

            # Failure: project specified by context does not contains file specified by name
            with self.assertSubprocessFailure(stderr_regexp="Unable to resolve", exit_code=3):
                run("dx download -o - {f}".format(f=file2_name))

    @unittest.skipUnless(testutil.TEST_ENV,
                         'skipping test that would clobber your local environment')
    def test_dx_download_project_context(self):
        proj1_name = 'test_proj1'
        proj2_name = 'test_proj2'

        with temporary_project(proj1_name, select=True) as proj, \
                temporary_project(proj2_name) as proj2, \
                chdir(tempfile.mkdtemp()):
            data1 = 'ABCD'
            file1_name = "file1"
            file1_id = self.gen_file(file1_name, data1, proj.get_id()).get_id()

            data2 = '1234'
            file2_name = "file2"
            file2_id = self.gen_file(file2_name, data2, proj2.get_id()).get_id()

            # Success: project from context contains file specified by ID
            self.prologue(file1_name, file2_name)
            run("dx download -f --no-progress {f}".format(f=file1_id))
            # Project context alone, when combined with file by ID, is
            # not sufficient to indicate user's intent to use that
            # project
            self.assertEqual(self.get_billed_project(), "")

            # Success: project from context contains file specified by dxlink
            self.prologue(file1_name, file2_name)
            run("dx download -f --no-progress '{{\"$dnanexus_link\": \"{f}\"}}'".format(f=file1_id))
            self.assertEqual(self.get_billed_project(), "")

            # Success: project from context contains file specified by name
            self.prologue(file1_name, file2_name)
            run("dx download -f --no-progress {f}".format(f=file1_name))
            self.assertEqual(self.get_billed_project(), proj.get_id())

            # Success: project specified by context does not contains file specified by ID
            self.prologue(file1_name, file2_name)
            run("dx download -f --no-progress {f}".format(f=file2_id))
            self.assertEqual(self.get_billed_project(), "")

            # Success: project specified by context does not contains file specified by dxlink
            self.prologue(file1_name, file2_name)
            run("dx download -f --no-progress '{{\"$dnanexus_link\": \"{f}\"}}'".format(f=file2_id))
            self.assertEqual(self.get_billed_project(), "")

            # Failure: project specified by context does not contains file specified by name
            self.prologue(file1_name, file2_name)
            with self.assertSubprocessFailure(stderr_regexp="Unable to resolve", exit_code=3):
                run("dx download -f --no-progress {f}".format(f=file2_name))

    def test_dx_download_project_explicit(self):
        proj1_name = 'test_proj1_' + str(time.time())
        proj2_name = 'test_proj2_' + str(time.time())

        with temporary_project(proj1_name, select=True) as proj, \
                temporary_project(proj2_name) as proj2, \
                chdir(tempfile.mkdtemp()):
            data1 = 'ABCD'
            file1_name = "file1"
            file1_id = self.gen_file(file1_name, data1, proj.get_id()).get_id()

            data2 = '1234'
            file2_name = "file2"
            file2_id = self.gen_file(file2_name, data2, proj2.get_id()).get_id()

            # Explicit project provided

            # Success: project specified by ID contains file specified by ID
            buf = run("dx download -o - {p}:{f}".format(p=proj2.get_id(), f=file2_id))
            self.assertEqual(buf, data2)
            self.assertEqual(self.get_billed_project(), proj2.get_id())

            # Success: project specified by ID contains file specified by name
            buf = run("dx download -o - {p}:{f}".format(p=proj.get_id(), f=file1_name))
            self.assertEqual(buf, data1)
            self.assertEqual(self.get_billed_project(), proj.get_id())

            # Success: project specified by name contains file specified by ID
            buf = run("dx download -o - {p}:{f}".format(p=proj2_name, f=file2_id))
            self.assertEqual(buf, data2)
            self.assertEqual(self.get_billed_project(), proj2.get_id())

            # Success: project specified by name contains file specified by name
            buf = run("dx download -o - {p}:{f}".format(p=proj1_name, f=file1_name))
            self.assertEqual(buf, data1)
            self.assertEqual(self.get_billed_project(), proj.get_id())

            # Failure: project specified by ID does not contain file specified by ID
            with self.assertSubprocessFailure(stderr_regexp="Error: project does not", exit_code=3):
                run("dx download -o - {p}:{f}".format(p=proj2.get_id(), f=file1_id))

            # Failure: project specified by ID does not contain file specified by name
            with self.assertSubprocessFailure(stderr_regexp="Unable to resolve", exit_code=3):
                run("dx download -o - {p}:{f}".format(p=proj.get_id(), f=file2_name))

            # Failure: project specified by name does not contain file specified by ID
            with self.assertSubprocessFailure(stderr_regexp="Error: project does not", exit_code=3):
                run("dx download -o - {p}:{f}".format(p=proj2_name, f=file1_id))

            # Failure: project specified by name does not contain file specified by name
            with self.assertSubprocessFailure(stderr_regexp="Unable to resolve", exit_code=3):
                run("dx download -o - {p}:{f}".format(p=proj1_name, f=file2_name))

            # Test api call parameters when downloading to local file instead of cat to std out

            # Success: project specified by ID contains file specified by ID
            self.prologue(file1_name, file2_name)
            run("dx download -f --no-progress {p}:{f}".format(p=proj2.get_id(), f=file2_id))
            self.assertEqual(self.get_billed_project(), proj2.get_id())

            # Success: project specified by ID contains file specified by name
            self.prologue(file1_name, file2_name)
            run("dx download -f --no-progress {p}:{f}".format(p=proj.get_id(), f=file1_name))
            self.assertEqual(self.get_billed_project(), proj.get_id())

            # Success: project specified by name contains file specified by ID
            self.prologue(file1_name, file2_name)
            run("dx download -f --no-progress {p}:{f}".format(p=proj2_name, f=file2_id))
            self.assertEqual(self.get_billed_project(), proj2.get_id())

            # Success: project specified by name contains file specified by name
            self.prologue(file1_name, file2_name)
            run("dx download -f --no-progress {p}:{f}".format(p=proj1_name, f=file1_name))
            self.assertEqual(self.get_billed_project(), proj.get_id())

            # Failure: project specified by ID does not contain file specified by ID
            with self.assertSubprocessFailure(stderr_regexp="Error: specified project does not", exit_code=1):
                run("dx download -f --no-progress {p}:{f}".format(p=proj2.get_id(), f=file1_id))

            # Failure: project specified by ID does not contain file specified by name
            with self.assertSubprocessFailure(stderr_regexp="Unable to resolve", exit_code=3):
                run("dx download -f --no-progress {p}:{f}".format(p=proj.get_id(), f=file2_name))

            # Failure: project specified by name does not contain file specified by ID
            with self.assertSubprocessFailure(stderr_regexp="Error: specified project does not", exit_code=1):
                run("dx download -f --no-progress {p}:{f}".format(p=proj2_name, f=file1_id))

            # Failure: project specified by name does not contain file specified by name
            with self.assertSubprocessFailure(stderr_regexp="Unable to resolve", exit_code=3):
                run("dx download -f --no-progress {p}:{f}".format(p=proj1_name, f=file2_name))

    def test_dx_download_multiple_projects_with_same_name(self):
        proj_name = 'test_proj1'

        with temporary_project(proj_name, select=True) as proj, \
                temporary_project(proj_name) as proj2, \
                chdir(tempfile.mkdtemp()):
            data1 = 'ABCD'
            file1_name = "file1"
            file1_id = self.gen_file(file1_name, data1, proj.get_id()).get_id()

            data2 = '1234'
            file2_name = "file1"
            file2_id = self.gen_file(file2_name, data2, proj2.get_id()).get_id()

            # Success: project specified by ID contains file specified by ID
            buf = run("dx download -o - {pid}:{f}".format(pid=proj2.get_id(), f=file2_id))
            self.assertEqual(buf, data2)
            self.assertEqual(self.get_billed_project(), proj2.get_id())

            # Failure: project specified by name contains file specified by ID
            with self.assertSubprocessFailure(stderr_regexp="ResolutionError: Found multiple projects", exit_code=3):
                run("dx download -o - {pname}:{f}".format(pname=proj_name, f=file2_id))

            # Replicate same tests for non-cat (download to file) route

            # Success: project specified by ID contains file specified by ID
            run("dx download -f --no-progress {pid}:{f}".format(pid=proj.get_id(), f=file1_id))
            self.assertEqual(self.get_billed_project(), proj.get_id())

            # Failure: project specified by name contains file specified by ID
            with self.assertSubprocessFailure(stderr_regexp="ResolutionError: Found multiple projects", exit_code=3):
                run("dx download -f --no-progress {pname}:{f}".format(pname=proj_name, f=file2_id))

    @unittest.skipUnless(testutil.TEST_ENV and testutil.TEST_RUN_JOBS,
                         'skipping test that would clobber your local environment and run jobs')
    def test_dx_download_jbors(self):
        proj1_name = 'test_proj1'
        proj2_name = 'test_proj2'

        with temporary_project(proj1_name, select=True) as proj1, \
                temporary_project(proj2_name) as proj2, \
                chdir(tempfile.mkdtemp()):
            dxfile = dxpy.upload_string("foo", project=proj1.get_id(), wait_on_close=True)
            applet_id = dxpy.api.applet_new({
                "project": proj1.get_id(),
                "dxapi": "0.0.1",
                "inputSpec": [{"name": "infile", "class": "file"}],
                "outputSpec": [{"name": "outfile", "class": "file"}],
                "runSpec": {"interpreter": "bash",
                            "code": """
dx-jobutil-add-output outfile `dx-jobutil-parse-link "$infile"`
"""
                            }})["id"]
            applet = dxpy.DXApplet(applet_id)
            dxjob1 = applet.run({"infile": {"$dnanexus_link": dxfile.get_id()}}, project=proj1.get_id())
            dxjob2 = applet.run({"infile": {"$dnanexus_link": dxfile.get_id()}}, project=proj2.get_id())
            dxjob1.wait_on_done()
            dxjob2.wait_on_done()

            # Test downloading from jobs running in the current project
            # context, and outside of the current project context
            run("dx download -f --no-progress {job1}:outfile".format(job1=dxjob1.get_id()))
            self.assertEqual(self.get_billed_project(), proj1.get_id())
            run("dx download --no-progress -o - {job1}:outfile".format(job1=dxjob1.get_id()))
            self.assertEqual(self.get_billed_project(), proj1.get_id())
            run("dx download -f --no-progress {job2}:outfile".format(job2=dxjob2.get_id()))
            self.assertEqual(self.get_billed_project(), proj2.get_id())
            run("dx download --no-progress -o - {job2}:outfile".format(job2=dxjob2.get_id()))
            self.assertEqual(self.get_billed_project(), proj2.get_id())

            # Test downloading without a project context set
            with without_project_context():
                run("dx download -f --no-progress {job1}:outfile".format(job1=dxjob1.get_id()))
                self.assertEqual(self.get_billed_project(), proj1.get_id())


class TestDXClientDescribe(DXTestCase):
    def test_projects(self):
        run("dx describe :")
        run("dx describe " + self.project)
        run("dx describe " + self.project + ":")

        # need colon to recognize as project name
        with self.assertSubprocessFailure(exit_code=3):
            run("dx describe dxclient_test_pröject")

        # bad project name
        with self.assertSubprocessFailure(exit_code=3):
            run("dx describe dne:")

        # nonexistent project ID
        with self.assertSubprocessFailure(exit_code=3):
            run("dx describe project-123456789012345678901234")

    def test_bad_current_project(self):
        with self.assertSubprocessFailure(stderr_regexp='No matches found', exit_code=3):
            run("dx describe nonexistent --project-context-id foo")

        run("dx describe " + self.project + " --project-context-id foo")

    def test_user_describe_self_shows_bill_to(self):
        ## Verify `billTo` from /user-xxxx/describe.
        user_id = dxpy.whoami()
        user_desc = dxpy.api.user_describe(user_id)
        self.assertTrue("billTo" in user_desc)
        self.assertEqual(user_desc.get("billTo"), user_id)

        ## Verify `billTo` from "dx describe user-xxxx".
        bill_to_label = "Default bill to"
        cli_user_desc = run("dx describe " + user_id).strip().split("\n")
        parsed_desc = filter(lambda line: line.startswith(bill_to_label),
                             cli_user_desc)
        self.assertEqual(len(parsed_desc), 1)
        key_and_value = parsed_desc[0].split(bill_to_label)
        self.assertEqual(key_and_value[0], "")
        self.assertEqual(key_and_value[1].strip(), user_id)

        ## Verify `billTo` from "dx describe user-xxxx --json".
        cli_user_desc_json = json.loads(
            run("dx describe " + user_id + " --json")
        )
        self.assertTrue("billTo" in cli_user_desc_json)
        self.assertEqual(cli_user_desc_json.get("billTo"), user_id)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_describe_deleted_app(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [],
                                         "outputSpec": [],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": ""},
                                         "name": "applet_to_delete"})['id']
        app_new_output = dxpy.api.app_new({"name": "app_to_delete",
                                           "applet": applet_id,
                                           "version": "1.0.0"})

        # make second app with no default tag
        app_new_output2 = dxpy.api.app_new({"name": "app_to_delete",
                                           "applet": applet_id,
                                           "version": "1.0.1"})
        dxpy.api.app_delete(app_new_output2["id"])

        run("dx describe " + app_new_output2["id"])


class TestDXClientRun(DXTestCase):
    def setUp(self):
        self.other_proj_id = run("dx new project other --brief").strip()
        super(TestDXClientRun, self).setUp()

    def tearDown(self):
        dxpy.api.project_destroy(self.other_proj_id, {'terminateJobs': True})
        super(TestDXClientRun, self).tearDown()

    def test_dx_run_applet_with_input_spec(self):
        record = dxpy.new_dxrecord(name="my_record")

        applet_id = dxpy.api.applet_new({
            "project": self.project,
            "dxapi": "0.0.1",
            "inputSpec": [
                {"name": "int0", "class": "int"},
                {"name": "int1", "class": "int", "optional": True},
                {"name": "string0", "class": "string"},
                {"name": "string1", "class": "string", "optional": True},
                {"name": "record0", "class": "record"},
                {"name": "record1", "class": "record", "optional": True},
            ],
            "outputSpec": [
                {"name": "outint", "class": "int"},
                {"name": "outstring", "class": "string"},
                {"name": "outrecord", "class": "record"},
            ],
            "runSpec": {"interpreter": "bash",
                        "code": """
dx-jobutil-add-output outint $int0
dx-jobutil-add-output outstring $string0
dx-jobutil-add-output outrecord $record0
"""
                        }})["id"]
        applet = dxpy.DXApplet(applet_id)

        #############################
        # With only required inputs #
        #############################

        # Run with applet handler.
        job = applet.run({"int0": 16, "string0": "input_string",
                          "record0": {"$dnanexus_link": record.get_id()}})
        job_desc = job.describe()
        exp = {"int0": 16, "string0": "input_string",
               "record0": {"$dnanexus_link": record.get_id()}}
        self.assertEquals(job_desc["input"], exp)

        # Run with "dx run".
        job_id = run("dx run {applet_id} -iint0=16 -istring0=input_string -irecord0={record_id} --brief".format(
            applet_id=applet_id, record_id=record.get_id())).strip()
        job_desc = dxpy.describe(job_id)
        self.assertEquals(job_desc["input"], exp)

        job_id = run("dx run {applet_id} -iint0:int=16 -istring0:string=input_string -irecord0:record={record_id} --brief".format(
            applet_id=applet_id, record_id=record.get_id())).strip()
        job_desc = dxpy.describe(job_id)
        self.assertEquals(job_desc["input"], exp)

        # Run with "dx run" with JBORs.
        other_job_id = run("dx run {applet_id} -iint0={job_id}:outint -istring0={job_id}:outstring -irecord0={job_id}:outrecord --brief".format(
            applet_id=applet_id, job_id=job_id)).strip()
        job_desc = dxpy.describe(other_job_id)
        exp = {"int0": {"$dnanexus_link": {"field": "outint",
                                           "job": job_id}},
               "string0": {"$dnanexus_link": {"field": "outstring",
                                              "job": job_id}},
               "record0": {"$dnanexus_link": {"field": "outrecord",
                                              "job": job_id}}}
        self.assertEquals(job_desc["input"], exp)

        # Run with "dx run" with input name mapped to data object name.
        job_id = run("dx run {applet_id} -iint0=16 -istring0=input_string -irecord0=my_record --brief".format(
            applet_id=applet_id)).strip()
        job_desc = dxpy.describe(job_id)
        exp = {"int0": 16, "string0": "input_string",
               "record0": {"$dnanexus_link": {"project": self.project,
                                              "id": record.get_id()}}}
        self.assertEquals(job_desc["input"], exp)

        #####################################
        # With required and optional inputs #
        #####################################

        second_record = dxpy.new_dxrecord()

        # Run with applet handler.
        job = applet.run({"int0": 16, "string0": "input_string",
                          "record0": {"$dnanexus_link": record.get_id()},
                          "int1": 32, "string1": "second_input_string",
                          "record1": {"$dnanexus_link": second_record.get_id()}})
        job_desc = job.describe()
        exp = {"int0": 16, "int1": 32, "string0": "input_string",
               "string1": "second_input_string",
               "record0": {"$dnanexus_link": record.get_id()},
               "record1": {"$dnanexus_link": second_record.get_id()}}
        self.assertEquals(job_desc["input"], exp)

        # Run with "dx run".
        job_id = run("dx run {applet_id} -iint0=16 -istring0=input_string -irecord0={record_id} -iint1=32 -istring1=second_input_string -irecord1={second_record_id} --brief".format(
            applet_id=applet_id, record_id=record.get_id(),
            second_record_id=second_record.get_id())).strip()
        job_desc = dxpy.describe(job_id)
        self.assertEquals(job_desc["input"], exp)

        # Run with "dx run" with JBORs.
        other_job_id = run("dx run {applet_id} -iint0=32 -iint1={job_id}:outint -istring0=second_input_string -istring1={job_id}:outstring -irecord0={second_record_id} -irecord1={job_id}:outrecord --brief".format(
            applet_id=applet_id, job_id=job_id,
            second_record_id=second_record.get_id())).strip()
        job_desc = dxpy.describe(other_job_id)
        exp = {"int0": 32,
               "int1": {"$dnanexus_link": {"field": "outint",
                                           "job": job_id}},
               "string0": "second_input_string",
               "string1": {"$dnanexus_link": {"field": "outstring",
                                              "job": job_id}},
               "record0": {"$dnanexus_link": second_record.get_id()},
               "record1": {"$dnanexus_link": {"field": "outrecord",
                                              "job": job_id}}}
        self.assertEquals(job_desc["input"], exp)

    def test_dx_run_applet_without_input_spec(self):
        record = dxpy.new_dxrecord(name="my_record")

        applet_id = dxpy.api.applet_new({
            "project": self.project,
            "dxapi": "0.0.1",
            "outputSpec": [
                {"name": "outint", "class": "int"},
                {"name": "outstring", "class": "string"},
                {"name": "outrecord", "class": "record"},
            ],
            "runSpec": {"interpreter": "bash",
                        "code": """
record_id=`dx new record --close --brief`
dx-jobutil-add-output outint 32
dx-jobutil-add-output outstring output_string
dx-jobutil-add-output outrecord $record_id
"""
                        }})["id"]
        applet = dxpy.DXApplet(applet_id)

        # Run with applet handler.
        job = applet.run({"int0": 16, "string0": "input_string",
                          "record0": {"$dnanexus_link": record.get_id()}})
        job_desc = job.describe()
        exp = {"int0": 16, "string0": "input_string",
               "record0": {"$dnanexus_link": record.get_id()}}
        self.assertEquals(job_desc["input"], exp)

        # Run with "dx run".
        job_id = run("dx run {applet_id} -iint0=16 -istring0=input_string -irecord0={record_id} --brief".format(applet_id=applet_id, record_id=record.get_id())).strip()
        job_desc = dxpy.describe(job_id)
        self.assertEquals(job_desc["input"], exp)

        job_id = run("dx run {applet_id} -iint0:int=16 -istring0:string=input_string -irecord0:record={record_id} --brief".format(applet_id=applet_id, record_id=record.get_id())).strip()
        job_desc = dxpy.describe(job_id)
        self.assertEquals(job_desc["input"], exp)

        # Run with "dx run" with JBORs.
        other_job_id = run("dx run {applet_id} -iint0={job_id}:outint -istring0={job_id}:outstring -irecord0={job_id}:outrecord --brief".format(applet_id=applet_id, job_id=job_id)).strip()
        job_desc = dxpy.describe(other_job_id)
        exp = {"int0": {"$dnanexus_link": {"field": "outint",
                                           "job": job_id}},
               "string0": {"$dnanexus_link": {"field": "outstring",
                                              "job": job_id}},
               "record0": {"$dnanexus_link": {"field": "outrecord",
                                              "job": job_id}}}
        self.assertEquals(job_desc["input"], exp)

        other_job_id = run("dx run {applet_id} -irecord0={record_id} -irecord1={job_id}:outrecord --brief".format(
            applet_id=applet_id, job_id=job_id, record_id=record.get_id()
        )).strip()
        job_desc = dxpy.describe(other_job_id)
        exp = {"record0": {"$dnanexus_link": record.get_id()},
               "record1": {"$dnanexus_link": {"field": "outrecord",
                                              "job": job_id}}}
        self.assertEquals(job_desc["input"], exp)

        # Run with "dx run" with repeated input names: order of input values
        # preserved.
        other_job_id = run("dx run {applet_id} -irecord0={record_id} -irecord0={job_id}:outrecord --brief".format(
            applet_id=applet_id, job_id=job_id, record_id=record.get_id()
        )).strip()
        job_desc = dxpy.describe(other_job_id)
        exp = {"record0": [{"$dnanexus_link": record.get_id()},
                           {"$dnanexus_link": {"field": "outrecord",
                                               "job": job_id}}]}
        self.assertEquals(job_desc["input"], exp)

        other_job_id = run("dx run {applet_id} -irecord0={job_id}:outrecord -irecord0={record_id} --brief".format(
            applet_id=applet_id, job_id=job_id, record_id=record.get_id()
        )).strip()
        job_desc = dxpy.describe(other_job_id)
        exp = {"record0": [{"$dnanexus_link": {"field": "outrecord",
                                               "job": job_id}},
                           {"$dnanexus_link": record.get_id()}]}
        self.assertEquals(job_desc["input"], exp)

        # Run with "dx run" with input name mapped to data object name.
        job_id = run("dx run {applet_id} -irecord0=my_record --brief".format(applet_id=applet_id)).strip()
        job_desc = dxpy.describe(job_id)
        exp = {"record0": {"$dnanexus_link": {"project": self.project,
                                              "id": record.get_id()}}}
        self.assertEquals(job_desc["input"], exp)

    def test_dx_resolve(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"}
                                         })['id']
        record_id0 = dxpy.api.record_new({"project": self.project,
                                          "dxapi": "1.0.0",
                                          "name": "resolve_record0"})['id']
        record_id1 = dxpy.api.record_new({"project": self.project,
                                          "dxapi": "1.0.0",
                                          "name": "resolve_record1"})['id']
        record_id2 = dxpy.api.record_new({"project": self.project,
                                          "dxapi": "1.0.0",
                                          "name": "resolve_record2"})['id']
        glob_id = dxpy.api.record_new({"project": self.project,
                                       "dxapi": "1.0.0",
                                       "name": "glob_resolve_record"})['id']

        job_id = run("dx run " + applet_id + " -iinput0=resolve_record0 -iinput1=resolve_record1 " +
                     "-iinput2=glob_resolve* -iint0=5 -iint1=15 --brief -y").strip()
        job_desc = dxpy.describe(job_id)

        self.assertEquals(job_desc['input']['input0']['$dnanexus_link']['id'], record_id0)
        self.assertEquals(job_desc['input']['input1']['$dnanexus_link']['id'], record_id1)
        self.assertEquals(job_desc['input']['input2']['$dnanexus_link']['id'], glob_id)
        self.assertEquals(job_desc['input']['int0'], 5)
        self.assertEquals(job_desc['input']['int1'], 15)

        # If multiple entities are provided with the same input name, then their resolved result should
        # appear in a list, in the order in which they were provided, no matter the method of resolution
        job_id = run("dx run " + applet_id + " -iinput0=resolve_record0 -iinput0=25 -iinput0=glob_resolve* " +
                     "-iinput0=resolve_record1 -iinput1=" + record_id0 + " -iinput1=50 -iinput1=resolve_record1 " +
                     "--brief -y").strip()
        job_desc = dxpy.describe(job_id)

        self.assertEquals(len(job_desc['input']['input0']), 4)
        self.assertEquals(job_desc['input']['input0'][0]['$dnanexus_link']['id'], record_id0)
        self.assertEquals(job_desc['input']['input0'][1], 25)
        self.assertEquals(job_desc['input']['input0'][2]['$dnanexus_link']['id'], glob_id)
        self.assertEquals(job_desc['input']['input0'][3]['$dnanexus_link']['id'], record_id1)
        self.assertEquals(len(job_desc['input']['input1']), 3)
        self.assertEquals(job_desc['input']['input1'][0]['$dnanexus_link'], record_id0)
        self.assertEquals(job_desc['input']['input1'][1], 50)
        self.assertEquals(job_desc['input']['input1'][2]['$dnanexus_link']['id'], record_id1)

        # If a record cannot be resolved, then the return value should just be the record name passed in
        job_id = run("dx run " + applet_id + " --brief -y -iinput0=cannot_resolve " +
                     "-iinput1=resolve_record0 -iint0=10").strip()
        job_desc = dxpy.describe(job_id)

        self.assertEquals(job_desc['input']['input0'], "cannot_resolve")
        self.assertEquals(job_desc['input']['input1']['$dnanexus_link']['id'], record_id0)
        self.assertEquals(job_desc['input']['int0'], 10)

        job_id = run("dx run " + applet_id + " --brief -y -iinput0=glob_cannot_resolve*").strip()
        job_desc = dxpy.describe(job_id)

        self.assertEquals(job_desc['input']['input0'], "glob_cannot_resolve*")

        # Should simply use given name if it corresponds to multiple records (glob or not);
        # length validation errors out, but exec_io catches it
        dup_record_id = dxpy.api.record_new({"project": self.project,
                                             "dxapi": "1.0.0",
                                             "name": "resolve_record0"})['id']

        job_id = run("dx run " + applet_id + " --brief -y -iinput0=resolve_record0").strip()
        job_desc = dxpy.describe(job_id)

        self.assertEquals(job_desc['input']['input0'], "resolve_record0")

        job_id = run("dx run " + applet_id + " --brief -y -iinput0=resolve_record*").strip()
        job_desc = dxpy.describe(job_id)

        self.assertEquals(job_desc['input']['input0'], "resolve_record*")

        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [
                                            {"name": "input0", "class": "record"},
                                            {"name": "input1", "class": "array:record", "optional": True},
                                            {"name": "int0", "class": "int"},
                                            {"name": "int1", "class": "array:int", "optional": True},
                                            {"name": "bool0", "class": "array:boolean", "optional": True}
                                         ],
                                         "outputSpec": [],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"}
                                         })['id']

        # Try with applet that has an input spec
        job_id = run("dx run " + applet_id + " --brief -y -iinput0=resolve_record1 -iint0=10 " +
                     "-iinput1=resolve_record2 -iinput1=resolve_record1 -iint1=0 -iint1=1 -iint1=2 " +
                     "-ibool0=true -ibool0=0").strip()
        job_desc = dxpy.describe(job_id)

        self.assertEquals(job_desc['input']['input0']['$dnanexus_link']['id'], record_id1)
        self.assertEquals(job_desc['input']['input1'][0]['$dnanexus_link']['id'], record_id2)
        self.assertEquals(job_desc['input']['input1'][1]['$dnanexus_link']['id'], record_id1)
        self.assertEquals(job_desc['input']['int0'], 10)
        self.assertEquals(job_desc['input']['int1'], [0, 1, 2])
        self.assertEquals(job_desc['input']['bool0'], [True, False])

        # Workflows should show same behavior as applets
        workflow_id = run("dx new workflow myworkflow --output-folder /foo --brief").strip()
        stage_id = dxpy.api.workflow_add_stage(workflow_id,
                                               {"editVersion": 0, "executable": applet_id})['stage']

        record_id = dxpy.api.record_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "name": "myrecord"})['id']

        analysis_id = run("dx run " + workflow_id + " -i" + stage_id + ".input0=myrecord -i" +
                          stage_id + ".int0=77 -y --brief").strip()
        analysis_desc = dxpy.describe(analysis_id)

        self.assertEquals(analysis_desc['input'][stage_id + '.input0']['$dnanexus_link']['id'], record_id)
        self.assertEquals(analysis_desc['input'][stage_id + '.int0'], 77)

    def test_dx_resolve_check_resolution_needed(self):
        # If no entity_name is given, no entity_name should be returned
        self.assertEquals(check_resolution("some_path", "project_id", "/", None), (False, "project_id", "/", None))
        self.assertEquals(check_resolution("some_path", self.project, "/", None), (False, self.project, "/", None))

        record_id = dxpy.api.record_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "name": "myrecord"})['id']

        self.assertEquals(check_resolution("some_path", self.project, "/", "myrecord"),
                          (True, self.project, "/", "myrecord"))

        self.assertEquals(check_resolution("some_path", "not_a_real_project_id", "/", "notarealrecord"),
                          (True, "not_a_real_project_id", "/", "notarealrecord"))

        # If the entity is a DX ID, but not an expected class, the result should be False, None, None, None
        result = check_resolution("some_path", self.project, "/", record_id, expected_classes=["file"])
        self.assertEquals(result, (False, None, None, None))

        # If entity_id is a hash, there is no need to resolve, and the describe
        # output is returned (should work no matter what project is given)
        result = check_resolution("some_path", self.project, "/", record_id, expected_classes=["record"])
        self.assertEquals(result[:3], (False, self.project, "/"))
        desc_output = result[3]
        self.assertEquals(desc_output["describe"]["project"], self.project)
        self.assertEquals(desc_output["describe"]["name"], "myrecord")
        self.assertEquals(desc_output["id"], record_id)
        desc_output = check_resolution("some_path", None, "/", record_id, enclose_in_list=True)[3][0]
        self.assertEquals(desc_output["describe"]["project"], self.project)
        self.assertEquals(desc_output["describe"]["name"], "myrecord")
        self.assertEquals(desc_output["id"], record_id)

        # Describing entity_id should work even if the project hint is wrong
        result = check_resolution("some_path", self.project, "/", record_id, describe={"project": self.other_proj_id,
                                                                                       "fields": {"sponsored": True}})
        self.assertEquals(result[:3], (False, self.project, "/"))
        desc_output = result[3]
        self.assertEquals(desc_output["describe"]["sponsored"], False)
        self.assertEquals(desc_output["id"], record_id)

        # Even if the given project is not a real project ID, the correct project ID
        # should be in the describe output
        desc_output = check_resolution("some_path", "not_a_real_project_id", "/", record_id)[3]
        self.assertEquals(desc_output["describe"]["project"], self.project)
        self.assertEquals(desc_output["describe"]["name"], "myrecord")
        self.assertEquals(desc_output["id"], record_id)

        # If describing an entity ID fails, then a ResolutionError should be
        # raised
        with self.assertRaisesRegexp(ResolutionError, "The entity record-\d+ could not be found"):
            check_resolution("some_path", self.project, "/", "record-123456789012345678901234")

    def test_dx_run_depends_on_success(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [],
                                         "outputSpec": [],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"}
                                         })['id']

        job_dep_id = run("dx run " + applet_id + " --brief -y").strip()

        record_dep_id = dxpy.api.record_new({"project": self.project})['id']

        job_id = run("dx run " + applet_id + " --brief -y").strip()
        job_desc = dxpy.describe(job_id)
        self.assertEquals(job_desc['dependsOn'], [])

        job_id = run("dx run " + applet_id + " --brief -y -d " + job_dep_id).strip()
        job_desc = dxpy.describe(job_id)
        self.assertEqual(job_desc['dependsOn'], [job_dep_id])

        job_id = run("dx run " + applet_id + " -d " + job_dep_id + " --depends-on " +
                     record_dep_id + " --brief -y").strip()
        job_desc = dxpy.describe(job_id)
        self.assertEqual(sorted(job_desc['dependsOn']), sorted([job_dep_id, record_dep_id]))

    def test_dx_run_depends_on_failure(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [],
                                         "outputSpec": [],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"}
                                         })['id']

        job1_dep_id = run("dx run " + applet_id + " --brief -y").strip()

        job2_dep_id = run("dx run " + applet_id + " --brief -y").strip()

        # Testing for missing arguments:
        with self.assertSubprocessFailure(stderr_regexp='-d/--depends-on.*expected one argument', exit_code=2):
            run("dx run " + applet_id + " --brief -y --depends-on " + job2_dep_id + " --depends-on")
        with self.assertSubprocessFailure(stderr_regexp='-d/--depends-on.*expected one argument', exit_code=2):
            run("dx run " + applet_id + " -d --depends-on " + job1_dep_id + " --brief -y")

        with self.assertSubprocessFailure(stderr_regexp='unrecognized arguments', exit_code=2):
            run("dx run " + applet_id + " --brief -y -d " + job2_dep_id + " " + job1_dep_id)

        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run("dx run " + applet_id + " --brief -y -d not_a_valid_id")

        # Testing for use of --depends-on with running workflows
        workflow_id = run("dx new workflow myworkflow --output-folder /foo --brief").strip()
        stage_id = dxpy.api.workflow_add_stage(workflow_id,
                                               {"editVersion": 0, "executable": applet_id})['stage']
        with self.assertSubprocessFailure(stderr_regexp='--depends-on.*workflows', exit_code=3):
            run("dx run " + workflow_id + " -d " + job1_dep_id + " -y --brief")
        with self.assertSubprocessFailure(stderr_regexp='--depends-on.*workflows', exit_code=3):
            run("dx run myworkflow -d " + job1_dep_id + " -y --brief")


    def test_dx_run_no_hidden_executables(self):
        # hidden applet
        applet_name = "hidden_applet"
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [],
                                         "outputSpec": [],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"},
                                         "hidden": True,
                                         "name": applet_name})['id']
        run("dx describe hidden_applet")
        with self.assertSubprocessFailure(stderr_regexp='ResolutionError: Unable to resolve "{f}"'
                                          .format(f=applet_name), exit_code=3):
            run("dx run hidden_applet")
        # by ID will still work
        run("dx run " + applet_id + " -y")

        # hidden workflow
        workflow_name = "hidden_workflow"
        dxworkflow = dxpy.new_dxworkflow(name=workflow_name, hidden=True)
        dxworkflow.add_stage(applet_id)
        with self.assertSubprocessFailure(stderr_regexp='ResolutionError: Unable to resolve "{f}"'
                                          .format(f=workflow_name), exit_code=3):
            run("dx run hidden_workflow")
        # by ID will still work
        run("dx run " + dxworkflow.get_id() + " -y")

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_run_jbor_array_ref(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "name": "myapplet",
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "record",
                                                        "class": "record",
                                                        "optional": True}],
                                         "outputSpec": [{"name": "record",
                                                         "class": "record"},
                                                        {"name": "record_array",
                                                         "class": "array:record"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "bundledDepends": [],
                                                     "execDepends": [],
                                                     "code": '''
first_record=$(dx new record firstrecord --brief)
dx close $first_record
second_record=$(dx new record secondrecord --brief)
dx close $second_record
dx-jobutil-add-output record $first_record
dx-jobutil-add-output record_array $first_record --array
dx-jobutil-add-output record_array $second_record --array
'''}})["id"]

        remote_job = dxpy.DXApplet(applet_id).run({})
        remote_job.wait_on_done()
        remote_job_output = remote_job.describe()["output"]["record_array"]

        # check other dx functionality here for convenience
        # dx describe/path resolution
        jbor_array_ref = '{job_id}:record_array.'.format(job_id=remote_job.get_id())
        desc_output = run('dx describe ' + jbor_array_ref + '0')
        self.assertIn("firstrecord", desc_output)
        self.assertNotIn("secondrecord", desc_output)
        with self.assertSubprocessFailure(exit_code=3):
            run("dx get " + remote_job.get_id() + ":record.foo")
        with self.assertSubprocessFailure(stderr_regexp='not an array', exit_code=3):
            run("dx get " + remote_job.get_id() + ":record.0")
        with self.assertSubprocessFailure(stderr_regexp='out of range', exit_code=3):
            run("dx get " + jbor_array_ref + '2')

        # dx run
        second_remote_job = run('dx run myapplet -y --brief -irecord=' + jbor_array_ref + '1').strip()
        second_remote_job_desc = run('dx describe ' + second_remote_job)
        self.assertIn(jbor_array_ref + '1', second_remote_job_desc)
        self.assertIn(remote_job_output[1]["$dnanexus_link"], second_remote_job_desc)
        self.assertNotIn(remote_job_output[0]["$dnanexus_link"], second_remote_job_desc)

        # use dx get to hydrate a directory and test dx-run-app-locally
        def create_app_dir_from_applet(applet_id):
            tempdir = tempfile.mkdtemp()
            with chdir(tempdir):
                run('dx get ' + applet_id)
                return os.path.join(tempdir, dxpy.describe(applet_id)['name'])
        appdir = create_app_dir_from_applet(applet_id)
        local_output = check_output(['dx-run-app-locally', appdir, '-irecord=' + jbor_array_ref + '1'])
        self.assertIn(remote_job_output[1]["$dnanexus_link"], local_output)
        self.assertNotIn(remote_job_output[0]["$dnanexus_link"], local_output)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_run_priority(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "name": "myapplet",
                                         "dxapi": "1.0.0",
                                         "runSpec": {"interpreter": "bash",
                                                     "code": ""},
                                         "access": {"project": "VIEW",
                                                    "allProjects": "VIEW",
                                                    "network": []}})["id"]
        normal_job_id = run("dx run myapplet --priority normal --brief -y").strip()
        normal_job_desc = dxpy.describe(normal_job_id)
        self.assertEqual(normal_job_desc["priority"], "normal")

        high_priority_job_id = run("dx run myapplet --priority high --brief -y").strip()
        high_priority_job_desc = dxpy.describe(high_priority_job_id)
        self.assertEqual(high_priority_job_desc["priority"], "high")

        # don't actually need these to run
        run("dx terminate " + normal_job_id)
        run("dx terminate " + high_priority_job_id)

        # --watch implies --priority high
        try:
            run("dx run myapplet -y --watch")
        except subprocess.CalledProcessError:
            # ignore any watching errors; just want to test requested
            # priority
            pass
        watched_job_id = run("dx find jobs -n 1 --brief").strip()
        self.assertNotIn(watched_job_id, [normal_job_id, high_priority_job_id])
        watched_job_desc = dxpy.describe(watched_job_id)
        self.assertEqual(watched_job_desc['applet'], applet_id)
        self.assertEqual(watched_job_desc['priority'], 'high')

        # errors
        with self.assertSubprocessFailure(exit_code=2):
            # expect argparse error code 2 for bad choice
            run("dx run myapplet --priority standard")

        # no warning when no special access requested
        dx_run_output = run("dx run myapplet --priority normal -y")
        for string in ["WARNING", "developer", "Internet", "write access"]:
            self.assertNotIn(string, dx_run_output)

        # test for printing a warning when extra permissions are
        # requested and run as normal priority
        extra_perms_applet = dxpy.api.applet_new({"project": self.project,
                                                  "dxapi": "1.0.0",
                                                  "runSpec": {"interpreter": "bash",
                                                              "code": ""},
                                                  "access": {"developer": True,
                                                             "project": "UPLOAD",
                                                             "network": ["github.com"]}})["id"]
        # no warning when running at high priority
        dx_run_output = run("dx run " + extra_perms_applet + " --priority high -y")
        for string in ["WARNING", "developer", "Internet", "write access"]:
            self.assertNotIn(string, dx_run_output)

        # warning when running at normal priority; mention special
        # permissions present
        dx_run_output = run("dx run " + extra_perms_applet + " --priority normal -y")
        for string in ["WARNING", "developer", "Internet", "write access"]:
            self.assertIn(string, dx_run_output)

        # no warning with --brief
        dx_run_output = run("dx run " + extra_perms_applet + " --priority normal --brief -y")
        self.assertRegex(dx_run_output.strip(), '^job-[0-9a-zA-Z]{24}$')

        # test with allProjects set but no explicit permissions to the
        # project context
        extra_perms_applet = dxpy.api.applet_new({"project": self.project,
                                                  "dxapi": "1.0.0",
                                                  "inputSpec": [],
                                                  "outputSpec": [],
                                                  "runSpec": {"interpreter": "bash",
                                                              "code": ""},
                                                  "access": {"allProjects": "CONTRIBUTE"}})["id"]
        # no warning when running at high priority
        dx_run_output = run("dx run " + extra_perms_applet + " --priority high -y")
        for string in ["WARNING", "developer", "Internet", "write access"]:
            self.assertNotIn(string, dx_run_output)

        # warning when running at normal priority; mention special
        # permissions present
        dx_run_output = run("dx run " + extra_perms_applet + " --priority normal -y")
        for string in ["WARNING", "write access"]:
            self.assertIn(string, dx_run_output)
        for string in ["developer", "Internet"]:
            self.assertNotIn(string, dx_run_output)

        # workflow tests

        workflow_id = run("dx new workflow myworkflow --brief").strip()
        run("dx add stage {workflow} {applet}".format(workflow=workflow_id,
                                                      applet=extra_perms_applet))

        # no warning when run at high priority
        dx_run_output = run("dx run myworkflow --priority high -y")
        for string in ["WARNING", "developer", "Internet", "write access"]:
            self.assertNotIn(string, dx_run_output)
        # and check that priority was set properly
        analysis_id = next(dxpy.find_executions(classname='analysis', limit=1))['id']
        self.assertEqual(dxpy.describe(analysis_id)["priority"], "high")

        # get warnings when run at normal priority
        dx_run_output = run("dx run myworkflow --priority normal -y")
        for string in ["WARNING", "write access"]:
            self.assertIn(string, dx_run_output)
        for string in ["developer", "Internet"]:
            self.assertNotIn(string, dx_run_output)
        # and check that priority was set properly
        analysis_id = next(dxpy.find_executions(classname='analysis', limit=1))['id']
        self.assertEqual(dxpy.describe(analysis_id)["priority"], "normal")

    def test_dx_run_tags_and_properties(self):
        # success
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"}
                                         })['id']
        property_names = ["$my.prop", "secoиdprop", "тhird prop"]
        property_values = ["$hello.world", "Σ2,n", "stuff"]
        the_tags = ["Σ1=n", "helloo0", "ωω"]
        job_id = run("dx run " + applet_id + ' -inumber=32 --brief -y ' +
                     " ".join(["--property '" + prop[0] + "'='" + prop[1] + "'" for
                               prop in zip(property_names, property_values)]) +
                     "".join([" --tag " + tag for tag in the_tags])).strip()

        job_desc = dxpy.api.job_describe(job_id)
        self.assertEqual(job_desc['tags'].sort(), the_tags.sort())
        self.assertEqual(len(job_desc['properties']), 3)
        for name, value in zip(property_names, property_values):
            self.assertEqual(job_desc['properties'][name], value)

        # Test setting tags and properties afterwards
        run("dx tag " + job_id + " foo bar foo")
        run("dx set_properties " + job_id + " foo=bar Σ_1^n=n")
        job_desc_lines = run("dx describe " + job_id + " --delim ' '").splitlines()
        found_tags = False
        found_properties = False
        for line in job_desc_lines:
            if line.startswith('Tags'):
                self.assertIn("foo", line)
                self.assertIn("bar", line)
                found_tags = True
            if line.startswith('Properties'):
                self.assertIn("foo=bar", line)
                self.assertIn("Σ_1^n=n", line)
                found_properties = True
        self.assertTrue(found_tags)
        self.assertTrue(found_properties)
        run("dx untag " + job_id + " foo")
        run("dx unset_properties " + job_id + " Σ_1^n")
        job_desc = json.loads(run("dx describe " + job_id + " --json"))
        self.assertIn("bar", job_desc['tags'])
        self.assertNotIn("foo", job_desc['tags'])
        self.assertEqual(job_desc["properties"]["foo"], "bar")
        self.assertNotIn("Σ_1^n", job_desc["properties"])

    def test_dx_run_extra_args(self):
        # success
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"}
                                         })['id']
        job_id = run("dx run " + applet_id + " -inumber=32 --name overwritten_name " +
                     '--delay-workspace-destruction --brief -y ' +
                     '--extra-args \'{"input": {"second": true}, "name": "new_name"}\'').strip()
        job_desc = dxpy.api.job_describe(job_id)
        self.assertTrue(job_desc['delayWorkspaceDestruction'])
        self.assertEqual(job_desc['name'], 'new_name')
        self.assertIn('number', job_desc['input'])
        self.assertEqual(job_desc['input']['number'], 32)
        self.assertIn('second', job_desc['input'])
        self.assertEqual(job_desc['input']['second'], True)

        # parsing error
        with self.assertSubprocessFailure(stderr_regexp='JSON', exit_code=3):
            run("dx run " + applet_id + " --extra-args not-a-JSON-string")

    def test_dx_run_clone(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"}
                                         })['id']
        other_applet_id = dxpy.api.applet_new({"project": self.project,
                                               "dxapi": "1.0.0",
                                               "runSpec": {"interpreter": "bash",
                                                           "code": "echo 'hello'"}
                                           })['id']

        def check_new_job_metadata(new_job_desc, cloned_job_desc, overridden_fields=[]):
            '''
            :param new_job_desc: the describe hash in the new job
            :param cloned_job_desc: the description of the job that was cloned
            :param overridden_fields: the metadata fields in describe that were overridden (and should not be checked)
            '''
            # check clonedFrom hash in new job's details
            self.assertIn('clonedFrom', new_job_desc['details'])
            self.assertEqual(new_job_desc['details']['clonedFrom']['id'], cloned_job_desc['id'])
            self.assertEqual(new_job_desc['details']['clonedFrom']['executable'],
                             cloned_job_desc.get('applet') or cloned_job_desc.get('app'))
            for metadata in ['project', 'folder', 'name', 'runInput', 'systemRequirements']:
                self.assertEqual(new_job_desc['details']['clonedFrom'][metadata],
                                 cloned_job_desc[metadata])
            # check not_overridden_fields match/have the correct transformation
            all_fields = set(['name', 'project', 'folder', 'input', 'systemRequirements',
                              'applet', 'tags', 'properties', 'priority'])
            fields_to_check = all_fields.difference(overridden_fields)
            for metadata in fields_to_check:
                if metadata == 'name':
                    self.assertEqual(new_job_desc[metadata], cloned_job_desc[metadata] + ' (re-run)')
                else:
                    self.assertEqual(new_job_desc[metadata], cloned_job_desc[metadata])

        # originally, set everything and have an instance type for all
        # entry points
        orig_job_id = run("dx run " + applet_id +
                          ' -inumber=32 --name jobname --folder /output ' +
                          '--instance-type mem2_hdd2_x2 ' +
                          '--tag Ψ --tag $hello.world ' +
                          '--property Σ_1^n=n --property $hello.=world ' +
                          '--priority normal ' +
                          '--brief -y').strip()
        orig_job_desc = dxpy.api.job_describe(orig_job_id)
        # control
        self.assertEqual(orig_job_desc['name'], 'jobname')
        self.assertEqual(orig_job_desc['project'], self.project)
        self.assertEqual(orig_job_desc['folder'], '/output')
        self.assertEqual(orig_job_desc['input'], {'number': 32})
        self.assertEqual(orig_job_desc['systemRequirements'], {'*': {'instanceType': 'mem2_hdd2_x2'}})

        # clone the job

        # nothing different
        new_job_desc = dxpy.api.job_describe(run("dx run --clone " + orig_job_id +
                                                 " --brief -y").strip())
        check_new_job_metadata(new_job_desc, orig_job_desc)

        def get_new_job_desc(cmd_suffix):
            new_job_id = run("dx run --clone " + orig_job_id + " --brief -y " + cmd_suffix).strip()
            return dxpy.api.job_describe(new_job_id)

        # override applet
        new_job_desc = get_new_job_desc(other_applet_id)
        self.assertEqual(new_job_desc['applet'], other_applet_id)
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['applet'])

        # override name
        new_job_desc = get_new_job_desc("--name newname")
        self.assertEqual(new_job_desc['name'], 'newname')
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['name'])

        # override tags
        new_job_desc = get_new_job_desc("--tag new_tag --tag second_new_tag")
        self.assertEqual(new_job_desc['tags'], ['new_tag', 'second_new_tag'])
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['tags'])

        # override properties
        new_job_desc = get_new_job_desc("--property foo=bar --property baz=quux")
        self.assertEqual(new_job_desc['properties'], {"foo": "bar", "baz": "quux"})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['properties'])

        # override priority
        new_job_desc = get_new_job_desc("--priority high")
        self.assertEqual(new_job_desc['priority'], "high")
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['priority'])

        # override folder
        new_job_desc = get_new_job_desc("--folder /otherfolder")
        self.assertEqual(new_job_desc['folder'], '/otherfolder')
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['folder'])

        # override project
        new_job_desc = get_new_job_desc("--project " + self.other_proj_id)
        self.assertEqual(new_job_desc['project'], self.other_proj_id)
        self.assertEqual(new_job_desc['folder'], '/output')
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['project', 'folder'])

        # override project and folder
        new_job_desc = get_new_job_desc("--folder " + self.other_proj_id + ":")
        self.assertEqual(new_job_desc['project'], self.other_proj_id)
        self.assertEqual(new_job_desc['folder'], '/')
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['project', 'folder'])

        # override input with -i
        new_job_desc = get_new_job_desc("-inumber=42")
        self.assertEqual(new_job_desc['input'], {"number": 42})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['input'])

        # add other input fields with -i
        new_job_desc = get_new_job_desc("-inumber2=42")
        self.assertEqual(new_job_desc['input'], {"number": 32, "number2": 42})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['input'])

        # override input with --input-json (original input discarded)
        new_job_desc = get_new_job_desc("--input-json '{\"number2\": 42}'")
        self.assertEqual(new_job_desc['input'], {"number2": 42})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['input'])

        # override the blanket instance type
        new_job_desc = get_new_job_desc("--instance-type mem2_hdd2_x1")
        self.assertEqual(new_job_desc['systemRequirements'],
                         {'*': {'instanceType': 'mem2_hdd2_x1'}})
        check_new_job_metadata(new_job_desc, orig_job_desc,
                               overridden_fields=['systemRequirements'])

        # override instance type for specific entry point(s)
        new_job_desc = get_new_job_desc("--instance-type '" +
                                        json.dumps({"some_ep": "mem2_hdd2_x1",
                                                    "some_other_ep": "mem2_hdd2_x4"}) + "'")
        self.assertEqual(new_job_desc['systemRequirements'],
                         {'*': {'instanceType': 'mem2_hdd2_x2'},
                          'some_ep': {'instanceType': 'mem2_hdd2_x1'},
                          'some_other_ep': {'instanceType': 'mem2_hdd2_x4'}})
        check_new_job_metadata(new_job_desc, orig_job_desc,
                               overridden_fields=['systemRequirements'])

        # new original job with entry point-specific systemRequirements
        orig_job_id = run("dx run " + applet_id +
                          " --instance-type '{\"some_ep\": \"mem2_hdd2_x1\"}' --brief -y").strip()
        orig_job_desc = dxpy.api.job_describe(orig_job_id)
        self.assertEqual(orig_job_desc['systemRequirements'],
                         {'some_ep': {'instanceType': 'mem2_hdd2_x1'}})

        # override all entry points
        new_job_desc = get_new_job_desc("--instance-type mem2_hdd2_x2")
        self.assertEqual(new_job_desc['systemRequirements'], {'*': {'instanceType': 'mem2_hdd2_x2'}})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['systemRequirements'])

        # override a different entry point; original untouched
        new_job_desc = get_new_job_desc("--instance-type '{\"some_other_ep\": \"mem2_hdd2_x2\"}'")
        self.assertEqual(new_job_desc['systemRequirements'],
                         {'some_ep': {'instanceType': 'mem2_hdd2_x1'},
                          'some_other_ep': {'instanceType': 'mem2_hdd2_x2'}})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['systemRequirements'])

        # override the same entry point
        new_job_desc = get_new_job_desc("--instance-type '{\"some_ep\": \"mem2_hdd2_x2\"}'")
        self.assertEqual(new_job_desc['systemRequirements'],
                         {'some_ep': {'instanceType': 'mem2_hdd2_x2'}})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['systemRequirements'])

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_describe_job_with_resolved_jbors(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "array", "class": "array:int"}],
                                         "outputSpec": [{"name": "array", "class": "array:int"}],
                                         "runSpec": {"interpreter": "python2.7",
                                                     "code": '''#!/usr/bin/env python

@dxpy.entry_point('main')
def main(array):
    output = {"array": array}
    return output
'''}})['id']
        first_job_handler = dxpy.DXJob(dxpy.api.applet_run(applet_id,
                                                           {"project": self.project,
                                                            "input": {"array": [0, 1, 5]}})['id'])

        # Launch a second job which depends on the first, using two
        # arrays in an array (to be flattened) as input
        second_job_run_input = {"project": self.project,
                                "input": {"array": [first_job_handler.get_output_ref("array"),
                                                    first_job_handler.get_output_ref("array")]}}
        second_job_handler = dxpy.DXJob(dxpy.api.applet_run(applet_id, second_job_run_input)['id'])
        first_job_handler.wait_on_done()
        # Need to wait for second job to become runnable (idle and
        # waiting_on_input are the only states before it becomes
        # runnable)
        while second_job_handler.describe()['state'] in ['idle', 'waiting_on_input']:
            time.sleep(0.1)
        second_job_desc = run("dx describe " + second_job_handler.get_id())
        first_job_res = first_job_handler.get_id() + ":array => [ 0, 1, 5 ]"
        self.assertIn(first_job_res, second_job_desc)

        # Launch another job which depends on the first done job and
        # the second (not-done) job; the first job can and should be
        # mentioned in the resolved JBORs list, but the second
        # shouldn't.
        third_job_run_input = {"project": self.project,
                               "input": {"array": [first_job_handler.get_output_ref("array"),
                                                   first_job_handler.get_output_ref("array", index=2),
                                                   second_job_handler.get_output_ref("array")]}}
        third_job = dxpy.api.applet_run(applet_id, third_job_run_input)['id']
        third_job_desc = run("dx describe " + third_job)
        self.assertIn(first_job_res, third_job_desc)
        self.assertIn(first_job_handler.get_id() + ":array.2 => 5", third_job_desc)
        self.assertNotIn(second_job_handler.get_id() + ":array =>", third_job_desc)

    def test_dx_run_ssh_no_config(self):
        # Create minimal applet.
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [],
                                         "outputSpec": [],
                                         "runSpec": {"interpreter": "python2.7",
                                                     "code": '''#!/usr/bin/env python

@dxpy.entry_point('main')
def main():
    return
'''}})['id']

        # Case: Execute "dx run --ssh" before configuring SSH.
        path = tempfile.mkdtemp()
        shell = pexpect.spawn("dx run --ssh " + applet_id, env=dict(os.environ, DX_USER_CONF_DIR=path))
        shell.expect("Warning:")
        shell.sendline("N")
        shell.expect("IOError")
        shell.expect(pexpect.EOF)
        shell.wait()
        shell.close()
        self.assertEqual(3, shell.exitstatus)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, "skipping test that would run jobs")
    def test_bundledDepends_name_with_whitespaces(self):
        # upload a tar.gz file with spaces in its name
        bundle_name = "test bundle with spaces.tar.gz"
        bundle_tmp_dir = tempfile.mkdtemp()
        os.mkdir(os.path.join(bundle_tmp_dir, "a"))
        with open(os.path.join(bundle_tmp_dir, 'a', 'foo.txt'), 'w') as file_in_bundle:
            file_in_bundle.write('foo\n')
        subprocess.check_call(['tar', '-czf', os.path.join(bundle_tmp_dir, bundle_name),
                               '-C', os.path.join(bundle_tmp_dir, 'a'), '.'])
        bundle_file = dxpy.upload_local_file(filename=os.path.join(bundle_tmp_dir, bundle_name),
                                             project=self.project,
                                             wait_on_close=True)

        app_spec = {
                    "project": self.project,
                    "name": "app-bundled-depends-name-with-spaces",
                    "dxapi": "1.0.0",
                    "runSpec": {
                                "interpreter": "bash",
                                "code": "echo 'hello'",
                                "bundledDepends": [{"name": bundle_name,
                                                    "id": {"$dnanexus_link": bundle_file.get_id()}}]
                                },
                    "inputSpec": [],
                    "outputSpec": [],
                    "version": "1.0.0"
                    }
        bundle_applet_id = dxpy.api.applet_new(app_spec)["id"]
        bundle_applet = dxpy.DXApplet(bundle_applet_id)
        applet_job = bundle_applet.run({})
        applet_job.wait_on_done()
        self.assertEqual(applet_job.describe()['state'], 'done')


class TestDXClientWorkflow(DXTestCaseBuildWorkflows):
    default_inst_type = "mem2_hdd2_x2"

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    def test_dx_run_workflow(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "number", "class": "int"}],
                                         "outputSpec": [{"name": "number", "class": "int"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "exit 1"}
                                         })['id']
        workflow_id = run("dx new workflow myworkflow --output-folder /foo --brief").strip()
        stage_id = dxpy.api.workflow_add_stage(workflow_id,
                                               {"editVersion": 0, "executable": applet_id})['stage']
        analysis_id = run("dx run " + workflow_id + " -i0.number=32 -y --brief").strip()
        self.assertTrue(analysis_id.startswith('analysis-'))
        analysis_desc = run("dx describe " + analysis_id)
        self.assertIn(stage_id + '.number = 32', analysis_desc)
        self.assertIn('foo', analysis_desc)
        analysis_desc = json.loads(run("dx describe " + analysis_id + " --json"))
        time.sleep(2) # May need to wait for job to be created in the system
        job_desc = run("dx describe " + analysis_desc["stages"][0]["execution"]["id"])
        self.assertIn(' number = 32', job_desc)

        # Test setting tags and properties on analysis
        run("dx tag " + analysis_id + " foo bar foo")
        run("dx set_properties " + analysis_id + " foo=bar Σ_1^n=n")
        analysis_desc_lines = run("dx describe " + analysis_id).splitlines()
        found_tags = False
        found_properties = False
        for line in analysis_desc_lines:
            if line.startswith('Tags'):
                self.assertIn("foo", line)
                self.assertIn("bar", line)
                found_tags = True
            if line.startswith('Properties'):
                self.assertIn("foo=bar", line)
                self.assertIn("Σ_1^n=n", line)
                found_properties = True
        self.assertTrue(found_tags)
        self.assertTrue(found_properties)
        run("dx untag " + analysis_id + " foo")
        run("dx unset_properties " + analysis_id + " Σ_1^n")
        analysis_desc = run("dx describe " + analysis_id + " --delim ' '")
        self.assertIn("Tags bar\n", analysis_desc)
        self.assertIn("Properties foo=bar\n", analysis_desc)

        # Missing input throws appropriate error
        with self.assertSubprocessFailure(stderr_regexp='Some inputs.+are missing', exit_code=3):
            run("dx run " + workflow_id + " -y")

        # Setting the input in the workflow allows it to be run
        run("dx update stage " + workflow_id + " 0 -inumber=42")
        run("dx run " + workflow_id + " -y")

        # initialize a new workflow from an analysis
        new_workflow_desc = run("dx new workflow --init " + analysis_id)
        self.assertNotIn(workflow_id, new_workflow_desc)
        self.assertIn(analysis_id, new_workflow_desc)
        self.assertIn(stage_id, new_workflow_desc)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that runs jobs')
    def test_dx_run_clone_analysis(self):
        dxpy.api.applet_new({
            "project": self.project,
            "name": "myapplet",
            "dxapi": "1.0.0",
            "inputSpec": [{"name": "number", "class": "int"}],
            "outputSpec": [{"name": "number", "class": "int"}],
            "runSpec": {"interpreter": "bash",
                        "code": "dx-jobutil-add-output number 32"}
        })

        # make a workflow with the stage twice
        run("dx new workflow myworkflow")
        run("dx add stage myworkflow myapplet -inumber=32 --instance-type mem2_hdd2_x2")
        run("dx add stage myworkflow myapplet -inumber=52 --instance-type mem2_hdd2_x1")

        # run it
        analysis_id = run("dx run myworkflow -y --brief").strip()

        # test cases
        no_change_analysis_id = run("dx run --clone " + analysis_id + " --brief -y").strip()
        change_an_input_analysis_id = run("dx run --clone " + analysis_id +
                                          " -i0.number=52 --brief -y").strip()
        change_inst_type_analysis_id = run("dx run --clone " + analysis_id +
                                           " --instance-type mem2_hdd2_x2 --brief -y").strip()

        time.sleep(2) # May need to wait for any new jobs to be created in the system

        # make assertions for test cases
        orig_analysis_desc = dxpy.describe(analysis_id)

        # no change: expect both stages to have reused jobs
        no_change_analysis_desc = dxpy.describe(no_change_analysis_id)
        self.assertEqual(no_change_analysis_desc['stages'][0]['execution']['id'],
                         orig_analysis_desc['stages'][0]['execution']['id'])
        self.assertEqual(no_change_analysis_desc['stages'][1]['execution']['id'],
                         orig_analysis_desc['stages'][1]['execution']['id'])

        # change an input: new job for that stage
        change_an_input_analysis_desc = dxpy.describe(change_an_input_analysis_id)
        self.assertEqual(change_an_input_analysis_desc['stages'][0]['execution']['input'],
                         {"number": 52})
        # second stage still the same
        self.assertEqual(change_an_input_analysis_desc['stages'][1]['execution']['id'],
                         orig_analysis_desc['stages'][1]['execution']['id'])

        # change inst type: only affects stage with different inst type
        change_inst_type_analysis_desc = dxpy.describe(change_inst_type_analysis_id)
        # first stage still the same
        self.assertEqual(change_inst_type_analysis_desc['stages'][0]['execution']['id'],
                         orig_analysis_desc['stages'][0]['execution']['id'])
        # second stage different
        self.assertNotEqual(change_inst_type_analysis_desc['stages'][1]['execution']['id'],
                            orig_analysis_desc['stages'][1]['execution']['id'])
        self.assertEqual(change_inst_type_analysis_desc['stages'][1]['execution']['instanceType'],
                         'mem2_hdd2_x2')

        # Run in a different project and add some metadata
        try:
            other_proj_id = run("dx new project 'cloned analysis project' --brief").strip()
            new_analysis_id = run("dx run --clone " + analysis_id + " --destination " + other_proj_id +
                                  ":foo --tag sometag --property propkey=propval " +
                                  "--brief -y").strip()
            new_analysis_desc = dxpy.describe(new_analysis_id)
            self.assertEqual(new_analysis_desc['project'], other_proj_id)
            self.assertEqual(new_analysis_desc['folder'], '/foo')
            self.assertEqual(new_analysis_desc['tags'], ['sometag'])
            self.assertEqual(new_analysis_desc['properties'], {'propkey': 'propval'})
            time.sleep(2)
            new_job_desc = dxpy.describe(new_analysis_desc['stages'][0]['execution']['id'])
            self.assertEqual(new_job_desc['project'], other_proj_id)
            self.assertEqual(new_job_desc['input']['number'], 32)
        finally:
            run("dx rmproject -y " + other_proj_id)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that runs jobs')
    def test_dx_run_workflow_prints_cached_executions(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "name": "myapplet",
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "number", "class": "int"}],
                                         "outputSpec": [{"name": "number", "class": "int"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "dx-jobutil-add-output number 32"}
                                         })['id']
        workflow_id = run("dx new workflow myworkflow --brief").strip()
        stage_id = run("dx add stage myworkflow myapplet --brief").strip()
        run_resp = dxpy.api.workflow_run(workflow_id,
                                         {"project": self.project,
                                          "input": {(stage_id + ".number"): 32}})
        first_analysis_id = run_resp['id']
        self.assertTrue(first_analysis_id.startswith('analysis-'))
        job_id = run_resp['stages'][0]
        self.assertTrue(job_id.startswith('job-'))

        # wait for events to propagate and for the job to be created
        time.sleep(2)

        # Running the workflow again with no changes should result in
        # the job getting reused
        run_output = run("dx run " + workflow_id + " -i0.number=32 -y").strip()
        self.assertIn('will reuse results from a previous analysis', run_output)
        self.assertIn(job_id, run_output)
        second_analysis_id = run_output[run_output.rfind('analysis-'):]
        self.assertNotEqual(first_analysis_id, second_analysis_id)

        # Running the workflow again with changes to the input should
        # NOT result in the job getting reused
        run_output = run("dx run " + workflow_id + " -i0.number=52 -y").strip()
        self.assertNotIn('will reuse results from a previous analysis', run_output)
        self.assertNotIn(job_id, run_output)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that runs jobs')
    def test_dx_run_workflow_with_inst_type_requests(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "name": "myapplet",
                                         "dxapi": "1.0.0",
                                         "inputSpec": [],
                                         "outputSpec": [],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": ""}
                                         })['id']

        workflow_id = run("dx new workflow myworkflow --brief").strip()
        stage_ids = [run("dx add stage myworkflow myapplet --name 'an=awful=name' --brief").strip(),
                     run("dx add stage myworkflow myapplet --name 'second' --brief").strip()]

        # control (no request)
        no_req_id = run('dx run myworkflow -y --brief').strip()
        # request for all stages
        all_stg_req_id = run('dx run myworkflow --instance-type mem2_hdd2_x1 -y --brief').strip()

        # request for a stage specifically (by name)
        stg_req_id = run('dx run myworkflow --instance-type an=awful=name=mem2_hdd2_x2 ' +
                         '--instance-type second=mem2_hdd2_x1 -y --brief').strip()

        time.sleep(2) # give time for all jobs to be populated

        no_req_desc = dxpy.describe(no_req_id)
        self.assertEqual(no_req_desc['stages'][0]['execution']['instanceType'],
                         self.default_inst_type)
        self.assertEqual(no_req_desc['stages'][1]['execution']['instanceType'],
                         self.default_inst_type)
        all_stg_req_desc = dxpy.describe(all_stg_req_id)
        self.assertEqual(all_stg_req_desc['stages'][0]['execution']['instanceType'],
                         'mem2_hdd2_x1')
        self.assertEqual(all_stg_req_desc['stages'][1]['execution']['instanceType'],
                         'mem2_hdd2_x1')
        stg_req_desc = dxpy.describe(stg_req_id)
        self.assertEqual(stg_req_desc['stages'][0]['execution']['instanceType'],
                         'mem2_hdd2_x2')
        self.assertEqual(stg_req_desc['stages'][1]['execution']['instanceType'],
                         'mem2_hdd2_x1')

        # request for a stage specifically (by index); if same inst
        # type as before, should reuse results
        self.assertIn(stg_req_desc['stages'][0]['execution']['id'],
                      run('dx run myworkflow --instance-type 0=mem2_hdd2_x2 -y'))
        # and by stage ID
        self.assertIn(stg_req_desc['stages'][0]['execution']['id'],
                      run('dx run myworkflow --instance-type ' + stage_ids[0] + '=mem2_hdd2_x2 -y'))

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would attempt to run a job')
    def test_dx_run_workflow_with_stage_folders(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "name": "myapplet",
                                         "dxapi": "1.0.0",
                                         "inputSpec": [],
                                         "outputSpec": [],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": ""}
                                         })['id']
        workflow_id = run("dx new workflow myworkflow --brief").strip()
        stage_ids = [run("dx add stage myworkflow myapplet --name 'a_simple_name' " +
                         "--output-folder /foo --brief").strip(),
                     run("dx add stage myworkflow myapplet --name 'second' " +
                         "--relative-output-folder foo --brief").strip()]

        cmd = 'dx run myworkflow --folder /output -y --brief --rerun-stage "*" '

        # control (no runtime request for stage folders)
        no_req_id = run(cmd).strip()
        # request for all stages
        all_stg_folder_id = run(cmd + '--stage-output-folder "*" bar').strip()
        all_stg_rel_folder_id = run(cmd + '--stage-relative-output-folder "*" /bar').strip()
        # request for stage specifically (by name)
        per_stg_folders_id = run(cmd + '--stage-relative-output-folder a_simple_name /baz ' + # as "baz"
                                 '--stage-output-folder second baz').strip() # resolves as ./baz
        # request for stage specifically (by index)
        per_stg_folders_id_2 = run(cmd + '--stage-output-folder 1 quux ' +
                                   '--stage-relative-output-folder 0 /quux').strip()
        # only modify one
        per_stg_folders_id_3 = run(cmd + '--stage-output-folder ' + stage_ids[0] + ' /hello').strip()

        time.sleep(2) # give time for all jobs to be generated

        def expect_stage_folders(analysis_id, first_stage_folder, second_stage_folder):
            analysis_desc = dxpy.describe(analysis_id)
            self.assertEqual(analysis_desc['stages'][0]['execution']['folder'],
                             first_stage_folder)
            self.assertEqual(analysis_desc['stages'][1]['execution']['folder'],
                             second_stage_folder)

        expect_stage_folders(no_req_id, '/foo', '/output/foo')
        expect_stage_folders(all_stg_folder_id, '/bar', '/bar')
        expect_stage_folders(all_stg_rel_folder_id, '/output/bar', '/output/bar')
        expect_stage_folders(per_stg_folders_id, '/output/baz', '/baz')
        expect_stage_folders(per_stg_folders_id_2, '/output/quux', '/quux')
        expect_stage_folders(per_stg_folders_id_3, '/hello', '/output/foo')

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would attempt to run a job')
    def test_inaccessible_stage(self):
        applet_id = dxpy.api.applet_new({"name": "myapplet",
                                         "project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "number", "class": "int"}],
                                         "outputSpec": [{"name": "number", "class": "int"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "exit 1"}
                                         })['id']
        workflow_id = run("dx new workflow myworkflow --brief").strip()
        run("dx add stage myworkflow myapplet")
        run("dx rm myapplet")

        # describe shows it
        desc = run("dx describe myworkflow")
        self.assertIn("inaccessible", desc)

        # list stages shows it
        list_output = run("dx list stages myworkflow")
        self.assertIn("inaccessible", list_output)

        # run refuses to run it
        with self.assertSubprocessFailure(stderr_regexp='following inaccessible stage\(s\)',
                                          exit_code=3):
            run("dx run myworkflow")

    @unittest.skipUnless(testutil.TEST_ENV, 'skipping test that would clobber your local environment')
    def test_dx_new_workflow_without_context(self):
        # Without project context, cannot create new object without
        # project qualified path
        with without_project_context():
            with self.assertSubprocessFailure(stderr_regexp='expected the path to be qualified with a project',
                                              exit_code=3):
                run("dx new workflow foo")
            # Can create object with explicit project qualifier
            workflow_id = run("dx new workflow --brief " + self.project + ":foo").strip()
            self.assertEqual(dxpy.DXWorkflow(workflow_id).name, "foo")

    def test_dx_new_workflow(self):
        workflow_id = run("dx new workflow --title=тitle --summary=SΨmmary --brief " +
                          "--description=DΣsc wØrkflØwname --output-folder /wØrkflØwØutput").strip()
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc["id"], workflow_id)
        self.assertEqual(desc["editVersion"], 0)
        self.assertEqual(desc["name"], "wØrkflØwname")
        self.assertEqual(desc["title"], "тitle")
        self.assertEqual(desc["summary"], "SΨmmary")
        self.assertEqual(desc["description"], "DΣsc")
        self.assertEqual(desc["outputFolder"], "/wØrkflØwØutput")
        self.assertEqual(desc["project"], self.project)

        # add some stages and then create a new one initializing from
        # the first
        applet_id = dxpy.api.applet_new({"name": "myapplet",
                                         "project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [],
                                         "outputSpec": [],
                                         "runSpec": {"interpreter": "bash", "code": ""}
                                         })['id']
        run("dx add stage wØrkflØwname " + applet_id)

        new_workflow_id = run("dx new workflow --init wØrkflØwname --title newtitle " +
                              "--summary newsummary --output-folder /output --brief").strip()
        desc = dxpy.describe(new_workflow_id)
        self.assertNotEqual(new_workflow_id, workflow_id)
        self.assertEqual(desc["id"], new_workflow_id)
        self.assertEqual(desc["editVersion"], 0)
        self.assertEqual(desc["name"], "wØrkflØwname")
        self.assertEqual(desc["title"], "newtitle")
        self.assertEqual(desc["summary"], "newsummary")
        self.assertEqual(desc["description"], "DΣsc")
        self.assertEqual(desc["outputFolder"], "/output")
        self.assertEqual(desc["project"], self.project)
        self.assertEqual(len(desc["stages"]), 1)
        self.assertEqual(desc["stages"][0]["executable"], applet_id)

        # run without --brief; should see initializedFrom information
        new_workflow_desc = run("dx new workflow --init " + workflow_id)
        self.assertIn(workflow_id, new_workflow_desc)

        # error when initializing from a nonexistent workflow
        run("dx rm " + workflow_id)
        with self.assertSubprocessFailure(stderr_regexp='could not be found', exit_code=3):
            run("dx new workflow --init " + workflow_id)

    def test_dx_workflow_resolution(self):
        with self.assertSubprocessFailure(stderr_regexp='Unable to resolve', exit_code=3):
            run("dx update workflow foo")

        record_id = run("dx new record --type pipeline --brief").strip()
        run("dx describe " + record_id)
        with self.assertSubprocessFailure(stderr_regexp='Could not resolve', exit_code=3):
            run("dx update workflow " + record_id)

    def test_dx_describe_workflow(self):
        workflow_id = run("dx new workflow myworkflow --title title --brief").strip()
        desc = run("dx describe " + workflow_id)
        self.assertIn("Input Spec", desc)
        self.assertIn("Output Spec", desc)
        applet_id = dxpy.api.applet_new({"name": "myapplet",
                                         "project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "number", "class": "int"}],
                                         "outputSpec": [{"name": "number", "class": "int"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "exit 0"}
                                         })['id']
        first_stage = run("dx add stage " + workflow_id + " -inumber=10 " + applet_id +
                          " --brief").strip()
        desc = run("dx describe myworkflow")
        self.assertIn("Input Spec", desc)
        self.assertIn("default=10", desc)

    def test_dx_add_remove_list_stages(self):
        workflow_id = run("dx new workflow myworkflow --title title --brief").strip()
        run("dx describe " + workflow_id)
        applet_id = dxpy.api.applet_new({"name": "myapplet",
                                         "project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "number", "class": "int"}],
                                         "outputSpec": [{"name": "number", "class": "int"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "exit 0"}
                                         })['id']
        stage_ids = []

        # list stages when there are no stages yet
        list_output = run("dx list stages myworkflow")
        self.assertIn("No stages", list_output)

        stage_ids.append(run("dx add stage " + workflow_id + " --name first " + applet_id +
                             " --brief").strip())
        # not-yet-existing folder path should work
        # also, set input and instance type
        stage_ids.append(run("dx add stage myworkflow --relative-output-folder output myapplet " +
                             "--brief -inumber=32 --instance-type mem2_hdd2_x2").strip())
        # test relative folder path
        run("dx mkdir -p a/b/c")
        cd("a/b/c")
        stage_ids.append(run("dx add stage " + workflow_id + " --name second --output-folder . " +
                             applet_id +
                             " --brief --instance-type '{\"main\": \"mem2_hdd2_x2\"}'").strip())
        with self.assertSubprocessFailure(stderr_regexp='not found in the input spec', exit_code=3):
            # input spec should be checked
            run("dx add stage " + workflow_id + " " + applet_id + " -inonexistent=42")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(len(desc['stages']), len(stage_ids))
        for i, stage_id in enumerate(stage_ids):
            self.assertEqual(desc['stages'][i]['id'], stage_id)
        self.assertEqual(desc['stages'][0]['folder'], None)
        self.assertEqual(desc['stages'][1]['folder'], 'output')
        self.assertEqual(desc['stages'][1]['input']['number'], 32)
        self.assertEqual(desc['stages'][1]['systemRequirements'],
                         {"*": {"instanceType": "mem2_hdd2_x2"}})
        self.assertEqual(desc['stages'][2]['folder'], '/a/b/c')
        self.assertEqual(desc['stages'][2]['systemRequirements'],
                         {"main": {"instanceType": "mem2_hdd2_x2"}})

        # errors
        # when adding a stage with both absolute and relative output folders
        with self.assertSubprocessFailure(stderr_regexp="output-folder", exit_code=2):
            run("dx add stage " + workflow_id + " " + applet_id +
                " --output-folder /foo --relative-output-folder foo")
        # bad executable that can't be found
        with self.assertSubprocessFailure(stderr_regexp="ResolutionError", exit_code=3):
            run("dx add stage " + workflow_id + " foo")
        # bad input
        with self.assertSubprocessFailure(stderr_regexp="parsed", exit_code=3):
            run("dx add stage " + workflow_id + " -inumber=foo " + applet_id)
        # bad instance type arg
        with self.assertSubprocessFailure(stderr_regexp="instance-type", exit_code=3):
            run("dx add stage " + workflow_id + " " + applet_id + " --instance-type {]")
        # unrecognized instance typ
        with self.assertSubprocessFailure(stderr_regexp="InvalidInput", exit_code=3):
            run("dx add stage " + workflow_id + " " + applet_id + " --instance-type foo")

        # list stages
        list_output = run("dx list stages " + workflow_id)
        self.assertIn("myworkflow (" + workflow_id + ")", list_output)
        self.assertIn("Title: title", list_output)
        self.assertIn("Output Folder: -", list_output)
        for i in range(0, len(stage_ids)):
            self.assertIn("Stage " + str(i), list_output)
        self.assertIn("<workflow output folder>/output", list_output)
        self.assertIn("number=32", list_output)
        self.assertIn("/a/b/c", list_output)

        run("dx describe " + workflow_id)
        # remove a stage by index
        remove_output = run("dx remove stage /myworkflow 1")
        self.assertIn(stage_ids[1], remove_output)
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(len(desc['stages']), 2)
        self.assertEqual(desc['stages'][0]['id'], stage_ids[0])
        self.assertEqual(desc['stages'][0]['folder'], None)
        self.assertEqual(desc['stages'][1]['id'], stage_ids[2])
        self.assertEqual(desc['stages'][1]['folder'], '/a/b/c')

        # remove a stage by ID
        remove_output = run("dx remove stage " + workflow_id + " " + stage_ids[0] + ' --brief').strip()
        self.assertEqual(remove_output, stage_ids[0])
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(len(desc['stages']), 1)
        self.assertEqual(desc['stages'][0]['id'], stage_ids[2])
        self.assertEqual(desc['stages'][0]['name'], 'second')
        self.assertEqual(desc['stages'][0]['folder'], '/a/b/c')

        # remove a stage by name
        run("dx remove stage " + workflow_id + " second")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(len(desc['stages']), 0)

        # remove something out of range
        with self.assertSubprocessFailure(stderr_regexp="out of range", exit_code=3):
            run("dx remove stage /myworkflow 5")

        # remove some bad stage ID
        with self.assertSubprocessFailure(
                stderr_regexp="could not be found as a stage ID nor as a stage name",
                exit_code=3):
            run("dx remove stage /myworkflow badstageID")

        # remove nonexistent stage
        with self.assertSubprocessFailure(stderr_regexp="DXError", exit_code=3):
            run("dx remove stage /myworkflow stage-123456789012345678901234")

    def test_dx_update_workflow(self):
        workflow_id = run("dx new workflow myworkflow --brief").strip()
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc['editVersion'], 0)
        self.assertEqual(desc['title'], "myworkflow")
        self.assertIsNone(desc["outputFolder"])

        # set title, summary, description, outputFolder
        run("dx update workflow myworkflow --title тitle --summary SΨmmary --description=DΣsc " +
            "--output-folder .")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc['editVersion'], 1)
        self.assertEqual(desc['title'], "тitle")
        self.assertEqual(desc['summary'], "SΨmmary")
        self.assertEqual(desc['description'], "DΣsc")
        self.assertEqual(desc['outputFolder'], "/")

        # describe
        describe_output = run("dx describe myworkflow --delim ' '")
        self.assertIn("Output Folder /", describe_output)

        # unset title, outputFolder
        run("dx update workflow myworkflow --no-title --no-output-folder")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc['editVersion'], 2)
        self.assertEqual(desc['title'], "myworkflow")
        self.assertIsNone(desc['outputFolder'])

        # describe
        describe_output = run("dx describe myworkflow --delim ' '")
        self.assertNotIn("Title тitle", describe_output)
        self.assertIn("Summary SΨmmary", describe_output)
        self.assertNotIn("Description", describe_output)
        self.assertNotIn("DΣsc", describe_output)
        self.assertIn("Output Folder -", describe_output)
        describe_output = run("dx describe myworkflow --verbose --delim ' '")
        self.assertIn("Description DΣsc", describe_output)

        # no-op
        output = run("dx update workflow myworkflow")
        self.assertIn("No updates requested", output)
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc['editVersion'], 2)
        self.assertEqual(desc['title'], "myworkflow")

        with self.assertSubprocessFailure(stderr_regexp="no-title", exit_code=2):
            run("dx update workflow myworkflow --title foo --no-title")
        with self.assertSubprocessFailure(stderr_regexp="no-title", exit_code=2):
            run("dx update workflow myworkflow --output-folder /foo --no-output-folder")

    def test_dx_update_stage(self):
        workflow_id = run("dx new workflow myworkflow --brief").strip()
        run("dx describe " + workflow_id)
        applet_id = dxpy.api.applet_new({"name": "myapplet",
                                         "project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "number", "class": "int"}],
                                         "outputSpec": [{"name": "number", "class": "int"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "exit 0"}
                                         })['id']
        stage_id = run("dx add stage " + workflow_id + " " + applet_id + " --brief").strip()
        empty_applet_id = dxpy.api.applet_new({"name": "emptyapplet",
                                               "project": self.project,
                                               "dxapi": "1.0.0",
                                               "inputSpec": [],
                                               "outputSpec": [],
                                               "runSpec": {"interpreter": "bash",
                                                           "code": "exit 0"}
                                           })['id']

        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertIsNone(desc["stages"][0]["name"])
        self.assertEqual(desc["stages"][0]["folder"], None)
        self.assertEqual(desc["stages"][0]["input"], {})
        self.assertEqual(desc["stages"][0]["systemRequirements"], {})

        # set the name, folder, some input, and the instance type
        run("dx update stage myworkflow 0 --name тitle -inumber=32 --relative-output-folder=foo " +
            "--instance-type mem2_hdd2_x2")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc["editVersion"], 2)
        self.assertEqual(desc["stages"][0]["name"], "тitle")
        self.assertEqual(desc["stages"][0]["folder"], "foo")
        self.assertEqual(desc["stages"][0]["input"]["number"], 32)
        self.assertEqual(desc["stages"][0]["systemRequirements"],
                         {"*": {"instanceType": "mem2_hdd2_x2"}})

        # use a relative folder path and also set instance type using JSON
        run("dx update stage myworkflow 0 --name тitle -inumber=32 --output-folder=. " +
            "--instance-type '{\"main\": \"mem2_hdd2_x2\"}'")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc["editVersion"], 3)
        self.assertEqual(desc["stages"][0]["folder"], "/")
        self.assertEqual(desc["stages"][0]["systemRequirements"],
                         {"main": {"instanceType": "mem2_hdd2_x2"}})

        # unset name
        run("dx update stage myworkflow " + stage_id + " --no-name")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc["editVersion"], 4)
        self.assertIsNone(desc["stages"][0]["name"])

        # set incompatible executable; expect a helpful error msg
        # telling us to use --force; then use it
        with self.assertSubprocessFailure(stderr_regexp="--force", exit_code=3):
            run("dx update stage myworkflow 0 --executable " + empty_applet_id)
        run("dx update stage myworkflow 0 --force --executable " + empty_applet_id)
        run("dx rm " + empty_applet_id)
        desc_string = run("dx describe myworkflow")
        run("dx update stage myworkflow 0 --force --executable " + applet_id)

        # some errors
        with self.assertSubprocessFailure(stderr_regexp="no-name", exit_code=2):
            run("dx update stage myworkflow 0 --name foo --no-name")
        with self.assertSubprocessFailure(stderr_regexp="output-folder", exit_code=2):
            run("dx update stage myworkflow 0 --output-folder /foo --relative-output-folder foo")
        with self.assertSubprocessFailure(stderr_regexp="parsed", exit_code=3):
            run("dx update stage myworkflow 0 -inumber=foo")
        with self.assertSubprocessFailure(stderr_regexp="ResolutionError", exit_code=3):
            run("dx update stage myworkflow 0 --executable foo")
        with self.assertSubprocessFailure(stderr_regexp="instance-type", exit_code=3):
            run("dx update stage myworkflow 0 --instance-type {]")

        # no-op
        output = run("dx update stage myworkflow 0 --alias default --force")
        self.assertIn("No updates requested", output)

        # update something out of range
        with self.assertSubprocessFailure(stderr_regexp="out of range", exit_code=3):
            run("dx update stage /myworkflow 5 --name foo")

        # remove some bad stage ID
        with self.assertSubprocessFailure(
                stderr_regexp="could not be found as a stage ID nor as a stage name",
                exit_code=3):
            run("dx update stage /myworkflow bad.stageID --name foo")

        # remove nonexistent stage
        with self.assertSubprocessFailure(stderr_regexp="DXError", exit_code=3):
            run("dx update stage /myworkflow stage-123456789012345678901234 --name foo")

    def test_dx_build_workflow(self):
        applet_id = dxpy.api.applet_new({"name": "my_first_applet",
                                         "project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "number", "class": "int"}],
                                         "outputSpec": [{"name": "number", "class": "int"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "exit 0"}
                                         })['id']

        stage0 = {"id": "stage_0",
                        "name": "stage_0_name",
                        "executable": applet_id,
                        "input": {"number": 123456},
                        "folder": "/stage_0_output",
                        "executionPolicy": {"restartOn": {}, "onNonRestartableFailure": "failStage"},
                        "systemRequirements": {"main": {"instanceType": self.default_inst_type}}}
        stage1 = {"id": "stage_1",
                        "executable": applet_id,
                        "input": {"number": {"$dnanexus_link": {"stage": "stage_0",
                                                                "outputField": "number"}}}}

        workflow_spec = {"name": "my_workflow",
                        "outputFolder": "/",
                        "stages": [stage0, stage1]}

        workflow_dir = self.write_workflow_directory("dxbuilt_workflow",
                                                     json.dumps(workflow_spec),
                                                     readme_content="Workflow Readme")

        new_workflow = json.loads(run("dx build --json " + workflow_dir))
        wf_describe = dxpy.get_handler(new_workflow["id"]).describe()
        self.assertEqual(wf_describe["class"], "workflow")
        self.assertEqual(wf_describe["id"], new_workflow["id"])
        self.assertEqual(wf_describe["editVersion"], 0)
        self.assertEqual(wf_describe["name"], "my_workflow")
        self.assertEqual(wf_describe["state"], "closed")
        self.assertEqual(wf_describe["outputFolder"], "/")
        self.assertEqual(wf_describe["project"], self.project)
        self.assertEqual(wf_describe["description"], "Workflow Readme")
        self.assertEqual(len(wf_describe["stages"]), 2)
        self.assertEqual(wf_describe["stages"][0]["id"], "stage_0")
        self.assertEqual(wf_describe["stages"][0]["name"], "stage_0_name")
        self.assertEqual(wf_describe["stages"][0]["executable"], applet_id)
        self.assertEqual(wf_describe["stages"][0]["executionPolicy"]["restartOn"], {})
        self.assertEqual(wf_describe["stages"][0]["executionPolicy"]["onNonRestartableFailure"],
            "failStage")
        self.assertEqual(wf_describe["stages"][0]["systemRequirements"]["main"]["instanceType"],
            self.default_inst_type)
        self.assertEqual(wf_describe["stages"][1]["id"], "stage_1")
        self.assertIsNone(wf_describe["stages"][1]["name"])
        self.assertEqual(wf_describe["stages"][1]["executable"], applet_id)

    def test_dx_build_workflow_with_destination(self):
        workflow_spec = {"name": "my_workflow"}
        workflow_dir = self.write_workflow_directory("dxbuilt_workflow",
                                                     json.dumps(workflow_spec))
        # PROJECT
        new_workflow = json.loads(run("dx build --json --destination {dest} {src_dir}".format(
                                      dest=self.project, src_dir=workflow_dir)))
        wf_describe = dxpy.get_handler(new_workflow["id"]).describe()
        self.assertEqual(wf_describe["id"], new_workflow["id"])
        self.assertEqual(wf_describe["project"], self.project)
        self.assertEqual(wf_describe["folder"], "/")
        self.assertEqual(wf_describe["name"], "my_workflow")

        # /ENTITYNAME
        destination = "/{entityname}".format(entityname="overriding_wf_name")
        new_workflow = json.loads(run("dx build --json -d {dest} {src_dir}".format(
                                      dest=destination, src_dir=workflow_dir)))
        wf_describe = dxpy.get_handler(new_workflow["id"]).describe()
        self.assertEqual(wf_describe["id"], new_workflow["id"])
        self.assertEqual(wf_describe["project"], self.project)
        self.assertEqual(wf_describe["folder"], "/")
        self.assertEqual(wf_describe["name"], "overriding_wf_name")

        # /FOLDER/
        dest_folder = "/foo"
        create_folder_in_project(self.project, dest_folder)
        destination = "{folder}/".format(folder=dest_folder)
        new_workflow = json.loads(run("dx build --json --destination {dest} {src_dir}".format(
                                      dest=destination, src_dir=workflow_dir)))
        wf_describe = dxpy.get_handler(new_workflow["id"]).describe()
        self.assertEqual(wf_describe["id"], new_workflow["id"])
        self.assertEqual(wf_describe["project"], self.project)
        self.assertEqual(wf_describe["folder"], dest_folder)
        self.assertEqual(wf_describe["name"], "my_workflow")

        # PROJECT:/FOLDER/ENTITYNAME
        dest_folder = "/wf_dest_folder"
        dest_name = "overriding_wf_name"
        create_folder_in_project(self.project, dest_folder)
        destination = "{project}:{folder}/{entityname}".format(project=self.project,
                                                                folder=dest_folder,
                                                                entityname=dest_name)
        new_workflow = json.loads(run("dx build --json --destination {dest} {src_dir}".format(
                                      dest=destination, src_dir=workflow_dir)))
        wf_describe = dxpy.get_handler(new_workflow["id"]).describe()
        self.assertEqual(wf_describe["id"], new_workflow["id"])
        self.assertEqual(wf_describe["project"], self.project)
        self.assertEqual(wf_describe["folder"], dest_folder)
        self.assertEqual(wf_describe["name"], dest_name)

        # Error: No such folder
        dest_folder = "/no_such_folder"
        destination = "{project}:{folder}/".format(project=self.project, folder=dest_folder)
        with self.assertSubprocessFailure(stderr_regexp="ResourceNotFound", exit_code=3):
            run("dx build --json --destination {dest} {src_dir}".format(dest=destination, src_dir=workflow_dir))

        # Error: Project not specified
        with without_project_context():
            with self.assertSubprocessFailure(stderr_regexp='expected the path to be qualified with a project',
                                              exit_code=3):
                new_workflow = run("dx build --json {src_dir}".format(src_dir=workflow_dir))

    def test_dx_build_get_build_workflow(self):
        # When building and getting a workflow multiple times we should
        # obtain functionally identical workflows, ie. identical dxworkflow.json specs.
        workflow_name = "orig_workflow_name"
        applet_id = dxpy.api.applet_new({"name": "my_first_applet",
                                         "project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "number", "class": "int"}],
                                         "outputSpec": [{"name": "number", "class": "int"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "exit 0"}
                                         })['id']
        stage0 = {"id": "stage_0",
                        "name": "stage_0_name",
                        "executable": applet_id,
                        "input": {"number": 123456},
                        "folder": "/stage_0_output"}
        stage1 = {"id": "stage_1",
                        "executable": applet_id,
                        "input": {"number": {"$dnanexus_link": {"stage": "stage_0",
                                                                "outputField": "number"}}}}
        workflow_spec = {
            "name": workflow_name,
            "outputFolder": "/",
            "stages": [stage0, stage1]
        }

        # 1. Build
        workflow_dir = self.write_workflow_directory("workflow_cycle",
                                                     json.dumps(workflow_spec),
                                                     readme_content="Workflow Cycle Readme")
        workflow_01 = json.loads(run("dx build --json " + workflow_dir))
        wf_describe_01 = dxpy.get_handler(workflow_01["id"]).describe()
        self.assertEqual(wf_describe_01["id"], workflow_01["id"])

        # 2. Get and compare with the initial workflow
        with chdir(tempfile.mkdtemp()):
            run("dx get {workflow_id}".format(workflow_id=workflow_01["id"]))
            self.assertTrue(os.path.exists(os.path.join(workflow_name, "dxworkflow.json")))
            self.assertTrue(os.path.exists(os.path.join(workflow_name, "Readme.md")))
            workflow_metadata = open(os.path.join(workflow_name, "dxworkflow.json")).read()
            output_json = json.loads(workflow_metadata, object_pairs_hook=collections.OrderedDict)
            self.assertEqual(output_json, workflow_spec)

            # 3. Build again and compare with the initial workflow
            os.chdir(workflow_name) # move to the directory created with dx get
            workflow_02 = json.loads(run("dx build --json"))
            wf_describe_02 = dxpy.get_handler(workflow_02["id"]).describe()
            self.assertEqual(wf_describe_02["class"], "workflow")
            self.assertEqual(wf_describe_02["id"], workflow_02["id"])
            self.assertEqual(wf_describe_02["editVersion"], 0)
            self.assertEqual(wf_describe_02["name"], workflow_name)
            self.assertEqual(wf_describe_02["state"], "closed")
            self.assertEqual(wf_describe_02["outputFolder"], "/")
            self.assertEqual(wf_describe_02["project"], self.project)
            self.assertEqual(wf_describe_02["description"], "Workflow Cycle Readme")
            self.assertEqual(len(wf_describe_02["stages"]), 2)
            self.assertEqual(wf_describe_02["stages"][0]["id"], "stage_0")
            self.assertEqual(wf_describe_02["stages"][0]["name"], "stage_0_name")
            self.assertEqual(wf_describe_02["stages"][0]["executable"], applet_id)
            self.assertEqual(wf_describe_02["stages"][0]["input"]["number"], 123456)
            self.assertEqual(wf_describe_02["stages"][1]["id"], "stage_1")
            self.assertIsNone(wf_describe_02["stages"][1]["name"])
            self.assertEqual(wf_describe_02["stages"][1]["executable"], applet_id)

    def test_build_worklow_malformed_dxworkflow_json(self):
        workflow_dir = self.write_workflow_directory("dxbuilt_workflow", "{")
        with self.assertSubprocessFailure(stderr_regexp='Could not parse dxworkflow\.json file', exit_code=3):
            run("dx build " + workflow_dir)

    # def test_build_worklow_warnings(self):
    #     applet_id = dxpy.api.applet_new({"name": "my_first_applet",
    #                                      "project": self.project,
    #                                      "dxapi": "1.0.0",
    #                                      "inputSpec": [],
    #                                      "outputSpec": [],
    #                                      "runSpec": {"interpreter": "bash",
    #                                                  "code": "exit 0"}
    #                                      })['id']
    #     #TODO: finish this test

class TestDXClientFind(DXTestCase):

    def assert_cmd_gives_ids(self, cmd, ids):
        self.assertEqual(set(execid.strip() for execid in run(cmd).splitlines()),
                         set(ids))

    def test_dx_find_apps(self):
        # simple test here does not assume anything about apps that do
        # or do not exist
        from dxpy.app_categories import APP_CATEGORIES
        category_help = run("dx find apps --category-help")
        for category in APP_CATEGORIES:
            self.assertIn(category, category_help)
        run("dx find apps --category foo") # any category can be searched

    def test_dx_find_data_formatted(self):
        record_id = dxpy.new_dxrecord(project=self.project, name="find_data_formatting", close=True).get_id()
        self.assertRegexpMatches(
            run("dx find data --name " + "find_data_formatting").strip(),
            r"^closed\s+\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+/find_data_formatting \(" + record_id + "\)$"
        )

    def test_dx_find_data_by_name(self):
        record_id = dxpy.new_dxrecord(name="find_data_by_name").get_id()
        self.assertEqual(run("dx find data --brief --name " + "find_data_by_name").strip(),
                         self.project + ':' + record_id)

    def test_dx_find_data_by_class(self):
        ids = {"record": run("dx new record --brief").strip(),
               "workflow": run("dx new workflow --brief").strip(),
               "file": run("echo foo | dx upload - --brief").strip()}

        for classname in ids:
            self.assertEqual(run("dx find data --brief --class " + classname).strip(),
                             self.project + ':' + ids[classname])

    def test_dx_find_data_by_tag(self):
        record_ids = [run("dx new record --brief --tag Ψ --tag foo --tag baz").strip(),
                      run("dx new record --brief --tag Ψ --tag foo --tag bar").strip()]

        found_records = run("dx find data --tag baz --brief").strip()
        self.assertEqual(found_records, dxpy.WORKSPACE_ID + ':' + record_ids[0])

        found_records = run("dx find data --tag Ψ --tag foo --tag foobar --brief").strip()
        self.assertEqual(found_records, '')

        found_records = run("dx find data --tag foo --tag Ψ --brief").strip().split("\n")
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[0], found_records)
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[1], found_records)

    def test_dx_find_data_by_property(self):
        record_ids = [run("dx new record --brief " +
                          "--property Ψ=world --property foo=bar --property bar=").strip(),
                      run("dx new record --brief --property Ψ=notworld --property foo=bar").strip()]

        found_records = run("dx find data --property Ψ=world --property foo=bar --brief").strip()
        self.assertEqual(found_records, dxpy.WORKSPACE_ID + ':' + record_ids[0])

        # presence
        found_records = run("dx find data --property Ψ --brief").strip().split("\n")
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[0], found_records)
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[1], found_records)

        found_records = run("dx find data --property Ψ --property foo=baz --brief").strip()
        self.assertEqual(found_records, '')

        found_records = run("dx find data --property Ψ --property foo=bar --brief").strip().split("\n")
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[0], found_records)
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[1], found_records)

        # Empty string values should be okay
        found_records = run("dx find data --property bar= --brief").strip()
        self.assertEqual(found_records, dxpy.WORKSPACE_ID + ':' + record_ids[0])

        # Errors parsing --property value
        with self.assertSubprocessFailure(stderr_regexp='nonempty strings', exit_code=3):
            run("dx find data --property ''")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx find data --property foo=bar=baz")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx find data --property =foo=bar=")
        # Property keys must be nonempty
        with self.assertSubprocessFailure(stderr_regexp='nonempty strings', exit_code=3):
            run("dx find data --property =bar")

    def test_dx_find_data_by_scope(self):
        # Name of temporary project to use in test cases.
        test_projectname = 'Test-Project-PTFM-7023'

        # Tests for deprecated --project flag.

        # Case: --project specified.
        test_dirname = '/test-folder-PTFM-7023-01'
        test_recordname = '/test-record-01'
        with temporary_project(test_projectname) as temp_project:
            test_projectid = temp_project.get_id()
            record_id = run('dx new record -p --brief ' + test_projectid + ':' + test_dirname +
                            test_recordname).strip()
            found_record_id = run('dx find data --brief --project ' + test_projectid).strip()
            self.assertEqual(found_record_id, test_projectid + ':' + record_id)

        # Tests for deprecated --folder flag.

        # Case: --folder specified, WORKSPACE_ID set.
        test_dirname = '/test-folder-PTFM-7023-02'
        test_subdirname = '/test-subfolder'
        test_recordname = '/test-record-02'
        record_ids = [run('dx new record -p --brief ' + test_dirname + test_recordname).strip(),
                      run('dx new record -p --brief ' + test_dirname + test_subdirname + test_recordname).strip()]
        found_record_ids = run('dx find data --brief --folder ' + test_dirname).strip().split('\n')
        self.assertEqual(set(dxpy.WORKSPACE_ID + ':' + record_id for record_id in record_ids), set(found_record_ids))

        # Case: --folder and --project specified.
        test_dirname = '/test-folder-PTFM-7023-03'
        test_recordname = '/test-record-03'
        with temporary_project(test_projectname) as temp_project:
            test_projectid = temp_project.get_id()
            record_id = run('dx new record -p --brief ' + test_projectid + ':' + test_dirname +
                            test_recordname).strip()
            found_record_id = run('dx find data --brief --project ' + test_projectid + ' --folder ' +
                                  test_dirname).strip()
            self.assertEqual(found_record_id, test_projectid + ':' + record_id)

        # Case: --folder and --norecurse specified, WORKSPACE_ID set.
        test_dirname = '/test-folder-PTFM-7023-04'
        test_subdirname = '/test-subfolder'
        test_recordname = '/test-record-04'
        record_id = run('dx new record -p --brief ' + test_dirname + test_recordname).strip()
        run('dx new record -p --brief ' + test_dirname + test_subdirname + test_recordname)
        found_record_id = run('dx find data --brief --folder ' + test_dirname + ' --norecurse').strip()
        self.assertEqual(found_record_id, dxpy.WORKSPACE_ID + ':' + record_id)

        # Case: --folder, --project, and --norecurse specified.
        test_dirname = '/test-folder-PTFM-7023-05'
        test_subdirname = '/test-subfolder'
        test_recordname = '/test-record-05'
        with temporary_project(test_projectname) as temp_project:
            test_projectid = temp_project.get_id()
            record_id = run('dx new record -p --brief ' + test_projectid + ':' + test_dirname +
                            test_recordname).strip()
            run('dx new record -p --brief ' + test_projectid + ':' + test_dirname + test_subdirname + test_recordname)
            found_record_id = run('dx find data --brief --project ' + test_projectid + ' --folder ' +
                                  test_dirname + ' --norecurse').strip()
            self.assertEqual(found_record_id, test_projectid + ':' + record_id)

        # Tests for --path flag.

        # Case: --path specified, WORKSPACE_ID set.
        test_dirname = '/test-folder-PTFM-7023-06'
        test_subdirname = '/test-subfolder'
        test_recordname = '/test-record-06'
        run('dx new record -p --brief ' + test_recordname)
        record_ids = [run('dx new record -p --brief ' + test_dirname + test_recordname).strip(),
                      run('dx new record -p --brief ' + test_dirname + test_subdirname + test_recordname).strip()]
        found_record_ids = run('dx find data --brief --path ' + test_dirname).strip().split('\n')
        self.assertEqual(set(dxpy.WORKSPACE_ID + ':' + record_id for record_id in record_ids), set(found_record_ids))

        # Case: --path and --project specified.
        test_dirname = '/test-folder-PTFM-7023-07'
        test_recordname = '/test-record-07'
        with temporary_project(test_projectname) as temp_project:
            test_projectid = temp_project.get_id()
            run('dx new record -p --brief ' + test_recordname)
            record_id = run('dx new record -p --brief ' + test_projectid + ':' + test_dirname +
                            test_recordname).strip()
            found_record_id = run('dx find data --brief --project ' + test_projectid + ' --path ' +
                                  test_dirname).strip()
            self.assertEqual(found_record_id, test_projectid + ':' + record_id)

        # Case: --path and --norecurse specified, WORKSPACE_ID set.
        test_dirname = '/test-folder-PTFM-7023-08'
        test_subdirname = '/test-subfolder'
        test_recordname = '/test-record-08'
        record_id = run('dx new record -p --brief ' + test_dirname + test_recordname).strip()
        run('dx new record -p --brief ' + test_dirname + test_subdirname + test_recordname)
        found_record_id = run('dx find data --brief --path ' + test_dirname + ' --norecurse').strip()
        self.assertEqual(found_record_id, dxpy.WORKSPACE_ID + ':' + record_id)

        # Case: --path, --project, and --norecurse specified.
        test_dirname = '/test-folder-PTFM-7023-09'
        test_subdirname = '/test-subfolder'
        test_recordname = '/test-record-09'
        with temporary_project(test_projectname) as temp_project:
            test_projectid = temp_project.get_id()
            record_id = run('dx new record -p --brief ' + test_projectid + ':' + test_dirname +
                            test_recordname).strip()
            run('dx new record -p --brief ' + test_projectid + ':' + test_dirname + test_subdirname + test_recordname)
            found_record_id = run('dx find data --brief --project ' + test_projectid + ' --path ' +
                                  test_dirname + ' --norecurse').strip()
            self.assertEqual(found_record_id, test_projectid + ':' + record_id)

        # Case: --path specified as PROJECTID:FOLDERPATH.
        test_dirname = '/test-folder-PTFM-7023-10'
        test_recordname = '/test-record-10'
        with temporary_project(test_projectname) as temp_project:
            test_projectid = temp_project.get_id()
            record_ids = [run('dx new record -p --brief ' + test_projectid + ':' + test_dirname +
                              test_recordname).strip(),
                          run('dx new record -p --brief ' + test_projectid + ':' + test_dirname +
                              test_subdirname + test_recordname).strip()]

            # Case: --norecurse not specified.
            found_record_id = run('dx find data --brief --path ' + test_projectid + ':' +
                                  test_dirname).strip().split('\n')
            self.assertEqual(set(found_record_id), set(test_projectid + ':' + record_id for record_id in record_ids))

            # Case: --norecurse specified.
            found_record_id = run('dx find data --brief --path ' + test_projectid + ':' + test_dirname +
                                  ' --norecurse').strip()
            self.assertEqual(found_record_id, test_projectid + ':' + record_ids[0])

        # Case: --path specified as relative path, WORKSPACE_ID set.
        test_dirname = '/test-folder-PTFM-7023-12'
        test_subdirname = '/test-subfolder'
        test_recordname = '/test-record-12'
        run('dx new record -p --brief ' + test_recordname)
        record_id = run('dx new record -p --brief ' + test_dirname + test_subdirname + test_recordname).strip()
        cd(test_dirname)
        found_record_id = run('dx find data --brief --path ' + test_subdirname[1:]).strip()
        self.assertEqual(found_record_id, dxpy.WORKSPACE_ID + ':' + record_id)

        run('dx clearenv')
        test_dirname = '/test-folder-PTFM-7023-14'
        test_recordname = '/test-record-14'
        with temporary_project(test_projectname) as temp_project, select_project(None):
            test_projectid = temp_project.get_id()
            run('dx new record -p --brief ' + test_projectid + ':' + test_dirname + test_recordname)

            # FIXME: the following test is flaky because we're not able
            # to effectively unset the project using
            # select_project(None). This merely unsets the environment
            # variable, which doesn't work because it just allows the
            # previous value of the project context (e.g. obtained from
            # the user-global config) to bleed through. Therefore,
            # although we run 'clearenv' above, another process can
            # swoop in and set a project which is then seen in the
            # subprocess call below-- contrary to our intentions. (Given
            # this, the current implementation of select_project(None)
            # may be completely faulty to begin with.)
            #
            # In order to really make this test work, we need to be able
            # to encode (in the environment variable or in the config
            # file) an empty project in such a way that it sticks.
            #
            # # Case: --path specified, WORKSPACE_ID not set (fail).
            # with self.assertSubprocessFailure(stderr_regexp="if a project is not specified", exit_code=1):
            #     run('dx find data --brief --path ' + test_dirname)

            # Case: --project and --path PROJECTID:FOLDERPATH specified (fail).
            with self.assertSubprocessFailure(stderr_regexp="Cannot supply both --project and --path " +
                                              "PROJECTID:FOLDERPATH", exit_code=3):
                run('dx find data --brief --project ' + test_projectid + ' --path ' + test_projectid + ':' +
                    test_dirname)

            # Case: --folder and --path specified (fail).
            with self.assertSubprocessFailure(stderr_regexp="Cannot supply both --folder and --path", exit_code=3):
                run('dx find data --brief --folder ' + test_projectid + ':' + test_dirname + ' --path ' +
                    test_projectid + ':' + test_dirname)

    def test_dx_find_projects(self):
        unique_project_name = 'dx find projects test ' + str(time.time())
        with temporary_project(unique_project_name) as unique_project:
            self.assertEqual(run("dx find projects --name " + pipes.quote(unique_project_name)),
                             unique_project.get_id() + ' : ' + unique_project_name + ' (ADMINISTER)\n')
            self.assertEqual(run("dx find projects --brief --name " + pipes.quote(unique_project_name)),
                             unique_project.get_id() + '\n')
            json_output = json.loads(run("dx find projects --json --name " + pipes.quote(unique_project_name)))
            self.assertEqual(len(json_output), 1)
            self.assertEqual(json_output[0]['id'], unique_project.get_id())

    def test_dx_find_projects_by_created(self):
        created_project_name = 'dx find projects test ' + str(time.time())
        with temporary_project(created_project_name) as unique_project:
            self.assertEqual(run("dx find projects --created-after=-1d --brief --name " +
                             pipes.quote(created_project_name)), unique_project.get_id() + '\n')
            self.assertEqual(run("dx find projects --created-before=" + str(int(time.time() + 1000) * 1000) +
                             " --brief --name " + pipes.quote(created_project_name)),
                             unique_project.get_id() + '\n')
            self.assertEqual(run("dx find projects --created-after=-1d --created-before=" +
                             str(int(time.time() + 1000) * 1000) + " --brief --name " +
                             pipes.quote(created_project_name)), unique_project.get_id() + '\n')
            self.assertEqual(run("dx find projects --created-after=" + str(int(time.time() + 1000) * 1000) + " --name "
                             + pipes.quote(created_project_name)), "")

    def test_dx_find_projects_by_tag(self):
        other_project_id = run("dx new project other --brief").strip()
        try:
            run("dx tag : Ψ world")
            proj_desc = dxpy.describe(dxpy.WORKSPACE_ID)
            self.assertEqual(len(proj_desc["tags"]), 2)
            self.assertIn("Ψ", proj_desc["tags"])
            self.assertIn("world", proj_desc["tags"])

            found_projects = run("dx find projects --tag Ψ --tag world --brief").strip().split('\n')
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertNotIn(other_project_id, found_projects)

            found_projects = run("dx find projects --tag Ψ --tag world --tag foobar --brief").strip().split('\n')
            self.assertNotIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertNotIn(other_project_id, found_projects)

            run("dx tag " + other_project_id + " Ψ world foobar")
            found_projects = run("dx find projects --tag world --tag Ψ --brief").strip().split("\n")
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertIn(other_project_id, found_projects)
        except:
            raise
        finally:
            run("dx rmproject -y " + other_project_id)

    def test_dx_find_projects_by_property(self):
        other_project_id = run("dx new project other --brief").strip()
        try:
            run("dx set_properties : Ψ=world foo=bar bar=")
            proj_desc = dxpy.api.project_describe(dxpy.WORKSPACE_ID, {"properties": True})
            self.assertEqual(len(proj_desc["properties"]), 3)
            self.assertEqual(proj_desc["properties"]["Ψ"], "world")
            self.assertEqual(proj_desc["properties"]["foo"], "bar")
            self.assertEqual(proj_desc["properties"]["bar"], "")

            run("dx set_properties " + other_project_id + " Ψ=notworld foo=bar")

            found_projects = run("dx find projects --property Ψ=world --property foo=bar --brief").strip().split("\n")
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertNotIn(other_project_id, found_projects)

            found_projects = run("dx find projects --property bar= --brief").strip().split('\n')
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertNotIn(other_project_id, found_projects)

            # presence
            found_projects = run("dx find projects --property Ψ --brief").strip().split("\n")
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertIn(other_project_id, found_projects)

            found_projects = run("dx find projects --property Ψ --property foo=baz --brief").strip().split("\n")
            self.assertNotIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertNotIn(other_project_id, found_projects)

            found_projects = run("dx find projects --property Ψ --property foo=bar --brief").strip().split("\n")
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertIn(other_project_id, found_projects)
        except:
            raise
        finally:
            run("dx rmproject -y " + other_project_id)

        # Errors parsing --property value
        with self.assertSubprocessFailure(stderr_regexp='nonempty strings', exit_code=3):
            run("dx find projects --property ''")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx find projects --property foo=bar=baz")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx find projects --property =foo=bar=")
        # Property keys must be nonempty
        with self.assertSubprocessFailure(stderr_regexp='nonempty strings', exit_code=3):
            run("dx find projects --property =bar")
        # Empty string values should be okay
        run("dx find projects --property bar=")

    def test_dx_find_projects_phi(self):
        projectName = "tempProject+{t}".format(t=time.time())
        with temporary_project(name=projectName) as project_1:
            res = run('dx find projects --phi true --brief --name ' + pipes.quote(projectName))
            self.assertTrue(len(res) == 0, "Expected no PHI projects to be found")

            res = run('dx find projects --phi false --brief --name ' + pipes.quote(projectName)).strip().split('\n')
            self.assertTrue(len(res) == 1, "Expected to find one project")
            self.assertTrue(res[0] == project_1.get_id())

            # --phi must contain one argument.
            with self.assertSubprocessFailure(stderr_regexp='expected one argument', exit_code=2):
                run('dx find projects --phi')

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_find_jobs_by_tags_and_properties(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"}
                                         })['id']
        property_names = ["$my.prop", "secoиdprop", "тhird prop"]
        property_values = ["$hello.world", "Σ2,n", "stuff"]
        the_tags = ["Σ1=n", "helloo0", "ωω"]
        job_id = run("dx run " + applet_id + ' -inumber=32 --brief -y ' +
                     " ".join(["--property '" + prop[0] + "'='" + prop[1] + "'" for prop in zip(property_names, property_values)]) +
                     "".join([" --tag " + tag for tag in the_tags])).strip()

        # matches
        self.assertEqual(run("dx find jobs --brief --tag " + the_tags[0]).strip(), job_id)
        self.assertEqual(run("dx find jobs --brief" + "".join([" --tag " + tag for tag in the_tags])).strip(),
                         job_id)
        self.assertEqual(run("dx find jobs --brief --property " + property_names[1]).strip(), job_id)
        self.assertEqual(run("dx find jobs --brief --property '" +
                             property_names[1] + "'='" + property_values[1] + "'").strip(),
                         job_id)
        self.assertEqual(run("dx find jobs --brief" +
                             "".join([" --property '" + key + "'='" + value + "'" for
                                       key, value in zip(property_names, property_values)])).strip(),
                         job_id)

        # no matches
        self.assertEqual(run("dx find jobs --brief --tag foo").strip(), "")
        self.assertEqual(run("dx find jobs --brief --property foo").strip(), "")
        self.assertEqual(run("dx find jobs --brief --property '" +
                             property_names[1] + "'=badvalue").strip(), "")

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping test that would run a job')
    def test_find_executions(self):
        dxapplet = dxpy.DXApplet()
        dxapplet.new(name="test_applet",
                     dxapi="1.0.0",
                     inputSpec=[{"name": "chromosomes", "class": "record"},
                                {"name": "rowFetchChunk", "class": "int"}
                                ],
                     outputSpec=[{"name": "mappings", "class": "record"}],
                     runSpec={"code": "def main(): pass",
                              "interpreter": "python2.7",
                              "execDepends": [{"name": "python-numpy"}]})
        dxrecord = dxpy.new_dxrecord()
        dxrecord.close()
        prog_input = {"chromosomes": {"$dnanexus_link": dxrecord.get_id()},
                      "rowFetchChunk": 100}
        dxworkflow = dxpy.new_dxworkflow(name='find_executions test workflow')
        stage = dxworkflow.add_stage(dxapplet, stage_input=prog_input)
        dxanalysis = dxworkflow.run({stage+".rowFetchChunk": 200},
                                    tags=["foo"],
                                    properties={"foo": "bar"})
        dxapplet.run(applet_input=prog_input)
        dxjob = dxapplet.run(applet_input=prog_input,
                             tags=["foo", "bar"],
                             properties={"foo": "baz"})

        cd("{project_id}:/".format(project_id=dxapplet.get_proj_id()))

        # Wait for job to be created
        executions = [stage['execution']['id'] for stage in dxanalysis.describe()['stages']]
        t = 0
        while len(executions) > 0:
            try:
                dxpy.api.job_describe(executions[len(executions) - 1], {})
                executions.pop()
            except DXAPIError:
                t += 1
                if t > 20:
                    raise Exception("Timeout while waiting for job to be created for an analysis stage")
                time.sleep(1)

        options = "--user=self"
        self.assertEqual(len(run("dx find executions "+options).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs "+options).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses "+options).splitlines()), 2)
        options += " --project="+dxapplet.get_proj_id()
        self.assertEqual(len(run("dx find executions "+options).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs "+options).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses "+options).splitlines()), 2)
        options += " --created-after=-150s --no-subjobs --applet="+dxapplet.get_id()
        self.assertEqual(len(run("dx find executions "+options).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs "+options).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses "+options).splitlines()), 2)
        options2 = options + " --brief -n 9000"
        self.assertEqual(len(run("dx find executions "+options2).splitlines()), 4)
        self.assertEqual(len(run("dx find jobs "+options2).splitlines()), 3)
        self.assertEqual(len(run("dx find analyses "+options2).splitlines()), 1)
        options3 = options2 + " --origin="+dxjob.get_id()
        self.assertEqual(len(run("dx find executions "+options3).splitlines()), 1)
        self.assertEqual(len(run("dx find jobs "+options3).splitlines()), 1)
        self.assertEqual(len(run("dx find analyses "+options3).splitlines()), 0)
        options3 = options2 + " --root="+dxanalysis.get_id()
        self.assertEqual(len(run("dx find executions "+options3).splitlines()), 2)
        self.assertEqual(len(run("dx find jobs "+options3).splitlines()), 1)
        self.assertEqual(len(run("dx find analyses "+options3).splitlines()), 1)
        options2 = options + " --origin-jobs"
        self.assertEqual(len(run("dx find executions "+options2).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs "+options2).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses "+options2).splitlines()), 2)
        options2 = options + " --origin-jobs -n 9000"
        self.assertEqual(len(run("dx find executions "+options2).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs "+options2).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses "+options2).splitlines()), 2)
        options2 = options + " --all-jobs"
        self.assertEqual(len(run("dx find executions "+options2).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs "+options2).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses "+options2).splitlines()), 2)
        options2 = options + " --state=done"
        self.assertEqual(len(run("dx find executions "+options2).splitlines()), 0)
        self.assertEqual(len(run("dx find jobs "+options2).splitlines()), 0)
        self.assertEqual(len(run("dx find analyses "+options2).splitlines()), 0)

        # Search by tag
        options2 = options + " --all-jobs --brief"
        options3 = options2 + " --tag foo"
        analysis_id = dxanalysis.get_id()
        job_id = dxjob.get_id()
        self.assert_cmd_gives_ids("dx find executions "+options3, [analysis_id, job_id])
        self.assert_cmd_gives_ids("dx find jobs "+options3, [job_id])
        self.assert_cmd_gives_ids("dx find analyses "+options3, [analysis_id])
        options3 = options2 + " --tag foo --tag bar"
        self.assert_cmd_gives_ids("dx find executions "+options3, [job_id])
        self.assert_cmd_gives_ids("dx find jobs "+options3, [job_id])
        self.assert_cmd_gives_ids("dx find analyses "+options3, [])

        # Search by property (presence and by value)
        options3 = options2 + " --property foo"
        self.assert_cmd_gives_ids("dx find executions "+options3, [analysis_id, job_id])
        self.assert_cmd_gives_ids("dx find jobs "+options3, [job_id])
        self.assert_cmd_gives_ids("dx find analyses "+options3, [analysis_id])
        options3 = options2 + " --property foo=baz"
        self.assert_cmd_gives_ids("dx find executions "+options3, [job_id])
        self.assert_cmd_gives_ids("dx find jobs "+options3, [job_id])
        self.assert_cmd_gives_ids("dx find analyses "+options3, [])

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping test that would run a job')
    def test_find_analyses_run_by_jobs(self):
        project_name = "tempProject+{t}".format(t=time.time())
        with temporary_project(name=project_name) as temp_proj:
            temp_proj_id = temp_proj.get_id()
            dxsubapplet = dxpy.DXApplet(project=temp_proj_id)
            dxapplet = dxpy.DXApplet(project=temp_proj_id)

            dxsubapplet.new(name="sub_applet",
                            dxapi="1.0.0",
                            inputSpec=[],
                            outputSpec=[],
                            runSpec={"code": "sleep 1200",
                                     "interpreter": "bash",
                                     "execDepends": [{"name": "dx-toolkit"}]},
                            project=temp_proj_id)

            dxworkflow = dxpy.new_dxworkflow(name='test_workflow', project=temp_proj_id)
            dxworkflow.add_stage(dxsubapplet, stage_input={})

            dxapplet.new(name="workflow_runner",
                         dxapi="1.0.0",
                         inputSpec=[],
                         outputSpec=[],
                         runSpec={"code": "dx run " + dxworkflow.get_id() + " --project " + temp_proj_id,
                                  "interpreter": "bash",
                                  "execDepends": [{"name": "dx-toolkit"}]},
                         project=temp_proj_id)

            job_id = dxapplet.run(applet_input={}, project=temp_proj_id).get_id()
            workflow_id = dxworkflow.get_id()
            jobapplet_id = dxapplet.get_id()

            cd("{project_id}:/".format(project_id=dxapplet.get_proj_id()))

            # Wait for analysis to be created
            t = 0
            while True:
                try:
                    analysis_id = dxpy.api.job_describe(job_id, {})['dependsOn'][0]
                    break
                except IndexError:
                    t += 1
                    if t > 300:
                        raise Exception("Timeout while waiting for workflow to be run by root execution")
                    time.sleep(1)

            # Wait for subjob to be run by analysis
            subjob_id = dxpy.api.analysis_describe(analysis_id, {})['stages'][0]['execution']['id']
            t = 0
            while True:
                try:
                    dxpy.api.job_describe(subjob_id, {})
                    break
                except DXAPIError:
                    t += 1
                    if t > 20:
                        raise Exception("Timeout while waiting for job to be created for an analysis stage")
                    time.sleep(1)

            options = "--brief --user=self --project="+temp_proj_id
            self.assert_cmd_gives_ids("dx find executions "+options, [job_id, analysis_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs "+options, [job_id, subjob_id])
            self.assert_cmd_gives_ids("dx find analyses "+options, [analysis_id])
            options2 = options + " --applet="+workflow_id
            self.assert_cmd_gives_ids("dx find executions "+options2, [job_id, analysis_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs "+options2, [])
            self.assert_cmd_gives_ids("dx find analyses "+options2, [analysis_id])
            options2 = options + " --applet="+jobapplet_id
            self.assert_cmd_gives_ids("dx find executions "+options2, [job_id, analysis_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs "+options2, [job_id])
            self.assert_cmd_gives_ids("dx find analyses "+options2, [])
            options2 = options + " -n 9000"
            self.assert_cmd_gives_ids("dx find executions "+options2, [job_id, analysis_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs "+options2, [job_id, subjob_id])
            self.assert_cmd_gives_ids("dx find analyses "+options2, [analysis_id])
            options3 = options2 + " --origin="+job_id
            self.assert_cmd_gives_ids("dx find executions "+options3, [job_id, analysis_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs "+options3, [job_id])
            self.assert_cmd_gives_ids("dx find analyses "+options3, [analysis_id])
            options2 = options + " --origin-jobs"
            self.assert_cmd_gives_ids("dx find executions "+options2, [job_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs "+options2, [job_id, subjob_id])
            self.assert_cmd_gives_ids("dx find analyses "+options2, [])
            options2 = options + " --origin-jobs -n 9000"
            self.assert_cmd_gives_ids("dx find executions "+options2, [job_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs "+options2, [job_id, subjob_id])
            self.assert_cmd_gives_ids("dx find analyses "+options2, [])
            options2 = options + " --all-jobs"
            self.assert_cmd_gives_ids("dx find executions "+options2, [job_id, analysis_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs "+options2, [job_id, subjob_id])
            self.assert_cmd_gives_ids("dx find analyses "+options2, [analysis_id])
            options2 = options + " --state=done"
            self.assert_cmd_gives_ids("dx find executions "+options2, [])
            self.assert_cmd_gives_ids("dx find jobs "+options2, [])
            self.assert_cmd_gives_ids("dx find analyses "+options2, [])

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that requires presence of test org')
    def test_find_orgs(self):
        org_with_billable_activities = "org-members_with_billing_rights"
        self.assertTrue(dxpy.api.org_describe(org_with_billable_activities)["allowBillableActivities"])
        org_without_billable_activities = "org-members_without_billing_rights"
        self.assertFalse(dxpy.api.org_describe(org_without_billable_activities)["allowBillableActivities"])
        org_with_admin = "org-piratelabs"
        self.assertTrue(dxpy.api.org_describe(org_with_admin)["level"] == "ADMIN")

        cmd = "dx find orgs --level {l} {o} --json"

        results = json.loads(run(cmd.format(l="MEMBER", o="")).strip())
        self.assertItemsEqual([org_with_billable_activities,
                               org_without_billable_activities,
                               org_with_admin],
                              [result["id"] for result in results])

        results = json.loads(run(cmd.format(
            l="MEMBER", o="--with-billable-activities")).strip())
        self.assertItemsEqual([org_with_billable_activities,
                               org_with_admin],
                              [result["id"] for result in results])

        results = json.loads(run(cmd.format(
            l="MEMBER", o="--without-billable-activities")).strip())
        self.assertItemsEqual([org_without_billable_activities],
                              [result["id"] for result in results])

        results = json.loads(run(cmd.format(l="ADMIN", o="")).strip())
        self.assertItemsEqual([org_with_admin],
                              [result["id"] for result in results])

        results = json.loads(run(cmd.format(
            l="ADMIN", o="--with-billable-activities")).strip())
        self.assertItemsEqual([org_with_admin],
                              [result["id"] for result in results])

        results = json.loads(run(cmd.format(
            l="ADMIN", o="--without-billable-activities")).strip())
        self.assertItemsEqual([], [result["id"] for result in results])

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that requires presence of test org')
    def test_find_orgs_format(self):
        cmd = "dx find orgs --level MEMBER {o}"

        # Assert that only org ids are returned, line-separated.
        results = run(cmd.format(o="--brief")).strip().split("\n")
        pattern = re.compile("^org-[a-zA-Z0-9_]*$")
        for result in results:
            self.assertTrue(pattern.match(result))

        # Assert that the return format is like: "<org_id><delim><org_name>"
        results = run(cmd.format(o="")).strip().split("\n")
        pattern = re.compile("^org-[a-zA-Z0-9_]* : .*$")
        for result in results:
            self.assertTrue(pattern.match(result))

        results = run(cmd.format(o="--delim ' @ '")).strip().split("\n")
        pattern = re.compile("^org-[a-zA-Z0-9_]* @ .*$")
        for result in results:
            self.assertTrue(pattern.match(result))


@unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that requires presence of test org, project, and user')
class TestDXClientFindInOrg(DXTestCaseBuildApps):
    @classmethod
    def setUpClass(cls):
        cls.org_id = "org-piratelabs"
        cls.user_alice = "user-alice"  # ADMIN
        cls.user_bob = "user-bob"
        dxpy.api.org_invite(cls.org_id, {"invitee": cls.user_bob})  # Invite user_bob as MEMEBER of org-piratelabs
        cls.project_ppb = "project-0000000000000000000000pb"  # public project in "org-piratelabs"

    @classmethod
    def tearDownClass(cls):
        dxpy.api.org_remove_member(cls.org_id, {"user": cls.user_bob})

    def test_dx_find_org_members_negative(self):
        # No org id
        with self.assertSubprocessFailure(stderr_regexp='dx find org members: error: too few arguments', exit_code=2):
            run("dx find org members")

        # No input to --level
        with self.assertSubprocessFailure(stderr_regexp='error: argument --level: expected one argument', exit_code=2):
            run("dx find org members org-piratelabs --level")

    def test_dx_find_org_members(self):
        org_members = [self.user_alice, self.user_bob]  # sorted ascending by user ID
        org_members.sort()

        # Basic test to check consistency of client output to directly invoking API
        output = run("dx find org members org-piratelabs --brief").strip().split("\n")
        dx_api_output = dxpy.api.org_find_members(self.org_id)
        self.assertEqual(output, [member['id'] for member in dx_api_output['results']])
        self.assertEqual(output, org_members)

        # With --level flag
        output = run("dx find org members org-piratelabs --level {l} --brief".format(l="ADMIN")).strip().split("\n")
        self.assertItemsEqual(output, [self.user_alice])

        output = run("dx find org members org-piratelabs --level {l} --brief".format(l="MEMBER")).strip().split("\n")
        self.assertItemsEqual(output, [self.user_bob])

    def test_dx_find_org_members_format(self):
        cmd = "dx find org members org-piratelabs {opts}"

        # Assert that only member ids are returned, line-separated
        output = run(cmd.format(opts="--brief")).strip().split("\n")
        pattern = "^user-[a-zA-Z0-9]*$"
        for result in output:
            self.assertRegex(result, pattern)

        # Assert that return format is like: "<user_id> : <user_name> (<level>)"
        levels = "(?:ADMIN|MEMBER)"
        output = run(cmd.format(opts="")).strip().split("\n")
        pattern = "^user-[a-zA-Z0-9]* : .* \(" + levels + "\)$"
        for result in output:
            self.assertRegex(result, pattern)

        # Test --json output
        # TODO: Deprecate 'createProjectsAndApps'
        output = json.loads(run(cmd.format(opts='--json')))
        query_user_describe = {"fields": {"class": True, "first": True, "last": True, "middle": True, "handle": True}}
        expected = [{"appAccess": True,
                     "projectAccess": "ADMINISTER",
                     "level": "ADMIN",
                     "createProjectsAndApps": True,
                     "allowBillableActivities": True,
                     "id": self.user_alice,
                     "describe": dxpy.api.user_describe(self.user_alice, query_user_describe)},
                    {"appAccess": True,
                     "projectAccess": "CONTRIBUTE",
                     "createProjectsAndApps": False,
                     "allowBillableActivities": False,
                     "level": "MEMBER",
                     "id": self.user_bob,
                     "describe": dxpy.api.user_describe(self.user_bob, query_user_describe)}]
        self.assertEqual(output, expected)

    def test_dx_find_org_projects_invalid(self):
        cmd = "dx find org projects org-irrelevant {opts}"

        # --ids must contain at least one id.
        with self.assertSubprocessFailure(stderr_regexp='expected at least one argument', exit_code=2):
            run(cmd.format(opts="--ids"))

        # --tag must contain at least one tag.
        with self.assertSubprocessFailure(stderr_regexp='expected one argument', exit_code=2):
            run(cmd.format(opts="--tag"))

        # --property must contain at least one property.
        with self.assertSubprocessFailure(stderr_regexp='expected one argument', exit_code=2):
            run(cmd.format(opts="--property"))

        # Only one of --public-only and --private-only may be specified.
        with self.assertSubprocessFailure(stderr_regexp='not allowed with argument', exit_code=2):
            run(cmd.format(opts="--public-only --private-only"))

        # --phi must contain one argument.
        with self.assertSubprocessFailure(stderr_regexp='expected one argument', exit_code=2):
            run(cmd.format(opts="--phi"))

    def test_dx_find_org_projects(self):
        with temporary_project() as project_1, temporary_project() as project_2:
            project1_id = project_1.get_id()
            project2_id = project_2.get_id()  # project not billed to org
            org_projects = [self.project_ppb, project1_id]

            dxpy.api.project_update(project1_id, {"billTo": self.org_id})
            self.assertEqual(dxpy.api.project_describe(project1_id)['billTo'], self.org_id)

            # Basic test to check consistency of client output to directly invoking API
            output = run("dx find org projects org-piratelabs --brief").strip().split("\n")
            dx_api_output = dxpy.api.org_find_projects(self.org_id)
            self.assertEqual(output, [result['id'] for result in dx_api_output['results']])
            self.assertItemsEqual(output, org_projects)

            # With --ids flag
            output = run("dx find org projects org-piratelabs --ids {p}".format(p=project2_id)).strip().split("\n")
            self.assertItemsEqual(output, [''])

            output = run("dx find org projects org-piratelabs --ids {p} --brief".format(
                         p=project1_id)).strip().split("\n")
            self.assertItemsEqual(output, [project1_id])

            output = run("dx find org projects org-piratelabs --ids {p1} {p2} --brief".format(p1=project1_id,
                         p2=project2_id)).strip().split("\n")
            self.assertItemsEqual(output, [project1_id])

            # With --tag
            dxpy.api.project_add_tags(project1_id, {'tags': ['tag-1', 'tag-2']})
            dxpy.api.project_add_tags(project2_id, {'tags': ['tag-1', 'tag-2']})
            output = run("dx find org projects org-piratelabs --tag {t1} --brief".format(
                         t1='tag-1')).strip().split("\n")
            self.assertEqual(output, [project1_id])

            # With multiple --tag
            output = run("dx find org projects org-piratelabs --tag {t1} --tag {t2} --brief".format(t1='tag-1',
                         t2='tag-2')).strip().split("\n")
            self.assertEqual(output, [project1_id])

            output = run("dx find org projects org-piratelabs --tag {t1} --tag {t2} --brief".format(t1='tag-1',
                         t2='tag-3')).strip().split("\n")
            self.assertEqual(output, [""])

            # With --property
            dxpy.api.project_set_properties(project1_id, {'properties': {'property-1': 'value1', 'property-2':
                                                          'value2'}})
            dxpy.api.project_set_properties(project2_id, {'properties': {'property-1': 'value1', 'property-2':
                                                          'value2'}})
            output = run("dx find org projects org-piratelabs --property {p1} --brief".format(
                         p1='property-1')).strip().split("\n")
            self.assertItemsEqual(output, [project1_id])

            # With multiple --property
            output = run("dx find org projects org-piratelabs --property {p1} --property {p2} --brief".format(
                         p1='property-1', p2='property-2')).strip().split("\n")
            self.assertItemsEqual(output, [project1_id])

            output = run("dx find org projects org-piratelabs --property {p1} --property {p2} --brief".format(
                         p1='property-1', p2='property-3')).strip().split("\n")
            self.assertItemsEqual(output, [""])

    def test_dx_find_org_projects_public(self):
        with temporary_project() as p1, temporary_project() as p2:
            # Private project in `org_id`.
            private_project_id = p1.get_id()
            dxpy.api.project_update(private_project_id, {"billTo": self.org_id})

            # Assert that `p2` exists.
            self.assertEqual(dxpy.api.project_describe(p2.get_id(), {})["level"], "ADMINISTER")

            cmd = "dx find org projects org-piratelabs {opts} --brief"

            output = run(cmd.format(opts="")).strip().split("\n")
            self.assertItemsEqual(output, [private_project_id, self.project_ppb])

            output = run(cmd.format(opts="--public-only")).strip().split("\n")
            self.assertItemsEqual(output, [self.project_ppb])

            output = run(cmd.format(opts="--private-only")).strip().split("\n")
            self.assertItemsEqual(output, [private_project_id])

    def test_dx_find_org_projects_created(self):
        with temporary_project() as unique_project:
            project_id = unique_project.get_id()
            org_projects = [self.project_ppb, project_id]
            dxpy.api.project_update(project_id, {"billTo": self.org_id})

            created = dxpy.api.project_describe(project_id)['created']

            # Test integer time stamp
            self.assertItemsEqual(run("dx find org projects org-piratelabs --created-before={cb} --brief".format(
                                  cb=str(created + 1000))).strip().split("\n"), org_projects)

            self.assertItemsEqual(run("dx find org projects org-piratelabs --created-after={ca} --brief".format(
                                  ca=str(created - 1000))).strip().split("\n"), [project_id])

            self.assertItemsEqual(run("dx find org projects org-piratelabs --created-after={ca} --created-before={cb} --brief".format(
                                  ca=str(created - 1000), cb=str(created + 1000))).strip().split("\n"), [project_id])

            self.assertItemsEqual(run("dx find org projects org-piratelabs --created-before={cb} --brief".format(
                                  cb=str(created - 1000))).strip().split("\n"), [self.project_ppb])

            # Test integer with suffix
            self.assertItemsEqual(run("dx find org projects org-piratelabs --created-before={cb} --brief".format(
                                  cb="-1d")).strip().split("\n"), [self.project_ppb])

            self.assertItemsEqual(run("dx find org projects org-piratelabs --created-after={ca} --brief".format(
                                  ca="-1d")).strip().split("\n"), [project_id])

            # Test date
            self.assertItemsEqual(run("dx find org projects org-piratelabs --created-before={cb} --brief".format(
                                  cb="2015-10-28")).strip().split("\n"), [self.project_ppb])

            self.assertItemsEqual(run("dx find org projects org-piratelabs --created-after={ca} --brief".format(
                                  ca="2015-10-28")).strip().split("\n"), [project_id])

    def test_dx_find_org_projects_format(self):
        cmd = "dx find org projects org-piratelabs {opts}"

        # Assert that only project ids are returned, line-separated
        output = run(cmd.format(opts="--brief")).strip().split("\n")
        pattern = "^project-[a-zA-Z0-9]{24}$"
        for result in output:
            self.assertRegex(result, pattern)

        # Assert that return format is like: "<project_id><project_name><level>"
        levels = "(?:ADMINISTER|CONTRIBUTE|UPLOAD|VIEW|NONE)"
        output = run(cmd.format(opts="")).strip().split("\n")
        pattern = "^project-[a-zA-Z0-9]{24} : .* \(" + levels + "\)$"
        for result in output:
            self.assertRegex(result, pattern)

        # Test --json output
        output = json.loads(run(cmd.format(opts="--json")))
        expected = [{"id": self.project_ppb,
                     "level": "ADMINISTER",
                     "public": True,
                     "describe": dxpy.api.project_describe(self.project_ppb)}]
        self.assertEqual(output, expected)

    def test_dx_find_org_projects_phi(self):
        projectName = "tempProject+{t}".format(t=time.time())
        with temporary_project(name=projectName) as project_1:
            project1_id = project_1.get_id()
            dxpy.api.project_update(project1_id, {"billTo": self.org_id})

            res = run('dx find org projects org-piratelabs --phi true --brief --name ' + pipes.quote(projectName))
            self.assertTrue(len(res) == 0, "Expected no PHI projects to be found")

            res = run('dx find org projects org-piratelabs --phi false --brief --name ' + pipes.quote(projectName)).strip().split("\n")

            self.assertTrue(len(res) == 1, "Expected to find one project")
            self.assertEqual(res[0], project1_id)

    def test_dx_find_org_apps(self):
        # Create a number of apps, some billed to self.org_id ("org-piratelabs")
        num_org_apps = 3
        num_nonorg_apps = 1

        apps = self.make_apps(num_org_apps,
                              "find_org_app",
                              bill_to=self.org_id)
        expected_app_ids = sorted([app['id'] for app in apps])
        self.make_apps(num_nonorg_apps, "unbilled_app")

        # Check that all org apps are found, and nothing else
        actual_app_ids = sorted(run("dx find org apps {} --brief".format(self.org_id)).strip().split("\n"))
        self.assertEqual(actual_app_ids, expected_app_ids)
        # Basic test to check consistency of client output to directly invoking API
        dx_api_output = dxpy.api.org_find_apps(self.org_id)
        self.assertEqual(len(dx_api_output['results']), num_org_apps)
        self.assertEqual(actual_app_ids, [app['id'] for app in dx_api_output['results']])

        # Same as above, without the --brief flag, so we need to destructure formatting
        lengthy_outputs = run("dx find org apps {}".format(self.org_id)).rstrip().split("\n")
        pattern = "^(\s\s|(\s\S)*x(\s\S)*)[a-zA-Z0-9_]*\s\([a-zA-Z0-9_]*\),\sv[0-9.]*$"
        for lengthy_output in lengthy_outputs:
            self.assertRegex(lengthy_output, pattern)

        output_titles = sorted([s.strip().split()[0] for s in lengthy_outputs])
        self.assertEqual(output_titles, sorted([app['title'] for app in apps]))


@unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping tests that require org creation')
class TestDXClientOrg(DXTestCase):

    @classmethod
    def get_unique_org_handle(cls):
        return "dx_test_new_org_{t}_{r}".format(t=int(time.time()), r=random.randint(0, 32768))

    def setUp(self):
        org_handle = TestDXClientOrg.get_unique_org_handle()
        org_new_input = {"name": org_handle, "handle": org_handle}
        self.org_id = dxpy.api.org_new(org_new_input)["id"]
        super(TestDXClientOrg, self).setUp()

    def test_create_new_org_negative(self):
        # No handle supplied
        with self.assertRaisesRegexp(subprocess.CalledProcessError, "error: argument --handle is required"):
            run('dx new org')

        with self.assertRaisesRegexp(subprocess.CalledProcessError, "error: argument --handle is required"):
            run('dx new org "Test Org"')

        with self.assertRaisesRegexp(subprocess.CalledProcessError, "error: argument --handle is required"):
            run('dx new org --member-list-visibility MEMBER')

        with self.assertRaisesRegexp(subprocess.CalledProcessError, "error: argument --handle is required"):
            run('dx new org --project-transfer-ability MEMBER')

        with self.assertRaisesRegexp(subprocess.CalledProcessError, "error: argument --handle is required"):
            run('dx new org --member-list-visibility ADMIN --project-transfer-ability MEMBER')

        with self.assertRaisesRegexp(subprocess.CalledProcessError,
                                     "error: argument --member-list-visibility: invalid choice"):
            run('dx new org --member-list-visibility NONE')

    def test_create_new_org(self):
        # Basic test with only required input args; optional input arg defaults propagated properly.
        org_handle = TestDXClientOrg.get_unique_org_handle()
        org_id = run('dx new org "Test New Org" --handle {h} --brief'.format(h=org_handle)).strip()
        res = dxpy.api.org_describe(org_id)
        self.assertEqual(res['handle'], org_handle)
        self.assertEqual(res['name'], "Test New Org")
        self.assertEqual(res['policies']['memberListVisibility'], "ADMIN")
        self.assertEqual(res['policies']['restrictProjectTransfer'], "ADMIN")

        # Test --member-list-visibility flag
        org_handle = TestDXClientOrg.get_unique_org_handle()
        policy_mlv = "MEMBER"
        org_id = run('dx new org "Test New Org" --handle {h} --member-list-visibility {mlv} --brief'
                     .format(h=org_handle, mlv=policy_mlv)).strip()
        res = dxpy.api.org_describe(org_id)
        self.assertEqual(res['handle'], org_handle)
        self.assertEqual(res['name'], "Test New Org")
        self.assertEqual(res['policies']['memberListVisibility'], policy_mlv)
        self.assertEqual(res['policies']['restrictProjectTransfer'], "ADMIN")

        org_handle = TestDXClientOrg.get_unique_org_handle()
        policy_mlv = "PUBLIC"
        org_id = run('dx new org "Test New Org" --handle {h} --member-list-visibility {mlv} --brief'
                     .format(h=org_handle, mlv=policy_mlv)).strip()
        res = dxpy.api.org_describe(org_id)
        self.assertEqual(res['handle'], org_handle)
        self.assertEqual(res['name'], "Test New Org")
        self.assertEqual(res['policies']['memberListVisibility'], policy_mlv)
        self.assertEqual(res['policies']['restrictProjectTransfer'], "ADMIN")

        # Test --project-transfer-ability flag
        org_handle = TestDXClientOrg.get_unique_org_handle()
        policy_pta = "MEMBER"
        org_id = run('dx new org "Test New Org" --handle {h} --project-transfer-ability {pta} --brief'
                     .format(h=org_handle, pta=policy_pta)).strip()
        res = dxpy.api.org_describe(org_id)
        self.assertEqual(res['handle'], org_handle)
        self.assertEqual(res['name'], "Test New Org")
        self.assertEqual(res['policies']['memberListVisibility'], "ADMIN")
        self.assertEqual(res['policies']['restrictProjectTransfer'], policy_pta)

        # Assert non-brief output format
        org_handle = TestDXClientOrg.get_unique_org_handle()
        output = run('dx new org "Test New Org" --handle {h}'.format(h=org_handle)).strip()
        self.assertEquals(output, 'Created new org called "Test New Org" (org-' + org_handle + ')')

    def test_create_new_org_prompt(self):
        # Prompt with only handle
        org_handle = TestDXClientOrg.get_unique_org_handle()
        dx_new_org = pexpect.spawn('dx new org --handle {h}'.format(h=org_handle), logfile=sys.stderr)
        dx_new_org.expect('Enter descriptive name')
        dx_new_org.sendline("Test New Org Prompt")
        dx_new_org.expect('Created new org')
        org_id = "org-" + org_handle
        res = dxpy.api.org_describe(org_id)
        self.assertEqual(res['handle'], org_handle)
        self.assertEqual(res['name'], "Test New Org Prompt")
        self.assertEqual(res['policies']["memberListVisibility"], "ADMIN")
        self.assertEqual(res['policies']["restrictProjectTransfer"], "ADMIN")

        # Prompt with "--member-list-visibility" & "--handle"
        org_handle = TestDXClientOrg.get_unique_org_handle()
        dx_new_org = pexpect.spawn('dx new org --handle {h} --member-list-visibility {mlv}'.format(h=org_handle,
                                   mlv="PUBLIC"), logfile=sys.stderr)
        dx_new_org.expect('Enter descriptive name')
        dx_new_org.sendline("Test New Org Prompt")
        dx_new_org.expect('Created new org')
        org_id = "org-" + org_handle
        res = dxpy.api.org_describe(org_id)
        self.assertEqual(res['handle'], org_handle)
        self.assertEqual(res['name'], "Test New Org Prompt")
        self.assertEqual(res['policies']["memberListVisibility"], "PUBLIC")
        self.assertEqual(res['policies']["restrictProjectTransfer"], "ADMIN")

        org_handle = TestDXClientOrg.get_unique_org_handle()
        dx_new_org = pexpect.spawn('dx new org --handle {h} --member-list-visibility {mlv}'.format(h=org_handle,
                                   mlv="MEMBER"), logfile=sys.stderr)
        dx_new_org.expect('Enter descriptive name')
        dx_new_org.sendline("Test New Org Prompt")
        dx_new_org.expect('Created new org')
        org_id = "org-" + org_handle
        res = dxpy.api.org_describe(org_id)
        self.assertEqual(res['handle'], org_handle)
        self.assertEqual(res['name'], "Test New Org Prompt")
        self.assertEqual(res['policies']["memberListVisibility"], "MEMBER")
        self.assertEqual(res['policies']["restrictProjectTransfer"], "ADMIN")

        org_handle = TestDXClientOrg.get_unique_org_handle()
        dx_new_org = pexpect.spawn('dx new org --handle {h} --member-list-visibility {mlv}'.format(h=org_handle,
                                   mlv="ADMIN"), logfile=sys.stderr)
        dx_new_org.expect('Enter descriptive name')
        dx_new_org.sendline("Test New Org Prompt")
        dx_new_org.expect('Created new org')
        org_id = "org-" + org_handle
        res = dxpy.api.org_describe(org_id)
        self.assertEqual(res['handle'], org_handle)
        self.assertEqual(res['name'], "Test New Org Prompt")
        self.assertEqual(res['policies']["memberListVisibility"], "ADMIN")
        self.assertEqual(res['policies']["restrictProjectTransfer"], "ADMIN")

        # Prompt with "--project-transfer-ability" & "handle"
        org_handle = TestDXClientOrg.get_unique_org_handle()
        dx_new_org = pexpect.spawn('dx new org --handle {h} --project-transfer-ability {pta}'.format(h=org_handle,
                                   pta="MEMBER"), logfile=sys.stderr)
        dx_new_org.expect('Enter descriptive name')
        dx_new_org.sendline("Test New Org Prompt")
        dx_new_org.expect('Created new org')
        org_id = "org-" + org_handle
        res = dxpy.api.org_describe(org_id)
        self.assertEqual(res['handle'], org_handle)
        self.assertEqual(res['name'], "Test New Org Prompt")
        self.assertEqual(res['policies']["memberListVisibility"], "ADMIN")
        self.assertEqual(res['policies']["restrictProjectTransfer"], "MEMBER")

        org_handle = TestDXClientOrg.get_unique_org_handle()
        dx_new_org = pexpect.spawn('dx new org --handle {h} --project-transfer-ability {pta}'.format(h=org_handle,
                                   pta="ADMIN"), logfile=sys.stderr)
        dx_new_org.expect('Enter descriptive name')
        dx_new_org.sendline("Test New Org Prompt")
        dx_new_org.expect('Created new org')
        org_id = "org-" + org_handle
        res = dxpy.api.org_describe(org_id)
        self.assertEqual(res['handle'], org_handle)
        self.assertEqual(res['name'], "Test New Org Prompt")
        self.assertEqual(res['policies']["memberListVisibility"], "ADMIN")
        self.assertEqual(res['policies']["restrictProjectTransfer"], "ADMIN")

        # Prompt with "--member-list-visibility", "--project-transfer-ability", & "--handle"
        org_handle = TestDXClientOrg.get_unique_org_handle()
        dx_new_org = pexpect.spawn('dx new org --handle {h} --member-list-visibility {p} --project-transfer-ability {p}'.format(
                                   h=org_handle, p="MEMBER"), logfile=sys.stderr)
        dx_new_org.expect('Enter descriptive name')
        dx_new_org.sendline("Test New Org Prompt")
        dx_new_org.expect('Created new org')
        org_id = "org-" + org_handle
        res = dxpy.api.org_describe(org_id)
        self.assertEqual(res['handle'], org_handle)
        self.assertEqual(res['name'], "Test New Org Prompt")
        self.assertEqual(res['policies']["memberListVisibility"], "MEMBER")
        self.assertEqual(res['policies']["restrictProjectTransfer"], "MEMBER")

    def test_org_update_negative(self):
        # Org id is required.
        invalid_cmds = ["dx update org",
                        "dx update org --name foo --member-list-visibility ADMIN --project-transfer-ability ADMIN"]
        for invalid_cmd in invalid_cmds:
            with self.assertSubprocessFailure(stderr_regexp="too few arguments", exit_code=2):
                run(invalid_cmd)

        # --project-transfer-ability may not be PUBLIC.
        with self.assertSubprocessFailure(stderr_regexp="--project-transfer-ability.*invalid", exit_code=2):
            run("dx update org {o} --project-transfer-ability PUBLIC".format(o=self.org_id))

    def test_org_update(self):
        def get_name_and_policies(org_id=None):
            if org_id is None:
                org_id = self.org_id
            org_desc = dxpy.api.org_describe(org_id)
            return (org_desc["name"], org_desc["policies"])

        # ---Regression tests---

        # Do not need to actually update the org at all.
        cur_org_name, cur_org_policies = get_name_and_policies()
        res = run('dx update org {o} --brief'.format(o=self.org_id)).strip()
        self.assertEqual(res, self.org_id)
        new_org_name, new_org_policies = get_name_and_policies(res)
        self.assertEqual(new_org_name, cur_org_name)
        self.assertEqual(new_org_policies, cur_org_policies)

        # --name.
        cur_org_name, cur_org_policies = new_org_name, new_org_policies
        proposed_org_name = "foo"
        self.assertNotEqual(proposed_org_name, cur_org_name)
        res = run('dx update org {o} --name "{n}" --brief'.format(o=self.org_id, n=proposed_org_name)).strip()
        self.assertEqual(res, self.org_id)
        new_org_name, new_org_policies = get_name_and_policies(res)
        self.assertEqual(new_org_name, proposed_org_name)
        self.assertEqual(new_org_policies, cur_org_policies)

        # --member-list-visibility.
        cur_org_name, cur_org_policies = new_org_name, new_org_policies
        proposed_mlv = "MEMBER"
        self.assertNotEqual(proposed_mlv, cur_org_policies["memberListVisibility"])
        exp_org_policies = dict(cur_org_policies, memberListVisibility=proposed_mlv)
        res = run('dx update org {o} --member-list-visibility {p} --brief'.format(o=self.org_id,
                                                                                  p=proposed_mlv)).strip()
        self.assertEqual(res, self.org_id)
        new_org_name, new_org_policies = get_name_and_policies(res)
        self.assertEqual(new_org_name, cur_org_name)
        self.assertEqual(new_org_policies, exp_org_policies)

        cur_org_name, cur_org_policies = new_org_name, new_org_policies
        proposed_mlv = "PUBLIC"
        self.assertNotEqual(proposed_mlv, cur_org_policies["memberListVisibility"])
        exp_org_policies = dict(cur_org_policies, memberListVisibility=proposed_mlv)
        res = run('dx update org {o} --member-list-visibility {p} --brief'.format(o=self.org_id,
                                                                                  p=proposed_mlv)).strip()
        self.assertEqual(res, self.org_id)
        new_org_name, new_org_policies = get_name_and_policies(res)
        self.assertEqual(new_org_name, cur_org_name)
        self.assertEqual(new_org_policies, exp_org_policies)

        # --project-transfer-ability.
        cur_org_name, cur_org_policies = new_org_name, new_org_policies
        proposed_pta = "ADMIN"
        self.assertNotEqual(proposed_pta, cur_org_policies["restrictProjectTransfer"])
        exp_org_policies = dict(cur_org_policies, restrictProjectTransfer=proposed_pta)
        res = run('dx update org {o} --project-transfer-ability {p} --brief'.format(o=self.org_id,
                                                                                    p=proposed_pta)).strip()
        self.assertEqual(res, self.org_id)
        new_org_name, new_org_policies = get_name_and_policies(res)
        self.assertEqual(new_org_name, cur_org_name)
        self.assertEqual(new_org_policies, exp_org_policies)

        # All args.
        cur_org_name, cur_org_policies = new_org_name, new_org_policies
        proposed_org_name = "bar"
        proposed_mlv = "ADMIN"
        proposed_pta = "MEMBER"
        exp_org_policies = dict(cur_org_policies, memberListVisibility=proposed_mlv,
                                restrictProjectTransfer=proposed_pta)
        res = run('dx update org {o} --name {n} --member-list-visibility {mlv} --project-transfer-ability {pta} --brief'.format(
            o=self.org_id, n=proposed_org_name, mlv=proposed_mlv, pta=proposed_pta)).strip()
        self.assertEqual(res, self.org_id)
        new_org_name, new_org_policies = get_name_and_policies(res)
        self.assertEqual(new_org_name, proposed_org_name)
        self.assertEqual(new_org_policies, exp_org_policies)

    def test_org_update_format(self):
        res = run('dx update org {o}'.format(o=self.org_id)).strip()
        self.assertRegex(res, "^Updated.*{o}$".format(o=self.org_id))

        res = run('dx update org {o} --brief'.format(o=self.org_id)).strip()
        self.assertEqual(res, self.org_id)


class TestDXClientNewProject(DXTestCase):
    def test_dx_new_project_with_region(self):
        project_id = run("dx new project --brief --region aws:us-east-1 ProjectInUSEast").strip()
        self.assertEquals(dxpy.api.project_describe(project_id, {})['region'], "aws:us-east-1")
        dxpy.api.project_destroy(project_id, {})

        with self.assertRaisesRegexp(subprocess.CalledProcessError, "InvalidInput"):
            run("dx new project --brief --region aws:not-a-region InvalidRegionProject")

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that requires presence of test org')
    def test_dx_create_new_project_with_bill_to(self):
        curr_bill_to = dxpy.api.user_describe(dxpy.whoami())['billTo']
        alice_id = "user-alice"
        org_id = "org-piratelabs"
        project_name = "test_dx_create_project"

        # Check that requesting user has allowBillableActivities permission in org
        member_access = dxpy.api.org_describe(org_id)
        self.assertTrue(member_access['level'] == 'ADMIN' or member_access['allowBillableActivities'])

        # Check that billTo of requesting user is the requesting user
        dxpy.api.user_update(dxpy.whoami(), {'billTo': alice_id})
        self.assertEquals(dxpy.api.user_describe(dxpy.whoami())['billTo'], alice_id)

        # Create project billTo org
        project_id = run("dx new project {name} --bill-to {billTo} --brief".format(name=project_name,
                         billTo=org_id)).strip()
        self.assertEquals(dxpy.api.project_describe(project_id, {'fields': {'billTo': True}})['billTo'], org_id)
        dxpy.api.project_destroy(project_id)

        # Create project billTo requesting user
        project_id = run("dx new project {name} --bill-to {billTo} --brief".format(name=project_name,
                         billTo=dxpy.whoami())).strip()
        self.assertEquals(dxpy.api.project_describe(project_id, {'fields': {'billTo': True}})['billTo'], dxpy.whoami())
        dxpy.api.project_destroy(project_id)

        # Create project billTo invalid org
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run("dx new project {name} --bill-to {billTo} --brief".format(name=project_name, billTo='org-invalid'))

        # With user's billTo set to org
        dxpy.api.user_update(dxpy.whoami(), {'billTo': org_id})
        self.assertEqual(dxpy.api.user_describe(dxpy.whoami())['billTo'], org_id)

        project_id = run("dx new project {name} --bill-to {billTo} --brief".format(name=project_name,
                         billTo=dxpy.whoami())).strip()
        self.assertEquals(dxpy.api.project_describe(project_id, {'fields': {'billTo': True}})['billTo'], dxpy.whoami())
        dxpy.api.project_destroy(project_id)

        project_id = run("dx new project {name} --bill-to {billTo} --brief".format(name=project_name,
                         billTo=org_id)).strip()
        self.assertEquals(dxpy.api.project_describe(project_id, {'fields': {'billTo': True}})['billTo'], org_id)
        dxpy.api.project_destroy(project_id)

        # reset original user settings
        dxpy.api.user_update(dxpy.whoami(), {'billTo': curr_bill_to})


@unittest.skipUnless(testutil.TEST_ISOLATED_ENV and testutil.TEST_WITH_AUTHSERVER,
                     'skipping tests that require presence of test org and running authserver')
class TestDXClientNewUser(DXTestCase):

    def _now(self):
        return str(int(time.time()))

    def _assert_user_desc(self, user_id, exp_user_desc):
        user_desc = dxpy.api.user_describe(user_id)
        for field in exp_user_desc:
            self.assertEqual(user_desc[field], exp_user_desc[field])

    def setUp(self):
        self.org_id = "org-piratelabs"
        super(TestDXClientNewUser, self).setUp()

    def tearDown(self):
        super(TestDXClientNewUser, self).tearDown()

    def test_create_user_account_and_set_bill_to_negative(self):
        username, email = generate_unique_username_email()
        first = "Asset"
        cmd = "dx new user"

        called_process_error_opts = [
            "",
            "--username {u}".format(u=username),
            "--email {e}".format(e=email),
            "--username {u} --email {e} --first {f} \
                --token-duration {t}".format(u=username, e=email, f=first,
                                             t="not_an_int"),
        ]
        for invalid_opts in called_process_error_opts:
            with self.assertRaises(subprocess.CalledProcessError):
                run(" ".join([cmd, invalid_opts]))

        dx_api_error_opts = [
            "--username {u} --email {e}".format(u=username, e=email),
            "--username {u} --email bad_email".format(u=username),
            "--username bu --email {e}".format(e=email),
            "--username {u} --email {e} --first {f} --org does_not_exist --set-bill-to".format(
                u=username, e=email, f=first),
        ]
        for invalid_opts in dx_api_error_opts:
            with self.assertRaisesRegexp(subprocess.CalledProcessError,
                                         "DXAPIError"):
                run(" ".join([cmd, invalid_opts]))

        resource_not_found_opts = [
            "--username {u} --email {e} --first {f} --org does_not_exist".format(
                u=username, e=email, f=first),
        ]
        for invalid_opts in resource_not_found_opts:
            with self.assertRaisesRegexp(subprocess.CalledProcessError,
                                         "ResourceNotFound"):
                run(" ".join([cmd, invalid_opts]))

        dx_cli_error_opts = [
            "--username {u} --email {e} --first {f} --level MEMBER".format(
                u=username, e=email, f=first),
            "--username {u} --email {e} --first {f} --set-bill-to".format(
                u=username, e=email, f=first),
            "--username {u} --email {e} --first {f} --allow-billable-activities".format(
                u=username, e=email, f=first),
            "--username {u} --email {e} --first {f} --no-app-access".format(
                u=username, e=email, f=first),
            "--username {u} --email {e} --first {f} --project-access VIEW".format(
                u=username, e=email, f=first),
            "--username {u} --email {e} --first {f} --no-email".format(
                u=username, e=email, f=first),
        ]
        for invalid_opts in dx_cli_error_opts:
            with self.assertRaisesRegexp(subprocess.CalledProcessError,
                                         "DXCLIError"):
                run(" ".join([cmd, invalid_opts]))


    def test_self_signup_negative(self):
        # How to unset context?
        pass

    def test_create_user_account_only(self):
        first = "Asset"
        last = "The"
        middle = "T."
        cmd = "dx new user"

        # Basic with first name only.
        username, email = generate_unique_username_email()
        user_id = run("{cmd} --username {u} --email {e} --first {f} --brief".format(
                      cmd=cmd, u=username, e=email, f=first)).strip()
        self._assert_user_desc(user_id, {"first": first})

        # Basic with last name only.
        username, email = generate_unique_username_email()
        user_id = run("{cmd} --username {u} --email {e} --last {l} --brief".format(
                      cmd=cmd, u=username, e=email, l=last)).strip()
        self._assert_user_desc(user_id, {"last": last})

        # Basic with all options we can verify.
        # TODO: Test --occupation.
        username, email = generate_unique_username_email()
        user_id = run("{cmd} --username {u} --email {e} --first {f} --middle {m} --last {l} --brief".format(
                      cmd=cmd, u=username, e=email, f=first, m=middle,
                      l=last)).strip()
        self._assert_user_desc(user_id, {"first": first,
                                         "last": last,
                                         "middle": middle})

    def test_create_user_account_and_invite_to_org(self):
        # TODO: Test --no-email flag.

        first = "Asset"
        cmd = "dx new user"

        # Grant default org membership level and permission flags.
        username, email = generate_unique_username_email()
        user_id = run("{cmd} --username {u} --email {e} --first {f} --org {o} --brief".format(
                      cmd=cmd, u=username, e=email, f=first,
                      o=self.org_id)).strip()
        self._assert_user_desc(user_id, {"first": first})
        exp = {
            "level": "MEMBER",
            "allowBillableActivities": False,
            "createProjectsAndApps": False,
            "appAccess": True,
            "projectAccess": "CONTRIBUTE",
            "id": user_id
        }
        res = dxpy.api.org_find_members(self.org_id, {"id": [user_id]})["results"][0]
        self.assertEqual(res, exp)

        # Grant default org membership level and permission flags; `username`
        # has uppercase chars.
        username, email = generate_unique_username_email()
        username = username.upper()
        user_id = run("{cmd} --username {u} --email {e} --first {f} --org {o} --brief".format(
                      cmd=cmd, u=username, e=email, f=first,
                      o=self.org_id)).strip()
        self._assert_user_desc(user_id, {"first": first})
        exp = {
            "level": "MEMBER",
            "allowBillableActivities": False,
            "createProjectsAndApps": False,
            "appAccess": True,
            "projectAccess": "CONTRIBUTE",
            "id": user_id
        }
        res = dxpy.api.org_find_members(self.org_id, {"id": [user_id]})["results"][0]
        self.assertEqual(res, exp)

        # Grant custom org membership level and permission flags.
        username, email = generate_unique_username_email()
        user_id = run("{cmd} --username {u} --email {e} --first {f} --org {o} --level {l} --allow-billable-activities --no-app-access --project-access {pa} --brief".format(
                      cmd=cmd, u=username, e=email, f=first,
                      o=self.org_id, l="MEMBER", pa="VIEW")).strip()
        self._assert_user_desc(user_id, {"first": first})
        exp = {
            "level": "MEMBER",
            "allowBillableActivities": True,
            "createProjectsAndApps": True,
            "appAccess": False,
            "projectAccess": "VIEW",
            "id": user_id
        }
        res = dxpy.api.org_find_members(self.org_id, {"id": [user_id]})["results"][0]
        self.assertEqual(res, exp)

        # Grant ADMIN org membership level; ignore all other org permission
        # options.
        username, email = generate_unique_username_email()
        user_id = run("{cmd} --username {u} --email {e} --first {f} --org {o} --level {l} --no-app-access --project-access {pa} --brief".format(
                      cmd=cmd, u=username, e=email, f=first,
                      o=self.org_id, l="ADMIN", pa="VIEW")).strip()
        self._assert_user_desc(user_id, {"first": first})
        exp = {
            "level": "ADMIN",
            "allowBillableActivities": True,
            "createProjectsAndApps": True,
            "appAccess": True,
            "projectAccess": "ADMINISTER",
            "id": user_id
        }
        res = dxpy.api.org_find_members(self.org_id, {"id": [user_id]})["results"][0]
        self.assertEqual(res, exp)

    def test_create_user_account_and_set_bill_to(self):
        first = "Asset"
        cmd = "dx new user --set-bill-to"  # Set --set-bill-to option.

        # --allow-billable-activities is implied; grant custom org membership
        # level and other permission flags.
        username, email = generate_unique_username_email()
        user_id = run("{cmd} --username {u} --email {e} --first {f} --org {o} --level {l} --project-access {pa} --brief".format(
                      cmd=cmd, u=username, e=email, f=first,
                      o=self.org_id, l="MEMBER", pa="VIEW")).strip()
        self._assert_user_desc(user_id, {"first": first})
        exp = {
            "level": "MEMBER",
            "allowBillableActivities": True,
            "createProjectsAndApps": True,
            "appAccess": True,
            "projectAccess": "VIEW",
            "id": user_id
        }
        res = dxpy.api.org_find_members(self.org_id, {"id": [user_id]})["results"][0]
        self.assertEqual(res, exp)

        # Grant ADMIN org membership level.
        username, email = generate_unique_username_email()
        user_id = run("{cmd} --username {u} --email {e} --first {f} --org {o} --level ADMIN --brief".format(
                      cmd=cmd, u=username, e=email, f=first,
                      o=self.org_id)).strip()
        self._assert_user_desc(user_id, {"first": first})
        exp = {
            "level": "ADMIN",
            "allowBillableActivities": True,
            "createProjectsAndApps": True,
            "appAccess": True,
            "projectAccess": "ADMINISTER",
            "id": user_id
        }
        res = dxpy.api.org_find_members(self.org_id, {"id": [user_id]})["results"][0]
        self.assertEqual(res, exp)

    def test_create_user_account_and_set_token_duration_negative(self):
        first = "Asset"
        username, email = "token_duration_neg", "token_duration_neg@example.com"
        cmd = "dx new user --username {u} --email {e} --first {f} --token-duration {td}"

        invalid_token_durations = [
            "8md",  # "md" is an invalid unit
            "8.5",  # float is an invalid input
            "8.5d",  # float with unit is an invalid input
            "31d"  # longer than 30 days
        ]

        # test invalid inputs for token duration
        for invalid_token_duration in invalid_token_durations:
            with self.assertRaisesRegexp(subprocess.CalledProcessError, "ValueError"):
                run(cmd.format(u=username.lower(), e=email, f=first, td=invalid_token_duration))

    def test_create_user_account_and_set_token_duration(self):
        first = "Asset"
        cmd = "dx new user --username {u} --email {e} --first {f} --token-duration={td} --brief"

        token_durations = ["10000", "10d", "-10000", "-10d"]

        for token_duration in token_durations:
            username, email = generate_unique_username_email()
            user_id = run(cmd.format(u=username, e=email, f=first, td=token_duration)).strip()
            self.assertEqual(user_id, "user-" + username.lower())


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

    def test_add_membership_default(self):
        cmd = "dx add member {o} {u} --level {l}"

        run(cmd.format(o=self.org_id, u=self.username, l="ADMIN"))
        exp_membership = {"id": self.user_id,
                          "level": "ADMIN",
                          "allowBillableActivities": True,
                          "createProjectsAndApps": True,
                          "appAccess": True,
                          "projectAccess": "ADMINISTER"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        self._remove_user(self.user_id)

        run(cmd.format(o=self.org_id, u=self.username, l="MEMBER"))
        exp_membership = {"id": self.user_id,
                          "level": "MEMBER",
                          "allowBillableActivities": False,
                          "createProjectsAndApps": False,
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
                          "createProjectsAndApps": True,
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
                          "createProjectsAndApps": True,
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
        with self.assertRaisesRegexp(subprocess.CalledProcessError, "DXCLIError"):
            run(" ".join([cmd, self.org_id, self.username, "--level ADMIN"]))

    def test_remove_membership_default(self):
        self._add_user(self.user_id)

        exp_membership = {"id": self.user_id,
                          "level": "ADMIN",
                          "allowBillableActivities": True,
                          "createProjectsAndApps": True,
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
                          "createProjectsAndApps": True,
                          "appAccess": True,
                          "projectAccess": "ADMINISTER"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        dx_rm_member_int = pexpect.spawn("dx remove member {o} {u}".format(
            o=self.org_id, u=self.username), logfile=sys.stderr)
        dx_rm_member_int.expect("Please confirm")
        dx_rm_member_int.sendline("")
        dx_rm_member_int.expect("Please confirm")

        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        dx_rm_member_int = pexpect.spawn("dx remove member {o} {u}".format(
            o=self.org_id, u=self.username), logfile=sys.stderr)
        dx_rm_member_int.expect("Please confirm")
        dx_rm_member_int.sendintr()

        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        dx_rm_member_int = pexpect.spawn("dx remove member {o} {u}".format(
            o=self.org_id, u=self.username), logfile=sys.stderr)
        dx_rm_member_int.expect("Please confirm")
        dx_rm_member_int.sendline("n")
        dx_rm_member_int.expect("Aborting removal")

        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        dx_rm_member_int = pexpect.spawn("dx remove member {o} {u}".format(
            o=self.org_id, u=self.username))
        dx_rm_member_int.logfile = sys.stdout
        dx_rm_member_int.expect("Please confirm")
        dx_rm_member_int.sendline("y")
        dx_rm_member_int.expect("Removed user-{u}".format(u=self.username))

    def test_remove_membership_interactive_conf_format(self):
        self._add_user(self.user_id)

        exp_membership = {"id": self.user_id,
                          "level": "ADMIN",
                          "allowBillableActivities": True,
                          "createProjectsAndApps": True,
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

        dx_rm_member_int = pexpect.spawn("dx remove member {o} {u}".format(
            o=self.org_id, u=self.username))
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
        with self.assertRaisesRegexp(subprocess.CalledProcessError,
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

    def test_update_membership_positive(self):
        # default test
        self._add_user(self.user_id)

        exp_membership = {"id": self.user_id,
                          "level": "ADMIN",
                          "allowBillableActivities": True,
                          "createProjectsAndApps": True,
                          "appAccess": True,
                          "projectAccess": "ADMINISTER"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        run("dx update member {o} {u} --level MEMBER --allow-billable-activities false --project-access VIEW --app-access true".format(
            o=self.org_id, u=self.username))
        exp_membership = {"id": self.user_id,
                          "level": "MEMBER",
                          "allowBillableActivities": False,
                          "createProjectsAndApps": False,
                          "projectAccess": "VIEW",
                          "appAccess": True}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        run("dx update member {o} {u} --allow-billable-activities true --app-access false".format(
            o=self.org_id, u=self.username))
        exp_membership = {"id": self.user_id,
                          "level": "MEMBER",
                          "allowBillableActivities": True,
                          "createProjectsAndApps": True,
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
               "createProjectsAndApps": True,
               "projectAccess": "ADMINISTER",
               "appAccess": True}
        membership_response = self._org_find_members(self.user_id)
        self.assertEqual(membership_response, exp)

        run(cmd)
        exp = {"id": self.user_id,
               "level": "MEMBER",
               "allowBillableActivities": False,
               "createProjectsAndApps": False,
               "projectAccess": "CONTRIBUTE",
               "appAccess": True}
        membership_response = self._org_find_members(self.user_id)
        self.assertEqual(membership_response, exp)

        # MEMBER to MEMBER.
        run(cmd + " --allow-billable-activities true")
        exp = {"id": self.user_id,
               "level": "MEMBER",
               "allowBillableActivities": True,
               "createProjectsAndApps": True,
               "projectAccess": "CONTRIBUTE",
               "appAccess": True}
        membership_response = self._org_find_members(self.user_id)
        self.assertEqual(membership_response, exp)

        run(cmd)
        membership_response = self._org_find_members(self.user_id)
        self.assertEqual(membership_response, exp)

    def test_update_membership_negative(self):
        cmd = "dx update member"

        # Cannot update the membership of a user who is not currently a member
        # of the org.
        with self.assertRaisesRegexp(subprocess.CalledProcessError,
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
            with self.assertRaisesRegexp(subprocess.CalledProcessError,
                                         "InvalidInput"):
                run(' '.join([cmd, invalid_opt]))

    def test_add_update_remove_membership(self):
        cmd = "dx add member {o} {u} --level {l} --project-access UPLOAD"
        run(cmd.format(o=self.org_id, u=self.username, l="MEMBER"))
        exp_membership = {"id": self.user_id,
                          "level": "MEMBER",
                          "allowBillableActivities": False,
                          "createProjectsAndApps": False,
                          "appAccess": True,
                          "projectAccess": "UPLOAD"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        cmd = "dx update member {o} {u} --level MEMBER --allow-billable-activities true"
        run(cmd.format(o=self.org_id, u=self.username))
        exp_membership.update(allowBillableActivities=True, createProjectsAndApps=True)
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        cmd = "dx update member {o} {u} --level ADMIN"
        run(cmd.format(o=self.org_id, u=self.username))
        exp_membership = {"id": self.user_id,
                          "level": "ADMIN",
                          "allowBillableActivities": True,
                          "createProjectsAndApps": True,
                          "appAccess": True,
                          "projectAccess": "ADMINISTER"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        cmd = "dx update member {o} {u} --level MEMBER --allow-billable-activities true --project-access CONTRIBUTE --app-access false"
        run(cmd.format(o=self.org_id, u=self.username))
        exp_membership.update(level="MEMBER", projectAccess="CONTRIBUTE", appAccess=False)
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

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
                          "createProjectsAndApps": False,
                          "appAccess": True,
                          "projectAccess": "UPLOAD"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        cmd = "dx update member {o} {u} --level MEMBER --allow-billable-activities true"
        run(cmd.format(o=self.org_id, u=self.user_id))
        exp_membership.update(allowBillableActivities=True, createProjectsAndApps=True)
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        cmd = "dx update member {o} {u} --level ADMIN"
        run(cmd.format(o=self.org_id, u=self.user_id))
        exp_membership = {"id": self.user_id,
                          "level": "ADMIN",
                          "allowBillableActivities": True,
                          "createProjectsAndApps": True,
                          "appAccess": True,
                          "projectAccess": "ADMINISTER"}
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

        cmd = "dx update member {o} {u} --level MEMBER --allow-billable-activities true --project-access CONTRIBUTE --app-access false"
        run(cmd.format(o=self.org_id, u=self.user_id))
        exp_membership.update(level="MEMBER", projectAccess="CONTRIBUTE", appAccess=False)
        membership = self._org_find_members(self.user_id)
        self.assertEqual(membership, exp_membership)

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

        #Update items one by one.
        for item in update_items:
            run(self.cmd.format(pid=self.project, item=item, n=pipes.quote(update_items[item])))
            describe_input = {}
            describe_input[item] = 'true'
            self.assertEqual(self.project_describe(describe_input)[item],
                             update_items[item])

    def test_update_multiple_items(self):
        #Test updating multiple items in a single api call
        update_items = {'name': 'NewProjectName' + str(time.time()),
                        'summary': 'This is new a summary',
                        'description': 'This is new a description',
                        'protected': 'false'}

        update_project_output = check_output(["dx", "update", "project", self.project, "--name",
                pipes.quote(update_items['name']), "--summary", update_items['summary'], "--description",
                update_items['description'], "--protected", update_items['protected']])
        update_project_json = json.loads(update_project_output);
        self.assertTrue("id" in update_project_json)
        self.assertEqual(self.project, update_project_json["id"])

        update_project_output = check_output(["dx", "update", "project", self.project, "--name",
                pipes.quote(update_items['name']), "--summary", update_items['summary'], "--description",
                update_items['description'], "--protected", update_items['protected'], "--brief"])
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

    def test_update_project_by_name(self):
        describe_input = {}
        describe_input['name'] = 'true'

        project_name = self.project_describe(describe_input)['name']
        new_name = 'Another Project Name' + str(time.time())

        run(self.cmd.format(pid=project_name, item='name', n=pipes.quote(new_name)))
        result = self.project_describe(describe_input)
        self.assertEqual(result['name'], new_name)

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


class TestDXBuildApp(DXTestCaseBuildApps):
    def run_and_assert_stderr_matches(self, cmd, stderr_regexp):
        with self.assertSubprocessFailure(stderr_regexp=stderr_regexp, exit_code=28):
            run(cmd + ' && exit 28')

    def test_help_without_security_context(self):
        env = override_environment(DX_SECURITY_CONTEXT=None, DX_APISERVER_HOST=None,
                                  DX_APISERVER_PORT=None, DX_APISERVER_PROTOCOL=None)
        run("dx build -h", env=env)

    def test_accepts_semver(self):
        self.assertTrue(dx_build_app.APP_VERSION_RE.match('3.1.41') is not None)
        self.assertTrue(dx_build_app.APP_VERSION_RE.match('3.1.41-rc.1') is not None)
        self.assertFalse(dx_build_app.APP_VERSION_RE.match('3.1.41-rc.1.') is not None)
        self.assertFalse(dx_build_app.APP_VERSION_RE.match('3.1.41-rc..1') is not None)
        self.assertTrue(dx_build_app.APP_VERSION_RE.match('22.0.999+git.abcdef') is not None)
        self.assertFalse(dx_build_app.APP_VERSION_RE.match('22.0.999+git.abcdef$') is not None)
        self.assertFalse(dx_build_app.APP_VERSION_RE.match('22.0.999+git.abcdef.') is not None)
        self.assertTrue(dx_build_app.APP_VERSION_RE.match('22.0.999-rc.1+git.abcdef') is not None)

    def test_version_suffixes(self):
        app_spec = {
            "name": "test_versioning_åpp",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("test_versioning_app", json.dumps(app_spec), "code.py")
        self.assertTrue(dx_build_app._get_version_suffix(app_dir, '1.0.0').startswith('+build.'))
        self.assertTrue(dx_build_app._get_version_suffix(app_dir, '1.0.0+git.abcdef')
                        .startswith('.build.'))

    def test_build_applet(self):
        app_spec = {
            "name": "minimal_applet",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("minimal_åpplet", json.dumps(app_spec), "code.py")
        new_applet = json.loads(run("dx build --json " + app_dir))
        applet_describe = dxpy.get_handler(new_applet["id"]).describe()
        self.assertEqual(applet_describe["class"], "applet")
        self.assertEqual(applet_describe["id"], applet_describe["id"])
        self.assertEqual(applet_describe["name"], "minimal_applet")

    def test_build_applet_ignores_regional_options_in_dxapp(self):
        name = "asset_applet_{t}".format(t=int(time.time() * 1000))
        app_spec = {
            "name": name,
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",

            # Will be ignored when building applets.
            "regionalOptions": {"aws:us-east-1": {}}
            }
        app_dir = self.write_app_directory(name, json.dumps(app_spec), "code.py")
        new_applet = json.loads(run("dx build --json " + app_dir))
        applet_describe = dxpy.get_handler(new_applet["id"]).describe()
        self.assertEqual(applet_describe["class"], "applet")
        self.assertEqual(applet_describe["id"], applet_describe["id"])
        self.assertEqual(applet_describe["name"], name)
        # We already know this via the API, but here goes.
        self.assertNotIn("regionalOptions", applet_describe)

    def test_dx_build_applet_dxapp_json_created_with_makefile(self):
        app_name = "nodxapp_applet"
        app_dir = self.write_app_directory(app_name, None, "code.py")
        app_spec = {
            "name": app_name,
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        makefile_str = "dxapp.json:\n\tcp temp_dxapp.json dxapp.json\n"
        with open(os.path.join(app_dir, 'temp_dxapp.json'), 'wb') as manifest:
            manifest.write(json.dumps(app_spec).encode())
        with open(os.path.join(app_dir, "Makefile"), 'w') as makefile:
            makefile.write(makefile_str)
        run("dx build " + app_dir)

    def test_dx_build_applet_no_app_linting(self):
        run("dx clearenv")

        # Case: Missing title, summary, description.
        app_spec = {
            "name": "dx_build_applet_missing_fields",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "categories": ["Annotation"]
            }
        app_dir = self.write_app_directory("dx_build_applet_missing_fields", json.dumps(app_spec), "code.py")
        args = ['dx', 'build', app_dir]
        p = subprocess.Popen(args, stderr=subprocess.PIPE)
        out, err = p.communicate()
        self.assertFalse(err.startswith("WARNING"))

        # Case: Usage of period at end of summary.
        app_spec = {
            "name": "dx_build_applet_summary_with_period",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "title": "Title",
            "summary": "Summary without period",
            "description": "Description with period.",
            "categories": ["Annotation"]
            }
        app_dir = self.write_app_directory("dx_build_applet_summary_with_period", json.dumps(app_spec), "code.py")
        args = ['dx', 'build', app_dir]
        p = subprocess.Popen(args, stderr=subprocess.PIPE)
        out, err = p.communicate()
        self.assertFalse(err.startswith("WARNING"))

        # Case: Usage of unknown categories.
        unknown_category = "asdf1234"
        app_spec = {
            "name": "dx_build_applet_unknown_cat",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "title": "Title",
            "summary": "Summary without period",
            "description": "Description without period",
            "categories": [unknown_category]
            }
        app_dir = self.write_app_directory("dx_build_applet_unknown_cat", json.dumps(app_spec), "code.py")
        args = ['dx', 'build', app_dir]
        p = subprocess.Popen(args, stderr=subprocess.PIPE)
        out, err = p.communicate()
        self.assertFalse(err.startswith("WARNING"))

    def test_build_applet_dry_run(self):
        app_spec = {
            "name": "minimal_applet_dry_run",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("minimal_applet_dry_run", json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp='cannot be specified together', exit_code=2):
            run("dx build --dry-run " + app_dir + " --run -y --brief")
        run("dx build --dry-run " + app_dir)
        self.assertEqual(len(list(dxpy.find_data_objects(name="minimal_applet_dry_run"))), 0)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    def test_build_applet_and_run_immediately(self):
        app_spec = {
            "name": "minimal_applet_to_run",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
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
        app_spec = {
            "name": "minimal_remote_build_applet_to_run",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
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
        app_spec = {
            "name": "minimal_remote_build_app",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("minimal_remote_build_åpp", json.dumps(app_spec), "code.py")
        run("dx build --remote --app " + app_dir)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS and testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps and run jobs')
    def test_remote_build_app_trusty(self):
        app_spec = {
            "name": "minimal_remote_build_app_trusty",
            "dxapi": "1.0.0",
            # Use a package specific to trusty but not in precise as part of the execdepends to ensure it is installed properly
            "runSpec": {"file": "code.py", "interpreter": "python2.7", "distribution": "Ubuntu", "release": "14.04", "buildDepends": [{"name": "postgresql-9.3"}], "systemRequirements": {"*": {"instanceType": "mem1_ssd1_x4"}}},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("minimal_remote_build_åpp_trusty", json.dumps(app_spec), "code.py")
        run("dx build --remote --app " + app_dir)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS and testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps and run jobs')
    def test_remote_build_applet(self):
        app_spec = {
            "name": "minimal_remote_build_applet",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("minimal_remote_build_åpplet", json.dumps(app_spec), "code.py")
        run("dx build --remote " + app_dir)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS and testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps and run jobs')
    def test_remote_build_applet_trusty(self):
        app_spec = {
            "name": "minimal_remote_build_applet_trusty",
            "dxapi": "1.0.0",
            # Use a package specific to trusty but not in precise as part of the execdepends to ensure it is installed properly
            "runSpec": {"file": "code.py", "interpreter": "python2.7", "distribution": "Ubuntu", "release": "14.04", "buildDepends": [{"name": "postgresql-9.3"}], "systemRequirements": {"*": {"instanceType": "mem1_ssd1_x4"}}},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("minimal_remote_build_åpplet_trusty", json.dumps(app_spec), "code.py")
        run("dx build --remote " + app_dir)

    def test_cannot_remote_build_multi_region_app(self):
        app_name = "asset_{t}_remote_multi_region_app".format(t=int(time.time()))
        app_spec = {
            "name": app_name,
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp='--region.*once for remote', exit_code=2):
            run("dx build --remote --app --region aws:us-east-1 --region azure:westus " + app_dir)

    def test_remote_build_app_and_run_immediately(self):
        app_spec = {
            "name": "minimal_remote_build_app_to_run",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
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
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
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
        app_spec = {
            "name": "Foo",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "foo"
            }
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

    @ unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create apps')
    def test_build_app_suggestions(self):
        app_spec = {
            "name": "test_build_app_suggestions",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [{"name": "testname", "class": "file", "suggestions": []}],
            "outputSpec": [],
            "version": "0.0.1"
        }

        # check if project exists
        app_spec["inputSpec"][0]["suggestions"] = [{"name": "somename", "project": "project-0000000000000000000000NA", "path": "/"}]
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
        app_spec["inputSpec"][0]["suggestions"] = [{"name": "somename", "$dnanexus_link": "gtable-0000000000000000000000NA"}]
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

    @ unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create apps')
    def test_build_app_suggestions_success(self):
        app_spec = {"name": "test_build_app_suggestions",
                    "dxapi": "1.0.0",
                    "runSpec": {"file": "code.py", "interpreter": "python2.7"},
                    "inputSpec": [{"name": "testname", "class": "gtable", "suggestions": []}],
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
    def test_build_single_region_app_without_regional_options(self):
        # Backwards-compatible.
        app_name = "asset_{t}_single_region_app".format(t=int(time.time()))
        app_spec = {
            "name": app_name,
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")
        new_app = json.loads(run("dx build --create-app --json " + app_dir))
        app_describe = json.loads(run("dx describe --json " + new_app["id"]))
        self.assertEqual(app_describe["class"], "app")
        self.assertEqual(app_describe["id"], app_describe["id"])
        self.assertEqual(app_describe["version"], "1.0.0")
        self.assertEqual(app_describe["name"], app_name)
        self.assertFalse("published" in app_describe)
        self.assertIn("regionalOptions", app_describe)
        self.assertItemsEqual(app_describe["regionalOptions"].keys(), ["aws:us-east-1"])

        self.assertTrue(os.path.exists(os.path.join(app_dir, 'code.py')))
        self.assertFalse(os.path.exists(os.path.join(app_dir, 'code.pyc')))

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_build_app_with_regional_options(self):
        app_name = "app_regional_options"
        app_spec = {
            "name": app_name,
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "regionalOptions": {"aws:us-east-1": {}}
            }
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")
        new_app = json.loads(run("dx build --create-app --json " + app_dir))
        app_describe = json.loads(run("dx describe --json " + new_app["id"]))
        self.assertEqual(app_describe["class"], "app")
        self.assertEqual(app_describe["id"], new_app["id"])
        self.assertEqual(app_describe["version"], "1.0.0")
        self.assertEqual(app_describe["name"], app_name)
        self.assertFalse("published" in app_describe)
        self.assertIn("regionalOptions", app_describe)
        self.assertItemsEqual(app_describe["regionalOptions"].keys(), app_spec["regionalOptions"].keys())

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
        self.assertItemsEqual(app_desc_res["regionalOptions"].keys(), app_spec["regionalOptions"].keys())

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
                self.assertItemsEqual(regional_options.keys(), app_spec["regionalOptions"].keys())

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
        app_name = "asset_{t}_multi_region_dxapp_json_with_regional_system_requirements".format(t=int(time.time()))

        aws_us_east_system_requirements = dict(main=dict(instanceType="mem2_hdd2_x1"))
        azure_westus_system_requirements = dict(main=dict(instanceType="azure:mem2_ssd1_x1"))
        app_spec = dict(self.base_app_spec, name=app_name,
                        regionalOptions={"aws:us-east-1": dict(systemRequirements=aws_us_east_system_requirements),
                                         "azure:westus": dict(systemRequirements=azure_westus_system_requirements)})
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")

        for region in ("aws:us-east-1", "azure:westus"):
            with temporary_project(region=region, select=True):
                applet_id = json.loads(run("dx build --json " + app_dir))["id"]
                applet_desc = dxpy.api.applet_describe(applet_id)
                bundled_depends_by_region = applet_desc["runSpec"]["bundledDependsByRegion"]
                self.assertEqual(bundled_depends_by_region.keys(), [region])

    def test_build_multi_region_app_without_regional_options(self):
        app_name = "asset_{t}_multi_region_app".format(t=int(time.time()))
        app_spec = dict(self.base_app_spec, name=app_name)
        app_dir = self.write_app_directory(app_name, json.dumps(app_spec), "code.py")

        cmd = "dx build --create-app --region aws:us-east-1 --region azure:westus --json {app_dir}".format(
                app_dir=app_dir)
        app_new_res = json.loads(run(cmd))
        app_desc_res = json.loads(run("dx describe --json " + app_new_res["id"]))
        self.assertEqual(app_desc_res["class"], "app")
        self.assertEqual(app_desc_res["id"], app_desc_res["id"])
        self.assertEqual(app_desc_res["version"], "1.0.0")
        self.assertEqual(app_desc_res["name"], app_name)
        self.assertFalse("published" in app_desc_res)
        self.assertIn("regionalOptions", app_desc_res)
        self.assertItemsEqual(app_desc_res["regionalOptions"].keys(), ["aws:us-east-1", "azure:westus"])

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
            with self.assertRaisesRegexp(DXCalledProcessError, error_message):
                run(cmd)

            # Build an app where both top-level "resources" and "regionalOptions" are set
            app_name = "asset_{t}_multi_region_app_resources".format(t=int(time.time()))
            app_spec_resources_regionalOpts = dict(self.base_app_spec, name=app_name,
                            resources = tmp_project.get_id(),
                            regionalOptions={region_0: dict(resources=tmp_project.get_id())})
            app_dir = self.write_app_directory(app_name,
                                               json.dumps(app_spec_resources_regionalOpts),
                                               "code.py")
            with self.assertRaisesRegexp(DXCalledProcessError, error_message):
                run("dx build --create-app --json " + app_dir)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV and testutil.TEST_AZURE,
                         'skipping test that would create apps')
    def test_update_multi_region_app(self):
        app_name = "asset_{t}_multi_region_app".format(t=int(time.time()))
        app_spec = {
            "name": app_name,
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "regionalOptions": {"aws:us-east-1": {},
                                "azure:westus": {}}
            }
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
        self.assertItemsEqual(app_desc_res["regionalOptions"].keys(), app_spec["regionalOptions"].keys())

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_build_app_and_update_code(self):
        app_spec = {
            "name": "update_app_code",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
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
        app_spec = {
            "name": "test_build_app_and_pretend_to_update_devs",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "developers": ['user-dnanexus']
            }
        app_dir = self.write_app_directory("test_build_app_and_pretend_to_update_devs",
                                           json.dumps(app_spec), "code.py")

        # Without --yes, the build will succeed except that it will skip
        # the developer update
        self.run_and_assert_stderr_matches('dx build --create-app --json ' + app_dir,
                                           'skipping requested change to the developer list')
        app_developers = dxpy.api.app_list_developers('app-test_build_app_and_pretend_to_update_devs')['developers']
        self.assertEqual(len(app_developers), 1) # the id of the user we are calling as

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create apps')
    def test_build_app_and_update_devs(self):
        app_spec = {
            "name": "test_build_app_and_update_devs",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
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
        app_spec = {
            "name": "minimal_app_regions",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("minimal_app_regions", json.dumps(app_spec), "code.py")
        new_app = json.loads(run("dx build --create-app --region aws:us-east-1 --json " + app_dir))
        app_describe = json.loads(run("dx describe --json " + new_app["id"]))
        self.assertEqual(app_describe["region"], "aws:us-east-1")

        with self.assertRaisesRegexp(subprocess.CalledProcessError, "InvalidInput"):
            run("dx build --create-app --region aws:not-a-region --json " + app_dir)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
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
        with self.assertRaisesRegexp(DXCalledProcessError, "PermissionDenied"):
            run("dx build --app --bill-to {} --json {}".format(org_id, app_dir))

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_invalid_project_context(self):
        app_spec = {
            "name": "invalid_project_context",
            "dxapi": "1.0.0",
            "runSpec": {
                "file": "code.py",
                "interpreter": "python2.7"
                },
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
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
        app_spec = {
            "name": "invalid_authorized_users",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "authorizedUsers": "PUBLIC"
            }
        app_dir = self.write_app_directory("invalid_authorized_users", json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp='Expected authorizedUsers to be a list of strings'):
            run("dx build --json " + app_dir)

        app_spec["authorizedUsers"] = ["foo"]
        app_dir = self.write_app_directory("invalid_authorized_users_2", json.dumps(app_spec),
                                           "code.py")
        with self.assertSubprocessFailure(stderr_regexp='contains an entry which is not'):
            run("dx build --json " + app_dir)

    def test_duplicate_keys_in_spec(self):
        app_spec = {
            "name": "test_duplicate_keys_in_spec",
            "dxapi": "1.0.0",
            "runSpec": {
                "file": "code.py",
                "interpreter": "python2.7"
            },
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        spec = json.dumps(app_spec).replace('"file": "code.py"', '"file": "code.py", "file": "code.py"')
        app_dir = self.write_app_directory("duplicate_keys_in_spec", spec, "code.py")
        with self.assertSubprocessFailure(stderr_regexp="duplicate key: "):
            run("dx build --json " + app_dir)

    def test_deps_without_network_access(self):
        app_spec = {
            "name": "test_deps_without_network_access",
            "dxapi": "1.0.0",
            "runSpec": {
                "file": "code.py",
                "interpreter": "python2.7",
                "execDepends": [{"name": "ddd", "package_manager": "pip"}]
                },
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("deps_without_network_access", json.dumps(app_spec),
                                           "code.py")
        with self.assertSubprocessFailure(stderr_regexp=("runSpec.execDepends specifies non-APT " +
                                                         "dependencies, but no network access spec " +
                                                         "is given")):
            run("dx build --json " + app_dir)

    def test_overwrite_applet(self):
        app_spec = {
            "name": "applet_overwriting",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
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
        app_spec = {
            "name": "applet_overwriting",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }

        app_dir = self.write_app_directory("applet_overwriting", json.dumps(app_spec), "code.py")

        # Create two applets in different directories, but with the same name
        run("dx mkdir subfolder1")
        app_1_spec = json.loads(run("dx build --json --destination=subfolder1/applet_overwriting " + app_dir))
        run("dx mkdir subfolder2")
        app_2_spec = json.loads(run("dx build --json --destination=subfolder2/applet_overwriting " + app_dir))

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
        app_final_spec = json.loads(run("dx build -f --json --destination=subfolder1/applet_overwriting " + app_dir))

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
        app1_spec = {
            "name": "update_app_categories",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "categories": ["A"]
            }
        app2_spec = {
            "name": "update_app_categories",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.1",
            "categories": ["B"]
            }
        app_dir = self.write_app_directory("update_app_categories", json.dumps(app1_spec), "code.py")
        app_id = json.loads(run("dx build --create-app --json " + app_dir))['id']
        self.assertEquals(json.loads(run("dx api " + app_id + " listCategories"))["categories"], ['A'])
        shutil.rmtree(app_dir)
        self.write_app_directory("update_app_categories", json.dumps(app2_spec), "code.py")
        run("dx build --create-app --json " + app_dir)
        self.assertEquals(json.loads(run("dx api " + app_id + " listCategories"))["categories"], ['B'])

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create apps')
    def test_update_app_authorized_users(self):
        app0_spec = {
            "name": "update_app_authorized_users",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "0.0.1"
            }
        app1_spec = {
            "name": "update_app_authorized_users",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "authorizedUsers": []
            }
        app2_spec = {
            "name": "update_app_authorized_users",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.1",
            "authorizedUsers": ["user-eve"]
            }
        app_dir = self.write_app_directory("update_app_authorized_users", json.dumps(app0_spec),
                                           "code.py")
        app_id = json.loads(run("dx build --create-app --json " + app_dir))['id']
        self.assertEquals(json.loads(run("dx api " + app_id +
                                         " listAuthorizedUsers"))["authorizedUsers"], [])
        shutil.rmtree(app_dir)
        self.write_app_directory("update_app_authorized_users", json.dumps(app1_spec), "code.py")
        run("dx build --create-app --json " + app_dir)
        self.assertEquals(json.loads(run("dx api " + app_id +
                                         " listAuthorizedUsers"))["authorizedUsers"], [])
        shutil.rmtree(app_dir)
        self.write_app_directory("update_app_authorized_users", json.dumps(app2_spec), "code.py")
        run("dx build --create-app --yes --json " + app_dir)
        self.assertEquals(json.loads(run("dx api " + app_id +
                                         " listAuthorizedUsers"))["authorizedUsers"], ["user-eve"])

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_dx_add_list_remove_users(self):
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
            app_spec = {
                "name": "test_dx_users",
                "dxapi": "1.0.0",
                "runSpec": {"file": "code.py", "interpreter": "python2.7"},
                "inputSpec": [],
                "outputSpec": [],
                "version": "0.0.1"
                }
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

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_dx_add_list_remove_developers(self):
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
            app_spec = {
                "name": "test_dx_developers",
                "dxapi": "1.0.0",
                "runSpec": {"file": "code.py", "interpreter": "python2.7"},
                "inputSpec": [],
                "outputSpec": [],
                "version": "0.0.1"
                }
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

        # Raise an error if you try to add an org developer (currently unsupported by the API)
        with self.assertSubprocessFailure(stderr_regexp='unsupported', exit_code=3):
            run('dx add developers test_dx_developers org-piratelabs')

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that would create apps')
    def test_build_app_autonumbering(self):
        app_spec = {
            "name": "build_app_autonumbering",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("build_app_autonumbering", json.dumps(app_spec), "code.py")
        run("dx build --create-app --json --publish " + app_dir)
        with self.assertSubprocessFailure(stderr_regexp="Could not create"):
            print(run("dx build --create-app --json --no-version-autonumbering " + app_dir))
        run("dx build --create-app --json " + app_dir) # Creates autonumbered version

    def test_build_failure(self):
        app_spec = {
            "name": "build_failure",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("build_failure", json.dumps(app_spec), "code.py")
        with open(os.path.join(app_dir, 'Makefile'), 'w') as makefile:
            makefile.write("all:\n\texit 7")
        with self.assertSubprocessFailure(stderr_regexp="make -j[0-9]+ in target directory failed with exit code"):
            run("dx build " + app_dir)
        # Somewhat indirect test of --no-parallel-build
        with self.assertSubprocessFailure(stderr_regexp="make in target directory failed with exit code"):
            run("dx build --no-parallel-build " + app_dir)

    def test_syntax_checks(self):
        app_spec = {
            "name": "syntax_checks",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
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
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
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
        self.assertEquals(invocation_job_describe['output']['out1'], 8675310)

    def test_applet_help(self):
        app_spec = {
            "name": "applet_help",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [
                {"name": "reads", "class": "array:gtable", "type": "LetterReads", "label": "Reads",
                 "help": "One or more Reads table objects."},
                {"name": "required", "class": "file", "label": "Required", "help": "Another parameter"},
                {"name": "optional", "class": "file", "label": "Optional",
                 "help": "Optional parameter", "optional": True}
            ],
            "outputSpec": [
                {"name": "mappings", "class": "gtable", "type": "LetterMappings", "label": "Mappings",
                 "help": "The mapped reads."}
            ],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("åpplet_help", json.dumps(app_spec),
                                           code_filename="code.py", code_content="")
        applet_id = json.loads(run("dx build --json " + app_dir))["id"]
        applet_help = run("dx run " + applet_id + " -h")
        self.assertTrue("Reads: -ireads=(gtable, type LetterReads) [-ireads=... [...]]" in applet_help)
        self.assertTrue("Required: -irequired=(file)" in applet_help)
        self.assertTrue("Optional: [-ioptional=(file)]" in applet_help)
        self.assertTrue("Mappings: mappings (gtable, type LetterMappings)" in applet_help)

    def test_upload_resources(self):
        run("dx mkdir /subfolder")
        cd("/subfolder")
        app_spec = {
            "name": "upload_resources",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("upload_app_resources", json.dumps(app_spec), "code.py")
        os.mkdir(os.path.join(app_dir, 'resources'))
        with open(os.path.join(app_dir, 'resources', 'test.txt'), 'w') as resources_file:
            resources_file.write('test\n')
        new_applet = json.loads(run("dx build --json " + app_dir))
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
        new_applet = json.loads(run("dx build -f --json " + args + " " + app_dir))
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
        app_spec = {
            "name": "upload_resources_symlink",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
        }
        test_symlink_dir = "upload_resources_symlink"
        os.mkdir(os.path.join(self.temp_file_path, test_symlink_dir))
        app_dir = self.write_app_directory(os.path.join(self.temp_file_path, test_symlink_dir, 'app'), json.dumps(app_spec), "code.py")
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

        with self.assertSubprocessFailure(stderr_regexp="Cannot include symlinks to directories outside of the resource directory"):
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
        os.symlink(os.path.join(app_dir, os.pardir, 'lib', 'remote_file'), os.path.join(app_dir, os.pardir, 'outside_link'))
        os.symlink(os.path.join(os.pardir, os.pardir, 'outside_link'), os.path.join(app_dir, 'resources', 'symbolic_link'))

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
        os.symlink(os.path.join(app_dir, 'resources', 'test_file2.txt'), os.path.join(app_dir, 'resources', 'symbolic_link'))

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

        with self.assertSubprocessFailure(stderr_regexp="Cannot include symlinks to directories outside of the resource directory"):
            run("dx build -f " + app_dir)

        # == Absolute links to directories are keps when --force-symlinks is used
        res_temp_dir = self._build_check_resources(app_dir, "--force-symlinks")

        # Test: symbolic_link is a symlink
        self.assertTrue(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))

        # ==== Case 8 ====

        # == Relative link to a file that extends outside the resource path is dereferenced
        os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))
        os.symlink(os.path.join(os.pardir, 'resources', 'test_file2.txt'), os.path.join(app_dir, 'resources', 'symbolic_link'))

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
        os.symlink(os.path.join(os.pardir, 'resources', 'local_dir'), os.path.join(app_dir, 'resources', 'symbolic_link'))

        with self.assertSubprocessFailure(stderr_regexp="Cannot include symlinks to directories outside of the resource directory"):
            run("dx build -f " + app_dir)

        # == Relative link to directory extending outside resource path are kept when --force-symlinks is used
        res_temp_dir = self._build_check_resources(app_dir, "--force-symlinks")

        # Test: symbolic_link is a symlink
        self.assertTrue(os.path.islink(os.path.join(res_temp_dir, 'symbolic_link')))

        shutil.rmtree(res_temp_dir)

    def test_upload_resources_permissions(self):
        app_spec = {
            "name": "upload_resources_permissions",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
        }
        test_perms_dir = "upload_resources_permissions"
        os.mkdir(os.path.join(self.temp_file_path, test_perms_dir))
        app_dir = self.write_app_directory(os.path.join(self.temp_file_path, test_perms_dir, 'app'), json.dumps(app_spec), "code.py")
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
        self.assertEquals(os.stat(os.path.join(res_temp_dir, "test_644.txt")).st_mode,
            stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
        # 660 => 664
        #self.assertEquals(os.stat(os.path.join(res_temp_dir, "test_660.txt")).st_mode,
        #    stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)
        # 400 => 444
        self.assertEquals(os.stat(os.path.join(res_temp_dir, "test_400.txt")).st_mode,
            stat.S_IFREG | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        # 755 => 755
        #self.assertEquals(os.stat(os.path.join(res_temp_dir, "test_755.txt")).st_mode,
        #    stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
        # 770 => 775
        #self.assertEquals(os.stat(os.path.join(res_temp_dir, "test_770.txt")).st_mode,
        #    stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
        # 670 => 674
        #self.assertEquals(os.stat(os.path.join(res_temp_dir, "test_670.txt")).st_mode,
        #    stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | stat.S_IROTH)

        shutil.rmtree(res_temp_dir)

        # If we make a permission change that does NOT affect the tar, we should re-use the resource bundle
        id1 = self._build_check_resources(app_dir, extract_resources=False)

        # change the 400 => 444
        os.chmod(os.path.join(app_dir, 'resources', 'test_400.txt'),
            stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

        id2 = self._build_check_resources(app_dir, extract_resources=False)

        self.assertEquals(id1, id2)

        # If we make a permission change that DOES affect the tar, we should rebuild

        # change 400 => 500 (will result in perms 555)
        os.chmod(os.path.join(app_dir, 'resources', 'test_400.txt'),
            stat.S_IRUSR | stat.S_IXUSR)

        id3 = self._build_check_resources(app_dir, extract_resources=False)

        self.assertNotEquals(id1, id3)


    def test_upload_resources_advanced(self):
        app_spec = {
            "name": "upload_resources_advanced",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("upload_åpp_resources_advanced", json.dumps(app_spec), "code.py")
        os.mkdir(os.path.join(app_dir, 'resources'))

        with open(os.path.join(app_dir, 'test_file1.txt'), 'w') as file1:
            file1.write('test_file1\n')  # Not in resources folder, so will not affect checksum
        with open(os.path.join(app_dir, 'resources', 'test_file2.txt'), 'w') as resources_file2:
            resources_file2.write('test_file2\n')

        # Create symbolic link to test_file1.txt
        if 'symbolic_link' in os.listdir(os.path.join(app_dir, 'resources')):
            os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))
        os.symlink(os.path.join(app_dir, 'test_file1.txt'), os.path.join(app_dir, 'resources', 'symbolic_link'))

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

        os.symlink(os.path.join(app_dir, 'outside_file1.txt'), os.path.join(app_dir, 'outside_link'))

        # First, testing dereferencing of links
        # Create a link to be dereferenced
        if 'symbolic_link' in os.listdir(os.path.join(app_dir, 'resources')):
            os.remove(os.path.join(app_dir, 'resources', 'symbolic_link'))

        # NOTE: we're going to have to use a link to a link in order to avoid modifying the directory mtime
        os.symlink(os.path.join(app_dir, 'outside_link'), os.path.join(app_dir, 'resources', 'symbolic_link'))

        idr1 = self._build_check_resources(app_dir, "", False)

        # Update the link target; modify target mtime
        with open(os.path.join(app_dir, 'outside_file1.txt'), 'w') as fn:
            fn.write('test_file1 Update!\n')

        idr2 = self._build_check_resources(app_dir, "", False)

        self.assertNotEqual(idr1, idr2) # Upload should happen

        # Change link destination; target mtime change
        os.remove(os.path.join(app_dir, 'outside_link'))
        os.symlink(os.path.join(app_dir, 'outside_file2.txt'), os.path.join(app_dir, 'outside_link'))

        idr3 = self._build_check_resources(app_dir, "", False)

        # New Upload should happen
        self.assertNotEqual(idr2, idr3)

        # Add another link the the chain, but eventual destination doesn't change
        os.remove(os.path.join(app_dir, 'outside_link'))
        os.symlink(os.path.join(app_dir, 'outside_file2.txt'), os.path.join(app_dir, 'outside_link_1'))
        os.symlink(os.path.join(app_dir, 'outside_link_1'), os.path.join(app_dir, 'outside_link'))

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
        app_spec = {
            "name": "archive_in_another_project",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("archive_in_another_project", json.dumps(app_spec), "code.py")

        with temporary_project("Temporary working project", select=True) as temp_project:
            orig_applet = json.loads(run("dx build --json -d {p}: {app_dir}".format(
                p=self.project, app_dir=app_dir)))["id"]
            new_applet = json.loads(run("dx build --json --archive -d {p}: {app_dir}".format(
                p=self.project, app_dir=app_dir)))["id"]
            self.assertEqual(dxpy.DXApplet(orig_applet).describe(incl_properties=True)["properties"]["replacedWith"],
                             new_applet)

    def test_categories_propagated_to_tags(self):
        app_spec = {
            "name": "categories_propagated_to_tags",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "tags": ["mytag"],
            "categories": ["Import"]
            }
        app_dir = self.write_app_directory("categories_propagated_to_tags", json.dumps(app_spec), "code.py")
        applet_id = json.loads(run("dx build --json -d categories1 " + app_dir))["id"]
        self.assertEqual(set(dxpy.DXApplet(applet_id).describe()["tags"]),
                         set(["mytag", "Import"]))

        app_spec2 = {
            "name": "categories_propagated_to_tags",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "categories": ["Import"]
            }
        app_dir2 = self.write_app_directory("categories_propagated_to_tags", json.dumps(app_spec2), "code.py")
        applet_id2 = json.loads(run("dx build --json -d categories2 " + app_dir2))["id"]
        self.assertEqual(set(dxpy.DXApplet(applet_id2).describe()["tags"]),
                         set(["Import"]))

    def test_bundled_depends_reuse(self):
        app_spec = {
            "name": "bundled_depends_reuse",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("bundled_depends_reuse", json.dumps(app_spec), "code.py")
        os.mkdir(os.path.join(app_dir, 'resources'))
        with open(os.path.join(app_dir, 'resources', 'foo.txt'), 'w') as file_in_resources:
            file_in_resources.write('foo\n')

        first_applet = json.loads(run("dx build --json -d {p}:applet1 {app_dir}".format(
            p=self.project, app_dir=app_dir)))["id"]
        second_applet = json.loads(run("dx build --json -d {p}:applet2 {app_dir}".format(
            p=self.project, app_dir=app_dir)))["id"]

        # The second applet should reuse the bundle from the first.

        # touch foo.txt
        os.utime(os.path.join(app_dir, 'resources', 'foo.txt'), None)

        # But the third applet should not share with the first two,
        # because the resources have been touched in between.

        third_applet = json.loads(run("dx build --json -d {p}:applet3 {app_dir}".format(
            p=self.project, app_dir=app_dir)))["id"]

        self.assertEquals(
            dxpy.DXApplet(first_applet).describe()['runSpec']['bundledDepends'][0]['id']['$dnanexus_link'],
            dxpy.DXApplet(second_applet).describe()['runSpec']['bundledDepends'][0]['id']['$dnanexus_link']
        )
        self.assertNotEqual(
            dxpy.DXApplet(first_applet).describe()['runSpec']['bundledDepends'][0]['id']['$dnanexus_link'],
            dxpy.DXApplet(third_applet).describe()['runSpec']['bundledDepends'][0]['id']['$dnanexus_link']
        )

    def test_bundled_depends_reuse_with_force(self):
        app_spec = {
            "name": "bundled_depends_reuse_with_force",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
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
        self.assertEquals(first_bundled_resources, second_bundled_resources)
        # ...and that the first applet has been removed
        with self.assertSubprocessFailure(exit_code=3):
            run("dx describe " + first_applet)

    @unittest.skipUnless(testutil.TEST_ENV, 'skipping test that would clobber your local environment')
    def test_build_without_context(self):
        app_spec = {
            "name": "applet_without_context",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
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
        record_name = "asset-record"
        record_details = {"archiveFileId": {"$dnanexus_link": asset_file.get_id()}}
        record_properties = {"version": "0.0.1"}
        dxpy.new_dxrecord(project=self.project, types=["AssetBundle"], details=record_details, name=record_name,
                          properties=record_properties, close=True)

        app_spec = {
            "name": "asset_depends",
            "dxapi": "1.0.0",
            "runSpec": {
                "file": "code.py",
                "interpreter": "python2.7",
                "assetDepends": [{"name": record_name, "version": "0.0.1", "project": self.project}]
            },
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("asset_depends", json.dumps(app_spec), "code.py")
        asset_applet = json.loads(run("dx build --json {app_dir}".format(app_dir=app_dir)))["id"]

        self.assertEquals(
            dxpy.DXApplet(asset_applet).describe()['runSpec']['bundledDepends'][0],
            {'id': {'$dnanexus_link': asset_file.get_id()}, 'name': asset_name}
        )

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

        app_spec = {
            "name": "asset_depends",
            "dxapi": "1.0.0",
            "runSpec": {
                "file": "code.py",
                "interpreter": "python2.7",
                "assetDepends": [{"id": record.get_id()}]
            },
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("asset_depends", json.dumps(app_spec), "code.py")
        asset_applet = json.loads(run("dx build --json {app_dir}".format(app_dir=app_dir)))["id"]
        self.assertEquals(
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

        app_spec = {
            "name": "asset_depends",
            "dxapi": "1.0.0",
            "runSpec": {
                "file": "code.py",
                "interpreter": "python2.7",
                "assetDepends": [{"name": record_name, "version": "0.1.1", "project": self.project}]
            },
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
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

        app_spec = {
            "name": "asset_depends",
            "dxapi": "1.0.0",
            "runSpec": {
                "file": "code.py",
                "interpreter": "python2.7",
                "assetDepends": [{"name": record_name, "version": "0.0.1", "project": self.project}]
            },
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
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
            app_spec = {
                "name": "asset_depends",
                "dxapi": "1.0.0",
                "runSpec": {
                    "file": "code.py",
                    "interpreter": "python2.7",
                    "assetDepends": [{"id": record.get_id()}]
                },
                "inputSpec": [],
                "outputSpec": [],
                "version": "1.0.0"
                }
            app_dir = self.write_app_directory("asset_depends", json.dumps(app_spec), "code.py")
            run("dx build --json {app_dir}".format(app_dir=app_dir))
            temp_record_id = run("dx ls {asset} --brief".format(asset=record_name)).strip()
            self.assertEquals(temp_record_id, record.get_id())

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
                                     "interpreter": "python2.7",
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

        app_spec = {
            "name": "asset_depends",
            "dxapi": "1.0.0",
            "runSpec": {
                "file": "code.py",
                "interpreter": "python2.7",
                "assetDepends": [{"name": record_name, "version": "0.0.1", "project": self.project}]
            },
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("asset_depends", json.dumps(app_spec), "code.py")
        asset_applet = json.loads(run("dx build --json {app_dir}".format(app_dir=app_dir)))["id"]

        # clone the applet to a different project and test that the hidden file is also cloned
        with temporary_project('test_select_project', select=True) as temp_project:
            dxpy.DXApplet(asset_applet, project=self.project).clone(temp_project.get_id())
            # check that asset_file is also cloned to this project
            temp_asset_fid = run("dx ls {asset} --brief".format(asset=asset_name)).strip()
            self.assertEquals(temp_asset_fid, asset_file.get_id())


class TestDXGetExecutables(DXTestCaseBuildApps):
    def test_get_workflow(self):
        workflow_id = run("dx new workflow get_workflow --brief").strip()
        applet_01_id = dxpy.api.applet_new({"name": "myapplet_01",
                                            "project": self.project,
                                            "dxapi": "1.0.0",
                                            "inputSpec": [{"name": "my_number_in_01", "class": "int"}],
                                            "outputSpec": [{"name": "my_number_out_01", "class": "int"}],
                                            "runSpec": {"interpreter": "bash", "code": "exit 0"}})['id']
        applet_02_id = dxpy.api.applet_new({"name": "myapplet_02",
                                            "project": self.project,
                                            "dxapi": "1.0.0",
                                            "inputSpec": [{"name": "my_number_in_02", "class": "int"}],
                                            "outputSpec": [{"name": "my_number_out_02", "class": "int"}],
                                            "runSpec": {"interpreter": "bash", "code": "exit 0"}})['id']
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
            "outputFolder": None,
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

    def test_get_applet(self):
        # TODO: not sure why self.assertEqual doesn't consider
        # assertEqual to pass unless the strings here are unicode strings
        app_spec = {
            "name": "get_applet",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
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
            }
        # description and developerNotes should be un-inlined back to files
        output_app_spec = dict((k, v) for (k, v) in app_spec.iteritems() if k not in ('description',
                                                                                      'developerNotes'))
        output_app_spec["runSpec"] = {"file": "src/code.py", "interpreter": "python2.7"}

        app_dir = self.write_app_directory("get_åpplet", json.dumps(app_spec), "code.py",
                                           code_content="import os\n")
        os.mkdir(os.path.join(app_dir, "resources"))
        with open(os.path.join(app_dir, "resources", "resources_file"), 'w') as f:
            f.write('content\n')
        new_applet_id = json.loads(run("dx build --json " + app_dir))["id"]
        with chdir(tempfile.mkdtemp()):
            run("dx get " + new_applet_id)
            self.assertTrue(os.path.exists("get_applet"))
            self.assertTrue(os.path.exists(os.path.join("get_applet", "dxapp.json")))

            applet_metadata = open(os.path.join("get_applet", "dxapp.json")).read()

            # Checking inputSpec/outputSpec patterns arrays were flattened
            self.assertTrue(applet_metadata.find('"patterns": ["*.bam", "*.babam", "*.pab\\"abam"]') >= 0)
            self.assertTrue(applet_metadata.find('"patterns": ["*.bam"]') >= 0)

            # Checking inputSpec keys ordering
            self.assertTrue(applet_metadata.find('"name": "in1"') < applet_metadata.find('"label": "A label for in1 input param"'))
            self.assertTrue(applet_metadata.find('"label": "A label for in1 input param"') < applet_metadata.find('"help": "A help for in1 input param"'))
            self.assertTrue(applet_metadata.find('"help": "A help for in1 input param"') < applet_metadata.find('"class": "file"'))
            self.assertTrue(applet_metadata.find('"class": "file"') < applet_metadata.find('"patterns": ["*.bam", "*.babam", "*.pab\\"abam"]'))
            self.assertTrue(applet_metadata.find('"patterns": ["*.bam", "*.babam", "*.pab\\"abam"]') < applet_metadata.find('"optional": false'))

            self.assertTrue(applet_metadata.find('"name": "reads_type"') < applet_metadata.find('"class": "string"'))
            self.assertTrue(applet_metadata.find('"class": "string"') < applet_metadata.find('"default": "paired-end"'))
            self.assertTrue(applet_metadata.find('"default": "paired-end"') < applet_metadata.find('"choices": ['))
            self.assertTrue(applet_metadata.find('"choices": [') < applet_metadata.find('"group": "Advanced Options"'))

            # Checking outputSpec keys ordering
            output_spec_index = applet_metadata.find('"outputSpec"')
            self.assertTrue(applet_metadata.find('"name": "out1"', output_spec_index) < applet_metadata.find('"class": "file"', output_spec_index))
            self.assertTrue(applet_metadata.find('"class": "file"', output_spec_index) < applet_metadata.find('"patterns": ["*.bam"]', output_spec_index))

            output_json = json.loads(applet_metadata)
            self.assertEqual(output_app_spec, output_json)
            self.assertNotIn("bundledDepends", output_json["runSpec"])

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
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
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
        output_app_spec = dict((k, v) for (k, v) in app_spec.iteritems() if k not in ('description',
                                                                                      'developerNotes'))
        output_app_spec["runSpec"] = {"file": "src/code.py", "interpreter": "python2.7"}

        app_dir = self.write_app_directory("get_åpplet", json.dumps(app_spec), "code.py",
                                           code_content="import os\n")
        os.mkdir(os.path.join(app_dir, "resources"))
        with open(os.path.join(app_dir, "resources", "resources_file"), 'w') as f:
            f.write('content\n')
        new_applet_id = json.loads(run("dx build --json " + app_dir))["id"]
        with chdir(tempfile.mkdtemp()):
            run("dx get --omit-resources " + new_applet_id)
            self.assertFalse(os.path.exists(os.path.join("get_applet", "resources")))

            output_json = json.load(open(os.path.join("get_applet", "dxapp.json")))
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
        app_spec = {
            "name": "get_applet_field_cleanup",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": []
            }
        output_app_spec = app_spec.copy()
        output_app_spec["runSpec"] = {"file": "src/code.py", "interpreter": "python2.7"}

        app_dir = self.write_app_directory("get_åpplet_field_cleanup", json.dumps(app_spec), "code.py",
                                           code_content="import os\n")
        os.mkdir(os.path.join(app_dir, "resources"))
        with open(os.path.join(app_dir, "resources", "resources_file"), 'w') as f:
            f.write('content\n')
        new_applet_id = json.loads(run("dx build --json " + app_dir))["id"]
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
        app_spec = {
            "name": "get_applet_windows",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": []
            }
        output_app_spec = app_spec.copy()
        output_app_spec["runSpec"] = {"file": "src/code.py", "interpreter": "python2.7"}

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
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
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
        output_app_spec["runSpec"] = {"file": "src/code.py", "interpreter": "python2.7"}

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
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
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
        output_app_spec["runSpec"] = {"file": "src/code.py", "interpreter": "python2.7"}

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
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
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
        app_name = "asset_{t}_multi_region_app_with_regional_system_requirements".format(t=int(time.time()))

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
        html = "<html><head><link rel='stylesheet' href='index.css' type='text/css'/><script src='index.js'></script></head><body><a href='/'/><a href='/' target='_new'/><img src='img.gif'/><img src='{}'/></body></html>".format(wiki_logo)
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
        self.assertEquals(len(re.split("src=\"data:image", html)), 3)
        self.assertEquals(len(re.split("<img", html)), 3)
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
        report = json.loads(run("dx-build-report-html {d}/index.html --remote /html_report -w 47 -g 63".format(d=self.temp_file_path)))
        fileId = report["fileIds"][0]
        desc = json.loads(run("dx describe {record} --details --json".format(record=report["recordId"])))
        self.assertEquals(desc["types"], ["Report", "HTMLReport"])
        self.assertEquals(desc["name"], "html_report")
        self.assertEquals(desc["details"]["files"][0]["$dnanexus_link"], fileId)
        self.assertEquals(desc["details"]["width"], "47")
        self.assertEquals(desc["details"]["height"], "63")
        desc = json.loads(run("dx describe {file} --details --json".format(file=fileId)))
        self.assertTrue(desc["hidden"])
        self.assertEquals(desc["name"], "index.html")
        run("dx rm {record} {file}".format(record=report["recordId"], file=fileId))


@unittest.skipUnless(testutil.TEST_TCSH, 'skipping tests that require tcsh to be installed')
class TestTcshEnvironment(unittest.TestCase):
    def test_tcsh_dash_c(self):
        # tcsh -c doesn't set $_, or provide any other way for us to determine the source directory, so
        # "source environment" only works from DNANEXUS_HOME
        run('cd $DNANEXUS_HOME && env - HOME=$HOME PATH=/usr/local/bin:/usr/bin:/bin tcsh -c "source /etc/csh.cshrc && source /etc/csh.login && source $DNANEXUS_HOME/environment && dx --help"')
        run('cd $DNANEXUS_HOME && env - HOME=$HOME PATH=/usr/local/bin:/usr/bin:/bin tcsh -c "source /etc/csh.cshrc && source /etc/csh.login && source $DNANEXUS_HOME/environment.csh && dx --help"')

    def test_tcsh_source_environment(self):
        tcsh = pexpect.spawn("env - HOME=$HOME PATH=/usr/local/bin:/usr/bin:/bin tcsh")
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
        run('dx-variants-to-vcf -h')
        run('dx-genes-to-gff -h')
        run('dx-genes-to-gtf -h')
        run('dx-mappings-to-fastq -h')
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

        #cleanup
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
        self.assertRegexpMatches(o, r"closed\s+\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+foo \(" + rec.get_id() + "\)")


class TestDXTree(DXTestCase):
    def test_regular_output(self):
        dxpy.new_dxrecord(project=self.project, name="foo", close=True)
        o = run("dx tree")
        self.assertEqual(o.strip(), '.\n└── foo')

    def test_tree(self):
        rec = dxpy.new_dxrecord(project=self.project, name="foo", close=True)
        o = run("dx tree -l")
        self.assertRegexpMatches(o.strip(),
                                 r".\n└── closed\s+\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+foo \(" + rec.get_id() + "\)")


if __name__ == '__main__':
    if 'DXTEST_FULL' not in os.environ:
        sys.stderr.write('WARNING: env var DXTEST_FULL is not set; tests that create apps or run jobs will not be run\n')
    unittest.main()
