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


@pytest.mark.serial
class TestDXClientFind(DXTestCase):

    def assert_cmd_gives_ids(self, cmd, ids):
        self.assertEqual(set(execid.strip() for execid in run(cmd).splitlines()),
                         set(ids))

    def test_dx_find_apps_and_globalworkflows_category(self):
        # simple test here does not assume anything about apps that do
        # or do not exist
        from dxpy.app_categories import APP_CATEGORIES

        category_help_apps = run("dx find apps --category-help")
        for category in APP_CATEGORIES:
            self.assertIn(category, category_help_apps)
        run("dx find apps --category foo")  # any category can be searched

        category_help_workflows = run("dx find globalworkflows --category-help")
        for category in APP_CATEGORIES:
            self.assertIn(category, category_help_workflows)
        run("dx find globalworkflows --category foo")  # any category can be searched

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that creates apps')
    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_API_APP_PUBLISH"])
    def test_dx_find_apps(self):
        test_applet_id = dxpy.api.applet_new({"name": "my_find_applet",
                                              "dxapi": "1.0.0",
                                              "project": self.project,
                                              "inputSpec": [],
                                              "outputSpec": [],
                                              "runSpec": {"interpreter": "bash",
                                                          "distribution": "Ubuntu",
                                                          "release": "14.04",
                                                          "code": "exit 0"}
                                              })['id']

        dxapp_spec = {
            "name": "app_find",
            "version": "0.0.1",
            "applet": test_applet_id
        }

        # Create a few apps

        # 1. Unpublished app
        # version 0.0.1
        app_find = "app_find_unpublished"
        spec = dict(dxapp_spec, name=app_find, version="0.0.1")
        dxapp = dxpy.DXApp()
        dxapp.new(**spec)
        # version 0.0.2
        spec = dict(dxapp_spec, name=app_find, version="0.0.2")
        dxapp = dxpy.DXApp()
        dxapp.new(**spec)
        desc = dxapp.describe()
        self.assertEqual(desc["version"], "0.0.2")

        # 2. Published app
        app_find_published_1 = "app_find_published_1"
        spec = dict(dxapp_spec, name=app_find_published_1, version="0.0.3")
        dxapp = dxpy.DXApp()
        dxapp.new(**spec)
        dxapp.publish()
        desc = dxapp.describe()
        self.assertTrue(desc["published"] > 0)

        # 3. Published app
        app_find_published_2 = "app_find_published_2"
        spec = dict(dxapp_spec, name=app_find_published_2, version="0.0.4")
        dxapp = dxpy.DXApp()
        dxapp.new(**spec)
        dxapp.publish()
        desc = dxapp.describe()
        self.assertTrue(desc["published"] > 0)

        # Tests

        # find only published
        output = run("dx find apps")
        self.assertIn(app_find_published_1, output)
        self.assertIn(app_find_published_2, output)
        self.assertNotIn(app_find, output)

        # find only unpublished
        output = run("dx find apps --unpublished")
        self.assertIn(app_find, output)
        self.assertNotIn(app_find_published_1, output)
        self.assertNotIn(app_find_published_2, output)

        # find by name
        output = run("dx find apps --name " + app_find_published_1)
        self.assertIn(app_find_published_1, output)
        self.assertNotIn(app_find_published_2, output)
        self.assertNotIn(app_find, output)

        # find all versions
        output = run("dx find apps --unpublished --all")
        self.assertNotIn(app_find_published_1, output)
        self.assertNotIn(app_find_published_2, output)
        self.assertIn(app_find, output)
        self.assertIn("0.0.1", output)
        self.assertIn("0.0.2", output)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that creates global workflows')
    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_WORKFLOW_LIST_AVAILABLE_WORKFLOWS_GLOBALWF"])
    def test_dx_find_globalworkflows(self):
        test_applet_id = dxpy.api.applet_new({"name": "my_find_applet",
                                              "dxapi": "1.0.0",
                                              "project": self.project,
                                              "inputSpec": [],
                                              "outputSpec": [],
                                              "runSpec": {"interpreter": "bash",
                                                          "distribution": "Ubuntu",
                                                          "release": "14.04",
                                                          "code": "exit 0"}
                                              })['id']

        workflow_spec = {"stages": [{"id": "stage_0", "executable": test_applet_id}]}
        dxworkflow = dxpy.DXWorkflow()
        dxworkflow.new(**workflow_spec)
        dxworkflow._close(dxworkflow.get_id())
        dxglobalworkflow_spec = {
            "name": "gwf_find",
            "version": "0.0.1",
            "regionalOptions": {
                "aws:us-east-1": {
                    "workflow": dxworkflow.get_id()
                }
            }
        }

        # Create a few global workflows

        # 1. Unpublished workflow
        # version 0.0.1
        gwf_find = "gwf_find_unpublished"
        spec = dict(dxglobalworkflow_spec, name=gwf_find, version="0.0.1")
        dxgwf = dxpy.DXGlobalWorkflow()
        dxgwf.new(**spec)
        # version 0.0.2
        spec = dict(dxglobalworkflow_spec, name=gwf_find, version="0.0.2")
        dxgwf = dxpy.DXGlobalWorkflow()
        dxgwf.new(**spec)
        desc = dxgwf.describe()
        self.assertEqual(desc["version"], "0.0.2")

        # 2. Published workflow
        gwf_find_published_1 = "gwf_find_published_1"
        spec = dict(dxglobalworkflow_spec, name=gwf_find_published_1, version="0.0.3")
        dxgwf = dxpy.DXGlobalWorkflow()
        dxgwf.new(**spec)
        dxgwf.publish()
        desc = dxgwf.describe()
        self.assertTrue(desc["published"] > 0)

        # 3. Published workflow
        gwf_find_published_2 = "gwf_find_published_2"
        spec = dict(dxglobalworkflow_spec, name=gwf_find_published_2, version="0.0.4")
        dxgwf = dxpy.DXGlobalWorkflow()
        dxgwf.new(**spec)
        dxgwf.publish()
        desc = dxgwf.describe()
        self.assertTrue(desc["published"] > 0)

        # Tests

        # find only published
        output = run("dx find globalworkflows")
        self.assertIn(gwf_find_published_1, output)
        self.assertIn(gwf_find_published_2, output)
        self.assertNotIn(gwf_find, output)

        # find only unpublished
        output = run("dx find globalworkflows --unpublished")
        self.assertIn(gwf_find, output)
        self.assertNotIn(gwf_find_published_1, output)
        self.assertNotIn(gwf_find_published_2, output)

        # find by name
        output = run("dx find globalworkflows --name " + gwf_find_published_1)
        self.assertIn(gwf_find_published_1, output)
        self.assertNotIn(gwf_find_published_2, output)
        self.assertNotIn(gwf_find, output)

        # find all versions
        output = run("dx find globalworkflows --unpublished --all")
        self.assertNotIn(gwf_find_published_1, output)
        self.assertNotIn(gwf_find_published_2, output)
        self.assertIn(gwf_find, output)
        self.assertIn("0.0.1", output)
        self.assertIn("0.0.2", output)

    def test_dx_find_data_formatted(self):
        record_id = dxpy.new_dxrecord(project=self.project, name="find_data_formatting", close=True).get_id()
        self.assertRegex(
            run("dx find data --name " + "find_data_formatting").strip(),
            r"^closed\s+\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+/find_data_formatting \(" + record_id + "\)$"
        )

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_DATA_OBJ_LIST_DATOBJECTS"])
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

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_API_DATA_OBJ_SEARCH_ACROSS_PROJECTS"])
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

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_AUTH_CLEAR_ENVIRONMENT"])
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

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that can take a long time outside local environment')
    def test_dx_find_data_by_region(self):
        with temporary_project("p_azure", region="azure:westus") as p_azure:
            record_id = dxpy.new_dxrecord(project=p_azure.get_id(), close=True).get_id()
            self.assertIn(record_id,
                          run("dx find data --all-projects --brief --region azure:westus"))
            self.assertNotIn(record_id,
                             run("dx find data --all-projects --brief --region aws:us-east-1"))

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_PROJ_LIST_PROJECTS"])
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

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that depends on a public project only defined in the nucleus integration tests')
    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_API_PROJ_VIEW_PUBLIC_PROJECTS"])
    def test_dx_find_public_projects(self):
        unique_project_name = 'dx find public projects test ' + str(time.time())
        with temporary_project(unique_project_name) as unique_project:
            # Check that the temporary project doesn't appear in the list of public apps
            self.assertNotIn(run("dx find projects --public"),
                             unique_project.get_id() + ' : ' + unique_project_name + ' (ADMINISTER)\n')
            # Check that a known public app appears in the list of public apps
            self.assertIn("public-test-project (ADMINISTER)", run("dx find projects --public"))

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

    def test_dx_find_projects_by_region(self):
        awseast = "aws:us-east-1"
        azurewest = "azure:westus"
        created_project_name = 'dx find projects test ' + str(time.time())
        with temporary_project(created_project_name, region=awseast) as unique_project:
            self.assertEqual(run("dx find projects --region {} --brief --name {}".format(
                awseast, pipes.quote(created_project_name))),
                unique_project.get_id() + '\n')
            self.assertIn(unique_project.get_id(),
                          run("dx find projects --region {} --brief".format(awseast)))
            self.assertNotIn(unique_project.get_id(),
                             run("dx find projects --region {} --brief".format(azurewest)))

        with temporary_project(created_project_name, region=azurewest) as unique_project:
            self.assertIn(unique_project.get_id(),
                          run("dx find projects --region {} --brief".format(azurewest)))

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
    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_API_PROJ_VIEW_EXECUTIONS"])
    def test_dx_find_jobs_by_tags_and_properties(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "runSpec": {"interpreter": "bash",
                                                     "distribution": "Ubuntu",
                                                     "release": "14.04",
                                                     "code": "echo 'hello'"}
                                         })['id']
        property_names = ["$my.prop", "secoиdprop", "тhird prop"]
        property_values = ["$hello.world", "Σ2,n", "stuff"]
        the_tags = ["Σ1=n", "helloo0", "ωω"]
        job_id = run("dx run " + applet_id + ' -inumber=32 --brief -y ' +
                     " ".join(["--property '" + prop[0] + "'='" + prop[1] + "'" for prop in
                               zip(property_names, property_values)]) +
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

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_EXE_LIST_ANALYSES",
                                          "DNA_CLI_EXE_LIST_EXECUTIONS",
                                          "DNA_CLI_EXE_LIST_JOBS",
                                          "DNA_API_EXE_FIND"])
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
                              "distribution": "Ubuntu", "release": "14.04",
                              "execDepends": [{"name": "python-numpy"}]})
        dxrecord = dxpy.new_dxrecord()
        dxrecord.close()
        prog_input = {"chromosomes": {"$dnanexus_link": dxrecord.get_id()},
                      "rowFetchChunk": 100}
        dxworkflow = dxpy.new_dxworkflow(name='find_executions test workflow')
        stage = dxworkflow.add_stage(dxapplet, stage_input=prog_input)
        dxanalysis = dxworkflow.run({stage + ".rowFetchChunk": 200},
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
        self.assertEqual(len(run("dx find executions " + options).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs " + options).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses " + options).splitlines()), 2)
        options += " --project=" + dxapplet.get_proj_id()
        self.assertEqual(len(run("dx find executions " + options).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs " + options).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses " + options).splitlines()), 2)
        options += " --created-after=-150s --no-subjobs --applet=" + dxapplet.get_id()
        self.assertEqual(len(run("dx find executions " + options).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs " + options).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses " + options).splitlines()), 2)
        options2 = options + " --brief -n 9000"
        self.assertEqual(len(run("dx find executions " + options2).splitlines()), 4)
        self.assertEqual(len(run("dx find jobs " + options2).splitlines()), 3)
        self.assertEqual(len(run("dx find analyses " + options2).splitlines()), 1)
        options3 = options2 + " --origin=" + dxjob.get_id()
        self.assertEqual(len(run("dx find executions " + options3).splitlines()), 1)
        self.assertEqual(len(run("dx find jobs " + options3).splitlines()), 1)
        self.assertEqual(len(run("dx find analyses " + options3).splitlines()), 0)
        options3 = options2 + " --root=" + dxanalysis.get_id()
        self.assertEqual(len(run("dx find executions " + options3).splitlines()), 2)
        self.assertEqual(len(run("dx find jobs " + options3).splitlines()), 1)
        self.assertEqual(len(run("dx find analyses " + options3).splitlines()), 1)
        options2 = options + " --origin-jobs"
        self.assertEqual(len(run("dx find executions " + options2).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs " + options2).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses " + options2).splitlines()), 2)
        options2 = options + " --origin-jobs -n 9000"
        self.assertEqual(len(run("dx find executions " + options2).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs " + options2).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses " + options2).splitlines()), 2)
        options2 = options + " --all-jobs"
        self.assertEqual(len(run("dx find executions " + options2).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs " + options2).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses " + options2).splitlines()), 2)
        options2 = options + " --state=done"
        self.assertEqual(len(run("dx find executions " + options2).splitlines()), 0)
        self.assertEqual(len(run("dx find jobs " + options2).splitlines()), 0)
        self.assertEqual(len(run("dx find analyses " + options2).splitlines()), 0)

        # Search by tag
        options2 = options + " --all-jobs --brief"
        options3 = options2 + " --tag foo"
        analysis_id = dxanalysis.get_id()
        job_id = dxjob.get_id()
        self.assert_cmd_gives_ids("dx find executions " + options3, [analysis_id, job_id])
        self.assert_cmd_gives_ids("dx find jobs " + options3, [job_id])
        self.assert_cmd_gives_ids("dx find analyses " + options3, [analysis_id])
        options3 = options2 + " --tag foo --tag bar"
        self.assert_cmd_gives_ids("dx find executions " + options3, [job_id])
        self.assert_cmd_gives_ids("dx find jobs " + options3, [job_id])
        self.assert_cmd_gives_ids("dx find analyses " + options3, [])

        # Search by property (presence and by value)
        options3 = options2 + " --property foo"
        self.assert_cmd_gives_ids("dx find executions " + options3, [analysis_id, job_id])
        self.assert_cmd_gives_ids("dx find jobs " + options3, [job_id])
        self.assert_cmd_gives_ids("dx find analyses " + options3, [analysis_id])
        options3 = options2 + " --property foo=baz"
        self.assert_cmd_gives_ids("dx find executions " + options3, [job_id])
        self.assert_cmd_gives_ids("dx find jobs " + options3, [job_id])
        self.assert_cmd_gives_ids("dx find analyses " + options3, [])

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
                                     "distribution": "Ubuntu",
                                     "release": "14.04",
                                     "execDepends": [{"name": "dx-toolkit"}]},
                            project=temp_proj_id)

            dxworkflow = dxpy.new_dxworkflow(name='test_workflow', project=temp_proj_id)
            dxworkflow.add_stage(dxsubapplet, stage_input={})

            dxapplet.new(name="workflow_runner",
                         dxapi="1.0.0",
                         inputSpec=[],
                         outputSpec=[],
                         runSpec={
                             "code": "dx run " + dxworkflow.get_id() + " --priority high --project " + temp_proj_id,
                             "interpreter": "bash",
                             "distribution": "Ubuntu", "release": "14.04",
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

            options = "--brief --user=self --project=" + temp_proj_id
            self.assert_cmd_gives_ids("dx find executions " + options, [job_id, analysis_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs " + options, [job_id, subjob_id])
            self.assert_cmd_gives_ids("dx find analyses " + options, [analysis_id])
            options2 = options + " --applet=" + workflow_id
            self.assert_cmd_gives_ids("dx find executions " + options2, [job_id, analysis_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs " + options2, [])
            self.assert_cmd_gives_ids("dx find analyses " + options2, [analysis_id])
            options2 = options + " --applet=" + jobapplet_id
            self.assert_cmd_gives_ids("dx find executions " + options2, [job_id, analysis_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs " + options2, [job_id])
            self.assert_cmd_gives_ids("dx find analyses " + options2, [])
            options2 = options + " -n 9000"
            self.assert_cmd_gives_ids("dx find executions " + options2, [job_id, analysis_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs " + options2, [job_id, subjob_id])
            self.assert_cmd_gives_ids("dx find analyses " + options2, [analysis_id])
            options3 = options2 + " --origin=" + job_id
            self.assert_cmd_gives_ids("dx find executions " + options3, [job_id, analysis_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs " + options3, [job_id])
            self.assert_cmd_gives_ids("dx find analyses " + options3, [analysis_id])
            options2 = options + " --origin-jobs"
            self.assert_cmd_gives_ids("dx find executions " + options2, [job_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs " + options2, [job_id, subjob_id])
            self.assert_cmd_gives_ids("dx find analyses " + options2, [])
            options2 = options + " --origin-jobs -n 9000"
            self.assert_cmd_gives_ids("dx find executions " + options2, [job_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs " + options2, [job_id, subjob_id])
            self.assert_cmd_gives_ids("dx find analyses " + options2, [])
            options2 = options + " --all-jobs"
            self.assert_cmd_gives_ids("dx find executions " + options2, [job_id, analysis_id, subjob_id])
            self.assert_cmd_gives_ids("dx find jobs " + options2, [job_id, subjob_id])
            self.assert_cmd_gives_ids("dx find analyses " + options2, [analysis_id])
            options2 = options + " --state=done"
            self.assert_cmd_gives_ids("dx find executions " + options2, [])
            self.assert_cmd_gives_ids("dx find jobs " + options2, [])
            self.assert_cmd_gives_ids("dx find analyses " + options2, [])

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_ORG_LIST_ORGS"])
    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                         'skipping test that requires presence of test org')
    def test_find_orgs(self):
        org_with_billable_activities = "org-members_with_billing_rights"
        self.assertTrue(dxpy.api.org_describe(org_with_billable_activities)["allowBillableActivities"])
        org_without_billable_activities = "org-members_without_billing_rights"
        self.assertFalse(dxpy.api.org_describe(org_without_billable_activities)["allowBillableActivities"])
        orgs_with_admin = ["org-piratelabs", "org-auth_file_app_download"]
        for org_with_admin in orgs_with_admin:
            self.assertTrue(dxpy.api.org_describe(org_with_admin)["level"] == "ADMIN")

        cmd = "dx find orgs --level {l} {o} --json"

        results = json.loads(run(cmd.format(l="MEMBER", o="")).strip())
        self.assertItemsEqual([org_with_billable_activities,
                               org_without_billable_activities
                               ] + orgs_with_admin,
                              [result["id"] for result in results])

        results = json.loads(run(cmd.format(
            l="MEMBER", o="--with-billable-activities")).strip())
        self.assertItemsEqual([org_with_billable_activities] + orgs_with_admin,
                              [result["id"] for result in results])

        results = json.loads(run(cmd.format(
            l="MEMBER", o="--without-billable-activities")).strip())
        self.assertItemsEqual([org_without_billable_activities],
                              [result["id"] for result in results])

        results = json.loads(run(cmd.format(l="ADMIN", o="")).strip())
        self.assertItemsEqual(orgs_with_admin,
                              [result["id"] for result in results])

        results = json.loads(run(cmd.format(
            l="ADMIN", o="--with-billable-activities")).strip())
        self.assertItemsEqual(orgs_with_admin,
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


if __name__ == '__main__':
    if 'DXTEST_FULL' not in os.environ:
        sys.stderr.write(
            'WARNING: env var DXTEST_FULL is not set; tests that create apps or run jobs will not be run\n')
    unittest.main()
