#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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

from __future__ import print_function, unicode_literals, division, absolute_import

import os, unittest, tempfile, filecmp, time, json, sys, shutil
import string
import subprocess

import requests
from requests.packages.urllib3.exceptions import SSLError

import dxpy
import dxpy_testutil as testutil
from dxpy.exceptions import (DXAPIError, DXFileError, DXError, DXJobFailureError, ResourceNotFound)
from dxpy.utils import pretty_print, warn
from dxpy.utils.resolver import resolve_path, resolve_existing_path, ResolutionError, is_project_explicit

def remove_all(proj_id, folder="/"):
    dxproject = dxpy.DXProject(proj_id)
    dxproject.remove_folder(folder, recurse=True)

def setUpTempProjects(thing):
    thing.old_workspace_id = dxpy.WORKSPACE_ID
    thing.proj_id = dxpy.api.project_new({'name': 'azure-test-project', 'region' : 'azure:westus'})['id']
    dxpy.set_workspace_id(thing.proj_id)

def tearDownTempProjects(thing):
    dxpy.api.project_destroy(thing.proj_id, {'terminateJobs': True})
    dxpy.set_workspace_id(thing.old_workspace_id)

class TestDXProject(unittest.TestCase):
    # Also test DXContainer here
    def setUp(self):
        setUpTempProjects(self)

    def tearDown(self):
        tearDownTempProjects(self)

    def test_init_and_set_id(self):
        for good_value in ["project-aB3456789012345678901234", None]:
            dxproject = dxpy.DXProject(good_value)
            dxproject.set_id(good_value)
        for bad_value in ["foo",
                          "container-123456789012345678901234",
                          3,
                          {},
                          "project-aB34567890123456789012345",
                          "project-aB345678901234567890123"]:
            with self.assertRaises(DXError):
                dxpy.DXProject(bad_value)
            with self.assertRaises(DXError):
                dxproject = dxpy.DXProject()
                dxproject.set_id(bad_value)

class TestDXFile(unittest.TestCase):

    '''
    Creates a temporary file containing "foo\n" once for all tests.
    It should not be modified by any of the tests.

    For each test, both local and remote empty file handles are
    created and are destroyed after the test, no matter if it fails.
    '''

    foo_str = "foo\n"

    @classmethod
    def setUpClass(cls):
        cls.foo_file = tempfile.NamedTemporaryFile(delete=False)
        cls.foo_file.write(cls.foo_str)
        cls.foo_file.close()

    @classmethod
    def tearDownClass(cls):
        os.remove(cls.foo_file.name)

    def setUp(self):
        setUpTempProjects(self)

        self.new_file = tempfile.NamedTemporaryFile(delete=False)
        self.new_file.close()

        self.dxfile = dxpy.DXFile()

    def tearDown(self):
        os.remove(self.new_file.name)
        tearDownTempProjects(self)

    def test_dx_upload_with_upload_perm(self):
        temp_project = dxpy.api.project_new({'name': 'azure-test-project', 'region' : 'azure:westus'})
        data = {"scope": {"projects": {"*": "UPLOAD"}}}
        upload_only_auth_token = dxpy.DXHTTPRequest(dxpy.get_auth_server_name() + '/system/newAuthToken', data,
            prepend_srv=False, always_retry=True)
        token_callable = dxpy.DXHTTPOAuth2({"auth_token": upload_only_auth_token["access_token"],
            "auth_token_type": upload_only_auth_token["token_type"],
            "auth_token_signature": upload_only_auth_token["token_signature"]})
        testdir = tempfile.mkdtemp();
        try:
            # Filename provided with path
            with open(os.path.join(testdir, 'myfilename'), 'w') as f:
                f.write('foo')
            remote_file = dxpy.upload_local_file(filename=os.path.join(testdir, 'myfilename'),
                   project=temp_project['id'], folder='/', auth=token_callable)
            self.assertEqual(remote_file.name, 'myfilename')
            # Filename provided with file handle
            remote_file2 = dxpy.upload_local_file(file=open(os.path.join(testdir, 'myfilename')),
                    project=temp_project['id'], folder='/', auth=token_callable)
            self.assertEqual(remote_file2.name, 'myfilename')
        finally:
            shutil.rmtree(testdir)

if __name__ == '__main__':
  if dxpy.AUTH_HELPER is None:
    sys.exit(1, 'Error: Need to be logged in to run these tests')
  if 'DXTEST_FULL' not in os.environ:
    if 'DXTEST_ISOLATED_ENV' not in os.environ:
      sys.stderr.write('WARNING: neither env var DXTEST_FULL nor DXTEST_ISOLATED_ENV are set; tests that create apps will not be run\n')
    if 'DXTEST_RUN_JOBS' not in os.environ:
      sys.stderr.write('WARNING: neither env var DXTEST_FULL nor DXTEST_RUN_JOBS are set; tests that run jobs will not be run\n')
  unittest.main()
