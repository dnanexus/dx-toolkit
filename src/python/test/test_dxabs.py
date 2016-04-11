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
    thing.proj_id = dxpy.api.project_new({'name': 'azure-test-project', 'region': 'azure:westus'})['id']
    dxpy.set_workspace_id(thing.proj_id)

def tearDownTempProjects(thing):
    dxpy.api.project_destroy(thing.proj_id, {'terminateJobs': True})
    dxpy.set_workspace_id(thing.old_workspace_id)

class TestDXFile(unittest.TestCase):

    '''
    Creates a temporary file containing "foo\n" once for all tests.
    It should not be modified by any of the tests.

    For each test, both local and remote empty file handles are
    created and are destroyed after the test, no matter if it fails.
    '''

    foo_str = "foo upload file to azure\n"

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

    def tearDown(self):
        os.remove(self.new_file.name)
        tearDownTempProjects(self)

    def test_upload_download_files_dxfile(self):
        self.dxfile = dxpy.upload_local_file(self.foo_file.name, project=self.proj_id)
        self.assertTrue(self.dxfile)
        self.dxfile.wait_on_close()
        self.assertTrue(self.dxfile.closed())

        self.assertEqual(self.dxfile.describe()["name"],
                         os.path.basename(self.foo_file.name))

        dxpy.download_dxfile(self.dxfile.get_id(), self.new_file.name)
        self.assertTrue(filecmp.cmp(self.foo_file.name, self.new_file.name))

if __name__ == '__main__':
  if dxpy.AUTH_HELPER is None:
    sys.exit(1, 'Error: Need to be logged in to run these tests')
  if 'DXTEST_FULL' not in os.environ:
    if 'DXTEST_ISOLATED_ENV' not in os.environ:
      sys.stderr.write('WARNING: neither env var DXTEST_FULL nor DXTEST_ISOLATED_ENV are set; tests that create apps will not be run\n')
    if 'DXTEST_RUN_JOBS' not in os.environ:
      sys.stderr.write('WARNING: neither env var DXTEST_FULL nor DXTEST_RUN_JOBS are set; tests that run jobs will not be run\n')
  unittest.main()
||||||| merged common ancestors
=======
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
    thing.proj_id = dxpy.api.project_new({'name': 'azure-test-project', 'region': 'azure:eastus'})['id']
    dxpy.set_workspace_id(thing.proj_id)

def tearDownTempProjects(thing):
    dxpy.api.project_destroy(thing.proj_id, {'terminateJobs': True})
    dxpy.set_workspace_id(thing.old_workspace_id)

class TestDXFile(unittest.TestCase):

    '''
    Creates a temporary file containing "foo\n" once for all tests.
    It should not be modified by any of the tests.

    For each test, both local and remote empty file handles are
    created and are destroyed after the test, no matter if it fails.
    '''

    foo_str = "foo upload file to azure\n"

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

    def tearDown(self):
        os.remove(self.new_file.name)
        tearDownTempProjects(self)

    def test_upload_download_files_dxfile(self):
        self.dxfile = dxpy.upload_local_file(self.foo_file.name, project=self.proj_id)
        self.assertTrue(self.dxfile)
        self.dxfile.wait_on_close()
        self.assertTrue(self.dxfile.closed())

        self.assertEqual(self.dxfile.describe()["name"],
                         os.path.basename(self.foo_file.name))

        dxpy.download_dxfile(self.dxfile.get_id(), self.new_file.name)
        self.assertTrue(filecmp.cmp(self.foo_file.name, self.new_file.name))

if __name__ == '__main__':
  if dxpy.AUTH_HELPER is None:
    sys.exit(1, 'Error: Need to be logged in to run these tests')
  if 'DXTEST_FULL' not in os.environ:
    if 'DXTEST_ISOLATED_ENV' not in os.environ:
      sys.stderr.write('WARNING: neither env var DXTEST_FULL nor DXTEST_ISOLATED_ENV are set; tests that create apps will not be run\n')
    if 'DXTEST_RUN_JOBS' not in os.environ:
      sys.stderr.write('WARNING: neither env var DXTEST_FULL nor DXTEST_RUN_JOBS are set; tests that run jobs will not be run\n')
  unittest.main()
