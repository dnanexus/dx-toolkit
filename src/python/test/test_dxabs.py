#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2016 DNAnexus, Inc.
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

import filecmp
import os
import shutil
import sys
import tempfile
import unittest

import dxpy
import dxpy_testutil as testutil


def setUpTempProjects(thing):
    thing.old_workspace_id = dxpy.WORKSPACE_ID
    thing.proj_id = dxpy.api.project_new({'name': 'azure-test-project', 'region': testutil.TEST_AZURE})['id']
    dxpy.set_workspace_id(thing.proj_id)


def tearDownTempProjects(thing):
    dxpy.api.project_destroy(thing.proj_id, {'terminateJobs': True})
    dxpy.set_workspace_id(thing.old_workspace_id)


@unittest.skipUnless(testutil.TEST_AZURE, 'skipping tests for Azure regions')
class TestDXFile(unittest.TestCase):
    '''
    Creates a temporary file and stores a handle to it as
    cls.sample_file. It should not be modified by any of the tests.
    '''

    foo_str = "foo upload file to azure\n"

    @classmethod
    def setUpClass(cls):
        cls.sample_file = tempfile.NamedTemporaryFile(delete=False)
        cls.sample_file.write(cls.foo_str)
        cls.sample_file.close()

    @classmethod
    def tearDownClass(cls):
        os.remove(cls.sample_file.name)

    def setUp(self):
        setUpTempProjects(self)
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempdir)
        tearDownTempProjects(self)

    def test_upload_download_files_dxfile(self):
        myfile = dxpy.upload_local_file(self.sample_file.name, project=self.proj_id, wait_on_close=True)
        self.assertTrue(myfile.closed())

        self.assertEqual(myfile.describe()["name"],
                         os.path.basename(self.sample_file.name))

        dxpy.download_dxfile(myfile, os.path.join(self.tempdir, 'downloaded'))
        self.assertTrue(filecmp.cmp(self.sample_file.name, os.path.join(self.tempdir, 'downloaded')))

    def test_upload_download_large_file_size_dxfile(self):
        test_file_name = os.path.join(self.tempdir, 'large_file')
        hundredMB = 1024*1024*100
        with open(test_file_name, 'w') as test_file:
            with open("/dev/urandom", 'r') as random_input:
                test_file.write(random_input.read(hundredMB + 4002080))

        myfile = dxpy.upload_local_file(test_file_name, project=self.proj_id,
                                        write_buffer_size=hundredMB, wait_on_close=True)
        self.assertTrue(myfile.closed())

        # Check file was split up into parts appropriately
        # 104857600 (or 100 MB) is the maximum size for a single part
        parts = myfile.describe(fields={"parts": True})['parts']
        self.assertEquals(parts['1']['size'], hundredMB)
        self.assertEquals(parts['2']['size'], 4002080)

        self.assertEqual(myfile.describe()["name"], 'large_file')
        downloaded_again = os.path.join(self.tempdir, 'large_file_2')
        dxpy.download_dxfile(myfile, downloaded_again)
        self.assertTrue(filecmp.cmp(test_file_name, downloaded_again))

    def test_upload_download_large_file_small_bufsize_dxfile(self):
        num_parts = 50000

        common_args = dict(mode='w', project=self.proj_id)

        with dxpy.new_dxfile(write_buffer_size=280000, expected_file_size=300000*num_parts, **common_args) as myfile:
            myfile.write("0" * 700000)
        myfile.close(block=True)
        parts = myfile.describe(fields={"parts": True})['parts']
        self.assertEquals(parts['1']['size'], 300000)

        with dxpy.new_dxfile(write_buffer_size=320000, expected_file_size=300000*num_parts, **common_args) as myfile:
            myfile.write("0" * 700000)
        myfile.close(block=True)
        parts = myfile.describe(fields={"parts": True})['parts']
        self.assertEquals(parts['1']['size'], 320000)


if __name__ == '__main__':
    if dxpy.AUTH_HELPER is None:
        sys.exit(1, 'Error: Need to be logged in to run these tests')
    unittest.main()
