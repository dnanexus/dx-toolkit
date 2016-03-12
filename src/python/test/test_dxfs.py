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

import os, unittest, tempfile, subprocess, shutil, time

import dxpy_testutil as testutil

import dxpy

@unittest.skipUnless(testutil.TEST_FUSE,
                     'skipping tests that would mount FUSE filesystems')
class TestDXFS(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if 'DXTEST_FUSE' not in os.environ:
            return
        proj_name = u"dxclient_test_pröject"
        cls.project_id = subprocess.check_output(u"dx new project '{p}' --brief".format(p=proj_name), shell=True).strip()
        dxpy.config["DX_PROJECT_CONTEXT_ID"] = cls.project_id
        dxpy.config["DX_CLI_WD"] = '/'
        cls.project = dxpy.DXProject(cls.project_id)
        dxpy.config.__init__(suppress_warning=True)

        subprocess.check_call(['dx', 'mkdir', 'foo'])
        subprocess.check_call(['dx', 'mkdir', 'bar'])
        dxpy.upload_local_file(__file__, wait_on_close=True)
        dxpy.new_dxrecord(name="A/B testing")

        cls.mountpoint = tempfile.mkdtemp()
        # TODO: redirect logs to someplace in case we need to debug
        # problems in these tests
        subprocess.check_call(['dx-mount', cls.mountpoint])

    @classmethod
    def tearDownClass(cls):
        try:
            subprocess.check_call(['fusermount', '-u', cls.mountpoint])
            subprocess.check_call(u"dx rmproject --yes {p}".format(p=cls.project_id), shell=True)
        except:
            pass

    def test_dxfs_operations(self):
        # FIXME: Make the mount live or add command to refresh it with remote changes
        #subprocess.check_call(['dx', 'mkdir', 'foo'])
        #subprocess.check_call(['dx', 'mkdir', 'bar'])
        #subprocess.check_call(['dx', 'mkdir', '-p', '/bar/baz'])

        self.assertEqual(set(os.listdir(self.mountpoint)), set(['foo', 'bar', 'AB testing', os.path.basename(__file__)]))

        # Reading
        self.assertEqual(open(__file__).read(), open(os.path.join(self.mountpoint, __file__)).read())

        # Moving
        #shutil.move(os.path.join(self.mountpoint, __file__), os.path.join(self.mountpoint, __file__+"2"))
        #self.assertEqual(set(os.listdir(self.mountpoint)), set(['foo', 'bar', os.path.basename(__file__+"2")]))
        #shutil.move(os.path.join(self.mountpoint, __file__+"2"), os.path.join(self.mountpoint, "foo"))
        #self.assertEqual(set(os.listdir(os.path.join(self.mountpoint, 'foo'))), set([os.path.basename(__file__+"2")]))
        #folder_listing = self.project.list_folder('/foo')
        #self.assertEqual(len(folder_listing['folders']), 0)
        #self.assertEqual(len(folder_listing['objects']), 1)
        #self.assertEqual(dxpy.get_handler(folder_listing['objects'][0]['id']).name, os.path.basename(__file__+"2"))
        #self.assertEqual(open(__file__).read(), open(os.path.join(self.mountpoint, 'foo', __file__+"2")).read())

    def test_dxfs_mkdir(self):
        os.mkdir(os.path.join(self.mountpoint, 'xyz'))
        self.assertIn('/xyz', self.project.list_folder('/')['folders'])
        os.mkdir(os.path.join(self.mountpoint, 'xyz', 'abc'))
        self.assertIn('/xyz/abc', self.project.list_folder('/xyz')['folders'])
        os.rmdir(os.path.join(self.mountpoint, 'xyz', 'abc'))
        self.assertNotIn('/xyz/abc', self.project.list_folder('/xyz')['folders'])
        os.rmdir(os.path.join(self.mountpoint, 'xyz'))
        self.assertNotIn('/xyz', self.project.list_folder('/')['folders'])

    #def test_dxfs_write(self):
    #    filename1 = os.path.join(self.mountpoint, 'foo', 'f1')
    #    with open(filename1, 'w') as fh:
    #        fh.write('0123456789ABCDEF'*256)
    #    subprocess.check_call(['dxfs', 'close', filename1, '--wait'])
    #    with open(filename1) as fh:
    #        d = fh.read()
    #        print len(d)
    #        self.assertEqual(d, '0123456789ABCDEF'*256, "File readback failed")

if __name__ == '__main__':
    unittest.main()
