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

import os, unittest, tempfile, filecmp, time, json, sys
import shutil
import string
import subprocess
import platform
import re

import requests
from requests.packages.urllib3.exceptions import SSLError
import OpenSSL

import dxpy
import dxpy_testutil as testutil
from dxpy.exceptions import (DXAPIError, DXFileError, DXError, DXJobFailureError, ResourceNotFound)
from dxpy.utils import pretty_print, warn, Nonce
from dxpy.utils.resolver import resolve_path, resolve_existing_path, ResolutionError, is_project_explicit
import dxpy.app_builder as app_builder

def get_objects_from_listf(listf):
    objects = []
    for result in listf["objects"]:
        objects.append(result["id"])
    return objects

def remove_all(proj_id, folder="/"):
    dxproject = dxpy.DXProject(proj_id)
    dxproject.remove_folder(folder, recurse=True)

def setUpTempProjects(thing):
    thing.old_workspace_id = dxpy.WORKSPACE_ID
    thing.proj_id = dxpy.api.project_new({'name': 'test project 1'})['id']
    thing.second_proj_id = dxpy.api.project_new({'name': 'test project 2'})['id']
    dxpy.set_workspace_id(thing.proj_id)

def tearDownTempProjects(thing):
    dxpy.api.project_destroy(thing.proj_id, {'terminateJobs': True})
    dxpy.api.project_destroy(thing.second_proj_id, {'terminateJobs': True})
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

    def test_dxcontainer_init_and_set_id(self):
        for good_value in ["container-aB3456789012345678901234"]:
            # Note: None is actually not a valid value if the current
            # project context is a project
            dxcontainer = dxpy.DXContainer(good_value)
            dxcontainer.set_id(good_value)
        for bad_value in ["foo",
                          "project-123456789012345678901234",
                          3,
                          {},
                          "container-aB34567890123456789012345",
                          "container-aB345678901234567890123"]:
            with self.assertRaises(DXError):
                dxpy.DXContainer(bad_value)
            with self.assertRaises(DXError):
                dxcontainer = dxpy.DXContainer()
                dxcontainer.set_id(bad_value)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that requires presence of test user')
    def test_invite_without_email(self):
        user_id = 'user-bob'
        dxproject = dxpy.DXProject(self.proj_id)

        # Check that user is not already invited to project
        project_members = dxpy.api.project_describe(dxproject.get_id(),
                                                    {'fields': {'permissions': True}})['permissions']
        self.assertNotIn(user_id, project_members.keys())

        dxproject.invite(user_id, 'VIEW', send_email=False)
        res = dxpy.api.project_describe(dxproject.get_id(), {'fields': {'permissions': True}})['permissions']
        self.assertEquals(res[user_id], 'VIEW')

    def test_new(self):
        dxproject = dxpy.DXProject()
        with self.assertRaises(TypeError):
            # Must be initialized with a name
            dxproject.new()
        default_proj_id = dxproject.new(name="newprojname")
        try:
            desc = dxproject.describe()
            # New ID must be different then the automatically assigned workspace id
            self.assertNotEqual(desc["id"], self.proj_id)
            self.assertEqual(desc["id"], default_proj_id)
            self.assertEqual(desc["name"], "newprojname")
            self.assertEqual(desc["summary"], "")
            self.assertEqual(desc["description"], "")
            self.assertEqual(desc["protected"], False)
            self.assertEqual(desc["restricted"], False)
            self.assertEqual(desc["containsPHI"], False)
            self.assertEqual(desc["tags"], [])
            prop = dxpy.api.project_describe(dxproject.get_id(),
                                             {'fields': {'properties': True}})
            self.assertEqual(prop['properties'], {})
            modified_proj_id = dxproject.new(name="newprojname2",
                                             protected=True,
                                             restricted=True,
                                             description="new description",
                                             properties={"prop1": "val1"},
                                             tags=["tag1", "tag2", "tag3"])
            try:
                desc2 = dxproject.describe()
                self.assertNotEqual(desc2["id"], self.proj_id)
                self.assertNotEqual(desc2["id"], desc["id"])
                self.assertEqual(desc2["id"], modified_proj_id)
                self.assertEqual(desc2["name"], "newprojname2")
                self.assertEqual(desc2["restricted"], True)
                self.assertEqual(desc2["protected"], True)
                self.assertEqual(desc2["description"], "new description")
                self.assertEqual(desc2["tags"], ["tag1", "tag2", "tag3"])
                prop2 = dxpy.api.project_describe(dxproject.get_id(),
                                                  {'fields': {'properties': True}})
                self.assertEqual(prop2['properties'], {"prop1": "val1"})
            finally:
                dxpy.api.project_destroy(modified_proj_id)

            dxpy.WORKSPACE_ID = None
            dxproject = dxpy.DXProject()
            self.assertIsNone(dxproject._dxid)
            with self.assertRaises(ResourceNotFound):
                # Cannot describe() because _dxid does not exist
                dxproject.describe()

            proj_id_3 = dxproject.new(name="newprojname3")
            try:
                self.assertNotEqual(dxproject._dxid, None)
                # Now we can describe because _dxid is generated by project/new
                valid_desc = dxproject.describe()
                self.assertEqual(valid_desc, dxproject._desc)
            finally:
                dxpy.api.project_destroy(proj_id_3)
        finally:
            dxpy.api.project_destroy(default_proj_id)

    def test_update_describe(self):
        dxproject = dxpy.DXProject()
        dxproject.update(name="newprojname", protected=True, restricted=True, description="new description")
        desc = dxproject.describe()
        self.assertEqual(desc["id"], self.proj_id)
        self.assertEqual(desc["class"], "project")
        self.assertEqual(desc["name"], "newprojname")
        self.assertEqual(desc["protected"], True)
        self.assertEqual(desc["restricted"], True)
        self.assertEqual(desc["description"], "new description")
        self.assertTrue("created" in desc)
        dxproject.update(restricted=False)
        desc = dxproject.describe()
        self.assertEqual(desc["restricted"], False)

    def test_new_list_remove_folders(self):
        dxproject = dxpy.DXProject()
        listf = dxproject.list_folder()
        self.assertEqual(listf["folders"], [])
        self.assertEqual(listf["objects"], [])

        dxrecord = dxpy.new_dxrecord()
        dxproject.new_folder("/a/b/c/d", parents=True)
        listf = dxproject.list_folder()
        self.assertEqual(listf["folders"], ["/a"])
        self.assertEqual(listf["objects"], [{"id": dxrecord.get_id()}])
        listf = dxproject.list_folder("/a")
        self.assertEqual(listf["folders"], ["/a/b"])
        self.assertEqual(listf["objects"], [])
        listf = dxproject.list_folder("/a/b")
        self.assertEqual(listf["folders"], ["/a/b/c"])
        listf = dxproject.list_folder("/a/b/c")
        self.assertEqual(listf["folders"], ["/a/b/c/d"])
        listf = dxproject.list_folder("/a/b/c/d")
        self.assertEqual(listf["folders"], [])

        with self.assertRaises(DXAPIError):
            dxproject.remove_folder("/a")
        dxproject.remove_folder("/a/b/c/d")
        dxproject.remove_folder("/a//b////c/")
        dxproject.remove_folder("/a/b")
        dxproject.remove_folder("/a")
        dxrecord.remove()
        listf = dxproject.list_folder()
        self.assertEqual(listf["objects"], [])

    def test_move(self):
        dxproject = dxpy.DXProject()
        dxproject.new_folder("/a/b/c/d", parents=True)
        dxrecords = []
        for i in range(4):
            dxrecords.append(dxpy.new_dxrecord(name=("record-%d" % i)))
        dxproject.move(destination="/a",
                       objects=[dxrecords[0].get_id(), dxrecords[1].get_id()],
                       folders=["/a/b/c/d"])
        listf = dxproject.list_folder()
        self.assertEqual(get_objects_from_listf(listf).sort(),
                         [dxrecords[2].get_id(), dxrecords[3].get_id()].sort())
        self.assertEqual(listf["folders"], ["/a"])

        listf = dxproject.list_folder("/a")
        self.assertEqual(get_objects_from_listf(listf).sort(),
                         [dxrecords[0].get_id(), dxrecords[1].get_id()].sort())
        self.assertEqual(listf["folders"], ["/a/b", "/a/d"])

        desc = dxrecords[0].describe()
        self.assertEqual(desc["folder"], "/a")

    def test_clone(self):
        dxproject = dxpy.DXProject()
        dxproject.new_folder("/a/b/c/d", parents=True)
        dxrecords = []
        for i in range(4):
            dxrecords.append(dxpy.new_dxrecord(name=("record-%d" % i)))

        with self.assertRaises(DXAPIError):
            dxproject.clone(self.second_proj_id,
                            destination="/",
                            objects=[dxrecords[0].get_id(), dxrecords[1].get_id()],
                            folders=["/a/b/c/d"])

        dxrecords[0].close()
        dxrecords[1].close()
        dxproject.clone(self.second_proj_id,
                        destination="/",
                        objects=[dxrecords[0].get_id(), dxrecords[1].get_id()],
                        folders=["/a/b/c/d"])

        second_proj = dxpy.DXProject(self.second_proj_id)
        listf = second_proj.list_folder()
        self.assertEqual(get_objects_from_listf(listf).sort(),
                         [dxrecords[0].get_id(), dxrecords[1].get_id()].sort())
        self.assertEqual(listf["folders"], ["/d"])

    def test_remove_objects(self):
        dxproject = dxpy.DXProject()
        dxrecord = dxpy.new_dxrecord()
        listf = dxproject.list_folder()
        self.assertEqual(get_objects_from_listf(listf), [dxrecord.get_id()])
        dxproject.remove_objects([dxrecord.get_id()])
        listf = dxproject.list_folder()
        self.assertEqual(listf["objects"], [])
        with self.assertRaises(DXAPIError):
            dxrecord.describe()

class TestDXFileFunctions(unittest.TestCase):
    def test_readable_part_size(self):
        self.assertEqual(dxpy.dxfile._readable_part_size(0), "0 bytes")
        self.assertEqual(dxpy.dxfile._readable_part_size(1), "1 byte")
        self.assertEqual(dxpy.dxfile._readable_part_size(2), "2 bytes")
        self.assertEqual(dxpy.dxfile._readable_part_size(2.5 * 1024), "2.50 KiB")
        self.assertEqual(dxpy.dxfile._readable_part_size(1024 * 1024), "1.00 MiB")
        self.assertEqual(dxpy.dxfile._readable_part_size(31415926535), "29.26 GiB")

    def test_get_buffer_size(self):
        amazon = {
            "maximumPartSize": 5368709120,
            "minimumPartSize": 5242880,
            "maximumFileSize": 5497558138880,
            "maximumNumParts": 10000,
            "emptyLastPartAllowed": True
        }

        azure = {
            "maximumPartSize": 4194304,
            "minimumPartSize": 1,
            "maximumFileSize": 209715200000,
            "maximumNumParts": 50000,
            "emptyLastPartAllowed": False
        }

        MB = 1024 * 1024
        GB = 1024 * 1024 * 1024

        self.assertEqual(dxpy.dxfile._get_write_buf_size(16 * MB, amazon, 1024 * MB), 16 * MB)
        self.assertEqual(dxpy.dxfile._get_write_buf_size(1 * MB, amazon, 500 * MB), 5 * MB)
        self.assertEqual(dxpy.dxfile._get_write_buf_size(16 * MB, amazon, 200000 * MB), 20 * MB)
        self.assertEqual(dxpy.dxfile._get_write_buf_size(6 * GB, amazon, 200000 * MB), 5 * GB)

        with self.assertRaises(DXFileError):
            dxpy.dxfile._get_write_buf_size(16 * MB, amazon, 5121 * GB)

        self.assertEqual(dxpy.dxfile._get_write_buf_size(5 * MB, azure, 35000 * MB), 4 * MB)
        self.assertEqual(dxpy.dxfile._get_write_buf_size(1 * MB, azure, 500 * MB), 1 * MB)
        self.assertEqual(dxpy.dxfile._get_write_buf_size(3 * MB, azure, 30000 * MB), 3 * MB)

        with self.assertRaises(DXFileError):
            dxpy.dxfile._get_write_buf_size(16 * MB, azure, 200001 * MB)

    def test_job_detection(self):
        if platform.system() == 'Windows':
            import nt
            env = dict(nt.environ, DX_JOB_ID=b'job-00000000000000000000')
        else:
            env = dict(os.environ, DX_JOB_ID=b'job-00000000000000000000')
        buffer_size = subprocess.check_output(
            'python -c "import dxpy; print dxpy.bindings.dxfile.DEFAULT_BUFFER_SIZE"', shell=True, env=env)
        self.assertEqual(int(buffer_size), 96 * 1024 * 1024)
        del env['DX_JOB_ID']
        buffer_size = subprocess.check_output(
            'python -c "import dxpy; print dxpy.bindings.dxfile.DEFAULT_BUFFER_SIZE"', shell=True, env=env)
        self.assertEqual(int(buffer_size), 16 * 1024 * 1024)

    def test_generate_read_requests(self):
        with testutil.temporary_project() as host:
            dxfile = dxpy.upload_string("foo", project=host.get_id(), wait_on_close=True)
            with testutil.temporary_project() as p, self.assertRaises(ResourceNotFound):
                # The file doesn't exist in this project
                list(dxfile._generate_read_requests(project=p.get_id()))
            with self.assertRaises(ResourceNotFound):
                # This project doesn't even exist
                list(dxfile._generate_read_requests(project="project-012301230123012301230123"))

            # Without a project argument, the function call should succeed
            l = list(dxfile._generate_read_requests())
            self.assertTrue(type(l) == list and len(l) > 0)


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

    def test_init_and_set_ids(self):
        for good_dxid, good_project in [
                ("file-aB3456789012345678901234", None),
                (None, "project-aB3456789012345678901234"),
                (None, "container-aB3456789012345678901234"),
                (None, None),
                ({"$dnanexus_link": {"id" : "file-aB3456789012345678901234"}}, None),
                ({"$dnanexus_link": {"id" : "file-aB3456789012345678901234",
                                     "project": "project-aB3456789012345678901234"}}, None),
                ({"$dnanexus_link": {"id" : "file-aB3456789012345678901234",
                                     "project": "container-aB3456789012345678901234"}}, None)
        ]:
            dxfile = dxpy.DXFile(good_dxid, project=good_project)
            dxfile.set_ids(good_dxid, project=good_project)
        for bad_dxid, bad_project in [
                ("foo", None),
                ("record-123456789012345678901234", None),
                (3, None),
                ({}, None),
                ({"$dnanexus_link": {"id": "foo"}}, None),
                ({"$dnanexus_link": {"id": "file-aB3456789012345678901234",
                                     "project": "foo"}}, None)
        ]:
            with self.assertRaises(DXError):
                dxpy.DXFile(bad_dxid, project=bad_project)
            with self.assertRaises(DXError):
                dxfile = dxpy.DXFile()
                dxfile.set_ids(bad_dxid, project=bad_project)

        # test logic
        dxfile = dxpy.DXFile({"$dnanexus_link": {"id" : "file-aB3456789012345678901234",
                                                 "project": "container-aB3456789012345678901234"}},
                             project="project-aB3456789012345678901234")
        self.assertEqual(dxfile.get_proj_id(), "project-aB3456789012345678901234")

    def test_upload_download_files_dxfile(self):
        self.dxfile = dxpy.upload_local_file(self.foo_file.name)

        self.dxfile.wait_on_close()
        self.assertTrue(self.dxfile.closed())

        self.assertEqual(self.dxfile.describe()["name"],
                         os.path.basename(self.foo_file.name))

        dxpy.download_dxfile(self.dxfile.get_id(), self.new_file.name)
        self.assertTrue(filecmp.cmp(self.foo_file.name, self.new_file.name))

        dxpy.download_dxfile(filename=self.new_file.name, dxid=self.dxfile.get_id())
        self.assertTrue(filecmp.cmp(self.foo_file.name, self.new_file.name))

        dxpy.download_dxfile(dxid=self.dxfile, filename=self.new_file.name)
        self.assertTrue(filecmp.cmp(self.foo_file.name, self.new_file.name))

        dxpy.download_dxfile(self.dxfile, filename=self.new_file.name)
        self.assertTrue(filecmp.cmp(self.foo_file.name, self.new_file.name))

    @unittest.skipUnless(testutil.TEST_MULTIPLE_USERS, 'skipping test that would require multiple users')
    def test_upload_file_with_custom_auth(self):
        tempdir = tempfile.mkdtemp()
        try:
            second_user_auth = dxpy.DXHTTPOAuth2(json.loads(testutil.as_second_user()['DX_SECURITY_CONTEXT']))
            templocalfile = os.path.join(tempdir, "foo.txt")
            with open(templocalfile, "w") as f:
                f.write("mydata")
            with testutil.temporary_project(auth=second_user_auth) as p1:
                fh = dxpy.upload_local_file(filename=templocalfile, project=p1.get_id(), wait_on_close=True,
                                            auth=second_user_auth)
                self.assertEqual(fh.describe(auth=second_user_auth)['project'], p1.get_id())
            with self.assertRaises(ResourceNotFound):
                fh.describe()
        finally:
            shutil.rmtree(tempdir)

    def test_upload_string_dxfile(self):
        self.dxfile = dxpy.upload_string(self.foo_str)

        self.dxfile.wait_on_close()
        self.assertTrue(self.dxfile.closed())

        dxpy.download_dxfile(self.dxfile.get_id(), self.new_file.name)

        self.assertTrue(filecmp.cmp(self.foo_file.name, self.new_file.name))

    def test_upload_empty_dxfile(self):
        self.assertEqual(0, os.path.getsize(self.new_file.name))
        # Checking default backend
        with dxpy.new_dxfile() as f:
            f.close(block=True)
            self.assertEqual(0, f.describe()["size"])
        with dxpy.upload_local_file(filename=self.new_file.name, project=self.proj_id, wait_on_close=True) as f:
            self.assertEqual(0, f.describe()["size"])
        with dxpy.upload_string('', wait_on_close=True) as f:
            self.assertEqual(0, f.describe()["size"])

    @unittest.skipUnless(testutil.TEST_AZURE, "Skipping test that would upload file to Azure")
    def test_upload_empty_dxfile_azure(self):
        self.assertEqual(0, os.path.getsize(self.new_file.name))
        # Checking Azure backend
        with testutil.temporary_project("TestDXFile.test_upload_empty_dxfile azure", select=True, region="azure:westus") as ap:
            with dxpy.new_dxfile(project=ap.get_id()) as f:
                f.close(block=True)
                self.assertEqual(0, f.describe()["size"])
            with dxpy.upload_local_file(filename=self.new_file.name, project=ap.get_id(), wait_on_close=True) as f:
                self.assertEqual(0, f.describe()["size"])
            with dxpy.upload_string('', wait_on_close=True, project=ap.get_id()) as f:
                self.assertEqual(0, f.describe()["size"])

    def test_write_read_dxfile(self):
        dxid = ""
        with dxpy.new_dxfile() as self.dxfile:
            dxid = self.dxfile.get_id()
            self.dxfile.write(self.foo_str)

        with dxpy.open_dxfile(dxid) as same_dxfile:
            same_dxfile.wait_on_close()
            self.assertTrue(same_dxfile.closed())

            buf = same_dxfile.read(len(self.foo_str))
            self.assertEqual(self.foo_str, buf)

            buf = same_dxfile.read()
            self.assertEqual(len(buf), 0)

            same_dxfile.seek(1)
            buf = same_dxfile.read()
            self.assertEqual(self.foo_str[1:], buf)

            same_dxfile.seek(1, 0)
            buf = same_dxfile.read()
            self.assertEqual(self.foo_str[1:], buf)

            same_dxfile.seek(2)
            same_dxfile.seek(-1, 1)
            buf = same_dxfile.read()
            self.assertEqual(self.foo_str[1:], buf)

            same_dxfile.seek(0, 2)
            buf = same_dxfile.read()
            self.assertEqual(b"", buf)

            same_dxfile.seek(-1, 2)
            buf = same_dxfile.read()
            self.assertEqual(self.foo_str[-1:], buf)

    def test_download_project_selection(self):
        with testutil.temporary_project() as p, testutil.temporary_project() as p2:
            # Same file is available in both projects
            f = dxpy.upload_string(self.foo_str, project=p.get_id(), wait_on_close=True)
            dxpy.api.project_clone(p.get_id(), {"objects": [f.get_id()], "project": p2.get_id()})

            # Project specified in handler: bill that project for download
            with testutil.TemporaryFile(close=True) as tmp:
                os.environ['_DX_DUMP_BILLED_PROJECT'] = tmp.name
                f1 = dxpy.DXFile(dxid=f.get_id(), project=p.get_id())
                f1.read(4)
                with open(tmp.name, "r") as fd:
                    self.assertEqual(fd.read(), p.get_id())

            # Project specified in read() call: overrides project specified in
            # handler
            with testutil.TemporaryFile(close=True) as tmp:
                os.environ['_DX_DUMP_BILLED_PROJECT'] = tmp.name
                f2 = dxpy.DXFile(dxid=f.get_id(), project=p.get_id())
                f2.read(4, project=p2.get_id())
                with open(tmp.name, "r") as fd:
                    self.assertEqual(fd.read(), p2.get_id())

            # Project specified in neither handler nor read() call: set no hint
            # when making API call
            with testutil.TemporaryFile(close=True) as tmp:
                os.environ['_DX_DUMP_BILLED_PROJECT'] = tmp.name
                f3 = dxpy.DXFile(dxid=f.get_id())  # project defaults to project context
                f3.read(4)
                with open(tmp.name, "r") as fd:
                    self.assertEqual(fd.read(), "")

            # Project specified in read() that doesn't contain the file.
            # The call should fail.
            del os.environ['_DX_DUMP_BILLED_PROJECT']
            dxpy.api.project_remove_objects(p2.get_id(), {"objects": [f.get_id()]})
            f4 = dxpy.DXFile(dxid=f.get_id())
            with self.assertRaises(ResourceNotFound):
                f4.read(4, project=p2.get_id())

            # Project specified in handler that doesn't contain the file. The
            # call must succeed for backward compatibility (and bill no project
            # in particular).
            with testutil.TemporaryFile(close=True) as tmp:
                os.environ['_DX_DUMP_BILLED_PROJECT'] = tmp.name
                f5 = dxpy.DXFile(dxid=f.get_id(), project=p2.get_id())
                f5.read(4)
                with open(tmp.name, "r") as fd:
                    self.assertEqual(fd.read(), "")

            del os.environ['_DX_DUMP_BILLED_PROJECT']

    def test_read_with_invalid_project(self):
        dxfile = dxpy.upload_string(self.foo_str, wait_on_close=True)
        with testutil.temporary_project() as p, self.assertRaises(ResourceNotFound):
            # The file doesn't exist in this project
            dxfile.read(project=p.get_id())
        # Try the same thing again, just to make sure read() doesn't have the
        # side effect of wedging the DXFile when it fails
        with testutil.temporary_project() as p, self.assertRaises(ResourceNotFound):
            dxfile.read(project=p.get_id())
        # Try the same thing again, now we should be able to succeed
        self.assertEqual(dxfile.read(), self.foo_str)

        dxfile = dxpy.upload_string(self.foo_str, wait_on_close=True)
        with self.assertRaises(ResourceNotFound):
            # This project doesn't even exist
            dxfile.read(project="project-012301230123012301230123")
        # Try the same thing again, now we should be able to succeed
        self.assertEqual(dxfile.read(), self.foo_str)

    def test_dxfile_sequential_optimization(self):
        # Make data longer than 128k to trigger the
        # first-sequential-read optimization
        data = (string.ascii_letters + string.digits + '._+') * 2017
        previous_job_id = dxpy.JOB_ID
        # Optimization is only applied within a job environment
        dxpy.set_job_id('job-000000000000000000000000')
        try:
            file_id = dxpy.upload_string(data, wait_on_close=True).get_id()
            for first_read_length in [65498, 120001, 230001]:
                fh = dxpy.DXFile(file_id)
                first_read = fh.read(first_read_length)
                cptr = fh.tell()
                self.assertEqual(cptr, min(first_read_length, len(data)))
                next_read = fh.read(2 ** 16)
                fh.seek(cptr)
                read_after_seek = fh.read(2 ** 16)
                self.assertEqual(next_read, read_after_seek)
                self.assertEqual(next_read, data[first_read_length:first_read_length + 2 ** 16].encode('utf-8'))
        finally:
            dxpy.set_job_id(previous_job_id)

    def test_iter_dxfile(self):
        dxid = ""
        with dxpy.new_dxfile() as self.dxfile:
            dxid = self.dxfile.get_id()
            self.dxfile.write("Line 1\nLine 2\nLine 3\n")

        with dxpy.open_dxfile(dxid) as same_dxfile:
            same_dxfile.wait_on_close()
            self.assertTrue(same_dxfile.closed())

            lineno = 1
            for line in same_dxfile:
                self.assertEqual(line, "Line " + str(lineno))
                lineno += 1

    def test_dxfile_errors(self):
        self.dxfile = dxpy.new_dxfile()
        self.dxfile.write("Line 1\nLine 2\nLine 3\n")

        with self.assertRaises(DXFileError):
            self.dxfile.read(3)
        with self.assertRaises(DXFileError):
            for line in self.dxfile:
                pass

    def test_file_context_manager(self):
        with dxpy.new_dxfile(mode='w') as self.dxfile:
            file_id = self.dxfile.get_id()
            self.dxfile.write("Haha")
        file2 = dxpy.open_dxfile(file_id)
        state = file2._get_state()
        self.assertTrue(state in ['closing', 'closed'])
        file2._wait_on_close()
        self.assertEqual(file2.describe()["size"], 4)

    def test_file_context_manager_destructor(self):
        dxfile = dxpy.new_dxfile(mode='w')
        dxfile.write("Haha")
        # No assertion here, but this should print an error

    def test_download_url_helper(self):
        dxfile = dxpy.upload_string(self.foo_str, wait_on_close=True)
        opts = {"preauthenticated": True, "filename": "foo"}
        # File download token/URL is cached
        dxfile = dxpy.open_dxfile(dxfile.get_id())
        url1 = dxfile.get_download_url(**opts)
        url2 = dxfile.get_download_url(**opts)
        self.assertEqual(url1, url2)
        # Cache is invalidated when the client knows the token has expired
        # (subject to clock skew allowance of 30s)
        dxfile = dxpy.open_dxfile(dxfile.get_id())
        url3 = dxfile.get_download_url(duration=30, **opts)
        url4 = dxfile.get_download_url(**opts)
        self.assertNotEqual(url3, url4)

    def test_download_url_rejects_invalid_project(self):
        dxfile = dxpy.upload_string(self.foo_str, wait_on_close=True)
        with testutil.temporary_project() as p, self.assertRaises(ResourceNotFound):
            # The file doesn't exist in this project
            dxfile.get_download_url(project=p.get_id())
        with self.assertRaises(ResourceNotFound):
            # This project doesn't even exist
            dxfile.get_download_url(project="project-012301230123012301230123")

    def test_get_download_url_from_handler(self):
        dxfile = dxpy.upload_string(self.foo_str, wait_on_close=True)
        url = dxfile.get_download_url()

        # Create a new DXFile handler with the correct project id.
        dxfile1 = dxpy.DXFile(dxfile.get_id(), project=dxfile.get_proj_id())
        url1 = dxfile1.get_download_url()
        self.assertEqual(url, url1)

        with testutil.temporary_project() as p:
            # Create a new DXFile handler with a project that does not correspond to the file.
            dxfile2 = dxpy.DXFile(dxfile.get_id(), project=p.get_id())
            url1 = dxfile2.get_download_url()
            # Verify that url1 is a tuple with a url and header
            self.assertTrue(len(url1) == 2)
            # Verify that the url contains the file id.
            self.assertTrue(dxfile2.get_id() in url1[0])
            # Verify that the url does not contain the project id from the wrong project.
            self.assertFalse(p.get_id() in url1[0])

    def test_part_splitting(self):
        with dxpy.new_dxfile(write_buffer_size=4 * 1024 * 1024, mode='w', project=self.proj_id) as myfile:
            myfile.write("0" * 8195384)
        myfile.wait_on_close()
        self.assertTrue(myfile.closed())

        # Check file was split up into parts appropriately
        parts = myfile.describe(fields={"parts": True})['parts']
        self.assertEquals(parts['1']['size'], 5242880)
        self.assertEquals(parts['2']['size'], 2952504)

    def test_download_in_job_env(self):
        os.environ['DX_JOB_ID'] = "fake_job_id"
        dxfile = dxpy.upload_string(self.foo_str, wait_on_close=True)

        with testutil.temporary_project() as p:
            with self.assertRaises(ResourceNotFound):
                dxfile.get_download_url(project=p.get_id())

            # The call should succeed if no project is specified
            url = dxfile.get_download_url()
            # Verify that url1 is a tuple with a url and header
            self.assertTrue(len(url) == 2)
            # Verify that the url contains the file id.
            self.assertTrue(dxfile.get_id() in url[0])
            # Verify that the url does not contain the project id from the wrong project.
            self.assertFalse(p.get_id() in url[0])

            # Getting the url by specifying the wrong project should fail in a job environment
            with self.assertRaises(ResourceNotFound):
                dxfile2 = dxpy.DXFile(dxid=dxfile.get_id(), project=p.get_id())
                dxfile2.get_download_url(project=dxfile2.get_proj_id())

            url2 = dxfile2.get_download_url()
            self.assertEqual(url, url2)

            # Change the current workspace to a project that does not contain the file
            workspace_id = dxpy.WORKSPACE_ID
            dxpy.WORKSPACE_ID = p.get_id()
            dxfile3 = dxpy.DXFile(dxid=dxfile.get_id())
            url3 = dxfile3.get_download_url()
            self.assertEqual(url, url3)
            dxpy.WORKSPACE_ID = workspace_id

        del os.environ['DX_JOB_ID']


class TestFolder(unittest.TestCase):

    def setUp(self):
        setUpTempProjects(self)
        self.temp_dir = tempfile.mkdtemp(prefix="test.dx-toolkit.dxpy.TestFolder.")
        self.temp_file_fd, self.temp_file_path = tempfile.mkstemp(prefix="test.dx-toolkit.dxpy.TestFile.")
        with os.fdopen(self.temp_file_fd, 'w') as temp_file:
            temp_file.write('42')

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        os.remove(self.temp_file_path)
        tearDownTempProjects(self)

    def test_download_folder(self):
        dxproject = dxpy.DXProject(self.proj_id)

        # Creating remote folders
        dxproject.new_folder("/a/b/c/d", parents=True)
        dxproject.new_folder("/a/e/f/g", parents=True)
        dxproject.new_folder("/h/i/j/k", parents=True)

        # Filling remote folders with objects
        for i, folder in enumerate(["/", "/a", "/a/b", "/a/b/c", "/a/b/c/d"]):
            dxpy.upload_string("{}-th\n file\n content\n".format(i + 1), wait_on_close=True,
                    name="file_{}.txt".format(i + 1), folder=folder)
            dxrecord = dxpy.new_dxrecord(name="record_{}".format(i + 1), folder=folder)

        # Checking root directory download
        root_dest_dir = os.path.join(self.temp_dir, "root")
        dxpy.download_folder(self.proj_id, root_dest_dir)
        path = []
        for i, f in enumerate([root_dest_dir, "a", "b", "c", "d"]):
            path.append(f)
            filename = os.path.join(os.path.join(*path), "file_{}.txt".format(i + 1))
            self.assertTrue(os.path.isfile(filename))
            self.assertEquals("{}-th\n file\n content\n".format(i + 1), open(filename, "r").read())
        self.assertTrue(os.path.isdir(os.path.join(root_dest_dir, "a", "e", "f", "g")))
        self.assertTrue(os.path.isdir(os.path.join(root_dest_dir, "h", "i", "j", "k")))

        # Checking non-root directory download
        a_dest_dir = os.path.join(self.temp_dir, "a")
        dxpy.download_folder(self.proj_id, a_dest_dir, folder="/a/")
        path = []
        for i, f in enumerate([a_dest_dir, "b", "c", "d"]):
            path.append(f)
            filename = os.path.join(os.path.join(*path), "file_{}.txt".format(i + 2))
            self.assertTrue(os.path.isfile(filename))
            self.assertEquals("{}-th\n file\n content\n".format(i + 2), open(filename, "r").read())

        # Checking 2-nd level subdirectory download
        ag = os.path.join(self.temp_dir, "b")
        dxpy.download_folder(self.proj_id, ag, folder="/a/b/")
        path = []
        for i, f in enumerate([ag, "c", "d"]):
            path.append(f)
            filename = os.path.join(os.path.join(*path), "file_{}.txt".format(i + 3))
            self.assertTrue(os.path.isfile(filename))
            self.assertEquals("{}-th\n file\n content\n".format(i + 3), open(filename, "r").read())

        # Checking download to existing structure
        dxpy.download_folder(self.proj_id, a_dest_dir, folder="/a", overwrite=True)
        path = []
        for i, f in enumerate([a_dest_dir, "b", "c", "d"]):
            path.append(f)
            filename = os.path.join(os.path.join(*path), "file_{}.txt".format(i + 2))
            self.assertTrue(os.path.isfile(filename))
            self.assertEquals("{}-th\n file\n content\n".format(i + 2), open(filename, "r").read())

        # Checking download to existing structure fails w/o overwrite flag
        with self.assertRaises(DXFileError):
            dxpy.download_folder(self.proj_id, a_dest_dir, folder="/a")

        # Checking download to existing file fails
        with self.assertRaises(DXFileError):
            dxpy.download_folder(self.proj_id, self.temp_file_path)

        # Checking download to file instead of subdir fails
        a1_dest_dir = os.path.join(self.temp_dir, "a1")
        os.mkdir(a1_dest_dir)
        with open(os.path.join(a1_dest_dir, "b"), "w") as f:
            f.write("42")
        with self.assertRaises(DXFileError):
            dxpy.download_folder(self.proj_id, a1_dest_dir, folder="/a")

        # Checking download to non-writable location fails
        if sys.platform != "win32":
            with self.assertRaises(OSError):
                dxpy.download_folder(self.proj_id, "/usr/bin/a", folder="/a")

        # Checking download to empty location fails
        with self.assertRaises(DXFileError):
            dxpy.download_folder(self.proj_id, " ", folder="/a")

        # Checking download from empty location fails
        with self.assertRaises(DXFileError):
            dxpy.download_folder(self.proj_id, os.path.join(self.temp_dir, "foobar"), folder="\t")

        # Checking download from non-existent location fails
        with self.assertRaises(DXFileError):
            dxpy.download_folder(self.proj_id, os.path.join(self.temp_dir, "foobar"), folder="/non_existent")

        # Checking download from invalid location fails
        with self.assertRaises(DXFileError):
            dxpy.download_folder(self.proj_id, os.path.join(self.temp_dir, "foobar"), folder="a/b")


class TestDXRecord(unittest.TestCase):
    """
    Most of these tests really are testing DXDataObject methods
    while using DXRecords as the most basic data object.
    """

    def setUp(self):
        setUpTempProjects(self)

    def tearDown(self):
        tearDownTempProjects(self)

    def test_set_id(self):
        dxrecord = dxpy.new_dxrecord()
        second_dxrecord = dxpy.DXRecord()
        second_dxrecord.set_ids(dxrecord.get_id(), dxrecord.get_proj_id())
        self.assertEqual(second_dxrecord.get_id(), dxrecord.get_id())
        self.assertEqual(second_dxrecord.get_proj_id(), self.proj_id)
        dxrecord.remove()

    def test_create_remove_dxrecord(self):
        '''Create a fresh DXRecord object and check that its ID is
        stored and that the record object has been stored.
        '''

        firstDXRecord = dxpy.new_dxrecord(details=["foo"])
        firstID = firstDXRecord.get_id()
        # test if firstDXRecord._dxid has been set to a valid ID
        try:
            self.assertRegexpMatches(firstDXRecord.get_id(), "^record-[0-9A-Za-z]{24}",
                                     'Object ID not of expected form: ' + \
                                         firstDXRecord.get_id())
        except AttributeError:
            self.fail("dxID was not stored in DXRecord creation")
        # test if firstDXRecord._proj has been set to proj_id
        self.assertEqual(firstDXRecord.get_proj_id(), self.proj_id)
        # test if details were set
        self.assertEqual(firstDXRecord.get_details(), ["foo"])

        '''Create a second DXRecord object which should use the first
        object's ID.  Check that its ID is stored and that it can be
        accessed.
        '''
        secondDXRecord = dxpy.DXRecord(firstDXRecord.get_id())
        self.assertEqual(firstDXRecord.get_id(), secondDXRecord.get_id())

        '''Create a new DXRecord object which should generate a new ID
        and in a different project.
        '''
        secondDXRecord.new(project=self.second_proj_id, details=["bar"])
        self.assertNotEqual(firstDXRecord.get_id(), secondDXRecord.get_id())
        # test if secondDXRecord._dxid has been set to a valid ID
        try:
            self.assertRegexpMatches(secondDXRecord.get_id(), "^record-[0-9A-Za-z]{24}",
                                     'Object ID not of expected form: ' + \
                                         secondDXRecord.get_id())
        except AttributeError:
            self.fail("dxID was not stored in DXRecord creation")
        # test if secondDXRecord._proj has been set to second_proj_id
        self.assertEqual(secondDXRecord.get_proj_id(), self.second_proj_id)
        # test if details were set
        self.assertEqual(secondDXRecord.get_details(), ["bar"])

        '''
        Remove the records
        '''
        try:
            firstDXRecord.remove()
        except DXError as error:
            self.fail("Unexpected error when removing record object: " +
                      str(error))

        self.assertIsNone(firstDXRecord.get_id())

        try:
            secondDXRecord.remove()
        except DXError as error:
            self.fail("Unexpected error when removing record object: " +
                      str(error))

        self.assertIsNone(secondDXRecord.get_id())

        third_record = dxpy.DXRecord(firstID)

        with self.assertRaises(DXAPIError) as cm:
            third_record.describe()
            self.assertEqual(cm.exception.name, "ResourceNotFound")

    def test_init_from(self):
        dxrecord = dxpy.new_dxrecord(details={"foo": "bar"}, types=["footype"],
                                     tags=["footag"])
        second_record = dxpy.new_dxrecord(init_from=dxrecord, types=["bartype"])
        first_desc = dxrecord.describe(incl_details=True)
        second_desc = second_record.describe(incl_details=True)
        self.assertEqual(first_desc["details"], second_desc["details"])
        self.assertEqual(first_desc["name"], second_desc["name"])
        self.assertEqual(first_desc["tags"], second_desc["tags"])
        self.assertFalse(first_desc["types"] == second_desc["types"])

    def test_describe_dxrecord(self):
        dxrecord = dxpy.new_dxrecord()
        desc = dxrecord.describe()
        self.assertEqual(desc["project"], self.proj_id)
        self.assertEqual(desc["id"], dxrecord.get_id())
        self.assertEqual(desc["class"], "record")
        self.assertEqual(desc["types"], [])
        self.assertTrue("created" in desc)
        self.assertEqual(desc["state"], "open")
        self.assertEqual(desc["hidden"], False)
        self.assertEqual(desc["links"], [])
        self.assertEqual(desc["name"], dxrecord.get_id())
        self.assertEqual(desc["folder"], "/")
        self.assertEqual(desc["tags"], [])
        self.assertTrue("modified" in desc)
        self.assertFalse("properties" in desc)
        self.assertFalse("details" in desc)

        desc = dxrecord.describe(incl_properties=True)
        self.assertEqual(desc["properties"], {})

        desc = dxrecord.describe(incl_details=True)
        self.assertEqual(desc["details"], {})

        types = ["mapping", "foo"]
        tags = ["bar", "baz"]
        properties = {"project": "cancer"}
        hidden = True
        details = {"$dnanexus_link": dxrecord.get_id()}
        folder = "/a"
        name = "Name"

        second_dxrecord = dxpy.new_dxrecord(types=types,
                                            properties=properties,
                                            hidden=hidden,
                                            details=details,
                                            tags=tags,
                                            folder=folder,
                                            parents=True,
                                            name=name)
        desc = second_dxrecord.describe(True, True)
        self.assertEqual(desc["project"], self.proj_id)
        self.assertEqual(desc["id"], second_dxrecord.get_id())
        self.assertEqual(desc["class"], "record")
        self.assertEqual(desc["types"], types)
        self.assertTrue("created" in desc)
        self.assertEqual(desc["state"], "open")
        self.assertEqual(desc["hidden"], hidden)
        self.assertEqual(desc["links"], [dxrecord.get_id()])
        self.assertEqual(desc["name"], name)
        self.assertEqual(desc["folder"], "/a")
        self.assertEqual(desc["tags"], tags)
        self.assertTrue("modified" in desc)
        self.assertEqual(desc["properties"], properties)
        self.assertEqual(desc["details"], details)

    def test_getattr_dxrecord(self):
        dxrecord = dxpy.new_dxrecord(name='foo')
        record_id = dxrecord.get_id()
        self.assertEqual(dxrecord.name, 'foo')
        with self.assertRaises(AttributeError):
            dxrecord.foo
        dxrecord.remove()
        dxrecord.set_ids(record_id)
        with self.assertRaises(DXAPIError):
            dxrecord.name

    def test_set_properties_of_dxrecord(self):
        dxrecord = dxpy.new_dxrecord()
        properties = {"project": "cancer project", "foo": "bar"}
        dxrecord.set_properties(properties)
        desc = dxrecord.describe(True)
        self.assertEqual(desc["properties"], properties)

        dxrecord.set_properties({"project": None})
        self.assertEqual(dxrecord.describe(True)["properties"], {"foo": "bar"})

    def test_types_of_dxrecord(self):
        dxrecord = dxpy.new_dxrecord()
        types = ["foo", "othertype"]
        dxrecord.add_types(types)
        self.assertEqual(dxrecord.describe()["types"], types)

        dxrecord.remove_types(["foo"])
        self.assertEqual(dxrecord.describe()["types"], ["othertype"])

    def test_tags_of_dxrecord(self):
        dxrecord = dxpy.new_dxrecord()
        tags = ["foo", "othertag"]
        dxrecord.add_tags(tags)
        self.assertEqual(dxrecord.describe()["tags"], tags)

        dxrecord.remove_tags(["foo"])
        self.assertEqual(dxrecord.describe()["tags"], ["othertag"])

    def test_visibility_of_dxrecord(self):
        dxrecord = dxpy.new_dxrecord()
        dxrecord.hide()
        self.assertEqual(dxrecord.describe()["hidden"], True)

        dxrecord.unhide()
        self.assertEqual(dxrecord.describe()["hidden"], False)

    def test_rename_dxrecord(self):
        dxrecord = dxpy.new_dxrecord()
        dxrecord.rename("newname")
        self.assertEqual(dxrecord.describe()["name"], "newname")

        dxrecord.rename("secondname")
        self.assertEqual(dxrecord.describe()["name"], "secondname")

    def test_list_projects_dxrecord(self):
        dxrecord = dxpy.new_dxrecord()
        dxrecord.close()
        second_dxrecord = dxrecord.clone(self.second_proj_id)
        self.assertTrue(self.proj_id in dxrecord.list_projects())
        self.assertTrue(self.second_proj_id in dxrecord.list_projects())

    def test_close_dxrecord(self):
        dxrecord = dxpy.new_dxrecord()
        dxrecord.close()
        with self.assertRaises(DXAPIError):
            dxrecord.hide()
        with self.assertRaises(DXAPIError):
            dxrecord.set_details(["foo"])

        self.assertEqual(dxrecord.get_details(), {})
        dxrecord.rename("newname")
        self.assertEqual(dxrecord.describe()["name"], "newname")

        dxrecord.rename("secondname")
        self.assertEqual(dxrecord.describe()["name"], "secondname")

    def test_get_set_details(self):
        details_no_link = {"foo": "bar"}

        dxrecord = dxpy.new_dxrecord()
        dxrecord.set_details(details_no_link)
        self.assertEqual(dxrecord.get_details(), details_no_link)
        self.assertEqual(dxrecord.describe()["links"], [])

        details_two_links = [{"$dnanexus_link": dxrecord.get_id()},
                             {"$dnanexus_link": dxrecord.get_id()}]

        dxrecord.set_details(details_two_links)
        self.assertEqual(dxrecord.get_details(), details_two_links)
        self.assertEqual(dxrecord.describe()["links"], [dxrecord.get_id()])

    def test_clone(self):
        dxrecord = dxpy.new_dxrecord(name="firstname", tags=["tag"])

        with self.assertRaises(DXAPIError):
            second_dxrecord = dxrecord.clone(self.second_proj_id)
        dxrecord.close()

        second_dxrecord = dxrecord.clone(self.second_proj_id)
        second_dxrecord.rename("newname")

        first_desc = dxrecord.describe()
        second_desc = second_dxrecord.describe()

        self.assertEqual(first_desc["id"], dxrecord.get_id())
        self.assertEqual(second_desc["id"], dxrecord.get_id())
        self.assertEqual(first_desc["project"], self.proj_id)
        self.assertEqual(second_desc["project"], self.second_proj_id)
        self.assertEqual(first_desc["name"], "firstname")
        self.assertEqual(second_desc["name"], "newname")
        self.assertEqual(first_desc["tags"], ["tag"])
        self.assertEqual(second_desc["tags"], ["tag"])
        self.assertEqual(first_desc["created"], second_desc["created"])
        self.assertEqual(first_desc["state"], "closed")
        self.assertEqual(second_desc["state"], "closed")

    def test_move(self):
        dxproject = dxpy.DXProject()
        dxproject.new_folder("/a/b/c/d", parents=True)
        dxrecord = dxpy.new_dxrecord()
        dxrecord.move("/a/b/c")
        listf = dxproject.list_folder()
        self.assertEqual(listf["objects"], [])
        listf = dxproject.list_folder("/a/b/c")
        self.assertEqual(get_objects_from_listf(listf), [dxrecord.get_id()])
        desc = dxrecord.describe()
        self.assertEqual(desc["folder"], "/a/b/c")

    def test_passthrough_args(self):
        dxrecord = dxpy.new_dxrecord(auth=dxpy.AUTH_HELPER)
        with self.assertRaises(TypeError):
            dxrecord = dxpy.new_dxrecord(foo=1)

    def test_custom_describe_fields(self):
        dxrecord = dxpy.new_dxrecord(name="recordname", tags=["tag"], details={}, folder="/")
        self.assertEqual(dxrecord.describe(fields={"name", "tags"}),
                         {"id": dxrecord.get_id(), "name": "recordname", "tags": ["tag"]})
        self.assertEqual(dxrecord.describe(fields={"name", "tags"}, default_fields=False),
                         {"id": dxrecord.get_id(), "name": "recordname", "tags": ["tag"]})
        describe_with_custom_fields = dxrecord.describe(fields={"name", "properties"}, default_fields=True)
        self.assertIn('name', describe_with_custom_fields)
        self.assertIn('modified', describe_with_custom_fields)
        self.assertIn('properties', describe_with_custom_fields)
        self.assertNotIn('details', describe_with_custom_fields)


@unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run a job')
class TestDXAppletJob(unittest.TestCase):
    def setUp(self):
        setUpTempProjects(self)

    def tearDown(self):
        tearDownTempProjects(self)

    def test_dxjob_init_and_set_id(self):
        for good_value in ["job-aB3456789012345678901234",
                           "localjob-1",
                           None]:
            dxjob = dxpy.DXJob(good_value)
            dxjob.set_id(good_value)
        for bad_value in ["foo",
                          "project-123456789012345678901234",
                          3,
                          {},
                          "job-aB34567890123456789012345",
                          "job-aB345678901234567890123"]:
            with self.assertRaises(DXError):
                dxpy.DXJob(bad_value)
            with self.assertRaises(DXError):
                dxjob = dxpy.DXJob()
                dxjob.set_id(bad_value)

    def test_run_dxapplet_and_job_metadata(self):
        dxapplet = dxpy.DXApplet()
        dxapplet.new(name="test_applet",
                     dxapi="1.04",
                     inputSpec=[{"name": "chromosomes", "class": "record"},
                                {"name": "rowFetchChunk", "class": "int"}
                            ],
                     outputSpec=[{"name": "mappings", "class": "record"}],
                     runSpec={"code": '''
@dxpy.entry_point('main')
def main():
    pass
''',
                              "interpreter": "python2.7",
                              "execDepends": [{"name": "python-numpy"}]})
        dxrecord = dxpy.new_dxrecord()
        dxrecord.close()
        prog_input = {"chromosomes": {"$dnanexus_link": dxrecord.get_id()},
                      "rowFetchChunk": 100}
        dxjob = dxapplet.run(applet_input=prog_input, details={"$dnanexus_link": "hello world"},
                             tags=['foo', '$foo.bar'], properties={'$dnanexus_link.foo': 'barbaz'},
                             priority="normal")
        jobdesc = dxjob.describe()
        self.assertEqual(jobdesc["class"], "job")
        self.assertEqual(jobdesc["function"], "main")
        self.assertEqual(jobdesc["originalInput"], prog_input)
        self.assertEqual(jobdesc["originJob"], jobdesc["id"])
        self.assertEqual(jobdesc["parentJob"], None)
        self.assertEqual(jobdesc["applet"], dxapplet.get_id())
        self.assertEqual(jobdesc["project"], dxapplet.get_proj_id())
        self.assertTrue("state" in jobdesc)
        self.assertTrue("created" in jobdesc)
        self.assertTrue("modified" in jobdesc)
        self.assertTrue("launchedBy" in jobdesc)
        self.assertTrue("output" in jobdesc)
        self.assertTrue("$dnanexus_link" in jobdesc["details"])
        self.assertEqual(jobdesc["details"]["$dnanexus_link"], "hello world")
        self.assertEqual(jobdesc["tags"].sort(), ['foo', '$foo.bar'].sort())
        self.assertEqual(len(jobdesc["properties"]), 1)
        self.assertEqual(jobdesc["properties"]["$dnanexus_link.foo"], "barbaz")
        self.assertEqual(jobdesc["priority"], "normal")

        # Test setting tags and properties on job
        dxjob.add_tags(["foo", "bar", "foo"])
        dxjob.set_properties({"foo": "bar", "$dnanexus.link": "thing"})
        jobdesc = dxjob.describe()
        self.assertEqual(set(jobdesc["tags"]), set(["foo", "bar", "$foo.bar"]))
        self.assertEqual(jobdesc["properties"], {"foo": "bar",
                                                 "$dnanexus.link": "thing",
                                                 "$dnanexus_link.foo": "barbaz"})
        dxjob.remove_tags(["bar", "baz"])
        dxjob.set_properties({"$dnanexus.link": None})
        jobdesc = dxjob.describe()
        self.assertEqual(set(jobdesc["tags"]), set(["foo", "$foo.bar"]))
        self.assertEqual(jobdesc["properties"], {"foo": "bar", "$dnanexus_link.foo": "barbaz"})

        # Test with fields parameter
        smaller_jobdesc = dxjob.describe(fields={"id": True, "state": True, "parentJob": True})
        self.assertEqual(len(smaller_jobdesc), 3)
        self.assertEqual(smaller_jobdesc['id'], dxjob.get_id())
        self.assertIsInstance(smaller_jobdesc['state'], basestring)
        self.assertIsNone(smaller_jobdesc['parentJob'])

        with self.assertRaises(DXError):
            dxjob.describe(fields={"id": True}, io=False)
        with self.assertRaises(DXError):
            dxjob.describe(fields={"id": True}, io=True)

        dxjob.terminate()

class TestDXWorkflow(unittest.TestCase):
    default_inst_type = "mem2_hdd2_x2"
    codeSpec = '''
@dxpy.entry_point('main')
def main(number):
    raise # Ensure that the applet fails
'''

    def setUp(self):
        setUpTempProjects(self)

    def tearDown(self):
        tearDownTempProjects(self)

    def test_dxanalysis_init_and_set_id(self):
        for good_value in ["analysis-aB3456789012345678901234", None]:
            dxanalysis = dxpy.DXAnalysis(good_value)
            dxanalysis.set_id(good_value)
        for bad_value in ["foo",
                          "project-123456789012345678901234",
                          3,
                          {},
                          "analysis-aB34567890123456789012345",
                          "analysis-aB345678901234567890123"]:
            with self.assertRaises(DXError):
                dxpy.DXAnalysis(bad_value)
            with self.assertRaises(DXError):
                dxanalysis = dxpy.DXAnalysis()
                dxanalysis.set_id(bad_value)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping test that would run a job')
    def test_run_workflow_and_analysis_metadata(self):
        dxworkflow = dxpy.DXWorkflow(dxpy.api.workflow_new({"project": self.proj_id,
                                                            "outputFolder": "/output"})['id'])
        dxapplet = dxpy.DXApplet()
        dxapplet.new(name="test_applet",
                     dxapi="1.04",
                     inputSpec=[{"name": "number", "class": "int"},
                                {"name": "othernumber", "class": "int"}],
                     outputSpec=[{"name": "number", "class": "int"}],
                     runSpec={"code": self.codeSpec,
                               "interpreter": "python2.7"})
        stage_id = dxpy.api.workflow_add_stage(dxworkflow.get_id(),
                                               {"editVersion": 0,
                                                "name": "stagename",
                                                "executable": dxapplet.get_id()})['stage']
        dxanalysis = dxworkflow.run({"0.number": 32,
                                     "stagename.othernumber": 42})
        dxanalysis.terminate()
        with self.assertRaises(DXJobFailureError):
            dxanalysis.wait_on_done(timeout=20)
        analysis_desc = dxanalysis.describe()
        self.assertEqual(analysis_desc['folder'], '/output')
        self.assertEqual(analysis_desc['input'].get(stage_id + '.number'), 32)
        self.assertEqual(analysis_desc['input'].get(stage_id + '.othernumber'), 42)
        dxjob = dxpy.DXJob(analysis_desc['stages'][0]['execution']['id'])
        self.assertEqual(dxjob.describe()['input'].get("number"), 32)

        # Test setting tags and properties on analysis
        dxanalysis.add_tags(["foo", "bar", "foo"])
        dxanalysis.set_properties({"foo": "bar", "$dnanexus.link": "thing"})
        analysis_desc = dxanalysis.describe()
        self.assertEqual(set(analysis_desc["tags"]), set(["foo", "bar"]))
        self.assertEqual(analysis_desc["properties"], {"foo": "bar", "$dnanexus.link": "thing"})
        dxanalysis.remove_tags(["bar", "baz"])
        dxanalysis.set_properties({"$dnanexus.link": None})
        analysis_desc = dxanalysis.describe()
        self.assertEqual(analysis_desc["tags"], ["foo"])
        self.assertEqual(analysis_desc["properties"], {"foo": "bar"})

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run a job')
    def test_run_workflow_with_instance_type(self):
        dxworkflow = dxpy.DXWorkflow(dxpy.api.workflow_new({"project": self.proj_id})['id'])
        dxapplet = dxpy.DXApplet()
        dxapplet.new(name="test_applet",
                     dxapi="1.04",
                     inputSpec=[],
                     outputSpec=[],
                     runSpec={"code": '',
                              "interpreter": "bash"})
        stage_id = dxpy.api.workflow_add_stage(dxworkflow.get_id(),
                                               {"editVersion": 0,
                                                "name": "stagename",
                                                "executable": dxapplet.get_id()})['stage']
        # control (no request)
        analysis_describe = testutil.analysis_describe_with_retry(dxworkflow.run({}))
        dxjob = dxpy.DXJob(analysis_describe['stages'][0]['execution']['id'])
        self.assertEqual(dxjob.describe()['instanceType'], self.default_inst_type)

        # request for all stages and all entry points
        analysis_describe = testutil.analysis_describe_with_retry(dxworkflow.run({}, instance_type="mem2_hdd2_x1"))
        dxjob = dxpy.DXJob(analysis_describe['stages'][0]['execution']['id'])
        self.assertEqual(dxjob.describe()['instanceType'], 'mem2_hdd2_x1')

        # request for all stages, overriding some entry points
        analysis_describe = testutil.analysis_describe_with_retry(
            dxworkflow.run({}, instance_type={"*": "mem2_hdd2_x1", "foo": "mem2_hdd2_x2"}))
        dxjob = dxpy.DXJob(analysis_describe['stages'][0]['execution']['id'])
        self.assertEqual(dxjob.describe()['instanceType'], 'mem2_hdd2_x1')

        # request for the stage specifically, for all entry points
        analysis_describe = testutil.analysis_describe_with_retry(
            dxworkflow.run({}, stage_instance_types={stage_id: "mem2_hdd2_x2"}))
        dxjob = dxpy.DXJob(analysis_describe['stages'][0]['execution']['id'])
        self.assertEqual(dxjob.describe()['instanceType'], 'mem2_hdd2_x2')

        # request for the stage specifically, overriding some entry points
        analysis_describe = testutil.analysis_describe_with_retry(
            dxworkflow.run({}, stage_instance_types={stage_id: {"*": "mem2_hdd2_x2", "foo": "mem2_hdd2_x1"}}))
        dxjob = dxpy.DXJob(analysis_describe['stages'][0]['execution']['id'])
        self.assertEqual(dxjob.describe()['instanceType'], 'mem2_hdd2_x2')

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run a job')
    def test_run_workflow_with_stage_folders(self):
        dxworkflow = dxpy.new_dxworkflow(output_folder="/output")
        dxapplet = dxpy.DXApplet()
        dxapplet.new(name="test_applet",
                     dxapi="1.04",
                     inputSpec=[],
                     outputSpec=[],
                     runSpec={"code": '', "interpreter": "bash"})
        dxworkflow.add_stage(dxapplet, name="stagename", folder="foo")
        second_stage_id = dxworkflow.add_stage(dxapplet, name="otherstagename", folder="/myoutput")

        # test cases; note that rerunning all stages is required since
        # changing the output folder does not constitute a good enough
        # reason to launch a new job

        # control (no request)
        control_dxanalysis = dxworkflow.run({})
        # override both options
        override_folders_dxanalysis = dxworkflow.run({},
                                                     stage_folders={"stagename": "/foo",
                                                                    1: "bar"},
                                                     rerun_stages=['*'])

        # use *
        use_default_folder_dxanalysis = dxworkflow.run({},
                                                       stage_folders={"*": "baz",
                                                                      second_stage_id: "quux"},
                                                       rerun_stages=['*'])

        desc = testutil.analysis_describe_with_retry(control_dxanalysis)
        self.assertEqual(desc['stages'][0]['execution']['folder'], '/output/foo')
        self.assertEqual(desc['stages'][1]['execution']['folder'], '/myoutput')
        desc = testutil.analysis_describe_with_retry(override_folders_dxanalysis)
        self.assertEqual(desc['stages'][0]['execution']['folder'], '/foo')
        self.assertEqual(desc['stages'][1]['execution']['folder'], '/output/bar')
        desc = testutil.analysis_describe_with_retry(use_default_folder_dxanalysis)
        self.assertEqual(desc['stages'][0]['execution']['folder'], '/output/baz')
        self.assertEqual(desc['stages'][1]['execution']['folder'], '/output/quux')

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run a job')
    def test_run_workflow_with_rerun_stages(self):
        dxworkflow = dxpy.new_dxworkflow()
        dxapplet = dxpy.DXApplet()
        dxapplet.new(name="test_applet",
                     dxapi="1.04",
                     inputSpec=[],
                     outputSpec=[],
                     runSpec={"code": '', "interpreter": "bash"})
        stage_id = dxworkflow.add_stage(dxapplet, name="stagename", folder="foo")

        # make initial analysis
        dxanalysis = dxworkflow.run({})
        job_ids = [dxanalysis.describe()['stages'][0]['execution']['id']]

        # empty rerun_stages should reuse results
        rerun_analysis = dxworkflow.run({}, rerun_stages=[])
        self.assertEqual(rerun_analysis.describe()['stages'][0]['execution']['id'],
                         job_ids[0])

        # use various identifiers to rerun the job
        for value in ['*', 0, stage_id, 'stagename']:
            rerun_analysis = dxworkflow.run({}, rerun_stages=[value])
            job_ids.append(rerun_analysis.describe()['stages'][0]['execution']['id'])
            self.assertNotIn(job_ids[-1], job_ids[:-1])

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that may run a job')
    def test_run_workflow_errors(self):
        dxworkflow = dxpy.DXWorkflow(dxpy.api.workflow_new({"project": self.proj_id})['id'])
        dxapplet = dxpy.DXApplet()
        dxapplet.new(name="test_applet",
                     dxapi="1.04",
                     inputSpec=[{"name": "number", "class": "int"}],
                     outputSpec=[{"name": "number", "class": "int"}],
                     runSpec={"code": self.codeSpec,
                               "interpreter": "python2.7"})
        dxworkflow.add_stage(dxapplet, name='stagename')

        # Can't specify the same input more than once (with a
        # stage-specific syntax)
        self.assertRaisesRegexp(DXError, 'more than once',
                                dxworkflow.run, {"0.number": 32, "stagename.number": 42})
        # Bad stage name
        self.assertRaisesRegexp(DXError, 'could not be found as a stage ID nor as a stage name',
                                dxworkflow.run, {"nonexistentstage.number": 32})

    def test_new_dxworkflow(self):
        # empty workflow
        blankworkflow = dxpy.new_dxworkflow()
        self.assertIsInstance(blankworkflow, dxpy.DXWorkflow)
        desc = blankworkflow.describe()
        self.assertEqual(desc['title'], blankworkflow.get_id())
        self.assertEqual(desc['summary'], '')
        self.assertEqual(desc['description'], '')
        self.assertEqual(desc['outputFolder'], None)
        self.assertEqual(desc['stages'], [])

        # workflow with metadata
        dxapplet = dxpy.DXApplet()
        dxapplet.new(name="test_applet",
                     dxapi="1.04",
                     inputSpec=[],
                     outputSpec=[],
                     runSpec={"code": '', "interpreter": "bash"})

        stage0 = {'id': 'stage_0',
                  'name': 'stage_0_name',
                  'executable': dxapplet.get_id(),
                  'folder': "/stage_0_output",
                  'executionPolicy': {'restartOn': {}, 'onNonRestartableFailure': 'failStage'},
                  'systemRequirements': {'main': {'instanceType': self.default_inst_type}}}
        stage1 = {'id': 'stage_1',
                  'executable': dxapplet.get_id()}

        dxworkflow = dxpy.new_dxworkflow(title='mytitle', summary='mysummary',
                                         description='mydescription', output_folder="/foo",
                                         stages=[stage0, stage1])
        stage_with_generated_id = dxworkflow.add_stage(dxapplet, name="stagename_generated_id", folder="foo")
        stage_with_user_id = dxworkflow.add_stage(dxapplet, stage_id="my_id", name="stagename_user_id", folder="foo")

        self.assertIsInstance(dxworkflow, dxpy.DXWorkflow)
        desc = dxworkflow.describe()
        self.assertEqual(desc['title'], 'mytitle')
        self.assertEqual(desc['summary'], 'mysummary')
        self.assertEqual(desc['description'], 'mydescription')
        self.assertEqual(desc['outputFolder'], '/foo')
        self.assertEqual(len(desc['stages']), 4)
        self.assertEqual(desc['stages'][0]['id'], 'stage_0')
        self.assertEqual(desc['stages'][0]['name'], 'stage_0_name')
        self.assertEqual(desc['stages'][1]['id'], 'stage_1')
        self.assertEqual(desc['stages'][1]['name'], None)
        self.assertEqual(desc['stages'][2]['id'], stage_with_generated_id)
        self.assertEqual(desc['stages'][2]['name'], 'stagename_generated_id')
        self.assertEqual(desc['stages'][3]['id'], stage_with_user_id)
        self.assertEqual(stage_with_user_id, 'my_id')
        self.assertEqual(desc['stages'][3]['name'], 'stagename_user_id')

        secondworkflow = dxpy.new_dxworkflow(init_from=dxworkflow)
        self.assertIsInstance(secondworkflow, dxpy.DXWorkflow)
        self.assertNotEqual(dxworkflow.get_id(), secondworkflow.get_id())
        desc = secondworkflow.describe()
        self.assertEqual(desc['title'], 'mytitle')
        self.assertEqual(desc['summary'], 'mysummary')
        self.assertEqual(desc['description'], 'mydescription')
        self.assertEqual(desc['outputFolder'], '/foo')
        self.assertEqual(len(desc['stages']), 4)
        self.assertEqual(desc['stages'][0]['id'], 'stage_0')
        self.assertEqual(desc['stages'][0]['name'], 'stage_0_name')
        self.assertEqual(desc['stages'][1]['id'], 'stage_1')
        self.assertEqual(desc['stages'][1]['name'], None)
        self.assertEqual(desc['stages'][2]['id'], stage_with_generated_id)
        self.assertEqual(desc['stages'][2]['name'], 'stagename_generated_id')
        self.assertEqual(desc['stages'][3]['id'], stage_with_user_id)
        self.assertEqual(stage_with_user_id, 'my_id')
        self.assertEqual(desc['stages'][3]['name'], 'stagename_user_id')

    def test_add_move_remove_stages(self):
        dxworkflow = dxpy.new_dxworkflow()
        dxapplet = dxpy.DXApplet()
        dxapplet.new(dxapi="1.0.0",
                     inputSpec=[{"name": "my_input", "class": "string"}],
                     outputSpec=[],
                     runSpec={"code": "", "interpreter": "bash"})
        # Add stages
        first_stage = dxworkflow.add_stage(dxapplet, name='stagename', folder="/outputfolder",
                                           stage_input={"my_input": "hello world"},
                                           instance_type="mem2_hdd2_x2")
        self.assertEqual(dxworkflow.editVersion, 1)
        self.assertEqual(dxworkflow.stages[0]["name"], "stagename")
        self.assertEqual(dxworkflow.stages[0]["folder"], "/outputfolder")
        self.assertEqual(dxworkflow.stages[0]["input"]["my_input"], "hello world")
        self.assertEqual(dxworkflow.stages[0]["systemRequirements"],
                         {"*": {"instanceType": "mem2_hdd2_x2"}})
        second_stage = dxworkflow.add_stage(dxapplet,
                                            name="stagename",
                                            folder="relativefolder",
                                            instance_type={"main": "mem2_hdd2_x2", "foo": "mem2_hdd2_x1"},
                                            edit_version=1)
        self.assertEqual(dxworkflow.editVersion, 2)
        self.assertEqual(len(dxworkflow.stages), 2)
        self.assertEqual(dxworkflow.stages[1]["executable"], dxapplet.get_id())
        self.assertEqual(dxworkflow.stages[1]["folder"], "relativefolder")
        self.assertEqual(dxworkflow.stages[1]["systemRequirements"],
                         {"main": {"instanceType": "mem2_hdd2_x2"},
                          "foo": {"instanceType": "mem2_hdd2_x1"}})
        with self.assertRaises(DXAPIError):
            dxworkflow.add_stage(dxapplet, edit_version=1)

        # Move stages
        dxworkflow.move_stage(0, 1)
        self.assertEqual(dxworkflow.editVersion, 3)
        self.assertEqual(dxworkflow.stages[0]["id"], second_stage)
        self.assertEqual(dxworkflow.stages[1]["id"], first_stage)
        dxworkflow.move_stage(first_stage, 0, edit_version=3)
        self.assertEqual(dxworkflow.editVersion, 4)
        self.assertEqual(dxworkflow.stages[0]["id"], first_stage)
        self.assertEqual(dxworkflow.stages[1]["id"], second_stage)

        # Remove stages

        # Removing stage by name doesn't work when there's more than
        # one of that name
        self.assertRaisesRegexp(DXError, 'more than one workflow stage was found',
                                dxworkflow.remove_stage, "stagename")

        removed_stage = dxworkflow.remove_stage(0)
        self.assertEqual(removed_stage, first_stage)
        self.assertEqual(dxworkflow.editVersion, 5)
        self.assertEqual(len(dxworkflow.stages), 1)
        self.assertEqual(dxworkflow.stages[0]["id"], second_stage)
        with self.assertRaises(DXError):
            dxworkflow.remove_stage(first_stage) # should already have been removed
        removed_stage = dxworkflow.remove_stage(second_stage, edit_version=5)
        self.assertEqual(removed_stage, second_stage)
        self.assertEqual(len(dxworkflow.stages), 0)
        # bad input throws DXError
        with self.assertRaises(DXError):
            dxworkflow.remove_stage({})
        with self.assertRaises(DXError):
            dxworkflow.remove_stage(5)

    def test_get_stage(self):
        dxworkflow = dxpy.new_dxworkflow()
        dxapplet = dxpy.DXApplet()
        dxapplet.new(dxapi="1.0.0",
                     inputSpec=[{"name": "my_input", "class": "string"}],
                     outputSpec=[],
                     runSpec={"code": "", "interpreter": "bash"})
        # Add stages
        first_stage = dxworkflow.add_stage(dxapplet, name='stagename', folder="/outputfolder",
                                           stage_input={"my_input": "hello world"})
        second_stage = dxworkflow.add_stage(dxapplet, name='stagename2', folder="/outputfolder",
                                            stage_input={"my_input": "hello world"})
        # Get stages
        test_cases = [[0, first_stage],
                      [first_stage, first_stage],
                      ['stagename', first_stage],
                      [1, second_stage],
                      [second_stage, second_stage],
                      ['stagename2', second_stage]]
        for tc in test_cases:
            stage_desc = dxworkflow.get_stage(tc[0])
            self.assertEqual(stage_desc['id'], tc[1])

        # Errors
        with self.assertRaises(DXError):
            dxworkflow.get_stage(-1)
        with self.assertRaises(DXError):
            dxworkflow.get_stage(3)
        with self.assertRaises(DXError):
            dxworkflow.get_stage('foo')
        with self.assertRaises(DXError):
            dxworkflow.get_stage('stage-123456789012345678901234')

    def test_update(self):
        dxworkflow = dxpy.new_dxworkflow(title='title', summary='summary', description='description', output_folder='/foo')
        self.assertEqual(dxworkflow.editVersion, 0)
        for metadata in ['title', 'summary', 'description']:
            self.assertEqual(getattr(dxworkflow, metadata), metadata)
        self.assertEqual(dxworkflow.outputFolder, '/foo')

        # update title, summary, description, outputFolder by value
        dxworkflow.update(title='Title', summary='Summary', description='Description', output_folder='/bar/baz')
        self.assertEqual(dxworkflow.editVersion, 1)
        for metadata in ['title', 'summary', 'description']:
            self.assertEqual(getattr(dxworkflow, metadata), metadata.capitalize())
        self.assertEqual(dxworkflow.outputFolder, '/bar/baz')

        # use unset_title
        dxworkflow.update(unset_title=True, edit_version=1)
        self.assertEqual(dxworkflow.editVersion, 2)
        self.assertEqual(dxworkflow.title, dxworkflow.get_id())

        # use unset_output_folder
        dxworkflow.update(unset_output_folder=True, edit_version=2)
        self.assertEqual(dxworkflow.editVersion, 3)
        self.assertIsNone(dxworkflow.outputFolder)

        # can't provide both title and unset_title=True
        with self.assertRaises(DXError):
            dxworkflow.update(title='newtitle', unset_title=True)
        self.assertEqual(dxworkflow.editVersion, 3)

        dxapplet = dxpy.DXApplet()
        dxapplet.new(dxapi="1.0.0",
                     inputSpec=[{"name": "my_input", "class": "string"}],
                     outputSpec=[],
                     runSpec={"code": "", "interpreter": "bash"})
        stage = dxworkflow.add_stage(dxapplet, name='stagename', folder="/outputfolder",
                                     stage_input={"my_input": "hello world"})
        self.assertEqual(dxworkflow.editVersion, 4)
        self.assertEqual(dxworkflow.stages[0]["input"]["my_input"], "hello world")

        # test stage modifications using update method
        dxworkflow.update(summary='newsummary',
                          stages={stage: {"folder": "/newoutputfolder",
                                          "input": {"my_input": None}}})
        self.assertEqual(dxworkflow.editVersion, 5)
        self.assertEqual(dxworkflow.summary, 'newsummary')
        self.assertEqual(dxworkflow.stages[0]["folder"], "/newoutputfolder")
        self.assertNotIn("my_input", dxworkflow.stages[0]["input"])

        # no-op update
        dxworkflow.update()
        self.assertEqual(dxworkflow.editVersion, 5)

    def test_update_stage(self):
        dxworkflow = dxpy.new_dxworkflow()
        dxapplet = dxpy.DXApplet()
        dxapplet.new(dxapi="1.0.0",
                     inputSpec=[{"name": "my_input", "class": "string"}],
                     outputSpec=[],
                     runSpec={"code": "", "interpreter": "bash"})
        # Add a stage
        stage = dxworkflow.add_stage(dxapplet, name='stagename', folder="/outputfolder",
                                     stage_input={"my_input": "hello world"},
                                     instance_type={"main": "mem2_hdd2_x2"})
        self.assertEqual(dxworkflow.editVersion, 1)
        self.assertEqual(dxworkflow.stages[0]["executable"], dxapplet.get_id())
        self.assertEqual(dxworkflow.stages[0]["name"], "stagename")
        self.assertEqual(dxworkflow.stages[0]["folder"], "/outputfolder")
        self.assertEqual(dxworkflow.stages[0]["input"]["my_input"], "hello world")
        self.assertEqual(dxworkflow.stages[0]["systemRequirements"],
                         {"main": {"instanceType": "mem2_hdd2_x2"}})

        # Update just its metadata
        dxworkflow.update_stage(stage, unset_name=True, folder="/newoutputfolder",
                                stage_input={"my_input": None},
                                instance_type="mem2_hdd2_x1")
        self.assertEqual(dxworkflow.editVersion, 2)
        self.assertIsNone(dxworkflow.stages[0]["name"])
        self.assertEqual(dxworkflow.stages[0]["folder"], "/newoutputfolder")
        self.assertNotIn("my_input", dxworkflow.stages[0]["input"])
        self.assertEqual(dxworkflow.stages[0]["systemRequirements"],
                         {"*": {"instanceType": "mem2_hdd2_x1"}})

        # Update using stage index
        dxworkflow.update_stage(0, folder="/", stage_input={"my_input": "foo"}, edit_version=2)
        self.assertEqual(dxworkflow.editVersion, 3)
        self.assertEqual(dxworkflow.stages[0]["folder"], "/")
        self.assertEqual(dxworkflow.stages[0]["input"]["my_input"], "foo")

        # no-op update
        dxworkflow.update_stage(0)
        self.assertEqual(dxworkflow.editVersion, 3)

        # error when providing name and unset_name
        with self.assertRaises(DXError):
            dxworkflow.update_stage(0, name='foo', unset_name=True)
        self.assertEqual(dxworkflow.editVersion, 3)

        # Update its executable
        second_applet = dxpy.DXApplet()
        second_applet.new(dxapi="1.0.0",
                          inputSpec=[{"name": "my_new_input", "class": "string"}],
                          outputSpec=[],
                          runSpec={"code": "", "interpreter": "bash"})

        # Incompatible executable
        try:
            dxworkflow.update_stage(stage, executable=second_applet)
            raise Exception("expected an error for updating a stage with an incompatible executable, but it succeeded")
        except DXAPIError as e:
            self.assertIsInstance(e, DXAPIError)
            self.assertEqual(e.name, 'InvalidState')
            self.assertEqual(dxworkflow.stages[0]["executable"], dxapplet.get_id())
            self.assertEqual(dxworkflow.editVersion, 3)

        # Successful update with force
        dxworkflow.update_stage(stage, executable=second_applet, force=True)
        self.assertEqual(dxworkflow.editVersion, 4)
        self.assertEqual(dxworkflow.stages[0]["executable"], second_applet.get_id())
        self.assertNotIn("my_input", dxworkflow.stages[0]["input"])


@unittest.skipUnless(testutil.TEST_ISOLATED_ENV,
                     'skipping test that would create an app')
class TestDXApp(unittest.TestCase):
    def setUp(self):
        setUpTempProjects(self)

    def tearDown(self):
        tearDownTempProjects(self)

    def test_init_and_set_id(self):
        for good_values in [("app-aB3456789012345678901234", None, None),
                            (None, 'name', 'tag'),
                            (None, 'name', None),
                            (None, None, None)]:
            dxapp = dxpy.DXApp(*good_values)
            dxapp.set_id(*good_values)
        for bad_values in [("foo", None, None),
                           ("app-aB3456789012345678901234", 'name', None),
                           ("app-aB3456789012345678901234", None, 'tag'),
                           ("app-aB3456789012345678901234", 'name', 'tag'),
                           ("project-123456789012345678901234", None, None),
                           (3, None, None),
                           ({}, None, None),
                           ("app-aB34567890123456789012345", None, None),
                           ("app-aB345678901234567890123", None, None),
                           (None, 3, None),
                           (None, 'name', {})]:
            with self.assertRaises(DXError):
                dxpy.DXApp(*bad_values)
            with self.assertRaises(DXError):
                dxapp = dxpy.DXApp()
                dxapp.set_id(*bad_values)

    def test_create_app(self):
        dxapplet = dxpy.DXApplet()
        dxapplet.new(name="test_applet",
                     dxapi="1.04",
                     inputSpec=[{"name": "chromosomes", "class": "record"},
                                {"name": "rowFetchChunk", "class": "int"}
                            ],
                     outputSpec=[{"name": "mappings", "class": "record"}],
                     runSpec={"code": "def main(): pass",
                              "interpreter": "python2.7",
                              "execDepends": [{"name": "python-numpy"}]})
        dxapp = dxpy.DXApp()
        my_userid = dxpy.whoami()
        dxapp.new(applet=dxapplet.get_id(), version="0.0.1", bill_to=my_userid, name="app_name")
        appdesc = dxapp.describe()
        self.assertEqual(appdesc["name"], "app_name")
        self.assertEqual(appdesc["version"], "0.0.1")
        self.assertTrue("0.0.1" in appdesc["aliases"])
        self.assertTrue("default" in appdesc["aliases"])
        dxsameapp = dxpy.DXApp(name="app_name")
        sameappdesc = dxsameapp.describe()
        self.assertEqual(appdesc, sameappdesc)
        dxanothersameapp = dxpy.DXApp(name="app_name", alias="0.0.1")
        anothersameappdesc = dxanothersameapp.describe()
        self.assertEqual(appdesc, anothersameappdesc)

        # test fields parameter for describe (different cases for when
        # the handler was created different ways and therefore
        # sometimes doesn't have the _dxid field)
        smaller_desc = dxapp.describe(fields={"name": True, "version": True})
        self.assertEqual(len(smaller_desc), 2)
        self.assertEqual(smaller_desc['name'], 'app_name')
        self.assertEqual(smaller_desc['version'], '0.0.1')

        smaller_desc = dxsameapp.describe(fields={"name": True, "version": True})
        self.assertEqual(len(smaller_desc), 2)
        self.assertEqual(smaller_desc['name'], 'app_name')
        self.assertEqual(smaller_desc['version'], '0.0.1')

        smaller_desc = dxanothersameapp.describe(fields={"name": True, "version": True})
        self.assertEqual(len(smaller_desc), 2)
        self.assertEqual(smaller_desc['name'], 'app_name')
        self.assertEqual(smaller_desc['version'], '0.0.1')

    def test_add_and_remove_tags(self):
        """Test addition and removal of tags."""
        dxapplet = dxpy.DXApplet()
        dxapplet.new(name="test_add_and_remove_tags_applet",
                     dxapi="1.04",
                     inputSpec=[{"name": "chromosomes", "class": "record"},
                                {"name": "rowFetchChunk", "class": "int"}
                            ],
                     outputSpec=[{"name": "mappings", "class": "record"}],
                     runSpec={"code": "def main(): pass",
                              "interpreter": "python2.7",
                              "execDepends": [{"name": "python-numpy"}]})
        dxapp = dxpy.DXApp()
        my_userid = dxpy.whoami()
        dxapp.new(applet=dxapplet.get_id(), version="0.0.1", bill_to=my_userid, name="test_add_and_remove_tags_app")
        appdesc = dxapp.describe()

        self.assertEqual(appdesc.get("tags", []), [])

        # ResourceNotFound will be thrown if the alias cannot be found
        with self.assertRaises(DXAPIError):
            dxpy.DXApp(name="test_add_and_remove_tags_app", alias="moo").applet
        with self.assertRaises(DXAPIError):
            dxpy.DXApp(name="test_add_and_remove_tags_app", alias="oink").applet

        dxapp.add_tags(["moo", "oink"])

        self.assertEqual(dxapplet.get_id(), dxpy.DXApp(name="test_add_and_remove_tags_app", alias="moo").applet)
        self.assertEqual(dxapplet.get_id(), dxpy.DXApp(name="test_add_and_remove_tags_app", alias="oink").applet)

        dxapp.remove_tags(["moo"])

        with self.assertRaises(DXAPIError):
            dxpy.DXApp(name="test_add_and_remove_tags_app", alias="moo").applet
        self.assertEqual(dxapplet.get_id(), dxpy.DXApp(name="test_add_and_remove_tags_app", alias="oink").applet)

        dxapp.remove_tags(["oink"])

        with self.assertRaises(DXAPIError):
            dxpy.DXApp(name="test_add_and_remove_tags_app", alias="moo").applet
        with self.assertRaises(DXAPIError):
            dxpy.DXApp(name="test_add_and_remove_tags_app", alias="oink").applet


class TestDXSearch(unittest.TestCase):
    def setUp(self):
        setUpTempProjects(self)

    def tearDown(self):
        tearDownTempProjects(self)

    def test_resolve_data_objects(self):
        # If the project is provided for an object, then it will be used instead of
        # the top-level project specification
        with testutil.temporary_project(name='test_resolve_data_objects') as p:
            dxrecord0 = dxpy.new_dxrecord(name="myrecord0", project=self.proj_id)
            dxrecord1 = dxpy.new_dxrecord(name="myrecord1", project=p.get_id())
            dxpy.new_dxrecord(name="myrecord2", project=self.proj_id)
            dxpy.new_dxrecord(name="myrecord3", project=p.get_id())
            records = [{"name": "myrecord0", "project": self.proj_id, "folder": "/"},
                       {"name": "myrecord1"},
                       {"name": "myrecord2"},
                       {"name": "myrecord3", "project": self.proj_id, "folder": "/"}]

            objects = list(dxpy.search.resolve_data_objects(records, project=p.get_id()))
            self.assertEqual(len(objects), 4)
            self.assertEqual(objects[0][0]["project"], self.proj_id)
            self.assertEqual(objects[0][0]["id"], dxrecord0.get_id())
            self.assertEqual(objects[1][0]["project"], p.get_id())
            self.assertEqual(objects[1][0]["id"], dxrecord1.get_id())
            self.assertEqual(objects[2], [])
            self.assertEqual(objects[3], [])

        # Test that batching happens correctly
        record_names = []
        record_ids = []
        for i in range(1005):
            record_ids.append(dxpy.new_dxrecord(name=("record" + str(i))).get_id())
            record_names.append({"name": "record" + str(i)})
        objects = list(dxpy.search.resolve_data_objects(record_names, project=self.proj_id))
        self.assertEqual(len(objects), 1005)
        self.assertEqual(objects[200][0]["id"], record_ids[200])
        self.assertEqual(objects[1003][0]["id"], record_ids[1003])

    def test_find_data_objs(self):
        dxrecord = dxpy.new_dxrecord()
        results = list(dxpy.search.find_data_objects(state="open", project=self.proj_id))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], {"project": self.proj_id,
                                      "id": dxrecord.get_id()})
        results = list(dxpy.search.find_data_objects(state="closed", project=self.proj_id))
        self.assertEqual(len(results), 0)
        dxrecord.close()
        results = list(dxpy.search.find_data_objects(state="closed", project=self.proj_id))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], {"project": self.proj_id,
                                      "id": dxrecord.get_id()})
        with self.assertRaises(DXError):
            dxpy.search.find_data_objects(tag='foo', tags=['foo', 'bar'])

    def test_find_data_objs_by_time(self):
        def query(**kwargs):
            return dxpy.search.find_data_objects(name='find_by_time', project=self.proj_id, **kwargs)

        dxrecord = dxpy.new_dxrecord(name='find_by_time')
        now = int(time.time()) * 1000

        # Negative integers interpreted as offsets (in ms) to the
        # current time
        #
        # Sleep for a short time so we can formulate tests that allow us
        # to verify that negative offsets are being interpreted as ms,
        # not as seconds.
        time.sleep(2.0)
        self.assertEqual(len(list(query(modified_after=-100))), 0)
        self.assertEqual(len(list(query(modified_after=-60 * 1000))), 1)
        self.assertEqual(len(list(query(modified_before=-60 * 1000))), 0)
        self.assertEqual(len(list(query(modified_before=-100))), 1)
        self.assertEqual(len(list(query(created_after=-60 * 1000))), 1)
        self.assertEqual(len(list(query(created_after=-100))), 0)
        self.assertEqual(len(list(query(created_before=-60 * 1000))), 0)
        self.assertEqual(len(list(query(created_before=-100))), 1)

        # Nonnegative integers interpreted as ms since epoch
        self.assertEqual(len(list(query(modified_after=now - 60 * 1000))), 1)
        self.assertEqual(len(list(query(modified_after=now + 60 * 1000))), 0)
        self.assertEqual(len(list(query(modified_before=now + 60 * 1000))), 1)
        self.assertEqual(len(list(query(modified_before=now - 60 * 1000))), 0)
        self.assertEqual(len(list(query(created_after=now - 60 * 1000))), 1)
        self.assertEqual(len(list(query(created_after=now + 60 * 1000))), 0)
        self.assertEqual(len(list(query(created_before=now + 60 * 1000))), 1)
        self.assertEqual(len(list(query(created_before=now - 60 * 1000))), 0)

        # Strings with (negative int + suffix) to be interpreted as
        # offset from the current time
        self.assertEqual(len(list(query(modified_after="-60s"))), 1)
        self.assertEqual(len(list(query(modified_before="-60s"))), 0)
        self.assertEqual(len(list(query(created_after="-60s"))), 1)
        self.assertEqual(len(list(query(created_before="-60s"))), 0)
        # Positive numbers don't get the same treatment; currently, they
        # are interpreted as offsets to the Epoch
        #
        # self.assertEqual(len(list(query(modified_after="60s"))), 0)
        # self.assertEqual(len(list(query(modified_before="60s"))), 1)
        # self.assertEqual(len(list(query(created_after="60s"))), 0)
        # self.assertEqual(len(list(query(created_before="60s"))), 1)

    def test_find_data_objs_in_workspace(self):
        old_workspace = dxpy.WORKSPACE_ID
        dxpy.WORKSPACE_ID = self.proj_id
        try:
            record1 = dxpy.new_dxrecord(name="foo")
            record1.close()
            record2 = dxpy.new_dxrecord(name="bar", folder='/a', parents=True)
            record2.close()
            record3 = dxpy.new_dxrecord(name="baz", folder='/a/b', parents=True)
            record3.close()
            # find_data_objects should run search in workspace without
            # being explicitly told a project to search in.
            results1 = list(dxpy.search.find_data_objects(folder='/a', recurse=False))
            self.assertEqual(results1, [{"project": self.proj_id, "id": record2.get_id()}])
            results2 = list(dxpy.search.find_data_objects(folder='/a', recurse=True))
            self.assertEqual(set([result['id'] for result in results2]),
                             set([record2.get_id(), record3.get_id()]))
            self.assertEqual(list(dxpy.search.find_data_objects(name="foo", folder='/')),
                             [{"project": self.proj_id, "id": record1.get_id()}])
            self.assertEqual(len(list(dxpy.search.find_data_objects(name="ba*", folder='/'))), 0)
            self.assertEqual(len(list(dxpy.search.find_data_objects(name="ba*", name_mode="glob", folder='/'))), 2)
            self.assertEqual(len(list(dxpy.search.find_data_objects(name="ba.*", folder='/'))), 0)
            self.assertEqual(len(list(dxpy.search.find_data_objects(name="ba.*", name_mode="regexp", folder='/'))), 2)
        finally:
            dxpy.WORKSPACE_ID = old_workspace

    def test_find_projects(self):
        dxproject = dxpy.DXProject()
        results = list(dxpy.find_projects())
        found_proj = False
        for result in results:
            if result["id"] == dxproject.get_id():
                self.assertEqual(result["level"], 'ADMINISTER')
                found_proj = True
            self.assertFalse('describe' in result)
        self.assertTrue(found_proj)

        results = list(dxpy.find_projects(level='VIEW', describe=True))
        found_proj = False
        for result in results:
            if result["id"] == self.second_proj_id:
                self.assertEqual(result["level"], 'ADMINISTER')
                found_proj = True
                self.assertTrue('describe' in result)
                self.assertEqual(result['describe']['name'], 'test project 2')
                break
        self.assertTrue(found_proj)

        billed_to = dxproject.billTo
        results = list(dxpy.find_projects(billed_to=billed_to))
        found_proj = False
        for result in results:
            if result["id"] == dxproject.id:
                found_proj = True
                break
        self.assertTrue(found_proj)

        created = dxproject.created
        matching_ids = (result["id"] for result in dxpy.find_projects(created_before=created + 1000))
        self.assertIn(dxproject.id, matching_ids)

        matching_ids = (result["id"] for result in dxpy.find_projects(created_after=created - 1000))
        self.assertIn(dxproject.id, matching_ids)

        matching_ids = (result["id"] for result in
                        dxpy.find_projects(created_before=created + 1000, created_after=created - 1000))
        self.assertIn(dxproject.id, matching_ids)

        matching_ids = (result["id"] for result in dxpy.find_projects(created_before=created - 1000))
        self.assertNotIn(dxproject.id, matching_ids)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that requires presence of test org')
    def test_find_org_projects_created(self):
        org_id = "org-piratelabs"
        dxproject = dxpy.DXProject(self.proj_id)
        dxpy.api.project_update(dxproject.get_id(), {"billTo": org_id})
        project_ppb = "project-0000000000000000000000pb"
        org_projects = [dxproject.get_id(), project_ppb]

        created = dxproject.created
        matching_ids = (result["id"] for result in dxpy.org_find_projects(org_id, created_before=created + 1000))
        self.assertItemsEqual(matching_ids, org_projects)

        matching_ids = (result["id"] for result in dxpy.org_find_projects(org_id, created_after=created - 1000))
        self.assertItemsEqual(matching_ids, [dxproject.get_id()])

        matching_ids = (result["id"] for result in dxpy.org_find_projects(org_id, created_before=created + 1000,
                        created_after=created - 1000))
        self.assertItemsEqual(matching_ids, [dxproject.get_id()])

        matching_ids = (result["id"] for result in dxpy.org_find_projects(org_id, created_before=created - 1000))
        self.assertItemsEqual(matching_ids, [project_ppb])

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run a job')
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

        with self.assertRaises(DXError):
            dxapplet.run(applet_input=prog_input, stage_instance_types={0: "mem2_hdd2_x2"})

        dxapplet.run(applet_input=prog_input)
        dxjob = dxapplet.run(applet_input=prog_input, tags=["foo", "bar"], properties={"foo": "baz"})

        # Wait for jobs to be created
        testutil.analysis_describe_with_retry(dxanalysis)

        me = None
        common_conditions = {'executable': dxapplet,
                             'project': dxapplet.get_proj_id(),
                             'created_after': '-150s',
                             'describe': True,
                             'first_page_size': 1}
        methods = (dxpy.find_executions, dxpy.find_analyses, dxpy.find_jobs)
        queries = ({'conditions': {}, 'n_results': [4, 1, 3]},
                   {'conditions': {'include_subjobs': False}, 'n_results': [4, 1, 3]},
                   {'conditions': {'origin_job': dxjob.get_id(), 'parent_job': 'none'}, 'n_results': [1, 0, 1]},
                   {'conditions': {'origin_job': dxjob.get_id(), 'no_parent_job': True}, 'n_results': [1, 0, 1]},
                   {'conditions': {'root_execution': dxjob.get_id()}, 'n_results': [1, 0, 1]},
                   {'conditions': {'root_execution': dxanalysis.get_id()}, 'n_results': [2, 1, 1]},
                   {'conditions': {'parent_analysis': dxanalysis.get_id()}, 'n_results': [1, 0, 1]},
                   {'conditions': {'no_parent_analysis': True}, 'n_results': [3, 1, 2]},
                   {'conditions': {'name': 'test_applet'}, 'n_results': [3, 0, 3]},
                   {'conditions': {'executable': dxworkflow}, 'n_results': [1, 1, 0]},
                   {'conditions': {'name': 'find_executions test workflow'}, 'n_results': [1, 1, 0]},
                   {'conditions': {'name': '?est_apple*', 'name_mode': 'glob'}, 'n_results': [3, 0, 3]},
                   {'conditions': {'name': 'test_apples*', 'name_mode': 'glob'}, 'n_results': [0, 0, 0]},
                   {'conditions': {'name': '[t]+est_apple.+', 'name_mode': 'regexp'}, 'n_results': [3, 0, 3]},
                   {'conditions': {'name': 'test_apples.+', 'name_mode': 'regexp'}, 'n_results': [0, 0, 0]},
                   {'conditions': {'tags': ['foo']}, 'n_results': [2, 1, 1]},
                   {'conditions': {'tags': ['bar']}, 'n_results': [1, 0, 1]},
                   {'conditions': {'properties': {'foo': True}}, 'n_results': [2, 1, 1]},
                   {'conditions': {'properties': {'foo': 'baz'}}, 'n_results': [1, 0, 1]})

        for query in queries:
            for i in range(len(methods)):
                conditions = dict(common_conditions, **query['conditions'])
                conditions['launched_by'] = me
                try:
                    results = list(methods[i](**conditions))
                except:
                    print('Exception occurred when processing query: %r, method: %r' % (query, methods[i]),
                          file=sys.stderr)
                    raise
                if len(results) != query['n_results'][i]:
                    raise Exception("Query " + json.dumps(query['conditions']) + " returned " + str(len(results)) + ", but " + str(query['n_results'][i]) + " were expected")
                self.assertEqual(len(results), query['n_results'][i])
                if len(results) > 0:
                    result = results[0]
                    self.assertTrue("describe" in result)
                    if result['describe']['id'].startswith('analysis'):
                        self.assertEqual(result["id"], dxanalysis.get_id())
                    else:
                        self.assertEqual(result["describe"]["class"], "job")
                        self.assertEqual(result["describe"]["applet"], dxapplet.get_id())
                        self.assertEqual(result["describe"]["project"], dxapplet.get_proj_id())
                        self.assertEqual(result["describe"]["name"], 'test_applet')

                    me = result["describe"]["launchedBy"]

        bad_queries = [{'no_parent_job': True, 'parent_job': dxjob.get_id()},
                       {'no_parent_analysis': True, 'parent_analysis': dxanalysis.get_id()},
                       {'name': 'foo', 'name_mode': 'nonexistent'}]
        for query in bad_queries:
            for method in methods:
                with self.assertRaises(DXError):
                    method(**query)

class TestPrettyPrint(unittest.TestCase):
    def test_string_escaping(self):
        self.assertEqual(pretty_print.escape_unicode_string("a"), "a")
        self.assertEqual(pretty_print.escape_unicode_string("foo\nbar"), "foo\\nbar")
        self.assertEqual(pretty_print.escape_unicode_string("foo\x11bar"), "foo\\x11bar")
        self.assertEqual(pretty_print.escape_unicode_string("foo\n\t\rbar"), "foo\\n\\t\\rbar")
        self.assertEqual(pretty_print.escape_unicode_string("\n\\"), "\\n\\\\")
        self.assertEqual(pretty_print.escape_unicode_string("ïñtérnaçiònale"), "ïñtérnaçiònale")

class TestWarn(unittest.TestCase):
    def test_warn(self):
        warn("testing, one two three...")


class TestHTTPResponses(unittest.TestCase):
    def test_content_type_no_sniff(self):
        resp = dxpy.api.system_find_projects({'limit': 1}, want_full_response=True)
        self.assertEqual(resp.headers['x-content-type-options'], 'nosniff')

    def test_retry_after(self):
        # Do this weird dance here in case there is clock skew between
        # client and server
        start_time = int(time.time() * 1000)
        server_time = dxpy.DXHTTPRequest('/system/comeBackLater', {})['currentTime']
        dxpy.DXHTTPRequest('/system/comeBackLater', {'waitUntil': server_time + 8000})
        end_time = int(time.time() * 1000)
        time_elapsed = end_time - start_time
        self.assertTrue(8000 <= time_elapsed)
        self.assertTrue(time_elapsed <= 16000)

    def test_retry_after_exceeding_max_retries(self):
        start_time = int(time.time() * 1000)
        server_time = dxpy.DXHTTPRequest('/system/comeBackLater', {})['currentTime']
        dxpy.DXHTTPRequest('/system/comeBackLater', {'waitUntil': server_time + 20000})
        end_time = int(time.time() * 1000)
        time_elapsed = end_time - start_time
        self.assertTrue(20000 <= time_elapsed)
        self.assertTrue(time_elapsed <= 30000)

    def test_retry_after_without_header_set(self):
        start_time = int(time.time() * 1000)
        server_time = dxpy.DXHTTPRequest('/system/comeBackLater', {})['currentTime']
        dxpy.DXHTTPRequest('/system/comeBackLater',
                           {'waitUntil': server_time + 10000, 'setRetryAfter': False})
        end_time = int(time.time() * 1000)
        time_elapsed = end_time - start_time

        # We'd better have waited at least 10 seconds (accounting for up to 0.5
        # seconds of clock skew)
        self.assertTrue(9500 <= time_elapsed)
        # After 10 seconds we must have completed the original request, plus
        # either 3 or 4 retries (r3 or r4 below). (3 retries take at most 1 + 2
        # + 4 < 10 seconds, so we must have done at least 3, and 5 retries take
        # at least 1 + 1 + 2 + 4 + 8 = > 10 seconds, so we can't have completed
        # 5).
        #
        # Therefore, we're in the middle of waiting at most 8 or 16 seconds, so
        # after 10 seconds, we can have no more than 16 more seconds to wait.
        # Add 2 seconds for clock skew plus the time it takes to do the
        # requests themselves.

        # r <-1s-> r1 <-- 1-2s --> r2 <---- 2-4s ----> r3 <----- 4-8 s -----> r4 <-- ...
        self.assertTrue(time_elapsed <= 10000 + 16000 + 2000)

    def test_generic_exception_not_retryable(self):
        self.assertFalse(dxpy._is_retryable_exception(KeyError('oops')))

    def test_bad_host(self):
        # Verify that the exception raised is one that dxpy would
        # consider to be retryable, but truncate the actual retry loop
        with self.assertRaises(requests.packages.urllib3.exceptions.ProtocolError) as exception_cm:
            dxpy.DXHTTPRequest('http://doesnotresolve.dnanexus.com/', {}, prepend_srv=False, always_retry=False,
                               max_retries=1)
        self.assertTrue(dxpy._is_retryable_exception(exception_cm.exception))

    def test_connection_refused(self):
        # Verify that the exception raised is one that dxpy would
        # consider to be retryable, but truncate the actual retry loop
        with self.assertRaises(requests.packages.urllib3.exceptions.ProtocolError) as exception_cm:
            # Connecting to a port on which there is no server running
            dxpy.DXHTTPRequest('http://localhost:20406', {}, prepend_srv=False, always_retry=False, max_retries=1)
        self.assertTrue(dxpy._is_retryable_exception(exception_cm.exception))

    def test_case_insensitive_response_headers(self):
        # Verify that response headers support case-insensitive lookup.
        res = dxpy.DXHTTPRequest("/system/whoami", {}, want_full_response=True)
        self.assertTrue("CONTENT-type" in res.headers)

    def test_ssl_options(self):
        dxpy.DXHTTPRequest("/system/whoami", {}, verify=False)
        dxpy.DXHTTPRequest("/system/whoami", {}, verify=requests.certs.where())
        dxpy.DXHTTPRequest("/system/whoami", {}, verify=requests.certs.where(), cert_file=None, key_file=None)
        with self.assertRaises(TypeError):
            dxpy.DXHTTPRequest("/system/whoami", {}, cert="nonexistent")
        if dxpy.APISERVER_PROTOCOL == "https":
            with self.assertRaisesRegexp((TypeError,SSLError), "file|string"):
                dxpy.DXHTTPRequest("/system/whoami", {}, verify="nonexistent")
            with self.assertRaisesRegexp((SSLError, IOError, OpenSSL.SSL.Error), "file"):
                dxpy.DXHTTPRequest("/system/whoami", {}, cert_file="nonexistent")

    def test_fake_errors(self):
        dxpy.DXHTTPRequest('/system/fakeError', {'errorType': 'Valid JSON'}, always_retry=True)

        # Minimal latency with retries, in seconds. This makes sure we actually did a retry.
        min_sec_with_retries = 1
        max_num_retries = 2
        start_time = time.time()
        with self.assertRaises(ValueError):
            dxpy.DXHTTPRequest('/system/fakeError', {'errorType': 'Invalid JSON'},
                               max_retries=max_num_retries, always_retry=True)
        end_time = time.time()
        self.assertGreater(end_time - start_time, min_sec_with_retries)

        start_time = time.time()
        with self.assertRaises(ValueError):
            dxpy.DXHTTPRequest('/system/fakeError', {'errorType': 'Error not decodeable'},
                               max_retries=max_num_retries, always_retry=True)
        end_time = time.time()
        self.assertGreater(end_time - start_time, min_sec_with_retries)

    def test_system_headers_user_agent(self):
        headers = dxpy.api.system_headers()
        self.assertTrue('user-agent' in headers)
        self.assertTrue(bool(re.match("dxpy/\d+\.\d+\.\d+.*\s+\(.*\)", headers['user-agent'])))



class TestDataobjectFunctions(unittest.TestCase):
    def setUp(self):
        setUpTempProjects(self)

    def tearDown(self):
        tearDownTempProjects(self)

    def test_dxlink(self):
        # Wrap a data object in a link
        dxrecord = dxpy.new_dxrecord(project=self.proj_id)
        self.assertEqual(dxpy.dxlink(dxrecord.get_id()),
                         {"$dnanexus_link": dxrecord.get_id()})
        self.assertEqual(dxpy.dxlink(dxrecord, self.proj_id),
                         {"$dnanexus_link": {"project": self.proj_id, "id": dxrecord.get_id()}})
        self.assertEqual(dxpy.dxlink(dxrecord),
                         {"$dnanexus_link": dxrecord.get_id()})

        # Wrapping an existing link is a no-op
        self.assertEqual(dxpy.dxlink(dxpy.dxlink(dxrecord)),
                         dxpy.dxlink(dxrecord))
        dxjob = dxpy.DXJob('job-123456789012345678901234')
        self.assertEqual(dxpy.dxlink(dxjob.get_output_ref('output')),
                         dxjob.get_output_ref('output'))

        # is_dxlink works as expected
        self.assertFalse(dxpy.is_dxlink(None))
        self.assertFalse(dxpy.is_dxlink({}))
        self.assertFalse(dxpy.is_dxlink({"$dnanexus_link": None}))
        self.assertFalse(dxpy.is_dxlink({"$dnanexus_link": {}}))
        self.assertTrue(dxpy.is_dxlink({"$dnanexus_link": "x"}))
        self.assertTrue(dxpy.is_dxlink({"$dnanexus_link": {"id": None}}))
        self.assertTrue(dxpy.is_dxlink({"$dnanexus_link": {"job": None}}))

    def test_get_handler(self):
        dxpy.set_workspace_id(self.second_proj_id)

        dxrecord = dxpy.new_dxrecord(project=self.proj_id)
        # Simple DXLink
        dxlink = {'$dnanexus_link': dxrecord.get_id()}
        handler = dxpy.get_handler(dxlink)
        self.assertEqual(handler.get_id(), dxrecord.get_id())
        # Default project is not going to be the correct one
        self.assertNotEqual(handler.get_proj_id(), self.proj_id)

        # Extended DXLink
        dxlink = {'$dnanexus_link': {'id': dxrecord.get_id(),
                                     'project': self.proj_id}}
        handler = dxpy.get_handler(dxlink)
        self.assertEqual(handler.get_id(), dxrecord.get_id())
        self.assertEqual(handler.get_proj_id(), self.proj_id)

        # Handle project IDs
        handler = dxpy.get_handler(self.proj_id)
        self.assertEqual(handler._dxid, self.proj_id)

        # Handle apps
        handler = dxpy.get_handler("app-foo")
        self.assertIsNone(handler._dxid)
        self.assertEqual(handler._name, 'foo')
        self.assertEqual(handler._alias, 'default')

        handler = dxpy.get_handler("app-foo/1.0.0")
        self.assertIsNone(handler._dxid)
        self.assertEqual(handler._name, 'foo')
        self.assertEqual(handler._alias, '1.0.0')

        app_id = "app-123456789012345678901234"
        handler = dxpy.get_handler(app_id)
        self.assertEqual(handler._dxid, app_id)
        self.assertIsNone(handler._name)
        self.assertIsNone(handler._alias)

        # Test that we parse the "app" part out correctly when the app
        # name itself has a hyphen in it
        app_with_hyphen_in_name = "app-swiss-army-knife"
        handler = dxpy.get_handler(app_with_hyphen_in_name)
        self.assertIsNone(handler._dxid)
        self.assertEqual(handler._name, "swiss-army-knife")
        self.assertEqual(handler._alias, "default")

        handler = dxpy.get_handler(app_with_hyphen_in_name + "/1.0.0")
        self.assertIsNone(handler._dxid)
        self.assertEqual(handler._name, "swiss-army-knife")
        self.assertEqual(handler._alias, "1.0.0")

    def test_describe_data_objects(self):
        objects = []
        types = []
        tags = []
        objects.append(dxpy.new_dxrecord())
        types.append('record')
        tags.append([])
        objects.append(dxpy.new_dxfile())
        types.append('file')
        tags.append([])
        objects.append(dxpy.new_dxworkflow())
        types.append('workflow')
        tags.append(['my_tag'])
        objects[-1].add_tags(tags[-1])

        # Should be able to handle a mix of raw ids and dxlinks.
        ids = [o.get_id() for o in objects]
        desc = dxpy.describe(ids)

        self.assertEqual(len(ids), len(desc))
        for i in xrange(len(desc)):
            self.assertEqual(desc[i]["project"], self.proj_id)
            self.assertEqual(desc[i]["id"], ids[i])
            self.assertEqual(desc[i]["class"], types[i])
            self.assertEqual(desc[i]["types"], [])
            self.assertIn("created", desc[i])
            self.assertEqual(desc[i]["state"], "open")
            self.assertEqual(desc[i]["hidden"], False)
            self.assertEqual(desc[i]["links"], [])
            self.assertEqual(desc[i]["folder"], "/")
            self.assertEqual(desc[i]["tags"], tags[i])
            self.assertIn("modified", desc[i])
            self.assertNotIn("properties", desc[i])
            self.assertNotIn("details", desc[i])


class TestResolver(testutil.DXTestCase):
    def setUp(self):
        super(TestResolver, self).setUp()
        setUpTempProjects(self)

    def tearDown(self):
        tearDownTempProjects(self)
        super(TestResolver, self).tearDown()

    def test_resolve_path(self):
        need_project_context_to_resolve = ("^(Cannot resolve \".*\": e|E)xpected (a project name or ID to the left of "
                                           "(a|the) colon,|the path to be qualified with a project name or ID, and a "
                                           "colon;) or for a current project to be set$")

        dxpy.WORKSPACE_ID = self.project
        temp_proj_name = 'resolve_path_' + str(time.time())
        not_a_project_name = 'doesnt_exist_' + str(time.time())
        dxpy.config['DX_CLI_WD'] = '/a'
        with testutil.temporary_project(name=temp_proj_name) as p:
            self.assertEqual(resolve_path(""),
                             (self.project, "/a", None))
            with self.assertRaisesRegexp(ResolutionError, "expected the path to be a non-empty string"):
                resolve_path("", allow_empty_string=False)
            self.assertEqual(resolve_path(":"),
                             (self.project, "/", None))

            self.assertEqual(resolve_path("project-012301230123012301230123"),
                             ("project-012301230123012301230123", "/", None))
            self.assertEqual(resolve_path("container-012301230123012301230123"),
                             ("container-012301230123012301230123", "/", None))
            self.assertEqual(resolve_path("file-111111111111111111111111"),
                             (self.project, None, "file-111111111111111111111111"))
            # TODO: this shouldn't be treated as a data object ID
            self.assertEqual(resolve_path("job-111111111111111111111111"),
                             (self.project, None, "job-111111111111111111111111"))

            with self.assertRaisesRegexp(ResolutionError, 'foo'):
                resolve_path("project-012301230123012301230123:foo:bar")
            with self.assertRaises(ResolutionError):
                resolve_path(not_a_project_name + ":")
            with self.assertRaises(ResolutionError):
                resolve_path(not_a_project_name + ":foo")

            self.assertEqual(resolve_path(":foo"),
                             (self.project, "/", "foo"))
            self.assertEqual(resolve_path(":foo/bar"),
                             (self.project, "/foo", "bar"))
            self.assertEqual(resolve_path(":/foo/bar"),
                             (self.project, "/foo", "bar"))

            self.assertEqual(resolve_path(temp_proj_name + ":"),
                             (p.get_id(), "/", None))
            self.assertEqual(resolve_path(temp_proj_name + ":foo"),
                             (p.get_id(), "/", "foo"))
            self.assertEqual(resolve_path(temp_proj_name + ":foo/bar"),
                             (p.get_id(), "/foo", "bar"))
            self.assertEqual(resolve_path(temp_proj_name + ":/foo/bar"),
                             (p.get_id(), "/foo", "bar"))
            # WD is ignored in project-qualified paths, even if the
            # project is the project context
            self.assertEqual(resolve_path(self.project + ":foo/bar"),
                             (self.project, "/foo", "bar"))

            self.assertEqual(resolve_path("job-111122223333111122223333:foo"),
                             ("job-111122223333111122223333", None, "foo"))

            self.assertEqual(resolve_path("foo"),
                             (self.project, "/a", "foo"))
            self.assertEqual(resolve_path("foo/bar"),
                             (self.project, "/a/foo", "bar"))
            self.assertEqual(resolve_path("../foo"),
                             (self.project, "/", "foo"))
            self.assertEqual(resolve_path("../../foo"),
                             (self.project, "/", "foo"))
            self.assertEqual(resolve_path("*foo"),
                             (self.project, "/a", "*foo"))
            self.assertEqual(resolve_path("/foo"),
                             (self.project, "/", "foo"))
            self.assertEqual(resolve_path("/foo/bar"),
                             (self.project, "/foo", "bar"))

            self.assertEqual(resolve_path("project-012301230123012301230123:foo"),
                             ("project-012301230123012301230123", "/", "foo"))
            self.assertEqual(resolve_path("container-012301230123012301230123:foo"),
                             ("container-012301230123012301230123", "/", "foo"))
            self.assertEqual(resolve_path("project-012301230123012301230123:foo/bar"),
                             ("project-012301230123012301230123", "/foo", "bar"))
            self.assertEqual(resolve_path("project-012301230123012301230123:/foo"),
                             ("project-012301230123012301230123", "/", "foo"))
            self.assertEqual(resolve_path("project-012301230123012301230123:/foo/bar"),
                             ("project-012301230123012301230123", "/foo", "bar"))
            self.assertEqual(resolve_path("project-012301230123012301230123:file-000011112222333344445555"),
                             ("project-012301230123012301230123", "/", "file-000011112222333344445555"))

            # JSON
            self.assertEqual(resolve_path(json.dumps({"$dnanexus_link": "file-111111111111111111111111"})),
                             (self.project, None, "file-111111111111111111111111"))
            self.assertEqual(
                resolve_path(json.dumps({"$dnanexus_link": {"project": "project-012301230123012301230123",
                                                            "id": "file-111111111111111111111111"}})),
                ("project-012301230123012301230123", "/", "file-111111111111111111111111")
            )

            # --- test some behavior when workspace is not set ---
            dxpy.WORKSPACE_ID = None
            with self.assertRaisesRegexp(ResolutionError, need_project_context_to_resolve):
                resolve_path("")
            with self.assertRaisesRegexp(ResolutionError, need_project_context_to_resolve):
                resolve_path(":")
            with self.assertRaisesRegexp(ResolutionError, need_project_context_to_resolve):
                resolve_path(":foo")
            with self.assertRaisesRegexp(ResolutionError, need_project_context_to_resolve):
                resolve_path("foo", expected="folder")
            self.assertEqual(resolve_path(temp_proj_name + ":"),
                             (p.get_id(), "/", None))
            with self.assertRaisesRegexp(ResolutionError, need_project_context_to_resolve):
                resolve_path("foo")
            with self.assertRaisesRegexp(ResolutionError, need_project_context_to_resolve):
                resolve_path("../foo")
            with self.assertRaisesRegexp(ResolutionError, need_project_context_to_resolve):
                resolve_path("../../foo")
            with self.assertRaisesRegexp(ResolutionError, need_project_context_to_resolve):
                resolve_path("/foo/bar")

            self.assertEqual(resolve_path("file-111111111111111111111111"),
                             (None, None, "file-111111111111111111111111"))
            # TODO: this shouldn't be treated as a data object ID; it
            # should be treated just like "foo" above
            self.assertEqual(resolve_path("job-111111111111111111111111"),
                             (None, None, "job-111111111111111111111111"))

            self.assertEqual(resolve_path(temp_proj_name + ":"),
                             (p.get_id(), "/", None))
            self.assertEqual(resolve_path(temp_proj_name + ":foo"),
                             (p.get_id(), "/", "foo"))
            self.assertEqual(resolve_path(temp_proj_name + ":foo/bar"),
                             (p.get_id(), "/foo", "bar"))

            self.assertEqual(resolve_path("project-012301230123012301230123"),
                             ("project-012301230123012301230123", "/", None))
            self.assertEqual(resolve_path("container-012301230123012301230123"),
                             ("container-012301230123012301230123", "/", None))
            self.assertEqual(resolve_path("project-012301230123012301230123:foo"),
                             ("project-012301230123012301230123", "/", "foo"))
            self.assertEqual(resolve_path("container-012301230123012301230123:foo"),
                             ("container-012301230123012301230123", "/", "foo"))
            self.assertEqual(resolve_path("project-012301230123012301230123:foo/bar"),
                             ("project-012301230123012301230123", "/foo", "bar"))
            self.assertEqual(resolve_path("project-012301230123012301230123:file-000011112222333344445555"),
                             ("project-012301230123012301230123", "/", "file-000011112222333344445555"))

            self.assertEqual(resolve_path("job-111122223333111122223333:foo"),
                             ("job-111122223333111122223333", None, "foo"))

            self.assertEqual(resolve_path(json.dumps({"$dnanexus_link": "file-111111111111111111111111"})),
                             (None, None, "file-111111111111111111111111"))
            self.assertEqual(
                resolve_path(json.dumps({"$dnanexus_link": {"project": "project-012301230123012301230123",
                                                            "id": "file-111111111111111111111111"}})),
                ("project-012301230123012301230123", "/", "file-111111111111111111111111")
            )

            # TODO: test multi project. This may require us to find some
            # way to disable or programmatically drive the interactive
            # prompt

    def test_resolve_existing_path(self):
        self.assertEquals(resolve_existing_path(''),
                          (dxpy.WORKSPACE_ID, "/", None))
        with self.assertRaises(ResolutionError):
            resolve_existing_path('', allow_empty_string=False)
        self.assertEquals(resolve_existing_path(':'),
                          (dxpy.WORKSPACE_ID, "/", None))

        dxpy.WORKSPACE_ID = None
        with self.assertRaises(ResolutionError):
            resolve_existing_path("foo")
        with self.assertRaises(ResolutionError):
            resolve_existing_path("/foo/bar")

    def test_clean_folder_path(self):
        from dxpy.utils.resolver import clean_folder_path as clean
        self.assertEqual(clean(""), ("/", None))
        self.assertEqual(clean("/foo"), ("/", "foo"))
        self.assertEqual(clean("/foo/bar/baz"), ("/foo/bar", "baz"))
        self.assertEqual(clean("/foo/bar////baz"), ("/foo/bar", "baz"))
        self.assertEqual(clean("/foo/bar/baz/"), ("/foo/bar/baz", None))
        self.assertEqual(clean("/foo/bar/baz///"), ("/foo/bar/baz", None))
        self.assertEqual(clean("/foo/bar/baz", expected="folder"), ("/foo/bar/baz", None))
        self.assertEqual(clean("/foo/bar/baz/."), ("/foo/bar/baz", None))
        self.assertEqual(clean("/foo/bar/baz/.."), ("/foo/bar", None))
        self.assertEqual(clean("/foo/bar/../.."), ("/", None))
        self.assertEqual(clean("/foo/bar/../../.."), ("/", None))
        self.assertEqual(clean("/foo/bar/../../../"), ("/", None))
        self.assertEqual(clean("/foo/\\/bar/\\/"), ("/foo/\\/bar", "/"))
        self.assertEqual(clean("/foo/\\//bar/\\/"), ("/foo/\\//bar", "/"))
        self.assertEqual(clean("/foo/bar/\\]/\\["), ("/foo/bar/\\]", "["))
        self.assertEqual(clean("/foo/bar/baz/../quux"), ("/foo/bar", "quux"))
        self.assertEqual(clean("/foo/bar/../baz/../quux"), ("/foo", "quux"))
        self.assertEqual(clean("/foo/././bar/../baz/../quux"), ("/foo", "quux"))
        self.assertEqual(clean("/foo/bar/../baz/../../quux"), ("/", "quux"))
        self.assertEqual(clean("/foo/bar/../../baz/../../quux"), ("/", "quux"))

    def test_resolution_batching(self):
        from dxpy.bindings.search import resolve_data_objects
        record_id0 = dxpy.api.record_new({"project": self.proj_id,
                                          "dxapi": "1.0.0",
                                          "name": "resolve_record0"})['id']
        record_id1 = dxpy.api.record_new({"project": self.proj_id,
                                          "dxapi": "1.0.0",
                                          "name": "resolve_record1"})['id']
        record_id2 = dxpy.api.record_new({"project": self.proj_id,
                                          "dxapi": "1.0.0",
                                          "name": "resolve_record2"})['id']
        results = resolve_data_objects([{"name": "resolve_record0"},
                                        {"name": "resolve_record1"},
                                        {"name": "resolve_record2"}],
                                       self.proj_id, "/", batchsize=2)
        self.assertEqual(results[0][0]["id"], record_id0)
        self.assertEqual(results[1][0]["id"], record_id1)
        self.assertEqual(results[2][0]["id"], record_id2)

        results = resolve_data_objects([{"name": "resolve_record0"},
                                        {"name": "resolve_record1"},
                                        {"name": "resolve_record2"}],
                                       self.proj_id, "/", batchsize=4)
        self.assertEqual(results[0][0]["id"], record_id0)
        self.assertEqual(results[1][0]["id"], record_id1)
        self.assertEqual(results[2][0]["id"], record_id2)

    def test_is_project_explicit(self):
        # All files specified by path are understood as explicitly indicating a
        # project, because (if they actually resolve to something) such paths
        # can only ever be understood in the context of a single project.
        self.assertTrue(is_project_explicit("./path/to/my/file"))
        self.assertTrue(is_project_explicit("myproject:./path/to/my/file"))
        self.assertTrue(is_project_explicit("project-012301230123012301230123:./path/to/my/file"))
        # Paths that specify an explicit project with a colon are understood as
        # explicitly indicating a project (even if the file is specified by ID)
        self.assertTrue(is_project_explicit("projectname:file-012301230123012301230123"))
        self.assertTrue(is_project_explicit("project-012301230123012301230123:file-012301230123012301230123"))
        self.assertTrue(is_project_explicit(
            '{"$dnanexus_link": {"project": "project-012301230123012301230123", "id": "file-012301230123012301230123"}'
        ))
        # A bare file ID is NOT treated as having an explicit project. Even if
        # the user's configuration supplies a project context that contains
        # this file, that's not clear enough.
        self.assertFalse(is_project_explicit("file-012301230123012301230123"))
        self.assertFalse(is_project_explicit('{"$dnanexus_link": "file-012301230123012301230123"}'))
        # Colon without project in front of it is understood to mean the
        # current project
        self.assertTrue(is_project_explicit(":file-012301230123012301230123"))
        # Every job exists in a single project so we'll treat JBORs as being
        # identified with a single project, too
        self.assertTrue(is_project_explicit("job-012301230123012301230123:ofield"))


class TestIdempotentRequests(unittest.TestCase):
    def setUp(self):
        setUpTempProjects(self)

    def tearDown(self):
        tearDownTempProjects(self)

    code = '''@dxpy.entry_point('main')\ndef main():\n    pass'''
    run_spec = {"code": code, "interpreter": "python2.7"}

    # Create an applet using DXApplet.new
    def create_applet(self, name="app_name"):
        dxapplet = dxpy.DXApplet()
        dxapplet.new(name=name,
                     dxapi="1.04",
                     runSpec=self.run_spec,
                     inputSpec=[{"name": "number", "class": "int"}],
                     outputSpec=[{"name": "number", "class": "int"}])
        return dxapplet

    def do_retry_http_request(self, api_method, args=None, kwargs={}):
        if args is None:
            result = api_method(_test_retry_http_request=True, **kwargs)
        else:
            result = api_method(*args, _test_retry_http_request=True, **kwargs)
        return [result, dxpy._get_retry_response()]

    def get_a_nonce(self):
        return str(Nonce())

    def test_idempotent_record_creation(self):
        input_params = {"project": self.proj_id, "name": "Unique Record"}

        records = self.do_retry_http_request(dxpy.api.record_new, kwargs={"input_params": input_params})
        self.assertItemsEqual(records[0], records[1])

        dxrecord = dxpy.api.record_new(input_params=input_params)
        self.assertNotIn(dxrecord, records)
        records.append(dxrecord)

        # A request with the same nonce, but different input, should fail
        input_params.update({"nonce": self.get_a_nonce()})
        dxrecord = dxpy.api.record_new(input_params=input_params)
        self.assertNotIn(dxrecord, records)
        with self.assertRaises(DXAPIError):
            input_params.update({"name": "Diff Name"})
            dxpy.api.record_new(input_params=input_params)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create an app')
    def test_idempotent_applet_and_app_creation(self):
        input_params = {"project": self.proj_id,
                        "name": "new_applet",
                        "dxapi": "1.04",
                        "runSpec": self.run_spec
                        }

        applets = self.do_retry_http_request(dxpy.api.applet_new, kwargs={"input_params": input_params})
        self.assertItemsEqual(applets[0], applets[1])

        applet = dxpy.api.applet_new(input_params)
        self.assertNotIn(applet, applets)
        applets.append(applet)

        input_params.update({"nonce": self.get_a_nonce()})
        applet = dxpy.api.applet_new(input_params)
        self.assertNotIn(applet, applets)

        with self.assertRaises(DXAPIError):
            input_params.update({"name": "different_name"})
            dxpy.api.applet_new(input_params)

        userid = dxpy.whoami()
        dxapplet = self.create_applet("test_applet")
        input_params = {"applet": dxapplet.get_id(), "version": "0.0.1", "bill_to": userid, "name": "new_app_name"}
        apps = self.do_retry_http_request(dxpy.api.app_new, kwargs={"input_params": input_params})
        self.assertItemsEqual(apps[0]['id'], apps[1]['id'])

        # A request with the same nonce, but different input, should fail
        input_params = {"applet": dxapplet.get_id(),
                        "version": "0.0.1",
                        "bill_to": userid,
                        "name": "new_app_name_2",
                        "nonce": self.get_a_nonce()}
        app = dxpy.api.app_new(input_params)
        self.assertNotIn(app, apps)

        with self.assertRaises(DXAPIError):
            # This is throwing 500 error
            input_params.update({"name": "another_name"})
            dxpy.api.app_new(input_params)

    def test_idempotent_file_creation(self):
        input_params = {"project": self.proj_id, "name": "myFile.txt"}
        files = self.do_retry_http_request(dxpy.api.file_new, kwargs={"input_params": input_params})
        self.assertItemsEqual(files[0], files[1])

        dxfile = dxpy.api.file_new(input_params=input_params)
        self.assertNotIn(dxfile, files)
        files.append(dxfile)

        # A request with the same nonce, but different input, should fail
        input_params.update({"nonce": self.get_a_nonce()})
        dxfile = dxpy.api.file_new(input_params=input_params)
        self.assertNotIn(dxfile, files)
        with self.assertRaises(DXAPIError):
            input_params.update({"name": "differentFileName.txt"})
            dxpy.api.file_new(input_params)

    def test_idempotent_workflow_creation(self):
        input_params = {"project": self.proj_id, "name": "The workflow"}
        workflows = self.do_retry_http_request(dxpy.api.workflow_new, kwargs={"input_params": input_params})
        self.assertItemsEqual(workflows[0], workflows[1])

        dxworkflow = dxpy.api.workflow_new(input_params)
        self.assertNotIn(dxworkflow, workflows)
        workflows.append(dxworkflow)

        # A request with the same nonce, but different input, should fail
        input_params.update({"nonce": self.get_a_nonce()})
        dxworkflow = dxpy.api.workflow_new(input_params)
        self.assertNotIn(dxworkflow, workflows)
        with self.assertRaises(DXAPIError):
            input_params.update({"name": "Another workflow"})
            dxpy.api.workflow_new(input_params)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create an app')
    def test_idempotent_runs(self):
        # Create an applet and run it.
        applet = self.create_applet()

        input_params = {"input": {"number": 32}, "project": self.proj_id}
        jobs = self.do_retry_http_request(dxpy.api.applet_run,
                                          args=[applet.get_id()],
                                          kwargs={"input_params": input_params})
        self.assertItemsEqual(jobs[0], jobs[1])

        job = dxpy.api.applet_run(applet.get_id(), input_params=input_params)
        self.assertNotIn(job, jobs)
        jobs.append(job)

        input_params.update({"nonce": self.get_a_nonce()})
        job = dxpy.api.applet_run(applet.get_id(), input_params)
        self.assertNotIn(job, jobs)

        with self.assertRaises(DXAPIError):
            input_params['input'].update({"number": 42})
            dxpy.api.applet_run(applet.get_id(), input_params)

        ## Create an app and run it.
        app = dxpy.DXApp()
        userid = dxpy.whoami()

        app.new(applet=applet.get_id(), version="0.0.1", bill_to=userid, name="app_name_other")
        input_params = {"input": {"number": 32}, "project": self.proj_id}
        jobs = self.do_retry_http_request(dxpy.api.app_run,
                                          args=[app.get_id()],
                                          kwargs={"input_params": input_params})
        self.assertEqual(jobs[0], jobs[1])

        job = dxpy.api.app_run(app.get_id(), input_params=input_params)
        self.assertNotIn(job, jobs)
        jobs.append(job)

        input_params.update({"nonce": self.get_a_nonce()})
        job = dxpy.api.applet_run(applet.get_id(), input_params)
        self.assertNotIn(job, jobs)
        with self.assertRaises(DXAPIError):
            input_params['input'].update({"number": 42})
            dxpy.api.applet_run(applet.get_id(), input_params)

    @unittest.skipUnless(testutil.TEST_ISOLATED_ENV, 'skipping test that would create an org')
    def test_idempotent_org_creation(self):
        input_params = {"name": "test_org", "handle": "some_handle"}
        orgs = self.do_retry_http_request(dxpy.api.org_new, kwargs={"input_params": input_params})
        self.assertItemsEqual(orgs[0], orgs[1])

        input_params = {"name": "test_org2", "handle": "another_handle"}
        org = dxpy.api.org_new(input_params=input_params)
        self.assertNotIn(org, orgs)
        orgs.append(org)

        input_params = {"name": "test_org3", "handle": "another_handle_3", "nonce": self.get_a_nonce()}
        org = dxpy.api.org_new(input_params=input_params)
        self.assertNotIn(org, orgs)
        with self.assertRaises(DXAPIError):
            input_params.update({"name": "another_test_org"})
            dxpy.api.org_new(input_params=input_params)


class TestAppBuilderUtils(unittest.TestCase):
    def test_assert_consistent_regions(self):
        assert_consistent_regions = app_builder.assert_consistent_regions

        # These calls should not raise exceptions.

        assert_consistent_regions(None, None)
        assert_consistent_regions(None, ["aws:us-east-1"])
        assert_consistent_regions({"aws:us-east-1": None}, None)
        # The actual key-value pairs are irrelevant.
        assert_consistent_regions({"aws:us-east-1": None}, ["aws:us-east-1"])

        with self.assertRaises(app_builder.AppBuilderException):
            assert_consistent_regions({"aws:us-east-1": None}, ["azure:westus"])


if __name__ == '__main__':
    if dxpy.AUTH_HELPER is None:
        sys.exit(1, 'Error: Need to be logged in to run these tests')
    if 'DXTEST_FULL' not in os.environ:
        if 'DXTEST_ISOLATED_ENV' not in os.environ:
            sys.stderr.write('WARNING: neither env var DXTEST_FULL nor DXTEST_ISOLATED_ENV are set; tests that create apps will not be run\n')
        if 'DXTEST_RUN_JOBS' not in os.environ:
            sys.stderr.write('WARNING: neither env var DXTEST_FULL nor DXTEST_RUN_JOBS are set; tests that run jobs will not be run\n')
    unittest.main()
