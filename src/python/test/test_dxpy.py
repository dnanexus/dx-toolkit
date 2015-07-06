#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2013-2014 DNAnexus, Inc.
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
import requests
import string
import subprocess

import dxpy
import dxpy_testutil as testutil
from dxpy.exceptions import DXAPIError, DXFileError, DXError, DXJobFailureError, ServiceUnavailable, InvalidInput
from dxpy.utils import pretty_print, warn

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
    def test_get_buffer_size(self):
        for file_is_mmapd in (False, True):
            # This method implements its own sanity checks, so just
            # ensure that those pass for a variety of sizes.
            dxpy.bindings.dxfile_functions._get_buffer_size_for_file(0, file_is_mmapd=file_is_mmapd)
            dxpy.bindings.dxfile_functions._get_buffer_size_for_file(1, file_is_mmapd=file_is_mmapd)
            dxpy.bindings.dxfile_functions._get_buffer_size_for_file(5 * 1024 * 1024, file_is_mmapd=file_is_mmapd)
            dxpy.bindings.dxfile_functions._get_buffer_size_for_file(16 * 1024 * 1024, file_is_mmapd=file_is_mmapd)
            dxpy.bindings.dxfile_functions._get_buffer_size_for_file(160 * 1024 * 1024 * 1024, file_is_mmapd=file_is_mmapd)
            dxpy.bindings.dxfile_functions._get_buffer_size_for_file(290 * 1024 * 1024 * 1024, file_is_mmapd=file_is_mmapd)

    def test_job_detection(self):
        env = dict(os.environ, DX_JOB_ID='job-00000000000000000000')
        buffer_size = subprocess.check_output(
            "python -c 'import dxpy; print dxpy.bindings.dxfile.DEFAULT_BUFFER_SIZE'", shell=True, env=env)
        self.assertEqual(int(buffer_size), 96 * 1024 * 1024)
        del env['DX_JOB_ID']
        buffer_size = subprocess.check_output(
            "python -c 'import dxpy; print dxpy.bindings.dxfile.DEFAULT_BUFFER_SIZE'", shell=True, env=env)
        self.assertEqual(int(buffer_size), 16 * 1024 * 1024)


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

    def test_upload_string_dxfile(self):
        self.dxfile = dxpy.upload_string(self.foo_str)

        self.dxfile.wait_on_close()
        self.assertTrue(self.dxfile.closed())

        dxpy.download_dxfile(self.dxfile.get_id(), self.new_file.name)

        self.assertTrue(filecmp.cmp(self.foo_file.name, self.new_file.name))

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
        for opts in {}, {"preauthenticated": True, "filename": "foo"}:
            # File download token/URL is cached
            dxfile = dxpy.open_dxfile(dxfile.get_id())
            url1 = dxfile.get_download_url(**opts)
            url2 = dxfile.get_download_url(**opts)
            self.assertEqual(url1, url2)
            # Cache is invalidated when the client knows the token has expired
            # (subject to clock skew allowance of 60s)
            dxfile = dxpy.open_dxfile(dxfile.get_id())
            url3 = dxfile.get_download_url(duration=60, **opts)
            url4 = dxfile.get_download_url(**opts)
            self.assertNotEqual(url3, url4)

class TestDXGTable(unittest.TestCase):
    """
    TODO: Test iterators, gri, and other queries
    """
    def setUp(self):
        setUpTempProjects(self)

    def tearDown(self):
        try:
            self.dxgtable.flush()
        except:
            pass
        tearDownTempProjects(self)

    def test_col_desc(self):
        columns = [dxpy.DXGTable.make_column_desc("a", "string"),
                   dxpy.DXGTable.make_column_desc("b", "int32")]
        self.assertEqual(columns, [{"name": "a", "type": "string"},
                                   {"name": "b", "type": "int32"}])
        columns = [dxpy.DXGTable.make_column_desc("c", "int32"),
                   dxpy.DXGTable.make_column_desc("d", "string")]

    def test_create_table(self):
        self.dxgtable = dxpy.new_dxgtable(
            [dxpy.DXGTable.make_column_desc("a", "string"),
             dxpy.DXGTable.make_column_desc("b", "int32")])
        self.dxgtable.close(block=True)
        desc = self.dxgtable.describe()
        self.assertEqual(desc["columns"],
                         [dxpy.DXGTable.make_column_desc("a", "string"),
                          dxpy.DXGTable.make_column_desc("b", "int32")])

    def get_col_names(self):
        self.dxgtable = dxpy.new_dxgtable(
            [dxpy.DXGTable.make_column_desc("a", "string"),
             dxpy.DXGTable.make_column_desc("b", "int32")])
        self.dxgtable.close(block=True)
        col_names = self.dxgtable.get_col_names()
        self.assertEqual(col_names, ["__id__", "a", "b"])

    def test_add_rows(self):
        self.dxgtable = dxpy.new_dxgtable(
            [dxpy.DXGTable.make_column_desc("a", "string"),
             dxpy.DXGTable.make_column_desc("b", "int32")])
        self.dxgtable.add_rows(data=[], part=9999)
        # Wrong number of columns
        with self.assertRaises(ValueError):
            self.dxgtable.add_rows(data=[[]], part=9997)

        for i in range(64):
            self.dxgtable.add_rows(data=[["row"+str(i), i]], part=i+1)
        self.dxgtable.close(block=True)

        with self.assertRaises(DXAPIError):
            self.dxgtable.close(block=True)

    def test_add_rows_bad_data(self):
        self.dxgtable = dxpy.new_dxgtable([
                dxpy.DXGTable.make_column_desc("a", "string"),
                dxpy.DXGTable.make_column_desc("b", "float"),
                dxpy.DXGTable.make_column_desc("c", "int32"),
                dxpy.DXGTable.make_column_desc("d", "boolean"),
                ])
        # Wrong column types
        with self.assertRaises(ValueError):
            self.dxgtable.add_rows(data=[[303, 1.248, 123, True]], part=1) # Bad column 0
        with self.assertRaises(ValueError):
            self.dxgtable.add_rows(data=[["303", "1.248", 123, True]], part=2) # Bad column 1
        with self.assertRaises(ValueError):
            self.dxgtable.add_rows(data=[["303", 1.248, 123.5, True]], part=3) # Bad column 2
        with self.assertRaises(ValueError):
            self.dxgtable.add_rows(data=[["303", 1.248, 123, "True"]], part=4) # Bad column 3
        # Correct column types
        self.dxgtable.add_rows(data=[["303", 1.248, 123, True]], part=5)
        self.dxgtable.close(block=True)

    def test_add_rows_no_index(self):
        self.dxgtable = dxpy.new_dxgtable(
            [dxpy.DXGTable.make_column_desc("a", "string"),
             dxpy.DXGTable.make_column_desc("b", "int32")])
        for i in range(64):
            self.dxgtable.add_rows(data=[["row"+str(i), i]])

        self.dxgtable.flush()
        desc = self.dxgtable.describe()
        self.assertEqual(len(desc["parts"]), 1)

        self.dxgtable.close(block=True)

        desc = self.dxgtable.describe()
        self.assertEqual(desc["length"], 64)

    def test_table_context_manager(self):
        # Writing a new_dxgtable with parts
        with dxpy.new_dxgtable(
            [dxpy.DXGTable.make_column_desc("a", "string"),
             dxpy.DXGTable.make_column_desc("b", "int32")], mode='w') as dxgtable:
            for i in range(64):
                dxgtable.add_rows(data=[["row"+str(i), i]], part=i+1)

        # Writing a new_dxgtable without parts
        with dxpy.new_dxgtable([dxpy.DXGTable.make_column_desc("a", "string"),
                                dxpy.DXGTable.make_column_desc("b", "int32")], mode='w') as table2:
            table2_id = table2.get_id()
            for i in range(64):
                table2.add_rows(data=[["row"+str(i), i]])
        table2 = dxpy.open_dxgtable(table2_id)
        self.assertEqual(table2.describe()["length"], 64)
        table2.remove()

        # Writing an open_dxgtable
        table3_id = dxpy.new_dxgtable([dxpy.DXGTable.make_column_desc("a", "string"),
                                       dxpy.DXGTable.make_column_desc("b", "int32")]).get_id()
        with dxpy.open_dxgtable(table3_id, mode='a') as table3:
            for i in range(64):
                table3.add_rows(data=[["row"+str(i), i]])
        with dxpy.open_dxgtable(table3_id, mode='w') as table3:
            for i in range(64):
                table3.add_rows(data=[["row"+str(i), i]])
        table3 = dxpy.open_dxgtable(table3_id)
        state = table3._get_state()
        self.assertTrue(state in ['closing', 'closed'])
        table3._wait_on_close()
        self.assertEqual(table3.describe()["length"], 128)
        table3.remove()

    def test_table_context_manager_destructor(self):
        self.dxgtable = dxpy.new_dxgtable([dxpy.DXGTable.make_column_desc("a", "string"),
                                           dxpy.DXGTable.make_column_desc("b", "int32")])
        for i in range(64):
            self.dxgtable.add_rows(data=[["row"+str(i), i]])
        # No assertion here, but this should print an error

    def test_table_context_manager_error_handling(self):
        # In each case, the flush that happens at the close of the context handler should wait for
        # the asynchronous requests and then raise the resulting error.

        # Note that this test assumes that the error is a semantic error in the add_row data that
        # is NOT caught by any local error checking.

        # Use new_dxgtable
        with self.assertRaises(DXAPIError):
            with dxpy.new_dxgtable([dxpy.DXGTable.make_column_desc("a", "string"),
                                    dxpy.DXGTable.make_column_desc("b", "int32")], mode='w') as table1:
                table1.add_row(["", 68719476736]) # Not in int32 range

        # Use open_dxgtable and close table
        table2_id = dxpy.new_dxgtable([dxpy.DXGTable.make_column_desc("a", "string"),
                                       dxpy.DXGTable.make_column_desc("b", "int32")], mode='w').get_id()
        with self.assertRaises(DXAPIError):
            with dxpy.open_dxgtable(table2_id) as table2:
                table2.add_row(["", 68719476736]) # Not in int32 range
        # TODO: why does the flush in this table's destructor fail? Nothing should be getting
        # flushed then...

        # Use open_dxgtable and leave table open
        table3_id = dxpy.new_dxgtable([dxpy.DXGTable.make_column_desc("a", "string"),
                                       dxpy.DXGTable.make_column_desc("b", "int32")]).get_id()
        with self.assertRaises(DXAPIError):
            with dxpy.open_dxgtable(table3_id, mode='a') as table3:
                table3.add_row(["", 68719476736]) # Not in int32 range

    def test_create_table_with_invalid_spec(self):
        with self.assertRaises(DXAPIError):
            dxpy.new_dxgtable([dxpy.DXGTable.make_column_desc("a", "string"),
                              dxpy.DXGTable.make_column_desc("b", "muffins")])

    def test_get_rows(self):
        self.dxgtable = dxpy.new_dxgtable(
            [dxpy.DXGTable.make_column_desc("a", "string"),
             dxpy.DXGTable.make_column_desc("b", "int32")])
        for i in range(64):
            self.dxgtable.add_rows(data=[["row"+str(i), i]], part=i+1)
        with self.assertRaises(DXAPIError):
            rows = self.dxgtable.get_rows()
        self.dxgtable.close(block=True)
        rows = self.dxgtable.get_rows()['data']
        assert(len(rows) == 64)

        # TODO: test get_rows parameters, genomic range index when
        # implemented

    def test_iter_table(self):
        self.dxgtable = dxpy.new_dxgtable(
            [dxpy.DXGTable.make_column_desc("a", "string"),
             dxpy.DXGTable.make_column_desc("b", "int32")])
        for i in range(64):
            self.dxgtable.add_rows(data=[["row"+str(i), i]], part=i+1)
        self.dxgtable.close(block=True)

        counter = 0
        for row in self.dxgtable:
            self.assertEqual(row[2], counter)
            counter += 1
        self.assertEqual(counter, 64)

        counter = 0
        for row in self.dxgtable.iterate_rows(start=1):
            self.assertEqual(row[2], counter+1)
            counter += 1
        self.assertEqual(counter, 63)

        counter = 0
        for row in self.dxgtable.iterate_rows(end=2):
            self.assertEqual(row[2], counter)
            counter += 1
        self.assertEqual(counter, 2)

        counter = 0
        for row in self.dxgtable.iterate_rows(start=1, end=63):
            self.assertEqual(row[2], counter+1)
            counter += 1
        self.assertEqual(counter, 62)

        counter = 0
        for row in self.dxgtable.iterate_rows(columns=['a'], start=1, end=63):
            counter += 1
        self.assertEqual(counter, 62)

    def test_gri(self):
        data10 = [['chr2', 22, 28, 'j'],
                  ['chr1',  0,  3, 'a'],
                  ['chr1',  5,  8, 'b'],
                  ['chr1', 25, 30, 'i'],
                  ['chr1',  6, 10, 'c'],
                  ['chr1', 19, 20, 'h'],
                  ['chr1',  8,  9, 'd'],
                  ['chr1', 17, 19, 'g'],
                  ['chr1', 15, 23, 'e'],
                  ['chr1', 16, 21, 'f']];
        columns = [{ "name": 'foo', "type": 'string' },
                   { "name": 'bar', "type": 'int32' },
                   { "name": 'baz', "type": 'int32' },
                   { "name": 'quux', "type": 'string' }];
        genomic_index = dxpy.DXGTable.genomic_range_index('foo', 'bar', 'baz')
        self.assertEqual(genomic_index, {"name": "gri", "type": "genomic",
                                         "chr": "foo", "lo": "bar", "hi": "baz"})

        self.dxgtable = dxpy.new_dxgtable(columns, indices=[genomic_index])
        desc = self.dxgtable.describe()
        self.assertEqual(desc["indices"], [genomic_index]);

        self.dxgtable.add_rows(data10[:3], 1)
        self.dxgtable.add_rows(data10[3:6], 10)
        self.dxgtable.add_rows(data10[6:9], 100)
        self.dxgtable.add_rows(data10[9:], 1000)

        self.dxgtable.close(True)

        desc = self.dxgtable.describe()
        self.assertEqual(desc["length"], 10)

        # Offset + limit queries
        result = self.dxgtable.get_rows(starting=0, limit=1);
        self.assertEqual(result["data"],
                         [[0, 'chr1',  0,  3, 'a']]);
        self.assertEqual(result["next"], 1);
        self.assertEqual(result["length"], 1);

        result = self.dxgtable.get_rows(starting=4, limit=3);
        self.assertEqual(result["data"],
                         [[4, 'chr1', 15, 23, 'e'],
                          [5, 'chr1', 16, 21, 'f'],
                          [6, 'chr1', 17, 19, 'g']]);
        self.assertEqual(result["next"], 7);
        self.assertEqual(result["length"], 3);

        # Range query
        genomic_query = dxpy.DXGTable.genomic_range_query('chr1', 22, 25)
        result = self.dxgtable.get_rows(query=genomic_query)
        self.assertEqual(result["data"],
                         [[4, 'chr1', 15, 23, 'e']]);
        self.assertEqual(result["next"], None);
        self.assertEqual(result["length"], 1);

        # Range query with nonconsecutive rows in result
        genomic_query = dxpy.DXGTable.genomic_range_query('chr1', 20, 26)
        result = self.dxgtable.get_rows(query=genomic_query)
        self.assertEqual(result["data"],
                   [[4, 'chr1', 15, 23, 'e'],
                    [5, 'chr1', 16, 21, 'f'],
                    [8, 'chr1', 25, 30, 'i']]);
        self.assertEqual(result["next"], None);
        self.assertEqual(result["length"], 3);

        # Testing iterate_rows
        row_num = 5
        for row in self.dxgtable.iterate_rows(5, 8):
            self.assertEqual(row_num, row[0])
            row_num += 1
        self.assertEqual(row_num, 8)

        # Testing iterate_query_rows
        genomic_query = dxpy.DXGTable.genomic_range_query('chr1', 20, 26)
        result_num = 0
        for row in self.dxgtable.iterate_query_rows(genomic_query):
            if result_num == 0:
                self.assertEqual(4, row[0])
            elif result_num == 1:
                self.assertEqual(5, row[0])
            elif result_num == 2:
                self.assertEqual(8, row[0])
            result_num += 1
        self.assertEqual(3, result_num)

    def test_lexicographic(self):
        lex_index = dxpy.DXGTable.lexicographic_index([
                dxpy.DXGTable.lexicographic_index_column("a", case_sensitive=False),
                dxpy.DXGTable.lexicographic_index_column("b", ascending=False)
                ], "search")
        self.dxgtable = dxpy.new_dxgtable([dxpy.DXGTable.make_column_desc("a", "string"),
                                           dxpy.DXGTable.make_column_desc("b", "int32")],
                                          indices=[lex_index])
        self.dxgtable.close(block=True)
        desc = self.dxgtable.describe()
        self.assertEqual({"name": "search",
                          "type": "lexicographic",
                          "columns": [{"name": "a", "order": "asc", "caseSensitive": False},
                                       {"name": "b", "order": "desc"}]},
                         desc['indices'][0])

    # TODO: Test with > 1 index

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
                     runSpec={"code": '''
@dxpy.entry_point('main')
def main(number):
    raise # Ensure that the applet fails
''',
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
        dxanalysis = dxworkflow.run({})
        time.sleep(2)
        dxjob = dxpy.DXJob(dxanalysis.describe()['stages'][0]['execution']['id'])
        self.assertEqual(dxjob.describe()['instanceType'], self.default_inst_type)

        # request for all stages and all entry points
        dxanalysis = dxworkflow.run({}, instance_type="mem2_hdd2_x1")
        time.sleep(2)
        dxjob = dxpy.DXJob(dxanalysis.describe()['stages'][0]['execution']['id'])
        self.assertEqual(dxjob.describe()['instanceType'], 'mem2_hdd2_x1')

        # request for all stages, overriding some entry points
        dxanalysis = dxworkflow.run({}, instance_type={"*": "mem2_hdd2_x1", "foo": "mem2_hdd2_x2"})
        time.sleep(2)
        dxjob = dxpy.DXJob(dxanalysis.describe()['stages'][0]['execution']['id'])
        self.assertEqual(dxjob.describe()['instanceType'], 'mem2_hdd2_x1')

        # request for the stage specifically, for all entry points
        dxanalysis = dxworkflow.run({}, stage_instance_types={stage_id: "mem2_hdd2_x2"})
        time.sleep(2)
        dxjob = dxpy.DXJob(dxanalysis.describe()['stages'][0]['execution']['id'])
        self.assertEqual(dxjob.describe()['instanceType'], 'mem2_hdd2_x2')

        # request for the stage specifically, overriding some entry points
        dxanalysis = dxworkflow.run({}, stage_instance_types={stage_id: {"*": "mem2_hdd2_x2", "foo": "mem2_hdd2_x1"}})
        time.sleep(2)
        dxjob = dxpy.DXJob(dxanalysis.describe()['stages'][0]['execution']['id'])
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

        time.sleep(2) # allow time for jobs to be created so we can inspect their metadata

        # make assertions
        desc = control_dxanalysis.describe()
        self.assertEqual(desc['stages'][0]['execution']['folder'], '/output/foo')
        self.assertEqual(desc['stages'][1]['execution']['folder'], '/myoutput')
        desc = override_folders_dxanalysis.describe()
        self.assertEqual(desc['stages'][0]['execution']['folder'], '/foo')
        self.assertEqual(desc['stages'][1]['execution']['folder'], '/output/bar')
        desc = use_default_folder_dxanalysis.describe()
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
                     runSpec={"code": '''
@dxpy.entry_point('main')
def main(number):
    raise # Ensure that the applet fails
''',
                               "interpreter": "python2.7"})
        dxworkflow.add_stage(dxapplet, name='stagename')

        # Can't specify the same input more than once (with a
        # stage-specific syntax)
        self.assertRaisesRegexp(DXError, 'more than once',
                                dxworkflow.run, {"0.number": 32, "stagename.number": 42})
        # Bad stage name
        self.assertRaisesRegexp(DXError, 'nor found as a stage name',
                                dxworkflow.run, {"nonexistentstage.number": 32})

    def test_new_dxworkflow(self):
        blankworkflow = dxpy.new_dxworkflow()
        self.assertIsInstance(blankworkflow, dxpy.DXWorkflow)
        desc = blankworkflow.describe()
        self.assertEqual(desc['title'], blankworkflow.get_id())
        self.assertEqual(desc['summary'], '')
        self.assertEqual(desc['description'], '')
        self.assertEqual(desc['outputFolder'], None)

        dxworkflow = dxpy.new_dxworkflow(title='mytitle', summary='mysummary', description='mydescription', output_folder="/foo")
        self.assertIsInstance(dxworkflow, dxpy.DXWorkflow)
        desc = dxworkflow.describe()
        self.assertEqual(desc['title'], 'mytitle')
        self.assertEqual(desc['summary'], 'mysummary')
        self.assertEqual(desc['description'], 'mydescription')
        self.assertEqual(desc['outputFolder'], '/foo')

        secondworkflow = dxpy.new_dxworkflow(init_from=dxworkflow)
        self.assertIsInstance(secondworkflow, dxpy.DXWorkflow)
        self.assertNotEqual(dxworkflow.get_id(), secondworkflow.get_id())
        desc = secondworkflow.describe()
        self.assertEqual(desc['title'], 'mytitle')
        self.assertEqual(desc['summary'], 'mysummary')
        self.assertEqual(desc['description'], 'mydescription')
        self.assertEqual(desc['outputFolder'], '/foo')

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
        with self.assertRaises(DXAPIError):
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

@unittest.skipUnless(testutil.TEST_CREATE_APPS,
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
        new_proj_id = dxpy.api.project_new({'name': 'test project 1'})['id']
        dxrecord0 = dxpy.new_dxrecord(name="myrecord0", project=self.proj_id)
        dxrecord1 = dxpy.new_dxrecord(name="myrecord1", project=new_proj_id)
        dxrecord2 = dxpy.new_dxrecord(name="myrecord2", project=self.proj_id)
        dxrecord2 = dxpy.new_dxrecord(name="myrecord3", project=new_proj_id)
        records = [{"name": "myrecord0", "project": self.proj_id, "folder": "/"},
                   {"name": "myrecord1"},
                   {"name": "myrecord2"},
                   {"name": "myrecord3", "project": self.proj_id, "folder": "/"}]

        objects = list(dxpy.search.resolve_data_objects(records, project=new_proj_id))
        self.assertEqual(len(objects), 4)
        self.assertEqual(objects[0][0]["project"], self.proj_id)
        self.assertEqual(objects[0][0]["id"], dxrecord0.get_id())
        self.assertEqual(objects[1][0]["project"], new_proj_id)
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
        found_proj = False;
        for result in results:
            if result["id"] == dxproject.get_id():
                self.assertEqual(result["level"], 'ADMINISTER')
                found_proj = True
            self.assertFalse('describe' in result)
        self.assertTrue(found_proj)

        results = list(dxpy.find_projects(level='VIEW', describe=True))
        found_proj = False;
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
        dxpy.DXHTTPRequest('/system/comeBackLater', {'waitUntil': server_time + 20000, 'setRetryAfter': False})
        end_time = int(time.time() * 1000)
        time_elapsed = end_time - start_time
        self.assertTrue(50000 <= time_elapsed)
        self.assertTrue(time_elapsed <= 70000)

    def test_generic_exception_not_retryable(self):
        self.assertFalse(dxpy._is_retryable_exception(KeyError('oops')))

    def test_bad_host(self):
        # Verify that the exception raised is one that dxpy would
        # consider to be retryable, but truncate the actual retry loop
        with self.assertRaises(requests.exceptions.ConnectionError) as exception_cm:
            dxpy.DXHTTPRequest('http://doesnotresolve.dnanexus.com/', {}, prepend_srv=False, always_retry=False,
                               max_retries=1)
        self.assertTrue(dxpy._is_retryable_exception(exception_cm.exception))

    def test_connection_refused(self):
        # Verify that the exception raised is one that dxpy would
        # consider to be retryable, but truncate the actual retry loop
        with self.assertRaises(requests.exceptions.ConnectionError) as exception_cm:
            # Connecting to a port on which there is no server running
            dxpy.DXHTTPRequest('http://localhost:20406', {}, prepend_srv=False, always_retry=False, max_retries=1)
        self.assertTrue(dxpy._is_retryable_exception(exception_cm.exception))


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
        dxproject = dxpy.get_handler(self.proj_id)

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

class TestResolver(unittest.TestCase):
    def setUp(self):
        setUpTempProjects(self)

    def tearDown(self):
        tearDownTempProjects(self)

    def test_basic_ops(self):
        from dxpy.utils.resolver import resolve_existing_path, ResolutionError
        resolve_existing_path('')
        with self.assertRaises(ResolutionError):
            resolve_existing_path('', allow_empty_string=False)
        proj_id, path, entity_id = resolve_existing_path(':')
        self.assertEqual(proj_id, dxpy.WORKSPACE_ID)

if __name__ == '__main__':
    if dxpy.AUTH_HELPER is None:
        sys.exit(1, 'Error: Need to be logged in to run these tests')
    if 'DXTEST_FULL' not in os.environ:
        if 'DXTEST_CREATE_APPS' not in os.environ:
            sys.stderr.write('WARNING: neither env var DXTEST_FULL nor DXTEST_CREATE_APPS are set; tests that create apps will not be run\n')
        if 'DXTEST_RUN_JOBS' not in os.environ:
            sys.stderr.write('WARNING: neither env var DXTEST_FULL nor DXTEST_RUN_JOBS are set; tests that run jobs will not be run\n')
    unittest.main()
