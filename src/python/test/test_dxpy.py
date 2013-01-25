#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 DNAnexus, Inc.
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

import os, unittest, tempfile, filecmp

import dxpy
from dxpy.exceptions import *

from dxpy.utils import pretty_print

# Store the following in PROJECT_CONTEXT_ID to make some of the tests pass
proj_id = "project-000000000000000000000001"
second_proj_id = 'project-000000000000000000000002'

def get_objects_from_listf(listf):
    objects = []
    for result in listf["objects"]:
        objects.append(result["id"])
    return objects

def remove_all(proj_id, folder="/"):
    dxproject = dxpy.DXProject(proj_id)
    dxproject.remove_folder(folder, recurse=True)

class TestDXProject(unittest.TestCase):
    def tearDown(self):
        remove_all(proj_id)
        remove_all(second_proj_id)

    def test_update_describe(self):
        dxproject = dxpy.DXProject()
        dxproject.update(name="newprojname", protected=True, restricted=True, description="new description")
        desc = dxproject.describe()
        self.assertEqual(desc["id"], proj_id)
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
            dxproject.clone(second_proj_id,
                            destination="/",
                            objects=[dxrecords[0].get_id(), dxrecords[1].get_id()],
                            folders=["/a/b/c/d"])

        dxrecords[0].close()
        dxrecords[1].close()
        dxproject.clone(second_proj_id,
                        destination="/",
                        objects=[dxrecords[0].get_id(), dxrecords[1].get_id()],
                        folders=["/a/b/c/d"])

        second_proj = dxpy.DXProject(second_proj_id)
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
        self.new_file = tempfile.NamedTemporaryFile(delete=False)
        self.new_file.close()

        self.dxfile = dxpy.DXFile()

    def tearDown(self):
        os.remove(self.new_file.name)

        try:
            self.dxfile.destroy()
        except:
            pass

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

class TestDXGTable(unittest.TestCase):
    """
    TODO: Test iterators, gri, and other queries
    """
    def setUp(self):
        self.dxgtable = None

    def tearDown(self):
        if self.dxgtable:
            try:
                state = self.dxgtable._get_state()
                if state == 'closing':
                    self.dxgtable._wait_on_close()
                self.dxgtable.remove()
            except:
                pass

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
        self.dxgtable.add_rows(data=[[u"303", 1.248, 123, True]], part=5)
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
             dxpy.DXGTable.make_column_desc("b", "int32")], mode='w') as self.dxgtable:
            for i in range(64):
                self.dxgtable.add_rows(data=[["row"+str(i), i]], part=i+1)

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
        dxgtable = dxpy.new_dxgtable([dxpy.DXGTable.make_column_desc("a", "string"),
                                      dxpy.DXGTable.make_column_desc("b", "int32")])
        for i in range(64):
            dxgtable.add_rows(data=[["row"+str(i), i]])
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
                                       dxpy.DXGTable.make_column_desc("b", "int32")])
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

        dxgtable = dxpy.new_dxgtable(columns, indices=[genomic_index])
        desc = dxgtable.describe()
        self.assertEqual(desc["indices"], [genomic_index]);

        dxgtable.add_rows(data10[:3], 1)
        dxgtable.add_rows(data10[3:6], 10)
        dxgtable.add_rows(data10[6:9], 100)
        dxgtable.add_rows(data10[9:], 1000)

        dxgtable.close(True)

        desc = dxgtable.describe()
        self.assertEqual(desc["length"], 10)

        # Offset + limit queries
        result = dxgtable.get_rows(starting=0, limit=1);
        self.assertEqual(result["data"],
                         [[0, 'chr1',  0,  3, 'a']]);
        self.assertEqual(result["next"], 1);
        self.assertEqual(result["length"], 1);

        result = dxgtable.get_rows(starting=4, limit=3);
        self.assertEqual(result["data"],
                         [[4, 'chr1', 15, 23, 'e'],
                          [5, 'chr1', 16, 21, 'f'],
                          [6, 'chr1', 17, 19, 'g']]);
        self.assertEqual(result["next"], 7);
        self.assertEqual(result["length"], 3);

        # Range query
        genomic_query = dxpy.DXGTable.genomic_range_query('chr1', 22, 25)
        result = dxgtable.get_rows(query=genomic_query)
        self.assertEqual(result["data"],
                         [[4, 'chr1', 15, 23, 'e']]);
        self.assertEqual(result["next"], None);
        self.assertEqual(result["length"], 1);

        # Range query with nonconsecutive rows in result
        genomic_query = dxpy.DXGTable.genomic_range_query('chr1', 20, 26)
        result = dxgtable.get_rows(query=genomic_query)
        self.assertEqual(result["data"],
                   [[4, 'chr1', 15, 23, 'e'],
                    [5, 'chr1', 16, 21, 'f'],
                    [8, 'chr1', 25, 30, 'i']]);
        self.assertEqual(result["next"], None);
        self.assertEqual(result["length"], 3);

        # Testing iterate_rows
        row_num = 5
        for row in dxgtable.iterate_rows(5, 8):
            self.assertEqual(row_num, row[0])
            row_num += 1
        self.assertEqual(row_num, 8)

        # Testing iterate_query_rows
        genomic_query = dxpy.DXGTable.genomic_range_query('chr1', 20, 26)
        result_num = 0
        for row in dxgtable.iterate_query_rows(genomic_query):
            if result_num == 0:
                self.assertEqual(4, row[0])
            elif result_num == 1:
                self.assertEqual(5, row[0])
            elif result_num == 2:
                self.assertEqual(8, row[0])
            result_num += 1
        self.assertEqual(3, result_num)

    # TODO: Test with > 1 index

class TestDXRecord(unittest.TestCase):
    """
    Most of these tests really are testing DXDataObject methods
    while using DXRecords as the most basic data object.
    """

    def tearDown(self):
        remove_all(proj_id)
        remove_all(second_proj_id)

    def test_set_id(self):
        dxrecord = dxpy.new_dxrecord()
        second_dxrecord = dxpy.DXRecord()
        second_dxrecord.set_ids(dxrecord.get_id(), dxrecord.get_proj_id())
        self.assertEqual(second_dxrecord.get_id(), dxrecord.get_id())
        self.assertEqual(second_dxrecord.get_proj_id(), proj_id)
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
        self.assertEqual(firstDXRecord.get_proj_id(), proj_id)
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
        secondDXRecord.new(project=second_proj_id, details=["bar"])
        self.assertNotEqual(firstDXRecord.get_id(), secondDXRecord.get_id())
        # test if secondDXRecord._dxid has been set to a valid ID
        try:
            self.assertRegexpMatches(secondDXRecord.get_id(), "^record-[0-9A-Za-z]{24}",
                                     'Object ID not of expected form: ' + \
                                         secondDXRecord.get_id())
        except AttributeError:
            self.fail("dxID was not stored in DXRecord creation")
        # test if secondDXRecord._proj has been set to second_proj_id
        self.assertEqual(secondDXRecord.get_proj_id(), second_proj_id)
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

        with self.assertRaises(AttributeError):
            firstDXRecord.get_id()

        try:
            secondDXRecord.remove()
        except DXError as error:
            self.fail("Unexpected error when removing record object: " +
                      str(error))

        with self.assertRaises(AttributeError):
            secondDXRecord.get_id()

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
        self.assertEqual(desc["project"], proj_id)
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
        self.assertEqual(desc["project"], proj_id)
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
        second_dxrecord = dxrecord.clone(second_proj_id)
        self.assertTrue(proj_id in dxrecord.list_projects())
        self.assertTrue(second_proj_id in dxrecord.list_projects())

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
            second_dxrecord = dxrecord.clone(second_proj_id)
        dxrecord.close()

        second_dxrecord = dxrecord.clone(second_proj_id)
        second_dxrecord.rename("newname")

        first_desc = dxrecord.describe()
        second_desc = second_dxrecord.describe()

        self.assertEqual(first_desc["id"], dxrecord.get_id())
        self.assertEqual(second_desc["id"], dxrecord.get_id())
        self.assertEqual(first_desc["project"], proj_id)
        self.assertEqual(second_desc["project"], second_proj_id)
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

    def test_passhtrough_args(self):
        dxrecord = dxpy.new_dxrecord(auth=dxpy.AUTH_HELPER)
        with self.assertRaises(TypeError):
            dxrecord = dxpy.new_dxrecord(foo=1)

@unittest.skip("Skipping tables; not yet implemented")
class TestDXTable(unittest.TestCase):
    pass

class TestDXAppletJob(unittest.TestCase):
    def test_run_dxapplet(self):
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
        dxrecord = dxpy.new_dxrecord()
        dxrecord.close()
        prog_input = {"chromosomes": {"$dnanexus_link": dxrecord.get_id()},
                      "rowFetchChunk": 100}
        dxjob = dxapplet.run(applet_input=prog_input)
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
        dxjob.terminate()

class TestDXApp(unittest.TestCase):
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
        dxapp.new(applet=dxapplet.get_id(), version="0.0.1",
                  bill_to="user-000000000000000000000000", name="app_name")
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

@unittest.skip("Skipping jobs and apps; running Python apps not yet supported")
class TestDXJob(unittest.TestCase):
    def test_job_from_app(self):
        test_json = dxpy.new_dxrecord({"details": {"jobsuccess": False} })
        job_id_json = dxpy.new_dxrecord({"details": {"jobid": None} })
        dxapplet = dxpy.new_dxapplet(codefile='test_dxjob.py')
        dxappletjob = dxapplet.run({"json_dxid": test_json.get_id(),
                                      "job_id_json": job_id_json.get_id()})
        dxappletjob.wait_on_done()

        dxjob_id = job_id_json.get_details()["jobid"]
        self.assertIsNotNone(dxjob_id)
        dxjob = dxpy.DXJob(dxjob_id)
        dxjob.wait_on_done()

        self.assertEqual(test_json.get_details(), {"jobsuccess":True})

        test_json.remove()
        dxapplet.remove()

class TestDXSearch(unittest.TestCase):
    def find_data_objs(self):
        dxrecord = dxpy.new_dxrecord()
        results = list(dxpy.search.find_data_objects(state="open"))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], {"project": proj_id,
                                      "id": dxrecord.get_id()})
        results = list(dxpy.search.find_data_objects(state="closed"))
        self.assertEqual(len(results), 0)
        dxrecord.close()
        results = list(dxpy.search.find_data_objects(state="closed"))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], {"project": proj_id,
                                      "id": dxrecord.get_id()})

    def find_projects(self):
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
            if result["id"] == 'project-0000000000000000000000pb':
                self.assertEqual(result["level"], 'ADMINISTER')
                found_proj = True
            self.assertTrue('describe' in result)
            self.assertEqual(result['describe']['name'], 'public-test-project')
        self.assertTrue(found_proj)

    def find_jobs(self):
        dxapplet = dxpy.DXApplet()
        dxapplet.new(name="test_applet",
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
        dxjob = dxapplet.run(applet_input=prog_input)
        results = list(dxpy.find_jobs(launched_by='user-000000000000000000000000',
                                      applet=dxapplet,
                                      project=dxapplet.get_proj_id(),
                                      origin_job=dxjob.get_id(),
                                      parent_job=None,
                                      modified_after=0,
                                      describe=True))
        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result["id"], dxjob.get_id())
        self.assertTrue("describe" in result)
        self.assertEqual(result["describe"]["id"], dxjob.get_id())
        self.assertEqual(result["describe"]["class"], "job")
        self.assertEqual(result["describe"]["applet"], dxapplet.get_id())
        self.assertEqual(result["describe"]["project"], dxapplet.get_proj_id())
        self.assertEqual(result["describe"]["originJob"], dxjob.get_id())
        self.assertEqual(result["describe"]["parentJob"], None)

class TestPrettyPrint(unittest.TestCase):
    def test_string_escaping(self):
        self.assertEqual(pretty_print.escape_unicode_string("a"), u"a")
        self.assertEqual(pretty_print.escape_unicode_string("foo\nbar"), u"foo\\nbar")
        self.assertEqual(pretty_print.escape_unicode_string("foo\x11bar"), u"foo\\x11bar")
        self.assertEqual(pretty_print.escape_unicode_string("foo\n\t\rbar"), u"foo\\n\\t\\rbar")
        self.assertEqual(pretty_print.escape_unicode_string("\n\\"), u"\\n\\\\")
        self.assertEqual(pretty_print.escape_unicode_string(u"ïñtérnaçiònale"), u"ïñtérnaçiònale")

if __name__ == '__main__':
    print "NOTE: This test requires environment variables to be set for DX_APISERVER_*, DX_SECURITY_CONTEXT, and a DX_PROJECT_CONTEXT_ID with which the security context has ADMINISTER access.  It should be run against a running API server and with a Mongo DB initialized with test entities such as the public test project project-0000000000000000000000pb."
    unittest.main()
