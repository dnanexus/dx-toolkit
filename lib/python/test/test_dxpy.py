#!/usr/bin/env python
import os, sys, unittest, json, tempfile, filecmp

import dxpy.bindings as dxpy
from dxpy.exceptions import *

# Store the following in PROJECT_ID to make some of the tests pass
proj_id = "project-000000000000000000000001"
second_proj_id = 'project-000000000000000000000002';

def remove_all(proj_id, folder="/"):
    dxproject = dxpy.DXProject(proj_id)
    listf = dxproject.list_folder(folder)
    dxproject.remove_objects(listf["objects"])
    for subfolder in listf["folders"]:
        remove_all(proj_id, subfolder)
        dxproject.remove_folder(subfolder)

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

    def test_new_list_remove_folders(self):
        dxproject = dxpy.DXProject()
        listf = dxproject.list_folder()
        self.assertEqual(listf["folders"], [])
        self.assertEqual(listf["objects"], [])

        dxrecord = dxpy.new_dxrecord()
        dxproject.new_folder("/a/b/c/d", parents=True)
        listf = dxproject.list_folder()
        self.assertEqual(listf["folders"], ["/a"])
        self.assertEqual(listf["objects"], [dxrecord.get_id()])
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
        self.assertEqual(listf["objects"], [dxrecords[2].get_id(),
                                            dxrecords[3].get_id()])
        self.assertEqual(listf["folders"], ["/a"])

        listf = dxproject.list_folder("/a")
        self.assertEqual(listf["objects"], [dxrecords[0].get_id(),
                                            dxrecords[1].get_id()])
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
        self.assertEqual(listf["objects"], [dxrecords[0].get_id(),
                                            dxrecords[1].get_id()])
        self.assertEqual(listf["folders"], ["/d"])

    def test_remove_objects(self):
        dxproject = dxpy.DXProject()
        dxrecord = dxpy.new_dxrecord()
        listf = dxproject.list_folder()
        self.assertEqual(listf["objects"], [dxrecord.get_id()])
        dxproject.remove_objects([dxrecord.get_id()])
        listf = dxproject.list_folder()
        self.assertEqual(listf["objects"], [])
        with self.assertRaises(DXAPIError):
            dxrecord.describe()

@unittest.skip("Skipping files; not updated yet for 1.03")
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

        self.assertEqual(self.dxfile.get_properties()["name"],
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

@unittest.skip("Skipping gtables; not yet implemented")
class TestDXGTable(unittest.TestCase):
    def setUp(self):
        self.dxtable = dxpy.DXTable(proj_id)

    def tearDown(self):
        try:
            self.dxtable.remove()
        except:
            pass

    def test_create_table(self):
        self.dxtable = dxpy.new_dxtable(proj_id, ['a:string', 'b:int32'])
        self.dxtable.close()
        desc = self.dxtable.describe()
        self.assertEqual(desc["columns"], ['a:string', 'b:int32'])

    def test_extend_table(self):
        table_to_extend = dxpy.new_dxtable(proj_id, ['a:string', 'b:int32'])
        try:
            table_to_extend.add_rows([["Row 1", 1], ["Row 2", 2]], 1)
            table_to_extend.close(block=True)
        except:
            self.fail("Error occurred when creating a table")
            table_to_extend.remove()

        try:
            self.dxtable = dxpy.extend_dxtable(table_to_extend.get_id(),
                                               proj_id,
                                               ['c:int32', 'd:string'])
        except:
            self.fail("Could not extend table");
        finally:
            table_to_extend.remove()

        self.assertEqual(self.dxtable.describe()["columns"],
                         ['a:string', 'b:int32', 'c:int32', 'd:string'])
        self.dxtable.add_rows([[10, "End row 1"], [20, "End row 2"]])
        try:
            self.dxtable.close()
        except DXAPIError:
            self.fail("Could not close table after table extension")
    
    def test_add_rows(self):
        self.dxtable = dxpy.new_dxtable(proj_id, ['a:string', 'b:int32'])
        self.dxtable.add_rows(data=[], index=9999)
        with self.assertRaises(DXAPIError):
            self.dxtable.add_rows(data=[[]], index=9997)

        for i in range(64):
            self.dxtable.add_rows(data=[["row"+str(i), i]], index=i+1)
        self.dxtable.close()

        with self.assertRaises(DXAPIError):
            self.dxtable.close()

    def test_add_rows_no_index(self):
        self.dxtable = dxpy.new_dxtable(proj_id, ['a:string', 'b:int32'])
        for i in range(64):
            self.dxtable.add_rows(data=[["row"+str(i), i]])

        self.dxtable.flush()
        desc = self.dxtable.describe()
        self.assertEqual(len(desc["parts"]), 1)

        self.dxtable.close(block=True)

        desc = self.dxtable.describe()
        self.assertEqual(desc["size"], 64)

    def test_table_context_manager(self):
        with dxpy.new_dxtable(proj_id, ['a:string', 'b:int32']) as self.dxtable:
            for i in range(64):
                self.dxtable.add_rows(data=[["row"+str(i), i]], index=i+1)

    def test_create_table_with_invalid_spec(self):
        with self.assertRaises(DXAPIError):
            dxpy.new_dxtable(proj_id, ['a:string', 'b:muffins'])

    def test_get_rows(self):
        self.dxtable = dxpy.new_dxtable(proj_id, ['a:string', 'b:int32'])
        for i in range(64):
            self.dxtable.add_rows(data=[["row"+str(i), i]], index=i+1)
        with self.assertRaises(DXAPIError):
            rows = self.dxtable.get_rows()
        self.dxtable.close(block=True)
        rows = self.dxtable.get_rows()['data']
        assert(len(rows) == 64)
        
        # TODO: test get_rows parameters, genomic range index when
        # implemented

    def test_iter_table(self):
        self.dxtable = dxpy.new_dxtable(['a:string', 'b:int32'])
        for i in range(64):
            self.dxtable.add_rows(data=[["row"+str(i), i]], index=i+1)
        self.dxtable.close(block=True)

        counter = 0
        for row in self.dxtable:
            self.assertEqual(row[2], counter)
            counter += 1
        self.assertEqual(counter, 64)

class TestDXRecord(unittest.TestCase):
    """
    Most of these tests really are testing DXDataObjClass methods
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
        but in the same project as the first.
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

        desc = dxrecord.describe(incl_properties=True)
        self.assertEqual(desc["properties"], {})

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
        desc = second_dxrecord.describe(True)
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
        dxrecord.set_visibility(hidden=True)
        self.assertEqual(dxrecord.describe()["hidden"], True)

        dxrecord.set_visibility(hidden=False)
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
            dxrecord.set_visibility(True)
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
        self.assertEqual(listf["objects"], [dxrecord.get_id()])
        desc = dxrecord.describe()
        self.assertEqual(desc["folder"], "/a/b/c")

@unittest.skip("Skipping tables; not yet implemented")
class TestDXTable(unittest.TestCase):
    pass

@unittest.skip("Skipping jobs and apps; running Python apps not yet supported")
class TestDXApp(unittest.TestCase):
    def test_create_dxapp(self):
        test_json = dxpy.new_dxrecord({"appsuccess": False})
        dxapp = dxpy.new_dxapp(codefile='test_dxapp.py')
        dxappjob = dxapp.run({"json_dxid": test_json.get_id()})
        dxappjob.wait_on_done()
        self.assertEqual(test_json.get(), {"appsuccess":True})
        test_json.destroy()
        dxapp.destroy()

@unittest.skip("Skipping jobs and apps; running Python apps not yet supported")
class TestDXJob(unittest.TestCase):
    def test_job_from_app(self):
        test_json = dxpy.new_dxrecord({"jobsuccess": False})
        job_id_json = dxpy.new_dxrecord({"jobid": None})
        dxapp = dxpy.new_dxapp(codefile='test_dxjob.py')
        dxappjob = dxapp.run({"json_dxid": test_json.get_id(),
                              "job_id_json": job_id_json.get_id()})
        dxappjob.wait_on_done()

        dxjob_id = job_id_json.get()["jobid"]
        self.assertIsNotNone(dxjob_id)
        dxjob = dxpy.DXJob(dxjob_id)
        dxjob.wait_on_done()

        self.assertEqual(test_json.get(), {"jobsuccess":True})

        test_json.destroy()
        dxapp.destroy()

if __name__ == '__main__':
    unittest.main()
