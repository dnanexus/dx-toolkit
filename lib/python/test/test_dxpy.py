#!/usr/bin/env python
import os, sys, unittest, json, tempfile, filecmp

import dxpy.bindings as dxpy
from dxpy.exceptions import *

# Store the following in PROJECT_ID to make some of the tests pass
proj_id = "project-000000000000000000000001"
second_proj_id = 'project-000000000000000000000002';

@unittest.skip("Skipping search; not implemented yet in 1.03")
class TestSearch(unittest.TestCase):

    def test_search(self):
        json = dxpy.new_dxjson({"foo": "bar"})
        properties = {"testing": "dxpy"}
        json.set_properties(properties)

        types = ["foo", "othertype"]
        json.add_types(types)

        self.assertTrue(json.get_id() in dxpy.search(classname="json"))
        self.assertTrue(json.get_id() in dxpy.search(classname="json",
                                                     properties=properties,
                                                     typename="foo"))
        self.assertFalse(json.get_id() in dxpy.search(classname="table",
                                                      properties=properties,
                                                      typename="foo"))
        self.assertFalse(json.get_id() in dxpy.search(typename="bar"))
        self.assertFalse(json.get_id() in
                         dxpy.search(properties={"testing": "notjson"}))

        json.destroy()

        # Testing on >1000 expected results (uses more than one fetch)

        jsons = []
        json_ids = []
        for i in range(1200):
            a_json = dxpy.new_dxjson(["foo", "bar", 3])
            a_json.set_properties(properties)
            jsons.append(a_json)
            json_ids.append(a_json.get_id())

        count = 0
        for result in dxpy.search(classname="json", properties=properties):
            if result in json_ids:
                json_ids.remove(result)
                count += 1
        # Make sure we found the correct number of objects
        self.assertEqual(count, len(jsons))
        # Make sure each JSON object was found
        self.assertEqual(len(json_ids), 0)

        # Cleanup
        for json in jsons:
            json.destroy()

class TestDXProject(unittest.TestCase):
    def test_update_describe(self):
        dxproject = dxpy.DXProject(proj_id)
        dxproject.update(name="newprojname", protected=True, restricted=True, description="new description")
        desc = dxproject.describe()
        self.assertEqual(desc["id"], proj_id)
        self.assertEqual(desc["class"], "project")
        self.assertEqual(desc["name"], "newprojname")
        self.assertEqual(desc["protected"], True)
        self.assertEqual(desc["restricted"], True)
        self.assertEqual(desc["description"], "new description")
        self.assertTrue("created" in desc)

    def test_folders(self):
        dxproject = dxpy.DXProject(proj_id)
        listf = dxproject.list_folder()
        self.assertEqual(listf["folders"], [])
        self.assertEqual(listf["objects"], [])

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

    # TODO: Test destruction once implemented

    def test_set_id(self):
        dxrecord = dxpy.new_dxrecord()
        second_dxrecord = dxpy.DXRecord()
        second_dxrecord.set_ids(dxrecord.get_id(), dxrecord.get_proj_id())
        self.assertEqual(second_dxrecord.get_id(), dxrecord.get_id())
        self.assertEqual(second_dxrecord.get_proj_id(), proj_id)
        dxrecord.remove()

    def test_create_destroy_dxrecord(self):
        '''Create a fresh DXRecord object and check that its ID is
        stored and that the record object has been stored.
        '''

        firstDXRecord = dxpy.new_dxrecord(details=["foo"])
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

        # FIXME when implemented
        # thirdJSON = dxpy.DXRecord(firstID)

        # with self.assertRaises(DXAPIError) as cm:
        #     thirdJSON.describe()
        #     self.assertEqual(cm.exception.name, "ResourceNotFound")

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

        dxproject = dxpy.DXProject(proj_id)
        dxproject.move(objects=[desc["id"]], destination="/")
        dxproject.remove_folder("/a")
        dxrecord.remove()
        second_dxrecord.remove()

    def test_set_properties_of_dxrecord(self):
        dxrecord = dxpy.new_dxrecord()
        properties = {"project": "cancer project", "foo": "bar"}
        dxrecord.set_properties(properties)
        desc = dxrecord.describe(True)
        self.assertEqual(desc["properties"], properties)

        dxrecord.set_properties({"project": None})
        self.assertEqual(dxrecord.describe(True)["properties"], {"foo": "bar"})

        dxrecord.remove()

    def test_types_of_dxrecord(self):
        dxrecord = dxpy.new_dxrecord()
        types = ["foo", "othertype"]
        dxrecord.add_types(types)
        self.assertEqual(dxrecord.describe()["types"], types)

        dxrecord.remove_types(["foo"])
        self.assertEqual(dxrecord.describe()["types"], ["othertype"])

        dxrecord.remove()

    def test_tags_of_dxrecord(self):
        dxrecord = dxpy.new_dxrecord()
        tags = ["foo", "othertag"]
        dxrecord.add_tags(tags)
        self.assertEqual(dxrecord.describe()["tags"], tags)

        dxrecord.remove_tags(["foo"])
        self.assertEqual(dxrecord.describe()["tags"], ["othertag"])

        dxrecord.remove()

    def test_visibility_of_dxrecord(self):
        dxrecord = dxpy.new_dxrecord()
        dxrecord.set_visibility(hidden=True)
        self.assertEqual(dxrecord.describe()["hidden"], True)

        dxrecord.set_visibility(hidden=False)
        self.assertEqual(dxrecord.describe()["hidden"], False)

        dxrecord.remove()

    def test_rename_dxrecord(self):
        dxrecord = dxpy.new_dxrecord()
        dxrecord.rename("newname")
        self.assertEqual(dxrecord.describe()["name"], "newname")

        dxrecord.rename("secondname")
        self.assertEqual(dxrecord.describe()["name"], "secondname")

        dxrecord.remove()

    # TODO: Test this more after cloning
    def test_list_projects_dxrecord(self):
        dxrecord = dxpy.new_dxrecord()
        self.assertTrue(proj_id in dxrecord.list_projects())

        dxrecord.remove()

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

        dxrecord.remove()

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

        dxrecord.remove()

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
