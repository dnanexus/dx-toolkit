from __future__ import print_function, unicode_literals, division, absolute_import
from uuid import uuid4
import random
import string
import unittest

import dxpy_testutil as testutil

import dxpy
from dxpy.sugar import objects

TAG = "sugar-test"

def random_name(name_len, prefix=None):
    if prefix:
        name_len -= len(prefix)
    name = "".join(
        random.choice(string.ascii_letters)
        for _ in range(name_len)
    )
    if prefix:
        name = "{}{}".format(prefix, name)
    return name


class TestProjectSearchSugar(unittest.TestCase):

    def test_get_project_by_id(self):
        with testutil.temporary_project(name="[dxpy sugar tests] Temp project") as temp_projdx:
            project_id = temp_projdx.get_id()
            proj = objects.get_project(project_id)
            self.assertIsInstance(proj, dxpy.DXProject)
            self.assertEqual(proj.get_id(), project_id)

    def test_get_project_by_name(self):
        project_name = random_name(15, prefix="[dxpy sugar tests] ")
        with testutil.temporary_project(name=project_name) as temp_projdx:
            proj = objects.get_project(project_name)
            self.assertIsInstance(proj, dxpy.DXProject)
            self.assertEqual(proj.get_id(), temp_projdx.get_id())

    def test_fail_to_find_project(self):
        with self.assertRaises(dxpy.DXSearchError):
            objects.get_project(random_name(20), exists=True, region="azure:westus")

    def test_create_project_on_search_fail(self):
        unq_project_name = "[dxpy sugar tests]-{}".format(uuid4())
        new_proj = None
        try:
            new_proj = objects.get_project(unq_project_name, exists=False, create=True)
            self.assertIsInstance(new_proj, dxpy.DXProject)
            self.assertEqual(new_proj.describe()["name"], unq_project_name)
        finally:
            if new_proj:
                new_proj.destroy()

    def test_create_project_in_region(self):
        unq_project_name = "[dxpy sugar tests]-{}".format(uuid4())
        new_proj = None
        try:
            new_proj = objects.get_project(
                unq_project_name, create=True, region="azure:westus"
            )
            self.assertIsInstance(new_proj, dxpy.DXProject)
            self.assertEqual(new_proj.describe()["name"], unq_project_name)
            self.assertEqual(new_proj.describe()["region"], "azure:westus")
        finally:
            if new_proj:
                new_proj.destroy()


class TestEnsureFolderSugar(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_projectdx = dxpy.DXProject(dxpy.api.project_new(
            {"name": "[dxpy sugar tests] Test Ensure folder"}
        )['id'])

    @classmethod
    def tearDownClass(cls):
        cls.temp_projectdx.destroy(input_params={"terminateJobs": True})

    def test_ensure_folder_fail_search(self):
        folder_name = random_name(name_len=20, prefix="/")
        with self.assertRaises(dxpy.DXSearchError):
            objects.ensure_folder(folder_name, self.temp_projectdx, exists=True)

    def test_ensure_folder_creates_folder(self):
        folder_name = random_name(name_len=20, prefix="/")
        objects.ensure_folder(folder_name, self.temp_projectdx, exists=False, create=True)
        ls = objects.ensure_folder(folder_name, self.temp_projectdx, exists=True)
        self.assertIsNotNone(ls)
        self.assertEqual(len(ls["folders"]), 0)
        self.assertEqual(len(ls["objects"]), 0)

class TestDataObjectSugar(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_project = dxpy.DXProject(dxpy.api.project_new(
            {"name": "[dxpy sugar tests] Test Data Object Sugar"}
        )['id'])

    @classmethod
    def tearDownClass(cls):
        cls.temp_project.destroy(input_params={"terminateJobs": True})

    def test_get_file_search_fail(self):
        file_name = random_name(name_len=20, prefix="[Doesn't exist] ")
        with self.assertRaises(dxpy.DXSearchError):
            objects.get_data_object(
                file_name, classname="file",
                project=self.temp_project,
                exists=True
            )

    def test_get_file_successfully(self):
        dxfile = dxpy.upload_string(
            "test",
            project=self.temp_project.get_id(),
            name=random_name(name_len=20),
            wait_on_close=True
        )
        found_dxfile = objects.get_data_object(
            file_name, exists=True, project=self.temp_project.get_id())
        self.assertEqual(
            dxfile.get_id(), found_dxfile.get_id()
        )

    def test_data_object_search_fail_incorrect_classname(self):
        file_name = random_name(name_len=20)
        dxfile = dxpy.upload_string(
            "test",
            project=self.temp_project.get_id(),
            name=file_name,
            wait_on_close=True
        )

        with self.assertRaises(dxpy.DXSearchError):
            objects.get_data_object(
                file_name,
                project=self.temp_project.get_id(),
                classname="record",
                exists=True
            )

    def test_data_object_false_exist_search_fail(self):
        file_name = random_name(name_len=20)
        dxfile = dxpy.upload_string(
            "test",
            project=self.temp_project.get_id(),
            name=file_name,
            wait_on_close=True
        )

        with self.assertRaises(dxpy.DXSearchError):
            objects.get_data_object(
                file_name,
                project=self.temp_project.get_id(),
                exists=False
            )

"""
@unittest.skipUnless(
    testutil.TEST_ENV, "skipping test that would clobber your local environment"
)
class TestExecutableObjectSugar(DXTestCaseBuildWorkflows):

    def test_get_app(self):
        self.assertIsInstance(
            objects.get_app(APP_ID),
            dxpy.DXApp
        )
        self.assertEqual(
            objects.get_app(APP_NAME).get_id(),
            APP_ID
        )

    def test_get_workflow(self):
        proj = objects.get_project(PROJECT_ID)
        self.assertIsInstance(
            objects.get_data_object(WORKFLOW_ID, proj, classname="workflow"),
            dxpy.DXWorkflow
        )
        with self.assertRaises(dxpy.DXSearchError):
            # Should be multiple workflows with the same name
            objects.get_data_object(
                WORKFLOW_NAME, proj, classname="workflow", exists=True
            )
        workflow = objects.get_data_object(
            WORKFLOW_NAME, proj, classname="workflow", tag=TAG
        )
        self.assertIsNotNone(workflow)
        self.assertEqual(workflow.get_id(), WORKFLOW_ID)
        self.assertEqual(
            objects.get_data_object(
                WORKFLOW_NAME, proj, classname="workflow", folder=WORKFLOW_FOLDER
            ).get_id(),
            WORKFLOW_ID
        )
        workflow_path = "{}/{}".format(WORKFLOW_FOLDER, WORKFLOW_NAME)
        self.assertEqual(
            objects.get_data_object(workflow_path, proj, classname="workflow").get_id(),
            WORKFLOW_ID
        )


    def test_get_applet(self):
        proj = objects.get_project(PROJECT_ID)
        self.assertIsInstance(
            objects.get_data_object(APPLET_ID, proj, classname="applet"),
            dxpy.DXApplet
        )
        with self.assertRaises(dxpy.DXSearchError):
            # Should be multiple workflows with the same name
            objects.get_data_object(
                APPLET_NAME, proj, classname="workflow", exists=True
            )
        self.assertEqual(
            objects.get_data_object(
                APPLET_NAME, proj, classname="applet", tag=TAG
            ).get_id(),
            APPLET_ID
        )
        self.assertEqual(
            objects.get_data_object(
                APPLET_NAME, proj, classname="applet", folder=APPLET_FOLDER
            ).get_id(),
            APPLET_ID
        )
        workflow_path = "{}/{}".format(APPLET_FOLDER, APPLET_NAME)
        self.assertEqual(
            objects.get_data_object(workflow_path, proj, classname="applet").get_id(),
            APPLET_ID
        )
"""
