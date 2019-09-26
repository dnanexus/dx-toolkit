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
        file_name = random_name(name_len=20)
        dxfile = dxpy.upload_string(
            "test",
            project=self.temp_project.get_id(),
            name=file_name,
            wait_on_close=True
        )
        found_dxfile = objects.get_data_object(
            file_name, exists=True, project=self.temp_project.get_id())
        self.assertEqual(
            dxfile.get_id(), found_dxfile.get_id()
        )

    def test_data_object_search_fail_incorrect_classname(self):
        file_name = random_name(name_len=20)
        _ = dxpy.upload_string(
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
        _ = dxpy.upload_string(
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


@unittest.skipUnless(
    testutil.TEST_ENV, "skipping test that would clobber your local environment"
)
class TestExecutableObjectSugar(testutil.DXTestCaseBuildWorkflows):

    def test_get_workflow(self):
        dxworkflow = self.create_workflow(self.project)
        self.assertIsInstance(
            objects.get_data_object(
                dxworkflow.get_id(),
                project=self.project,
                classname="workflow"
            ),
            dxpy.DXWorkflow
        )

    def test_fail_to_find_workflow(self):
        with self.assertRaises(dxpy.DXSearchError):
            objects.get_data_object(
                data_obj_desc=random_name(20),
                project=self.project,
                classname="workflow",
                exists=True
            )
    
    def test_get_workflow_by_name(self):
        workflow_name = random_name(15)
        workflow_spec = self.create_workflow_spec(self.project, workflow_name=workflow_name)
        dxworkflow = self.create_workflow(self.project, workflow_spec=workflow_spec)
        workflow = objects.get_data_object(
            workflow_name, project=self.project, classname="workflow"
        )
        self.assertIsNotNone(workflow)
        self.assertEqual(workflow.get_id(), dxworkflow.get_id())

    def test_get_workflow_failed_multiple_matches(self):
        # Create 2 workflows with the same name
        workflow_name = random_name(15)
        workflow_spec = self.create_workflow_spec(self.project, workflow_name=workflow_name)
        _ = self.create_workflow(self.project, workflow_spec=workflow_spec)
        _ = self.create_workflow(self.project, workflow_spec=workflow_spec)

        # Fail search due to duplicate workflows in search scope
        with self.assertRaises(dxpy.DXSearchError):
            # Should be multiple workflows with the same name
            objects.get_data_object(
                data_obj_desc=workflow_name,
                project=self.project,
                classname="workflow",
                exists=True
            )

    def test_get_workflow_in_specific_folder(self):
        # Create 2 workflows with the same name
        workflow_name = random_name(15)
        workflow_spec = self.create_workflow_spec(self.project, workflow_name=workflow_name)
        _ = self.create_workflow(self.project, workflow_spec=workflow_spec)
        dxworkflow = self.create_workflow(self.project, workflow_spec=workflow_spec)

        # Move workflow to a different folder
        projectdx = dxpy.DXProject(self.project)
        dest_fld = random_name(10, prefix="/")
        projectdx.new_folder(dest_fld, parents=True)
        projectdx.move(destination=dest_fld, objects=[dxworkflow.get_id()])

        # Search for a workflow with the duplicate name above in a specific folder
        self.assertEqual(
            objects.get_data_object(
                workflow_name, project=self.project, classname="workflow", folder=dest_fld
            ).get_id(),
            dxworkflow.get_id()
        )

    def test_get_workflow_by_absolute_path(self):
        # Create 2 workflows with the same name
        workflow_name = random_name(15)
        workflow_spec = self.create_workflow_spec(self.project, workflow_name=workflow_name)
        dxworkflow = self.create_workflow(self.project, workflow_spec=workflow_spec)

        # Move workflow to a folder
        projectdx = dxpy.DXProject(self.project)
        dest_fld = random_name(10, prefix="/")
        projectdx.new_folder(dest_fld, parents=True)
        projectdx.move(destination=dest_fld, objects=[dxworkflow.get_id()])

        # Verify Search for folder explicitly works
        workflow_path = "{}/{}".format(dest_fld, workflow_name)
        self.assertEqual(
            objects.get_data_object(workflow_path, project=self.project, classname="workflow").get_id(),
            dxworkflow.get_id()
        )

    def test_get_applet(self):
        self.assertIsInstance(
            objects.get_data_object(self.test_applet_id, project=self.project, classname="applet"),
            dxpy.DXApplet
        )
    
    def test_get_applet_by_name(self):
        appletdx = dxpy.DXApplet(self.test_applet_id)
        self.assertEqual(
            objects.get_data_object(
                appletdx.name, project=self.project, classname="applet"
            ).get_id(),
            self.test_applet_id
        )
    
    def test_get_applet_in_folder(self):
        applet_name = random_name(20)
        applet_spec = self.create_applet_spec(self.project, applet_name=applet_name)
        applet_id = self.create_applet(project_id=self.project, applet_spec=applet_spec)
        
        # Move applet to a different folder
        projectdx = dxpy.DXProject(self.project)
        dest_fld = random_name(10, prefix="/")
        projectdx.new_folder(dest_fld, parents=True)
        projectdx.move(destination=dest_fld, objects=[applet_id])

        # Search for applet and verify success
        self.assertEqual(
            objects.get_data_object(
                applet_name, project=self.project, classname="applet", folder=dest_fld
            ).get_id(),
            applet_id
        )


class TestAppExecutableObjectSugar(testutil.DXTestCaseBuildApps):
    def test_get_app(self):
        #Create App
        app_id = self.make_apps(num_apps=1, name_prefix="Test_App")[0]["id"]

        # Search for App
        appdx = objects.get_app(app_id)

        # Verify Search
        self.assertIsInstance(
            appdx,
            dxpy.DXApp
        )
        self.assertEqual(
            appdx.get_id(),
            app_id
        )
