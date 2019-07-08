from __future__ import print_function, unicode_literals, division, absolute_import
import random
import string
import unittest

import dxpy
from dxpy.sugar import objects


PROJECT_ID = "project-BzQf6k80V3bJk7x0yv6z82j7"
PROJECT_NAME = "DNAnexus Regression Testing Project AWS US east"
WORKFLOW_ID = "workflow-F417G8Q0V3bGVjG642Zjx1Gv"
WORKFLOW_FOLDER = "/gatk3/2017_04_27_22_51_39"
WORKFLOW_NAME = "GATK3 best practices"
APPLET_ID = "applet-F417G0j0V3b6bxQG68KF3j2q"
APPLET_NAME = "gatk3_bqsr_parallel"
APPLET_FOLDER = "/gatk3/2017_04_27_22_51_39/Applets"
TAG = "sugar-test"
APP_ID = "app-FYzxFq09ZZYPKkfp05027F5g"
APP_NAME = "bwa_mem_fastq_read_mapper"


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


class TestObjects(unittest.TestCase):
    def test_get_project(self):
        proj = objects.get_project(PROJECT_ID)
        self.assertIsInstance(proj, dxpy.DXProject)
        self.assertEqual(proj.get_id(), PROJECT_ID)

        proj = objects.get_project(PROJECT_NAME)
        self.assertIsInstance(proj, dxpy.DXProject)
        self.assertEqual(proj.get_id(), PROJECT_ID)

        with self.assertRaises(dxpy.DXSearchError):
            objects.get_project(PROJECT_NAME, exists=True, region="azure:westus")

        newproj_name_len = 20
        newproj_name = random_name(newproj_name_len)
        with self.assertRaises(dxpy.DXSearchError):
            objects.get_project(newproj_name, exists=True)

        newproj = None
        cleanup = True
        try:
            newproj = objects.get_project(newproj_name, exists=False, create=True)
            self.assertIsInstance(newproj, dxpy.DXProject)
            self.assertEqual(newproj.describe()["name"], newproj_name)
        except dxpy.AppError as err:
            # There is a small chance a project will already exist -
            # if so, don't delete it.
            if "exists" in err.message:
                cleanup = False
        finally:
            if newproj and cleanup:
                newproj.destroy()

        newproj = None
        try:
            newproj = objects.get_project(
                newproj_name, create=True, region="azure:westus"
            )
            self.assertIsInstance(proj, dxpy.DXProject)
            self.assertEqual(newproj.describe()["name"], newproj_name)
            self.assertEqual(newproj.describe()["region"], "azure:westus")
            self.assertEqual(
                newproj.describe()["name"],
                objects.get_project(newproj.get_id()).describe()["name"]
            )
            self.assertEqual(
                newproj.get_id(),
                objects.get_project(
                    newproj.describe()["name"], region="azure:westus"
                ).get_id()
            )
        finally:
            if newproj:
                newproj.destroy()

    def test_ensure_folder(self):
        proj = objects.get_project(PROJECT_ID)
        folder_name_len = 20
        folder_name = random_name(folder_name_len, prefix="/")
        with self.assertRaises(dxpy.DXSearchError):
            objects.ensure_folder(folder_name, proj, exists=True)

        cleanup = True
        try:
            objects.ensure_folder(folder_name, proj, exists=False, create=True)
            ls = objects.ensure_folder(folder_name, proj, exists=True)
            self.assertIsNotNone(ls)
            self.assertEqual(len(ls["folders"]), 0)
            self.assertEqual(len(ls["objects"]), 0)
        except dxpy.AppError as err:
            # There is a small chance a folder will already exist -
            # if so, don't delete it.
            if "exists" in err.message:
                cleanup = False
        finally:
            if cleanup:
                proj.remove_folder(folder_name)

    def test_get_file(self):
        proj = objects.get_project(PROJECT_ID)
        file_name_len = 20
        file_name = random_name(file_name_len)
        folder_name_len = 20
        folder_name = random_name(folder_name_len, prefix="/")

        dxfile = None
        cleanup_folder = False
        try:
            objects.ensure_folder(folder_name, proj, exists=False, create=True)
            cleanup_folder = True

            with self.assertRaises(dxpy.DXSearchError):
                objects.get_data_object(file_name, proj, exists=True)
            dxfile = dxpy.upload_string(
                "test",
                project=proj.get_id(),
                name=file_name,
                folder=folder_name,
                wait_on_close=True
            )

            self.assertEqual(
                dxfile.get_id(), objects.get_data_object(file_name, proj).get_id()
            )
            with self.assertRaises(dxpy.DXSearchError):
                objects.get_data_object(
                    file_name, proj, classname="record", exists=True)
            with self.assertRaises(dxpy.DXSearchError):
                objects.get_data_object(file_name, proj, exists=False)
        except dxpy.AppError as err:
            # There is a small chance a folder will already exist -
            # if so, don't delete it.
            if "exists" in err.message:
                cleanup_folder = False
        finally:
            if cleanup_folder:
                proj.remove_folder(folder_name, recurse=True, force=True)
            if dxfile:
                try:
                    dxfile.remove()
                except dxpy.exceptions.ResourceNotFound:
                    pass

    def test_get_workflow(self):
        # TODO: test global workflows
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

    def test_get_app(self):
        self.assertIsInstance(
            objects.get_app(APP_ID),
            dxpy.DXApp
        )
        self.assertEqual(
            objects.get_app(APP_NAME).get_id(),
            APP_ID
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
