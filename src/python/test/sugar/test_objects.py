from __future__ import print_function, unicode_literals, division, absolute_import
import random
import string
import unittest

import dxpy
from dxpy.sugar import objects


PROJECT_ID = "project-BzQf6k80V3bJk7x0yv6z82j7"
PROJECT_NAME = "DNAnexus Regression Testing Project AWS US east"


class TestObjects(unittest.TestCase):
    def test_get_project(self):
        proj = objects.get_project(PROJECT_ID)
        self.assertIsInstance(proj, dxpy.DXProject)
        self.assertEquals(proj.get_id(), PROJECT_ID)

        proj = objects.get_project(PROJECT_NAME)
        self.assertIsInstance(proj, dxpy.DXProject)
        self.assertEquals(proj.get_id(), PROJECT_ID)

        with self.assertRaises(dxpy.AppError):
            objects.get_project(PROJECT_NAME, region="azure:westus")

        newproj_name_len = 20
        newproj_name = "".join(
            random.choice(string.ascii_letters)
            for _ in range(newproj_name_len)
        )
        with self.assertRaises(dxpy.AppError):
            newproj = None
            try:
                objects.get_project(newproj_name)
            finally:
                if newproj:
                    newproj.destroy()

        newproj = None
        try:
            newproj = objects.get_project(newproj_name, create=True)
            self.assertIsInstance(newproj, dxpy.DXProject)
            self.assertEquals(newproj.describe()["name"], newproj_name)
        finally:
            if newproj:
                newproj.destroy()

        newproj = None
        try:
            newproj = objects.get_project(
                newproj_name, create=True, region="azure:westus"
            )
            self.assertIsInstance(proj, dxpy.DXProject)
            self.assertEquals(newproj.describe()["name"], newproj_name)
            self.assertEquals(newproj.describe()["region"], "azure:westus")
            self.assertEquals(
                newproj.describe()["name"],
                objects.get_project(newproj.get_id()).describe()["name"]
            )
            self.assertEquals(
                newproj.get_id(),
                objects.get_project(
                    newproj.describe()["name"], region="azure:westus"
                ).get_id()
            )
        finally:
            if newproj:
                newproj.destroy()

