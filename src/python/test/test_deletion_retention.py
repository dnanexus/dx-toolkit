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
"""
Offline tests for the deletion-retention (soft delete / recycle bin) API
bindings. These only assert that the dxpy wrappers and handler methods build
the expected route and input hash -- the backend behaviour behind the routes is
covered by the platform's own test suites.
"""

from __future__ import print_function, unicode_literals, division, absolute_import

import unittest

try:
    from unittest import mock
except ImportError:
    import mock

import dxpy
import dxpy.api
from dxpy.bindings.dxapp import DXApp
from dxpy.bindings.dxglobalworkflow import DXGlobalWorkflow
from dxpy.bindings.dxproject import DXProject

PROJECT_ID = "project-0000000000000000000000Z1"
FILE_ID = "file-0000000000000000000000F1"
RECORD_ID = "record-0000000000000000000000R1"
DBCLUSTER_ID = "dbcluster-0000000000000000000000D1"
APP_ID = "app-0000000000000000000000A1"
GWF_ID = "globalworkflow-0000000000000000000000W1"


class TestDeletionRetentionAPIWrappers(unittest.TestCase):
    """The generated dxpy.api wrappers point at the right routes."""

    def setUp(self):
        patcher = mock.patch.object(dxpy.api, "DXHTTPRequest")
        self.http = patcher.start()
        self.addCleanup(patcher.stop)
        self.http.return_value = {}

    def assert_called_with_route(self, route, input_params):
        args, kwargs = self.http.call_args
        self.assertEqual(args[0], route)
        self.assertEqual(args[1], input_params)
        self.assertTrue(kwargs["always_retry"])

    def test_project_list_recycle_bin(self):
        dxpy.api.project_list_recycle_bin(PROJECT_ID, {})
        self.assert_called_with_route("/%s/listRecycleBin" % PROJECT_ID, {})

    def test_project_recover_objects(self):
        dxpy.api.project_recover_objects(PROJECT_ID, {"objects": [FILE_ID]})
        self.assert_called_with_route("/%s/recoverObjects" % PROJECT_ID, {"objects": [FILE_ID]})

    def test_project_purge_recycle_bin_objects(self):
        dxpy.api.project_purge_recycle_bin_objects(PROJECT_ID, {"objects": [FILE_ID, RECORD_ID]})
        self.assert_called_with_route("/%s/purgeRecycleBinObjects" % PROJECT_ID,
                                      {"objects": [FILE_ID, RECORD_ID]})

    def test_dbcluster_recover(self):
        dxpy.api.dbcluster_recover(DBCLUSTER_ID, {})
        self.assert_called_with_route("/%s/recover" % DBCLUSTER_ID, {})

    def test_app_recover_by_id(self):
        dxpy.api.app_recover(APP_ID, input_params={})
        self.assert_called_with_route("/%s/recover" % APP_ID, {})

    def test_app_recover_by_name_and_alias(self):
        dxpy.api.app_recover("app-my_app", alias="1.0.0", input_params={})
        self.assert_called_with_route("/app-my_app/1.0.0/recover", {})

    def test_global_workflow_recover_by_id(self):
        dxpy.api.global_workflow_recover(GWF_ID, input_params={})
        self.assert_called_with_route("/%s/recover" % GWF_ID, {})

    def test_global_workflow_recover_by_name_and_alias(self):
        dxpy.api.global_workflow_recover("globalworkflow-my_wf", alias="1.0.0", input_params={})
        self.assert_called_with_route("/globalworkflow-my_wf/1.0.0/recover", {})


class TestDXProjectRecycleBinBindings(unittest.TestCase):
    """DXProject handler methods delegate to the right dxpy.api wrapper."""

    def setUp(self):
        self.project = DXProject(PROJECT_ID)

    def test_list_recycle_bin(self):
        entry = {"id": FILE_ID, "class": "file", "name": "f.txt",
                 "deletedAt": 1, "purgeAt": 2, "previousFolder": "/a/b"}
        response = {"id": PROJECT_ID, "objects": [entry]}
        with mock.patch.object(dxpy.api, "project_list_recycle_bin",
                               return_value=response) as wrapper:
            self.assertEqual(self.project.list_recycle_bin(), response)
        wrapper.assert_called_once_with(PROJECT_ID, {})

    def test_recover_objects(self):
        response = {"id": PROJECT_ID, "dataObjects": [FILE_ID]}
        with mock.patch.object(dxpy.api, "project_recover_objects",
                               return_value=response) as wrapper:
            self.assertEqual(self.project.recover_objects([FILE_ID]), response)
        wrapper.assert_called_once_with(PROJECT_ID, {"objects": [FILE_ID]})

    def test_purge_recycle_bin_objects(self):
        response = {"id": PROJECT_ID,
                    "scheduled": [FILE_ID],
                    "failed": [{"id": RECORD_ID, "reason": "notFoundInRecycleBin"}]}
        with mock.patch.object(dxpy.api, "project_purge_recycle_bin_objects",
                               return_value=response) as wrapper:
            self.assertEqual(self.project.purge_recycle_bin_objects([FILE_ID, RECORD_ID]), response)
        wrapper.assert_called_once_with(PROJECT_ID, {"objects": [FILE_ID, RECORD_ID]})

    def test_kwargs_are_forwarded(self):
        with mock.patch.object(dxpy.api, "project_list_recycle_bin", return_value={}) as wrapper:
            self.project.list_recycle_bin(always_retry=False)
        wrapper.assert_called_once_with(PROJECT_ID, {}, always_retry=False)


class TestDXProjectRetentionFields(unittest.TestCase):
    """deletionRetentionEnabled round-trips through /project/new and /update."""

    def test_new_sends_deletion_retention_enabled(self):
        with mock.patch.object(dxpy.api, "project_new",
                               return_value={"id": PROJECT_ID}) as wrapper:
            DXProject(PROJECT_ID).new("retained", deletion_retention_enabled=True)
        self.assertEqual(wrapper.call_args[0][0]["deletionRetentionEnabled"], True)

    def test_new_omits_deletion_retention_enabled_by_default(self):
        with mock.patch.object(dxpy.api, "project_new",
                               return_value={"id": PROJECT_ID}) as wrapper:
            DXProject(PROJECT_ID).new("plain")
        self.assertNotIn("deletionRetentionEnabled", wrapper.call_args[0][0])

    def test_update_sends_deletion_retention_enabled(self):
        with mock.patch.object(dxpy.api, "project_update") as wrapper:
            DXProject(PROJECT_ID).update(deletion_retention_enabled=False)
        wrapper.assert_called_once_with(PROJECT_ID, {"deletionRetentionEnabled": False})

    def test_update_omits_deletion_retention_enabled_by_default(self):
        with mock.patch.object(dxpy.api, "project_update") as wrapper:
            DXProject(PROJECT_ID).update(name="renamed")
        self.assertNotIn("deletionRetentionEnabled", wrapper.call_args[0][1])


class TestExecutableRecoverBindings(unittest.TestCase):
    """DXApp / DXGlobalWorkflow recover() undo a retention-aware delete."""

    def test_app_recover_by_id(self):
        with mock.patch.object(dxpy.api, "app_recover", return_value={"id": APP_ID}) as wrapper:
            DXApp(dxid=APP_ID).recover()
        wrapper.assert_called_once_with(APP_ID)

    def test_app_recover_by_name(self):
        with mock.patch.object(dxpy.api, "app_recover", return_value={"id": APP_ID}) as wrapper:
            DXApp(name="my_app", alias="1.0.0").recover()
        wrapper.assert_called_once_with("app-my_app", alias="1.0.0")

    def test_global_workflow_recover_by_id(self):
        with mock.patch.object(dxpy.api, "global_workflow_recover",
                               return_value={"id": GWF_ID}) as wrapper:
            DXGlobalWorkflow(dxid=GWF_ID).recover()
        wrapper.assert_called_once_with(GWF_ID)

    def test_global_workflow_recover_by_name(self):
        with mock.patch.object(dxpy.api, "global_workflow_recover",
                               return_value={"id": GWF_ID}) as wrapper:
            DXGlobalWorkflow(name="my_wf", alias="1.0.0").recover()
        wrapper.assert_called_once_with("globalworkflow-my_wf", alias="1.0.0")


if __name__ == "__main__":
    unittest.main()
