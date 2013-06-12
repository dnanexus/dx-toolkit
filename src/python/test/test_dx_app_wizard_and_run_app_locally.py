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

import os, sys, unittest, json, tempfile, subprocess, csv, shutil, re
import pexpect

from dxpy_testutil import DXTestCase

import dxpy
from dxpy.scripts import dx_build_app

class TestDXAppWizardAndRunAppLocally(DXTestCase):
    @unittest.skipIf('DXTEST_RUN_JOBS' not in os.environ,
                     'skipping test that would run jobs')
    def test_dx_run_app_locally_and_compare_results(self):
        appdir = self.test_dx_run_app_locally()
        print "Setting current project to", self.project
        dxpy.WORKSPACE_ID = self.project
        dxpy.PROJECT_CONTEXT_ID = self.project
        applet_id = dx_build_app.build_and_upload_locally(appdir,
                                                          mode='applet',
                                                          overwrite=True,
                                                          dx_toolkit_autodep=False,
                                                          return_object_dump=True)['id']
        remote_job = dxpy.DXApplet(applet_id).run({"in1": 8})
        print "Waiting for", remote_job, "to complete"
        remote_job.wait_on_done()
        result = remote_job.describe()
        self.assertEqual(result["output"]["out1"], 140)

if __name__ == '__main__':
    unittest.main()
