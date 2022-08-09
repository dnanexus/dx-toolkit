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

from __future__ import print_function, unicode_literals, division, absolute_import

from dxpy_testutil import (DXTestCase, DXTestCaseBuildApps, DXTestCaseBuildWorkflows, check_output, temporary_project,
                           select_project, cd, override_environment, generate_unique_username_email,
                           without_project_context, without_auth, as_second_user, chdir, run, DXCalledProcessError)

# from dxpy_testutil import (DXTestCase, run)
from dxpy.compat import str
from datetime import datetime
import uuid
import unittest
import json
import shutil
import subprocess
import locale

def build_nextflow_applet(app_dir, project_id):
    updated_app_dir = app_dir + str(uuid.uuid1())
    # updated_app_dir = os.path.abspath(os.path.join(tempdir, os.path.basename(app_dir)))
    # shutil.copytree(app_dir, updated_app_dir)

    build_output = run(['dx', 'build', '--nextflow', './nextflow'])
    return json.loads(build_output)['id']

class TestNextflow(DXTestCase):
    def test_temp(self):
        print("test-message")
        assert False


    def test_basic_hello(self):
        applet = build_nextflow_applet("./nextflow/", "project-GFYvg4Q0469VKVVVP359Yfpp")
        print(applet)
        self.assertFalse(True, "Expected command to fail with CalledProcessError but it succeeded")
