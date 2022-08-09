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

import os, sys, unittest, json, tempfile, subprocess, shutil, re, base64, random, time
import filecmp
import pipes
import stat
import hashlib
import collections
import string
from contextlib import contextmanager
import pexpect
import requests
import textwrap
import pytest
import gzip
import tarfile
from mock import patch

import dxpy
import dxpy.executable_builder
import dxpy.workflow_builder
from dxpy.scripts import dx_build_app
from dxpy_testutil import (DXTestCase, DXTestCaseBuildApps, DXTestCaseBuildWorkflows, check_output, temporary_project,
                           select_project, cd, override_environment, generate_unique_username_email,
                           without_project_context, without_auth, as_second_user, chdir, run, DXCalledProcessError)
import dxpy_testutil as testutil
from dxpy.exceptions import DXAPIError, DXSearchError, EXPECTED_ERR_EXIT_STATUS, HTTPError
from dxpy.compat import USING_PYTHON2, str, sys_encoding, open
from dxpy.utils.resolver import ResolutionError, _check_resolution_needed as check_resolution


def test_basic_hello(self):
    applet_id = dxpy.api.applet_new({"name": "my_first_applet",
                                     "project": self.project,
                                     "dxapi": "1.0.0",
                                     "inputSpec": [{"name": "number", "class": "int"}],
                                     "outputSpec": [{"name": "number", "class": "int"}],
                                     "runSpec": {"interpreter": "bash",
                                                 "distribution": "Ubuntu",
                                                 "release": "14.04",
                                                 "code": "exit 0"}
                                     })['id']


    wf_input = [{"name": "foo", "class": "int"}]
    wf_output = [{"name": "bar", "class": "int", "outputSource":
        {"$dnanexus_link": {"stage": "stage_0", "outputField": "number"}}}]

    workflow_spec = {"name": "my_workflow",
                     "outputFolder": "/",
                     "stages": [stage0, stage1],
                     "inputs": wf_input,
                     "outputs": wf_output
                     }

    workflow_dir = self.write_workflow_directory("dxbuilt_workflow",
                                                 json.dumps(workflow_spec),
                                                 readme_content="Workflow Readme")

    new_workflow = json.loads(run("dx build --json " + workflow_dir))
    wf_describe = dxpy.get_handler(new_workflow["id"]).describe()
    self.assertEqual(wf_describe["class"], "workflow")
    self.assertEqual(wf_describe["id"], new_workflow["id"])
    self.assertEqual(wf_describe["editVersion"], 0)
    self.assertEqual(wf_describe["name"], "my_workflow")
    self.assertEqual(wf_describe["state"], "closed")
    self.assertEqual(wf_describe["outputFolder"], "/")
    self.assertEqual(wf_describe["project"], self.project)
    self.assertEqual(wf_describe["description"], "Workflow Readme")
    self.assertEqual(len(wf_describe["stages"]), 2)
    self.assertEqual(wf_describe["stages"][0]["id"], "stage_0")
    self.assertEqual(wf_describe["stages"][0]["name"], "stage_0_name")
    self.assertEqual(wf_describe["stages"][0]["executable"], applet_id)
    self.assertEqual(wf_describe["stages"][0]["executionPolicy"]["restartOn"], {})
    self.assertEqual(wf_describe["stages"][0]["executionPolicy"]["onNonRestartableFailure"],
                     "failStage")
    self.assertEqual(wf_describe["stages"][0]["systemRequirements"]["main"]["instanceType"],
                     self.default_inst_type)
    self.assertEqual(wf_describe["stages"][1]["id"], "stage_1")
    self.assertIsNone(wf_describe["stages"][1]["name"])
    self.assertEqual(wf_describe["stages"][1]["executable"], applet_id)
    self.assertEqual(wf_describe["inputs"], wf_input)
    self.assertEqual(wf_describe["outputs"], wf_output)