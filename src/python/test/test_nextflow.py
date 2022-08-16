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
from parameterized import parameterized

import os, sys, unittest, json
from dxpy.nextflow.nextflow_templates import get_nextflow_src
from dxpy.nextflow.nextflow_templates import get_nextflow_dxapp
from dxpy.nextflow.nextflow_templates import get_default_inputs
from dxpy.nextflow.nextflow_builder import prepare_inputs

import uuid
from dxpy_testutil import (DXTestCase, DXTestCaseBuildApps, DXTestCaseBuildWorkflows, check_output, temporary_project,
                           select_project, cd, override_environment, generate_unique_username_email,
                           without_project_context, without_auth, as_second_user, chdir, run, DXCalledProcessError)
import dxpy_testutil as testutil
from dxpy.exceptions import DXAPIError, DXSearchError, EXPECTED_ERR_EXIT_STATUS, HTTPError
from dxpy.compat import USING_PYTHON2, str, sys_encoding, open
from dxpy.utils.resolver import ResolutionError, _check_resolution_needed as check_resolution
if USING_PYTHON2:
    spawn_extra_args = {}
else:
    # Python 3 requires specifying the encoding
    spawn_extra_args = {"encoding" : "utf-8" }


default_input_len = 7
input1 = {
    "class": "file",
    "name": "first_input",
    "optional": True,
    "help": "(Optional) First input",
    "label": "Test"
}
input2 = {
    "class": "string",
    "name": "second_input",
    "help": "second input",
    "label": "Test2"
}

def build_nextflow_applet(app_dir):
    with temporary_project('test proj', reclaim_permissions=True, cleanup=False) as temp_project:

        # updated_app_dir = app_dir + str(uuid.uuid1())
        # updated_app_dir = os.path.abspath(os.path.join(tempdir, os.path.basename(app_dir)))
        # shutil.copytree(app_dir, updated_app_dir)
        build_output = run(['dx', 'build', '--nextflow', './nextflow', '-f', '--project', temp_project.get_id()])
        print(build_output, file=sys.stderr)
        return json.loads(build_output)['id']


# class TestNextflow(DXTestCase):
#
#     def test_basic_hello(self):
#         applet = build_nextflow_applet("./nextflow/")
#         print(applet)

class TestNextflowTemplates(DXTestCase):

    def are_inputs_in_spec(self, inputSpec, inputs):
        found=[False] * len(inputs)
        input_names = [i["name"] for i in inputs]
        input_pairs = dict(zip(input_names, found))
        for i in inputSpec:
            if i.get("name") in input_names:
                if input_pairs.get(i.get("name")):
                    raise Exception("Input was found twice!")
                input_pairs[i.get("name")] = True
        return all(value is True for value in input_pairs.values())

    def test_inputs(self):
        inputs = get_default_inputs()
        self.assertEqual(len(inputs), default_input_len)

    def test_dxapp(self):
        dxapp = get_nextflow_dxapp()
        self.assertEqual(dxapp.get("name"), "nextflow pipeline")

    @parameterized.expand([
        [input1],
        [input2],
        [input1, input2]
    ])
    def test_dxapp_custom_input(self, *inputs):
        inputs = list(inputs)
        dxapp = get_nextflow_dxapp(custom_inputs=inputs)
        self.assertTrue(self.are_inputs_in_spec(dxapp.get("inputSpec"), inputs))
        self.assertEqual(len(dxapp.get("inputSpec")), default_input_len + len(inputs))

    def test_src_basic(self):
        src = get_nextflow_src()
        self.assertTrue("#!/usr/bin/env bash" in src)
        self.assertTrue("nextflow run" in src)

    def test_src_profile(self):
        src = get_nextflow_src(profile="test_profile")
        self.assertTrue("-profile test_profile" in src)

    def test_src_inputs(self):
        src = get_nextflow_src(inputs=[input1, input2])
        self.assertTrue("--{}=${}".format(input1.get("name"), input1.get("name")) in src)
        self.assertTrue("--{}=${}".format(input2.get("name"), input2.get("name")) in src)

    def test_prepare_inputs(self):
        inputs = prepare_inputs("./nextflow/schema2.json")
        names = [i["name"] for i in inputs]
        self.assertTrue("input" in names and "outdir" in names and "save_merged_fastq" in names)
        self.assertEqual(len(names), 3)

    def test_prepare_inputs(self):
        inputs = prepare_inputs("./nextflow/schema1.json")
        print(len(inputs))
        self.assertEqual(len(inputs), 72)



if __name__ == '__main__':
    if 'DXTEST_FULL' not in os.environ:
        sys.stderr.write('WARNING: env var DXTEST_FULL is not set; tests that create apps or run jobs will not be run\n')
    unittest.main()