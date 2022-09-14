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
from dxpy.nextflow.nextflow_builder import prepare_inputs

import uuid
from dxpy_testutil import (DXTestCase, DXTestCaseBuildNextflowApps, run)
import dxpy_testutil as testutil
from dxpy.compat import USING_PYTHON2, str, sys_encoding, open
from dxpy.utils.resolver import ResolutionError, _check_resolution_needed as check_resolution
import dxpy

if USING_PYTHON2:
    spawn_extra_args = {}
else:
    # Python 3 requires specifying the encoding
    spawn_extra_args = {"encoding" : "utf-8" }


default_input_len = 4
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

    def test_dxapp(self):
        dxapp = get_nextflow_dxapp()
        self.assertEqual(dxapp.get("name"), "Nextflow pipeline")
        self.assertEqual(dxapp.get("details", {}).get("repository"), "local")

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
        self.assertTrue("nextflow" in src)

    def test_src_profile(self):
        src = get_nextflow_src(profile="test_profile")
        self.assertTrue("-profile test_profile" in src)

    def test_src_inputs(self):
        src = get_nextflow_src(inputs=[input1, input2])
        self.assertTrue("--{}=\"${{{}}}\"".format(input2.get("name"), input2.get("name")) in src)
        self.assertTrue("--{}=\"dx://$".format(input1.get("name"), input1.get("name")) in src)

    def test_prepare_inputs(self):
        inputs = prepare_inputs("./nextflow/schema2.json")
        names = [i["name"] for i in inputs]
        self.assertTrue("input" in names and "outdir" in names and "save_merged_fastq" in names)
        self.assertEqual(len(names), 3)

    def test_prepare_inputs_single(self):
        inputs = prepare_inputs("./nextflow/schema3.json")
        self.assertEqual(len(inputs), 1)
        i = inputs[0]
        self.assertEqual(i["name"], "outdir")
        self.assertEqual(i["title"], "outdir")
        self.assertEqual(i["help"], "out_directory help text")
        self.assertEqual(i["hidden"], False)
        self.assertEqual(i["class"], "string")

    def test_prepare_inputs_large_file(self):
        inputs = prepare_inputs("./nextflow/schema1.json")
        self.assertEqual(len(inputs), 93)

class TestDXBuildNextflowApplet(DXTestCaseBuildNextflowApps):

    def test_dx_build_nextflow_set_default_metadata(self):
        pipeline_name = "hello"
        applet_dir = self.write_nextflow_applet_directory(pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(run("dx build --nextflow --json " + applet_dir))["id"]
        app = dxpy.describe(applet_id)
        self.assertEqual(app['name'], pipeline_name)
        self.assertEqual(app['title'], pipeline_name)
        self.assertEqual(app['summary'], pipeline_name)

    def test_dx_build_nextflow_with_abs_and_relative_path(self):
        pipeline_name = "hello_abs"
        applet_dir = self.write_nextflow_applet_directory(pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(run("dx build --nextflow --json " + applet_dir))["id"]
        app = dxpy.describe(applet_id)
        self.assertEqual(app['name'], pipeline_name)

        pipeline_name = "hello_abs_with_trailing_slash"
        applet_dir = self.write_nextflow_applet_directory(pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(run("dx build --nextflow --json " + applet_dir + "/"))["id"]
        app = dxpy.describe(applet_id)
        self.assertEqual(app['name'], pipeline_name)

        pipeline_name = "hello_rel"
        applet_dir = self.write_nextflow_applet_directory(pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(run("cd {} && dx build --nextflow . --json".format(applet_dir)))["id"]
        app = dxpy.describe(applet_id)
        self.assertEqual(app['name'], pipeline_name)

    def test_dx_build_nextflow_with_space_in_name(self):
        pipeline_name = "hello pipeline"
        applet_dir = self.write_nextflow_applet_directory(pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(run("dx build --nextflow '{}' --json".format(applet_dir)))["id"]
        app = dxpy.describe(applet_id)
        self.assertEqual(app['name'], pipeline_name)

    def test_dx_build_nextflow_with_extra_args(self):
        pipeline_name = "hello"
        extra_args = '{"name": "testing_name_hello"}'
        applet_dir = self.write_nextflow_applet_directory(pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(run("dx build --nextflow '{}' --json --extra-args '{}'".format(applet_dir, extra_args)))["id"]
        app = dxpy.describe(applet_id)
        self.assertEqual(app["name"], json.loads(extra_args)["name"])
        self.assertEqual(app["title"], pipeline_name)
        self.assertEqual(app["summary"], pipeline_name)

        extra_args = '{"name": "new_name", "title": "new title"}'
        applet_id = json.loads(run("dx build --nextflow '{}' --json --extra-args '{}'".format(applet_dir, extra_args)))["id"]
        app = dxpy.describe(applet_id)
        self.assertEqual(app["name"], json.loads(extra_args)["name"])
        self.assertEqual(app["title"], json.loads(extra_args)["title"])
        self.assertEqual(app["summary"], pipeline_name)

if __name__ == '__main__':
    if 'DXTEST_FULL' not in os.environ:
        sys.stderr.write('WARNING: env var DXTEST_FULL is not set; tests that create apps or run jobs will not be run\n')
    unittest.main()
