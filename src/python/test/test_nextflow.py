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

import os
import sys
import unittest
import json
from dxpy.nextflow.nextflow_templates import get_nextflow_src, get_nextflow_dxapp

import uuid
from dxpy_testutil import (DXTestCase, DXTestCaseBuildNextflowApps, run, chdir)
import dxpy_testutil as testutil
from dxpy.compat import USING_PYTHON2, str, sys_encoding, open
from dxpy.utils.resolver import ResolutionError
import dxpy
from dxpy.nextflow.nextflow_builder import prepare_custom_inputs

if USING_PYTHON2:
    spawn_extra_args = {}
else:
    # Python 3 requires specifying the encoding
    spawn_extra_args = {"encoding": "utf-8"}


default_input_len = 5
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
        found = [False] * len(inputs)
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
        self.assertTrue(self.are_inputs_in_spec(
            dxapp.get("inputSpec"), inputs))
        self.assertEqual(len(dxapp.get("inputSpec")),
                         default_input_len + len(inputs))

    def test_src_basic(self):
        src = get_nextflow_src()
        self.assertTrue("#!/usr/bin/env bash" in src)
        self.assertTrue("nextflow" in src)

    def test_src_profile(self):
        src = get_nextflow_src(profile="test_profile")
        self.assertTrue("-profile test_profile" in src)

    def test_src_inputs(self):
        src = get_nextflow_src(custom_inputs=[input1, input2])
        self.assertTrue(
            "--{}=${{{}}}".format(input2.get("name"), input2.get("name")) in src)
        self.assertTrue(
            "dx://${{DX_WORKSPACE_ID}}:/$(echo ${{{}}} | jq .[$dnanexus_link] -r | xargs -I {{}} dx describe {{}} --json | jq -r .name)".format(input1.get("name")) in src)

    def test_prepare_inputs(self):
        inputs = prepare_custom_inputs(schema_file="./nextflow/schema2.json")
        names = [i["name"] for i in inputs]
        self.assertTrue(
            "input" in names and "outdir" in names and "save_merged_fastq" in names)
        self.assertEqual(len(names), 3)

    def test_prepare_inputs_single(self):
        inputs = prepare_custom_inputs(schema_file="./nextflow/schema3.json")
        self.assertEqual(len(inputs), 1)
        i = inputs[0]
        self.assertEqual(i["name"], "outdir")
        self.assertEqual(i["title"], "outdir")
        self.assertEqual(i["help"], "(Nextflow pipeline required) out_directory help text")
        self.assertEqual(i["hidden"], False)
        self.assertEqual(i["class"], "string")

    def test_prepare_inputs_large_file(self):
        inputs = prepare_custom_inputs(schema_file="./nextflow/schema1.json")
        self.assertEqual(len(inputs), 93)


class TestDXBuildNextflowApplet(DXTestCaseBuildNextflowApps):

    def test_dx_build_nextflow_default_metadata(self):
        pipeline_name = "hello"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(
            run("dx build --nextflow --json " + applet_dir))["id"]
        applet = dxpy.DXApplet(applet_id)
        desc = applet.describe()
        self.assertEqual(desc["name"], pipeline_name)
        self.assertEqual(desc["title"], pipeline_name)
        self.assertEqual(desc["summary"], pipeline_name)

        details = applet.get_details()
        self.assertEqual(details["repository"], "local")

    def test_dx_build_nextflow_with_abs_and_relative_path(self):
        pipeline_name = "hello_abs"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(
            run("dx build --nextflow --json " + applet_dir))["id"]
        app = dxpy.describe(applet_id)
        self.assertEqual(app["name"], pipeline_name)

        pipeline_name = "hello_abs_with_trailing_slash"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(
            run("dx build --nextflow --json " + applet_dir + "/"))["id"]
        app = dxpy.describe(applet_id)
        self.assertEqual(app["name"], pipeline_name)

        pipeline_name = "hello_rel"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        with chdir(applet_dir):
            applet_id = json.loads(
            run("dx build --nextflow . --json".format(applet_dir)))["id"]
        app = dxpy.describe(applet_id)
        self.assertEqual(app["name"], pipeline_name)

    def test_dx_build_nextflow_with_space_in_name(self):
        pipeline_name = "hello pipeline"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(
            run("dx build --nextflow '{}' --json".format(applet_dir)))["id"]
        app = dxpy.describe(applet_id)
        self.assertEqual(app["name"], pipeline_name)

    def test_dx_build_nextflow_with_extra_args(self):
        pipeline_name = "hello"
        extra_args = '{"name": "testing_name_hello"}'
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(run(
            "dx build --nextflow '{}' --json --extra-args '{}'".format(applet_dir, extra_args)))["id"]
        app = dxpy.describe(applet_id)
        self.assertEqual(app["name"], json.loads(extra_args)["name"])
        self.assertEqual(app["title"], pipeline_name)
        self.assertEqual(app["summary"], pipeline_name)

        extra_args = '{"name": "new_name", "title": "new title"}'
        applet_id = json.loads(run(
            "dx build --nextflow '{}' --json --extra-args '{}'".format(applet_dir, extra_args)))["id"]
        app = dxpy.describe(applet_id)
        self.assertEqual(app["name"], json.loads(extra_args)["name"])
        self.assertEqual(app["title"], json.loads(extra_args)["title"])
        self.assertEqual(app["summary"], pipeline_name)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_build_nextflow_from_repository_default_metadata(self):
        pipeline_name = "hello"
        hello_repo_url = "https://github.com/nextflow-io/hello"
        applet_id = run(
            "dx build --nextflow --repository '{}' --brief".format(hello_repo_url)).strip()
        applet = dxpy.DXApplet(applet_id)
        desc = applet.describe()
        self.assertEqual(desc["name"], pipeline_name)
        self.assertEqual(desc["title"], pipeline_name)
        self.assertEqual(desc["summary"], pipeline_name)

        details = applet.get_details()
        self.assertEqual(details["repository"], hello_repo_url)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_build_nextflow_from_repository_with_extra_args(self):
        pipeline_name = "hello"
        hello_repo_url = "https://github.com/nextflow-io/hello"
        extra_args = '{"name": "new name", "title": "new title"}'
        applet_id = run("dx build --nextflow --repository '{}' --extra-args '{}' --brief".format(hello_repo_url, extra_args)).strip()
        applet = dxpy.DXApplet(applet_id)
        desc = applet.describe()
        self.assertEqual(desc["name"], "new name")
        self.assertEqual(desc["title"], "new title")
        self.assertEqual(desc["summary"], pipeline_name)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_build_nextflow_with_publishDir(self):
        pipeline_name = "cat_ls"
        # extra_args = '{"name": "testing_cat_ls"}'
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, nf_file_name="main.nf", existing_nf_file_path="nextflow/publishDir/main.nf")
        applet_id = json.loads(run(
            "dx build --nextflow '{}' --json".format(applet_dir)))["id"]
        desc = dxpy.describe(applet_id)

        # Run with "dx run".
        dxfile = dxpy.upload_string("foo", name="foo.txt", folder="/a/b/c",
                                    project=self.project, parents=True, wait_on_close=True)
        inFile_path = "dx://{}:/a/b/c/foo.txt".format(self.project)
        inFolder_path = "dx://{}:/a/".format(self.project)
        outdir = "nxf_outdir"
        pipeline_args = "'--outdir {} --inFile {} --inFolder {}'".format(
            outdir, inFile_path, inFolder_path)

        job_id = run(
            "dx run {applet_id} -idebug=true -inextflow_pipeline_params={pipeline_args} --folder :/test-cat-ls/ -y --brief".format(
                applet_id=applet_id, pipeline_args=pipeline_args)
        ).strip()
        job_handler = dxpy.DXJob(job_id)
        job_handler.wait_on_done()
        job_desc = dxpy.describe(job_id)

        print(job_desc["output"])
        self.assertEqual(len(job_desc["output"]["nextflow_log"]), 1)

        # the output files will be: nxf_runtime.config, ls_folder.txt, cat_file.txt
        self.assertEqual(len(job_desc["output"]["output_files"]), 3)

    def test_dx_build_nextflow_with_destination(self):
        pipeline_name = "hello"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(
            run("dx build --nextflow --json --destination MyApplet " + applet_dir))["id"]
        applet = dxpy.DXApplet(applet_id)
        desc = applet.describe()
        self.assertEqual(desc["name"], "MyApplet")
        self.assertEqual(desc["title"], pipeline_name)
        self.assertEqual(desc["summary"], pipeline_name)
class TestRunNextflowApplet(DXTestCaseBuildNextflowApps):

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_run_nextflow_with_additional_parameters(self):
        pipeline_name = "hello"
        applet_dir = self.write_nextflow_applet_directory(pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(run("dx build --nextflow --json " + applet_dir))["id"]
        applet = dxpy.DXApplet(applet_id)

        job = applet.run({
                         "nextflow_pipeline_params": "--input 'Printed_test_message'",
                         "nextflow_top_level_opts": "-quiet"
        })

        watched_run_output = run("dx watch {}".format(job.get_id()))
        self.assertIn("Printed_test_message", watched_run_output)
        # Running with the -quiet option reduces the amount of log and the lines such as:
        # STDOUT Launching `/home/dnanexus/hello/main.nf` [run-c8804f26-2eac-48d2-9a1a-a707ad1189eb] DSL2 - revision: 72a5d52d07
        # are not printed
        self.assertNotIn("Launching", watched_run_output)

if __name__ == '__main__':
    if 'DXTEST_FULL' not in os.environ:
        sys.stderr.write(
            'WARNING: env var DXTEST_FULL is not set; tests that create apps or run jobs will not be run\n')
    unittest.main()

