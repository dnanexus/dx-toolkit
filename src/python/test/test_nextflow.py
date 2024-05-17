#!/usr/bin/env python3
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

import tempfile
import shutil
import os
import sys
import unittest
import json
from dxpy.nextflow.nextflow_templates import get_nextflow_src, get_nextflow_dxapp
from dxpy.nextflow.nextflow_utils import get_template_dir
from dxpy.nextflow.collect_images import bundle_docker_images

import uuid
from dxpy_testutil import (DXTestCase, DXTestCaseBuildNextflowApps, run, chdir)
import dxpy_testutil as testutil
from dxpy.compat import USING_PYTHON2, str, sys_encoding, open
from dxpy.utils.resolver import ResolutionError
import dxpy
from dxpy.nextflow.nextflow_builder import prepare_custom_inputs
from dxpy.nextflow.nextflow_utils import find_readme
if USING_PYTHON2:
    spawn_extra_args = {}
else:
    # Python 3 requires specifying the encoding
    spawn_extra_args = {"encoding": "utf-8"}
from pathlib import Path

THIS_DIR = Path(__file__).parent


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
input3 = {
    "class": "file",
    "name": "third_input",
    "help": "(Nextflow pipeline optional)third input",
    "label": "Test3"
}
input4 = {
    "class": "file",
    "name": "fourth_input",
    "help": "(Nextflow pipeline required)fourth input",
    "label": "Test4"
}


class TestNextflowUtils(DXTestCase):

    @parameterized.expand([
        [None, 'file1.txt', 'file2.txt', 'main.nf'],
        ['README.txt', 'README.txt', 'readme.txt', 'main.nf'],
        ['README.md', 'README.md', 'main.nf'],
        [None]
    ])
    def test_searching_readmes(self, expected, *file_list):
        temp_dir = tempfile.mkdtemp()
        try:
            # creating a folder with files for find_readme
            for filename in file_list:
                file_path = os.path.join(temp_dir, filename)
                open(file_path, 'a').close()
            actual = find_readme(temp_dir)
        finally:
            shutil.rmtree(temp_dir)
        assert actual == expected


class TestNextflowTemplates(DXTestCase):

    def test_dxapp(self):
        dxapp = get_nextflow_dxapp()
        self.assertEqual(dxapp.get("name"), "python")  # name is by default set to the resources directory name
        self.assertEqual(dxapp.get("details", {}).get("repository"), "local")

    @parameterized.expand([
        [input1],
        [input2],
        [input1, input2]
    ])
    def test_dxapp_custom_input(self, *inputs):
        with open(os.path.join(str(get_template_dir()), 'dxapp.json'), 'r') as f:
            default_dxapp = json.load(f)

        inputs = list(inputs)
        dxapp = get_nextflow_dxapp(custom_inputs=inputs)
        self.assertEqual(dxapp.get("inputSpec"), inputs + default_dxapp.get("inputSpec"))

    @unittest.skipIf(USING_PYTHON2,
        'Skipping as the Nextflow template from which applets are built is for Py3 interpreter only')
    def test_src_basic(self):
        src = get_nextflow_src()
        self.assertTrue("#!/usr/bin/env bash" in src)
        self.assertTrue("nextflow" in src)

    @unittest.skipIf(USING_PYTHON2,
        'Skipping as the Nextflow template from which applets are built is for Py3 interpreter only')
    def test_src_profile(self):
        src = get_nextflow_src(profile="test_profile")
        self.assertTrue("-profile test_profile" in src)

    @unittest.skipIf(USING_PYTHON2,
        'Skipping as the Nextflow template from which applets are built is for Py3 interpreter only')
    def test_src_inputs(self):
        '''
        Tests that code that handles custom nextflow input parameters (e.g. from nextflow schema) with different classes
        are properly added in the applet source script. These input arguments should be appended to nextflow cmd as runtime parameters
        '''
        src = get_nextflow_src(custom_inputs=[input1, input2, input3, input4])
        # case 1: file input, need to convert from dnanexus link to its file path inside job workspace
        self.assertTrue("if [ -n \"${}\" ];".format(input1.get("name")) in src)
        value1 = 'dx://${DX_WORKSPACE_ID}:/$(echo ${%s} | jq .[$dnanexus_link] -r | xargs -I {} dx describe {} --json | jq -r .name)' % input1.get(
            "name")
        self.assertTrue("applet_runtime_inputs+=(--{} \"{}\")".format(input1.get("name"), value1) in src)
        # case 2: string input, need no conversion
        self.assertTrue("if [ -n \"${}\" ];".format(input2.get("name")) in src)
        value2 = '${%s}' % input2.get("name")
        self.assertTrue("applet_runtime_inputs+=(--{} \"{}\")".format(input2.get("name"), value2) in src)
        # case 3: file input (nextflow pipeline optional), same as case 1
        self.assertTrue("if [ -n \"${}\" ];".format(input3.get("name")) in src)
        value3 = 'dx://${DX_WORKSPACE_ID}:/$(echo ${%s} | jq .[$dnanexus_link] -r | xargs -I {} dx describe {} --json | jq -r .name)' % input3.get(
            "name")
        self.assertTrue("applet_runtime_inputs+=(--{} \"{}\")".format(input3.get("name"), value3) in src)
        # case 4: file input (nextflow pipeline required), same as case 1
        self.assertTrue("if [ -n \"${}\" ];".format(input4.get("name")) in src)
        value4 = 'dx://${DX_WORKSPACE_ID}:/$(echo ${%s} | jq .[$dnanexus_link] -r | xargs -I {} dx describe {} --json | jq -r .name)' % input4.get(
            "name")
        self.assertTrue("applet_runtime_inputs+=(--{} \"{}\")".format(input4.get("name"), value4) in src)
        
        self.assertTrue("dx-download-all-inputs --parallel --except {} --except {} --except {}".format(
            input1.get("name"), input3.get("name"), input4.get("name")) in src)

    def test_prepare_inputs(self):
        inputs = prepare_custom_inputs(schema_file=THIS_DIR / "nextflow/schema2.json")
        names = [i["name"] for i in inputs]
        self.assertTrue(
            "input" in names and "outdir" in names and "save_merged_fastq" in names)
        self.assertEqual(len(names), 3)

    def test_prepare_inputs_single(self):
        inputs = prepare_custom_inputs(schema_file=THIS_DIR / "nextflow/schema3.json")
        self.assertEqual(len(inputs), 1)
        i = inputs[0]
        self.assertEqual(i["name"], "outdir")
        self.assertEqual(i["title"], "outdir")
        self.assertEqual(i["help"], "(Nextflow pipeline required) out_directory help text")
        self.assertEqual(i["hidden"], False)
        self.assertEqual(i["class"], "string")

    def test_prepare_inputs_large_file(self):
        inputs = prepare_custom_inputs(schema_file=THIS_DIR / "nextflow/schema1.json")
        self.assertEqual(len(inputs), 93)


class TestDXBuildNextflowApplet(DXTestCaseBuildNextflowApps):

    def test_dx_build_nextflow_default_metadata(self):
        # Name of folder containing *.nf
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
        # Name of folder containing *.nf
        pipeline_name = "hello"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, existing_nf_file_path=self.base_nextflow_nf)

        # Override metadata values
        extra_args = '{"name": "name-9Oxvx2tCZe", "title": "Title VsnhPeFBqt", "summary": "Summary 3E7fFfEXdB"}'
        applet_id = json.loads(run(
            "dx build --nextflow '{}' --json --extra-args '{}'".format(applet_dir, extra_args)))["id"]

        applet = dxpy.DXApplet(applet_id)
        desc = applet.describe()
        self.assertEqual(desc["name"], json.loads(extra_args)["name"])
        self.assertEqual(desc["title"], json.loads(extra_args)["title"])
        self.assertEqual(desc["summary"], json.loads(extra_args)["summary"])

        details = applet.get_details()
        self.assertEqual(details["repository"], "local")

    @unittest.skipUnless(testutil.TEST_NF_DOCKER,
                         'skipping tests that require docker')
    def test_bundle_docker_images(self):
        image_refs = [
            {
                "engine": "docker",
                "process": "proc1",
                "digest": "sha256:cca7bbfb3cd4dc1022f00cee78c51aa46ecc3141188f0dd520978a620697e7ad",
                "image_name": "busybox",
                "tag": "1.36"
            },
            {
                "engine": "docker",
                "process": "proc2",
                "digest": "sha256:cca7bbfb3cd4dc1022f00cee78c51aa46ecc3141188f0dd520978a620697e7ad",
                "image_name": "busybox",
                "tag": "1.36"
            }
        ]
        bundled_images = bundle_docker_images(image_refs)
        self.assertEqual(len(bundled_images), 1)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    @unittest.skipUnless(testutil.TEST_NF_DOCKER,
                         'skipping tests that require docker')
    def test_dx_build_nextflow_from_local_cache_docker(self):
        applet_id = json.loads(
            run("dx build --brief --nextflow '{}' --cache-docker".format(self.base_nextflow_docker)).strip()
        )["id"]

        applet = dxpy.DXApplet(applet_id)
        desc = applet.describe()
        dependencies = desc.get("runSpec").get("bundledDepends")
        docker_dependency = [x for x in dependencies if x["name"] == "bash"]
        self.assertEqual(len(docker_dependency), 1)
        details = applet.get_details()
        self.assertTrue(details["repository"].startswith("project-"))


    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_build_nextflow_from_repository_default_metadata(self):
        pipeline_name = "hello"
        hello_repo_url = "https://github.com/nextflow-io/hello"
        applet_json = run(
            "dx build --nextflow --repository '{}' --brief".format(hello_repo_url)).strip()
        applet_id = json.loads(applet_json).get("id")

        applet = dxpy.DXApplet(applet_id)
        desc = applet.describe()
        self.assertEqual(desc["name"], pipeline_name)
        self.assertEqual(desc["title"], pipeline_name)
        self.assertEqual(desc["summary"], pipeline_name)

        details = applet.get_details()
        self.assertEqual(details["repository"], hello_repo_url)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_build_nextflow_from_repository_destination(self):
        hello_repo_url = "https://github.com/nextflow-io/hello"
        folder = "/test_dx_build_nextflow_from_repository_destination/{}".format(str(uuid.uuid4().hex))
        run("dx mkdir -p {}".format(folder))
        applet_json = run(
            "dx build --nextflow --repository '{}' --brief --destination {}".format(hello_repo_url, folder)).strip()
        applet_id = json.loads(applet_json).get("id")

        applet = dxpy.DXApplet(applet_id)
        desc = applet.describe()
        self.assertEqual(desc["folder"], folder)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_build_nextflow_from_repository_with_extra_args(self):
        hello_repo_url = "https://github.com/nextflow-io/hello"

        # Override metadata values
        extra_args = '{"name": "name-l1DeZYnTyQ", "title": "Title KkWUaqpHh1", "summary": "Summary Yqf37VpDTY"}'
        applet_json = run("dx build --nextflow --repository '{}' --extra-args '{}' --brief".format(hello_repo_url, extra_args)).strip()

        applet_id = json.loads(applet_json).get("id")
        applet = dxpy.DXApplet(applet_id)
        desc = applet.describe()
        self.assertEqual(desc["name"], json.loads(extra_args)["name"])
        self.assertEqual(desc["title"], json.loads(extra_args)["title"])
        self.assertEqual(desc["summary"], json.loads(extra_args)["summary"])

        details = applet.get_details()
        self.assertEqual(details["repository"], hello_repo_url)

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

    # @unittest.skipUnless(testutil.TEST_RUN_JOBS,
    #                      'skipping tests that would run jobs')
    @unittest.skip("skipping flaky test; to be fixed separately")
    def test_dx_run_retry_fail(self):
        pipeline_name = "retryMaxRetries"
        nextflow_file = THIS_DIR / "nextflow/RetryMaxRetries/main.nf"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, existing_nf_file_path=nextflow_file)
        applet_id = json.loads(
            run("dx build --nextflow --json " + applet_dir))["id"]
        applet = dxpy.DXApplet(applet_id)

        job = applet.run({})
        self.assertRaises(dxpy.exceptions.DXJobFailureError, job.wait_on_done)
        desc = job.describe()
        self.assertEqual(desc.get("properties", {}).get("nextflow_errorStrategy"), "retry-exceedsMaxValue")

        errored_subjob = dxpy.DXJob(desc.get("properties", {})["nextflow_errored_subjob"])
        self.assertRaises(dxpy.exceptions.DXJobFailureError, errored_subjob.wait_on_done)
        subjob_desc = errored_subjob.describe()
        self.assertEqual(subjob_desc.get("properties").get("nextflow_errorStrategy"), "retry-exceedsMaxValue")
        self.assertEqual(subjob_desc.get("properties").get("nextflow_errored_subjob"), "self")

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_run_nextflow_with_additional_parameters(self):
        pipeline_name = "hello"
        applet_dir = self.write_nextflow_applet_directory(pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(run("dx build --nextflow --json " + applet_dir))["id"]
        applet = dxpy.DXApplet(applet_id)

        job = applet.run({
                         "nextflow_pipeline_params": "--input 'Printed test message'",
                         "nextflow_top_level_opts": "-quiet"
        })

        watched_run_output = run("dx watch {}".format(job.get_id()))
        self.assertIn("hello STDOUT Printed test message world!", watched_run_output)
        # Running with the -quiet option reduces the amount of log and the lines such as:
        # STDOUT Launching `/home/dnanexus/hello/main.nf` [run-c8804f26-2eac-48d2-9a1a-a707ad1189eb] DSL2 - revision: 72a5d52d07
        # are not printed
        self.assertNotIn("Launching", watched_run_output)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_run_nextflow_by_cloning(self):
        pipeline_name = "hello"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(
            run("dx build --nextflow --json " + applet_dir))["id"]
        applet = dxpy.DXApplet(applet_id)

        orig_job = applet.run({
            "preserve_cache": True,
            "debug" : True
        })

        orig_job.wait_on_done()
        orig_job_desc = orig_job.describe()
        self.assertDictSubsetOf({"nextflow_executable": "hello",
                                "nextflow_preserve_cache": "true"}, orig_job_desc["properties"])

        orig_job.set_properties(
            {"extra_user_prop": "extra_value", "nextflow_preserve_cache": "invalid_boolean", "nextflow_nonexistent_prop": "nonexistent_nextflow_prop_value"})

        new_job_id = run("dx run --clone " +
                         orig_job.get_id() + " --brief -y ").strip()
        dxpy.DXJob(new_job_id).wait_on_done()
        new_job_desc = dxpy.api.job_describe(new_job_id)
        self.assertDictSubsetOf({"nextflow_executable": "hello", "nextflow_preserve_cache": "true",
                                "extra_user_prop": "extra_value"}, new_job_desc["properties"])
        self.assertNotIn("nextflow_nonexistent_prop", new_job_desc["properties"])

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_run_nextflow_with_unsupported_runtime_opts(self):
        pipeline_name = "hello"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(
            run("dx build --nextflow --json " + applet_dir))["id"]
        applet = dxpy.DXApplet(applet_id)

        job = applet.run({
                         "nextflow_run_opts": "-w user_workdir",
                         })
        self.assertRaises(dxpy.exceptions.DXJobFailureError, job.wait_on_done)
        desc = job.describe()
        job_desc = dxpy.DXJob(job.get_id()).describe()
        self.assertEqual(job_desc["failureReason"], "AppError")
        self.assertIn("Please remove workDir specification",
                      job_desc["failureMessage"])

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_run_nextflow_with_publishDir(self):
        pipeline_name = "cat_ls"
        # extra_args = '{"name": "testing_cat_ls"}'
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, nf_file_name="main.nf", existing_nf_file_path=THIS_DIR / "nextflow/publishDir/main.nf")
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

        # the output files will be: ls_folder.txt, cat_file.txt
        self.assertEqual(len(job_desc["output"]["published_files"]), 2)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_run_override_profile(self):
        pipeline_name = "profile_test"

        applet_dir = self.write_nextflow_applet_directory_from_folder(pipeline_name, THIS_DIR / "nextflow/profile/")
        applet_id = json.loads(run(
            "dx build --nextflow --profile test '{}' --json".format(applet_dir)))["id"]
        job_id = run(
            "dx run {applet_id} -y -inextflow_run_opts=\"-profile second\" --brief".format(applet_id=applet_id)
        ).strip()

        job_handler = dxpy.DXJob(job_id)
        job_handler.wait_on_done()
        watched_run_output = run("dx watch {} --no-follow".format(job_id))

        self.assertTrue("second_config world!" in watched_run_output, "second_config world! test was NOT found in the job log of {job_id}".format(job_id=job_id))
        self.assertTrue("test_config world!" not in watched_run_output, "test_config world! test was found in the job log of {job_id}, but it should have been overriden".format(job_id=job_id))

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_run_nextflow_with_soft_conf_files(self):
        pipeline_name = "print_env"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name,  existing_nf_file_path=THIS_DIR / "nextflow/print_env_nextflow_soft_confs/main.nf")
        applet_id = json.loads(run(
            "dx build --nextflow '{}' --json".format(applet_dir)))["id"]

        # Run with "dx run".
        first_config = dxpy.upload_local_file(THIS_DIR / "nextflow/print_env_nextflow_soft_confs/first.config", project=self.project, wait_on_close=True).get_id()
        second_config = dxpy.upload_local_file(THIS_DIR / "nextflow/print_env_nextflow_soft_confs/second.config", project=self.project, wait_on_close=True).get_id()

        job_id = run(
            "dx run {applet_id} -idebug=true -inextflow_soft_confs={first_config} -inextflow_soft_confs={second_config} --brief -y".format(
                applet_id=applet_id, first_config=first_config, second_config=second_config)
        ).strip()
        job_handler = dxpy.DXJob(job_id)
        job_handler.wait_on_done()
        watched_run_output = run("dx watch {} --no-follow".format(job_id))
        self.assertTrue("-c /home/dnanexus/in/nextflow_soft_confs/0/first.config -c /home/dnanexus/in/nextflow_soft_confs/1/second.config" in watched_run_output)
        # env var ALPHA specified in first.config and second.config
        # the value in second.config overrides the one in first.config
        self.assertTrue("The env var ALPHA is: runtime alpha 2" in watched_run_output)
        # env var BETA specified in first.config only
        self.assertTrue("The env var BETA is: runtime beta 1" in watched_run_output)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_run_nextflow_with_runtime_param_file(self):
        pipeline_name = "print_params"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name,  existing_nf_file_path=THIS_DIR / "nextflow/print_param_nextflow_params_file/main.nf")
        applet_id = json.loads(run(
            "dx build --nextflow '{}' --json".format(applet_dir)))["id"]

        # Run with "dx run".
        params_file = dxpy.upload_local_file(THIS_DIR / "nextflow/print_param_nextflow_params_file/params_file.yml", project=self.project, wait_on_close=True).get_id()

        job_id = run(
            "dx run {applet_id} -idebug=true -inextflow_params_file={params_file} -inextflow_pipeline_params=\"--BETA 'CLI beta'\" --brief -y".format(
                applet_id=applet_id, params_file=params_file)
        ).strip()
        job_handler = dxpy.DXJob(job_id)
        job_handler.wait_on_done()
        watched_run_output = run("dx watch {} --no-follow".format(job_id))

        self.assertTrue("-params-file /home/dnanexus/in/nextflow_params_file/params_file.yml" in watched_run_output)
        # precedence of the input parameter values: nextflow_params_file < nextflow_pipeline_params < other applet runtime inputs parsed from nextflow schema
        self.assertTrue("The parameter ALPHA is: param file alpha" in watched_run_output)
        self.assertTrue("The parameter BETA is: CLI beta" in watched_run_output)

if __name__ == '__main__':
    if 'DXTEST_FULL' not in os.environ:
        sys.stderr.write(
            'WARNING: env var DXTEST_FULL is not set; tests that create apps or run jobs will not be run\n')
    unittest.main()

