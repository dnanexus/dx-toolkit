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
import unittest.mock
import json
import dxpy.exceptions
from dxpy.nextflow.nextflow_templates import get_nextflow_src, get_nextflow_dxapp
from dxpy.nextflow.nextflow_utils import get_template_dir, resolve_version
from dxpy.nextflow.nextflow_builder import _npi_supports_version_selection
from dxpy.nextflow.collect_images import bundle_docker_images

import uuid
from dxpy_testutil import (DXTestCase, DXTestCaseBuildNextflowApps, run, chdir)
import dxpy_testutil as testutil
import dxpy
from dxpy.nextflow.nextflow_builder import prepare_custom_inputs
from dxpy.nextflow.nextflow_utils import find_readme
from pathlib import Path

spawn_extra_args = {"encoding": "utf-8"}

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


class TestNextflowVersionResolution(unittest.TestCase):

    def test_resolve_version_default(self):
        version, config = resolve_version(None)
        self.assertEqual(version, "25.10")
        self.assertEqual(config["status"], "supported")

    def test_resolve_version_explicit(self):
        version, config = resolve_version("25.10")
        self.assertEqual(version, "25.10")
        self.assertEqual(config["nextflow_assets"], "nextflow_assets_25_10.json")

    def test_resolve_version_deprecated_warning(self):
        import io
        captured = io.StringIO()
        old_stderr = sys.stderr
        try:
            sys.stderr = captured
            version, config = resolve_version("24.10")
        finally:
            sys.stderr = old_stderr
        self.assertEqual(version, "24.10")
        self.assertIn("deprecated", captured.getvalue())

    def test_resolve_version_invalid(self):
        from dxpy.exceptions import DXError
        with self.assertRaises(DXError) as ctx:
            resolve_version("99.99")
        self.assertIn("99.99", str(ctx.exception))
        self.assertIn("Available versions", str(ctx.exception))

    @unittest.mock.patch("dxpy.nextflow.nextflow_templates.get_regional_options")
    def test_dxapp_records_nextflow_version(self, mock_regional):
        mock_regional.return_value = {"aws:us-east-1": {}}
        dxapp = get_nextflow_dxapp(nextflow_version="25.10")
        self.assertEqual(dxapp["details"]["nextflowVersion"], "25.10")

    @unittest.mock.patch("dxpy.nextflow.nextflow_templates.get_regional_options")
    def test_dxapp_records_default_nextflow_version(self, mock_regional):
        mock_regional.return_value = {"aws:us-east-1": {}}
        dxapp = get_nextflow_dxapp()
        self.assertEqual(dxapp["details"]["nextflowVersion"], "25.10")

    @unittest.mock.patch("dxpy.nextflow.nextflow_templates.get_regional_options")
    def test_dxapp_threads_default_version_config(self, mock_regional):
        mock_regional.return_value = {"aws:us-east-1": {}}
        get_nextflow_dxapp()
        _, kwargs = mock_regional.call_args
        self.assertEqual(kwargs["version_config"]["nextflow_assets"], "nextflow_assets_25_10.json")
        self.assertEqual(kwargs["version_config"]["nextaur_assets"], "nextaur_assets_25_10.json")
        self.assertEqual(kwargs["version_config"]["awscli_assets"], "awscli_assets_25_10.json")

    @unittest.mock.patch("dxpy.nextflow.nextflow_templates.get_regional_options")
    def test_dxapp_threads_deprecated_version_config(self, mock_regional):
        mock_regional.return_value = {"aws:us-east-1": {}}
        get_nextflow_dxapp(nextflow_version="24.10")
        _, kwargs = mock_regional.call_args
        self.assertEqual(kwargs["version_config"]["nextflow_assets"], "nextflow_assets_24_10.json")
        self.assertEqual(kwargs["version_config"]["nextaur_assets"], "nextaur_assets_24_10.json")
        self.assertEqual(kwargs["version_config"]["awscli_assets"], "awscli_assets_24_10.json")

    @unittest.mock.patch("dxpy.DXApp")
    def test_npi_auto_detect_unsupported(self, mock_app_cls):
        mock_app = unittest.mock.MagicMock()
        mock_app.describe.return_value = {"inputSpec": [{"name": "repository_url"}, {"name": "config_profile"}]}
        mock_app_cls.return_value = mock_app
        self.assertFalse(_npi_supports_version_selection())

    @unittest.mock.patch("dxpy.DXApp")
    def test_npi_auto_detect_supported(self, mock_app_cls):
        mock_app = unittest.mock.MagicMock()
        mock_app.describe.return_value = {"inputSpec": [{"name": "repository_url"}, {"name": "nextflow_version"}]}
        mock_app_cls.return_value = mock_app
        self.assertTrue(_npi_supports_version_selection())

    @unittest.mock.patch("dxpy.DXApp")
    def test_npi_auto_detect_exception(self, mock_app_cls):
        mock_app_cls.side_effect = dxpy.exceptions.DXAPIError({"error": {"type": "NotFound", "message": "not found"}}, 404)
        self.assertFalse(_npi_supports_version_selection())

    @unittest.mock.patch("dxpy.describe")
    def test_get_nextflow_assets_with_version_config(self, mock_describe):
        from dxpy.nextflow.nextflow_utils import get_nextflow_assets
        _, config = resolve_version("25.10")
        nextaur, nextflow, awscli = get_nextflow_assets("aws:us-east-1", version_config=config)
        self.assertTrue(nextaur.startswith("record-"))
        self.assertTrue(nextflow.startswith("record-"))
        self.assertTrue(awscli.startswith("record-"))
        mock_describe.assert_called_once()

    @unittest.mock.patch("dxpy.describe")
    def test_get_nextflow_assets_staging_fallback(self, mock_describe):
        from dxpy.nextflow.nextflow_utils import get_nextflow_assets
        from dxpy.exceptions import ResourceNotFound
        mock_describe.side_effect = ResourceNotFound({"error": {"type": "ResourceNotFound", "message": "not found"}}, 404)
        _, config = resolve_version("25.10")
        # Should fall through to staging files without error
        nextaur, nextflow, awscli = get_nextflow_assets("aws:us-east-1", version_config=config)
        self.assertTrue(nextaur.startswith("record-"))
        self.assertTrue(nextflow.startswith("record-"))
        self.assertTrue(awscli.startswith("record-"))

    def test_get_nextflow_assets_invalid_region(self):
        from dxpy.nextflow.nextflow_utils import get_nextflow_assets
        from dxpy.exceptions import DXError
        _, config = resolve_version("25.10")
        with self.assertRaises(DXError) as ctx:
            get_nextflow_assets("aws:nonexistent-region", version_config=config)
        self.assertIn("nonexistent-region", str(ctx.exception))

    def test_manifest_validation_missing_keys(self):
        from dxpy.nextflow.nextflow_utils import _load_versions_manifest
        from dxpy.exceptions import DXError
        bad_manifest = json.dumps({"foo": "bar"})
        with unittest.mock.patch("builtins.open", unittest.mock.mock_open(read_data=bad_manifest)):
            with self.assertRaises(DXError) as ctx:
                _load_versions_manifest()
            self.assertIn("missing", str(ctx.exception))

    # --- Tier 1: Manifest loading error cases ---

    def test_manifest_file_not_found(self):
        from dxpy.nextflow.nextflow_utils import _load_versions_manifest
        with unittest.mock.patch("builtins.open", side_effect=FileNotFoundError("no such file")):
            with self.assertRaises(dxpy.exceptions.DXCLIError) as ctx:
                _load_versions_manifest()
            self.assertIn("Failed to load", str(ctx.exception))

    def test_manifest_invalid_json(self):
        from dxpy.nextflow.nextflow_utils import _load_versions_manifest
        with unittest.mock.patch("builtins.open", unittest.mock.mock_open(read_data="not json{{")):
            with self.assertRaises(dxpy.exceptions.DXCLIError) as ctx:
                _load_versions_manifest()
            self.assertIn("Failed to load", str(ctx.exception))

    def test_manifest_missing_default_key(self):
        from dxpy.nextflow.nextflow_utils import _load_versions_manifest
        manifest = json.dumps({"versions": {"25.10": {
            "status": "supported", "nextflow_assets": "a.json",
            "nextaur_assets": "b.json", "awscli_assets": "c.json"}}})
        with unittest.mock.patch("builtins.open", unittest.mock.mock_open(read_data=manifest)):
            with self.assertRaises(dxpy.exceptions.DXCLIError) as ctx:
                _load_versions_manifest()
            self.assertIn("missing", str(ctx.exception).lower())

    def test_manifest_missing_versions_key(self):
        from dxpy.nextflow.nextflow_utils import _load_versions_manifest
        manifest = json.dumps({"default": "25.10"})
        with unittest.mock.patch("builtins.open", unittest.mock.mock_open(read_data=manifest)):
            with self.assertRaises(dxpy.exceptions.DXCLIError) as ctx:
                _load_versions_manifest()
            self.assertIn("missing", str(ctx.exception).lower())

    # --- Tier 1: Version resolution edge cases ---

    def test_resolve_version_deprecated_warn_false(self):
        import io
        captured = io.StringIO()
        old_stderr = sys.stderr
        try:
            sys.stderr = captured
            version, config = resolve_version("24.10", warn=False)
        finally:
            sys.stderr = old_stderr
        self.assertEqual(version, "24.10")
        self.assertEqual(captured.getvalue(), "")

    def test_resolve_version_default_misconfigured(self):
        bad_manifest = {
            "default": "99.99",
            "versions": {"25.10": {
                "status": "supported", "nextflow_assets": "a.json",
                "nextaur_assets": "b.json", "awscli_assets": "c.json",
                "cache_digest_type": "config"}}
        }
        with unittest.mock.patch("builtins.open", unittest.mock.mock_open(read_data=json.dumps(bad_manifest))):
            with self.assertRaises(dxpy.exceptions.DXCLIError) as ctx:
                resolve_version(None)
            self.assertIn("misconfigured", str(ctx.exception))

    # --- Tier 1: Asset loading mutual exclusion ---

    def test_get_nextflow_assets_both_params_error(self):
        from dxpy.nextflow.nextflow_utils import get_nextflow_assets
        _, config = resolve_version("25.10")
        with self.assertRaises(dxpy.exceptions.DXCLIError) as ctx:
            get_nextflow_assets("aws:us-east-1", nextflow_version="25.10", version_config=config)
        self.assertIn("not both", str(ctx.exception))

    # --- Tier 2: Staging fallback edge cases ---

    @unittest.mock.patch("dxpy.describe")
    def test_get_nextflow_assets_staging_file_not_found(self, mock_describe):
        from dxpy.nextflow.nextflow_utils import get_nextflow_assets
        from dxpy.exceptions import ResourceNotFound
        mock_describe.side_effect = ResourceNotFound(
            {"error": {"type": "ResourceNotFound", "message": "not found"}}, 404)
        _, config = resolve_version("25.10")
        # Patch open so that staging files raise FileNotFoundError
        original_open = open
        def _mock_open(filepath, *args, **kwargs):
            if "staging" in str(filepath):
                raise FileNotFoundError("staging file missing")
            return original_open(filepath, *args, **kwargs)
        with unittest.mock.patch("builtins.open", side_effect=_mock_open):
            with self.assertRaises(dxpy.exceptions.DXCLIError) as ctx:
                get_nextflow_assets("aws:us-east-1", version_config=config)
            self.assertIn("Staging asset files not found", str(ctx.exception))

    @unittest.mock.patch("dxpy.describe")
    def test_get_nextflow_assets_staging_region_missing(self, mock_describe):
        from dxpy.nextflow.nextflow_utils import get_nextflow_assets
        from dxpy.exceptions import ResourceNotFound
        mock_describe.side_effect = ResourceNotFound(
            {"error": {"type": "ResourceNotFound", "message": "not found"}}, 404)
        _, config = resolve_version("25.10")
        # Staging files exist but won't have "aws:fake-region-99"
        with self.assertRaises(dxpy.exceptions.DXCLIError) as ctx:
            get_nextflow_assets("aws:fake-region-99", version_config=config)
        self.assertIn("fake-region-99", str(ctx.exception))

    # --- Tier 2: NPI auto-detect edge cases ---

    @unittest.mock.patch("dxpy.DXApp")
    def test_npi_auto_detect_empty_inputspec(self, mock_app_cls):
        mock_app = unittest.mock.MagicMock()
        mock_app.describe.return_value = {"inputSpec": []}
        mock_app_cls.return_value = mock_app
        self.assertFalse(_npi_supports_version_selection())

    @unittest.mock.patch("dxpy.DXApp")
    def test_npi_auto_detect_auth_error(self, mock_app_cls):
        mock_app_cls.side_effect = dxpy.exceptions.DXAPIError(
            {"error": {"type": "Unauthorized", "message": "unauthorized"}}, 401)
        self.assertFalse(_npi_supports_version_selection())

    # --- Tier 2: Version-specific asset content verification ---

    @unittest.mock.patch("dxpy.nextflow.nextflow_utils.get_project_with_assets", return_value="project-FAKE")
    @unittest.mock.patch("dxpy.nextflow.nextflow_utils.get_instance_type", return_value="mem1_ssd1_v2_x4")
    @unittest.mock.patch("dxpy.describe")
    def test_regional_options_assets_match_local_files(self, mock_describe, _mock_inst, _mock_proj):
        """For each version in versions.json, verify that get_regional_options
        produces assetDepends record IDs that exactly match the local asset files."""
        from dxpy.nextflow.nextflow_utils import get_regional_options, _load_versions_manifest
        region = "aws:us-east-1"
        manifest = _load_versions_manifest()
        nextflow_basepath = os.path.join(os.path.dirname(dxpy.__file__), "nextflow")

        for ver_key, ver_config in manifest["versions"].items():
            with self.subTest(version=ver_key):
                # Read expected record IDs directly from local asset files
                with open(os.path.join(nextflow_basepath, ver_config["nextaur_assets"])) as f:
                    expected_nextaur = json.load(f)[region]
                with open(os.path.join(nextflow_basepath, ver_config["nextflow_assets"])) as f:
                    expected_nextflow = json.load(f)[region]
                with open(os.path.join(nextflow_basepath, ver_config["awscli_assets"])) as f:
                    expected_awscli = json.load(f)[region]

                result = get_regional_options(
                    region, resources_dir=None, profile=None,
                    cache_docker=False, nextflow_pipeline_params=None,
                    version_config=ver_config,
                )
                asset_depends = result[region]["assetDepends"]
                actual_ids = [entry["id"]["$dnanexus_link"]["id"] for entry in asset_depends]

                self.assertEqual(actual_ids, [expected_nextaur, expected_nextflow, expected_awscli],
                                 f"Version {ver_key}: assetDepends record IDs do not match local asset files")

    # --- Tier 3: Error message quality ---

    def test_invalid_version_error_lists_available(self):
        with self.assertRaises(dxpy.exceptions.DXCLIError) as ctx:
            resolve_version("99.99")
        err_msg = str(ctx.exception)
        self.assertIn("24.10", err_msg)
        self.assertIn("25.10", err_msg)

    def test_deprecated_warning_suggests_default(self):
        import io
        captured = io.StringIO()
        old_stderr = sys.stderr
        try:
            sys.stderr = captured
            resolve_version("24.10")
        finally:
            sys.stderr = old_stderr
        self.assertIn("25.10", captured.getvalue())


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

    def test_src_basic(self):
        src = get_nextflow_src()
        self.assertTrue("#!/usr/bin/env bash" in src)
        self.assertTrue("nextflow" in src)

    def test_src_profile(self):
        src = get_nextflow_src(profile="test_profile")
        self.assertTrue("-profile test_profile" in src)

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
        self.assertEqual(i["help"], "(Nextflow pipeline required) Default value: results_dir. Some description here. out_directory help text")
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
            run("dx build --nextflow . --json"))["id"]
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
        extra_args = '{"name": "name-9Oxvx2tCZe", "title": "Title VsnhPeFBqt", "summary": "Summary 3E7fFfEXdB", "runSpec": {"release": "20.04"}}'
        applet_id = json.loads(run(
            "dx build --nextflow '{}' --json --extra-args '{}'".format(applet_dir, extra_args)))["id"]

        applet = dxpy.DXApplet(applet_id)
        desc = applet.describe()
        self.assertEqual(desc["name"], json.loads(extra_args)["name"])
        self.assertEqual(desc["title"], json.loads(extra_args)["title"])
        self.assertEqual(desc["summary"], json.loads(extra_args)["summary"])
        self.assertEqual(desc["runSpec"]["release"], json.loads(extra_args)["runSpec"]["release"])

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

    def test_dx_build_nextflow_with_default_version(self):
        pipeline_name = "hello_default_ver"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(
            run("dx build --nextflow --json " + applet_dir))["id"]
        applet = dxpy.DXApplet(applet_id)
        details = applet.get_details()
        self.assertEqual(details["nextflowVersion"], "25.10")

    def test_dx_build_nextflow_with_explicit_version(self):
        pipeline_name = "hello_explicit_ver"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        applet_id = json.loads(
            run("dx build --nextflow --nextflow-version 25.10 --json " + applet_dir))["id"]
        applet = dxpy.DXApplet(applet_id)
        details = applet.get_details()
        self.assertEqual(details["nextflowVersion"], "25.10")

    def test_dx_build_nextflow_with_invalid_version(self):
        pipeline_name = "hello_invalid_ver"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        with self.assertSubprocessFailure(stderr_regexp="invalid choice", exit_code=2):
            run("dx build --nextflow --nextflow-version 99.99 --json " + applet_dir)

    def test_dx_build_nextflow_version_without_nextflow_flag(self):
        with self.assertSubprocessFailure(stderr_regexp="--nextflow-version", exit_code=2):
            run("dx build --nextflow-version 25.10 .")

    def test_dx_build_nextflow_with_deprecated_version(self):
        pipeline_name = "hello_deprecated_ver"
        applet_dir = self.write_nextflow_applet_directory(
            pipeline_name, existing_nf_file_path=self.base_nextflow_nf)
        import subprocess
        proc = subprocess.run(
            ["dx", "build", "--nextflow", "--nextflow-version", "24.10",
             "--json", applet_dir],
            capture_output=True, text=True
        )
        self.assertEqual(proc.returncode, 0, f"Build failed: {proc.stderr}")
        self.assertIn("deprecated", proc.stderr)
        applet_json = json.loads(proc.stdout.strip())
        applet = dxpy.DXApplet(applet_json["id"])
        details = applet.get_details()
        self.assertEqual(details["nextflowVersion"], "24.10")

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_build_nextflow_npi_with_version_warning(self):
        hello_repo_url = "https://github.com/nextflow-io/hello"
        import subprocess
        proc = subprocess.run(
            ["dx", "build", "--nextflow", "--nextflow-version", "25.10",
             "--repository", hello_repo_url, "--brief"],
            capture_output=True, text=True
        )
        # NPI may or may not support version selection yet;
        # either the build succeeds, or we get a warning about NPI not supporting it
        if proc.returncode == 0:
            applet_json = json.loads(proc.stdout.strip())
            applet = dxpy.DXApplet(applet_json["id"])
            details = applet.get_details()
            # nextflowVersion is only recorded if the worker's dxpy has multi-version support;
            # if present, verify it's the requested version
            if "nextflowVersion" in details:
                self.assertEqual(details["nextflowVersion"], "25.10")
        else:
            # Build may fail for other reasons (auth, etc.), but should not crash
            self.assertNotIn("Traceback", proc.stderr)


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
        job.describe()
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
        dxpy.describe(applet_id)

        # Run with "dx run".
        dxpy.upload_string("foo", name="foo.txt", folder="/a/b/c",
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

