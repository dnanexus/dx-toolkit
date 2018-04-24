#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2016 DNAnexus, Inc.
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

import os
import unittest
import tempfile
import json
import sys
import shutil
import subprocess
import pytest

import dxpy
import dxpy_testutil as testutil
from dxpy_testutil import (DXTestCase, check_output, override_environment, chdir)


def run(command, **kwargs):
    print("$ %s" % (command,))
    output = check_output(command, shell=True, **kwargs)
    print(output)
    return output


class TestDXBuildAsset(DXTestCase):
    def setUp(self):
        super(TestDXBuildAsset, self).setUp()
        self.temp_file_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_file_path)
        super(TestDXBuildAsset, self).tearDown()

    def write_asset_directory(self, asset_name, dxasset_str, asset_rsc_dir=None, makefile_str=None):
        # Note: if called twice with the same asset_name, will overwrite
        # the dxasset.json
        try:
            os.mkdir(os.path.join(self.temp_file_path, asset_name))
        except OSError as e:
            if e.errno != 17:
                raise e
        if dxasset_str is not None:
            with open(os.path.join(self.temp_file_path, asset_name, 'dxasset.json'), 'wb') as manifest:
                manifest.write(dxasset_str.encode())
        if asset_rsc_dir:
            try:
                os.mkdir(os.path.join(self.temp_file_path, asset_name, asset_rsc_dir))
            except OSError as e:
                if e.errno != 17:
                    raise e
        if makefile_str is not None:
            with open(os.path.join(self.temp_file_path, asset_name, 'Makefile'), 'wb') as manifest:
                manifest.write(makefile_str.encode())

        return os.path.join(self.temp_file_path, asset_name)

    def write_app_directory(self, app_name, dxapp_str, code_filename=None, code_content="\n"):
        # Note: if called twice with the same app_name, will overwrite
        # the dxapp.json and code file (if specified) but will not
        # remove any other files that happened to be present
        try:
            os.mkdir(os.path.join(self.temp_file_path, app_name))
        except OSError as e:
            if e.errno != 17:  # directory already exists
                raise e
        if dxapp_str is not None:
            with open(os.path.join(self.temp_file_path, app_name, 'dxapp.json'), 'wb') as manifest:
                manifest.write(dxapp_str.encode())
        if code_filename:
            with open(os.path.join(self.temp_file_path, app_name, code_filename), 'w') as code_file:
                code_file.write(code_content)
        return os.path.join(self.temp_file_path, app_name)

    def test_build_asset_help(self):
        env = override_environment(DX_SECURITY_CONTEXT=None, DX_APISERVER_HOST=None,
                                   DX_APISERVER_PORT=None, DX_APISERVER_PROTOCOL=None)
        run("dx build_asset -h", env=env)

    def test_build_asset_with_no_dxasset_json(self):
        asset_dir = self.write_asset_directory("asset_with_no_json", None)
        with self.assertSubprocessFailure(stderr_regexp='is not a valid DNAnexus asset source directory',
                                          exit_code=1):
            run("dx build_asset " + asset_dir)

    def test_build_asset_with_malformed_dxasset_json(self):
        asset_dir = self.write_asset_directory("asset_with_malform_json", "{")
        with self.assertSubprocessFailure(stderr_regexp='Could not parse dxasset\.json', exit_code=1):
            run("dx build_asset " + asset_dir)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_DATA_OBJ_BUILD_ASSET"])
    def test_build_asset_with_valid_dxasset(self):
        asset_spec = {
            "name": "asset_library_name",
            "title": "A human readable name",
            "description": "A detailed description about the asset",
            "version": "0.0.1",
            "distribution": "Ubuntu",
            "release": "12.04",
            "instanceType": "mem1_ssd1_x2",
            "execDepends": [{"name": "python-numpy"}]
        }
        asset_dir = self.write_asset_directory("asset_with_valid_json", json.dumps(asset_spec))
        asset_bundle_id = json.loads(run('dx build_asset --json ' + asset_dir))['id']
        self.assertIn('record', asset_bundle_id)
        self.assertEqual(dxpy.describe(asset_bundle_id)['project'], self.project)
        job_id = dxpy.describe(asset_bundle_id)['createdBy']['job']
        self.assertEqual(dxpy.describe(job_id)['instanceType'], "mem1_ssd1_x2")

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    def test_build_asset_with_valid_destination(self):
        asset_spec = {
            "name": "foo",
            "title": "A human readable name",
            "description": "A detailed description about the asset",
            "version": "0.0.1",
            "distribution": "Ubuntu",
            "release": "12.04",
            "execDepends": [{"name": "python-numpy"}]
        }
        asset_dir = self.write_asset_directory("asset_with_valid_destination", json.dumps(asset_spec))
        with testutil.temporary_project() as other_project:
            test_dirname = 'asset_dir'
            run('dx mkdir -p {project}:{dirname}'.format(project=other_project.get_id(), dirname=test_dirname))
            asset_bundle_id = json.loads(run('dx build_asset --json --destination ' + other_project.get_id() +
                                             ':/' + test_dirname + '/ ' + asset_dir))['id']
            self.assertIn('record', asset_bundle_id)
            asset_desc = dxpy.describe(asset_bundle_id)
            self.assertEqual(asset_desc['project'], other_project.get_id())
            self.assertEqual(asset_desc['folder'], '/asset_dir')

    def test_build_asset_invalid_destination(self):
        asset_spec = {
            "name": "asset_library_name",
            "title": "A human readable name",
            "description": "A detailed description about the asset",
            "version": "0.0.1",
            "distribution": "Ubuntu",
            "release": "12.04"
        }
        asset_dir = self.write_asset_directory("asset_with_invalid_destination", json.dumps(asset_spec))
        with self.assertSubprocessFailure(stderr_regexp='Could not find a project named', exit_code=3):
            run("dx build_asset -d nonexistent_project:/new-name " + asset_dir)

    def test_build_asset_missing_fields(self):
        asset_spec = {
            "title": "A human readable name",
            "description": "A detailed description about the asset",
            "version": "0.0.1",
            "distribution": "Ubuntu",
            "release": "12.04"
        }
        asset_dir = self.write_asset_directory("asset_with_missing_fields", json.dumps(asset_spec))
        with self.assertSubprocessFailure(stderr_regexp='The asset configuration does not contain', exit_code=1):
            run("dx build_asset " + asset_dir)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    def test_build_asset_with_resources(self):
        asset_spec = {
            "name": "asset_library_name",
            "title": "A human readable name",
            "description": "A detailed description about the asset",
            "version": "0.0.1",
            "distribution": "Ubuntu",
            "release": "12.04"
        }
        asset_dir = self.write_asset_directory("asset_with_resources", json.dumps(asset_spec), "resources")
        asset_bundle_id = json.loads(run('dx build_asset --json ' + asset_dir))['id']
        self.assertIn('record', asset_bundle_id)
        self.assertEqual(dxpy.describe(asset_bundle_id)['project'], self.project)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    def test_build_asset_with_invalid_makefile(self):
        asset_spec = {
            "name": "asset_library_name",
            "title": "A human readable name",
            "description": "A detailed description about the asset",
            "version": "0.0.1",
            "distribution": "Ubuntu",
            "release": "12.04"
        }
        asset_dir = self.write_asset_directory("asset_with_invalid_makefile", json.dumps(asset_spec),
                                               None, "echo")
        # TODO exit_code=3
        with self.assertSubprocessFailure(stderr_regexp='', exit_code=1):
            run("dx build_asset --json " + asset_dir)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    def test_build_and_use_asset(self):
        asset_spec = {
            "name": "asset library name with space",
            "title": "A human readable name",
            "description": "A detailed description about the asset",
            "version": "0.0.1",
            "distribution": "Ubuntu",
            "release": "12.04"
        }
        asset_dir = self.write_asset_directory("build_and_use_asset", json.dumps(asset_spec), "resources")

        run("mkdir -p " + os.path.join(asset_dir, "resources/usr/local/bin"))
        with open(os.path.join(asset_dir, "resources/usr/local/bin", 'test.sh'), 'wb') as manifest:
            manifest.write("echo 'hi'".encode())
        run("chmod +x " + os.path.join(asset_dir, "resources/usr/local/bin", 'test.sh'))

        asset_bundle_id = json.loads(run('dx build_asset --json ' + asset_dir))['id']
        code_str = """#!/bin/bash
                    main(){
                        test.sh
                    }
                    """
        app_spec = {
            "name": "asset_depends",
            "dxapi": "1.0.0",
            "runSpec": {
                "code": code_str,
                "interpreter": "bash",
                "distribution": "Ubuntu",
                "release": "14.04",
                "assetDepends":  [{"id": asset_bundle_id}]
            },
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
        }
        app_dir = self.write_app_directory("asset_depends", json.dumps(app_spec))
        asset_applet_id = json.loads(run("dx build --json {app_dir}".format(app_dir=app_dir)))["id"]
        asset_applet = dxpy.DXApplet(asset_applet_id)
        applet_job = asset_applet.run({})
        applet_job.wait_on_done()
        self.assertEqual(applet_job.describe()['state'], 'done')

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    def test_build_asset_with_invalid_instance_type(self):
        asset_spec = {
            "name": "asset_library_name",
            "title": "A human readable name",
            "description": "A detailed description about the asset",
            "version": "0.0.1",
            "distribution": "Ubuntu",
            "release": "12.04",
            "instanceType": "mem1_ssd1_x3"
        }
        asset_dir = self.write_asset_directory("build_asset_with_invalid_instance_type", json.dumps(asset_spec))
        # TODO exit_code=3
        with self.assertSubprocessFailure(stderr_regexp='code 422', exit_code=1):
            run("dx build_asset --json " + asset_dir)

    @unittest.skipUnless(testutil.TEST_ONLY_MASTER and testutil.TEST_RUN_JOBS,
                         'skipping test that requires latest server version')
    def test_build_asset_inside_job(self):
        asset_spec = {
            "name": "asset library name with space",
            "title": "A human readable name",
            "description": " A detailed description about the asset",
            "version": "0.0.1",
            "distribution": "Ubuntu",
            "release": "12.04"
        }
        asset_dir = self.write_asset_directory("test_build_asset_inside_job", json.dumps(asset_spec))
        asset_conf_file_id = run("dx upload " + os.path.join(asset_dir, "dxasset.json") + " --brief --wait").strip()
        code_str = """#!/bin/bash
                    main(){
                        dx download "${asset_conf}" -o dxasset.json
                        dx build_asset
                    }
                    """
        app_spec = {
            "name": "run_build_asset",
            "dxapi": "1.0.0",
            "runSpec": {
                "code": code_str,
                "interpreter": "bash",
                "distribution": "Ubuntu",
                "release": "14.04"
            },
            "inputSpec": [{"name": "asset_conf", "class": "file"}],
            "outputSpec": [],
            "version": "1.0.0"
        }
        app_dir = self.write_app_directory("run_build_asset", json.dumps(app_spec))
        asset_applet_id = json.loads(run("dx build --json {app_dir}".format(app_dir=app_dir)))["id"]

        asset_applet = dxpy.DXApplet(asset_applet_id)
        applet_job = asset_applet.run({"asset_conf": {"$dnanexus_link": asset_conf_file_id}})
        applet_job.wait_on_done()
        self.assertEqual(applet_job.describe()['state'], 'done')

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    def test_get_appet_with_asset(self):
        bundle_name = "test-bundle-depends.tar.gz"
        bundle_tmp_dir = tempfile.mkdtemp()
        os.mkdir(os.path.join(bundle_tmp_dir, "a"))
        with open(os.path.join(bundle_tmp_dir, 'a', 'foo.txt'), 'w') as file_in_bundle:
            file_in_bundle.write('foo\n')
        subprocess.check_call(['tar', '-czf', os.path.join(bundle_tmp_dir, bundle_name),
                               '-C', os.path.join(bundle_tmp_dir, 'a'), '.'])
        bundle_file = dxpy.upload_local_file(filename=os.path.join(bundle_tmp_dir, bundle_name),
                                             project=self.project,
                                             wait_on_close=True)

        asset_file = dxpy.upload_local_file(filename=os.path.join(bundle_tmp_dir, bundle_name),
                                            project=self.project,
                                            wait_on_close=True)

        dxrecord_details = {"archiveFileId": {"$dnanexus_link": asset_file.get_id()}}
        dxrecord = dxpy.new_dxrecord(project=self.project, types=["AssetBundle"], details=dxrecord_details,
                                     name='asset-lib-test', properties={"version": "0.0.1"})
        dxrecord.close()
        asset_bundle_id = dxrecord.get_id()

        asset_file.set_properties({"AssetBundle": asset_bundle_id})

        code_str = """#!/bin/bash
                    main(){
                        echo 'Hello World'
                    }
                    """
        app_spec = {
            "name": "asset_depends",
            "dxapi": "1.0.0",
            "runSpec": {
                "code": code_str,
                "interpreter": "bash",
                "distribution": "Ubuntu",
                "release": "14.04",
                "assetDepends":  [{"id": asset_bundle_id}],
                "bundledDepends": [{"name": bundle_name, "id": {"$dnanexus_link": bundle_file.get_id()}}]
            },
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
        }
        app_dir = self.write_app_directory("asset_depends", json.dumps(app_spec))
        asset_applet_id = json.loads(run("dx build --json {app_dir}".format(app_dir=app_dir)))["id"]
        with chdir(tempfile.mkdtemp()):
            run("dx get --omit-resources " + asset_applet_id)
            self.assertTrue(os.path.exists("asset_depends"))
            self.assertFalse(os.path.exists(os.path.join("asset_depends", "resources")))
            self.assertTrue(os.path.exists(os.path.join("asset_depends", "dxapp.json")))

            applet_spec = json.load(open(os.path.join("asset_depends", "dxapp.json")))
            self.assertEqual([{"name": "asset-lib-test",
                               "project": self.project,
                               "folder": "/",
                               "version": "0.0.1"}
                              ],
                             applet_spec["runSpec"]["assetDepends"])
            self.assertEqual([{"name": bundle_name, "id": {"$dnanexus_link": bundle_file.get_id()}}],
                             applet_spec["runSpec"]["bundledDepends"])

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    def test_set_assetbundle_tarball_property(self):
        asset_spec = {
            "name": "tarball_property_assetbundle",
            "title": "A human readable name",
            "description": "A detailed description about the asset",
            "version": "0.0.1",
            "distribution": "Ubuntu",
            "release": "12.04"
        }
        asset_dir = self.write_asset_directory("set_tarball_property", json.dumps(asset_spec))
        asset_bundle_id = json.loads(run('dx build_asset --json ' + asset_dir))['id']
        self.assertIn('record', asset_bundle_id)
        tarball_file_id = dxpy.describe(asset_bundle_id,
                                        fields={"details"})["details"]["archiveFileId"]["$dnanexus_link"]
        self.assertEqual(dxpy.describe(tarball_file_id,
                                       fields={"properties"})["properties"]["AssetBundle"], asset_bundle_id)

if __name__ == '__main__':
    if dxpy.AUTH_HELPER is None:
        sys.exit(1, 'Error: Need to be logged in to run these tests')
    if 'DXTEST_FULL' not in os.environ:
        if 'DXTEST_RUN_JOBS' not in os.environ:
            sys.stderr.write('WARNING: neither env var DXTEST_FULL nor DXTEST_RUN_JOBS are set; \
            tests that run jobs will not be run\n')
    unittest.main()
