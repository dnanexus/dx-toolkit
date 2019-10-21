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

import os
import unittest
import shutil
import random
import time
import dxpy
from dxpy_testutil import (DXTestCase, temporary_project, run)
import dxpy_testutil as testutil
import pytest

CACHE_DIR = '/tmp/dx-docker-cache'


def create_file_in_project(fname, trg_proj_id, folder=None):
    data = "foo"
    if folder is None:
        dxfile = dxpy.upload_string(data, name=fname, project=trg_proj_id, wait_on_close=True)
    else:
        dxfile = dxpy.upload_string(data, name=fname, project=trg_proj_id, folder=folder, wait_on_close=True)
    return dxfile.get_id()


def create_project():
    project_name = "test_dx_cp_" + str(random.randint(0, 1000000)) + "_" + str(int(time.time() * 1000))
    return dxpy.api.project_new({'name': project_name})['id']


def rm_project(proj_id):
    dxpy.api.project_destroy(proj_id, {"terminateJobs": True})


def create_folder_in_project(proj_id, path):
    dxpy.api.project_new_folder(proj_id, {"folder": path})


@unittest.skipUnless(testutil.TEST_DX_DOCKER,
                    'skipping tests that would run dx-docker')
class TestDXDocker(DXTestCase):

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(CACHE_DIR)

    @classmethod
    def setUpClass(cls):
        run("docker pull ubuntu:14.04")
        run("docker pull busybox")

    def test_dx_docker_pull(self):
        run("dx-docker pull ubuntu:14.04")
        self.assertTrue(os.path.isfile(os.path.join(CACHE_DIR, 'ubuntu%3A14.04.aci')))
        run("dx-docker pull ubuntu:15.04")
        self.assertTrue(os.path.isfile(os.path.join(CACHE_DIR, 'ubuntu%3A15.04.aci')))

    def test_dx_docker_pull_silent(self):
        dx_docker_out = run("dx-docker pull -q busybox").strip()
        self.assertEqual(dx_docker_out, '')

    def test_dx_docker_pull_hash_or_not(self):
        run("dx-docker pull dnanexus/testdocker")
        self.assertTrue(os.path.isfile(os.path.join(CACHE_DIR, 'dnanexus%2Ftestdocker.aci')))
        repo = "dnanexus/testdocker@sha256:4f983c07e762f5afadf9c45ccd6a557e1a414460e769676826b01c99c4ccb1cb"
        run("dx-docker pull {}".format(repo))
        sanit='dnanexus%2Ftestdocker%40sha256%3A4f983c07e762f5afadf9c45ccd6a557e1a414460e769676826b01c99c4ccb1cb.aci'
        self.assertTrue(os.path.isfile(os.path.join(CACHE_DIR, sanit)))

    def test_dx_docker_pull_failure(self):
        with self.assertSubprocessFailure(exit_code=1, stderr_regexp='Failed to obtain image'):
            run("dx-docker pull busyboxasdf")

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_APP_RUN_DOCKER_CONTAINERS"])
    def test_dx_docker_basic_commands(self):
        run("dx-docker run ubuntu:14.04 ls --color")
        run("dx-docker run ubuntu:15.04 ls")

    def test_dx_docker_run_from_hash(self):
        repo = "dnanexus/testdocker@sha256:4f983c07e762f5afadf9c45ccd6a557e1a414460e769676826b01c99c4ccb1cb"
        run("dx-docker run {}".format(repo))

    def test_dx_docker_run_error_codes(self):
        with self.assertSubprocessFailure(exit_code=1):
            run("dx-docker run ubuntu:14.04 false")
        run("dx-docker run ubuntu:14.04 true")

    def test_dx_docker_volume(self):
        os.makedirs('dxdtestdata')
        run("dx-docker run -v dxdtestdata:/data-host ubuntu:14.04 touch /data-host/newfile.txt")
        self.assertTrue(os.path.isfile(os.path.join('dxdtestdata', 'newfile.txt')))
        shutil.rmtree('dxdtestdata')

    def test_dx_docker_entrypoint_cmd(self):
        docker_out = run("docker run dnanexus/testdocker /bin")
        dx_docker_out = run("dx-docker run -q dnanexus/testdocker /bin")
        self.assertEqual(docker_out, dx_docker_out)

    def test_dx_docker_home_dir(self):
        run("dx-docker run julia:0.5.0 julia -E 'println(\"hello world\")'")

    def test_dx_docker_run_rm(self):
        run("dx-docker run --rm ubuntu ls")

    def test_dx_docker_set_env(self):
        dx_docker_out = run("dx-docker run --env HOME=/somethingelse busybox env")
        self.assertTrue(dx_docker_out.find("HOME=/somethingelse") != -1)

    def test_dx_docker_add_to_applet(self):
        os.makedirs('tmpapp')
        run("docker pull busybox")
        with self.assertSubprocessFailure(exit_code=1, stderr_regexp='does not appear to have a dxapp.json that parses'):
            run("dx-docker add-to-applet busybox tmpapp")
        with open('tmpapp/dxapp.json', 'w') as dxapp:
            dxapp.write("[]")
        run("dx-docker add-to-applet busybox tmpapp")
        self.assertTrue(os.path.isfile(os.path.join('tmpapp', 'resources/tmp/dx-docker-cache/busybox.aci')))
        shutil.rmtree('tmpapp')

    def test_dx_docker_create_asset(self):
        with temporary_project(select=True) as temp_project:
            test_projectid = temp_project.get_id()
            run("docker pull ubuntu:14.04")
            run("dx-docker create-asset ubuntu:14.04")
            self.assertEqual(run("dx ls ubuntu\\\\:14.04").strip(), 'ubuntu:14.04')

            create_folder_in_project(test_projectid, '/testfolder')
            run("dx-docker create-asset busybox -o testfolder")

            ls_out = run("dx ls /testfolder").strip()
            self.assertEqual(ls_out, 'busybox')

            ls_out = run("dx ls testfolder\\/busybox.tar.gz").strip()
            self.assertEqual(ls_out, 'busybox.tar.gz')

    def test_dx_docker_create_asset_with_short_imageid(self):
        with temporary_project(select=True) as temp_project:
            test_projectid = temp_project.get_id()
            run("docker pull ubuntu:14.04")
            short_id = run("docker images -q ubuntu:14.04").strip()
            create_folder_in_project(test_projectid, '/testfolder')
            run("dx-docker create-asset {short_id} -o testfolder".format(short_id=short_id))
            ls_out = run("dx ls /testfolder").strip()
            self.assertEqual(ls_out, short_id)

    def test_dx_docker_create_asset_with_long_imageid(self):
        with temporary_project(select=True) as temp_project:
            test_projectid = temp_project.get_id()
            run("docker pull ubuntu:14.04")
            long_id = run("docker images --no-trunc -q ubuntu:14.04").strip()
            create_folder_in_project(test_projectid, '/testfolder')
            run("dx-docker create-asset {long_id} -o testfolder".format(long_id=long_id))
            ls_out = run("dx ls /testfolder").strip()
            self.assertEqual(ls_out, long_id)

    def test_dx_docker_create_asset_with_image_digest(self):
        with temporary_project(select=True) as temp_project:
            test_projectid = temp_project.get_id()
            run("docker pull ubuntu:14.04")
            create_folder_in_project(test_projectid, '/testfolder')
            image_digest = run("docker inspect ubuntu:14.04 | jq -r '.[] | .RepoDigests[0]'").strip()
            run("dx-docker create-asset {image_digest} -o testfolder".format(image_digest=image_digest))
            ls_out = run("dx ls /testfolder").strip()
            self.assertEqual(ls_out, image_digest)

    def test_dx_docker_additional_container(self):
        run("dx-docker run busybox ls")

    def test_dx_docker_working_dir_override(self):
        run("dx-docker run -v $PWD:/tmp -w /tmp busybox ls")

    def test_complex_quote(self):
        run('dx-docker run python:2-slim /bin/sh -c "echo \'{"foo": {"location": "file:///"}}\' > /dev/stdout"')
