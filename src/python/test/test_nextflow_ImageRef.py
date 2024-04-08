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
import sys
import unittest

from parameterized import parameterized
from dxpy_testutil import DXTestCase, TEST_NF_DOCKER
from dxpy.nextflow.ImageRef import ImageRef, DockerImageRef

class TestImageRef(DXTestCase):

    @parameterized.expand([
        ["proc1", "sha256aasdfadfadfafddasfdsfa"]
    ])
    def test_ImageRef_cache(self, process, digest):
        image_ref = ImageRef(process, digest)
        with self.assertRaises(NotImplementedError) as err:
            _ = image_ref._cache("file_name")
            self.assertEqual(
                err.exception,
                "Abstract class. Method not implemented. Use the concrete implementations."
            )


    @parameterized.expand([
        ["proc1", "sha256:3fbc632167424a6d997e74f52b878d7cc478225cffac6bc977eedfe51c7f4e79", "busybox", "1.36"]
    ])
    @unittest.skipUnless(TEST_NF_DOCKER,
                         'skipping tests that require docker')
    def test_DockerImageRef_cache(self, process, digest, image_name, tag):
        image_ref = DockerImageRef(process=process, digest=digest, image_name=image_name, tag=tag)
        bundle_dx_file_id = image_ref.bundled_depends
        self.assertEqual(
            bundle_dx_file_id,
            {
                "name": "busybox_1.36",
                "id": {"$dnanexus_link": image_ref._dx_file_id}
            }
        )


if __name__ == '__main__':
    if 'DXTEST_FULL' not in os.environ:
        sys.stderr.write(
            'WARNING: env var DXTEST_FULL is not set; tests that create apps or run jobs will not be run\n')
    unittest.main()
