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
from dxpy_testutil import DXTestCase
from dxpy.compat import USING_PYTHON2
from dxpy.nextflow.ImageRefFactory import ImageRefFactory, ImageRefFactoryError
from dxpy.nextflow.ImageRef import DockerImageRef

if USING_PYTHON2:
    spawn_extra_args = {}
else:
    # Python 3 requires specifying the encoding
    spawn_extra_args = {"encoding": "utf-8"}


class TestImageRef(DXTestCase):
    @parameterized.expand([
        [{"engine": "docker", "process": "proc1", "digest": "sha256aasdfadfadfafddasfdsfa"}]
    ])
    @unittest.skipIf(USING_PYTHON2,
        'Skipping Python 3 code')
    def test_ImageRefFactory(self, image_ref):
        image_ref_factory = ImageRefFactory(image_ref)
        image = image_ref_factory.get_image()
        self.assertTrue(isinstance(image, DockerImageRef))

    @parameterized.expand([
        [{"process": "proc1", "digest": "sha256aasdfadfadfafddasfdsfa"}, "Provide the container engine"],
        [{"engine": "singularity", "process": "proc1", "digest": "sha256aasdfadfadfafddasfdsfa"}, "Unsupported container engine: singularity"]
    ])
    @unittest.skipIf(USING_PYTHON2,
        'Skipping Python 3 code')
    def test_ImageRefFactory_errors(self, image_ref, exception):
        with self.assertRaises(ImageRefFactoryError) as err:
            image_ref_factory = ImageRefFactory(image_ref)
            _ = image_ref_factory.get_image()
            self.assertEqual(err.exception, exception)



if __name__ == '__main__':
    if 'DXTEST_FULL' not in os.environ:
        sys.stderr.write(
            'WARNING: env var DXTEST_FULL is not set; tests that create apps or run jobs will not be run\n')
    unittest.main()
