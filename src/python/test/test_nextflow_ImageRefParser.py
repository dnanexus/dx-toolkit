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
from dxpy.nextflow.ImageRefParser import DxPathParser, DockerImageParser, ImageRefParserFactory

if USING_PYTHON2:
    spawn_extra_args = {}
else:
    # Python 3 requires specifying the encoding
    spawn_extra_args = {"encoding": "utf-8"}


class TestImageRefParser(DXTestCase):

    @parameterized.expand([
        ['dx:///alpha/beta', None, None, '/alpha/beta'],
        ['dx://project-123:/alpha/beta', 'project-123', 'project-123', '/alpha/beta'],
        ['dx://project-123:alpha/beta', 'project-123', 'project-123', 'alpha/beta'],
        ['dx://project-123:/', 'project-123', 'project-123', '/'],
        ['dx://project-123:/some/path/*_{1,2}.fq', 'project-123', 'project-123', '/some/path/*_{1,2}.fq'],
        ['dx://hola:/', 'hola', 'hola', '/'],
        ['dx://hola:', 'hola', 'hola', ''],
        ['dx://hola:', 'hola', 'hola', '']
    ])
    @unittest.skipIf(USING_PYTHON2,
        'Skipping Python 3 code')
    def test_DxPathParser(self, image_ref, name, context_id, file_path):
        dx_path_parser = ImageRefParserFactory(image_ref)
        tokens = dx_path_parser.parse
        self.assertTrue(isinstance(tokens, DxPathParser))
        self.assertTrue(tokens.name == name)
        self.assertTrue(tokens.context_id == context_id)
        self.assertTrue(tokens.file_path == file_path)

    @parameterized.expand([
        ['myregistryhost:5000/fedora/httpd:version1.0', 'myregistryhost:5000/fedora/', 'httpd', 'version1.0', ''],
        ['fedora/httpd:version1.0-alpha', 'fedora/', 'httpd', 'version1.0-alpha', ''],
        ['fedora/httpd:version1.0', 'fedora/', 'httpd', 'version1.0', ''],
        ['rabbit:3', '', 'rabbit', '3', ''],
        ['rabbit', '', 'rabbit', '', ''],
        ['repository/rabbit:3', 'repository/', 'rabbit', '3', ''],
        ['repository/rabbit', 'repository/', 'rabbit', '', ''],
        ['rabbit@sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d', '', 'rabbit', '', 'sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d'],
        ['repository/rabbit@sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d', 'repository/', 'rabbit', '', 'sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d']
    ])
    @unittest.skipIf(USING_PYTHON2,
        'Skipping Python 3 code')
    def test_DockerImageParser(self, image_ref, repository, image, tag, digest):
        docker_parser = ImageRefParserFactory(image_ref)
        tokens = docker_parser.parse
        self.assertTrue(isinstance(tokens, DockerImageParser))
        self.assertTrue(tokens.repository == repository)
        self.assertTrue(tokens.image == image)
        self.assertTrue(tokens.tag == tag)
        self.assertTrue(tokens.digest == digest)


if __name__ == '__main__':
    if 'DXTEST_FULL' not in os.environ:
        sys.stderr.write(
            'WARNING: env var DXTEST_FULL is not set; tests that create apps or run jobs will not be run\n')
    unittest.main()
