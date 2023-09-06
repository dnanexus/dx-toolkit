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

from parameterized import parameterized
from dxpy_testutil import DXTestCase
from dxpy import DXFile
from dxpy.compat import USING_PYTHON2
from dxpy.nextflow.ImageRefParser import ImageRefParser, DxPathParser, DockerImageParser, ImageRefParserFactory

if USING_PYTHON2:
    spawn_extra_args = {}
else:
    # Python 3 requires specifying the encoding
    spawn_extra_args = {"encoding": "utf-8"}


class TestImageRefParser(DXTestCase):

    # for dx path
    fixture_1 = ['dx:///alpha/beta', None, None, '/alpha/beta']
    fixture_2 = ['dx://project-123:/alpha/beta', 'project-123', 'project-123', '/alpha/beta']
    fixture_3 = ['dx://project-123:alpha/beta', 'project-123', 'project-123', 'alpha/beta']
    fixture_4 = ['dx://project-123:/', 'project-123', 'project-123', '/']
    fixture_5 = ['dx://project-123:/some/path/*_{1,2}.fq', 'project-123', 'project-123', '/some/path/*_{1,2}.fq']
    fixture_6 = ['dx://hola:/', 'hola', 'hola', '/']
    fixture_7 = ['dx://hola:', 'hola', 'hola', '']
    fixture_8 = ['dx://hola:', 'hola', 'hola', '']

    # for docker image
    fixture_9 = ['myregistryhost:5000/fedora/httpd:version1.0', 'myregistryhost:5000/fedora/', 'httpd', 'version1.0', '']
    fixture_10 = ['fedora/httpd:version1.0-alpha', 'fedora/', 'httpd', 'version1.0-alpha', '']
    fixture_11 = ['fedora/httpd:version1.0', 'fedora/', 'httpd', 'version1.0', '']
    fixture_12 = ['rabbit:3', '', 'rabbit', '3', '']
    fixture_13 = ['rabbit', '', 'rabbit', '', '']
    fixture_14 = ['repository/rabbit:3', 'repository/', 'rabbit', '3', '']
    fixture_15 = ['repository/rabbit', 'repository/', 'rabbit', '', '']
    fixture_16 = ['rabbit@sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d', '', 'rabbit', '', 'sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d']
    fixture_17 = ['repository/rabbit@sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d', 'repository/', 'rabbit', '', 'sha256:974219f34a18afde9517b27f3b81403c3a08f6908cbf8d7b717097b93b11583d']

    @parameterized.expand([
        fixture_1,
        fixture_2,
        fixture_3,
        fixture_4,
        fixture_5,
        fixture_6,
        fixture_7,
        fixture_8
    ])
    def test_DxPathParser(self, image_ref, name, context_id, file_path):
        dx_path_parser = ImageRefParserFactory.parse(image_ref)
        self.assertTrue(isinstance(dx_path_parser, DxPathParser))
        self.assertTrue(dx_path_parser.name == name)
        self.assertTrue(dx_path_parser.context_id == context_id)
        self.assertTrue(dx_path_parser.file_path == file_path)

    @parameterized.expand([
        fixture_9,
        fixture_10,
        fixture_11,
        fixture_12,
        fixture_13,
        fixture_14,
        fixture_15,
        fixture_16,
        fixture_17
    ])
    def test_DockerImageParser(self, image_ref, repository, image, tag, digest):
        dx_path_parser = ImageRefParserFactory.parse(image_ref)
        self.assertTrue(isinstance(dx_path_parser, DockerImageParser))
        self.assertTrue(dx_path_parser.repository == repository)
        self.assertTrue(dx_path_parser.image == image)
        self.assertTrue(dx_path_parser.tag == tag)
        self.assertTrue(dx_path_parser.digest == digest)
