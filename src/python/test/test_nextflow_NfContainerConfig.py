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

from dxpy_testutil import DXTestCase
from dxpy.compat import USING_PYTHON2
from dxpy.nextflow.NfContainerConfig import NfConfigFile
from dxpy.nextflow.DockerImageRef import DockerImageRef

if USING_PYTHON2:
    spawn_extra_args = {}
else:
    # Python 3 requires specifying the encoding
    spawn_extra_args = {"encoding": "utf-8"}


class TestNfConfigFile(DXTestCase):

    def test__extract_docker_refs_from_src(self):
        pipeline_name = "profile_with_docker"

        config_file_url = os.path.join("nextflow", pipeline_name, "nextflow.config")
        nf_config = NfConfigFile(config_file_url)
        _ = nf_config._extract_docker_refs_from_src()

        self.assertTrue(len(nf_config.image_refs) == 2)
        self.assertTrue(all(isinstance(x, DockerImageRef) for x in nf_config.image_refs))
