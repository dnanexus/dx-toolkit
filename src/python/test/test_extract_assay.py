#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 DNAnexus, Inc.
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

import unittest
import tempfile
import dxpy
import os
import subprocess
from dxpy_testutil import cd, chdir

test_record = "project-G9j1pX00vGPzF2XQ7843k2Jq:record-GQGF8x80qYFQxv7gz49ZP7Y7"
test_filter_directory = "/Users/jmulka@dnanexus.com/Development/dx-toolkit/src/python/test/CLIGAM_tests/test_input/unit_tests"


class TestDXExtractAssay(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        proj_name = "dx-toolkit_test_data"
        proj_id = list(
            dxpy.find_projects(describe=False, level="VIEW", name=proj_name)
        )[0]["id"]
        cd(proj_id + ":/")

    def test_allele_01(self):
        filter_path = os.join(test_filter_directory, "allele_01.json")
        command = (
            "dx extract_assay germline {} --retrieve-allele {} --output {}".format(
                test_record,
                filter_path,
            )
        )


if __name__ == "__main__":
    unittest.main()
