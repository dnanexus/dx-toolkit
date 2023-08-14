#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023 DNAnexus, Inc.
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

# TODO: update path to test folder
# Run manually with python2 and python3 src/python/test/test_<FEATURE_NAME>.py

import dxpy
import unittest
import os
import sys
import subprocess

from dxpy_testutil import cd
from dxpy.cli.dataset_utilities import (
    get_assay_name_info,
    resolve_validate_record_path,
    DXDataset,
)

# TODO: remove this example and import functions from new feature
from dxpy.dx_extract_utils.somatic_filter_payload import (
    basic_filter,
)

dirname = os.path.dirname(__file__)

python_version = sys.version_info.major


class TestFeatureName(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_project_name = "dx-toolkit_test_data"
        cls.proj_id = list(
            dxpy.find_projects(describe=False, level="VIEW", name=test_project_name)
        )[0]["id"]
        cd(cls.proj_id + ":/")

    def example_e2e_test(self):
        pass

    # An example of a unit tests.  Imports a single function from the code of the feature, and checks its output against
    # a known output
    def example_unit_test(self):
        print("testing basic filter")
        table = "variant_read_optimized"
        friendly_name = "allele_id"
        values = ["chr21_40590995_C_C"]
        project_context = None
        genome_reference = None

        expected_output = {
            "variant_read_optimized$allele_id": [
                {"condition": "in", "values": ["chr21_40590995_C_C"]}
            ]
        }

        # Use unittest's assertEqual function to ensure the test output is the same as the expected output
        self.assertEqual(
            basic_filter(table, friendly_name, values),
            expected_output,
        )


if __name__ == "__main__":
    unittest.main()
