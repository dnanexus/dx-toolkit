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


# Run manually with python3 src/python/test/test_extract_expression.py

import unittest
import subprocess
import sys
import os
import dxpy
import json
import shutil
from dxpy_testutil import cd, chdir
from dxpy.bindings.apollo.ValidateJSONbySchema import JSONValidator
from dxpy.bindings.apollo.assay_filtering_json_schemas import (
    EXTRACT_ASSAY_EXPRESSION_JSON_SCHEMA,
)

dirname = os.path.dirname(__file__)

python_version = sys.version_info.major


class TestDXExtractExpression(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_project_name = "dx-toolkit_test_data"
        cls.proj_id = list(
            dxpy.find_projects(describe=False, level="VIEW", name=test_project_name)
        )[0]["id"]
        cd(cls.proj_id + ":/")
        cls.general_input_dir = os.path.join(dirname, "expression_test_files/input/")
        cls.general_output_dir = os.path.join(dirname, "expression_test_files/output/")
        cls.schema = EXTRACT_ASSAY_EXPRESSION_JSON_SCHEMA

        if not os.path.exists(cls.general_output_dir):
            os.makedirs(cls.general_output_dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.general_output_dir)

    # Test list fields dataset validation
    def test_bad_dataset_version(self):
        test_record = "{}:Extract_Expression/bad_version_dataset".format(self.proj_id)
        expected_error_message = "{}: Version of the cohort or dataset is too old.. Version must be 3.0".format(test_record)

        command = ["dx", "extract_assay", "expression", test_record, "--additional-fields-help"]
        process = subprocess.Popen(command, stderr=subprocess.PIPE, universal_newlines=True)

        actual_err_msg = process.communicate()[1]
        
        self.assertTrue(expected_error_message in actual_err_msg)

     # Test list fields dataset validation
    def test_bad_dataset_type(self):
        test_record = "{}:Extract_Expression/wrong_type_file".format(self.proj_id)
        expected_error_message = "{}: Invalid path. The path must point to a record type of cohort or dataset".format(test_record)

        command = ["dx", "extract_assay", "expression", test_record, "--additional-fields-help"]
        process = subprocess.Popen(command, stderr=subprocess.PIPE, universal_newlines=True)

        actual_err_msg = process.communicate()[1]
       
        self.assertTrue(expected_error_message in actual_err_msg)

if __name__ == "__main__":
    unittest.main()
