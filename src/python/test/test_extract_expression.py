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
from dxpy.bindings.apollo.path_validator import PathValidator
from dxpy.utils.resolver import resolve_existing_path

from dxpy.bindings.apollo.assay_filtering_json_schemas import (
    EXTRACT_ASSAY_EXPRESSION_JSON_SCHEMA,
)
from dxpy.bindings.apollo.input_arguments_validation_schemas import EXTRACT_ASSAY_EXPRESSION_INPUT_ARGS_SCHEMA
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
        cls.json_schema = EXTRACT_ASSAY_EXPRESSION_JSON_SCHEMA
        cls.input_args_schema = EXTRACT_ASSAY_EXPRESSION_INPUT_ARGS_SCHEMA
        cls.bad_version_dataset = "{}:Extract_Expression/bad_version_dataset".format(cls.proj_id)
        cls.wrong_type_path_file = "{}:Extract_Expression/wrong_type_file".format(cls.proj_id)

        if not os.path.exists(cls.general_output_dir):
            os.makedirs(cls.general_output_dir)

    @classmethod
    def input_arg_error_handler(cls, message):
        raise ValueError(message)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.general_output_dir)


    #
    # Path Validation tests
    # There are # ways this function can fail.  Each of the following assumes the previous conditions have been met:
    # 1. Object in wrong project
    # 2. Object not of class record
    # 3. Object not of recordType "Dataset" or "CohortBrowser"
    # 4. Object is not of correct version (3.0 at the time of this writing)
    # 5. Object is CohortBrowser type and --assay-name or --list-assays has been given on the command line
    #


    # 2. Object not of class record
    def test_bad_dataset_type_unit(self):
        test_record = self.wrong_type_path_file
        expected_error_message = "{}: Invalid path. The path must point to a record type of cohort or dataset".format(test_record)
        project, folder_path, entity_result = resolve_existing_path(test_record)
        validator = PathValidator(self.input_args_schema,self.proj_id,entity_result["describe"],error_handler=self.input_arg_error_handler)

        with self.assertRaises(ValueError) as cm:
            validator.validate()

        self.assertEqual(expected_error_message, str(cm.exception).strip())


    # 3. Object not of recordType "Dataset" or "CohortBrowser"
    def test_bad_dataset_version(self):
        expected_error_message = "{}: Version of the cohort or dataset is too old. Version must be 3.0".format(self.bad_version_dataset)
        project, folder_path, entity_result = resolve_existing_path(self.bad_version_dataset)
        validator = PathValidator(self.input_args_schema,self.proj_id,entity_result["describe"],error_handler=self.input_arg_error_handler)

        # print(entity_result["describe"])
        with self.assertRaises(ValueError) as cm:
            validator.validate()

        self.assertEqual(expected_error_message, str(cm.exception).strip())


if __name__ == "__main__":
    unittest.main()
