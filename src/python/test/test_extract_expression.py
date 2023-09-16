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
from expression_test_files.input_dict import CLIEXPRESS_TEST_INPUT

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

    @classmethod
    def json_error_handler(cls, message):
        raise ValueError(message)

    def standard_negative_filter_test(self, json_name, expected_error_message):
        input_json = CLIEXPRESS_TEST_INPUT["malformed"][json_name]
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def standard_positive_filter_test(self, json_name):
        input_json = CLIEXPRESS_TEST_INPUT["valid"][json_name]

        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)

        validator.validate(input_json)

    #
    # Malformed input json tests
    #

    def test_annotation_conflicting_keys(self):
        self.standard_negative_filter_test(
            "annotation_conflicting_keys",
            "Conflicting keys feature_name and feature_id cannot be present together.",
        )

    def test_annotation_id_maxitem(self):
        self.standard_negative_filter_test(
            "annotation_id_maxitem", "error message not yet defined"
        )

    def test_annotation_id_type(self):
        self.standard_negative_filter_test(
            "annotation_id_type",
            "Key 'feature_id' has an invalid type. Expected <class 'list'> but got <class 'dict'>",
        )

    def test_annotation_name_maxitem(self):
        self.standard_negative_filter_test(
            "annotation_name_maxitem",
            "Key 'feature_id' has an invalid type. Expected <class 'list'> but got <class 'dict'>",
        )

    def test_annotation_name_type(self):
        self.standard_negative_filter_test(
            "annotation_name_type",
            "Key 'feature_name' has an invalid type. Expected <class 'list'> but got <class 'dict'>",
        )

    def test_annotation_type(self):
        self.standard_negative_filter_test(
            "annotation_type",
            "Key 'annotation' has an invalid type. Expected <class 'dict'> but got <class 'list'>",
        )

    def test_bad_dependent_conditional(self):
        self.standard_negative_filter_test(
            "bad_dependent_conditional",
            "When expression is present, one of the following keys must be also present: annotation, location.",
        )

    def test_bad_toplevel_key(self):
        self.standard_negative_filter_test(
            "bad_toplevel_key", "error message not yet defined"
        )

    def test_conflicting_toplevel(self):
        self.standard_negative_filter_test(
            "conflicting_toplevel",
            "Conflicting keys feature_name and feature_id cannot be present together.",
        )

    def test_empty_dict(self):
        self.standard_negative_filter_test(
            "empty_dict", "error message not yet defined"
        )

    def test_expression_empty_dict(self):
        self.standard_negative_filter_test(
            "expression_empty_dict", "error message not yet defined"
        )

    def test_expression_max_type(self):
        self.standard_negative_filter_test(
            "expression_max_type",
            "Key 'max_value' has an invalid type. Expected <class 'str'> but got <class 'int'>",
        )

    def test_expression_min_type(self):
        self.standard_negative_filter_test(
            "expression_min_type",
            "Key 'min_value' has an invalid type. Expected <class 'str'> but got <class 'int'>",
        )

    def test_expression_type(self):
        self.standard_negative_filter_test(
            "expression_type",
            "Key 'expression' has an invalid type. Expected <class 'dict'> but got <class 'list'>",
        )

    def test_location_chrom_type(self):
        self.standard_negative_filter_test(
            "location_chrom_type",
            "Key 'chromosome' has an invalid type. Expected <class 'str'> but got <class 'int'>",
        )

    def test_location_end_before_start(self):
        self.standard_negative_filter_test(
            "location_end_before_start", "error message not yet defined"
        )

    def test_location_end_type(self):
        self.standard_negative_filter_test(
            "location_end_type",
            "Key 'ending_position' has an invalid type. Expected <class 'str'> but got <class 'int'>",
        )

    def test_location_item_type(self):
        self.standard_negative_filter_test(
            "location_item_type", "error message not yet defined"
        )

    def test_location_max_width(self):
        self.standard_negative_filter_test(
            "location_max_width", "error message not yet defined"
        )

    def test_location_missing_chr(self):
        self.standard_negative_filter_test(
            "location_missing_chr",
            "Required key 'chromosome' was not found in the input JSON.",
        )

    def test_location_missing_end(self):
        self.standard_negative_filter_test(
            "location_missing_end",
            "Required key 'ending_position' was not found in the input JSON.",
        )

    def test_location_missing_start(self):
        self.standard_negative_filter_test(
            "location_missing_start",
            "Required key 'starting_position' was not found in the input JSON.",
        )

    def test_location_start_type(self):
        self.standard_negative_filter_test(
            "location_start_type",
            "Key 'starting_position' has an invalid type. Expected <class 'str'> but got <class 'int'>",
        )

    def test_location_type(self):
        self.standard_negative_filter_test(
            "location_type", "error message not yet defined"
        )

    def test_sample_id_maxitem(self):
        self.standard_negative_filter_test(
            "sample_id_maxitem", "error message not yet defined"
        )

    def test_sample_id_type(self):
        self.standard_negative_filter_test(
            "sample_id_type", "Expected list but got <class 'dict'> for sample_id"
        )

    #
    # Correct JSON inputs
    #

    def test_annotation_feature_id(self):
        self.standard_positive_filter_test("annotation_feature_id")

    def test_annotation_feature_name(self):
        self.standard_positive_filter_test("annotation_feature_name")

    def test_dependent_conditional_annotation(self):
        self.standard_positive_filter_test("dependent_conditional_annotation")

    def test_dependent_conditional_location(self):
        self.standard_positive_filter_test("dependent_conditional_location")

    def test_expression_max_only(self):
        self.standard_positive_filter_test("expression_max_only")

    def test_expression_min_and_max(self):
        self.standard_positive_filter_test("expression_min_and_max")

    def test_expression_min_only(self):
        self.standard_positive_filter_test("expression_min_only")

    def test_multi_location(self):
        self.standard_positive_filter_test("multi_location")

    def test_single_location(self):
        self.standard_positive_filter_test("single_location")


# Start the test
if __name__ == "__main__":
    unittest.main()
