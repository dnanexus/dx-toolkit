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

    @classmethod
    def json_error_handler(cls, message):
        raise ValueError(message)

    #
    # Malformed input json tests
    #

    def test_annotation_conflicting_keys(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "annotation_conflicting_keys.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = (
            "Conflicting keys feature_name and feature_id cannot be present together."
        )
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_annotation_id_maxitem(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "annotation_id_maxitem.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = (
            "error message not yet defined"
        )
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())


    def test_annotation_id_type(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "annotation_id_type.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "Key 'feature_id' has an invalid type. Expected <class 'list'> but got <class 'dict'>"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_annotation_name_maxitem(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "annotation_name_maxitem.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "Key 'feature_id' has an invalid type. Expected <class 'list'> but got <class 'dict'>"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_annotation_name_type(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "annotation_name_type.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "Key 'feature_name' has an invalid type. Expected <class 'list'> but got <class 'dict'>"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_annotation_type(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "annotation_type.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "Key 'annotation' has an invalid type. Expected <class 'dict'> but got <class 'list'>"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_bad_dependent_conditional(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "bad_dependent_conditional.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "When expression is present, one of the following keys must be also present: annotation, location."
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_bad_toplevel_key(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "bad_toplevel_key.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "error message not yet defined"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_conflicting_toplevel(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "conflicting_toplevel.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = (
            "Conflicting keys feature_name and feature_id cannot be present together."
        )
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_empty_dict(self):
        json_path = os.path.join(self.general_input_dir, "malformed", "empty_dict.json")
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "error message not yet defined"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_expression_empty_dict(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "expression_empty_dict.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "error message not yet defined"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_expression_max_type(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "expression_max_type.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "Key 'max_value' has an invalid type. Expected <class 'str'> but got <class 'int'>"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_expression_min_type(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "expression_min_type.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "Key 'min_value' has an invalid type. Expected <class 'str'> but got <class 'int'>"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_expression_type(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "expression_type.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "Key 'expression' has an invalid type. Expected <class 'dict'> but got <class 'list'>"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_location_chrom_type(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "location_chrom_type.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "Key 'chromosome' has an invalid type. Expected <class 'str'> but got <class 'int'>"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_location_end_before_start(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "location_end_before_start.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "error message not yet defined"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_location_end_type(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "location_end_type.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "Key 'ending_position' has an invalid type. Expected <class 'str'> but got <class 'int'>"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_location_item_type(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "location_item_type.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "error message not yet defined"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_location_max_width(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "location_max_width.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "error message not yet defined"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_location_missing_chr(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "location_missing_chr.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = (
            "Required key 'chromosome' was not found in the input JSON."
        )
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_location_missing_end(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "location_missing_end.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = (
            "Required key 'ending_position' was not found in the input JSON."
        )
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_location_missing_start(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "location_missing_start.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = (
            "Required key 'starting_position' was not found in the input JSON."
        )
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_location_start_type(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "location_start_type.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "Key 'starting_position' has an invalid type. Expected <class 'str'> but got <class 'int'>"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_location_type(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "location_type.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "error message not yet defined"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_sample_id_maxitem(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "sample_id_maxitem.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "error message not yet defined"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def test_sample_id_type(self):
        json_path = os.path.join(
            self.general_input_dir, "malformed", "sample_id_type.json"
        )
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        expected_error_message = "Expected list but got <class 'dict'> for sample_id"
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    #
    # Correct JSON inputs
    #

    def test_annotation_feature_id(self):
        json_path = os.path.join(
            self.general_input_dir, "valid", "annotation_feature_id.json"
        )
        
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        validator.validate(input_json)

    def test_annotation_feature_name(self):
        json_path = os.path.join(
            self.general_input_dir, "valid", "annotation_feature_name.json"
        )
        
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        validator.validate(input_json)

    def test_dependent_conditional_annotation(self):
        json_path = os.path.join(
            self.general_input_dir, "valid", "dependent_conditional_annotation.json"
        )
        
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        validator.validate(input_json)

    def test_dependent_conditional_location(self):
        json_path = os.path.join(
            self.general_input_dir, "valid", "dependent_conditional_location.json"
        )
        
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        validator.validate(input_json)
        
    def test_expression_max_only(self):
        json_path = os.path.join(
            self.general_input_dir, "valid", "expression_max_only.json"
        )
        
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        validator.validate(input_json)

    def test_expression_min_and_max(self):
        json_path = os.path.join(
            self.general_input_dir, "valid", "expression_min_and_max.json"
        )
        
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        validator.validate(input_json)

    def test_expression_min_only(self):
        json_path = os.path.join(
            self.general_input_dir, "valid", "expression_min_only.json"
        )
        
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        validator.validate(input_json)

    def test_multi_location(self):
        json_path = os.path.join(
            self.general_input_dir, "valid", "multi_location.json"
        )
        
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        validator.validate(input_json)

    def test_single_location(self):
        json_path = os.path.join(
            self.general_input_dir, "valid", "expression_min_only.json"
        )
        
        validator = JSONValidator(self.schema, error_handler=self.json_error_handler)
        
        with open(json_path, "r") as infile:
            input_json = json.load(infile)

        validator.validate(input_json)
    

# Start the test
if __name__ == "__main__":
    unittest.main()
