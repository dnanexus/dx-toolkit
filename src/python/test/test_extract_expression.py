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
import copy
import json
import shutil
from dxpy_testutil import cd, chdir
from dxpy.bindings.apollo.ValidateJSONbySchema import JSONValidator
from dxpy.bindings.apollo.path_validator import PathValidator
from dxpy.utils.resolver import resolve_existing_path

from dxpy.bindings.apollo.assay_filtering_json_schemas import (
    EXTRACT_ASSAY_EXPRESSION_JSON_SCHEMA,
)
from dxpy.bindings.apollo.input_arguments_validation_schemas import (
    EXTRACT_ASSAY_EXPRESSION_INPUT_ARGS_SCHEMA,
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
        cls.json_schema = EXTRACT_ASSAY_EXPRESSION_JSON_SCHEMA
        cls.input_args_schema = EXTRACT_ASSAY_EXPRESSION_INPUT_ARGS_SCHEMA
        cls.test_record = "record-GZ0KYfQ0vGPV99875py791fj"

        if not os.path.exists(cls.general_output_dir):
            os.makedirs(cls.general_output_dir)

        cls.default_entity_describe = {
            "id": cls.test_record,
            "project": cls.proj_id,
            "class": "record",
            "sponsored": False,
            "name": "fake_assay",
            "types": ["Dataset"],
            "state": "closed",
            "hidden": False,
            "links": ["database-xxxx", "file-zzzzzz"],
            "folder": "/",
            "tags": [],
            "created": 0,
            "modified": 0,
            "createdBy": {
                "user": "user-test",
                "job": "job-xyz",
                "executable": "app-xyz",
            },
            "size": 0,
            "properties": {},
            "details": {
                "descriptor": {"$dnanexus_link": "file-xyz"},
                "version": "3.0",
                "schema": "ds-molecular_expression_quantification",
                "databases": [{"assay": {"$dnanexus_link": "database-yyyyyy"}}],
                "name": "fake_assay",
                "description": "Dataset: assay",
            },
        }

        cls.default_parser_dict = {
            "apiserver_host": None,
            "apiserver_port": None,
            "apiserver_protocol": None,
            "project_context_id": None,
            "workspace_id": None,
            "security_context": None,
            "auth_token": None,
            "env_help": None,
            "version": None,
            "command": "extract_assay",
            "path": None,
            "list_assays": False,
            "retrieve_expression": False,
            "additional_fields_help": False,
            "assay_name": None,
            "input_json": None,
            "input_json_file": None,
            "json_help": False,
            "sql": False,
            "additional_fields": None,
            "expression_matrix": False,
            "delim": None,
            "output": None,
        }

    @classmethod
    def path_validation_error_handler(cls, message):
        raise ValueError(message)

    @classmethod
    def json_error_handler(cls, message):
        raise ValueError(message)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.general_output_dir)

    #
    # Helper functions used by different types of tests
    #

    def common_negative_path_validation_test(
        self, expected_error_message, parser_dict, entity_describe
    ):
        validator = PathValidator(
            parser_dict,
            self.proj_id,
            entity_describe,
            error_handler=self.path_validation_error_handler,
        )

        with self.assertRaises(ValueError) as cm:
            validator.validate()

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def common_negative_filter_test(self, json_name, expected_error_message):
        input_json = CLIEXPRESS_TEST_INPUT["malformed"][json_name]
        validator = JSONValidator(
            self.json_schema, error_handler=self.json_error_handler
        )

        with self.assertRaises(ValueError) as cm:
            validator.validate(input_json)

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    def common_positive_filter_test(self, json_name):
        input_json = CLIEXPRESS_TEST_INPUT["valid"][json_name]

        validator = JSONValidator(
            self.json_schema, error_handler=self.json_error_handler
        )

        validator.validate(input_json)

    #
    # Path Validation tests
    # There are 5 ways this function can detect a bad dataset/path.  Checked in the following order:
    # 1. (EM-1?) Object in wrong project
    # 2. (EM-1?) Object not of class record
    # 3. (EM-3) Object not of recordType "Dataset" or "CohortBrowser"
    # 4. (EM-5) Object is not of correct version (3.0 at the time of this writing)
    # 5. (EM-6) Object is CohortBrowser type and --assay-name or --list-assays has been given on the command line
    #

    # EM-1
    # 1. Object in wrong project
    def test_bad_dataset_project(self):
        # deep copy the standard entity describe and parser dictionaries
        entity_describe = copy.deepcopy(self.default_entity_describe)
        parser_dict = copy.deepcopy(self.default_parser_dict)
        parser_dict["path"] = "{}:{}".format(self.proj_id, self.test_record)
        # Overwrite project, but not record id, so there is a project
        entity_describe["project"] = "project-fakeproject419857"
        expected_error_message = 'Unable to resolve "{}:{}" to a data object or folder name in {}. Please make sure your object is in your selected project.'.format(
            self.proj_id, self.test_record, self.proj_id
        )

        self.common_negative_path_validation_test(
            expected_error_message, parser_dict, entity_describe
        )

    # EM-1
    # 2. Object not of class record
    def test_object_not_class_record(self):
        # deep copy the standard entity describe and parser dictionaries
        entity_describe = copy.deepcopy(self.default_entity_describe)
        parser_dict = copy.deepcopy(self.default_parser_dict)
        parser_dict["path"] = "{}:{}".format(self.proj_id, self.test_record)
        entity_describe["class"] = "not_record"
        expected_error_message = "Invalid path. The path must point to a record type of cohort or dataset and not a {} object.".format(
            entity_describe["class"]
        )

        self.common_negative_path_validation_test(
            expected_error_message, parser_dict, entity_describe
        )

    # EM-3
    # 3. Object not of recordType "Dataset" or "CohortBrowser"
    def test_bad_dataset_type(self):
        entity_describe = copy.deepcopy(self.default_entity_describe)
        parser_dict = copy.deepcopy(self.default_parser_dict)
        parser_dict["path"] = "{}:{}".format(self.proj_id, self.test_record)
        # Overwrite type in default entity_describe dict with something other than CohortBrowser or Dataset
        entity_describe["types"] = ["bad_type"]
        expected_error_message = "{} Invalid path. The path must point to a record type of cohort or dataset and not a ['bad_type'] object.".format(
            entity_describe["id"]
        )

        self.common_negative_path_validation_test(
            expected_error_message, parser_dict, entity_describe
        )

    # EM-5
    # 4. Object is not of correct version (3.0 at the time of this writing)
    def test_bad_dataset_version(self):
        entity_describe = copy.deepcopy(self.default_entity_describe)
        parser_dict = copy.deepcopy(self.default_parser_dict)
        parser_dict["path"] = "{}:{}".format(self.proj_id, self.test_record)
        entity_describe["details"]["version"] = "2.0"
        expected_error_message = "2.0: Version of the cohort or dataset is too old. Version must be at least 3.0.".format(
            self.test_record
        )

        self.common_negative_path_validation_test(
            expected_error_message, parser_dict, entity_describe
        )

    # (EM-6)
    # 5. Object is CohortBrowser type and --assay-name or --list-assays has been given on the command line
    def test_cohort_browser_assay_name(self):
        entity_describe = copy.deepcopy(self.default_entity_describe)
        parser_dict = copy.deepcopy(self.default_parser_dict)
        parser_dict["path"] = "{}:{}".format(self.proj_id, self.test_record)
        entity_describe["types"] = ["CohortBrowser"]
        parser_dict["assay_name"] = True
        expected_error_message = "Currently --assay-name and --list-assays may not be used with a CohortBrowser record (Cohort Object) as input. To select a specific assay or to list assays, please use a Dataset Object as input."
        self.common_negative_path_validation_test(
            expected_error_message, parser_dict, entity_describe
        )

    # (EM-6)
    # 5. Object is CohortBrowser type and --assay-name or --list-assays has been given on the command line
    def test_cohort_browser_list_assays(self):
        entity_describe = {
            key: value for key, value in self.default_entity_describe.items()
        }
        parser_dict = {key: value for key, value in self.default_parser_dict.items()}
        parser_dict["path"] = "{}:{}".format(self.proj_id, self.test_record)
        entity_describe["types"] = ["CohortBrowser"]
        parser_dict["list_assays"] = True
        expected_error_message = "Currently --assay-name and --list-assays may not be used with a CohortBrowser record (Cohort Object) as input. To select a specific assay or to list assays, please use a Dataset Object as input."
        self.common_negative_path_validation_test(
            expected_error_message, parser_dict, entity_describe
        )

    def test_positive_path_validation(self):
        entity_describe = {
            key: value for key, value in self.default_entity_describe.items()
        }
        parser_dict = {key: value for key, value in self.default_parser_dict.items()}
        parser_dict["path"] = "{}:{}".format(self.proj_id, self.test_record)
        validator = PathValidator(
            parser_dict,
            self.proj_id,
            entity_describe,
            error_handler=self.path_validation_error_handler,
        )
        validator.validate()

    #
    # Malformed input json tests
    #

    def test_annotation_conflicting_keys(self):
        self.common_negative_filter_test(
            "annotation_conflicting_keys",
            "Conflicting keys feature_name and feature_id cannot be present together.",
        )

    def test_annotation_id_maxitem(self):
        self.common_negative_filter_test(
            "annotation_id_maxitem", "error message not yet defined"
        )

    def test_annotation_id_type(self):
        self.common_negative_filter_test(
            "annotation_id_type",
            "Key 'feature_id' has an invalid type. Expected <class 'list'> but got <class 'dict'>",
        )

    def test_annotation_name_maxitem(self):
        self.common_negative_filter_test(
            "annotation_name_maxitem",
            "Key 'feature_id' has an invalid type. Expected <class 'list'> but got <class 'dict'>",
        )

    def test_annotation_name_type(self):
        self.common_negative_filter_test(
            "annotation_name_type",
            "Key 'feature_name' has an invalid type. Expected <class 'list'> but got <class 'dict'>",
        )

    def test_annotation_type(self):
        self.common_negative_filter_test(
            "annotation_type",
            "Key 'annotation' has an invalid type. Expected <class 'dict'> but got <class 'list'>",
        )

    def test_bad_dependent_conditional(self):
        self.common_negative_filter_test(
            "bad_dependent_conditional",
            "When expression is present, one of the following keys must be also present: annotation, location.",
        )

    def test_bad_toplevel_key(self):
        self.common_negative_filter_test(
            "bad_toplevel_key", "error message not yet defined"
        )

    def test_conflicting_toplevel(self):
        self.common_negative_filter_test(
            "conflicting_toplevel",
            "Conflicting keys feature_name and feature_id cannot be present together.",
        )

    def test_empty_dict(self):
        self.common_negative_filter_test("empty_dict", "error message not yet defined")

    def test_expression_empty_dict(self):
        self.common_negative_filter_test(
            "expression_empty_dict", "error message not yet defined"
        )

    def test_expression_max_type(self):
        self.common_negative_filter_test(
            "expression_max_type",
            "Key 'max_value' has an invalid type. Expected <class 'str'> but got <class 'int'>",
        )

    def test_expression_min_type(self):
        self.common_negative_filter_test(
            "expression_min_type",
            "Key 'min_value' has an invalid type. Expected <class 'str'> but got <class 'int'>",
        )

    def test_expression_type(self):
        self.common_negative_filter_test(
            "expression_type",
            "Key 'expression' has an invalid type. Expected <class 'dict'> but got <class 'list'>",
        )

    def test_location_chrom_type(self):
        self.common_negative_filter_test(
            "location_chrom_type",
            "Key 'chromosome' has an invalid type. Expected <class 'str'> but got <class 'int'>",
        )

    def test_location_end_before_start(self):
        self.common_negative_filter_test(
            "location_end_before_start", "error message not yet defined"
        )

    def test_location_end_type(self):
        self.common_negative_filter_test(
            "location_end_type",
            "Key 'ending_position' has an invalid type. Expected <class 'str'> but got <class 'int'>",
        )

    def test_location_item_type(self):
        self.common_negative_filter_test(
            "location_item_type", "error message not yet defined"
        )

    def test_location_max_width(self):
        self.common_negative_filter_test(
            "location_max_width", "error message not yet defined"
        )

    def test_location_missing_chr(self):
        self.common_negative_filter_test(
            "location_missing_chr",
            "Required key 'chromosome' was not found in the input JSON.",
        )

    def test_location_missing_end(self):
        self.common_negative_filter_test(
            "location_missing_end",
            "Required key 'ending_position' was not found in the input JSON.",
        )

    def test_location_missing_start(self):
        self.common_negative_filter_test(
            "location_missing_start",
            "Required key 'starting_position' was not found in the input JSON.",
        )

    def test_location_start_type(self):
        self.common_negative_filter_test(
            "location_start_type",
            "Key 'starting_position' has an invalid type. Expected <class 'str'> but got <class 'int'>",
        )

    def test_location_type(self):
        self.common_negative_filter_test(
            "location_type", "error message not yet defined"
        )

    def test_sample_id_maxitem(self):
        self.common_negative_filter_test(
            "sample_id_maxitem", "error message not yet defined"
        )

    def test_sample_id_type(self):
        self.common_negative_filter_test(
            "sample_id_type", "Expected list but got <class 'dict'> for sample_id"
        )

    #
    # Correct JSON inputs
    #

    def test_annotation_feature_id(self):
        self.common_positive_filter_test("annotation_feature_id")

    def test_annotation_feature_name(self):
        self.common_positive_filter_test("annotation_feature_name")

    def test_dependent_conditional_annotation(self):
        self.common_positive_filter_test("dependent_conditional_annotation")

    def test_dependent_conditional_location(self):
        self.common_positive_filter_test("dependent_conditional_location")

    def test_expression_max_only(self):
        self.common_positive_filter_test("expression_max_only")

    def test_expression_min_and_max(self):
        self.common_positive_filter_test("expression_min_and_max")

    def test_expression_min_only(self):
        self.common_positive_filter_test("expression_min_only")

    def test_multi_location(self):
        self.common_positive_filter_test("multi_location")

    def test_single_location(self):
        self.common_positive_filter_test("single_location")


# Start the test
if __name__ == "__main__":
    unittest.main()
