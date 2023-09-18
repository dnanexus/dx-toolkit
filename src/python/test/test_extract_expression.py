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
from dxpy.bindings.apollo.cmd_line_options_validator import ValidateArgsBySchema
from dxpy.bindings.apollo.input_arguments_validation_schemas import (
    EXTRACT_ASSAY_EXPRESSION_INPUT_ARGS_SCHEMA,
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
        cls.test_record = cls.proj_id + ":/Extract_Expression/standin_test_record"
        cls.cohort_browser_record = (
            cls.proj_id + ":/Extract_Expression/cohort_browser_object"
        )
        # Note: there would usually be a "func" key with a function object as its value
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

        if not os.path.exists(cls.general_output_dir):
            os.makedirs(cls.general_output_dir)

    @classmethod
    def input_arg_error_handler(cls, message):
        raise ValueError(message)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.general_output_dir)

    def standard_input_args_test(self, input_argument_dict, expected_error_message):
        # Deep copy the default parser dictionary
        parser_dict = {key: value for key, value in self.default_parser_dict.items()}
        for input_argument in input_argument_dict:
            if input_argument in self.default_parser_dict:
                parser_dict[input_argument] = input_argument_dict[input_argument]
            else:
                print("unrecognized argument in input args")
                return False

        input_arg_validator = ValidateArgsBySchema(
            parser_dict,
            EXTRACT_ASSAY_EXPRESSION_INPUT_ARGS_SCHEMA,
            error_handler=self.input_arg_error_handler,
        )
        with self.assertRaises(ValueError) as cm:
            input_arg_validator.validate_input_combination()

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    # EM-1
    # Test PATH argument not provided
    def test_path_missing(self):
        input_dict = {}
        expected_error_message = (
            'At least one of the following arguments is required: "Path", "--json-help"'
        )
        self.standard_input_args_test(input_dict, expected_error_message)

    # EM-1
    # The structure of "Path" is invalid
    @unittest.skip("test record not yet created")
    def test_missing_dataset(self):
        missing_dataset = self.proj_id + ":/Extract_Expression/missing_dataset"
        expected_error_message = (
            "dxpy.utils.resolver.ResolutionError: Could not find a {}".format(
                missing_dataset
            )
        )
        command = [
            "dx",
            "extract_assay",
            "expression",
            missing_dataset,
            "--list-assays",
        ]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-2
    # The user does not have access to the object
    # expected_error_message = "dxpy.exceptions.PermissionDenied: VIEW permission required in project-xxxx to perform this action, code 401"

    # EM-3
    # The record id or path is not a cohort or dataset
    # TODO: This is tested on another branch

    # EM-4
    # The record id or path is a cohort or dataset but is invalid (maybe corrupted, descriptor not accessible...etc)
    # expected_error_message = "..... : Invalid cohort or dataset"

    # EM-5
    # The record id or path is a cohort or dataset but the version is less than 3.0.
    # TODO: This is tested on another branch

    # EM-6
    # If record is a Cohort Browser Object and either –list-assays or --assay-name is provided.
    @unittest.skip("test record not yet created")
    def test_list_assay_cohort_browser(self):
        # TODO: add cohort browser object to test project
        expected_error_message = "Currently --assay-name and --list-assays may not be used with a CohortBrowser record (Cohort Object) as input. To select a specific assay or to list assays, please use a Dataset Object as input."
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.cohort_browser_record,
            "--list-assays",
        ]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-6
    # If record is a Cohort Browser Object and either –list-assays or --assay-name is provided.
    @unittest.skip("test record not yet created")
    def test_assay_name_cohort_browser(self):
        expected_error_message = "Currently --assay-name and --list-assays may not be used with a CohortBrowser record (Cohort Object) as input. To select a specific assay or to list assays, please use a Dataset Object as input."
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.cohort_browser_record,
            "--assay-name",
            "test_assay",
        ]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-7
    # Value specified for this option specified is not a valid assay
    @unittest.skip("test record not yet created")
    def test_invalid_assay_name(self):
        assay_name = "invalid_assay"
        expected_error_message = "Assay {} does not exist in the [PATH]".assay_name
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.test_record,
            "--assay-name",
            assay_name,
        ]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-8
    # When –assay-name is not provided and the dataset has no assays
    @unittest.skip("test record not yet created")
    def test_no_assay_dataset(self):
        # TODO: create dataset with no assays in test project
        no_assay_dataset = self.proj_id + ":/Extract_Expression/no_assay_dataset"
        expected_error_message = (
            "When --assay-name is not provided and the dataset has no assays"
        )
        command = ["dx", "extract_assay", "expression", no_assay_dataset]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-9
    # When the provided assay name is not a molecular expression assay
    @unittest.skip("test record not yet created")
    def test_wrong_assay_type(self):
        # TODO: Add dataset with somatic or other non CLIEXPRESS assay to test project
        somatic_assay_name = "somatic_assay"
        expected_error_message = "The assay name provided cannot be recognized as a molecular expression assay. For valid assays accepted by the function, `extract_assay expression` ,please use the --list-assays flag"
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.test_record,
            "--assay-name",
            somatic_assay_name,
        ]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-10
    # When --additional-fields-help is presented with other options
    def test_additional_fields_help_other_options(self):
        expected_error_message = (
            "--additional-fields-help cannot be presented with other options"
        )
        input_dict = {
            "path": self.test_record,
            "assay_name": "test_assay",
            "additional_fields_help": True,
        }
        self.standard_input_args_test(input_dict, expected_error_message)

    # EM-11
    # When invalid additional fields are passed
    def invalid_additional_fields(self):
        expected_error_message = "One or more of the supplied fields using --additional-fields are invalid. Please run --additional-fields-help for a list of valid fields"
        input_dict = {
            "path": self.test_record,
            "retrieve_expression": True,
            "input_json": r'{"annotation": {"feature_id": ["ENSG0000001", "ENSG00000002"]}}',
            "additional_fields": "feature_name,bad_field",
        }
        self.standard_input_args_test(input_dict, expected_error_message)

    # EM-12
    # When –list-assays is presented with other options
    def test_list_assays_assay_name(self):
        expected_error_message = (
            '"--list-assays" cannot be presented with other options'
        )
        input_dict = {
            "path": self.test_record,
            "list_assays": True,
            "assay_name": "fake_assay",
        }
        self.standard_input_args_test(input_dict, expected_error_message)

    # EM-13
    # When –list-assays is passed but there is no “Molecular Expression” Assay
    @unittest.skip("test record not yet created")
    def test_no_molec_exp_assay(self):
        # This is meant to return empty with no error message
        expected_error_message = ""
        no_molec_exp_assay = self.proj_id + ":/Extract_Expression/no_molec_exp_assay"
        command = [
            "dx",
            "extract_assay",
            "expression",
            no_molec_exp_assay,
            "--list_assays",
        ]

        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-14
    # When file already exist
    @unittest.skip("not yet implemented")
    def test_output_already_exist(self):
        # TODO replace this with a tempfile
        output_path = os.path.join(
            self.general_output_dir, "already_existing_output_file.tsv"
        )
        expected_error_message = (
            "{} already exists. Please specify a new file path".format(output_path)
        )
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.test_record,
            "--output",
            output_path,
        ]

        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-15
    # When a --retrieve-expression flag is passed without any value or when an empty JSON (an empty file or just {}) is passed with --retrieve-expression flag
    # Note: empty JSON is tested in the JSON validation section
    def test_no_value_retrieve_exp(self):
        expected_error_message = "No filter json is passed with --retrieve-expression or JSON or --retrieve-expression does not contain valid filter information."
        input_dict = {"path": self.test_record, "retrieve_expression": True}
        self.standard_input_args_test(input_dict, expected_error_message)

    # EM-16
    # When the string provided is a malformed JSON
    def test_malformed_retr_exp_json(self):
        expected_error_message = (
            "JSON provided for --retrieve-expression is malformatted."
        )
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.test_record,
            "--retrieve-expression",
            r"{thisisbadjson",
        ]

        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-17
    # When the .json file provided does not exist
    # Note: this probably needs to be tested with a Popen rather than with the ValidateArgsBySchema function
    def test_json_file_not_exist(self):
        missing_json_path = os.path.join(self.general_input_dir, "nonexistent.json")
        expected_error_message = (
            "JSON file provided to --retrieve-expression does not exist".format(
                missing_json_path
            )
        )
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.test_record,
            "--retrieve-expression",
            missing_json_path,
        ]

        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-18
    # When content in the JSON is mis-represented, including: invalid keys, invalid-values, duplicate keys
    # TODO This set of tests is implemented on a different branch

    # EM-19
    # When a filter has more entries in the list than what is allowed
    # TODO This set of tests is implemented on a different branch

    # EM-20
    # When none of the required filters is provided or when more than one of the required filters are provided
    # TODO This set of tests is implemented on a different branch

    # EM-21
    # When --json-help is passed with another option from --assay-name, --sql, --additional-fields, --expression-matix, --output
    def test_json_help_other_option(self):
        expected_error_message = "--json-help cannot be passed with any of --assay-name, --sql, --additional-fields, --expression-matix, or --output"
        input_dict = {
            "path": self.test_record,
            "json_help": True,
            "assay_name": "test_assay",
        }
        self.standard_input_args_test(input_dict, expected_error_message)

    # EM-22
    # When --expression-matrix is passed with other arguments other than, any context other than, --retrieve-expression
    # It seems that every combination of args that could be passed with this cause a different issue to be caught first
    # Which is fine but the error message will be for the other error
    def test_exp_matrix_other_args(self):
        # expected_error_message = "--expression-matrix cannot be passed with any argument other than --retrieve-expression"
        expected_error_message = "“--json-help cannot be passed with any of --assay-name, --sql, --additional-fields, --expression-matix, or --output”"
        input_dict = {
            "path": self.test_record,
            "expression_matrix": True,
            "additional_fields": "feature_name",
            "json_help": True,
        }
        self.standard_input_args_test(input_dict, expected_error_message)

    # EM-23
    # --expression-matrix/-em cannot be used with --sql
    def test_exp_matrix_sql(self):
        expected_error_message = (
            "--expression-matrix/-em cannot be passed with the flag, --sql"
        )
        input_dict = {
            "path": self.test_record,
            "expression_matrix": True,
            "retrieve_expression": True,
            "input_json": r'{"annotation": {"feature_name": ["BRCA2"]}}',
            "sql": True,
        }
        self.standard_input_args_test(input_dict, expected_error_message)

    # EM-24
    # Query times out
    @unittest.skip("test record not yet created")
    def test_timeout(self):
        # TODO: find a large dataset that this will always time out on
        expected_error_message = "Please consider using ‘--sql’ option to generate the SQL query and execute query via a private compute cluster"
        large_dataset = self.proj_id + ":/Extract_Expression/large_dataset"

        command = [
            "dx",
            "extract_assay",
            "expression",
            large_dataset,
            "--retrieve-expression",
            r'{"location": [{"chromosome": "1","starting_position": "1","ending_position": "240000000"}]}',
        ]

        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)


if __name__ == "__main__":
    unittest.main()
