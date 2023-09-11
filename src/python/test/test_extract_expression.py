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
        cls.test_record = cls.proj_id + ":/Extract_Expression/standin_test_record"
        cls.cohort_browser_record = (
            cls.proj_id + ":/Extract_Expression/cohort_browser_object"
        )

        if not os.path.exists(cls.general_output_dir):
            os.makedirs(cls.general_output_dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.general_output_dir)

    # Test PATH argument not provided
    def test_path_missing(self):
        expected_error_message = (
            'At least one of the following arguments is required: "Path", "--json-help"'
        )
        command = ["dx", "extract_assay", "expression"]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]

        self.assertTrue(expected_error_message in actual_err_msg)

    # Test --list-assays and --assay-name being provided together
    def test_list_assays_assay_name(self):
        expected_error_message = (
            '"--list-assays" cannot be presented with other options'
        )
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.test_record,
            "--list-assays",
            "--assay-name",
            "fake_assay",
        ]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-1
    # The structure of "Path" is invalid
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
    def test_list_assay_cohort_browser(self):
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
    def test_no_assay_dataset(self):
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
    def test_wrong_assay_type(self):
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
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.test_record,
            "--assay-name",
            "test_assay",
            "--additional-fields-help",
        ]

        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-11
    # When invalid additional fields are passed
    def invalid_additional_fields(self):
        expected_error_message = "One or more of the supplied fields using --additional-fields are invalid. Please run --additional-fields-help for a list of valid fields"
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.test_record,
            "--additional-fields",
            "feature_name,bad_field",
        ]

        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-12
    # When –list-assays is presented with other options
    # TODO this is implemented on a different branch

    # EM-13
    # When –list-assays is passed but there is no “Molecular Expression” Assay
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
    def test_no_value_retrieve_exp(self):
        expected_error_message = "No filter json is passed with --retrieve-expression or JSON or --retrieve-expression does not contain valid filter information."
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.test_record,
            "--retrieve-expression",
        ]

        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-15-2
    # When a --retrieve-expression flag is passed without any value or when an empty JSON (an empty file or just {}) is passed with --retrieve-expression flag
    def test_empty_json_retrieve_exp(self):
        expected_error_message = "No filter json is passed with --retrieve-expression or JSON or --retrieve-expression does not contain valid filter information."
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.test_record,
            "--retrieve-expression",
            r"{}",
        ]

        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

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
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.test_record,
            "--json-help",
            "--assay-name",
            "test_assay",
        ]

        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-22
    # When --expression-matrix is passed with other arguments other than, any context other than, --retrieve-expression
    def test_exp_matrix_other_args(self):
        expected_error_message = "--expression-matrix cannot be passed with any argument other than --retrieve-expression"
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.test_record,
            "--expression-matrix",
            "--assay-name",
            "test_assay",
        ]

        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-23
    # --expression-matrix/-em cannot be used with --sql
    def test_exp_matrix_sql(self):
        expected_error_message = (
            "--expression-matrix/-em cannot be passed with the flag, --sql"
        )
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.test_record,
            "--expression-matrix",
            "--retrieve-expression",
            r'{"annotation": {"feature_name": ["BRCA2"]}}',
            "--sql",
        ]

        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        self.assertTrue(expected_error_message in actual_err_msg)

    # EM-24
    # Query times out
    # TODO: find a large dataset that this will always time out on
    def test_timeout(self):
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
