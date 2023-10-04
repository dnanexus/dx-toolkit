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


# Run manually with python3 src/python/test/test_extract_expression.py TestDXExtractExpression

import unittest
import subprocess
import sys
import os
import dxpy
import copy
import json
import tempfile
import csv

import shutil
from dxpy_testutil import cd, chdir
from dxpy.bindings.apollo.ValidateJSONbySchema import JSONValidator
from dxpy.bindings.apollo.path_validator import PathValidator
from dxpy.utils.resolver import resolve_existing_path

from dxpy.bindings.apollo.assay_filtering_json_schemas import (
    EXTRACT_ASSAY_EXPRESSION_JSON_SCHEMA,
)
from dxpy.bindings.apollo.cmd_line_options_validator import ArgsValidator
from dxpy.bindings.apollo.input_arguments_validation_schemas import (
    EXTRACT_ASSAY_EXPRESSION_INPUT_ARGS_SCHEMA,
)
from dxpy.bindings.apollo.expression_test_input_dict import CLIEXPRESS_TEST_INPUT

from dxpy.cli.output_handling import write_expression_output
from dxpy.cli.help_messages import EXTRACT_ASSAY_EXPRESSION_JSON_TEMPLATE
from dxpy.bindings.dxrecord import DXRecord
from dxpy.bindings.apollo.dataset import Dataset

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
        # Make an output directory if it doesn't already exists
        if not os.path.exists(cls.general_output_dir):
            os.makedirs(cls.general_output_dir)
        cls.json_schema = EXTRACT_ASSAY_EXPRESSION_JSON_SCHEMA
        cls.input_args_schema = EXTRACT_ASSAY_EXPRESSION_INPUT_ARGS_SCHEMA
        cls.test_record = (
            "project-G5Bzk5806j8V7PXB678707bv:record-GYPg9Jj06j8pp3z41682J23p"
        )
        cls.cohort_browser_record = (
            cls.proj_id + ":/Extract_Expression/cohort_browser_object"
        )

        # In python3, str(type(object)) looks like <{0} 'obj_class'> but in python 2, it would be <type 'obj_class'>
        # This impacts our expected error messages
        cls.type_representation = "class"
        if python_version == 2:
            cls.type_representation = "type"
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
            "filter_json": None,
            "filter_json_file": None,
            "json_help": False,
            "sql": False,
            "additional_fields": None,
            "expression_matrix": False,
            "delim": None,
            "output": None,
        }

        

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
            "filter_json": None,
            "filter_json_file": None,
            "json_help": False,
            "sql": False,
            "additional_fields": None,
            "expression_matrix": False,
            "delim": None,
            "output": None,
        }

        cls.vizserver_data_mock_response = {
                "results": [
                    {
                        "feature_id": "ENST00000450305",
                        "sample_id": "sample_2",
                        "expression": 50,
                        "strand": "+",
                    },
                    {
                        "feature_id": "ENST00000456328",
                        "sample_id": "sample_2",
                        "expression": 90,
                        "strand": "+",
                    },
                    {
                        "feature_id": "ENST00000488147",
                        "sample_id": "sample_2",
                        "expression": 90,
                        "strand": "-",
                    },
                ]
            }
        cls.argparse_expression_help_message = os.path.join(dirname, "help_messages/extract_expression_help_message.txt")
        cls.expression_dataset_name = "molecular_expression1.dataset"
        cls.expression_dataset = cls.proj_id + ":/" + cls.expression_dataset_name
        cls.combined_expression_cohort_name = "Combined_Expression_Cohort"
        cls.combined_expression_cohort = cls.proj_id + ":/" + cls.combined_expression_cohort_name

    @classmethod
    def path_validation_error_handler(cls, message):
        raise ValueError(message)

    @classmethod
    def input_arg_error_handler(cls, message):
        raise ValueError(message)

    @classmethod
    def json_error_handler(cls, message):
        raise ValueError(message)

    @classmethod
    def common_value_error_handler(cls, message):
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

    def common_input_args_test(self, input_argument_dict, expected_error_message):
        # Deep copy the default parser dictionary
        parser_dict = {key: value for key, value in self.default_parser_dict.items()}
        for input_argument in input_argument_dict:
            if input_argument in self.default_parser_dict:
                parser_dict[input_argument] = input_argument_dict[input_argument]
            else:
                print("unrecognized argument in input args")
                return False

        input_arg_validator = ArgsValidator(
            parser_dict,
            EXTRACT_ASSAY_EXPRESSION_INPUT_ARGS_SCHEMA,
            error_handler=self.input_arg_error_handler,
        )
        with self.assertRaises(ValueError) as cm:
            input_arg_validator.validate_input_combination()

        self.assertEqual(expected_error_message, str(cm.exception).strip())

    #
    # Positive output tests
    #

    def test_output_data_format(self):
        expected_result = """feature_id,sample_id,expression,strand
            ENST00000450305,sample_2,50,+
            ENST00000456328,sample_2,90,+
            ENST00000488147,sample_2,90,-""".replace(" ","")
        output_path = os.path.join(
            self.general_output_dir, "extract_assay_expression_data.csv"
        )
        # Generate the formatted output file
        write_expression_output(
            output_path,
            ",",
            False,
            self.vizserver_data_mock_response["results"],
        )
        # Read the output file back in and compare to expected result
        # Since the test should fail if the formatting is wrong, not just if the data is wrong, we
        # can do a simple string comparison
        with open(output_path, "r") as infile:
            data = infile.read()
        self.assertEqual(expected_result.strip(),data.strip())


    def test_output_sql_format(self):
        sql_mock_response = {
            "sql": "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression`, `expr_annotation_1`.`strand` AS `strand` FROM `database_gypg8qq06j8kzzp2yybfbzfk__enst_short_multiple_assays2`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gypg8qq06j8kzzp2yybfbzfk__enst_short_multiple_assays2`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`value` >= 1 AND `expr_annotation_1`.`chr` = '1' AND (`expr_annotation_1`.`end` BETWEEN 7 AND 250000000 OR `expr_annotation_1`.`start` BETWEEN 7 AND 250000000 OR `expr_annotation_1`.`end` >= 250000000 AND `expr_annotation_1`.`start` <= 7)"
        }
        expected_result = "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression`, `expr_annotation_1`.`strand` AS `strand` FROM `database_gypg8qq06j8kzzp2yybfbzfk__enst_short_multiple_assays2`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gypg8qq06j8kzzp2yybfbzfk__enst_short_multiple_assays2`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`value` >= 1 AND `expr_annotation_1`.`chr` = '1' AND (`expr_annotation_1`.`end` BETWEEN 7 AND 250000000 OR `expr_annotation_1`.`start` BETWEEN 7 AND 250000000 OR `expr_annotation_1`.`end` >= 250000000 AND `expr_annotation_1`.`start` <= 7)"
        output_path = os.path.join(
            self.general_output_dir, "extract_assay_expression_sql.csv"
        )
        # Generate the formatted output file
        write_expression_output(
            arg_output=output_path,
            arg_delim=",",
            arg_sql=True,
            output_listdict_or_string=sql_mock_response["sql"],
        )
        # Read the output file back in and compare to expected result
        # Since the test should fail if the formatting is wrong, not just if the data is wrong, we
        # can do a simple string comparison
        with open(output_path, "r") as infile:
            data = infile.read()
        self.assertEqual(expected_result.strip(),data.strip())

    #
    # Negative output tests
    #

    def test_output_sql_not_string(self):
        expected_error_message = "Expected SQL query to be a string"
        with self.assertRaises(ValueError) as cm:
            write_expression_output(
                arg_output="-",
                arg_delim=",",
                arg_sql=True,
                output_listdict_or_string=["not a string-formatted SQL query"],
                error_handler=self.common_value_error_handler
            )
        err_msg = str(cm.exception).strip()
        self.assertEqual(expected_error_message, err_msg)

    def test_output_bad_delimiter(self):
        bad_delim = "|"
        expected_error_message =  "Unsupported delimiter: {}".format(bad_delim)
        with self.assertRaises(ValueError) as cm:
            write_expression_output(
                arg_output= "-",
                arg_delim=bad_delim,
                arg_sql=False,
                output_listdict_or_string=self.vizserver_data_mock_response["results"],
                save_uncommon_delim_to_txt = False,
                error_handler=self.common_value_error_handler
            )
        err_msg = str(cm.exception).strip()
        self.assertEqual(expected_error_message, err_msg)
    
    # EM-14
    def test_output_already_exist(self):
        output_path = os.path.join(
            self.general_output_dir, "already_existing_output.csv"
        )
        expected_error_message = "{} already exists. Please specify a new file path".format(output_path)

        with open(output_path,"w") as outfile:
            outfile.write("this output file already created")

        with self.assertRaises(ValueError) as cm:
            write_expression_output(
                arg_output= output_path,
                arg_delim=",",
                arg_sql=False,
                output_listdict_or_string=self.vizserver_data_mock_response["results"],
                save_uncommon_delim_to_txt = False,
                error_handler=self.common_value_error_handler
            )

        err_msg = str(cm.exception).strip()
        self.assertEqual(expected_error_message, err_msg)

    def test_output_is_directory(self):
        output_path = os.path.join(
            self.general_output_dir, "directory"
        )
        expected_error_message = "{} is a directory. Please specify a new file path".format(output_path)
        os.mkdir(output_path)
        with self.assertRaises(ValueError) as cm:
            write_expression_output(
                arg_output= output_path,
                arg_delim=",",
                arg_sql=False,
                output_listdict_or_string=self.vizserver_data_mock_response["results"],
                save_uncommon_delim_to_txt = False,
                error_handler=self.common_value_error_handler
            )

        err_msg = str(cm.exception).strip()
        self.assertEqual(expected_error_message, err_msg)

    @unittest.skip
    def test_incorrect_file_extension(self):
        expected_error_message = 'File extension ".tsv" does not match delimiter ","'
        output_path = os.path.join(
            self.general_output_dir, "wrong_extension.tsv"
        )
        with self.assertRaises(ValueError) as cm:
            write_expression_output(
                arg_output= output_path,
                arg_delim=",",
                arg_sql=False,
                output_listdict_or_string=self.vizserver_data_mock_response["results"],
                save_uncommon_delim_to_txt = False,
                error_handler=self.common_value_error_handler
            )
        err_msg = str(cm.exception).strip()
        self.assertEqual(expected_error_message, err_msg)

    # EM-1
    # Test PATH argument not provided
    def test_path_missing(self):
        input_dict = {}
        expected_error_message = (
            'At least one of the following arguments is required: "Path", "--json-help"'
        )
        self.common_input_args_test(input_dict, expected_error_message)

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

    # EM-6-2
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
        expected_error_message = '"--additional-fields-help" cannot be passed with any option other than "--retrieve-expression".'
        input_dict = {
            "path": self.test_record,
            "assay_name": "test_assay",
            "additional_fields_help": True,
        }
        self.common_input_args_test(input_dict, expected_error_message)

    # EM-11
    # When invalid additional fields are passed
    def invalid_additional_fields(self):
        expected_error_message = "One or more of the supplied fields using --additional-fields are invalid. Please run --additional-fields-help for a list of valid fields"
        input_dict = {
            "path": self.test_record,
            "retrieve_expression": True,
            "filter_json": r'{"annotation": {"feature_id": ["ENSG0000001", "ENSG00000002"]}}',
            "additional_fields": "feature_name,bad_field",
        }
        self.common_input_args_test(input_dict, expected_error_message)

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
        self.common_input_args_test(input_dict, expected_error_message)

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

    # EM-16
    # When the string provided is a malformed JSON
    @unittest.skip
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
    # Note: this probably needs to be tested with a Popen rather than with the ArgsValidator function
    @unittest.skip
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

    # EM-21
    # When --json-help is passed with another option from --assay-name, --sql, --additional-fields, --expression-matix, --output
    def test_json_help_other_option(self):
        expected_error_message = '"--json-help" cannot be passed with any option other than "--retrieve-expression".'
        input_dict = {
            "path": self.test_record,
            "json_help": True,
            "assay_name": "test_assay",
        }
        self.common_input_args_test(input_dict, expected_error_message)

    # EM-22
    # When --expression-matrix is passed with other arguments other than, any context other than, --retrieve-expression
    # It seems that every combination of args that could be passed with this cause a different issue to be caught first
    # Which is fine but the error message will be for the other error
    @unittest.skip
    def test_exp_matrix_other_args(self):
        # expected_error_message = "--expression-matrix cannot be passed with any argument other than --retrieve-expression"
        expected_error_message = "--json-help cannot be passed with any of --assay-name, --sql, --additional-fields, --expression-matrix, or --output"
        input_dict = {
            "path": self.test_record,
            "expression_matrix": True,
            "additional_fields": "feature_name",
            "json_help": True,
        }
        self.common_input_args_test(input_dict, expected_error_message)

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
            "filter_json": r'{"annotation": {"feature_name": ["BRCA2"]}}',
            "sql": True,
        }
        self.common_input_args_test(input_dict, expected_error_message)

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
    # EM-18, EM-19, EM-20
    #

    def test_annotation_conflicting_keys(self):
        self.common_negative_filter_test(
            "annotation_conflicting_keys",
            "Conflicting keys feature_name and feature_id cannot be present together.",
        )

    @unittest.skip
    def test_annotation_id_maxitem(self):
        self.common_negative_filter_test(
            "annotation_id_maxitem", "error message not yet defined"
        )

    def test_annotation_id_type(self):
        self.common_negative_filter_test(
            "annotation_id_type",
            "Key 'feature_id' has an invalid type. Expected <{0} 'list'> but got <{0} 'dict'>".format(
                self.type_representation
            ).format(
                self.type_representation
            ),
        )

    @unittest.skip
    def test_annotation_name_maxitem(self):
        self.common_negative_filter_test(
            "annotation_name_maxitem",
            "Key 'feature_id' has an invalid type. Expected <{0} 'list'> but got <{0} 'dict'>".format(
                self.type_representation
            ),
        )

    def test_annotation_name_type(self):
        self.common_negative_filter_test(
            "annotation_name_type",
            "Key 'feature_name' has an invalid type. Expected <{0} 'list'> but got <{0} 'dict'>".format(
                self.type_representation
            ),
        )

    def test_annotation_type(self):
        self.common_negative_filter_test(
            "annotation_type",
            "Key 'annotation' has an invalid type. Expected <{0} 'dict'> but got <{0} 'list'>".format(
                self.type_representation
            ),
        )

    def test_bad_dependent_conditional(self):
        self.common_negative_filter_test(
            "bad_dependent_conditional",
            "When expression is present, one of the following keys must be also present: annotation, location.",
        )

    def test_bad_toplevel_key(self):
        self.common_negative_filter_test(
            "bad_toplevel_key", "Found following invalid filters: ['not_real_key']"
        )

    def test_conflicting_toplevel(self):
        self.common_negative_filter_test(
            "conflicting_toplevel",
            "Conflicting keys feature_name and feature_id cannot be present together.",
        )

    # EM-15
    def test_empty_dict(self):
        self.common_negative_filter_test(
            "empty_dict", "Input JSON must be a non-empty dict."
        )

    @unittest.skip
    def test_expression_empty_dict(self):
        self.common_negative_filter_test(
            "expression_empty_dict", "error message not yet defined"
        )

    def test_expression_max_type(self):
        self.common_negative_filter_test(
            "expression_max_type",
            "Key 'max_value' has an invalid type. Expected (<{0} 'int'>, <{0} 'float'>) but got <{0} 'str'>".format(
                self.type_representation
            ),
        )

    def test_expression_min_type(self):
        self.common_negative_filter_test(
            "expression_min_type",
            "Key 'min_value' has an invalid type. Expected (<{0} 'int'>, <{0} 'float'>) but got <{0} 'str'>".format(
                self.type_representation
            ),
        )

    def test_expression_type(self):
        self.common_negative_filter_test(
            "expression_type",
            "Key 'expression' has an invalid type. Expected <{0} 'dict'> but got <{0} 'list'>".format(
                self.type_representation
            ),
        )

    def test_location_chrom_type(self):
        self.common_negative_filter_test(
            "location_chrom_type",
            "Key 'chromosome' has an invalid type. Expected <{0} 'str'> but got <{0} 'int'>".format(
                self.type_representation
            ),
        )

    @unittest.skip
    def test_location_end_before_start(self):
        self.common_negative_filter_test(
            "location_end_before_start", "error message not yet defined"
        )

    def test_location_end_type(self):
        self.common_negative_filter_test(
            "location_end_type",
            "Key 'ending_position' has an invalid type. Expected <{0} 'str'> but got <{0} 'int'>".format(
                self.type_representation
            ),
        )

    def test_location_item_type(self):
        self.common_negative_filter_test(
            "location_item_type",
            "Expected items of type <{0} 'dict'> but got <{0} 'list'>".format(
                self.type_representation
            ),
        )

    @unittest.skip
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
            "Key 'starting_position' has an invalid type. Expected <{0} 'str'> but got <{0} 'int'>".format(
                self.type_representation
            ),
        )

    def test_location_type(self):
        self.common_negative_filter_test(
            "location_type",
            "Key 'location' has an invalid type. Expected <{0} 'list'> but got <{0} 'dict'>".format(
                self.type_representation
            ),
        )

    @unittest.skip
    def test_sample_id_maxitem(self):
        self.common_negative_filter_test(
            "sample_id_maxitem", "error message not yet defined"
        )

    def test_sample_id_type(self):
        self.common_negative_filter_test(
            "sample_id_type",
            "Key 'sample_id' has an invalid type. Expected <{0} 'list'> but got <{0} 'dict'>".format(
                self.type_representation
            ),
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

    ##### Test argparse's --help output
    def test_argparse_help_txt(self):
        expected_result = self.argparse_expression_help_message
        with open(expected_result) as f:
            #lines = f.readlines()
            file = f.read()
        process = subprocess.check_output("dx extract_assay expression -h", shell=True)
        help_output = process.decode()

        # In Python 3 self.assertEqual(file,help_output) passes,
        # However in Python 2 it fails due to some differences in where linebreaks appear in the text
        self.assertEqual(
            file.replace(" ", "").replace("\n", ""), 
            help_output.replace(" ", "").replace("\n", "")
        )

    #### Test --json-help
    def test_json_help_template(self):
        process = subprocess.check_output("dx extract_assay expression --retrieve-expression fakepath --json-help", shell=True)
        self.assertIn(EXTRACT_ASSAY_EXPRESSION_JSON_TEMPLATE, process.decode())
        self.assertIn("Additional descriptions of filtering keys and permissible values", process.decode())


    def load_record_via_dataset_class(self, record_path):
        _, _, entity = resolve_existing_path(record_path)
        entity_describe = entity["describe"]
        record = DXRecord(entity_describe["id"], entity_describe["project"])
        dataset, cohort_info = Dataset.resolve_cohort_to_dataset(record)

        return dataset, cohort_info, record
    
    def test_dataset_class_basic(self):
        dataset, cohort, record = self.load_record_via_dataset_class(self.expression_dataset)

        record_details = record.describe(default_fields=True, fields={"properties", "details"})
        
        self.assertIsNone(cohort)
        self.assertEqual(dataset.descriptor_file_dict["name"], self.expression_dataset_name)
        self.assertIn("vizserver", dataset.visualize_info["url"])
        self.assertEqual("3.0", dataset.visualize_info["version"])
        self.assertEqual("3.0", dataset.visualize_info["datasetVersion"])
        self.assertEqual(dataset.descriptor_file, record_details["details"]["descriptor"]["$dnanexus_link"])
        self.assertIn("molecular_expression1", dataset.assay_names_list("molecular_expression"))
        self.assertEqual(dataset.detail_describe["types"], record_details["types"])

    def test_dataset_class_cohort_resolution(self):
        dataset, cohort, record = self.load_record_via_dataset_class(self.combined_expression_cohort)

        record_details = record.describe(default_fields=True, fields={"properties", "details"})
        expected_dataset_id = record_details["details"]["dataset"]["$dnanexus_link"]
        expected_dataset_describe = DXRecord(expected_dataset_id).describe(default_fields=True, fields={"properties", "details"})
        expected_descriptor_id = expected_dataset_describe["details"]["descriptor"]["$dnanexus_link"]
        
        self.assertIsNotNone(cohort)
        self.assertIn("SELECT `sample_id`", cohort["details"]["baseSql"])
        self.assertIn("pheno_filters", cohort["details"]["filters"])
        self.assertIn("CohortBrowser", cohort["types"])
        self.assertEqual(dataset.get_id(), expected_dataset_id)
        self.assertEqual(dataset.descriptor_file, expected_descriptor_id)
        self.assertIn("molecular_expression1", dataset.assay_names_list("molecular_expression"))
        self.assertEqual("molecular_expression", dataset.descriptor_file_dict["assays"][0]["generalized_assay_model"])
        self.assertIn("Dataset", dataset.detail_describe["types"])
        self.assertIn("vizserver", dataset.vizserver_url)


# Start the test
if __name__ == "__main__":
    unittest.main()
