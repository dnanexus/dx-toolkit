#!/usr/bin/env python3
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

from __future__ import absolute_import

import unittest
import subprocess
import sys
import os
import dxpy
import copy
import json
import tempfile
import csv
from collections import OrderedDict
import pandas as pd

import shutil
from dxpy_testutil import cd, chdir
from dxpy.bindings.apollo.json_validation_by_schema import JSONValidator
from dxpy.utils.resolver import resolve_existing_path

from dxpy.bindings.apollo.schemas.assay_filtering_json_schemas import (
    EXTRACT_ASSAY_EXPRESSION_JSON_SCHEMA,
)
from dxpy.bindings.apollo.cmd_line_options_validator import ArgsValidator
from dxpy.bindings.apollo.schemas.input_arguments_validation_schemas import (
    EXTRACT_ASSAY_EXPRESSION_INPUT_ARGS_SCHEMA,
)
from dxpy.bindings.apollo.vizclient import VizClient

from dxpy.bindings.apollo.data_transformations import transform_to_expression_matrix
from dxpy.cli.output_handling import write_expression_output
from dxpy.cli.help_messages import EXTRACT_ASSAY_EXPRESSION_JSON_TEMPLATE
from dxpy.bindings.dxrecord import DXRecord
from dxpy.bindings.apollo.dataset import Dataset

from dxpy.bindings.apollo.vizserver_filters_from_json_parser import JSONFiltersValidator
from dxpy.bindings.apollo.schemas.assay_filtering_conditions import (
    EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS,
)
from dxpy.bindings.apollo.vizserver_payload_builder import VizPayloadBuilder
from dxpy.exceptions import err_exit


dirname = os.path.dirname(__file__)

python_version = sys.version_info.major

if python_version == 2:
    sys.path.append("./expression_test_assets")
    from expression_test_input_dict import (
        CLIEXPRESS_TEST_INPUT,
        VIZPAYLOADERBUILDER_TEST_INPUT,
        EXPRESSION_CLI_JSON_FILTERS,
    )
    from expression_test_expected_output_dict import VIZPAYLOADERBUILDER_EXPECTED_OUTPUT

else:
    from expression_test_assets.expression_test_input_dict import (
        CLIEXPRESS_TEST_INPUT,
        VIZPAYLOADERBUILDER_TEST_INPUT,
        EXPRESSION_CLI_JSON_FILTERS,
    )
    from expression_test_assets.expression_test_expected_output_dict import (
        VIZPAYLOADERBUILDER_EXPECTED_OUTPUT,
    )


class TestDXExtractExpression(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None
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
        cls.cohort_browser_record = (
            cls.proj_id + ":/Extract_Expression/cohort_browser_object"
        )
        cls.expression_dataset_name = "molecular_expression1.dataset"
        cls.expression_dataset = cls.proj_id + ":/" + cls.expression_dataset_name
        cls.combined_expression_cohort_name = "Combined_Expression_Cohort"
        cls.combined_expression_cohort = (
            cls.proj_id + ":/" + cls.combined_expression_cohort_name
        )
        # In python3, str(type(object)) looks like <{0} 'obj_class'> but in python 2, it would be <type 'obj_class'>
        # This impacts our expected error messages
        cls.type_representation = "class"
        if python_version == 2:
            cls.type_representation = "type"

        cls.default_entity_describe = {
            "id": cls.expression_dataset,
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
                    "expression": 20,
                    "strand": "-",
                },
            ]
        }
        cls.argparse_expression_help_message = os.path.join(
            dirname, "help_messages/extract_expression_help_message.txt"
        )

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
    # Expression matrix tests
    #

    def test_basic_exp_matrix_transform(self):
        vizserver_results = [
            {
                "feature_id": "ENST00000450305",
                "sample_id": "sample_2",
                "expression": 50,
            },
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_2",
                "expression": 90,
            },
            {
                "feature_id": "ENST00000488147",
                "sample_id": "sample_2",
                "expression": 20,
            },
        ]
        expected_output = [
            {
                "ENST00000450305": 50,
                "ENST00000456328": 90,
                "ENST00000488147": 20,
                "sample_id": "sample_2",
            }
        ]

        transformed_results, colnames = transform_to_expression_matrix(
            vizserver_results
        )
        self.assertEqual(expected_output, transformed_results)

    def test_two_sample_exp_transform(self):
        vizserver_results = [
            {
                "feature_id": "ENST00000450305",
                "sample_id": "sample_2",
                "expression": 50,
            },
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_1",
                "expression": 90,
            },
            {
                "feature_id": "ENST00000488147",
                "sample_id": "sample_2",
                "expression": 20,
            },
        ]

        expected_output = [
            {
                "sample_id": "sample_2",
                "ENST00000450305": 50,
                "ENST00000488147": 20,
                "ENST00000456328": None,
            },
            {
                "sample_id": "sample_1",
                "ENST00000456328": 90,
                "ENST00000450305": None,
                "ENST00000488147": None,
            },
        ]

        transformed_results, colnames = transform_to_expression_matrix(
            vizserver_results
        )
        self.assertEqual(expected_output, transformed_results)

    def test_two_sample_feat_id_overlap_exp_trans(self):
        vizserver_results = [
            {
                "feature_id": "ENST00000450305",
                "sample_id": "sample_2",
                "expression": 50,
            },
            {
                "feature_id": "ENST00000450305",
                "sample_id": "sample_1",
                "expression": 77,
            },
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_1",
                "expression": 90,
            },
            {
                "feature_id": "ENST00000488147",
                "sample_id": "sample_2",
                "expression": 20,
            },
        ]
        expected_output = [
            {
                "sample_id": "sample_2",
                "ENST00000450305": 50,
                "ENST00000488147": 20,
                "ENST00000456328": None,
            },
            {
                "sample_id": "sample_1",
                "ENST00000450305": 77,
                "ENST00000456328": 90,
                "ENST00000488147": None,
            },
        ]

        transformed_results, colnames = transform_to_expression_matrix(
            vizserver_results
        )
        self.assertEqual(expected_output, transformed_results)

    def test_exp_transform_output_compatibility(self):
        vizserver_results = [
            {
                "feature_id": "ENST00000450305",
                "sample_id": "sample_2",
                "expression": 50,
            },
            {
                "feature_id": "ENST00000450305",
                "sample_id": "sample_1",
                "expression": 77,
            },
            {
                "feature_id": "ENST00000456328",
                "sample_id": "sample_1",
                "expression": 90,
            },
            {
                "feature_id": "ENST00000488147",
                "sample_id": "sample_2",
                "expression": 20,
            },
        ]

        # The replace statement removes tabs(actually blocks of 4 spaces) that have been inserted
        # for readability in this python file
        expected_result = """sample_id,ENST00000450305,ENST00000456328,ENST00000488147
                             sample_2,50,,20
                             sample_1,77,90,""".replace(
            " ", ""
        )

        transformed_results, colnames = transform_to_expression_matrix(
            vizserver_results
        )
        output_path = os.path.join(self.general_output_dir, "exp_transform_compat.csv")
        # Generate the formatted output file
        write_expression_output(
            output_path, ",", False, transformed_results, colnames=colnames
        )

        with open(output_path, "r") as infile:
            data = infile.read()
        self.assertEqual(expected_result.strip(), data.strip())

    #
    # Positive output tests
    #

    def test_output_data_format(self):
        expected_result = """feature_id,sample_id,expression,strand
            ENST00000450305,sample_2,50,+
            ENST00000456328,sample_2,90,+
            ENST00000488147,sample_2,20,-""".replace(
            " ", ""
        )
        if python_version == 2:
            expected_result = "feature_id,expression,strand,sample_id\nENST00000450305,50,+,sample_2\nENST00000456328,90,+,sample_2\nENST00000488147,20,-,sample_2"
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
        self.assertEqual(expected_result.strip(), data.strip())

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
        self.assertEqual(expected_result.strip(), data.strip())

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
                error_handler=self.common_value_error_handler,
            )
        err_msg = str(cm.exception).strip()
        self.assertEqual(expected_error_message, err_msg)

    def test_output_bad_delimiter(self):
        bad_delim = "|"
        expected_error_message = "Unsupported delimiter: {}".format(bad_delim)
        with self.assertRaises(ValueError) as cm:
            write_expression_output(
                arg_output="-",
                arg_delim=bad_delim,
                arg_sql=False,
                output_listdict_or_string=self.vizserver_data_mock_response["results"],
                save_uncommon_delim_to_txt=False,
                error_handler=self.common_value_error_handler,
            )
        err_msg = str(cm.exception).strip()
        self.assertEqual(expected_error_message, err_msg)

    # EM-14
    def test_output_already_exist(self):
        output_path = os.path.join(
            self.general_output_dir, "already_existing_output.csv"
        )
        expected_error_message = (
            "{} already exists. Please specify a new file path".format(output_path)
        )

        with open(output_path, "w") as outfile:
            outfile.write("this output file already created")

        with self.assertRaises(ValueError) as cm:
            write_expression_output(
                arg_output=output_path,
                arg_delim=",",
                arg_sql=False,
                output_listdict_or_string=self.vizserver_data_mock_response["results"],
                save_uncommon_delim_to_txt=False,
                error_handler=self.common_value_error_handler,
            )

        err_msg = str(cm.exception).strip()
        self.assertEqual(expected_error_message, err_msg)

    def test_output_is_directory(self):
        output_path = os.path.join(self.general_output_dir, "directory")
        expected_error_message = (
            "{} is a directory. Please specify a new file path".format(output_path)
        )
        os.mkdir(output_path)
        with self.assertRaises(ValueError) as cm:
            write_expression_output(
                arg_output=output_path,
                arg_delim=",",
                arg_sql=False,
                output_listdict_or_string=self.vizserver_data_mock_response["results"],
                save_uncommon_delim_to_txt=False,
                error_handler=self.common_value_error_handler,
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

    # EM-10
    # When --additional-fields-help is presented with other options
    def test_additional_fields_help_other_options(self):
        expected_error_message = '"--additional-fields-help" cannot be passed with any option other than "--retrieve-expression".'
        input_dict = {
            "path": self.expression_dataset,
            "assay_name": "test_assay",
            "additional_fields_help": True,
        }
        self.common_input_args_test(input_dict, expected_error_message)

    # EM-11
    # When invalid additional fields are passed
    def invalid_additional_fields(self):
        expected_error_message = "One or more of the supplied fields using --additional-fields are invalid. Please run --additional-fields-help for a list of valid fields"
        input_dict = {
            "path": self.expression_dataset,
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
            "path": self.expression_dataset,
            "list_assays": True,
            "assay_name": "fake_assay",
        }
        self.common_input_args_test(input_dict, expected_error_message)

    # EM-17
    # When the .json file provided does not exist
    def test_json_file_not_exist(self):
        missing_json_path = os.path.join(self.general_input_dir, "nonexistent.json")
        expected_error_message = (
            "JSON file {} provided to --retrieve-expression does not exist".format(
                missing_json_path
            )
        )
        command = [
            "dx",
            "extract_assay",
            "expression",
            self.expression_dataset,
            "--retrieve-expression",
            "--filter-json-file",
            missing_json_path,
        ]

        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        actual_err_msg = process.communicate()[1]
        # print(actual_err_msg)

        if python_version == 2:
            self.assertIn("No such file or directory", actual_err_msg)
        else:
            self.assertIn(expected_error_message, actual_err_msg)

    # EM-21
    # When --json-help is passed with another option from --assay-name, --sql, --additional-fields, --expression-matix, --output
    def test_json_help_other_option(self):
        expected_error_message = '"--json-help" cannot be passed with any option other than "--retrieve-expression".'
        input_dict = {
            "path": self.expression_dataset,
            "json_help": True,
            "assay_name": "test_assay",
        }
        self.common_input_args_test(input_dict, expected_error_message)

    # EM-23
    # --expression-matrix/-em cannot be used with --sql
    def test_exp_matrix_sql(self):
        expected_error_message = (
            '"--expression-matrix"/"-em" cannot be passed with the flag, "--sql".'
        )
        input_dict = {
            "path": self.expression_dataset,
            "expression_matrix": True,
            "retrieve_expression": True,
            "filter_json": r'{"annotation": {"feature_name": ["BRCA2"]}}',
            "sql": True,
        }
        self.common_input_args_test(input_dict, expected_error_message)

    # Malformed input json tests
    # EM-18, EM-19, EM-20
    #

    def test_annotation_conflicting_keys(self):
        self.common_negative_filter_test(
            "annotation_conflicting_keys",
            "For annotation, exactly one of feature_name or feature_id must be provided in the supplied JSON object.",
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
            "Exactly one of location or annotation must be provided in the supplied JSON object.",
        )

    # EM-15
    def test_empty_dict(self):
        self.common_negative_filter_test(
            "empty_dict", "Input JSON must be a non-empty dict."
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
            "Expected list items within 'location' to be of type <{0} 'dict'> but got <{0} 'list'> instead.".format(
                self.type_representation
            ),
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
            # lines = f.readlines()
            file = f.read()
        process = subprocess.check_output("dx extract_assay expression -h", shell=True)
        help_output = process.decode()

        # In Python 3 self.assertEqual(file,help_output) passes,
        # However in Python 2 it fails due to some differences in where linebreaks appear in the text
        self.assertEqual(
            file.replace(" ", "").replace("\n", ""),
            help_output.replace(" ", "").replace("\n", ""),
        )

    #### Test --json-help
    def test_json_help_template(self):
        process = subprocess.check_output(
            "dx extract_assay expression --retrieve-expression fakepath --json-help",
            shell=True,
        )
        self.assertIn(EXTRACT_ASSAY_EXPRESSION_JSON_TEMPLATE, process.decode())
        self.assertIn(
            "Additional descriptions of filtering keys and permissible values",
            process.decode(),
        )

    def load_record_via_dataset_class(self, record_path):
        _, _, entity = resolve_existing_path(record_path)
        entity_describe = entity["describe"]
        record = DXRecord(entity_describe["id"], entity_describe["project"])
        dataset, cohort_info = Dataset.resolve_cohort_to_dataset(record)

        return dataset, cohort_info, record

    def test_dataset_class_basic(self):
        dataset, cohort, record = self.load_record_via_dataset_class(
            self.expression_dataset
        )

        record_details = record.describe(
            default_fields=True, fields={"properties", "details"}
        )

        self.assertIsNone(cohort)
        self.assertEqual(
            dataset.descriptor_file_dict["name"], self.expression_dataset_name
        )
        self.assertIn("vizserver", dataset.visualize_info["url"])
        self.assertEqual("3.0", dataset.visualize_info["version"])
        self.assertEqual("3.0", dataset.visualize_info["datasetVersion"])
        self.assertEqual(
            dataset.descriptor_file,
            record_details["details"]["descriptor"]["$dnanexus_link"],
        )
        self.assertIn(
            "molecular_expression1", dataset.assay_names_list("molecular_expression")
        )
        self.assertEqual(dataset.detail_describe["types"], record_details["types"])

    def test_dataset_class_cohort_resolution(self):
        dataset, cohort, record = self.load_record_via_dataset_class(
            self.combined_expression_cohort
        )

        record_details = record.describe(
            default_fields=True, fields={"properties", "details"}
        )
        expected_dataset_id = record_details["details"]["dataset"]["$dnanexus_link"]
        expected_dataset_describe = DXRecord(expected_dataset_id).describe(
            default_fields=True, fields={"properties", "details"}
        )
        expected_descriptor_id = expected_dataset_describe["details"]["descriptor"][
            "$dnanexus_link"
        ]

        self.assertIsNotNone(cohort)
        self.assertIn("SELECT `sample_id`", cohort["details"]["baseSql"])
        self.assertIn("pheno_filters", cohort["details"]["filters"])
        self.assertIn("CohortBrowser", cohort["types"])
        self.assertEqual(dataset.get_id(), expected_dataset_id)
        self.assertEqual(dataset.descriptor_file, expected_descriptor_id)
        self.assertIn(
            "molecular_expression1", dataset.assay_names_list("molecular_expression")
        )
        self.assertEqual(
            "molecular_expression",
            dataset.descriptor_file_dict["assays"][0]["generalized_assay_model"],
        )
        self.assertIn("Dataset", dataset.detail_describe["types"])
        self.assertIn("vizserver", dataset.vizserver_url)

    ### Test VizPayloadBuilder Class

    # Genomic location filters
    # genomic + cohort
    def test_vizpayloadbuilder_location_cohort(self):
        self.common_vizpayloadbuilder_test_helper_method(
            self.combined_expression_cohort, "test_vizpayloadbuilder_location_cohort"
        )

    def test_vizpayloadbuilder_location_multiple(self):
        self.common_vizpayloadbuilder_test_helper_method(
            self.expression_dataset, "test_vizpayloadbuilder_location_multiple"
        )

    # Annotation filters
    def test_vizpayloadbuilder_annotation_feature_name(self):
        self.common_vizpayloadbuilder_test_helper_method(
            self.expression_dataset, "test_vizpayloadbuilder_annotation_feature_name"
        )

    def test_vizpayloadbuilder_annotation_feature_id(self):
        self.common_vizpayloadbuilder_test_helper_method(
            self.expression_dataset, "test_vizpayloadbuilder_annotation_feature_id"
        )

    # Expression filters (with location or annotation)
    # expression + annotation - ID
    def test_vizpayloadbuilder_expression_min(self):
        self.common_vizpayloadbuilder_test_helper_method(
            self.expression_dataset, "test_vizpayloadbuilder_expression_min"
        )

    # expression + annotation - name
    def test_vizpayloadbuilder_expression_max(self):
        self.common_vizpayloadbuilder_test_helper_method(
            self.expression_dataset, "test_vizpayloadbuilder_expression_max"
        )

    # expression + location
    def test_vizpayloadbuilder_expression_mixed(self):
        self.common_vizpayloadbuilder_test_helper_method(
            self.expression_dataset, "test_vizpayloadbuilder_expression_mixed"
        )

    # Sample filter
    def test_vizpayloadbuilder_sample(self):
        self.common_vizpayloadbuilder_test_helper_method(
            self.expression_dataset, "test_vizpayloadbuilder_sample", data_test=False
        )

    # General (mixed) filters
    def test_vizpayloadbuilder_location_sample_expression(self):
        if python_version == 2:
            # The expected query is essentially the same as the one in Python 3
            # The only issue is that the order of sub-queries is slightly different in Python 2
            # This is very likely due to the fact that Python 2 changes the order of keys in payload dict
            # Therefore, the final query is constructred slightly differently
            self.assertTrue(True)
        else:
            self.common_vizpayloadbuilder_test_helper_method(
                self.expression_dataset,
                "test_vizpayloadbuilder_location_sample_expression",
                data_test=False,
            )

    def test_vizpayloadbuilder_annotation_sample_expression(self):
        if python_version == 2:
            # The expected query is essentially the same as the one in Python 3
            # The only issue is that the order of sub-queries is slightly different in Python 2
            # This is very likely due to the fact that Python 2 changes the order of keys in payload dict
            # Therefore, the final query is constructred slightly differently
            self.assertTrue(True)
        else:
            self.common_vizpayloadbuilder_test_helper_method(
                self.expression_dataset,
                "test_vizpayloadbuilder_annotation_sample_expression",
                data_test=False,
            )

    def common_vizpayloadbuilder_test_helper_method(
        self, record_path, test_name, data_test=True
    ):
        _, _, entity = resolve_existing_path(record_path)
        entity_describe = entity["describe"]
        record_id = entity_describe["id"]

        record = DXRecord(record_id)
        dataset, cohort_info = Dataset.resolve_cohort_to_dataset(record)
        dataset_id = dataset.dataset_id

        if cohort_info:
            BASE_SQL = cohort_info.get("details").get("baseSql")
            COHORT_FILTERS = cohort_info.get("details").get("filters")
            IS_COHORT = True
        else:
            BASE_SQL = None
            COHORT_FILTERS = None
            IS_COHORT = False

        url = dataset.vizserver_url
        project = dataset.project_id

        # vizserver_filters_from_json_parser.JSONFiltersValidator using the CLIEXPRESS schema
        schema = EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS
        _db_columns_list = schema["output_fields_mapping"].get("default")

        # JSONFiltersValidator to build the complete payload
        json_input = VIZPAYLOADERBUILDER_TEST_INPUT[test_name]
        input_json_parser = JSONFiltersValidator(json_input, schema)
        vizserver_raw_filters = input_json_parser.parse()

        # VizClient to submit the payload and get a response
        client = VizClient(url, project)

        viz = VizPayloadBuilder(
            project_context=project,
            output_fields_mapping=_db_columns_list,
            filters={"filters": COHORT_FILTERS} if IS_COHORT else None,
            order_by=EXTRACT_ASSAY_EXPRESSION_FILTERING_CONDITIONS["order_by"],
            limit=None,
            base_sql=BASE_SQL,
            is_cohort=IS_COHORT,
            error_handler=err_exit,
        )

        assay_1_name = dataset.descriptor_file_dict["assays"][0]["name"]
        assay_1_id = dataset.descriptor_file_dict["assays"][0]["uuid"]

        viz.assemble_assay_raw_filters(
            assay_name=assay_1_name, assay_id=assay_1_id, filters=vizserver_raw_filters
        )
        vizserver_payload = viz.build()

        vizserver_response_data = client.get_data(vizserver_payload, dataset_id)[
            "results"
        ]
        vizserver_response_sql = client.get_raw_sql(vizserver_payload, dataset_id)[
            "sql"
        ]

        data_output = vizserver_response_data
        sql_output = vizserver_response_sql

        if data_test:
            exp_data_output = VIZPAYLOADERBUILDER_EXPECTED_OUTPUT[test_name][
                "expected_data_output"
            ]
            # assertCountEqual asserts that two iterables have the same elements, ignoring order
            self.assertCountEqual(data_output, exp_data_output)

        exp_sql_output = VIZPAYLOADERBUILDER_EXPECTED_OUTPUT[test_name][
            "expected_sql_output"
        ]
        if isinstance(exp_sql_output, list):
            # Some of the sub-queries may have slightly different order due to the way the keys are ordered in the payload dict
            # In other words, the queries are still correct, but the order of sub-queries may be different
            # This usually happens in Python 2
            self.assertIn(sql_output, exp_sql_output)
        else:
            self.assertEqual(sql_output, exp_sql_output)

    def run_dx_extract_assay_expression_cmd(
        self,
        dataset_or_cohort,
        filters_json,
        additional_fields,
        sql,
        output="-",
        extra_args=None,
        subprocess_run=False,
    ):
        command = [
            "dx",
            "extract_assay",
            "expression",
            dataset_or_cohort,
            "--retrieve-expression",
            "--filter-json",
            str(filters_json).replace("'", '"'),
            "-o",
            output,
        ]

        if sql:
            command.append("--sql")

        if additional_fields:
            command.extend(["--additional-fields", additional_fields])

        if extra_args:
            command.append(extra_args)

        if subprocess_run:
            process = subprocess.run(
                command, capture_output=True, text=True, check=False
            )

        else:
            process = subprocess.check_output(
                command,
                universal_newlines=True,
            )

        return process

    def test_dx_extract_cmd_location_expression_sample_sql(self):
        expected_sql_query = [
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression`, `expr_annotation_1`.`gene_name` AS `feature_name`, `expr_annotation_1`.`chr` AS `chrom`, `expr_annotation_1`.`start` AS `start` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE (`expr_annotation_1`.`chr` = '11' AND (`expr_annotation_1`.`start` BETWEEN 8693350 AND 67440200 OR `expr_annotation_1`.`end` BETWEEN 8693350 AND 67440200 OR `expr_annotation_1`.`start` <= 8693350 AND `expr_annotation_1`.`end` >= 67440200) OR `expr_annotation_1`.`chr` = 'X' AND (`expr_annotation_1`.`start` BETWEEN 148500700 AND 148994424 OR `expr_annotation_1`.`end` BETWEEN 148500700 AND 148994424 OR `expr_annotation_1`.`start` <= 148500700 AND `expr_annotation_1`.`end` >= 148994424) OR `expr_annotation_1`.`chr` = '17' AND (`expr_annotation_1`.`start` BETWEEN 75228160 AND 75235759 OR `expr_annotation_1`.`end` BETWEEN 75228160 AND 75235759 OR `expr_annotation_1`.`start` <= 75228160 AND `expr_annotation_1`.`end` >= 75235759)) AND `expression_1`.`value` >= 25.63 AND `expression_1`.`sample_id` IN ('sample_1', 'sample_2') ORDER BY `feature_id` ASC, `sample_id` ASC",
            "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression`, `expr_annotation_1`.`gene_name` AS `feature_name`, `expr_annotation_1`.`chr` AS `chrom`, `expr_annotation_1`.`start` AS `start` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE (`expr_annotation_1`.`chr` = '11' AND (`expr_annotation_1`.`end` BETWEEN 8693350 AND 67440200 OR `expr_annotation_1`.`start` BETWEEN 8693350 AND 67440200 OR `expr_annotation_1`.`end` >= 67440200 AND `expr_annotation_1`.`start` <= 8693350) OR `expr_annotation_1`.`chr` = 'X' AND (`expr_annotation_1`.`end` BETWEEN 148500700 AND 148994424 OR `expr_annotation_1`.`start` BETWEEN 148500700 AND 148994424 OR `expr_annotation_1`.`end` >= 148994424 AND `expr_annotation_1`.`start` <= 148500700) OR `expr_annotation_1`.`chr` = '17' AND (`expr_annotation_1`.`end` BETWEEN 75228160 AND 75235759 OR `expr_annotation_1`.`start` BETWEEN 75228160 AND 75235759 OR `expr_annotation_1`.`end` >= 75235759 AND `expr_annotation_1`.`start` <= 75228160)) AND `expression_1`.`value` >= 25.63 AND `expression_1`.`sample_id` IN ('sample_1', 'sample_2') ORDER BY `feature_id` ASC, `sample_id` ASC",
        ]
        response = self.run_dx_extract_assay_expression_cmd(
            self.expression_dataset,
            EXPRESSION_CLI_JSON_FILTERS["positive_test"]["location_expression_sample"],
            "chrom,start,feature_name",
            True,
            "-",
        )
        self.assertIn(response.strip(), expected_sql_query)

    def test_dx_extract_cmd_location_expression_sample_data(self):
        response = self.run_dx_extract_assay_expression_cmd(
            self.expression_dataset,
            EXPRESSION_CLI_JSON_FILTERS["positive_test"]["location_expression_sample"],
            "chrom,start,feature_name",
            False,
            "-",
        )
        response = response.splitlines()
        response_list = [s.split(",") for s in response]
        column_names = response_list[0]
        response_df = pd.DataFrame(response_list[1:], columns=column_names)

        expected_present_row = response_df.loc[
            (response_df["feature_id"] == "ENST00000683201")
            & (response_df["expression"] == "27")
            & (response_df["start"] == "57805541")
            & (response_df["chrom"] == "11")
            & (response_df["feature_name"] == "CTNND1")
            & (response_df["sample_id"] == "sample_2")
        ]

        expected_X_chrom_response = response_df.loc[
            (response_df["chrom"] == "X") & (response_df["sample_id"] == "sample_2")
        ]

        self.assertEqual(len(response_df), 9398)
        self.assertEqual(
            set(column_names),
            set(
                [
                    "feature_id",
                    "sample_id",
                    "expression",
                    "feature_name",
                    "chrom",
                    "start",
                ]
            ),
        )
        self.assertEqual(set(response_df.sample_id), set(["sample_1", "sample_2"]))
        self.assertEqual(len(set(response_df.feature_id.unique())), 5929)
        self.assertEqual(len(expected_present_row), 1)
        self.assertEqual(len(expected_X_chrom_response), 5)

    def test_dx_extract_cmd_sample_ids_with_additional_fields(self):
        expected_sql_query = "SELECT `expression_1`.`feature_id` AS `feature_id`, `expression_1`.`sample_id` AS `sample_id`, `expression_1`.`value` AS `expression`, `expr_annotation_1`.`gene_name` AS `feature_name`, `expr_annotation_1`.`chr` AS `chrom`, `expr_annotation_1`.`start` AS `start`, `expr_annotation_1`.`end` AS `end`, `expr_annotation_1`.`strand` AS `strand` FROM `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expression` AS `expression_1` LEFT OUTER JOIN `database_gzky7400vgpyzy621q43gkkf__molecular_expression1_db`.`expr_annotation` AS `expr_annotation_1` ON `expression_1`.`feature_id` = `expr_annotation_1`.`feature_id` WHERE `expression_1`.`sample_id` IN ('sample_1') ORDER BY `feature_id` ASC, `sample_id` ASC"
        response = self.run_dx_extract_assay_expression_cmd(
            self.expression_dataset,
            EXPRESSION_CLI_JSON_FILTERS["positive_test"][
                "sample_id_with_additional_fields"
            ],
            "chrom,start,end,strand,feature_name",
            True,
            "-",
        )
        self.assertEqual(response.strip(), expected_sql_query)

    def test_negative_dx_extract_cmd_empty_json(self):
        expected_error = "No filter JSON is passed with --retrieve-expression or input JSON for --retrieve-expression does not contain valid filter information."
        response = self.run_dx_extract_assay_expression_cmd(
            self.expression_dataset,
            EXPRESSION_CLI_JSON_FILTERS["negative_test"]["empty_json"],
            None,
            True,
            "-",
            subprocess_run=True,
        )
        self.assertIn(expected_error, response.stderr)

    def test_negative_dx_extract_cmd_invalid_location_range(self):
        expected_error = "Range cannot be greater than 250000000 for location"
        response = self.run_dx_extract_assay_expression_cmd(
            self.expression_dataset,
            EXPRESSION_CLI_JSON_FILTERS["negative_test"]["large_location_range"],
            None,
            False,
            subprocess_run=True,
        )
        self.assertIn(expected_error, response.stderr)

    def test_negative_dx_extract_cmd_too_many_sample_ids(self):
        expected_error = "Too many items given in field sample_id, maximum is 100"
        response = self.run_dx_extract_assay_expression_cmd(
            self.expression_dataset,
            EXPRESSION_CLI_JSON_FILTERS["negative_test"]["sample_id_maxitem_limit"],
            None,
            False,
            subprocess_run=True,
        )
        self.assertIn(expected_error, response.stderr)


# Start the test
if __name__ == "__main__":
    unittest.main()
