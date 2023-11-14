#!/usr/bin/env python3

from __future__ import print_function, unicode_literals, division, absolute_import

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

# Run manually with python2 and python3 src/python/test/test_create_cohort.py


import json
import unittest
import os
import re
import subprocess
import dxpy
import sys
import hashlib
import uuid
from parameterized import parameterized
from dxpy.bindings import DXRecord, DXProject

from dxpy.cli.dataset_utilities import (
    resolve_validate_dx_path,
    validate_project_access,
    resolve_validate_record_path,
    raw_cohort_query_api_call
)
from dxpy.dx_extract_utils.cohort_filter_payload import (
    generate_pheno_filter,
    cohort_filter_payload,
    cohort_final_payload,
)

dirname = os.path.dirname(__file__)
payloads_dir = os.path.join(dirname, "create_cohort_test_files/payloads/")

python_version = sys.version_info.major

class DescribeDetails:
    """
    Strictly parses describe output into objects attributes.
    ID                                record-GYvjYf00F69fGVYkgXqfzfQ2
    Class                             record
    ...
    Size                              620
    """
    def __init__(self, describe):
        self.parse_atributes(describe)

    def parse_atributes(self, describe):
        for line in describe.split("\n"):
            if line != "":
                p_line = line.split("   ")
                setattr(self, p_line[0].replace(" ", "_"), p_line[-1].strip(" "))


class TestCreateCohort(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        proj_name = "dx-toolkit_test_data"
        proj_id = list(
            dxpy.find_projects(describe=False, level="VIEW", name=proj_name)
        )[0]["id"]
        cls.general_input_dir = os.path.join(dirname, "create_cohort_test_files/input/")
        # cls.general_output_dir = os.path.join(dirname, "create_cohort_test_files/output/")
        cls.payloads_dir = payloads_dir

        # TODO: setup project folders
        cls.proj_id = proj_id
        cls.temp_proj = DXProject()
        cls.temp_proj.new(name="temp_test_create_cohort_{}".format(uuid.uuid4()))
        cls.temp_proj_id = cls.temp_proj._dxid
        dxpy.config["DX_PROJECT_CONTEXT_ID"] = cls.temp_proj_id
        cls.test_record_geno = "{}:/Create_Cohort/create_cohort_geno_dataset".format(proj_name)
        cls.test_record_pheno = "{}:/Create_Cohort/create_cohort_pheno_dataset".format(proj_name)
        cls.test_invalid_rec_type = "{}:/Create_Cohort/non_dataset_record".format(proj_name)
        with open(
            os.path.join(dirname, "create_cohort_test_files", "usage_message.txt"), "r"
        ) as infile:
            cls.usage_message = infile.read()

        cls.maxDiff = None

    @classmethod
    def tearDownClass(cls):
        print("Remmoving temporary testing project {}".format(cls.temp_proj_id))
        cls.temp_proj.destroy()
        del cls.temp_proj

    def find_record_id(self, text): 
        match = re.search(r"\b(record-[A-Za-z0-9]{24})\b", text)
        if match:
            return match.group(0)
        
    def is_record_id(self, text):
        return bool(re.match(r"^(record-[A-Za-z0-9]{24})",text))
    
    def build_command(self, path=None, input_record=None, cohort_ids=None, cohort_ids_file=None):
        command = [
            "dx",
            "create_cohort",
            "--from",
            input_record
        ]
        if path:
            command.append(path)

        if cohort_ids:
            command.extend(["--cohort-ids", cohort_ids])
        
        if cohort_ids_file:
            command.extend(["--cohort-ids-file", cohort_ids_file])
        
        return command
    
    # Test the message printed on stdout when the --help flag is provided
    # This message is also printed on every error caught by argparse, before the specific message
    def test_help_text(self):
        expected_result = self.usage_message
        command = "dx create_cohort --help"

        process = subprocess.check_output(command, shell=True) 

        self.assertEqual(expected_result, process.decode())

    # testing that command accepts file with sample ids
    def test_accept_file_ids(self):
        command = self.build_command(
            path = "{}:/".format(self.temp_proj_id),
            input_record = self.test_record_pheno,
            cohort_ids_file = "{}sample_ids_valid_pheno.txt".format(self.general_input_dir)
        )

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        stdout, stderr = process.communicate()

        # testing if record object was created, retrieve record_id from stdout
        record_id = self.find_record_id(stdout)
        self.assertTrue(bool(record_id), "Record object was not created")
        

    # EM-1
    # testing resolution of invalid sample_id provided via file
    def test_accept_file_ids_negative(self):
        command = self.build_command(
            path = "{}:/".format(self.temp_proj_id),
            input_record = self.test_record_pheno,
            cohort_ids_file = "{}sample_ids_wrong.txt".format(self.general_input_dir)
        )
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        stderr = process.communicate()[1]
        expected_error = (
            "The following supplied IDs do not match IDs in the main entity of dataset"
        )
        self.assertTrue(expected_error in stderr, msg = stderr)

    def test_accept_cli_ids(self):
        command = self.build_command(
            path = "{}:/".format(self.temp_proj_id),
            input_record = self.test_record_geno,
            cohort_ids = " sample_1_1 , sample_1_10 "
        )
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        stdout, stderr = process.communicate()

        # testing if record object was created, retrieve record_id from stdout
        record_id = self.find_record_id(stdout)
        self.assertTrue(bool(record_id), "Record object was not created")


    # EM-1
    # Supplied IDs do not match IDs of main entity in Dataset/Cohort
    def test_accept_cli_ids_negative(self):
        command = self.build_command(
            path = "{}:/".format(self.temp_proj_id),
            input_record = self.test_record_geno,
            cohort_ids = "wrong,sample,id"
        )
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        stderr = process.communicate()[1]
        expected_error = (
            "The following supplied IDs do not match IDs in the main entity of dataset"
        )
        self.assertTrue(expected_error in stderr, msg = stderr)


    # EM-2
    # The structure of '--from' is invalid. This should be able to be reused from other dx functions
    def test_errmsg_invalid_path(self):
        bad_record = "record-badrecord"
        expected_error_message = (
            'Unable to resolve "{}" to a data object or folder name in'.format(bad_record)
        )
        command = self.build_command(
            input_record = "{}:{}".format(self.proj_id, bad_record),
            cohort_ids = "id1,id2"
        )

        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
        )

        err_msg = process.communicate()[1]

        # stdout should be the first element in this list and stderr the second
        self.assertIn(expected_error_message, err_msg.strip("\n"))

    # EM-3
    # The user does not have access to the object
    def test_errmsg_no_data_access(self):
        pass

    # EM-4
    # The record id or path is not a cohort or dataset
    # This should fail before the id validity check
    def test_errmsg_not_cohort_dataset(self):

        expected_error_message = "{}: Invalid path. The path must point to a record type of cohort or dataset".format(
            self.test_invalid_rec_type
        )
        command = self.build_command(
            input_record = self.test_invalid_rec_type,
            cohort_ids = "fakeid"
        )
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE ,stderr=subprocess.PIPE, universal_newlines=True
        )

        # stdout should be the first element in this list and stderr the second
        self.assertEqual(expected_error_message, process.communicate()[1].strip())

    # EM-5
    # The record id or path is a cohort or dataset but is invalid (maybe corrupted, descriptor not accessible...etc)
    def test_errmsg_invalid_record(self):
        pass

    # EM-6
    # The record id or path is a cohort or dataset but the version is less than 3.0.
    def test_errmsg_dataset_version(self):
        pass

    # EM-7
    # If PATH is of the format `project-xxxx:folder/` and the project does not exist
    def test_errmsg_project_not_exist(self):
        bad_project = "project-notarealproject7843k2Jq"
        expected_error_message = 'ResolutionError: Could not find a project named "{}"'.format(
            bad_project
        )
        command = self.build_command(
            path = "{}:/".format(self.temp_proj_id),
            input_record = "{}:/".format(bad_project),
            cohort_ids = "id_1,id_2"
        )
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        err_msg = process.communicate()[1]
        self.assertIn(expected_error_message, err_msg.strip("\n"))

    # EM-8
    # If PATH is of the format `project-xxxx:folder/` and the user does not have CONTRIBUTE or ADMINISTER access
    # Note that this is the PATH that the output cohort is being created in, not the input dataset or cohort
    def test_errmsg_no_path_access(self):
        pass

    # EM-9
    # If PATH is of the format `folder/subfolder/` and the path does not exist
    def test_errmsg_subfolder_not_exist(self):
        bad_path = "{}:Create_Cohort/missing_folder/file_name".format(self.proj_id)
        expected_error_message = "The folder: {} could not be found in the project: {}".format(
            "/Create_Cohort/missing_folder", self.proj_id
        )
        command = self.build_command(
            path = bad_path,
            input_record = self.test_record_pheno,
            cohort_ids = "patient_1,patient_2"
        )
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        # split("\n")[-1] is added to ignore the warning message that gets added
        # since DX_PROJECT_CONTEXT_ID environment variable is manually updated in setup class
        err_msg = process.communicate()[1].strip("\n").split("\n")[-1]
        self.assertEqual(expected_error_message, err_msg)

    # EM-10
    # If both --cohort-ids and --cohort-ids-file are supplied in the same call
    # The file needs to exist for this check to be performed
    def test_errmsg_incompat_args(self):
        expected_error_message = "dx create_cohort: error: argument --cohort-ids-file: not allowed with argument --cohort-ids"
        command = self.build_command(
            input_record = self.test_record_pheno,
            cohort_ids = "id1,id2",
            cohort_ids_file = os.path.join(self.general_input_dir, "sample_ids_10.txt")
        )
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        err_msg = process.communicate()[1]
        self.assertIn(expected_error_message, err_msg)

    # EM-11 The vizserver returns an error when attempting to validate cohort IDs
    def test_vizserver_error(self):
        pass

    def test_raw_cohort_query_api(self):
        test_payload = {
            "filters": {
                "pheno_filters": {
                    "compound": [
                        {
                            "name": "phenotype",
                            "logic": "and",
                            "filters": {
                                "patient$patient_id": [
                                    {
                                        "condition": "in",
                                        "values": [
                                            "patient_1",
                                            "patient_2",
                                            "patient_3"
                                        ]
                                    }
                                ]
                            }
                        }
                    ],
                    "logic": "and"
                },
                "logic": "and"
            },
            "project_context": self.proj_id
        }

        expected_results = "SELECT `patient_1`.`patient_id` AS `patient_id` FROM `database_yyyyyyyyyyyyyyyyyyyyyyyy__create_cohort_pheno_database`.`patient` AS `patient_1` WHERE `patient_1`.`patient_id` IN ('patient_1', 'patient_2', 'patient_3');"

        from_project, entity_result, resp, dataset_project = resolve_validate_record_path(self.test_record_pheno)
        sql = raw_cohort_query_api_call(resp, test_payload)
        self.assertEqual(expected_results, re.sub(r"\bdatabase_\w{24}__\w+", "database_yyyyyyyyyyyyyyyyyyyyyyyy__create_cohort_pheno_database", sql))

    def test_create_pheno_filter(self):
        """Verifying the correctness of created filters by examining this flow:
            1. creating the filter with: dxpy.dx_extract_utils.cohort_filter_payload.generate_pheno_filter
            2. obtaining sql with: dxpy.cli.dataset_utilities.raw_cohort_query_api_call
            3. creating record with obtained sql and the filter by: dxpy.bindings.dxrecord.new_dxrecord
        """

        # test creating pheno filter
        values = ["patient_1", "patient_2", "patient_3"]
        entity = "patient"
        field = "patient_id"
        filters = {
            "pheno_filters": {
                "compound": [
                    {
                        "name": "phenotype",
                        "logic": "and",
                        "filters": {
                            "patient$patient_id": [
                                {
                                    "condition": "in",
                                    "values": ["patient_1", "patient_2", "patient_6"],
                                }
                            ]
                        },
                    }
                ],
                "logic": "and",
            },
            "logic": "and",
        }
        expected_filter = {
            "pheno_filters": {
                "compound": [
                    {
                        "name": "phenotype",
                        "logic": "and",
                        "filters": {
                            "patient$patient_id": [
                                {
                                    "condition": "in",
                                    "values": ["patient_1", "patient_2"]
                                }
                            ]
                        },
                    }
                ],
                "logic": "and",
            },
            "logic": "and",
        }
        expected_sql = "SELECT `patient_1`.`patient_id` AS `patient_id` FROM `database_yyyyyyyyyyyyyyyyyyyyyyyy__create_cohort_pheno_database`.`patient` AS `patient_1` WHERE `patient_1`.`patient_id` IN ('patient_1', 'patient_2');"
        lambda_for_list_conv = lambda a, b: a+[str(b)]
        
        generated_filter = generate_pheno_filter(values, entity, field, filters, lambda_for_list_conv)
        self.assertEqual(expected_filter, generated_filter)

        # Testing raw cohort query api
        resp = resolve_validate_record_path(self.test_record_pheno)[2]
        payload = {"filters": generated_filter, "project_context": self.proj_id}

        sql = raw_cohort_query_api_call(resp, payload)
        self.assertEqual(expected_sql, re.sub(r"\bdatabase_\w{24}__\w+", "database_yyyyyyyyyyyyyyyyyyyyyyyy__create_cohort_pheno_database", sql))

        # Testing new record with generated filter and sql
        details = {
            "databases": [resp["databases"]],
            "dataset": {"$dnanexus_link": resp["dataset"]},
            "description": "",
            "filters": generated_filter,
            "schema": "create_cohort_schema",
            "sql": sql,
            "version": "3.0",
        }


        new_record = dxpy.bindings.dxrecord.new_dxrecord(
            details=details,
            project=self.temp_proj_id,
            name=None,
            types=["DatabaseQuery", "CohortBrowser"],
            folder="/",
            close=True,
        )
        new_record_details = new_record.get_details()
        new_record.remove()
        e = None
        self.assertTrue(isinstance(new_record, DXRecord))
        self.assertEqual(new_record_details, details, "Details of created record does not match expected details.")

    @parameterized.expand(
        os.path.splitext(file_name)[0] for file_name in sorted(os.listdir(os.path.join(payloads_dir, "raw-cohort-query_input")))
    )
    def test_cohort_filter_payload(self, payload_name):
        with open(os.path.join(self.payloads_dir, "input_parameters", "{}.json".format(payload_name))) as f:
            input_parameters = json.load(f)
        values = input_parameters["values"]
        entity = input_parameters["entity"]
        field = input_parameters["field"]
        project_context = input_parameters["project"]

        with open(os.path.join(self.payloads_dir, "visualize_response", "{}.json".format(payload_name))) as f:
            visualize_response = json.load(f)
        filters = visualize_response.get("filters", {})
        base_sql = visualize_response.get("baseSql", visualize_response.get("base_sql"))
        lambda_for_list_conv = lambda a, b: a+[str(b)]

        test_payload = cohort_filter_payload(values, entity, field, filters, project_context, lambda_for_list_conv, base_sql)

        with open(os.path.join(self.payloads_dir, "raw-cohort-query_input", "{}.json".format(payload_name))) as f:
            valid_payload = json.load(f)

        self.assertDictEqual(test_payload, valid_payload)

    @parameterized.expand(
        os.path.splitext(file_name)[0] for file_name in sorted(os.listdir(os.path.join(payloads_dir, "dx_new_input")))
    )
    def test_cohort_final_payload(self, payload_name):
        name = None

        with open(os.path.join(self.payloads_dir, "input_parameters", "{}.json".format(payload_name))) as f:
            input_parameters = json.load(f)
        folder = input_parameters["folder"]
        project = input_parameters["project"]

        with open(os.path.join(self.payloads_dir, "visualize_response", "{}.json".format(payload_name))) as f:
            visualize = json.load(f)
        dataset = visualize["dataset"]
        databases = visualize["databases"]
        schema = visualize["schema"]
        base_sql = visualize.get("baseSql", visualize.get("base_sql"))
        combined = visualize.get("combined")

        with open(os.path.join(self.payloads_dir, "raw-cohort-query_input", "{}.json".format(payload_name))) as f:
            filters = json.load(f)["filters"]

        with open(os.path.join(self.payloads_dir, "raw-cohort-query_output", "{}.sql".format(payload_name))) as f:
            sql = f.read()

        test_output = cohort_final_payload(name, folder, project, databases, dataset, schema, filters, sql, base_sql, combined)

        with open(os.path.join(self.payloads_dir, "dx_new_input", "{}.json".format(payload_name))) as f:
            valid_output = json.load(f)

        valid_output["name"] = None

        self.assertDictEqual(test_output, valid_output)

    def test_brief_verbose(self):
        command = self.build_command(
            path = "{}:/".format(self.temp_proj_id),
            input_record = self.test_record_geno,
            cohort_ids = "sample_1_1,sample_1_10"
        )

        for stdout_mode in ["--verbose", "--brief", ""]:
            cmd = command + [stdout_mode] if stdout_mode != "" else command
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            stdout, stderr = process.communicate()
            if stdout_mode == "--brief":
                record_id = stdout.strip("\n").strip(" ")
                self.assertTrue(self.is_record_id(record_id), 
                                "Brief stdout has to be a record-id"
                                )
            elif stdout_mode == "--verbose":
                self.assertIn(
                    "Details", stdout, "Verbose stdout has to contain 'Details' string"
                )
            else:
                self.assertIn(
                    "Types", stdout, "Default stdout has to contain 'Types' string"
                )


    def test_path_upload_access(self):
        # Having at least UPLOAD access to a project
        err_msg = validate_project_access(self.temp_proj_id)
        self.assertIsNone(err_msg)

    def test_path_upload_access_negative(self):
        #TODO: delegate this to QE
        pass

    def test_path_options(self):
        """
        Testing different path formats.
        Various path options and expected results are parametrized. 
        The dictionary `expected_in_out_pairs` expects form: {"<path>": (<results tuple>)}
        """
        self.temp_proj.new_folder("/folder/subfolder", parents=True)
        
        expected_in_out_pairs = {
            "{}:/".format(self.proj_id): (self.proj_id, "/", None, None),
            "{}:/folder/subfolder/record_name1".format(self.temp_proj_id): (self.temp_proj_id, "/folder/subfolder", "record_name1", None ),
            "record_name": (self.temp_proj_id, "/", "record_name", None),
            "/folder/record_name": (self.temp_proj_id, "/folder", "record_name", None),
            "/folder/subfolder/record_name": (
                self.temp_proj_id,
                "/folder/subfolder",
                "record_name",
                None,
            ),
            "/folder/subfolder/no_exist/record_name": (
                self.temp_proj_id,
                "/folder/subfolder/no_exist",
                "record_name",
                "The folder: /folder/subfolder/no_exist could not be found in the project: {}".format(
                    self.temp_proj_id
                ),
            ),
            "/folder/": (self.temp_proj_id, "/folder", None, None),
        }

        for path, expected_result in expected_in_out_pairs.items():
            result = resolve_validate_dx_path(path)
            self.assertEqual(result, expected_result) 

    def test_path_options_negative(self):
        expected_result = (
            self.temp_proj_id,
            "/folder/subfolder/no_exist",
            "record_name",
            "The folder: /folder/subfolder/no_exist could not be found in the project: {}".format(
                self.temp_proj_id
            )
        )
        result = resolve_validate_dx_path("/folder/subfolder/no_exist/record_name") 
        self.assertEqual(result, expected_result)

    def test_path_options_cli(self):
        """
        Testing different path formats. 
        Focusing on default values with record name or folder not specified. 
        """
        # create subfolder structure
        self.temp_proj.new_folder("/folder/subfolder", parents=True)
        # set cwd
        dxpy.config['DX_CLI_WD'] = "/folder"

        command = self.build_command(
            input_record = self.test_record_geno,
            cohort_ids = "sample_1_1,sample_1_10"
        )
        
        path_options = [        
            "record_name2", #Should create record in CWD
            "/folder/subfolder/", #Name of record should be record-id
            "", #Combination of above
        ]
        for path_format in path_options:
            cmd = command[:]
            if path_format != "":
                cmd.insert(2, path_format)

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                universal_newlines=True,
            )
            stdout = process.communicate()[0]
            desc = DescribeDetails(stdout)

            if path_format == "record_name2":
                self.assertEqual(desc.Folder, "/folder")
                self.assertEqual(desc.Project, self.temp_proj_id)
                self.assertEqual(desc.Name, "record_name2")
            elif path_format =="/folder/subfolder/":
                self.assertEqual(desc.Folder, "/folder/subfolder")
                self.assertEqual(desc.Project, self.temp_proj_id)
                self.assertTrue(self.is_record_id(desc.Name), 
                                "Record name should be a record-id"
                                )
            elif path_format =="":
                self.assertEqual(desc.Folder, "/folder")
                self.assertEqual(desc.Project, self.temp_proj_id)
                self.assertTrue(self.is_record_id(desc.Name), 
                                "Record name should be a record-id"
                                )
                
                
    def test_path_options_cli_negative(self):
        command = self.build_command(
            path = "/folder/subfolder/no_exist/record_name",
            input_record = self.test_record_geno,
            cohort_ids = "sample_1_1,sample_1_10"
        )
        process = subprocess.Popen(
                command,
                stderr=subprocess.PIPE,
                stdout=subprocess.PIPE,
                universal_newlines=True,
            )
        stderr = process.communicate()[1]
        self.assertEqual(
            "The folder: /folder/subfolder/no_exist could not be found in the project: {}".format(
                self.temp_proj_id
                ), 
            stderr.strip("\n"))


if __name__ == "__main__":
    unittest.main()

