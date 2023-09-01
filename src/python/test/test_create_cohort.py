from __future__ import print_function, unicode_literals, division, absolute_import

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
from dxpy_testutil import cd, chdir
from dxpy.bindings import DXRecord, DXProject

from dxpy.cli.dataset_utilities import (
    get_assay_name_info,
    resolve_validate_record_path,
    DXDataset,
    cohort_query_api_call
)
from dxpy.dx_extract_utils.cohort_filter_payload import (
    generate_pheno_filter,
    cohort_filter_payload,
    cohort_final_payload,
)

dirname = os.path.dirname(__file__)

python_version = sys.version_info.major


class TestCreateCohort(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        proj_name = "dx-toolkit_test_data"
        proj_id = list(
            dxpy.find_projects(describe=False, level="VIEW", name=proj_name)
        )[0]["id"]
        cd(proj_id + ":/")
        cls.general_input_dir = os.path.join(dirname, "create_cohort_test_files/input/")
        # cls.general_output_dir = os.path.join(dirname, "create_cohort_test_files/output/")
        cls.payloads_dir = os.path.join(dirname, "create_cohort_test_files/payloads/")

        # TODO: setup project folders
        cls.test_record = "{}:/Create_Cohort/somatic_indels_1k".format(proj_name)
        cls.proj_id = proj_id
        cls.temp_proj = DXProject()
        cls.temp_proj.new(name="temp_test_create_cohort_{}".format(uuid.uuid4()))
        cls.test_record_geno = "{}:/Create_Cohort/create_cohort_geno_dataset".format(proj_name)
        cls.test_record_pheno = "{}:/Create_Cohort/create_cohort_pheno_dataset".format(proj_name)
        with open(
            os.path.join(dirname, "create_cohort_test_files", "usage_message.txt"), "r"
        ) as infile:
            cls.usage_message = infile.read()

        cls.maxDiff = None

    @classmethod
    def tearDownClass(cls):
        print("Remmoving testing temp project {}".format(cls.temp_proj._dxid))
        cls.temp_proj.destroy()

    def find_record_id(self, text): 
        match = re.search(r"\b(record-[A-Za-z0-9]{24})\b", text)
        if match:
            return match[1]
    

    # Test the message printed on stdout when the --help flag is provided
    # This message is also printed on every error caught by argparse, before the specific message
    def test_help_text(self):
        expected_result = self.usage_message
        command = "dx create_cohort --help"

        process = subprocess.check_output(command, shell=True, text=True)

        self.assertEqual(expected_result, process)

    # testing that command accepts file with sample ids
    def test_accept_file_ids(self):
        command = [
            "dx",
            "create_cohort",
            "{}:/".format(self.temp_proj._dxid),
            "--from",
            self.test_record_pheno,
            "--cohort-ids-file",
            "{}sample_ids_valid_pheno.txt".format(self.general_input_dir),
        ]
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        stdout, stderr = process.communicate()
        self.assertTrue(len(stderr) == 0, msg = stderr)

        # testing if record object was created, retrieve record_id from stdout
        try:
            record_id = self.find_record_id(stdout)
            subprocess.check_output('dx rm {}'.format(record_id), shell=True, text=True)
            e = None
        except Exception as e:
            pass 
        self.assertTrue(bool(record_id), str(e))
        

    # EM-1
    # testing resolution of invalid sample_id provided via file
    def test_accept_file_ids_negative(self):
        command = [
            "dx",
            "create_cohort",
            "{}:/".format(self.temp_proj._dxid),
            "--from",
            self.test_record_pheno,
            "--cohort-ids-file",
            "{}sample_ids_wrong.txt".format(self.general_input_dir),
        ]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        stderr = process.communicate()[1]
        expected_error = (
            "The following supplied IDs do not match IDs in the main entity of dataset"
        )
        self.assertTrue(expected_error in stderr, msg = stderr)

    def test_accept_cli_ids(self):
        command = [
            "dx",
            "create_cohort",
            "{}:/".format(self.temp_proj._dxid),
            "--from",
            self.test_record_geno,
            "--cohort-ids",
            "sample_1_1,sample_1_10",
        ]
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        stdout, stderr = process.communicate()
        self.assertTrue(len(stderr) == 0, msg = stderr)

        # testing if record object was created, retrieve record_id from stdout
        try:
            record_id = self.find_record_id(stdout)
            subprocess.check_output('dx rm {}'.format(record_id), shell=True, text=True)
            e = None
        except Exception as e:
            pass 
        self.assertTrue(bool(record_id), str(e))


    # EM-1
    # Supplied IDs do not match IDs of main entity in Dataset/Cohort
    def test_accept_cli_ids_negative(self):
        command = [
            "dx",
            "create_cohort",
            "{}:/".format(self.temp_proj._dxid),
            "--from",
            self.test_record_geno,
            "--cohort-ids",
            "wrong,sample,id",
        ]
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
            "Unable to resolve {} to a data object or folder name in {}".format(bad_record, self.proj_id)
        )
        command = [
            "dx",
            "create_cohort",
            "--from",
            "{}:{}".format(self.proj_id, bad_record),
            "--cohort-ids",
            "id1,id2",
        ]

        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )

        err_msg = process.communicate()[1]

        # stdout should be the first element in this list and stderr the second
        self.assertEqual(expected_error_message, err_msg)

    # EM-3
    # The user does not have access to the object
    def test_errmsg_no_data_access(self):
        pass

    # EM-4
    # The record id or path is not a cohort or dataset
    # This should fail before the id validity check
    def test_errmsg_not_cohort_dataset(self):
        non_dataset_record = "{}:workflow-GYYBq4j0vGPq598PY8JJX7x9".format(self.proj_id)

        expected_error_message = "{}: Invalid path. The path must point to a record type of cohort or dataset".format(
            non_dataset_record
        )
        command = [
            "dx",
            "create_cohort",
            "--from",
            non_dataset_record,
            "--cohort-ids",
            "fakeid",
        ]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
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
        bad_path = "project-notarealproject7843k2Jq:record-GX8jB2j0VBvZf6Qxx0pKxBk0"
        expected_error_message = 'dxpy.utils.resolver.ResolutionError: Could not find a project named "{}"'.format(
            bad_path
        )
        command = [
            "dx",
            "create_cohort",
            "--from",
            bad_path,
            "--cohort-ids",
            "id_1,id_2",
        ]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        err_msg = process.communicate()[1]
        self.assertEqual(expected_error_message, err_msg)

    # EM-8
    # If PATH is of the format `project-xxxx:folder/` and the user does not have CONTRIBUTE or ADMINISTER access
    # Note that this is the PATH that the output cohort is being created in, not the input dataset or cohort
    def test_errmsg_no_path_access(self):
        pass

    # EM-9
    # If PATH is of the format `folder/subfolder/` and the path does not exist
    def test_errmsg_subfolder_not_exist(self):
        bad_path = "{}:Create_Cohort/missing_folder".format(self.proj_id)
        expected_error_message = "dxpy.utils.resolver.ResolutionError: The folder: {} could not be found in the project: {}".format(
            bad_path, self.proj_id
        )
        command = [
            "dx",
            "create_cohort",
            "--from",
            bad_path,
            "--cohort-ids",
            "id_1,id_2",
        ]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        err_msg = process.communicate()[1]
        self.assertEqual(expected_error_message, err_msg)

    # EM-10
    # If both --cohort-ids and --cohort-ids-file are supplied in the same call
    # The file needs to exist for this check to be performed
    def test_errmsg_incompat_args(self):
        expected_error_message = "{}\ndx create_cohort: error: argument --cohort-ids-file: not allowed with argument --cohort-ids".format(
            self.usage_message
        )
        command = command = [
            "dx",
            "create_cohort",
            "--from",
            self.test_record,
            "--cohort-ids",
            "id1,id2",
            "--cohort-ids-file",
            os.path.join(self.general_input_dir, "sample_ids_10.txt"),
        ]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        err_msg = process.communicate()[1]
        # removing all whitespace before comparing
        self.assertEqual("".join(expected_error_message.split()), "".join(err_msg.split()))

    # EM-11 The vizserver returns an error when attempting to validate cohort IDs
    def test_vizserver_error(self):
        pass

    def test_cohort_query_api(self):
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
            "project_context": "project-G9j1pX00vGPzF2XQ7843k2Jq"
        }

        expected_results = "SELECT `patient_1`.`patient_id` AS `patient_id` FROM `database_gyk2yg00vgppzj7ygy3vjxb9__create_cohort_pheno_database`.`patient` AS `patient_1` WHERE `patient_1`.`patient_id` IN ('patient_1', 'patient_2', 'patient_3');"

        from_project, entity_result, resp, dataset_project = resolve_validate_record_path(self.test_record_pheno)
        sql = cohort_query_api_call(resp, test_payload)
        self.assertEqual(expected_results,sql)

    def test_create_pheno_filter(self):
        """Verifying the correctness of created filters by examining this flow:
            1. creating the filter with: dxpy.dx_extract_utils.cohort_filter_payload.generate_pheno_filter
            2. obtaining sql with: dxpy.cli.dataset_utilities.cohort_query_api_call
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
                                    "values": ["patient_4", "patient_5", "patient_6"],
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
                                    "values": ["patient_4", "patient_5", "patient_6"],
                                },
                                {
                                    "condition": "in",
                                    "values": ["patient_1", "patient_2", "patient_3"],
                                },
                            ]
                        },
                    }
                ],
                "logic": "and",
            },
            "logic": "and",
        }
        expected_sql = "SELECT `patient_1`.`patient_id` AS `patient_id` FROM `database_gyk2yg00vgppzj7ygy3vjxb9__create_cohort_pheno_database`.`patient` AS `patient_1` WHERE `patient_1`.`patient_id` IN ('patient_4', 'patient_5', 'patient_6') AND `patient_1`.`patient_id` IN ('patient_1', 'patient_2', 'patient_3');"
        
        generated_filter = generate_pheno_filter(values, entity, field, filters)
        self.assertEqual(expected_filter, generated_filter)

        # Testing cohort query api
        resp = resolve_validate_record_path(self.test_record_pheno)[2]
        payload = {"filters": generated_filter, "project_context": self.proj_id}

        sql = cohort_query_api_call(resp, payload)
        self.assertEqual(expected_sql, sql)

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

        try:
            new_record = dxpy.bindings.dxrecord.new_dxrecord(
                details=details,
                project=self.temp_proj._dxid,
                name=None,
                types=["DatabaseQuery", "CohortBrowser"],
                folder="/",
                close=True,
            )
            new_record_details = new_record.get_details()
            new_record.remove()
            e = None
        except Exception as e:
            pass

        self.assertTrue(isinstance(new_record, DXRecord), str(e))
        self.assertEqual(new_record_details, details, "Details of created record does not match expected details.")

    @property
    def _payload_names(self):
        for file_name in sorted(os.listdir(os.path.join(self.payloads_dir, "dx_new_input"))):
            yield os.path.splitext(file_name)[0]

    def _test_cohort_filter_payload(self, payload_name):
        project_context = "project-G9j1pX00vGPzF2XQ7843k2Jq"

        with open(os.path.join(self.payloads_dir, "input_parameters", "{}.json".format(payload_name))) as f:
            input_parameters = json.load(f)
        values = input_parameters["values"]
        entity = input_parameters["entity"]
        field = input_parameters["field"]

        with open(os.path.join(self.payloads_dir, "visualize_response", "{}.json".format(payload_name))) as f:
            visualize_response = json.load(f)
        filters = visualize_response.get("filters", {})
        base_sql = visualize_response.get("baseSql", visualize_response.get("base_sql"))

        test_payload = cohort_filter_payload(values, entity, field, filters, project_context, base_sql)

        with open(os.path.join(self.payloads_dir, "cohort-query_input", "{}.json".format(payload_name))) as f:
            valid_payload = json.load(f)

        with self.subTest(payload_name):
            self.assertDictEqual(test_payload, valid_payload)

    def test_cohort_filter_payloads(self):
        for payload_name in self._payload_names:
            self._test_cohort_filter_payload(payload_name)

    def _test_cohort_final_payload(self, payload_name):
        name = None
        folder = "/Create_Cohort/manually_created_output_cohorts",
        project = "project-G9j1pX00vGPzF2XQ7843k2Jq"

        with open(os.path.join(self.payloads_dir, "visualize_response", "{}.json".format(payload_name))) as f:
            visualize = json.load(f)
        dataset = visualize["dataset"]
        databases = visualize["databases"]
        base_sql = visualize.get("baseSql", visualize.get("base_sql"))
        combined = visualize.get("combined")

        with open(os.path.join(self.payloads_dir, "cohort-query_input", "{}.json".format(payload_name))) as f:
            filters = json.load(f)["filters"]

        with open(os.path.join(self.payloads_dir, "cohort-query_output", "{}.sql".format(payload_name))) as f:
            sql = f.read()

        test_output = cohort_final_payload(name, folder, project, databases, dataset, filters, sql, base_sql, combined)

        with open(os.path.join(self.payloads_dir, "dx_new_input", "{}.json".format(payload_name))) as f:
            valid_output = json.load(f)

        valid_output["name"] = None

        with self.subTest(payload_name):
            self.assertDictEqual(test_output, valid_output)

    def test_cohort_final_payloads(self):
        for payload_name in self._payload_names:
            self._test_cohort_final_payload(payload_name)


if __name__ == "__main__":
    unittest.main()
