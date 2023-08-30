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


import unittest
import os
import re
import subprocess
import dxpy
import sys
import hashlib
from dxpy_testutil import cd, chdir

from dxpy.cli.dataset_utilities import (
    get_assay_name_info,
    resolve_validate_record_path,
    DXDataset,
    cohort_query_api_call
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

        # TODO: setup project folders
        cls.test_record = "{}:/Create_Cohort/somatic_indels_1k".format(proj_name)
        cls.proj_id = proj_id
        cls.test_record_geno = "{}:/Create_Cohort/create_cohort_geno_dataset".format(proj_name)
        cls.test_record_pheno = "{}:/Create_Cohort/create_cohort_pheno_dataset".format(proj_name)
        with open(
            os.path.join(dirname, "create_cohort_test_files", "usage_message.txt"), "r"
        ) as infile:
            cls.usage_message = infile.read()

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
        self.assertTrue(stderr == "", msg = stderr)

        # testing if record object was created, retrieve record_id from stdout
        record_id = self.find_record_id(stdout)
        self.assertTrue(bool(recod_id), "Record object was not created!")
        # Make sure to remove created record
        subprocess.check_output('dx rm {}'.format(record_id), shell=True, text=True)

    # EM-1
    # testing resolution of invalid sample_id provided via file
    def test_accept_file_ids_negative(self):
        command = [
            "dx",
            "create_cohort",
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
        self.assertTrue(stderr == "", msg = stderr)

        # testing if record object was created, retrieve record_id from stdout
        recod_id = self.find_record_id(stdout)
        self.assertTrue(bool(recod_id), "Record object was not created!")
        # Make sure to remove created record
        subprocess.check_output('dx rm {}'.format(recod_id), shell=True, text=True)

    # EM-1
    # Supplied IDs do not match IDs of main entity in Dataset/Cohort
    def test_accept_cli_ids_negative(self):
        command = [
            "dx",
            "create_cohort",
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
    # The structure of “--from” is invalid. This should be able to be reused from other dx functions
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




if __name__ == "__main__":
    unittest.main()
