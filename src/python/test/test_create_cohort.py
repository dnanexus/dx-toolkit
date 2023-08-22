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
            os.path.join(cls.general_input_dir, "usage_message.txt"), "r"
        ) as infile:
            cls.usage_message = infile.read()

    def is_record_object(self, name):
        bool(re.match(r"^(record-[A-Za-z0-9]{24}|[a-z][a-z_0-9]{1,255})$", name))

    # Test the message printed on stdout when the --help flag is provided
    # This message is also printed on every error caught by argparse, before the specific message
    def test_help_text(self):
        expected_result = self.usage_message
        command = "dx create_cohort --help"

        process = subprocess.check_output(command, shell=True, text=True)

        self.assertEqual(expected_result, process)

    def test_accept_file_ids(self):
        command = [
            "dx",
            "create_cohort",
            "fakepath",
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

        # TODO: uncomment when record-id is returned + get record id from stdout
        # self.assertTrue(self.is_record_object(stdout))
        # Make sure to remove created record
        # subprocess.check_output('dx rm {}'.format(stdout), shell=True, text=True)

    # EM-1
    def test_accept_file_ids_negative(self):
        command = [
            "dx",
            "create_cohort",
            "fakepath",
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
            "fakepath",
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

        # TODO: uncomment when record-id is returned + get record id from stdout
        # self.assertTrue(self.is_record_object(stdout))
        # Make sure to remove created record
        # subprocess.check_output('dx rm {}'.format(stdout), shell=True, text=True)

    # EM-1
    # Supplied IDs do not match IDs of main entity in Dataset/Cohort
    def test_accept_cli_ids_negative(self):
        command = [
            "dx",
            "create_cohort",
            "fakepath",
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
    # The structure of “Path” is invalid. This should be able to be reused from other dx functions
    def test_errmsg_invalid_path(self):
        expected_error_message = (
            "Structure of PATH is invalid.  Must be in format *.cohort"
        )
        command = [
            "dx",
            "create_cohort",
            "{}:/Create_Cohort/bad_name_format.txt".format(self.proj_id),
            "--from",
            self.test_record,
            "--cohort-ids",
            "id1,id2",
        ]

        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )

        # stdout should be the first element in this list and stderr the second
        self.assertEqual(expected_error_message, process.communicate()[1])

    # EM-3
    # The user does not have access to the object
    def test_errmsg_no_data_access(self):
        pass

    # EM-4
    # The record id or path is not a cohort or dataset
    # This should fail before the id validity check
    def test_errmsg_not_cohort_dataset(self):
        quaytest_applet_id = "{}:applet-GPbxvJQ0vGPx0yQV5843YYqy".format(self.proj_id)

        expected_error_message = "{}: Invalid path. The path must point to a record type of cohort or dataset".format(
            quaytest_applet_id
        )
        command = [
            "dx",
            "create_cohort",
            "fakepath",
            "--from",
            quaytest_applet_id,
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
            "fakepath",
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
            "fakepath",
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
            "fakepath",
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

    


if __name__ == "__main__":
    unittest.main()
