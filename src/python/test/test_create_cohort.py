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
import tempfile
import shutil
import os
import re
import subprocess
import pandas as pd
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


        cls.test_record_geno = "{}:/Create_Cohort/create_cohort_geno_dataset".format(proj_name)
        cls.test_record_pheno = "{}:/Create_Cohort/create_cohort_pheno_dataset".format(proj_name)

    def is_recod_object(self, name):
        bool(re.match(r"^(record-[A-Za-z0-9]{24}|[a-z][a-z_0-9]{1,255})$", name))

    def test_help_text(self):
        print("testing help text")

        # An MD5sum hash of the correct error message
        expected_result = "fae9f07f1aad8cf69223ca666b20de35"

        command = "dx create_cohort --help"

        process = subprocess.check_output(command, shell=True, text=True)

        # Get the md5sum hash of the captured error message
        test_md5sum = hashlib.md5(process.encode("utf-8")).hexdigest()

        self.assertEqual(expected_result, test_md5sum)

    # Supplied IDs do not match IDs of main entity in Dataset/Cohort
    def test_errmsg_id_match(self):
        pass

    # The structure of “Path” is invalid. This should be able to be reused from other dx functions
    def test_errmsg_invalid_path(self):
        pass

    # The user does not have access to the object
    def test_errmsg_no_data_access(self):
        pass

    # The record id or path is not a cohort or dataset
    def test_errmsg_not_cohort_dataset(self):
        pass

    # The record id or path is a cohort or dataset but is invalid (maybe corrupted, descriptor not accessible...etc)
    def test_errmsg_invalid_record(self):
        pass

    # The record id or path is a cohort or dataset but the version is less than 3.0.
    def test_errmsg_dataset_version(self):
        pass

    # If PATH is of the format `project-xxxx:folder/` and the project does not exist
    def test_errmsg_project_not_exist(self):
        pass

    # If PATH is of the format `project-xxxx:folder/` and the user does not have CONTRIBUTE or ADMINISTER access
    # Note that this is the PATH that the output cohort is being created in, not the input dataset or cohort
    def test_errmsg_no_path_access(self):
        pass

    # If PATH is of the format `folder/subfolder/` and the path does not exist
    def test_errmsg_subfolder_not_exist(self):
        pass

    # If both --cohort-ids and --cohort-ids-file are supplied in the same call
    def test_errmsg_incompat_args(self):
        pass

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

        # TODO: uncomment when record-id is returned + get record id from stdout
        # self.assertTrue(self.is_recod_object(stdout))
        # Make sure to remove created record
        # subprocess.check_output('dx rm {}'.format(stdout), shell=True, text=True)

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

        # TODO: uncomment when record-id is returned + get record id from stdout
        # self.assertTrue(self.is_recod_object(stdout))
        # Make sure to remove created record
        # subprocess.check_output('dx rm {}'.format(stdout), shell=True, text=True)

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

        # TODO: uncomment when record-id is returned + get record id from stdout
        # self.assertTrue(self.is_recod_object(stdout))
        # Make sure to remove created record
        # subprocess.check_output('dx rm {}'.format(stdout), shell=True, text=True)


if __name__ == "__main__":
    unittest.main()
