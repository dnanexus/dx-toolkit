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

    def test_help_text(self):
        print("testing help text")

        # An MD5sum hash of the correct error message
        expected_result = "fae9f07f1aad8cf69223ca666b20de35"

        command = "dx create_cohort --help"

        process = subprocess.check_output(command, shell=True, text=True)

        # Get the md5sum hash of the captured error message
        test_md5sum = hashlib.md5(process.encode("utf-8")).hexdigest()

        self.assertEqual(expected_result, test_md5sum)

    # EM-1
    # Supplied IDs do not match IDs of main entity in Dataset/Cohort
    def test_errmsg_id_match(self):
        command = [
            "dx",
            "create_cohort",
            "fakepath",
            "--from",
            self.test_record,
            "--cohort-ids",
            "sample00000,sample00003,bad_id_1",
        ]
        print(command)
        expected_error_message = "The following supplied IDs do not match IDs in the main entity of dataset, project-G9j1pX00vGPzF2XQ7843k2Jq: {{bad_id_1}}".format()
        print(expected_error_message)
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        # stdout should be the first element in this list and stderr the second
        self.assertEqual(expected_error_message, process.communicate()[1])

    # EM-2
    # The structure of “Path” is invalid. This should be able to be reused from other dx functions
    def test_errmsg_invalid_path(self):
        pass

    # EM-3
    # The user does not have access to the object
    def test_errmsg_no_data_access(self):
        pass

    # EM-4
    # The record id or path is not a cohort or dataset
    # This should fail before the id validity check
    def test_errmsg_not_cohort_dataset(self):
        quaytest_applet_id = (
            "project-G9j1pX00vGPzF2XQ7843k2Jq:applet-GPbxvJQ0vGPx0yQV5843YYqy"
        )
        expected_error_message = "{} : Invalid path. The path must point to a record type of cohort or dataset".format(
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
        self.assertEqual(expected_error_message, process.communicate()[1])

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
        ]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        self.assertEqual(expected_error_message, process.communicate()[1])

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
        ]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        self.assertEqual(expected_error_message, process.communicate()[1])

    # EM-10
    # If both --cohort-ids and --cohort-ids-file are supplied in the same call
    # This should fail before checking to see if the cohort id file actually exists
    def test_errmsg_incompat_args(self):
        expected_error_message = "Only one --cohort-ids and --cohort-ids-file may be supplied at a given time. Please use either --cohort-ids or --cohort-ids-file, and not both."
        command = command = [
            "dx",
            "create_cohort",
            "fakepath",
            "--from",
            self.test_record,
            "--cohort-ids",
            "id1,id2",
            "--cohort-file",
            "cohort_id_file.txt",
        ]
        process = subprocess.Popen(
            command, stderr=subprocess.PIPE, universal_newlines=True
        )
        self.assertEqual(expected_error_message, process.communicate()[1])

    def test_retrieve_cohort_id(self):
        pass

    def test_accept_file_ids(self):
        pass

    def test_accept_cli_ids(self):
        pass


if __name__ == "__main__":
    unittest.main()
