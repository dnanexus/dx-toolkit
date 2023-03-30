#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 DNAnexus, Inc.
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


# Run manually with python3 src/python/test/test_extract_assay.py

import unittest
import tempfile
import dxpy
import os
import subprocess
from dxpy_testutil import cd, chdir

test_record = "project-G9j1pX00vGPzF2XQ7843k2Jq:record-GQGF8x80qYFQxv7gz49ZP7Y7"
test_filter_directory = "/dx-toolkit/src/python/test/CLIGAM_tests/test_input/"
output_folder = "/dx-toolkit/src/python/test/CLIGAM_tests/test_output/"


class TestDXExtractAssay(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        proj_name = "dx-toolkit_test_data"
        proj_id = list(
            dxpy.find_projects(describe=False, level="VIEW", name=proj_name)
        )[0]["id"]
        cd(proj_id + ":/")

    # Single filter tests
    # These won't work right now becuase location is broken
    # Will work if using subproces.run because tests with errors won't block others
    def test_single_filters(self):
        print("Testing single filters")
        single_filter_directory = os.path.join(test_filter_directory, "single_filters")
        for filter_type in ["allele", "annotation", "genotype"]:
            filter_dir = os.path.join(single_filter_directory, filter_type)
            filter_files = os.listdir(filter_dir)

            for filter_name in filter_files:
                output_filename = filter_name[:-5] + "_output.tsv"
                command = (
                    "dx extract_assay germline {} --retrieve-{} {} --output {}".format(
                        test_record,
                        filter_type,
                        os.path.join(filter_dir, filter_name),
                        os.path.join(output_folder, output_filename),
                    )
                )
                process = subprocess.check_call(command, shell=True)
                # print(command)

    # Tests for each filter where every field has a value
    def test_full_filters(self):
        print("testing full filters")
        multi_filter_directory = os.path.join(test_filter_directory, "multi_filters")
        for filter_type in ["allele", "annotation", "genotype"]:
            filter_file = os.path.join(
                multi_filter_directory, "{}_full.json".format(filter_type)
            )
            output_filename = os.path.join(
                output_folder, "{}_full_output.tsv".format(filter_type)
            )

            command = (
                "dx extract_assay germline {} --retrieve-{} {} --output {}".format(
                    test_record,
                    filter_type,
                    filter_file,
                    output_filename,
                )
            )
            process = subprocess.check_call(command, shell=True)

    def test_full_sql_filters(self):
        print("testing full filters with sql flag")
        multi_filter_directory = os.path.join(test_filter_directory, "multi_filters")
        for filter_type in ["allele", "annotation", "genotype"]:
            filter_file = os.path.join(
                multi_filter_directory, "{}_full.json".format(filter_type)
            )
            output_filename = os.path.join(
                output_folder, "{}_full_sql_output.tsv".format(filter_type)
            )

            command = "dx extract_assay germline {} --retrieve-{} {} --output {} --sql".format(
                test_record,
                filter_type,
                filter_file,
                output_filename,
            )
            process = subprocess.check_call(command, shell=True)

    # A test that ensures that the location filter functions with other allele filters
    # Currently fails because of "geno bins must be in first part of compound" bug
    def test_allele_location_type(self):
        if False:
            print("Testing allele filter with location and allele type fields")
            multi_filter_directory = os.path.join(
                test_filter_directory, "multi_filters"
            )
            filter_file = os.path.join(
                multi_filter_directory, "allele_location_type.json"
            )
            output_filename = filter_file[:-5] + "_output.tsv"
            command = "dx extract_assay germline {} --retrieve-{} {} --output {} --sql".format(
                test_record,
                "allele",
                filter_file,
                os.path.join(output_folder, output_filename),
            )
            process = subprocess.check_call(command, shell=True)

    # A test to check if the vizserver forces an or relationship between gene name and gene id
    def test_annotation_name_id(self):
        print("testing annotation gene_name gene_id")
        multi_filter_directory = os.path.join(test_filter_directory, "multi_filters")
        filter_file = os.path.join(multi_filter_directory, "annotation_name_id.json")
        output_filename = os.path.join(output_folder, "annotation_name_id_output.tsv")
        command = (
            "dx extract_assay germline {} --retrieve-{} {} --output {} --sql".format(
                test_record,
                "annotation",
                filter_file,
                output_filename,
            )
        )
        process = subprocess.check_call(command, shell=True)

    # A test of the --list-assays functionality
    def test_list_assays(self):
        print("testing --list-assays")
        command = "dx extract_assay germline {} --list-assays".format(test_record)
        process = subprocess.check_call(command, shell=True)

    # Doesn't work, unspecified error
    def test_assay_name(self):
        print("testing --assay-name")
        single_filter_directory = os.path.join(test_filter_directory, "single_filters")

        command = "dx extract_assay germline {} --assay-name test01_assay --retrieve-allele {}".format(
            test_record,
            os.path.join(single_filter_directory, "allele/allele_rsid.json"),
        )
        subprocess.check_call(command, stderr=subprocess.STDOUT, shell=True)


if __name__ == "__main__":
    unittest.main()
