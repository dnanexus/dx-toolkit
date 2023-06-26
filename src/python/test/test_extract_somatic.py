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


# Run manually with python3 src/python/test/test_extract_somatic.py

# Unit testing for the dx extract_assay somatic command
# Similar to the dx extract_assay germline tests


import dxpy
import unittest
import os
import subprocess

from dxpy_testutil import cd
from dxpy.cli.dataset_utilities import (
    get_assay_name_info,
    resolve_validate_path,
    DXDataset,
)

dirname = os.path.dirname(__file__)

general_input_dir = os.path.join(dirname, "clisam_test_filters/input/")
general_output_dir = os.path.join(dirname, "clisam_test_filters/output/")

#
# Select test suite
#
dataset = "single_assay"

if dataset == "single_assay":
    # Single assay
    test_project = "PMUX-1324-SCIPROD-CLISAM"
    test_record = "{}:/test_keegan_202306231200".format(test_project)
elif dataset == "multi_assay_sciprod_1347_v2":
    #multi assay dataset
    test_project = "PMUX-1324-SCIPROD-CLISAM"
    test_record = "{}:/test_datasets/SCIPROD-1347/sciprod_1347_v2".format(test_project)
elif dataset == "small_original":
    test_project = "PMUX-1324-SCIPROD-CLISAM"
    test_record = "{}:test_datasets/assay_title_annot_complete".format(test_project)


proj_id = list(dxpy.find_projects(describe=False, level="VIEW", name=test_project))[0][
    "id"
]


class TestDXExtractSomatic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cd(proj_id + ":/")

    ############
    # Unit Tests
    ############

    def test_get_assay_name_info(self):
        list_assays = True
        assay_name = "annot_complete"
        path = test_record
        friendly_assay_type = "somatic"

        project, entity_result, resp, dataset_project = resolve_validate_path(path)
        dataset_id = resp["dataset"]
        rec_descriptor = DXDataset(dataset_id, project=dataset_project).get_descriptor()


    def test_single_location(self):
        print("testing single location")
        input_filter_path = os.path.join(general_input_dir, "single_location.json")
        output_path = os.path.join(general_output_dir, "single_location_output.tsv")

        command = (
            "dx extract_assay somatic {} --retrieve-variant {} --output {}".format(
                test_record, input_filter_path, output_path
            )
        )

        process = subprocess.check_output(command, shell=True)

    def test_additional_fields(self):
        input_filter_path = os.path.join(testgen_filter_directory, "single_location.json")
        output_path = os.path.join(general_output_dir, "additional_fields_output.tsv")

        command = 'dx extract_assay somatic {} --retrieve-variant {} --output {} --additional-fields "{}"'.format(
            test_record,
            input_filter_path,
            output_path,
            "sample_id,tumor_normal,symbolic_type",
        )

        process = subprocess.check_output(command, shell=True)

    def test_tumor_normal(self):
        input_filter_path = os.path.join(general_input_dir, "single_location.json")
        output_path = os.path.join(general_output_dir, "tumor_normal_output.tsv")

        command = 'dx extract_assay somatic {} --retrieve-variant {} --output {} --include-normal-sample --additional-fields "{}"'.format(
            test_record,
            input_filter_path,
            output_path,
            "sample_id,tumor_normal",
        )

        process = subprocess.check_output(command, shell=True)

    def test_multi_location(self):
        input_filter_path = os.path.join(
            general_input_dir, "e2e/multi_location.json"
        )
        output_path = os.path.join(general_output_dir, "multi_location_output.tsv")

        command = (
            "dx extract_assay somatic {} --retrieve-variant {} --output {}".format(
                test_record, input_filter_path, output_path
            )
        )

        process = subprocess.check_output(command, shell=True)

    #####
    # E2E tests
    #####

    def test_e2e_filters(self):
        print("Testing e2e filters")
        e2e_filter_directory = os.path.join(general_input_dir, dataset,"e2e")
        filter_files = os.listdir(e2e_filter_directory)
        e2e_output_dir = os.path.join(general_output_dir,dataset,"e2e_output")

        for filter_name in filter_files:
            print("testing {}".format(filter_name))
            output_filename = filter_name[:-5] + "_output.tsv"
            command = (
                "dx extract_assay somatic {} --retrieve-variant {} --output {}".format(
                    test_record,
                    os.path.join(e2e_filter_directory, filter_name),
                    os.path.join(e2e_output_dir, output_filename),
                )
            )
            process = subprocess.check_call(command, shell=True)
            # print(command)


if __name__ == "__main__":
    unittest.main()
