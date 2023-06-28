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
from dxpy.dx_extract_utils.somatic_filter_payload import (
    basic_filter,
    location_filter,
    generate_pheno_filter,
    somatic_final_payload,
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
    test_record = "{}:/test_single_assay_202306231200".format(test_project)
elif dataset == "multi_assay_sciprod_1347_v2":
    # multi assay dataset
    test_project = "PMUX-1324-SCIPROD-CLISAM"
    test_record = "{}:/test_datasets/SCIPROD-1347/sciprod_1347_v2".format(test_project)
elif dataset == "small_original":
    test_project = "PMUX-1324-SCIPROD-CLISAM"
    test_record = "{}:test_datasets/assay_title_annot_complete".format(test_project)

e2e_filter_directory = os.path.join(general_input_dir, dataset, "e2e")
e2e_output_directory = os.path.join(general_input_dir, dataset, "e2e_output")

# Ensure output directories exist
if not os.path.exists(e2e_output_directory):
    os.makedirs(e2e_output_directory)


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

    def test_basic_filter(self):
        print("testing basic filter")
        table = "variant_read_optimized"
        friendly_name = "allele_id"
        values = ["chr21_40590995_C_C"]
        project_context = None
        genome_reference = None

        expected_output = {
            "variant_read_optimized$allele_id": [
                {"condition": "in", "values": ["chr21_40590995_C_C"]}
            ]
        }

        self.assertEqual(
            basic_filter(table, friendly_name, values),
            expected_output,
        )

    def test_location_filter(self):
        print("testing location filter")
        raw_location_list = [
            {
                "chromosome": "chr21",
                "starting_position": "100",
                "ending_position": "50000000",
            }
        ]
        expected_output = {
            "compound": [
                {
                    "filters": {
                        "variant_read_optimized$CHROM": [
                            {"condition": "is", "values": "chr21"}
                        ],
                        "variant_read_optimized$POS": [
                            {"condition": "greater-than", "values": 100},
                            {"condition": "less-than", "values": 50000000},
                        ],
                    },
                    "logic": "and",
                }
            ],
            "logic": "or",
        }

        self.assertEqual(location_filter(raw_location_list), expected_output)

    def test_generate_pheno_filter(self):
        print("testing generate pheno filter")
        full_input_dict = {"allele": {"allele_id": ["chr21_40590995_C_C"]}}
        name = "test_single_assay_202306231200"
        id = "0c69a39f-a34f-4030-a866-5056c8112da4"
        project_context = "project-GX0Jpp00ZJ46qYPq5G240k1k"
        expected_output = {
            "pheno_filters": {
                "name": "test_single_assay_202306231200",
                "id": "0c69a39f-a34f-4030-a866-5056c8112da4",
                "logic": "and",
                "compound": [
                    {
                        "filters": {
                            "variant_read_optimized$allele_id": [
                                {"condition": "in", "values": ["chr21_40590995_C_C"]}
                            ],
                            "variant_read_optimized$tumor_normal": [
                                {"condition": "is", "values": "tumor"}
                            ],
                        },
                        "logic": "and",
                    }
                ],
            }
        }
        self.assertEqual(
            generate_pheno_filter(full_input_dict, name, id, project_context),
            expected_output,
        )

    def test_somatic_final_payload(self):
        print("testing somatic final payload")
        full_input_dict = {"allele": {"allele_id": ["chr21_40590995_C_C"]}}
        name = "test_single_assay_202306231200"
        id = "0c69a39f-a34f-4030-a866-5056c8112da4"
        project_context = "project-GX0Jpp00ZJ46qYPq5G240k1k"
        expected_output = {
            "project_context": "project-GX0Jpp00ZJ46qYPq5G240k1k",
            "fields": [
                {"assay_sample_id": "variant_read_optimized$assay_sample_id"},
                {"allele_id": "variant_read_optimized$allele_id"},
                {"CHROM": "variant_read_optimized$CHROM"},
                {"POS": "variant_read_optimized$POS"},
                {"REF": "variant_read_optimized$REF"},
                {"allele": "variant_read_optimized$allele"},
            ],
            "raw_filters": {
                "pheno_filters": {
                    "name": "test_single_assay_202306231200",
                    "id": "0c69a39f-a34f-4030-a866-5056c8112da4",
                    "logic": "and",
                    "compound": [
                        {
                            "filters": {
                                "variant_read_optimized$allele_id": [
                                    {
                                        "condition": "in",
                                        "values": ["chr21_40590995_C_C"],
                                    }
                                ],
                                "variant_read_optimized$tumor_normal": [
                                    {"condition": "is", "values": "tumor"}
                                ],
                            },
                            "logic": "and",
                        }
                    ],
                }
            },
            "distinct": True,
        }
        expected_output_fields = [
            "assay_sample_id",
            "allele_id",
            "CHROM",
            "POS",
            "REF",
            "allele",
        ]

    def test_additional_fields(self):
        print("testing --additional-fields")
        input_filter_path = os.path.join(e2e_filter_directory, "single_location.json")
        output_path = os.path.join(
            general_output_dir, dataset, "e2e_output", "additional_fields_output.tsv"
        )

        command = 'dx extract_assay somatic {} --retrieve-variant {} --output {} --additional-fields "{}"'.format(
            test_record,
            input_filter_path,
            output_path,
            "sample_id,tumor_normal,symbolic_type",
        )

        process = subprocess.check_output(command, shell=True)

    def test_tumor_normal(self):
        print("testing --include-normal-sample")
        input_filter_path = os.path.join(e2e_filter_directory, "single_location.json")
        output_path = os.path.join(
            general_output_dir, dataset, "e2e_output", "tumor_normal_output.tsv"
        )

        command = 'dx extract_assay somatic {} --retrieve-variant {} --output {} --include-normal-sample --additional-fields "{}"'.format(
            test_record,
            input_filter_path,
            output_path,
            "sample_id,tumor_normal",
        )

        process = subprocess.check_output(command, shell=True)

    ####
    # Input validation test
    ####

    def test_malformed_json(self):
        # For somatic assays, json validation is not in a single function
        malformed_json_dir = os.path.join(general_input_dir, "malformed_json")
        malformed_json_filenames = os.listdir(malformed_json_dir)
        for name in malformed_json_filenames:
            filter_path = os.path.join(malformed_json_dir, name)
            command = "dx extract_assay somatic {} --retrieve-variant {}".format(
                test_record,
                os.path.join(malformed_json_dir, filter_path),
            )
            try:
                process = subprocess.check_output(
                    command,
                    shell=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                print(
                    "Uh oh, malformed JSON passed detection, file is {}".format(
                        name
                    )
                )
            except:
                print("malformed json {} detected succesfully".format(name))

    #####
    # E2E tests
    #####

    def test_e2e_filters(self):
        print("Testing e2e filters")
        e2e_filter_directory = os.path.join(general_input_dir, dataset, "e2e")
        filter_files = os.listdir(e2e_filter_directory)
        e2e_output_dir = os.path.join(general_output_dir, dataset, "e2e_output")

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
