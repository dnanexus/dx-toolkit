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


# Run manually with python3 src/python/test/test_extract_somatic.py

# Unit testing for the dx extract_assay somatic command
# Similar to the dx extract_assay germline tests


import dxpy
import unittest
import os
import sys
import subprocess
import shutil

from dxpy_testutil import cd
from dxpy.cli.dataset_utilities import (
    get_assay_name_info,
    resolve_validate_record_path,
    DXDataset,
)
from dxpy.dx_extract_utils.somatic_filter_payload import (
    basic_filter,
    location_filter,
    generate_assay_filter,
    somatic_final_payload,
)

dirname = os.path.dirname(__file__)

python_version = sys.version_info.major

class TestDXExtractSomatic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_project_name = "dx-toolkit_test_data"
        cls.proj_id = list(
            dxpy.find_projects(describe=False, level="VIEW", name=test_project_name)
        )[0]["id"]
        cd(cls.proj_id + ":/")
        cls.general_input_dir = os.path.join(dirname, "clisam_test_filters/input/")
        cls.general_output_dir = os.path.join(dirname, "clisam_test_filters/output/")

        #
        # Select test suite
        #
        cls.dataset = "single_assay"

        if cls.dataset == "single_assay":
            # Single assay
            cls.test_record = "{}:/Extract_Assay_Somatic/test_single_assay_202306231200_new".format(
                test_project_name
            )
        elif cls.dataset == "multi_assay_sciprod_1347_v2":
            # multi assay dataset
            cls.test_record = (
                "{}:/Extract_Assay_Somatic/test_datasets/SCIPROD-1347/sciprod_1347_v2".format(
                    test_project_name
                )
            )
        elif cls.dataset == "small_original":
            cls.test_record = "{}:test_datasets/assay_title_annot_complete".format(test_project_name)
        
        cls.e2e_filter_directory = os.path.join(cls.general_input_dir, cls.dataset, "e2e")
        cls.e2e_output_directory = os.path.join(cls.general_output_dir, cls.dataset, "e2e_output")

        # Ensure output directories exist
        if not os.path.exists(cls.e2e_output_directory):
            os.makedirs(cls.e2e_output_directory)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.general_output_dir)

    ############
    # Unit Tests
    ############

    def test_get_assay_name_info(self):
        # Set to true for the list assay utilities response instead of the normal functionality
        list_assays = False
        # When assay name is none, function looks for and selects first assay of type somatic that it finds
        assay_name = None
        friendly_assay_type = "somatic"
        project, entity_result, resp, dataset_project = resolve_validate_record_path(
            self.test_record
        )
        dataset_id = resp["dataset"]
        rec_descriptor = DXDataset(dataset_id, project=dataset_project).get_descriptor()
        # Expected Results
        expected_assay_name = "test_single_assay_202306231200"
        expected_assay_id = "5c359e55-0639-46bc-bbf3-5eb22d5a5780"
        expected_ref_genome = "GRCh38.109"
        expected_additional_descriptor_info = {}

        (
            selected_assay_name,
            selected_assay_id,
            selected_ref_genome,
            additional_descriptor_info,
        ) = get_assay_name_info(
            list_assays=False,
            assay_name=assay_name,
            path=self.test_record,
            friendly_assay_type=friendly_assay_type,
            rec_descriptor=rec_descriptor,
        )

        self.assertEqual(expected_assay_name, selected_assay_name)
        self.assertEqual(expected_assay_id, selected_assay_id)
        self.assertEqual(expected_ref_genome, selected_ref_genome)
        self.assertEqual(expected_additional_descriptor_info, additional_descriptor_info)

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
            "variant_read_optimized$allele_id": [
                {
                    "condition": "in",
                    "values": [],
                    "geno_bins": [
                        {
                            "chr": "21",
                            "start": 100,
                            "end": 50000000
                        }
                    ]
                },
            ]
        }
        expected_chrom = ['chr21']
        loc_filter, chrom = location_filter(raw_location_list)
        self.assertEqual(loc_filter, expected_output)
        self.assertEqual(chrom, expected_chrom)

    def test_generate_assay_filter(self):
        print("testing generate assay filter")
        full_input_dict = {"allele": {"allele_id": ["chr21_40590995_C_C"]}}
        name = "test_single_assay_202306231200"
        id = "0c69a39f-a34f-4030-a866-5056c8112da4"
        project_context = "project-GX0Jpp00ZJ46qYPq5G240k1k"
        expected_output = {
            "assay_filters": {
                "name": "test_single_assay_202306231200",
                "id": "0c69a39f-a34f-4030-a866-5056c8112da4",
                "logic": "and",
                "filters": {
                    "variant_read_optimized$allele_id": [
                        {"condition": "in", "values": ["chr21_40590995_C_C"]}
                    ],
                    "variant_read_optimized$tumor_normal": [
                        {"condition": "is", "values": "tumor"}
                    ],
                }
            }
        }
        self.assertEqual(
            generate_assay_filter(full_input_dict, name, id, project_context),
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
            "order_by": [
                {"CHROM":"asc"}, 
                {"POS":"asc"}, 
                {"allele_id":"asc"}, 
                {"assay_sample_id":"asc"}
            ],
            "raw_filters": {
                "assay_filters": {
                    "name": "test_single_assay_202306231200",
                    "id": "0c69a39f-a34f-4030-a866-5056c8112da4",
                    "logic": "and",
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
                    }
                }
            },
            "distinct": True,
            "adjust_geno_bins": False
        }
        expected_output_fields = [
            "assay_sample_id",
            "allele_id",
            "CHROM",
            "POS",
            "REF",
            "allele",
        ]
        test_payload, test_fields = somatic_final_payload(full_input_dict, name, id, project_context, genome_reference=None, additional_fields=None, include_normal=False)
        self.assertEqual(test_payload, expected_output)
        self.assertEqual(test_fields, expected_output_fields)

    def test_somatic_final_payload_location(self):
        print("testing somatic final payload with location filter")
        full_input_dict = {"location":[{"chromosome":"chrUn_JTFH01000732v1_decoy","starting_position":"40", "ending_position":"45"}]}
        name = "assay_dummy"
        id = "id_dummy"
        project_context = "project-dummy"
        expected_output = {
            "filters": {
                "variant_read_optimized$allele_id": [
                    {
                        "condition": "in",
                        "values": [],
                        "geno_bins": [
                            {
                                "chr": "Other",
                                "start": 40,
                                "end": 45
                            }
                        ] 
                    }
                ],
                "variant_read_optimized$CHROM": [
                    {
                        "condition": "in",
                        "values": [
                            "chrUn_JTFH01000732v1_decoy"
                        ]
                    }
                ],
                "variant_read_optimized$tumor_normal": [
                    {
                        "condition": "is",
                        "values": "tumor"
                    }
                ]
            }
        }

        test_payload, _ = somatic_final_payload(full_input_dict, name, id, project_context, genome_reference=None, additional_fields=None, include_normal=False)
        self.assertEqual(test_payload["raw_filters"]["assay_filters"]["filters"], expected_output["filters"])

    def test_multiple_empty_required_keys(self):
        print("testing multiple empty required keys")
        full_input_dict = {"location":[{"chromosome":"chr21","starting_position":"40", "ending_position":"45"}],
                           "allele": {"allele_id": []},
                           "annotation": {"gene": [], "symbol": [], "feature": []}}
        name = "assay_dummy"
        id = "id_dummy"
        project_context = "project-dummy"
        expected_output = {
            "filters": {
                "variant_read_optimized$allele_id": [
                    {
                        "condition": "in",
                        "values": [],
                        "geno_bins": [
                            {
                                "chr": "21",
                                "start": 40,
                                "end": 45
                            }
                        ] 
                    }
                ],
                "variant_read_optimized$CHROM": [
                    {
                        "condition": "in",
                        "values": [
                            "chr21"
                        ]
                    }
                ],
                "variant_read_optimized$tumor_normal": [
                    {
                        "condition": "is",
                        "values": "tumor"
                    }
                ]
            }
        }
        test_payload, _ = somatic_final_payload(full_input_dict, name, id, project_context, genome_reference=None, additional_fields=None, include_normal=False)
        self.assertEqual(test_payload["raw_filters"]["assay_filters"]["filters"], expected_output["filters"])

    def test_additional_fields(self):
        print("testing --additional-fields")
        input_filter_path = os.path.join(self.e2e_filter_directory, "single_location.json")
        output_path = os.path.join(
            self.general_output_dir, self.dataset, "e2e_output", "additional_fields_output.tsv"
        )

        command = 'dx extract_assay somatic {} --retrieve-variant {} --output {} --additional-fields "{}"'.format(
            self.test_record,
            input_filter_path,
            output_path,
            "sample_id,tumor_normal,symbolic_type",
        )

        process = subprocess.check_output(command, shell=True)

    def test_tumor_normal(self):
        print("testing --include-normal-sample")
        input_filter_path = os.path.join(self.e2e_filter_directory, "single_location.json")
        output_path = os.path.join(
            self.general_output_dir, self.dataset, "e2e_output", "tumor_normal_output.tsv"
        )

        command = 'dx extract_assay somatic {} --retrieve-variant {} --output {} --include-normal-sample --additional-fields "{}"'.format(
            self.test_record,
            input_filter_path,
            output_path,
            "sample_id,tumor_normal",
        )

        process = subprocess.check_output(command, shell=True)

    def test_retrieve_meta_info(self):
        print("testing --retrieve-meta-info")
        expected_result = b"e79cdc96ab517d8d3eebafa8ffe4469b  -\n"

        if python_version == 2:
            # subprocess pipe doesn't work with python 2, just check to make sure the command runs in that case
            command = "dx extract_assay somatic {} --retrieve-meta-info --output - > /dev/null".format(self.test_record)
            #print(command)
            process = subprocess.check_output(command,shell=True)
        else:
            with subprocess.Popen(
                ["dx", "extract_assay", "somatic", self.test_record, "--retrieve-meta-info", "--output", "-"],
                stdout=subprocess.PIPE,
            ) as p1:
                p2 = subprocess.Popen(
                    ["md5sum"], stdin=p1.stdout, stdout=subprocess.PIPE
                )
                out, err = p2.communicate()

            self.assertEqual(expected_result, out)

    ####
    # Input validation test
    ####

    def test_malformed_json(self):
        # For somatic assays, json validation is not in a single function
        malformed_json_dir = os.path.join(self.general_input_dir, "malformed_json")
        malformed_json_filenames = os.listdir(malformed_json_dir)
        for name in malformed_json_filenames:
            filter_path = os.path.join(malformed_json_dir, name)
            command = "dx extract_assay somatic {} --retrieve-variant {}".format(
                self.test_record,
                os.path.join(malformed_json_dir, filter_path),
            )
            try:
                process = subprocess.check_output(
                    command,
                    shell=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                print("Uh oh, malformed JSON passed detection, file is {}".format(name))
            except:
                print("malformed json {} detected succesfully".format(name))

    #####
    # E2E tests
    #####

    def test_e2e_filters(self):
        print("Testing e2e filters")
        filter_files = os.listdir(self.e2e_filter_directory)

        for filter_name in filter_files:
            print("testing {}".format(filter_name))
            output_filename = filter_name[:-5] + "_output.tsv"
            command = (
                "dx extract_assay somatic {} --retrieve-variant {} --output {}".format(
                    self.test_record,
                    os.path.join(self.e2e_filter_directory, filter_name),
                    os.path.join(self.e2e_output_directory, output_filename),
                )
            )
            process = subprocess.check_call(command, shell=True)
            # print(command)


if __name__ == "__main__":
    unittest.main()
