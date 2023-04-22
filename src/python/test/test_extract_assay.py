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
import json
from dxpy_testutil import cd, chdir
from dxpy.dx_extract_utils.filter_to_payload import (
    retrieve_geno_bins,
    BasicFilter,
    LocationFilter,
    GenerateAssayFilter,
    FinalPayload,
    ValidateJSON,
)

test_project = "dx-toolkit_test_data"
test_record = "{}:Extract_Assay_Germline/test01_dataset".format(test_project)
test_filter_directory = "/dx-toolkit/src/python/test/extract_assay_germline/test_input/"
output_folder = "/dx-toolkit/src/python/test/extract_assay_germline/test_output/"
# Controls whether output files for the end to end tests are written to file or stdout
write_output = False
if write_output:
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)


class TestDXExtractAssay(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        proj_name = "dx-toolkit_test_data"
        proj_id = list(
            dxpy.find_projects(describe=False, level="VIEW", name=proj_name)
        )[0]["id"]
        cd(proj_id + ":/")

    ############
    # Unit Tests
    ############

    # Test retrieve_geno_bins
    def test_retrieve_geno_bins(self):
        # list_of_genes, project, genome_reference
        list_of_genes = ["ENSG00000173213"]
        project = "project-G9j1pX00vGPzF2XQ7843k2Jq"
        genome_reference = "GRCh38.92"
        expected_output = [{"chr": "18", "start": "47390", "end": "49557"}]
        self.assertEqual(
            retrieve_geno_bins(list_of_genes, project, genome_reference),
            expected_output,
        )

    def test_basic_filter_allele(self):
        table = "allele"
        friendly_name = "rsid"
        values = ["rs1342568097"]
        project_context = "project-G9j1pX00vGPzF2XQ7843k2Jq"
        genome_reference = "Homo_sapiens.GRCh38.92"

        expected_output = {
            "allele$dbsnp151_rsid": [{"condition": "any", "values": ["rs1342568097"]}]
        }

        self.assertEqual(
            BasicFilter(
                table, friendly_name, values, project_context, genome_reference
            ),
            expected_output,
        )

    def test_basic_filter_annotation(self):
        table = "annotation"
        friendly_name = "gene_id"
        values = ["ENSG00000173213"]
        project_context = "project-G9j1pX00vGPzF2XQ7843k2Jq"
        genome_reference = "Homo_sapiens.GRCh38.92"

        expected_output = {
            "annotation$gene_id": [
                {
                    "condition": "in",
                    "values": ["ENSG00000173213"],
                    "geno_bins": [{"chr": "18", "start": "47390", "end": "49557"}],
                }
            ]
        }

        self.assertEqual(
            BasicFilter(
                table, friendly_name, values, project_context, genome_reference
            ),
            expected_output,
        )

    def test_basic_filter_genotype(self):
        table = "genotype"
        friendly_name = "allele_id"
        values = ["18_47361_T_G"]
        project_context = "project-G9j1pX00vGPzF2XQ7843k2Jq"
        genome_reference = "Homo_sapiens.GRCh38.92"

        expected_output = {
            "allele$a_id": [{"condition": "in", "values": ["18_47361_T_G"]}]
        }

        self.assertEqual(
            BasicFilter(
                table, friendly_name, values, project_context, genome_reference
            ),
            expected_output,
        )

    def test_location_filter(self):
        location_list = [
            {
                "chromosome": "18",
                "starting_position": "47361",
                "ending_position": "47364",
            }
        ]

        expected_output = {
            "allele$a_id": [
                {
                    "condition": "in",
                    "values": [],
                    "geno_bins": [{"chr": "18", "start": 47361, "end": 47364}],
                }
            ]
        }

        self.assertEqual(LocationFilter(location_list), expected_output)

    # TODO location filter with two location

    def test_generate_assay_filter(self):
        # A small payload, uses allele_rsid.json
        full_input_dict = {"rsid": ["rs1342568097"]}
        name = "test01_assay"
        id = "c6e9c0ea-5752-4299-8de2-8620afba7b82"
        project_context = "project-G9j1pX00vGPzF2XQ7843k2Jq"
        genome_reference = "Homo_sapiens.GRCh38.92"
        filter_type = "allele"

        expected_output = {
            "assay_filters": {
                "name": "test01_assay",
                "id": "c6e9c0ea-5752-4299-8de2-8620afba7b82",
                "filters": {
                    "allele$dbsnp151_rsid": [
                        {"condition": "any", "values": ["rs1342568097"]}
                    ]
                },
                "logic": "and",
            }
        }

        self.assertEqual(
            GenerateAssayFilter(
                full_input_dict,
                name,
                id,
                project_context,
                genome_reference,
                filter_type,
            ),
            expected_output,
        )

    def test_final_payload(self):
        full_input_dict = {"rsid": ["rs1342568097"]}
        name = "test01_assay"
        id = "c6e9c0ea-5752-4299-8de2-8620afba7b82"
        project_context = "project-G9j1pX00vGPzF2XQ7843k2Jq"
        genome_reference = "Homo_sapiens.GRCh38.92"
        filter_type = "allele"

        expected_output_payload = {
            "project_context": "project-G9j1pX00vGPzF2XQ7843k2Jq",
            "fields": [
                {"allele_id": "allele$a_id"},
                {"chromosome": "allele$chr"},
                {"starting_position": "allele$pos"},
                {"ref": "allele$ref"},
                {"alt": "allele$alt"},
                {"rsid": "allele$dbsnp151_rsid"},
                {"allele_type": "allele$allele_type"},
                {"dataset_alt_freq": "allele$alt_freq"},
                {"gnomad_alt_freq": "allele$gnomad201_alt_freq"},
                {"worst_effect": "allele$worst_effect"},
            ],
            "adjust_geno_bins": False,
            "raw_filters": {
                "assay_filters": {
                    "name": "test01_assay",
                    "id": "c6e9c0ea-5752-4299-8de2-8620afba7b82",
                    "filters": {
                        "allele$dbsnp151_rsid": [
                            {"condition": "any", "values": ["rs1342568097"]}
                        ]
                    },
                    "logic": "and",
                }
            },
            "is_cohort": False,
        }

        expected_output_fields = [
            "allele_id",
            "chromosome",
            "starting_position",
            "ref",
            "alt",
            "rsid",
            "allele_type",
            "dataset_alt_freq",
            "gnomad_alt_freq",
            "worst_effect",
        ]

        test_payload, test_fields = FinalPayload(
            full_input_dict,
            name,
            id,
            project_context,
            genome_reference,
            filter_type,
        )

        self.assertEqual(
            test_payload,
            expected_output_payload,
        )
        self.assertEqual(test_fields, expected_output_fields)

    def test_validate_json(self):
        filter = {
            "rsid": ["rs1342568097"],
            "type": ["SNP", "Del", "Ins"],
            "dataset_alt_af": {"min": 1e-05, "max": 0.5},
            "gnomad_alt_af": {"min": 1e-05, "max": 0.5},
        }
        type = "allele"

        # This just needs to complete without error
        ValidateJSON(filter, type)

    def test_malformed_json(self):
        malformed_json_dir = "/dx-toolkit/src/python/test/extract_assay_germline/test_input/malformed_json"
        for filter_type in ["allele", "annotation", "genotype"]:
            malformed_json_filenames = os.listdir(
                os.path.join(malformed_json_dir, filter_type)
            )
            for name in malformed_json_filenames:
                file_path = os.path.join(malformed_json_dir, filter_type, name)
                with open(file_path, "r") as infile:
                    filter = json.load(infile)
                    try:
                        ValidateJSON(filter, filter_type)
                        print(
                            "Uh oh, malformed JSON passed detection, file is {}".format(
                                file_path
                            )
                        )
                    except:
                        print("task failed succesfully")

    ###########
    # E2E Tests
    ###########

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
                        os.path.join(output_folder, output_filename)
                        if write_output
                        else "- > /dev/null",
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
                    output_filename if write_output else "- > /dev/null",
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
                output_filename if write_output else "- > /dev/null",
            )
            process = subprocess.check_call(command, shell=True)

    # A test that ensures that the location filter functions with other allele filters
    def test_allele_location_type(self):
        print("Testing allele filter with location and allele type fields")
        multi_filter_directory = os.path.join(test_filter_directory, "multi_filters")
        filter_file = os.path.join(multi_filter_directory, "allele_location_type.json")
        output_filename = os.path.join(output_folder, "allele_location_type_output.tsv")
        command = "dx extract_assay germline {} --retrieve-{} {} --output {}".format(
            test_record,
            "allele",
            filter_file,
            output_filename if write_output else "- > /dev/null",
        )
        process = subprocess.check_call(command, shell=True)

    # A test of the --list-assays functionality
    # Does not write any output to file, function only outputs to stdout
    def test_list_assays(self):
        print("testing --list-assays")
        command = "dx extract_assay germline {} --list-assays".format(test_record)
        subprocess.check_call(command, shell=True)

    # A test of the --assay-name functionality, returns the same output as allele_rsid.json
    def test_assay_name(self):
        print("testing --assay-name")
        single_filter_directory = os.path.join(test_filter_directory, "single_filters")
        output_filename = os.path.join(output_folder, "assay_name_output.tsv")

        command = "dx extract_assay germline {} --assay-name test01_assay --retrieve-allele {} --output {}".format(
            test_record,
            os.path.join(single_filter_directory, "allele/allele_rsid.json"),
            output_filename if write_output else "- > /dev/null",
        )
        subprocess.check_call(command, stderr=subprocess.STDOUT, shell=True)


if __name__ == "__main__":
    unittest.main()
