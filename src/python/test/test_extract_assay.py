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
import dxpy
import os
import subprocess
import json
import sys

from dxpy_testutil import cd
from dxpy.dx_extract_utils.filter_to_payload import (
    retrieve_geno_bins,
    basic_filter,
    location_filter,
    generate_assay_filter,
    final_payload,
    validate_JSON,
)

python_version = sys.version_info.major

dirname = os.path.dirname(__file__)

test_project = "dx-toolkit_test_data"
test_record = "{}:Extract_Assay_Germline/test01_dataset".format(test_project)

output_folder = os.path.join(dirname, "extract_assay_germline/test_output/")
malformed_json_dir = os.path.join(
    dirname, "ea_malformed_json"
)

# Controls whether output files for the end to end tests are written to file or stdout
write_output = False
if write_output:
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)


class TestDXExtractAssay(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.proj_id = list(
            dxpy.find_projects(describe=False, level="VIEW", name=test_project)
        )[0]["id"]
        cd(cls.proj_id + ":/")

    ############
    # Unit Tests
    ############

    # Test retrieve_geno_bins
    def test_retrieve_geno_bins(self):
        # list_of_genes, project, genome_reference
        list_of_genes = ["ENSG00000173213"]
        genome_reference = "GRCh38.92"
        expected_output = [{"chr": "18", "start": "47390", "end": "49557"}]
        self.assertEqual(
            retrieve_geno_bins(list_of_genes, self.proj_id, genome_reference),
            expected_output,
        )

    def test_basic_filter_allele(self):
        table = "allele"
        friendly_name = "rsid"
        values = ["rs1342568097"]
        genome_reference = "Homo_sapiens.GRCh38.92"

        expected_output = {
            "allele$dbsnp151_rsid": [{"condition": "any", "values": ["rs1342568097"]}]
        }

        self.assertEqual(
            basic_filter(
                table, friendly_name, values, self.proj_id, genome_reference
            ),
            expected_output,
        )

    def test_basic_filter_annotation(self):
        table = "annotation"
        friendly_name = "gene_id"
        values = ["ENSG00000173213"]
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
            basic_filter(
                table, friendly_name, values, self.proj_id, genome_reference
            ),
            expected_output,
        )

    def test_basic_filter_genotype(self):
        table = "genotype"
        friendly_name = "allele_id"
        values = ["18_47361_T_G"]
        genome_reference = "Homo_sapiens.GRCh38.92"

        expected_output = {
            "allele$a_id": [{"condition": "in", "values": ["18_47361_T_G"]}]
        }

        self.assertEqual(
            basic_filter(
                table, friendly_name, values, self.proj_id, genome_reference
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

        self.assertEqual(location_filter(location_list), expected_output)

    def test_generate_assay_filter(self):
        # A small payload, uses allele_rsid.json
        full_input_dict = {"rsid": ["rs1342568097"]}
        name = "test01_assay"
        id = "c6e9c0ea-5752-4299-8de2-8620afba7b82"
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
            generate_assay_filter(
                full_input_dict,
                name,
                id,
                self.proj_id,
                genome_reference,
                filter_type,
            ),
            expected_output,
        )

    def test_final_payload(self):
        full_input_dict = {"rsid": ["rs1342568097"]}
        name = "test01_assay"
        id = "c6e9c0ea-5752-4299-8de2-8620afba7b82"
        genome_reference = "Homo_sapiens.GRCh38.92"
        filter_type = "allele"

        expected_output_payload = {
            "project_context": self.proj_id,
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
            "is_cohort": True,
            "distinct": True,
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

        test_payload, test_fields = final_payload(
            full_input_dict,
            name,
            id,
            self.proj_id,
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
        validate_JSON(filter, type)

    def test_malformed_json(self):
        for filter_type in ["allele", "annotation", "genotype"]:
            malformed_json_filenames = os.listdir(
                os.path.join(malformed_json_dir, filter_type)
            )
            for name in malformed_json_filenames:
                file_path = os.path.join(malformed_json_dir, filter_type, name)
                with open(file_path, "r") as infile:
                    filter = json.load(infile)
                    try:
                        validate_JSON(filter, filter_type)
                        print(
                            "Uh oh, malformed JSON passed detection, file is {}".format(
                                file_path
                            )
                        )
                    except:
                        print("task failed succesfully")

    ##########
    # Normal Command Lines
    ##########

    def test_json_help(self):
        """Print the help text for the retrieve allele filter"""
        # TODO this should eventually be compared to a static output
        command = "dx extract_assay germline fakepath --retrieve-allele --json-help > /dev/null"
        process = subprocess.check_call(command, shell=True)

    def test_generic_help(self):
        """Test the generic help message"""
    command = "dx extract_assay germline -h > /dev/null"
    process = subprocess.check_call(command, shell=True)

    # Does not write any output to file, function only outputs to stdout
    def test_list_assays(self):
        print("testing --list-assays")
        command = "dx extract_assay germline {} --list-assays".format(test_record)
        subprocess.check_call(command, shell=True)

    # A test of the --assay-name functionality, returns the same output as allele_rsid.json
    def test_assay_name(self):
        print("testing --assay-name")
        output_filename = os.path.join(output_folder, "assay_name_output.tsv")
        allele_rsid_filter = {"rsid": ["rs1342568097"]}

        command = "dx extract_assay germline {} --assay-name test01_assay --retrieve-allele '{}' --output {}".format(
            test_record,
            json.dumps(allele_rsid_filter),
            output_filename if write_output else "- > /dev/null",
        )
        subprocess.check_call(command, stderr=subprocess.STDOUT, shell=True)

    ###########
    # Malformed command lines
    ###########
    

    def test_filter_mutex(self):
        print("testing filter mutex")
        """Ensure that the failure mode of multiple filter types being provided is caught"""
        # Grab two random filter JSONs of different types
        allele_json = "{\"rsid\": [\"rs1342568097\"]}"
        annotation_json = "{\"allele_id\": [\"18_47408_G_A\"]}"

        command = (
            "dx extract_assay germline {} --retrieve-allele {} --retrieve-annotation {} - 2>&1 /dev/null".format(
                test_record,
                allele_json,
                annotation_json
            )
        )
        try:
            process = subprocess.check_output(command, shell=True)
            print("Uh oh, malformed command line passed detection")
        except:
            print("filter mutex failed succesfully")

if __name__ == "__main__":
    unittest.main()
