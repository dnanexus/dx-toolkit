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


# Run manually with python2 and python3 src/python/test/test_extract_assay.py

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


class TestDXExtractAssay(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_project_name = "dx-toolkit_test_data"
        cls.test_record = "{}:Extract_Assay_Germline/test01_dataset".format(
            test_project_name
        )
        cls.output_folder = os.path.join(dirname, "extract_assay_germline/test_output/")
        cls.malformed_json_dir = os.path.join(dirname, "ea_malformed_json")
        cls.proj_id = list(
            dxpy.find_projects(describe=False, level="VIEW", name=test_project_name)
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
        genome_reference = "GRCh38.92"

        expected_output = {
            "allele$dbsnp151_rsid": [{"condition": "any", "values": ["rs1342568097"]}]
        }

        self.assertEqual(
            basic_filter(table, friendly_name, values, self.proj_id, genome_reference),
            expected_output,
        )

    def test_basic_filter_annotation(self):
        table = "annotation"
        friendly_name = "gene_id"
        values = ["ENSG00000173213"]
        genome_reference = "GRCh38.92"

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
            basic_filter(table, friendly_name, values, self.proj_id, genome_reference),
            expected_output,
        )

    def test_basic_filter_genotype(self):
        table = "genotype"
        friendly_name = "allele_id"
        values = ["18_47361_T_G"]
        genome_reference = "GRCh38.92"

        expected_output = {
            "allele$a_id": [{"condition": "in", "values": ["18_47361_T_G"]}]
        }

        self.assertEqual(
            basic_filter(table, friendly_name, values, self.proj_id, genome_reference),
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
        genome_reference = "GRCh38.92"
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
        genome_reference = "GRCh38.92"
        filter_type = "allele"

        expected_output_payload = {
            "project_context": self.proj_id,
            "order_by": [{"allele_id":"asc"}],
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

        try:
            validate_JSON(filter, type)
        except:
            self.fail("This just needs to complete without error")

    def test_malformed_json(self):
        for filter_type in ["allele", "annotation", "genotype"]:
            malformed_json_filenames = os.listdir(
                os.path.join(self.malformed_json_dir, filter_type)
            )
            for name in malformed_json_filenames:
                file_path = os.path.join(self.malformed_json_dir, filter_type, name)
                with open(file_path, "r") as infile:
                    filter = json.load(infile)
                with self.assertRaises(SystemExit) as cm:
                    validate_JSON(filter, filter_type)
                self.assertEqual(cm.exception.code, 1)

    def test_bad_rsid(self):
        filter = {"rsid": ["rs1342568097","rs1342568098"]}
        test_project = "dx-toolkit_test_data"
        test_record = "{}:Extract_Assay_Germline/test01_dataset".format(test_project)

        command = ["dx", "extract_assay", "germline", test_record, "--retrieve-allele", json.dumps(filter)]
        process = subprocess.Popen(command, stderr=subprocess.PIPE, universal_newlines=True)
        expected_error_message = "At least one rsID provided in the filter is not present in the provided dataset or cohort"
        self.assertTrue(expected_error_message in process.communicate()[1])

    ##########
    # Normal Command Lines
    ##########

    def test_json_help(self):
        """Check successful call of help for the retrieve allele filter"""
        command = ["dx", "extract_assay", "germline", "fakepath", "--retrieve-allele", "--json-help"]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=True)
        expected_help = """# Filters and respective definitions
#
#  rsid: rsID associated with an allele or set of alleles. If multiple values
#  are provided, the conditional search will be, "OR." For example, ["rs1111",
#  "rs2222"], will search for alleles which match either "rs1111" or "rs2222".
#  String match is case sensitive.
#
#  type: Type of allele. Accepted values are "SNP", "Ins", "Del", "Mixed". If
#  multiple values are provided, the conditional search will be, "OR." For
#  example, ["SNP", "Ins"], will search for variants which match either "SNP"
#  or "Ins". String match is case sensitive.
#
#  dataset_alt_af: Dataset alternate allele frequency, a json object with
#  empty content or two sets of key/value pair: {min: 0.1, max:0.5}. Accepted
#  numeric value for each key is between and including 0 and 1.  If a user
#  does not want to apply this filter but still wants this information in the
#  output, an empty json object should be provided.
#
#  gnomad_alt_af: gnomAD alternate allele frequency. a json object with empty
#  content or two sets of key/value pair: {min: 0.1, max:0.5}. Accepted value
#  for each key is between 0 and 1. If a user does not want to apply this
#  filter but still wants this information in the output, an empty json object
#  should be provided.
#
#  location: Genomic range in the reference genome where the starting position
#  of alleles fall into. If multiple values are provided in the list, the
#  conditional search will be, "OR." String match is case sensitive.
#
# JSON filter template for --retrieve-allele
{
  "rsid": ["rs11111", "rs22222"],
  "type": ["SNP", "Del", "Ins"],
  "dataset_alt_af": {"min": 0.001, "max": 0.05},
  "gnomad_alt_af": {"min": 0.001, "max": 0.05},
  "location": [
    {
      "chromosome": "1",
      "starting_position": "10000",
      "ending_position": "20000"
    },
    {
      "chromosome": "X",
      "starting_position": "500",
      "ending_position": "1700"
    }
  ]
}
"""
        self.assertEqual(expected_help, process.communicate()[0])

    def test_generic_help(self):
        """Test the generic help message"""
        command = "dx extract_assay germline -h > /dev/null"
        subprocess.check_call(command, shell=True)

    def test_list_assays(self):
        command = ["dx", "extract_assay", "germline", self.test_record, "--list-assays"]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=True)
        self.assertEqual("test01_assay", process.communicate()[0].strip())


    def test_assay_name(self):
        """A test of the --assay-name functionality, returns the same output."""
        allele_rsid_filter = json.dumps({"rsid": ["rs1342568097"]})
        command1 = ["dx", "extract_assay", "germline", self.test_record, "--assay-name", "test01_assay", "--retrieve-allele", allele_rsid_filter, "-o", "-"]
        process1 = subprocess.Popen(command1, stdout=subprocess.PIPE, universal_newlines=True)
        command2 = ["dx", "extract_assay", "germline", self.test_record, "--retrieve-allele", allele_rsid_filter, "-o", "-"]
        process2 = subprocess.Popen(command2, stdout=subprocess.PIPE, universal_newlines=True)
        self.assertEqual(process1.communicate(), process2.communicate())

    ###########
    # Malformed command lines
    ###########

    def test_filter_mutex(self):
        print("testing filter mutex")
        """Ensure that the failure mode of multiple filter types being provided is caught"""
        # Grab two random filter JSONs of different types
        allele_json = '{"rsid": ["rs1342568097"]}'
        annotation_json = '{"allele_id": ["18_47408_G_A"]}'
        command = ["dx", "extract_assay", "germline", self.test_record,  "--retrieve-allele", allele_json, "--retrieve-annotation", annotation_json, "-o", "-"]

        process = subprocess.Popen(command, stderr=subprocess.PIPE, universal_newlines=True)
        expected_error_message = "dx extract_assay germline: error: argument --retrieve-annotation: not allowed with argument --retrieve-allele"
        self.assertTrue(expected_error_message in process.communicate()[1])


if __name__ == "__main__":
    unittest.main()
