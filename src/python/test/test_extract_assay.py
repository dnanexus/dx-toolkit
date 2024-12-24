#!/usr/bin/env python3
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

from unittest.mock import patch
from io import StringIO

from parameterized import parameterized

from dxpy_testutil import cd
from dxpy.dx_extract_utils.filter_to_payload import (
    retrieve_geno_bins,
    basic_filter,
    location_filter,
    generate_assay_filter,
    final_payload,
    validate_JSON,
)
from dxpy.dx_extract_utils.germline_utils import (
    filter_results,
    _produce_loci_dict,
    infer_genotype_type
)
from dxpy.cli.dataset_utilities import (
    DXDataset,
    resolve_validate_record_path,
    get_assay_name_info,
)
from dxpy.dx_extract_utils.input_validation import validate_filter_applicable_genotype_types


python_version = sys.version_info.major

dirname = os.path.dirname(__file__)


class TestDXExtractAssay(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_project_name = "dx-toolkit_test_data"
        cls.test_v1_record = "{}:/Extract_Assay_Germline/test01_dataset".format(
            test_project_name
        )
        cls.test_record = "{}:/Extract_Assay_Germline/test01_v1_0_1_dataset".format(
            test_project_name
        )
        cls.test_non_alt_record = "{}:/Extract_Assay_Germline/test03_dataset".format(
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
    def test_get_assay_name_info(self):
        # Set to true for the list assay utilities response instead of the normal functionality
        list_assays = False
        # When assay name is none, function looks for and selects first assay of type somatic that it finds
        assay_name = None
        friendly_assay_type = "germline"
        project, entity_result, resp, dataset_project = resolve_validate_record_path(
            self.test_record
        )
        dataset_id = resp["dataset"]
        rec_descriptor = DXDataset(dataset_id, project=dataset_project).get_descriptor()
        # Expected Results
        expected_assay_name = "test01_assay"
        expected_assay_id = "cc5dcc31-000c-4a2c-b225-ecad6233a0a3"
        expected_ref_genome = "GRCh38.92"
        expected_additional_descriptor_info = {
            "exclude_refdata": True,
            "exclude_halfref": True,
            "exclude_nocall": True,
            "genotype_type_table": "genotype_alt_read_optimized",
        }

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

        self.assertEqual(location_filter(location_list, "allele"), expected_output)

    def test_genotype_location_filter(self):
        location_list = [
            {
                "chromosome": "18",
                "starting_position": "47361",
            }
        ]

        expected_output = {
            "genotype$a_id": [
                {
                    "condition": "in",
                    "values": [],
                    "geno_bins": [{"chr": "18", "start": 47361, "end": 47361}],
                }
            ]
        }

        self.assertEqual(location_filter(location_list, "genotype"), expected_output)

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
        test_record = "{}:Extract_Assay_Germline/test01_v1_0_1_dataset".format(test_project)

        command = ["dx", "extract_assay", "germline", test_record, "--retrieve-allele", json.dumps(filter)]
        process = subprocess.Popen(command, stderr=subprocess.PIPE, universal_newlines=True)
        expected_error_message = "At least one rsID provided in the filter is not present in the provided dataset or cohort"
        self.assertTrue(expected_error_message in process.communicate()[1])

    def test_duplicate_rsid(self):
        table = "allele"
        friendly_name = "rsid"
        values = ["rs1342568097", "rs1342568097"]
        genome_reference = "GRCh38.92"

        expected_output = {
            "allele$dbsnp151_rsid": [{"condition": "any", "values": ["rs1342568097"]}]
        }

        self.assertEqual(
            basic_filter(table, friendly_name, values, self.proj_id, genome_reference),
            expected_output,
        )

    # Test filters for exclusion options
    def test_no_call_warning(self):
        # This is a test of the validate_filter_applicable_genotype_types function "no-call", "ref" requested
        filter_dict = {"genotype_type": ["no-call", "ref"]}
        exclude_nocall = True
        exclude_refdata = True
        infer_nocall = False
        expected_warnings = [
            "WARNING: Filter requested genotype type 'no-call', genotype entries of this type were not ingested in the provided dataset and the --infer-nocall flag is not set!",
            "WARNING: Filter requested genotype type 'ref', genotype entries of this type were not ingested in the provided dataset and the --infer-ref flag is not set!"
            ]
        with patch("sys.stderr", new=StringIO()) as fake_err:
            validate_filter_applicable_genotype_types(
                infer_nocall, infer_ref=False, filter_dict=filter_dict,
                exclude_refdata=exclude_refdata, exclude_nocall=exclude_nocall, exclude_halfref=False
            )
            output = fake_err.getvalue().strip()
            for warning in expected_warnings:
                self.assertIn(warning, output)

    def test_no_genotype_type_warning(self):
        # This is a test of the validate_filter_applicable_genotype_types function no genotype type requested
        filter_dict = {"genotype_type": []}
        exclude_nocall = True

        with patch("sys.stderr", new=StringIO()) as fake_err:
            validate_filter_applicable_genotype_types(
                infer_nocall=False, infer_ref=False, filter_dict=filter_dict,
                exclude_refdata=False, exclude_nocall=exclude_nocall, exclude_halfref=False
            )
            output = fake_err.getvalue().strip()
            self.assertEqual(output, "WARNING: No genotype type requested in the filter. All genotype types will be returned. Genotype entries of type 'no-call' were not ingested in the provided dataset and the --infer-nocall flag is not set!")

    def test_no_genotype_type_warning_exclude_halfref(self):
        # This is a test of the validate_filter_applicable_genotype_types function half genotype type requested
        filter_dict = {"genotype_type": []}
        exclude_halfref = True

        with patch("sys.stderr", new=StringIO()) as fake_err:
            validate_filter_applicable_genotype_types(
                infer_nocall=False, infer_ref=False, filter_dict=filter_dict,
                exclude_refdata=False, exclude_nocall=False, exclude_halfref=exclude_halfref
            )
            output = fake_err.getvalue().strip()
            self.assertEqual(output, "WARNING: No genotype type requested in the filter. All genotype types will be returned.  'half-ref' genotype entries (0/.) were not ingested in the provided dataset!")
    
    def test_filter_results(self):
        # Define sample input data
        results = [
            {
                "sample_id": "SAMPLE_1",
                "allele_id": "1_1076145_A_AT",
                "locus_id": "1_1076145_A_T",
                "chromosome": "1",
                "starting_position": 1076145,
                "ref": "A",
                "alt": "AT",
                "genotype_type": "het-alt",
            },
            {
                "sample_id": "SAMPLE_2",
                "allele_id": "1_1076146_A_AT",
                "locus_id": "1_1076146_A_T",
                "chromosome": "1",
                "starting_position": 1076146,
                "ref": "A",
                "alt": "AT",
                "genotype_type": "het-alt",
            },
            {
                "sample_id": "SAMPLE_3",
                "allele_id": "1_1076147_A_AT",
                "locus_id": "1_1076147_A_T",
                "chromosome": "1",
                "starting_position": 1076147,
                "ref": "A",
                "alt": "AT",
                "genotype_type": "ref",
            },
        ]
        # Call the function to filter the results
        filtered_results = filter_results(
            results=results, key="genotype_type", restricted_values=["het-alt"]
        )

        # Define the expected output
        expected_output = [
            {
                "sample_id": "SAMPLE_3",
                "allele_id": "1_1076147_A_AT",
                "locus_id": "1_1076147_A_T",
                "chromosome": "1",
                "starting_position": 1076147,
                "ref": "A",
                "alt": "AT",
                "genotype_type": "ref",
            },
        ]

        # Assert that the filtered results match the expected output
        self.assertEqual(filtered_results, expected_output)

    def test_produce_loci_dict(self):
        # Define the input data
        loci = [
            {
                "locus_id": "18_47361_A_T",
                "chromosome": "18",
                "starting_position": 47361,
                "ref": "A",
            },
            {
                "locus_id": "X_1000_C_A",
                "chromosome": "X",
                "starting_position": 1000,
                "ref": "C",
            },
            {
                "locus_id": "1_123_A_.",
                "chromosome": "1",
                "starting_position": 123,
                "ref": "A",
            },
        ]
        results_entries = [
            {
                "locus_id": "18_47361_A_T",
                "allele_id": "18_47361_A_T",
                "sample_id": "sample1",
                "chromosome": "18",
                "starting_position": 47361,
                "ref": "A",
                "alt": "T",
            },
            {
                "locus_id": "18_47361_A_T",
                "allele_id": "18_47361_A_G",
                "sample_id": "sample2",
                "chromosome": "18",
                "starting_position": 47361,
                "ref": "A",
                "alt": "G",
            },
            {
                "locus_id": "X_1000_C_A",
                "allele_id": "X_1000_C_A",
                "sample_id": "sample1",
                "chromosome": "X",
                "starting_position": 1000,
                "ref": "C",
                "alt": "A",
            },
        ]

        # Define the expected output
        expected_output = {
            "18_47361_A_T": {
                "samples": {"sample1", "sample2"},
                "entry": {
                    "allele_id": None,
                    "locus_id": "18_47361_A_T",
                    "chromosome": "18",
                    "starting_position": 47361,
                    "ref": "A",
                    "alt": None,
                },
            },
            "X_1000_C_A": {
                "samples": {"sample1"},
                "entry": {
                    "allele_id": None,
                    "locus_id": "X_1000_C_A",
                    "chromosome": "X",
                    "starting_position": 1000,
                    "ref": "C",
                    "alt": None,
                },
            },
            "1_123_A_.": {
                "samples": set(),
                "entry": {
                    "allele_id": None,
                    "locus_id": "1_123_A_.",
                    "chromosome": "1",
                    "starting_position": 123,
                    "ref": "A",
                    "alt": None,
                },
            },
        }

        # Call the function
        result = _produce_loci_dict(loci, results_entries)

        # Assert the result
        self.assertEqual(result, expected_output)

    def test_infer_genotype_type(self):
        samples = ["SAMPLE_1", "SAMPLE_2", "SAMPLE_3"]
        loci = [
            {
                "locus_id": "1_1076145_A_T",
                "chromosome": "1",
                "starting_position": 1076145,
                "ref": "A",
            },
            {
                "locus_id": "2_1042_G_CC",
                "chromosome": "2",
                "starting_position": 1042,
                "ref": "G",
            },
        ]
        result_entries = [
            {
                "sample_id": "SAMPLE_2",
                "allele_id": "1_1076145_A_AT",
                "locus_id": "1_1076145_A_T",
                "chromosome": "1",
                "starting_position": 1076145,
                "ref": "A",
                "alt": "AT",
                "genotype_type": "het-alt",
            },
            {
                "sample_id": "SAMPLE_3",
                "allele_id": "1_1076145_A_T",
                "locus_id": "1_1076145_A_T",
                "chromosome": "1",
                "starting_position": 1076145,
                "ref": "A",
                "alt": "T",
                "genotype_type": "hom-ref",
            },
        ]
        type_to_infer = "no-call"

        expected_output = [
            {
                "sample_id": "SAMPLE_1",
                "allele_id": None,
                "locus_id": "1_1076145_A_T",
                "chromosome": "1",
                "starting_position": 1076145,
                "ref": "A",
                "alt": None,
                "genotype_type": "no-call",
            },
            {
                "sample_id": "SAMPLE_1",
                "allele_id": None,
                "locus_id": "2_1042_G_CC",
                "chromosome": "2",
                "starting_position": 1042,
                "ref": "G",
                "alt": None,
                "genotype_type": "no-call",
            },
            {
                "sample_id": "SAMPLE_2",
                "allele_id": None,
                "locus_id": "2_1042_G_CC",
                "chromosome": "2",
                "starting_position": 1042,
                "ref": "G",
                "alt": None,
                "genotype_type": "no-call",
            },
            {
                "sample_id": "SAMPLE_3",
                "allele_id": None,
                "locus_id": "2_1042_G_CC",
                "chromosome": "2",
                "starting_position": 1042,
                "ref": "G",
                "alt": None,
                "genotype_type": "no-call",
            },
        ]

        output = infer_genotype_type(samples, loci, result_entries, type_to_infer)
        self.assertEqual(output, result_entries + expected_output)
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
#  String match is case sensitive. Duplicate values are permitted and will be
#  handled silently.
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


    @parameterized.expand([
        ("test_record", ["ref", "het-ref", "hom", "het-alt", "half", "no-call"]),
        ("test_v1_record", ["het-ref", "hom", "het-alt", "half"]),
    ])
    def test_retrieve_genotype(self, record, genotype_types):
        """Testing --retrieve-genotype functionality"""
        allele_genotype_type_filter = json.dumps({
            "allele_id": ["18_47408_G_A"], 
            "genotype_type": genotype_types,
            })
        expected_result = "sample_1_3\t18_47408_G_A\t18_47408_G_A\t18\t47408\tG\tA\thet-ref"
        command = ["dx", "extract_assay", "germline", getattr(self, record), "--retrieve-genotype", allele_genotype_type_filter, "-o", "-"]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=True)
        self.assertIn(expected_result, process.communicate()[0])


    @unittest.skip("Vizserver implementation of PMUX-1652 needs to be deployed")
    def test_retrieve_non_alt_genotype(self):
        """Testing --retrieve-genotype functionality"""
        location_genotype_type_filter = json.dumps({
            "location": [{
                "chromosome": "20",
                "starting_position": "14370",
            }],
            "genotype_type": ["ref", "half", "no-call"]
        })
        # not a comprehensive list
        expected_results = [
            "S01_m_m\t\t20_14370_G_A\t20\t14370\tG\t\tno-call",
            "S02_m_0\t\t20_14370_G_A\t20\t14370\tG\t\thalf",
            "S06_0_m\t\t20_14370_G_A\t20\t14370\tG\t\thalf",
            "S07_0_0\t\t20_14370_G_A\t20\t14370\tG\t\tref",
        ]
        command = ["dx", "extract_assay", "germline", self.test_non_alt_record, "--retrieve-genotype", location_genotype_type_filter, "-o", "-"]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=True)
        result = process.communicate()[0]
        [self.assertIn(expected_result, result) for expected_result in expected_results]

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
