#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2013-2016 DNAnexus, Inc.
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
from __future__ import print_function, unicode_literals, division, absolute_import
from re import X

import unittest
import tempfile
import shutil
import os
import subprocess
import pandas as pd

class TestDXExtractDataset(unittest.TestCase):
    def test_e2e_dataset_ddd(self):
        dataset_record = "project-G9j1pX00vGPzF2XQ7843k2Jq:record-G9k12VQ06G1P42KK7fFK3yKB"
        out_directory = tempfile.mkdtemp()
        cmd = ["dx", "extract_dataset", dataset_record, "-ddd", "-o", out_directory]
        subprocess.check_call(cmd)
        self.end_to_end_ddd(out_directory=out_directory, rec_name = "test_dml_out01")

    def test_e2e_cohortbrowser_ddd(self):
        cohort_record = "project-G9j1pX00vGPzF2XQ7843k2Jq:record-G9k3pGj0vGPvKg77BP1Yxq8q"
        out_directory = tempfile.mkdtemp()
        cmd = ["dx", "extract_dataset", cohort_record, "-ddd", "-o", out_directory]
        subprocess.check_call(cmd)
        self.end_to_end_ddd(out_directory=out_directory, rec_name = "test_cohort")

    def test_e2e_dataset_sql(self):
        dataset_record = "project-G9j1pX00vGPzF2XQ7843k2Jq:record-G9k12VQ06G1P42KK7fFK3yKB"
        truth_output = "SELECT `patient_1`.`patient_id` AS `patient.patient_id`, `patient_1`.`name` AS `patient.name`, `patient_1`.`weight` AS `patient.weight`, `patient_1`.`height` AS `patient.height`, `patient_1`.`size` AS `patient.size` FROM `database_g9k1260089qpxpf468f9zybj__test_dml_out01`.`patient` AS `patient_1` WHERE `patient_1`.`patient_id` IN (SELECT DISTINCT `patient_1`.`patient_id` AS `patient_id` FROM `database_g9k1260089qpxpf468f9zybj__test_dml_out01`.`patient` AS `patient_1`);"
        cmd = ["dx", "extract_dataset", dataset_record, "--fields", "patient.patient_id" , ",", "patient.name", ",", "patient.weight", ",",
            "patient.height", ",", "patient.size","--sql", "-o", "-"]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
        stdout = process.communicate()[0]
        self.assertTrue(truth_output==stdout.strip())

    def test_e2e_cohortbrowser_sql(self):
        cohort_record = "project-G9j1pX00vGPzF2XQ7843k2Jq:record-GB8ZQ9Q0vGPk8xzV4JZF288p"
        truth_output = "SELECT `patient_1`.`patient_id` AS `patient.patient_id`, `patient_1`.`name` AS `patient.name`, `patient_1`.`weight` AS `patient.weight`, `patient_1`.`height` AS `patient.height`, `patient_1`.`size` AS `patient.size` FROM `database_g9k1260089qpxpf468f9zybj__test_dml_out01`.`patient` AS `patient_1` WHERE `patient_1`.`patient_id` IN (SELECT DISTINCT `patient_1`.`patient_id` AS `patient_id` FROM `database_g9k1260089qpxpf468f9zybj__test_dml_out01`.`patient` AS `patient_1` WHERE `patient_1`.`patient_id` IN (SELECT `patient_id` FROM (SELECT DISTINCT `patient_1`.`patient_id` AS `patient_id` FROM `database_g9k1260089qpxpf468f9zybj__test_dml_out01`.`patient` AS `patient_1` WHERE `patient_1`.`height` BETWEEN 68 AND 70 INTERSECT SELECT DISTINCT `patient_1`.`patient_id` AS `patient_id` FROM `database_g9k1260089qpxpf468f9zybj__test_dml_out01`.`patient` AS `patient_1` WHERE UNIX_TIMESTAMP(`patient_1`.`dob`) BETWEEN 925516800 AND 988675200)));"
        cmd = ["dx", "extract_dataset", cohort_record, "--fields", "patient.patient_id" , ",", "patient.name", ",", "patient.weight", ",",
            "patient.height", ",", "patient.size","--sql", "-o", "-"]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
        stdout = process.communicate()[0]
        self.assertTrue(truth_output==stdout.strip())

    def test_e2e_dataset_fields(self):
        dataset_record = "project-G9j1pX00vGPzF2XQ7843k2Jq:record-G9k12VQ06G1P42KK7fFK3yKB"
        out_directory = tempfile.mkdtemp()
        cmd = ["dx", "extract_dataset", dataset_record, "--fields", "patient.patient_id" , ",", "patient.name", ",", "patient.weight", ",",
            "patient.height", ",", "patient.size", "-o", out_directory]
        subprocess.check_call(cmd)
        truth_file = "project-G9j1pX00vGPzF2XQ7843k2Jq:file-GBBpbq80vGPVB1Q1K9Vq7YQV"
        self.end_to_end_fields(out_directory=out_directory, rec_name = "test_dml_out01.txt", truth_file=truth_file)

    def test_e2e_cohortbrowser_fields(self):
        cohort_record = "project-G9j1pX00vGPzF2XQ7843k2Jq:record-GB8ZQ9Q0vGPk8xzV4JZF288p"
        out_directory = tempfile.mkdtemp()
        cmd = ["dx", "extract_dataset", cohort_record, "--fields", "patient.patient_id" , ",", "patient.name", ",", "patient.weight", ",",
            "patient.height", ",", "patient.size", "-o", out_directory]
        subprocess.check_call(cmd)
        truth_file = "project-G9j1pX00vGPzF2XQ7843k2Jq:file-GBBpbq80vGPy35K9B1kyVQ6k"
        self.end_to_end_fields(out_directory=out_directory, rec_name = "Combined_Cohort.txt", truth_file=truth_file)

    def end_to_end_ddd(self, out_directory, rec_name):
        truth_files_directory = tempfile.mkdtemp()
        os.chdir(truth_files_directory)
        cmd = ["dx", "download", "project-G9j1pX00vGPzF2XQ7843k2Jq:file-G9k2Yv80vGPbgP551jJ8Xbpx", 
                                 "project-G9j1pX00vGPzF2XQ7843k2Jq:file-G9jv8pj0vGPx7yPZBP49y9KB",
                                 "project-G9j1pX00vGPzF2XQ7843k2Jq:file-G9jv8pj0vGPj85byBG764zxV"]
        subprocess.check_call(cmd)
        os.chdir("..")
        truth_file_list = os.listdir(truth_files_directory)

        for file in truth_file_list:
            dframe1 = pd.read_csv(os.path.join(truth_files_directory, file)).dropna(axis=1, how='all').sort_index(axis=1)
            dframe2 = pd.read_csv(os.path.join(out_directory, f"{rec_name}.{file}")).dropna(axis=1, how='all').sort_index(axis=1)
            if file == 'codings.csv':
                dframe1 = dframe1.sort_values(by='code', axis=0, ignore_index=True)
                dframe2 = dframe2.sort_values(by='code', axis=0, ignore_index=True)
            if file == 'entity_dictionary.csv':
                dframe1 = dframe1.sort_values(by='entity', axis=0, ignore_index=True)
                dframe2 = dframe2.sort_values(by='entity', axis=0, ignore_index=True)
            self.assertTrue(dframe1.equals(dframe2))
        
        shutil.rmtree(out_directory)
        shutil.rmtree(truth_files_directory)

    def end_to_end_fields(self, out_directory, rec_name, truth_file):
        truth_files_directory = tempfile.mkdtemp()
        os.chdir(truth_files_directory)
        cmd = ["dx", "download", truth_file]
        subprocess.check_call(cmd)
        os.chdir("..")
        dframe1 = pd.read_csv(os.path.join(truth_files_directory,os.listdir(truth_files_directory)[0]))
        dframe1 = dframe1.sort_values(by=list(dframe1.columns), axis=0, ignore_index=True)
        dframe2 = pd.read_csv(os.path.join(out_directory, rec_name))
        dframe2 = dframe2.sort_values(by=list(dframe2.columns), axis=0, ignore_index=True)
        self.assertTrue(dframe1.equals(dframe2))

if __name__ == '__main__':
    unittest.main()