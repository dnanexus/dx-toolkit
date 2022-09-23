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
from __future__ import print_function, unicode_literals, division, absolute_import

import unittest
import tempfile
import shutil
import os
import re
import subprocess
import pandas as pd
import dxpy
from dxpy_testutil import cd, chdir

class TestDXExtractDataset(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        proj_name = "dx-toolkit_test_data"
        proj_id = list(dxpy.find_projects(describe=False, level='VIEW', name=proj_name))[0]['id']
        cd(proj_id + ":/")

    def test_e2e_dataset_ddd(self):
        dataset_record = "dx-toolkit_test_data:Extract_Dataset/extract_dataset_test"
        out_directory = tempfile.mkdtemp()
        cmd = ["dx", "extract_dataset", dataset_record, "-ddd", "-o", out_directory]
        subprocess.check_call(cmd)
        self.end_to_end_ddd(out_directory=out_directory, rec_name = "extract_dataset_test")

    def test_e2e_cohortbrowser_ddd(self):
        cohort_record = "dx-toolkit_test_data:Extract_Dataset/Combined_Cohort_Test"
        out_directory = tempfile.mkdtemp()
        cmd = ["dx", "extract_dataset", cohort_record, "-ddd", "-o", out_directory]
        subprocess.check_call(cmd)
        self.end_to_end_ddd(out_directory=out_directory, rec_name = "Combined_Cohort_Test")

    def test_e2e_dataset_sql(self):
        dataset_record = "dx-toolkit_test_data:Extract_Dataset/extract_dataset_test"
        truth_output = "SELECT `patient_0001_1`.`patient_id` AS `patient.patient_id`, `patient_0001_1`.`name` AS `patient.name`, `patient_0002_1`.`weight` AS `patient.weight`, `patient_0001_1`.`date_of_birth` AS `patient.date_of_birth`, `patient_0002_1`.`verified_dtm` AS `patient.verified_dtm`, `test_1`.`test_id` AS `test.test_id`, `trial_visit_0001_1`.`visit_id` AS `trial_visit.visit_id`, `baseline_0001_1`.`baseline_id` AS `baseline.baseline_id`, `hospital_0001_1`.`hospital_id` AS `hospital.hospital_id`, `doctor_0001_1`.`doctor_id` AS `doctor.doctor_id` FROM `database_[a-z0-9]{24}__extract_dataset_test`.`patient_0001` AS `patient_0001_1` LEFT OUTER JOIN `database_[a-z0-9]{24}__extract_dataset_test`.`patient_0002` AS `patient_0002_1` ON `patient_0001_1`.`patient_id` = `patient_0002_1`.`patient_id` LEFT OUTER JOIN `database_[a-z0-9]{24}__extract_dataset_test`.`trial_visit_0001` AS `trial_visit_0001_1` ON `patient_0001_1`.`patient_id` = `trial_visit_0001_1`.`visit_patient_id` LEFT OUTER JOIN `database_[a-z0-9]{24}__extract_dataset_test`.`test` AS `test_1` ON `trial_visit_0001_1`.`visit_id` = `test_1`.`test_visit_id` LEFT OUTER JOIN `database_[a-z0-9]{24}__extract_dataset_test`.`baseline_0001` AS `baseline_0001_1` ON `patient_0001_1`.`patient_id` = `baseline_0001_1`.`b_patient_id` LEFT OUTER JOIN `database_[a-z0-9]{24}__extract_dataset_test`.`hospital_0001` AS `hospital_0001_1` ON `patient_0001_1`.`hid` = `hospital_0001_1`.`hospital_id` LEFT OUTER JOIN `database_[a-z0-9]{24}__extract_dataset_test`.`doctor_0001` AS `doctor_0001_1` ON `trial_visit_0001_1`.`visit_did` = `doctor_0001_1`.`doctor_id` WHERE `patient_0001_1`.`patient_id` IN \(SELECT DISTINCT `patient_0001_1`.`patient_id` AS `patient_id` FROM `database_[a-z0-9]{24}__extract_dataset_test`.`patient_0001` AS `patient_0001_1`\);"
        cmd = ["dx", "extract_dataset", dataset_record, "--fields", "patient.patient_id" , ",", "patient.name", ",", "patient.weight", ",",
               "patient.date_of_birth", ",", "patient.verified_dtm", ",", "test.test_id", ",", "trial_visit.visit_id", ",", "baseline.baseline_id", 
               ",", "hospital.hospital_id", ",", "doctor.doctor_id","--sql", "-o", "-"]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
        stdout = process.communicate()[0]
        self.assertTrue(re.match(truth_output,stdout.strip()))

    def test_e2e_cohortbrowser_sql(self):
        cohort_record = "dx-toolkit_test_data:Extract_Dataset/Combined_Cohort_Test"
        truth_output = "SELECT `patient_0001_1`.`patient_id` AS `patient.patient_id`, `patient_0001_1`.`name` AS `patient.name`, `patient_0002_1`.`weight` AS `patient.weight`, `patient_0001_1`.`date_of_birth` AS `patient.date_of_birth`, `patient_0002_1`.`verified_dtm` AS `patient.verified_dtm`, `test_1`.`test_id` AS `test.test_id`, `trial_visit_0001_1`.`visit_id` AS `trial_visit.visit_id`, `baseline_0001_1`.`baseline_id` AS `baseline.baseline_id`, `hospital_0001_1`.`hospital_id` AS `hospital.hospital_id`, `doctor_0001_1`.`doctor_id` AS `doctor.doctor_id` FROM `database_[a-z0-9]{24}__extract_dataset_test`.`patient_0001` AS `patient_0001_1` LEFT OUTER JOIN `database_[a-z0-9]{24}__extract_dataset_test`.`patient_0002` AS `patient_0002_1` ON `patient_0001_1`.`patient_id` = `patient_0002_1`.`patient_id` LEFT OUTER JOIN `database_[a-z0-9]{24}__extract_dataset_test`.`trial_visit_0001` AS `trial_visit_0001_1` ON `patient_0001_1`.`patient_id` = `trial_visit_0001_1`.`visit_patient_id` LEFT OUTER JOIN `database_[a-z0-9]{24}__extract_dataset_test`.`test` AS `test_1` ON `trial_visit_0001_1`.`visit_id` = `test_1`.`test_visit_id` LEFT OUTER JOIN `database_[a-z0-9]{24}__extract_dataset_test`.`baseline_0001` AS `baseline_0001_1` ON `patient_0001_1`.`patient_id` = `baseline_0001_1`.`b_patient_id` LEFT OUTER JOIN `database_[a-z0-9]{24}__extract_dataset_test`.`hospital_0001` AS `hospital_0001_1` ON `patient_0001_1`.`hid` = `hospital_0001_1`.`hospital_id` LEFT OUTER JOIN `database_[a-z0-9]{24}__extract_dataset_test`.`doctor_0001` AS `doctor_0001_1` ON `trial_visit_0001_1`.`visit_did` = `doctor_0001_1`.`doctor_id` WHERE `patient_0001_1`.`patient_id` IN \(SELECT DISTINCT `patient_0001_1`.`patient_id` AS `patient_id` FROM `database_[a-z0-9]{24}__extract_dataset_test`.`patient_0001` AS `patient_0001_1` WHERE `patient_0001_1`.`patient_id` IN \(SELECT `patient_id` FROM \(SELECT DISTINCT `cohort_subquery`.`patient_id` AS `patient_id` FROM \(SELECT DISTINCT `patient_0001_1`.`patient_id` AS `patient_id`, `patient_0001_1`.`hid` AS `hid` FROM `database_[a-z0-9]{24}__extract_dataset_test`.`patient_0001` AS `patient_0001_1` WHERE EXISTS \(SELECT `hospital_0001_1`.`hospital_id` AS `hospital_id` FROM `database_[a-z0-9]{24}__extract_dataset_test`.`hospital_0001` AS `hospital_0001_1` WHERE `hospital_0001_1`.`hospital_id` BETWEEN 2 AND 5 AND `hospital_0001_1`.`hospital_id` = `patient_0001_1`.`hid`\)\) AS `cohort_subquery` INTERSECT SELECT DISTINCT `patient_0001_1`.`patient_id` AS `patient_id` FROM `database_[a-z0-9]{24}__extract_dataset_test`.`patient_0001` AS `patient_0001_1` WHERE `patient_0001_1`.`patient_id` BETWEEN 2 AND 9\)\)\);"
        cmd = ["dx", "extract_dataset", cohort_record, "--fields", "patient.patient_id" , ",", "patient.name", ",", "patient.weight", ",",
               "patient.date_of_birth", ",", "patient.verified_dtm", ",", "test.test_id", ",", "trial_visit.visit_id", ",", "baseline.baseline_id", 
               ",", "hospital.hospital_id", ",", "doctor.doctor_id","--sql", "-o", "-"]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
        stdout = process.communicate()[0]
        self.assertTrue(re.match(truth_output,stdout.strip()))

    def test_e2e_dataset_fields(self):
        dataset_record = "dx-toolkit_test_data:Extract_Dataset/extract_dataset_test"
        out_directory = tempfile.mkdtemp()
        cmd = ["dx", "extract_dataset", dataset_record, "--fields", "patient.patient_id" , ",", "patient.name", ",", "patient.weight", ",",
               "patient.date_of_birth", ",", "patient.verified_dtm", ",", "test.test_id", ",", "trial_visit.visit_id", ",", "baseline.baseline_id", 
               ",", "hospital.hospital_id", ",", "doctor.doctor_id", "-o", out_directory]
        subprocess.check_call(cmd)
        truth_file = "dx-toolkit_test_data:Extract_Dataset/extract_dataset_test.csv"
        self.end_to_end_fields(out_directory=out_directory, rec_name = "extract_dataset_test.csv", truth_file=truth_file)

    def test_e2e_cohortbrowser_fields(self):
        cohort_record = "dx-toolkit_test_data:Extract_Dataset/Combined_Cohort_Test"
        out_directory = tempfile.mkdtemp()
        cmd = ["dx", "extract_dataset", cohort_record, "--fields", "patient.patient_id" , ",", "patient.name", ",", "patient.weight", ",",
               "patient.date_of_birth", ",", "patient.verified_dtm", ",", "test.test_id", ",", "trial_visit.visit_id", ",", "baseline.baseline_id", 
               ",", "hospital.hospital_id", ",", "doctor.doctor_id", "-o", out_directory]
        subprocess.check_call(cmd)
        truth_file = "dx-toolkit_test_data:Extract_Dataset/Combined_Cohort_Test.csv"
        self.end_to_end_fields(out_directory=out_directory, rec_name = "Combined_Cohort_Test.csv", truth_file=truth_file)
    
    def test_file_already_exists(self):
        cohort_record = "dx-toolkit_test_data:Extract_Dataset/Combined_Cohort_Test"
        out_directory = tempfile.mkdtemp()
        open(os.path.join(out_directory, "Combined_Cohort_Test.csv"), 'w').close()
        cmd = ["dx", "extract_dataset", cohort_record, "--fields", "patient.patient_id" , ",", "patient.name", "-o", out_directory]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        stdout = process.communicate()[0]
        self.assertTrue("Error: path already exists" in stdout.strip())
        shutil.rmtree(out_directory)

    def end_to_end_ddd(self, out_directory, rec_name):
        truth_files_directory = tempfile.mkdtemp()
        with chdir(truth_files_directory):
            cmd = ["dx", "download", "dx-toolkit_test_data:Extract_Dataset/data_dictionary.csv", 
                                    "dx-toolkit_test_data:Extract_Dataset/codings.csv",
                                    "dx-toolkit_test_data:Extract_Dataset/entity_dictionary.csv"]
            subprocess.check_call(cmd)
            os.chdir("..")
            truth_file_list = os.listdir(truth_files_directory)

            for file in truth_file_list:
                dframe1 = pd.read_csv(os.path.join(truth_files_directory, file)).dropna(axis=1, how='all').sort_index(axis=1)
                fil_nam = rec_name + "." + file
                dframe2 = pd.read_csv(os.path.join(out_directory, fil_nam)).dropna(axis=1, how='all').sort_index(axis=1)
                if file == 'codings.csv':
                    #continue
                    dframe1 = dframe1.sort_values(by=['code','coding_name'], axis=0).reset_index(drop=True)
                    dframe2 = dframe2.sort_values(by=['code','coding_name'], axis=0).reset_index(drop=True)
                elif file in ['entity_dictionary.csv', 'data_dictionary.csv']:
                    dframe1 = dframe1.sort_values(by='entity', axis=0).reset_index(drop=True)
                    dframe2 = dframe2.sort_values(by='entity', axis=0).reset_index(drop=True)
                self.assertTrue(dframe1.equals(dframe2))
            
            shutil.rmtree(out_directory)
            shutil.rmtree(truth_files_directory)

    def end_to_end_fields(self, out_directory, rec_name, truth_file):
        truth_files_directory = tempfile.mkdtemp()
        with chdir(truth_files_directory):
            cmd = ["dx", "download", truth_file]
            subprocess.check_call(cmd)
            os.chdir("..")
            dframe1 = pd.read_csv(os.path.join(truth_files_directory,os.listdir(truth_files_directory)[0]))
            dframe1 = dframe1.sort_values(by=list(dframe1.columns), axis=0).reset_index(drop=True)
            dframe2 = pd.read_csv(os.path.join(out_directory, rec_name))
            dframe2 = dframe2.sort_values(by=list(dframe2.columns), axis=0).reset_index(drop=True)
            self.assertTrue(dframe1.equals(dframe2))

            shutil.rmtree(out_directory)
            shutil.rmtree(truth_files_directory)

if __name__ == '__main__':
    unittest.main()