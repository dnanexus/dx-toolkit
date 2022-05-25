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

    def end_to_end_ddd(self, out_directory, rec_name):
        truth_files_directory = tempfile.mkdtemp()
        os.chdir(truth_files_directory)
        cmd = ["dx", "download", "project-G9j1pX00vGPzF2XQ7843k2Jq:file-G9k2Yv80vGPbgP551jJ8Xbpx", 
                                 "project-G9j1pX00vGPzF2XQ7843k2Jq:file-G9jv8pj0vGPx7yPZBP49y9KB",
                                 "project-G9j1pX00vGPzF2XQ7843k2Jq:file-G9jv8pj0vGPj85byBG764zxV"]
        subprocess.check_call(cmd)
        os.chdir("..")
        truth_file_list = os.listdir(truth_files_directory)
        print(os.listdir(out_directory))

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

if __name__ == '__main__':
    unittest.main()