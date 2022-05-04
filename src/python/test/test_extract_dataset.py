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
from dxpy.cli.dataset_utilities import DXDataset
from dxpy.utils.resolver import resolve_existing_path
from dxpy.bindings import DXRecord

class TestDXDataset(unittest.TestCase):
    def test_e2e_dataset(self):
        dataset_record = "project-G9j1pX00vGPzF2XQ7843k2Jq:record-G9k12VQ06G1P42KK7fFK3yKB"
        self.end_to_end(dataset_record)

    def test_e2e_cohortbrowser(self):
        cohort_record = "project-G9j1pX00vGPzF2XQ7843k2Jq:record-G9k3pGj0vGPvKg77BP1Yxq8q"
        self.end_to_end(cohort_record)

    def end_to_end(self, input_record):
        out_directory = tempfile.mkdtemp()
        project, path, entity_result = resolve_existing_path(input_record)
        if entity_result['describe']['types'] == ['DatabaseQuery', 'CohortBrowser']:
            dataset_id = DXRecord(DXRecord(entity_result['id'],project).get_details()['dataset']['$dnanexus_link']).get_id()
        elif entity_result['describe']['types'] == ['Dataset']:
            dataset_id = entity_result['id']
        rec = DXDataset(dataset_id,project=project)
        write_out = rec.get_dictionary().write(output_path=out_directory, file_name_prefix=rec.name)
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
            dframe2 = pd.read_csv(os.path.join(out_directory, f"{rec.name}.{file}")).dropna(axis=1, how='all').sort_index(axis=1)
            if file == 'coding_dictionary.csv':
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