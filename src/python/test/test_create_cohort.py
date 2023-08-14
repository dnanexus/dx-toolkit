from __future__ import print_function, unicode_literals, division, absolute_import
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

# Run manually with python2 and python3 src/python/test/test_create_cohort.py


import unittest
import tempfile
import shutil
import os
import re
import subprocess
import pandas as pd
import dxpy
import sys
from dxpy_testutil import cd, chdir

from dxpy.cli.dataset_utilities import (
    get_assay_name_info,
    resolve_validate_record_path,
    DXDataset,
)


dirname = os.path.dirname(__file__)

python_version = sys.version_info.major

class TestCreateCohort(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        proj_name = "dx-toolkit_test_data"
        proj_id = list(dxpy.find_projects(describe=False, level='VIEW', name=proj_name))[0]['id']
        cd(proj_id + ":/")
        cls.general_input_dir = os.path.join(dirname, "create_cohort_test_files/input/")
        # cls.general_output_dir = os.path.join(dirname, "create_cohort_test_files/output/")

        #TODO: setup project folders 
        cls.test_record = "{}:/Create_Cohort/somatic_indels_1k".format(
                proj_name
            )

    def test_help_text(self):
        print("testing help text")

        expected_result = "fae9f07f1aad8cf69223ca666b20de35"

        command = 'dx create_cohort --help'

        process = subprocess.check_output(command, shell=True,text=True)

        test_md5sum = hashlib.md5(process.encode("utf-8")).hexdigest()

        self.assertEqual(expected_result,test_md5sum)


    def test_retrieve_cohort_id(self):
        pass

    def test_accept_file_ids(self):
        pass

    def test_accept_cli_ids(self):
        pass


if __name__ == "__main__":
    unittest.main()
