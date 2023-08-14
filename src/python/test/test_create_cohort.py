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


class TestDXCreateCohort(unittest.TestCase):
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

    def test_retrieve_cohort_id(self):
        pass

    def test_accept_file_ids(self):
        pass

    def test_accept_cli_ids(self):
        pass


    