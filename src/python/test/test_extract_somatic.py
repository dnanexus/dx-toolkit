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


# Run manually with python3 src/python/test/test_extract_somatic.py

# Unit testing for the dx extract_assay somatic command
# Similar to the dx extract_assay germline tests

import dxpy
import unittest

from dxpy_testutil import cd
from dxpy.cli.dataset_utilities import get_assay_name_info, resolve_validate_path, DXDataset

test_project = "PMUX-1324-SCIPROD-CLISAM"
test_record = "{}:test_datasets/assay_title_annot_complete"

proj_id = list(
            dxpy.find_projects(describe=False, level="VIEW", name=test_project)
        )[0]["id"]

class TestDXExtractAssaySomatic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.proj_id = list(
            dxpy.find_projects(describe=False, level="VIEW", name=test_project)
        )[0]["id"]
        cd(proj_id + ":/")

    ############
    # Unit Tests
    ############

    def test_get_assay_name_info(self):
        list_assays = True
        assay_name = "annot_complete"
        path = test_record
        friendly_assay_type = "somatic"

        project, entity_result, resp, dataset_project = resolve_validate_path(path)
        dataset_id = resp["dataset"]
        rec_descriptor = DXDataset(dataset_id, project=dataset_project).get_descriptor()

        selected_assay_name, selected_assay_id, selected_ref_genome = get_assay_name_info(args.list_assays,args.assay_name,args.path,"somatic",rec_descriptor)

        # TODO generate expected results for this
