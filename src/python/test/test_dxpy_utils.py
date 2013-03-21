#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 DNAnexus, Inc.
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

import os, unittest, json, tempfile, subprocess, csv, shutil, re
import dxpy.utils.describe

class TestDescribe(unittest.TestCase):
    def test_is_job_ref(self):
        # Positive results
        jobref = {"job": "job-B55ZF5kZKQGz1Xxyb5FQ0003", "field": "number"}
        self.assertTrue(dxpy.utils.describe.is_job_ref(jobref))
        jobref = {"$dnanexus_link": jobref}
        self.assertTrue(dxpy.utils.describe.is_job_ref(jobref))

        # Negative results
        jobref = {"job": "job-B55ZF5kZKQGz1Xxyb5FQ0003", "field": "number", "other": "field"}
        self.assertFalse(dxpy.utils.describe.is_job_ref(jobref))
        jobref = {"job": "job-B55ZF5kZKQGz1Xxyb5FQ0003", "field": 32}
        self.assertFalse(dxpy.utils.describe.is_job_ref(jobref))
        jobref = {"$dnanexus_link": jobref}
        self.assertFalse(dxpy.utils.describe.is_job_ref(jobref))
        jobref = {"$dnanexus_link": "job-B55ZF5kZKQGz1Xxyb5FQ0003"}
        self.assertFalse(dxpy.utils.describe.is_job_ref(jobref))

    def test_get_resolved_jbors(self):
        resolved_jbors = {}
        orig_thing = {"job": "job-B55ZF5kZKQGz1Xxyb5FQ0003", "field": "number"}
        resolved_thing = 32
        dxpy.utils.describe.get_resolved_jbors(resolved_thing, orig_thing, resolved_jbors)
        self.assertIn("job-B55ZF5kZKQGz1Xxyb5FQ0003:number", resolved_jbors)

        resolved_jbors = {}
        orig_thing = {"$dnanexus_link": {"job": "job-B55ZF5kZKQGz1Xxyb5FQ0003", "field": "number"}}
        resolved_thing = 32
        dxpy.utils.describe.get_resolved_jbors(resolved_thing, orig_thing, resolved_jbors)
        self.assertIn("job-B55ZF5kZKQGz1Xxyb5FQ0003:number", resolved_jbors)

if __name__ == '__main__':
    unittest.main()
