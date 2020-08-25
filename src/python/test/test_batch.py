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

import os, sys, unittest, json, tempfile, subprocess, csv, shutil, re

import dxpy
from dxpy_testutil import (DXTestCase, check_output, temporary_project,
                           select_project,
                           run, DXCalledProcessError)
import dxpy_testutil as testutil
from dxpy.exceptions import DXAPIError, DXSearchError, EXPECTED_ERR_EXIT_STATUS, HTTPError
from dxpy.compat import USING_PYTHON2, str, sys_encoding, open
# from dxpy.utils.resolver import ResolutionError, _check_resolution_needed as check_resolution

if USING_PYTHON2:
    delimiter = '\t'.encode("utf-8")
    write_mode = 'wb'
else:
    write_mode = 'w'
    delimiter = '\t'

@unittest.skipUnless(testutil.TEST_RUN_JOBS,
                     'skipping test that would run jobs')
class TestDXRunBatch(DXTestCase):
    def test_basic(self):
        # write python code into code.py file
        tmp_path = tempfile.mkdtemp()
        code_path = os.path.join(tmp_path, 'code.py')
        with open(code_path, write_mode) as f:
            f.write("@dxpy.entry_point('main')\n")
            f.write("def main(**job_inputs):\n")
            f.write("\toutput = {}\n")
            f.write("\toutput['thresholds'] = job_inputs['thresholds']\n")
            f.write("\toutput['pie'] = job_inputs['pie'] + 1\n")
            f.write("\toutput['misc'] = {'n': 'non', 'y': 'oui'}\n")
            f.write("\treturn output\n")
            f.write("\n")
            f.write("dxpy.run()\n")
        with open(code_path, 'r') as f:
            code = f.read()

        # write arguments table
        arg_table = os.path.join(tmp_path, 'table.csv')
        with open(arg_table, write_mode) as csvfile:
            writer = csv.writer(csvfile, delimiter=delimiter)
            header = ["batch ID", "thresholds", "pie", "misc"]
            writer.writerow(header)
            writer.writerow(["SRR_1", "[10,81]", "3.12", "{}"])

        applet = dxpy.api.applet_new({
            "name": "copy_all",
            "project": self.project,
            "dxapi": "1.0.0",
            "inputSpec": [ { "name": "thresholds", "class": "array:int"},
                           { "name": "pie", "class": "float" },
                           { "name": "misc", "class": "hash" } ],
            "outputSpec": [ { "name": "thresholds", "class": "array:int" },
                            { "name": "pie", "class": "float" },
                            { "name": "misc", "class": "hash" } ],
            "runSpec": { "interpreter": "python2.7",
                         "code": code,
                         "distribution": "Ubuntu",
                         "release": "14.04" }
        })

        # run in batch mode
        job_id = run("dx run {} --batch-tsv={} --yes --brief"
                     .format(applet["id"], arg_table)).strip()
        job_desc = dxpy.api.job_describe(job_id)
        self.assertEqual(job_desc["executableName"], 'copy_all')
        self.assertEqual(job_desc["input"], { "thresholds": [10,81],
                                               "misc": {},
                                               "pie": 3.12 })

        # run in batch mode with --batch-folders
        job_id = run("dx run {} --batch-tsv={} --batch-folders --yes --brief"
                     .format(applet["id"], arg_table)).strip()
        job_desc = dxpy.api.job_describe(job_id)
        self.assertEqual(job_desc["folder"], "/SRR_1")

        # run in batch mode with --batch-folders and --destination
        job_id = run("dx run {} --batch-tsv={} --batch-folders --destination={}:/run_01 --yes --brief"
                     .format(applet["id"], arg_table, self.project)).strip()
        job_desc = dxpy.api.job_describe(job_id)
        self.assertEqual(job_desc["folder"], "/run_01/SRR_1")

    def test_files(self):
        # Create file with junk content
        dxfile = dxpy.upload_string("xxyyzz", project=self.project,
                                    wait_on_close=True, name="bubbles")

        # write python code into code.py file
        tmp_path = tempfile.mkdtemp()
        code_path = os.path.join(tmp_path, 'code.py')
        with open(code_path, write_mode) as f:
            f.write("@dxpy.entry_point('main')\n")
            f.write("def main(**job_inputs):\n")
            f.write("\toutput = {}\n")
            f.write("\toutput['plant'] = job_inputs['plant']\n")
            f.write("\treturn output\n")
            f.write("\n")
            f.write("dxpy.run()\n")
        with open(code_path, 'r') as f:
            code = f.read()

        # write arguments table
        arg_table = os.path.join(tmp_path, 'table.csv')
        with open(arg_table, write_mode) as csvfile:
            writer = csv.writer(csvfile, delimiter=delimiter)
            header = ["batch ID", "plant", "plant ID"]
            writer.writerow(header)
            writer.writerow(["SRR_1", "bubbles", dxfile.get_proj_id() +":"+ dxfile.get_id()])

        applet = dxpy.api.applet_new({
            "name": "copy_file",
            "project": self.project,
            "dxapi": "1.0.0",
            "inputSpec": [ { "name": "plant", "class": "file" } ],
            "outputSpec": [ { "name": "plant", "class": "file" } ],
            "runSpec": { "interpreter": "python2.7",
                         "code": code,
                         "distribution": "Ubuntu",
                         "release": "14.04" }
        })
        job_id = run("dx run {} --batch-tsv={} --yes --brief"
                     .format(applet["id"], arg_table)).strip()
        job_desc = dxpy.api.job_describe(job_id)
        self.assertEqual(job_desc["executableName"], 'copy_file')
        self.assertEqual(job_desc["input"], { "plant": {"$dnanexus_link": {"project": dxfile.get_proj_id(), "id":dxfile.get_id()} }})

    def test_file_arrays(self):
        # Create file with junk content
        dxfile = dxpy.upload_string("xxyyzz", project=self.project,
                                    wait_on_close=True, name="bubbles")

        # write python code into code.py file
        tmp_path = tempfile.mkdtemp()
        code_path = os.path.join(tmp_path, 'code.py')
        with open(code_path, write_mode) as f:
            f.write("@dxpy.entry_point('main')\n")
            f.write("def main(**job_inputs):\n")
            f.write("\toutput = {}\n")
            f.write("\toutput['plant'] = job_inputs['plant']\n")
            f.write("\treturn output\n")
            f.write("\n")
            f.write("dxpy.run()\n")
        with open(code_path, 'r') as f:
            code = f.read()

        # write arguments table. These ara arrays with a single element.
        arg_table = os.path.join(tmp_path, 'table.csv')
        with open(arg_table, write_mode) as csvfile:
            writer = csv.writer(csvfile, delimiter=delimiter)
            header = ["batch ID", "plant", "plant ID"]
            writer.writerow(header)
            writer.writerow(["SRR_1",
                             "[bubbles]",
                             "[" + dxfile.get_proj_id() +":"+ dxfile.get_id() + "]"
            ])

        applet = dxpy.api.applet_new({
            "name": "ident_file_array",
            "project": self.project,
            "dxapi": "1.0.0",
            "inputSpec": [ { "name": "plant", "class": "array:file" } ],
            "outputSpec": [ { "name": "plant", "class": "array:file" } ],
            "runSpec": { "interpreter": "python2.7",
                         "code": code,
                         "distribution": "Ubuntu",
                         "release": "14.04" }
        })
        job_id = run("dx run {} --batch-tsv={} --yes --brief"
                     .format(applet["id"], arg_table)).strip()
        job_desc = dxpy.api.job_describe(job_id)
        self.assertEqual(job_desc["executableName"], 'ident_file_array')
        self.assertEqual(job_desc["input"],
                         { "plant":
                           [{ "$dnanexus_link": {"project" : dxfile.get_proj_id(), "id":  dxfile.get_id()} }]
                         })


if __name__ == '__main__':
    if 'DXTEST_FULL' not in os.environ:
        sys.stderr.write('WARNING: env var DXTEST_FULL is not set; tests that create apps or run jobs will not be run\n')
    unittest.main()
