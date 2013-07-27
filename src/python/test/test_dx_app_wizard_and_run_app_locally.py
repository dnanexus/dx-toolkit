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

import os, sys, unittest, json, tempfile, subprocess, csv, shutil, re
import pexpect

from dxpy_testutil import DXTestCase
import dxpy_testutil as testutil

import dxpy
from dxpy.scripts import dx_build_app

def run_dx_app_wizard():
    old_cwd = os.getcwd()
    tempdir = tempfile.mkdtemp()
    os.chdir(tempdir)
    try:
        wizard = pexpect.spawn("dx-app-wizard")
        wizard.logfile = sys.stdout
        wizard.expect("App Name:")
        wizard.sendline("MyTestApp")
        wizard.expect("Title")
        wizard.sendline()
        wizard.expect("Summary")
        wizard.sendline()
        wizard.expect("Description")
        wizard.sendline()
        wizard.expect("Version")
        wizard.sendline()
        wizard.expect("Choose a category")
        wizard.sendline("Assembly")
        wizard.expect("Choose a category")
        wizard.sendline()
        wizard.expect("1st input name")
        wizard.sendline("in1")
        wizard.expect("Label")
        wizard.sendline()
        wizard.expect("Choose a class")
        wizard.sendline("int")
        wizard.expect("optional parameter")
        wizard.sendline("n")
        wizard.expect("2nd input name")
        wizard.sendline()
        wizard.expect("1st output name")
        wizard.sendline("out1")
        wizard.expect("Label")
        wizard.sendline()
        wizard.expect("Choose a class")
        wizard.sendline("int")
        wizard.expect("2nd output name")
        wizard.sendline()
        wizard.expect("Programming language")
        wizard.sendline("Python")
        wizard.expect("Execution pattern")
        wizard.sendline("parallelized")
        wizard.expect("Will this app need access to the Internet?")
        wizard.sendline("y")
        wizard.expect("Will this app need access to the parent project?")
        wizard.sendline("y")
        wizard.expect("App directory created")
        wizard.close()

        appdir = os.path.join(tempdir, "MyTestApp")
        return appdir
    finally:
        os.chdir(old_cwd)

def create_app_dir():
    appdir = run_dx_app_wizard()
    with open(os.path.join(appdir, "src", "MyTestApp.py")) as src_fh:
        src = [line.rstrip() for line in src_fh.readlines()]
    with open(os.path.join(appdir, "src", "MyTestApp.py"), "w") as src_fh:
        for line in src:
            if line == '    return { "answer": "placeholder value" }':
                line = '    return { "answer": sum(process_outputs) }'
            elif line == '    return { "output": "placeholder value" }':
                line = '    return { "output": input1 ** 2 }'
            elif line == '    for i in range(10):':
                line = '    for i in range(in1):'
            elif line == '        subjob_input = { "input1": True }':
                line = '        subjob_input = { "input1": i }'
            elif line == '    output["out1"] = out1':
                src_fh.write('    out1 = postprocess_job.get_output_ref("answer")\n')
            src_fh.write(line + "\n")
    return appdir

class TestDXAppWizardAndRunAppLocally(DXTestCase):
    def test_dx_app_wizard(self):
        run_dx_app_wizard()

    def test_dx_run_app_locally(self):
        appdir = create_app_dir()
        output = subprocess.check_output(['dx-run-app-locally', appdir, '-iin1=8'])
        print output
        self.assertIn("App finished successfully", output)
        self.assertIn("Final output: out1 = 140", output)
        return appdir

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping test that would run jobs')
    def test_dx_run_app_locally_and_compare_results(self):
        appdir = create_app_dir()
        print "Setting current project to", self.project
        dxpy.WORKSPACE_ID = self.project
        dxpy.PROJECT_CONTEXT_ID = self.project
        applet_id = dx_build_app.build_and_upload_locally(appdir,
                                                          mode='applet',
                                                          overwrite=True,
                                                          dx_toolkit_autodep=False,
                                                          return_object_dump=True)['id']
        remote_job = dxpy.DXApplet(applet_id).run({"in1": 8})
        print "Waiting for", remote_job, "to complete"
        remote_job.wait_on_done()
        result = remote_job.describe()
        self.assertEqual(result["output"]["out1"], 140)

if __name__ == '__main__':
    unittest.main()
