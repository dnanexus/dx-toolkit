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

import dxpy
from dxpy.scripts import dx_build_app

class TestDXAppWizardAndRunAppLocally(unittest.TestCase):
    def test_dx_app_wizard(self):
        tempdir = tempfile.mkdtemp()
        os.chdir(tempdir)
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
        wizard.expect("1st input name")
        wizard.sendline("i")
        wizard.expect("Label")
        wizard.sendline()
        wizard.expect("Choose a class")
        wizard.sendline("int")
        wizard.expect("optional parameter")
        wizard.sendline("n")
        wizard.expect("2nd input name")
        wizard.sendline()
        wizard.expect("1st output name")
        wizard.sendline("o")
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
        wizard.expect("App directory created")
        wizard.close()

        appdir = os.path.join(tempdir, "MyTestApp")
        return appdir

    def test_dx_run_app_locally(self):
        appdir = self.test_dx_app_wizard()
        with open(os.path.join(appdir, "src", "MyTestApp.py")) as src_fh:
            src = [line.rstrip() for line in src_fh.readlines()]
        with open(os.path.join(appdir, "src", "MyTestApp.py"), "w") as src_fh:
            for line in src:
                if line == '    return { "answer": "placeholder value" }':
                    line = '    return { "answer": sum(process_outputs) }'
                elif line == '    return { "output": "placeholder value" }':
                    line = '    return { "output": input1 ** 2 }'
                elif line == '        subjob_input = { "input1": True }':
                    line = '        subjob_input = { "input1": i }'
                elif line == '    output["o"] = o':
                    src_fh.write('    o = postprocess_job.get_output_ref("answer")\n')
                src_fh.write(line + "\n")

        output = subprocess.check_output(['dx-run-app-locally', appdir, '-ii=10'])
        print output
        self.assertIn("App finished successfully", output)
        self.assertIn("Final output: o = 285", output)
        return appdir

    def test_dx_run_app_locally_and_compare_results(self):
        appdir = self.test_dx_run_app_locally()
        applet_id = dx_build_app.main(args=[appdir, '--create-applet', '-f', '--json'])['id']
        remote_job = dxpy.DXApplet(applet_id).run({"i": 10})
        print "Waiting for", remote_job, "to complete"
        remote_job.wait_on_done()
        result = remote_job.describe()
        self.assertEqual(result["output"]["o"], 285)

if __name__ == '__main__':
    unittest.main()
