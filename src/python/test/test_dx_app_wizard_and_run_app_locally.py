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

supported_languages = ['Python', 'C++', 'bash']

def run_dx_app_wizard():
    old_cwd = os.getcwd()
    tempdir = tempfile.mkdtemp()
    os.chdir(tempdir)
    try:
        wizard = pexpect.spawn("dx-app-wizard")
        wizard.logfile = sys.stdout
        wizard.setwinsize(20, 90)
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

def create_app_dir_with_dxapp_json(dxapp_json, language):
    old_cwd = os.getcwd()
    tempdir = tempfile.mkdtemp()
    os.chdir(tempdir)
    try:
        with open('dxapp.json', 'w') as fd:
            json.dump(dxapp_json, fd)

        wizard = pexpect.spawn("dx-app-wizard --json-file dxapp.json --language " + language)
        wizard.setwinsize(20, 90)
        wizard.logfile = sys.stdout
        wizard.expect("App Name")
        wizard.sendline()
        wizard.expect("Version")
        wizard.sendline()
        wizard.expect("Execution pattern")
        wizard.sendline()
        wizard.expect("Will this app need access to the Internet?")
        wizard.sendline()
        wizard.expect("Will this app need access to the parent project?")
        wizard.sendline()
        wizard.expect("App directory created")
        wizard.close()

        appdir = os.path.join(tempdir, dxapp_json['name'])
        return appdir
    finally:
        os.chdir(old_cwd)

class TestDXAppWizardAndRunAppLocally(DXTestCase):
    def test_dx_app_wizard(self):
        appdir = run_dx_app_wizard()
        dxapp_json = json.load(open(os.path.join(appdir, 'dxapp.json')))
        self.assertEqual(dxapp_json.get('authorizedUsers'), [])

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

    def test_file_download(self):
        '''
        This test assumes a well-formed input spec and tests that the
        templates created automatically download the files only if
        they are available and does something sensible otherwise.
        '''
        print "Setting current project to", self.project
        dxpy.WORKSPACE_ID = self.project
        dxpy.PROJECT_CONTEXT_ID = self.project
        # Make a couple files for testing
        dxfile = dxpy.upload_string("foo", name="afile")
        dxpy.upload_string("foobar", name="otherfile")

        dxapp_json = {
            "name": "files",
            "title": "files",
            "summary": "files",
            "dxapi": "1.0.0",
            "version": "0.0.1",
            "categories": [],
            "inputSpec": [
                {"name": "required_file",
                 "class": "file",
                 "optional": False
                },
                {
                    "name": "optional_file",
                    "class": "file",
                    "optional": True
                },
                {
                    "name": "default_file",
                    "class": "file",
                    "optional": True,
                    "default": {"$dnanexus_link": dxfile.get_id()}
                },
                {
                    "name": "required_file_array",
                    "class": "array:file",
                    "optional": False
                },
                {
                    "name": "optional_file_array",
                    "class": "array:file",
                    "optional": True
                }
            ],
            "outputSpec": []
        }

        for lang in supported_languages:
            appdir = create_app_dir_with_dxapp_json(dxapp_json, lang)
            # Test with bare-minimum of inputs
            output = subprocess.check_output(['dx-run-app-locally', appdir, '-irequired_file=afile',
                                              '-irequired_file_array=afile'])
            print output
            self.assertIn("App finished successfully", output)
            self.assertIn("Local job workspaces can be found in:", output)
            local_workdir = output.split("Local job workspaces can be found in:")[1].strip()
            file_list = os.listdir(os.path.join(local_workdir, 'localjob-0'))
            self.assertIn("required_file", file_list)
            self.assertEqual(os.path.getsize(os.path.join(local_workdir, 'localjob-0', 'required_file')), 3)
            self.assertNotIn("optional_file", file_list)
            self.assertIn("default_file", file_list)
            self.assertEqual(os.path.getsize(os.path.join(local_workdir, 'localjob-0', 'default_file')), 3)

            # Test with giving an input to everything
            output = subprocess.check_output(['dx-run-app-locally', appdir,
                                              '-irequired_file=afile',
                                              '-ioptional_file=afile',
                                              '-idefault_file=otherfile',
                                              '-irequired_file_array=afile',
                                              '-ioptional_file_array=afile'])
            print output
            self.assertIn("App finished successfully", output)
            self.assertIn("Local job workspaces can be found in:", output)
            local_workdir = output.split("Local job workspaces can be found in:")[1].strip()
            file_list = os.listdir(os.path.join(local_workdir, 'localjob-0'))
            self.assertIn("required_file", file_list)
            self.assertEqual(os.path.getsize(os.path.join(local_workdir, 'localjob-0', 'required_file')), 3)
            self.assertIn("optional_file", file_list)
            self.assertEqual(os.path.getsize(os.path.join(local_workdir, 'localjob-0', 'optional_file')), 3)
            self.assertIn("default_file", file_list)
            self.assertEqual(os.path.getsize(os.path.join(local_workdir, 'localjob-0', 'default_file')), 6)
            concatenated_file_list = ",".join(file_list)
            # Different languages have different naming conventions
            # right now, so just look for the array variable name
            self.assertIn("required_file_array", concatenated_file_list)
            self.assertIn("optional_file_array", concatenated_file_list)

    def test_var_initialization(self):
        '''
        This test assumes a well-formed input spec and mostly just
        tests that everything compiles and the variable initialization
        code does not throw any errors.
        '''

        print "Setting current project to", self.project
        dxpy.WORKSPACE_ID = self.project
        dxpy.PROJECT_CONTEXT_ID = self.project

        # Make some data objects for input
        dxapplet = dxpy.api.applet_new({"project": dxpy.WORKSPACE_ID,
                                        "name": "anapplet",
                                        "dxapi": "1.0.0",
                                        "runSpec": {"code": "", "interpreter": "bash"}})['id']
        dxfile = dxpy.upload_string("foo", name="afile")
        dxgtable = dxpy.new_dxgtable(columns=[{"name": "int_col", "type": "int"}], name="agtable")
        dxgtable.add_rows([[3], [0]])
        dxgtable.close(block=True)
        dxrecord = dxpy.new_dxrecord(name="arecord")
        dxrecord.close()

        dxapp_json = {
            "name": "all_vars",
            "title": "all_vars",
            "summary": "all_vars",
            "dxapi": "1.0.0",
            "version": "0.0.1",
            "categories": [],
            "inputSpec": [],
            "outputSpec": []
        }

        classes = ['applet', 'record', 'file', 'gtable',
                   'boolean', 'int', 'float', 'string', 'hash',
                   'array:applet', 'array:record', 'array:file', 'array:gtable',
                   'array:boolean', 'array:int', 'array:float', 'array:string']

        for classname in classes:
            dxapp_json['inputSpec'].append({"name": "required_" + classname.replace(":", "_"),
                                            "class": classname,
                                            "optional": False})
            # Note: marking outputs as optional so that empty arrays
            # will be acceptable; keeping names the same (as required)
            # in order to allow pass-through from input variables
            dxapp_json['outputSpec'].append({"name": "required_" + classname.replace(":", "_"),
                                             "class": classname,
                                             "optional": True})
            dxapp_json['inputSpec'].append({"name": "optional_" + classname.replace(":", "_"),
                                            "class": classname,
                                            "optional": True})

        cmdline_args = ['-irequired_applet=anapplet',
                        '-irequired_array_applet=anapplet',
                        '-irequired_record=arecord',
                        '-irequired_array_record=arecord',
                        '-irequired_file=afile',
                        '-irequired_array_file=afile',
                        '-irequired_gtable=agtable',
                        '-irequired_array_gtable=agtable',
                        '-irequired_boolean=true',
                        '-irequired_array_boolean=true',
                        '-irequired_int=32',
                        '-irequired_array_int=42',
                        '-irequired_float=3.4',
                        '-irequired_array_float=.42',
                        '-irequired_string=foo',
                        '-irequired_array_string=bar',
                        '-irequired_hash={"foo":"bar"}']
        for lang in supported_languages:
            appdir = create_app_dir_with_dxapp_json(dxapp_json, lang)
            # Test with bare-minimum of inputs
            output = subprocess.check_output(['dx-run-app-locally', appdir] + cmdline_args)
            print output
            self.assertIn("App finished successfully", output)

            if testutil.TEST_RUN_JOBS:
                # Now actually make it an applet and run it
                subprocess.check_output(['dx-build-applet', appdir, '--destination', dxapp_json['name'] + '-' + lang])
                subprocess.check_output(['dx', 'run', dxapp_json['name'] + '-' + lang, '-y', '--wait'] + cmdline_args)

if __name__ == '__main__':
    unittest.main()
