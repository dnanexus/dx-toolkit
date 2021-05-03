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

import os, sys, unittest, json, tempfile, subprocess
import pexpect
import pipes

from dxpy_testutil import DXTestCase, check_output
import dxpy_testutil as testutil

import dxpy
from dxpy.compat import USING_PYTHON2
from dxpy.utils.completer import InstanceTypesCompleter
import pytest

supported_languages = ['Python', 'bash']

if USING_PYTHON2:
    spawn_extra_args = {}
else:
    # Python 3 requires specifying the encoding
    spawn_extra_args = {"encoding" : "utf-8" }

def run_dx_app_wizard(instance_type=None):
    old_cwd = os.getcwd()
    tempdir = tempfile.mkdtemp(prefix='Программа')
    os.chdir(tempdir)
    try:
        wizard = pexpect.spawn("dx-app-wizard --template parallelized",
                               **spawn_extra_args)
        wizard.logfile = sys.stdout
        wizard.setwinsize(20, 90)
        wizard.expect("App Name:")
        wizard.sendline("Имя")
        wizard.expect("The name of your app must match")
        wizard.expect("App Name:")
        wizard.sendline("MyTestApp")
        wizard.expect("Title")
        wizard.sendline("Заголовок")
        wizard.expect("Summary")
        wizard.sendline("Конспект")
        wizard.expect("Version")
        wizard.sendline("1.2.3")
        wizard.expect("1st input name")
        wizard.sendline("in1")
        wizard.expect("Label")
        wizard.sendline("Метка")
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
        wizard.sendline("целое")
        wizard.expect("Not a recognized class")
        wizard.sendline("int")
        wizard.expect("2nd output name")
        wizard.sendline()
        wizard.expect("Timeout policy")
        wizard.sendline("31d")
        wizard.expect("Error: max allowed timeout is 30 days")
        wizard.sendline("ЄЯTЪЦGЇCЄкЇ")
        wizard.expect("Error: enter an int with a single-letter suffix")
        wizard.expect("Timeout policy")
        wizard.sendline("24h")
        wizard.expect("Programming language")
        wizard.sendline("АЛГОЛ")
        wizard.expect("Error: unrecognized response")
        wizard.sendline("Python")
        wizard.expect("Will this app need access to the Internet?")
        wizard.sendline("y")
        wizard.expect("Will this app need access to the parent project?")
        wizard.sendline("y")
        wizard.expect("Choose an instance type for your app")
        wizard.sendline("t1.микро")
        wizard.expect("Error: unrecognized response, expected one of")
        wizard.expect("Choose an instance type for your app")
        if instance_type is not None:
            wizard.sendline(instance_type)
        else:
            wizard.sendline()
        wizard.expect("App directory created")
        wizard.close()

        appdir = os.path.join(tempdir, "MyTestApp")
        return appdir
    finally:
        os.chdir(old_cwd)

def create_app_dir_with_dxapp_json(dxapp_json, language):
    old_cwd = os.getcwd()
    tempdir = tempfile.mkdtemp()
    os.chdir(tempdir)
    try:
        with open('dxapp.json', 'w') as fd:
            json.dump(dxapp_json, fd)

        wizard = pexpect.spawn("dx-app-wizard --json-file dxapp.json --language " + language,
                               **spawn_extra_args)
        wizard.setwinsize(20, 90)
        wizard.logfile = sys.stdout
        wizard.expect("App Name")
        wizard.sendline()
        wizard.expect("Version")
        wizard.sendline()
        wizard.expect("Timeout policy")
        wizard.sendline()
        wizard.expect("Will this app need access to the Internet?")
        wizard.sendline()
        wizard.expect("Will this app need access to the parent project?")
        wizard.sendline()
        wizard.expect("Choose an instance type for your app")
        wizard.sendline()
        wizard.expect("App directory created")
        wizard.close()

        appdir = os.path.join(tempdir, dxapp_json['name'])
        return appdir
    finally:
        os.chdir(old_cwd)

class TestDXAppWizard(DXTestCase):
    def test_invalid_arguments(self):
        with self.assertRaises(testutil.DXCalledProcessError):
            check_output(['dx-app-wizard', '--template=par'])

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_HELP_CREATE_APP_WIZARD"])
    def test_dx_app_wizard(self):
        appdir = run_dx_app_wizard()
        with open(os.path.join(appdir, 'dxapp.json')) as fh:
            dxapp_json = json.load(fh)
        self.assertEqual(dxapp_json['regionalOptions']['aws:us-east-1']['systemRequirements']['*']['instanceType'],
                         InstanceTypesCompleter.default_instance_type.Name)
        self.assertEqual(dxapp_json['runSpec']['distribution'], 'Ubuntu')
        self.assertEqual(dxapp_json['runSpec']['release'], '20.04')
        self.assertEqual(dxapp_json['runSpec']['version'], '0')
        self.assertEqual(dxapp_json['runSpec']['interpreter'], 'python3')
        self.assertEqual(dxapp_json['runSpec']['timeoutPolicy']['*']['hours'], 24)

    def test_dx_app_wizard_with_azure_instance_type(self):
        appdir = run_dx_app_wizard("azure:mem1_ssd1_x2")
        with open(os.path.join(appdir, 'dxapp.json')) as fh:
            dxapp_json = json.load(fh)
        self.assertEqual(dxapp_json['regionalOptions']['azure:westus']['systemRequirements']['*']['instanceType'],
                         "azure:mem1_ssd1_x2")

    def test_var_initialization(self):
        '''
        This test assumes a well-formed input spec and mostly just
        tests that everything compiles and the variable initialization
        code does not throw any errors.
        '''

        print("Setting current project to", self.project)
        dxpy.WORKSPACE_ID = self.project
        dxpy.PROJECT_CONTEXT_ID = self.project

        # Make some data objects for input
        dxpy.api.applet_new({"project": dxpy.WORKSPACE_ID,
                             "name": "anapplet",
                             "dxapi": "1.0.0",
                             "runSpec": {"code": "", "interpreter": "bash",
                                         "distribution": "Ubuntu", "release": "14.04"}})['id']
        dxpy.upload_string("foo", name="afile")
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

        classes = ['applet', 'record', 'file',
                   'boolean', 'int', 'float', 'string', 'hash',
                   'array:applet', 'array:record', 'array:file',
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
                        '-irequired_boolean=true',
                        '-irequired_array_boolean=true',
                        '-irequired_array_boolean=false',
                        '-irequired_int=32',
                        '-irequired_array_int=42',
                        '-irequired_float=3.4',
                        '-irequired_array_float=.42',
                        '-irequired_string=foo',
                        '-irequired_array_string=bar',
                        '-irequired_hash={"foo":"bar"}']
        for lang in supported_languages:
            appdir = create_app_dir_with_dxapp_json(dxapp_json, lang)

            # See PTFM-13697 for CentOS 5 details
            if testutil.TEST_RUN_JOBS and not testutil.host_is_centos_5():
                # Now actually make it an applet and run it
                applet_name = dxapp_json['name'] + '-' + lang
                subprocess.check_output(['dx', 'build', appdir, '--destination', applet_name])
                subprocess.check_output(['dx', 'run', applet_name, '-y', '--wait'] + cmdline_args)


if __name__ == '__main__':
    unittest.main()
