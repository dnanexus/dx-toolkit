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

import os, sys, unittest, json, tempfile, subprocess, re
import pexpect
import pipes

from dxpy_testutil import DXTestCase, check_output
import dxpy_testutil as testutil

import dxpy
from dxpy.scripts import dx_build_app
from dxpy.utils.completer import InstanceTypesCompleter
import pytest


def run(command, **kwargs):
    print("$ %s" % (command,))
    output = check_output(command, shell=True, **kwargs)
    print(output)
    return output


supported_languages = ['Python', 'bash']

def run_dx_app_wizard(instance_type=None):
    old_cwd = os.getcwd()
    tempdir = tempfile.mkdtemp(prefix='Программа')
    os.chdir(tempdir)
    try:
        wizard = pexpect.spawn("dx-app-wizard --template parallelized")
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

class TestDXAppWizardAndRunAppLocally(DXTestCase):
    def test_invalid_arguments(self):
        with self.assertRaises(testutil.DXCalledProcessError):
            check_output(['dx-app-wizard', '--template=par'])

    def test_dx_app_wizard(self):
        appdir = run_dx_app_wizard()
        dxapp_json = json.load(open(os.path.join(appdir, 'dxapp.json')))
        self.assertEqual(dxapp_json['regionalOptions']['aws:us-east-1']['systemRequirements']['*']['instanceType'],
                         InstanceTypesCompleter.default_instance_type.Name)
        self.assertEqual(dxapp_json['runSpec']['distribution'], 'Ubuntu')
        self.assertEqual(dxapp_json['runSpec']['release'], '14.04')
        self.assertEqual(dxapp_json['runSpec']['timeoutPolicy']['*']['hours'], 24)

    def test_dx_app_wizard_with_azure_instance_type(self):
        appdir = run_dx_app_wizard("azure:mem1_ssd1_x2")
        dxapp_json = json.load(open(os.path.join(appdir, 'dxapp.json')))
        self.assertEqual(dxapp_json['regionalOptions']['azure:westus']['systemRequirements']['*']['instanceType'],
                         "azure:mem1_ssd1_x2")

    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_HELP_CREATE_APP_WIZARD"])
    def test_dx_run_app_locally_interactively(self):
        appdir = create_app_dir()
        local_run = pexpect.spawn("dx-run-app-locally {} -iin1=8".format(appdir))
        local_run.expect("Confirm")
        local_run.sendline()
        local_run.expect("App finished successfully")
        local_run.expect("Final output: out1 = 140")
        local_run.close()

    def test_dx_run_app_locally_noninteractively(self):
        appdir = create_app_dir()
        output = check_output(['dx-run-app-locally', appdir, '-iin1=8'])
        print(output)
        self.assertIn("App finished successfully", output)
        self.assertIn("Final output: out1 = 140", output)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping test that would run jobs')
    def test_dx_run_app_locally_and_compare_results(self):
        appdir = create_app_dir()
        print("Setting current project to", self.project)
        dxpy.WORKSPACE_ID = self.project
        dxpy.PROJECT_CONTEXT_ID = self.project
        applet_id = dx_build_app.build_and_upload_locally(appdir,
                                                          mode='applet',
                                                          overwrite=True,
                                                          dx_toolkit_autodep=False,
                                                          return_object_dump=True)['id']
        remote_job = dxpy.DXApplet(applet_id).run({"in1": 8})
        print("Waiting for", remote_job, "to complete")
        remote_job.wait_on_done()
        result = remote_job.describe()
        self.assertEqual(result["output"]["out1"], 140)

    def test_dx_build_app_locally_using_app_builder(self):
        appdir = create_app_dir()
        print("Setting current project to", self.project)
        dxpy.WORKSPACE_ID = self.project
        dxpy.PROJECT_CONTEXT_ID = self.project
        bundled_resources = dxpy.app_builder.upload_resources(appdir)
        applet_id, _ignored_applet_spec = dxpy.app_builder.upload_applet(appdir, bundled_resources, overwrite=True, dx_toolkit_autodep=False)
        app_obj = dxpy.DXApplet(applet_id)
        self.assertEqual(app_obj.describe()['id'], app_obj.get_id())


    def test_file_download(self):
        '''
        This test assumes a well-formed input spec and tests that the
        templates created automatically download the files only if
        they are available and does something sensible otherwise.
        '''
        print("Setting current project to", self.project)
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
                {
                    "name": "required_file",
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
            print(output)
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
            print(output)
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
            # Test with bare-minimum of inputs
            output = subprocess.check_output(['dx-run-app-locally', appdir] + cmdline_args)
            print(output)
            # Verify array is printed total 3 times once in each input, logs, and final output
            self.assertEquals(len(re.findall("required_array_boolean = \[ true, false ]", output)), 3)
            self.assertIn("App finished successfully", output)

            # See PTFM-13697 for CentOS 5 details
            if testutil.TEST_RUN_JOBS and not testutil.host_is_centos_5():
                # Now actually make it an applet and run it
                applet_name = dxapp_json['name'] + '-' + lang
                subprocess.check_output(['dx', 'build', appdir, '--destination', applet_name])
                subprocess.check_output(['dx', 'run', applet_name, '-y', '--wait'] + cmdline_args)

    @unittest.skipUnless(testutil.TEST_ENV, 'skipping test that would clobber your local environment')
    def test_dx_run_app_locally_without_auth(self):
        temp_file_path = tempfile.mkdtemp()
        app_spec = dict(testutil.DXTestCaseBuildApps.base_app_spec, name="test_run_locally_without_auth",
                        inputSpec = [{"name": "foo", "class": "file"}])
        app_dir_path = os.path.join(temp_file_path, app_spec['name'])
        os.mkdir(app_dir_path)
        with open(os.path.join(app_dir_path, 'dxapp.json'), 'w') as manifest:
            manifest.write(json.dumps(app_spec))
        with open(os.path.join(app_dir_path, 'code.py'), 'w') as code_file:
            code_file.write('')
        with testutil.without_auth(), testutil.without_project_context():
            with self.assertSubprocessFailure(stderr_regexp="logged in", exit_code=3):
                run("dx-run-app-locally " + pipes.quote(app_dir_path) + " -ifoo=nothing")

    def test_dx_run_app_locally_invalid_interpreter(self):
        temp_file_path = tempfile.mkdtemp()
        app_spec = dict(testutil.DXTestCaseBuildApps.base_app_spec,
                        name="test_run_locally_invalid_interpreter",
                        runSpec = {"file": "code.py", "interpreter": "python",
                                    "distribution": "Ubuntu", "release": "14.04"})
        app_dir_path = os.path.join(temp_file_path, app_spec['name'])
        os.mkdir(app_dir_path)
        with open(os.path.join(app_dir_path, 'dxapp.json'), 'w') as manifest:
            manifest.write(json.dumps(app_spec))
        with open(os.path.join(app_dir_path, 'code.py'), 'w') as code_file:
            code_file.write('')
        with self.assertSubprocessFailure(stderr_regexp="Unknown interpreter python", exit_code=3):
            run("dx-run-app-locally " + pipes.quote(app_dir_path))


'''
test the upload/download helpers by running them locally
'''
class TestDXBashHelpers(DXTestCase):
    def run_test_app_locally(self, app_name, arg_list):
        '''
        :param app_name: name of app to run
        :param arg_list: list of command line arguments given to an app

        Runs an app locally, with a given set of command line arguments
        '''
        path = os.path.join(os.path.dirname(__file__), "file_load")
        args = ['dx-run-app-locally', os.path.join(path, app_name)]
        args.extend(arg_list)
        check_output(args)

    def test_vars(self):
        """Tests bash variable generation """
        # Make a couple files for testing
        dxpy.upload_string("1234", name="A.txt", wait_on_close=True)
        self.run_test_app_locally('vars', ['-iseq1=A.txt', '-iseq2=A.txt', '-igenes=A.txt', '-igenes=A.txt',
                                           '-ii=5', '-ix=4.2', '-ib=true', '-is=hello',
                                           '-iil=6', '-iil=7', '-iil=8',
                                           '-ixl=3.3', '-ixl=4.4', '-ixl=5.0',
                                           '-ibl=true', '-ibl=false', '-ibl=true',
                                           '-isl=hello', '-isl=world', '-isl=next',
                                           '-imisc={"hello": "world", "foo": true}'])

    def test_prefix_patterns(self):
        """ Tests that the bash prefix variable works correctly, and
        respects patterns.
        """
        buf = "1234"
        filenames = ["A.bar", "A.json.dot.bar", "A.vcf.pam", "A.foo.bar", "fooxxx.bam", "A.bar.gz", "x13year23.sam"]
        for fname in filenames:
            dxpy.upload_string(buf, name=fname, wait_on_close=True)
        self.run_test_app_locally('prefix_patterns', ['-iseq1=A.bar',
                                                      '-iseq2=A.json.dot.bar',
                                                      '-igene=A.vcf.pam',
                                                      '-imap=A.foo.bar',
                                                      '-imap2=fooxxx.bam',
                                                      '-imap3=A.bar',
                                                      '-imap4=A.bar.gz',
                                                      '-imulti=x13year23.sam'])

    def test_deepdirs(self):
        self.run_test_app_locally('deepdirs', [])

    def test_basic(self):
        '''Tests upload/download helpers

        '''
        # Make a couple files for testing
        dxpy.upload_string("1234", wait_on_close=True, name="A.txt")

        # this invocation should fail with a CLI exception
        with self.assertRaises(testutil.DXCalledProcessError):
            self.run_test_app_locally('basic', ['-iseq1=A.txt', '-iseq2=B.txt'])

        dxpy.upload_string("ABCD", wait_on_close=True, name="B.txt")

        # these should succeed
        self.run_test_app_locally('basic', ['-iseq1=A.txt', '-iseq2=B.txt',
                                            '-iref=A.txt', '-iref=B.txt',
                                            "-ivalue=5", '-iages=1'])
        self.run_test_app_locally('basic', ['-iseq1=A.txt', '-iseq2=B.txt', '-ibar=A.txt',
                                            '-iref=A.txt', '-iref=B.txt',
                                            "-ivalue=5", '-iages=1'])
        self.run_test_app_locally('basic', ['-iseq1=A.txt', '-iseq2=B.txt',
                                            '-iref=A.txt', '-iref=B.txt', "-ivalue=5",
                                            '-iages=1', '-iages=11', '-iages=33'])

        # check the except flags
        self.run_test_app_locally('basic_except', ['-iseq1=A.txt', '-iseq2=B.txt',
                                                   '-iref=A.txt', '-iref=B.txt', "-ivalue=5",
                                                   '-iages=1', '-iages=11', '-iages=33'])

    def test_sub_jobs(self):
        '''  Tests a bash script that generates sub-jobs '''
        dxpy.upload_string("1234", wait_on_close=True, name="A.txt")
        dxpy.upload_string("ABCD", wait_on_close=True, name="B.txt")
        self.run_test_app_locally('with-subjobs', ["-ifiles=A.txt", "-ifiles=B.txt"])

    def test_parseq(self):
        ''' Tests the parallel/sequential variations '''
        dxpy.upload_string("1234", wait_on_close=True, name="A.txt")
        dxpy.upload_string("ABCD", wait_on_close=True, name="B.txt")
        self.run_test_app_locally('parseq', ["-iseq1=A.txt", "-iseq2=B.txt", "-iref=A.txt", "-iref=B.txt"])

    def test_file_optional(self):
        ''' Tests that file optional input arguments are handled correctly '''
        self.run_test_app_locally('file_optional', ["-icreate_seq3=true"])

if __name__ == '__main__':
    unittest.main()
