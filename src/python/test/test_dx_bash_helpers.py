#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2016 DNAnexus, Inc.
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

import dxpy
import dxpy_testutil as testutil
import json
import os
import pipes
import pytest
import shutil
import tempfile
import unittest
from dxpy.utils.completer import InstanceTypesCompleter
from dxpy_testutil import DXTestCase, check_output, temporary_project, override_environment
from dxpy.exceptions import DXJobFailureError
from dxpy.bindings.download_all_inputs import _get_num_parallel_threads

def run(command, **kwargs):
    try:
        if isinstance(command, list) or isinstance(command, tuple):
            print("$ %s" % ' '.join(pipes.quote(f) for f in command))
            output = check_output(command, **kwargs)
        else:
            print("$ %s" % (command,))
            output = check_output(command, shell=True, **kwargs)
    except testutil.DXCalledProcessError as e:
        print('== stdout ==')
        print(e.output)
        print('== stderr ==')
        print(e.stderr)
        raise
    print(output)
    return output

TEST_APPS = os.path.join(os.path.dirname(__file__), 'file_load')
LOCAL_SCRIPTS = os.path.join(os.path.dirname(__file__), '..', 'scripts')
LOCAL_UTILS = os.path.join(os.path.dirname(__file__), '..', 'dxpy', 'utils')


def ignore_folders(directory, contents):
    accepted_bin = ['dx-unpack', 'dx-unpack-file', 'dxfs', 'register-python-argcomplete',
                    'python-argcomplete-check-easy-install-script']
    # Omit Python test dir since it's pretty large
    if "src/python/test" in directory:
        return contents
    if "../bin" in directory:
        return [f for f in contents if f not in accepted_bin]
    return []

def build_app_with_bash_helpers(app_dir, project_id):
    tempdir = tempfile.mkdtemp()
    try:
        updated_app_dir = os.path.join(tempdir, os.path.basename(app_dir))
        #updated_app_dir = os.path.abspath(os.path.join(tempdir, os.path.basename(app_dir)))
        shutil.copytree(app_dir, updated_app_dir)
        # Copy the current verion of dx-toolkit. We will build it on the worker
        # and source this version which will overload the stock version of dx-toolkit.
        # This we we can test all bash helpers as they would appear locally with all
        # necessary dependencies
        #dxtoolkit_dir = os.path.abspath(os.path.join(updated_app_dir, 'resources', 'dxtoolkit'))
        #local_dxtoolkit = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
        dxtoolkit_dir = os.path.join(updated_app_dir, 'resources', 'dxtoolkit')
        local_dxtoolkit = os.path.join(os.path.dirname(__file__), '..', '..', '..')
        shutil.copytree(local_dxtoolkit, dxtoolkit_dir)

        # Add lines to the beginning of the job to make and use our new dx-toolkit
        preamble = []
        #preamble.append("cd {appdir}/resources && git clone https://github.com/dnanexus/dx-toolkit.git".format(appdir=updated_app_dir))
        preamble.append('sudo pip install --upgrade virtualenv\n')
        #preamble.append('make -C {toolkitdir} python\n'.format(toolkitdir=dxtoolkit_dir))
        #preamble.append('source {toolkitdir}/environment\n'.format(toolkitdir=dxtoolkit_dir))
        preamble.append('make -C /dxtoolkit python\n')
        preamble.append('source /dxtoolkit/environment\n')
        # Now find the applet entry point file and prepend the
        # operations above, overwriting it in place.
        dxapp_json = json.load(open(os.path.join(app_dir, 'dxapp.json')))
        if dxapp_json['runSpec']['interpreter'] != 'bash':
            raise Exception('Sorry, I only know how to patch bash apps for remote testing')
        entry_point_filename = os.path.join(app_dir, dxapp_json['runSpec']['file'])
        entry_point_data = ''.join(preamble) + open(entry_point_filename).read()
        with open(os.path.join(updated_app_dir, dxapp_json['runSpec']['file']), 'w') as fh:
            fh.write(entry_point_data)

        build_output = run(['dx', 'build', '--json', '--destination', project_id + ':', updated_app_dir])
        return json.loads(build_output)['id']
    finally:
        shutil.rmtree(tempdir)

def update_environ(**kwargs):
    """
    Returns a copy of os.environ with the specified updates (VAR=value for each kwarg)
    """
    output = os.environ.copy()
    for k, v in kwargs.iteritems():
        if v is None:
            del output[k]
        else:
            output[k] = v
    return output


@unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping tests that would run jobs')
class TestDXBashHelpers(DXTestCase):
    @pytest.mark.TRACEABILITY_MATRIX
    @testutil.update_traceability_matrix(["DNA_CLI_HELP_PROVIDE_BASH_HELPER_COMMANDS"])
    def test_vars(self):
        '''  Quick test for the bash variables '''
        with temporary_project('TestDXBashHelpers.test_app1 temporary project') as p:
            env = update_environ(DX_PROJECT_CONTEXT_ID=p.get_id())

            # Upload some files for use by the applet
            dxpy.upload_string("1234\n", project=p.get_id(), name="A.txt")

            # Build the applet, patching in the bash helpers from the
            # local checkout
            applet_id = build_app_with_bash_helpers(os.path.join(TEST_APPS, 'vars'), p.get_id())

            # Run the applet
            applet_args = ['-iseq1=A.txt', '-iseq2=A.txt', '-igenes=A.txt', '-igenes=A.txt',
                           '-ii=5', '-ix=4.2', '-ib=true', '-is=hello',
                           '-iil=6', '-iil=7', '-iil=8',
                           '-ixl=3.3', '-ixl=4.4', '-ixl=5.0',
                           '-ibl=true', '-ibl=false', '-ibl=true',
                           '-isl=hello', '-isl=world', '-isl=next',
                           '-imisc={"hello": "world", "foo": true}']
            cmd_args = ['dx', 'run', '--yes', '--watch', applet_id]
            cmd_args.extend(applet_args)
            run(cmd_args, env=env)

    def test_basic(self):
        with temporary_project('TestDXBashHelpers.test_app1 temporary project') as dxproj:
            env = update_environ(DX_PROJECT_CONTEXT_ID=dxproj.get_id())

            # Upload some files for use by the applet
            dxpy.upload_string("1234\n", project=dxproj.get_id(), name="A.txt")
            dxpy.upload_string("ABCD\n", project=dxproj.get_id(), name="B.txt")

            # Build the applet, patching in the bash helpers from the
            # local checkout
            applet_id = build_app_with_bash_helpers(os.path.join(TEST_APPS, 'basic'), dxproj.get_id())

            # Run the applet
            applet_args = ['-iseq1=A.txt', '-iseq2=B.txt', '-iref=A.txt', '-iref=B.txt', "-ivalue=5", "-iages=4"]
            cmd_args = ['dx', 'run', '--yes', '--watch', applet_id]
            cmd_args.extend(applet_args)
            run(cmd_args, env=env)

    def test_sub_jobs(self):
        '''  Tests a bash script that generates sub-jobs '''
        with temporary_project('TestDXBashHelpers.test_app1 temporary project') as dxproj:
            env = update_environ(DX_PROJECT_CONTEXT_ID=dxproj.get_id())

             # Upload some files for use by the applet
            dxpy.upload_string("1234\n", project=dxproj.get_id(), name="A.txt")
            dxpy.upload_string("ABCD\n", project=dxproj.get_id(), name="B.txt")

            # Build the applet, patching in the bash helpers from the
            # local checkout
            applet_id = build_app_with_bash_helpers(os.path.join(TEST_APPS, 'with-subjobs'), dxproj.get_id())
             # Run the applet.
            # Since the job creates two sub-jobs, we need to be a bit more sophisticated
            # in order to wait for completion.
            applet_args = ["-ifiles=A.txt", "-ifiles=B.txt"]
            cmd_args = ['dx', 'run', '--yes', '--brief', applet_id]
            cmd_args.extend(applet_args)
            job_id = run(cmd_args, env=env).strip()

            dxpy.DXJob(job_id).wait_on_done()

            # Assertions -- making sure the script worked
            # Assertions to make about the job's output after it is done running:
            # - *first_file* is a file named first_file.txt containing the string:
            #     "contents of first_file"
            # - *final_file* is a file named final_file.txt containing the
            #   *concatenation of the two input files in *files*
            print("Test completed successfully, checking file content\n")

            job_handler = dxpy.get_handler(job_id)
            job_output = job_handler.output

            def strip_white_space(_str):
                return ''.join(_str.split())

            def silent_file_remove(filename):
                try:
                    os.remove(filename)
                except OSError:
                    pass

            # The output should include two files, this section verifies that they have
            # the correct data.
            def check_file_content(out_param_name, out_filename, tmp_fname, str_content):
                """
                Download a file, read it from local disk, and verify that it has the correct contents
                """
                if not out_param_name in job_output:
                    raise "Error: key {} does not appear in the job output".format(out_param_name)
                dxlink = job_output[out_param_name]

                # check that the filename gets preserved
                trg_fname = dxpy.get_handler(dxlink).name
                self.assertEqual(trg_fname, out_filename)

                # download the file and check the contents
                silent_file_remove(tmp_fname)
                dxpy.download_dxfile(dxlink, tmp_fname)
                with open(tmp_fname, "r") as fh:
                    data = fh.read()
                    print(data)
                    if not (strip_white_space(data) == strip_white_space(str_content)):
                        raise Exception("contents of file {} do not match".format(out_param_name))
                silent_file_remove(tmp_fname)

            check_file_content('first_file', 'first_file.txt', "f1.txt", "contents of first_file")
            check_file_content('final_file', 'final_file.txt', "f2.txt", "1234ABCD")

    def test_parseq(self):
        ''' Tests the parallel/sequential variations '''
        with temporary_project('TestDXBashHelpers.test_app1 temporary project') as dxproj:
            env = update_environ(DX_PROJECT_CONTEXT_ID=dxproj.get_id())

            # Upload some files for use by the applet
            dxpy.upload_string("1234\n", project=dxproj.get_id(), name="A.txt")
            dxpy.upload_string("ABCD\n", project=dxproj.get_id(), name="B.txt")

            # Build the applet, patching in the bash helpers from the
            # local checkout
            applet_id = build_app_with_bash_helpers(os.path.join(TEST_APPS, 'parseq'), dxproj.get_id())

            # Run the applet
            applet_args = ["-iseq1=A.txt", "-iseq2=B.txt", "-iref=A.txt", "-iref=B.txt"]
            cmd_args = ['dx', 'run', '--yes', '--watch', applet_id]
            cmd_args.extend(applet_args)
            run(cmd_args, env=env)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run a job')
    def test_file_optional(self):
        ''' Tests that optional and non-optional file output arguments are
        handled correctly '''
        with temporary_project('TestDXBashHelpers.test_app1 temporary project') as dxproj:
            env = update_environ(DX_PROJECT_CONTEXT_ID=dxproj.get_id())

            # Build the applet, patching in the bash helpers from the
            # local checkout
            applet_id = build_app_with_bash_helpers(
                os.path.join(TEST_APPS, 'file_optional'),
                dxproj.get_id())

            # Run the applet. This checks a correct scenario where
            # the applet generates:
            # 1) an empty directory for an optional file output
            # 2) a file for a non-optional file output.
            applet_args = ["-icreate_seq3=true"]
            cmd_args = ['dx', 'run', '--yes', '--brief', applet_id]
            cmd_args.extend(applet_args)
            job_id = run(cmd_args, env=env).strip()
            dxpy.DXJob(job_id).wait_on_done()

            # Run the applet --- this will not create the seq3 output file.
            # This should cause an exception from the job manager.
            applet_args = ["-icreate_seq3=false"]
            cmd_args = ['dx', 'run', '--yes', '--brief', applet_id]
            cmd_args.extend(applet_args)
            job_id = run(cmd_args, env=env).strip()
            job = dxpy.DXJob(job_id)
            with self.assertRaises(DXJobFailureError):
                job.wait_on_done()
            desc = job.describe()
            self.assertEqual(desc["failureReason"], "OutputError")

    def test_prefix_patterns(self):
        """ Tests that the bash prefix variable works correctly, and
        respects patterns.
        """
        with temporary_project('TestDXBashHelpers.test_app1 temporary project') as dxproj:
            env = update_environ(DX_PROJECT_CONTEXT_ID=dxproj.get_id())
            filenames = ["A.bar", "A.json.dot.bar", "A.vcf.pam", "A.foo.bar",
                         "fooxxx.bam", "A.bar.gz", "x13year23.sam"]
            for fname in filenames:
                dxpy.upload_string("1234", project=dxproj.get_id(), name=fname)

            # Build the applet, patching in the bash helpers from the
            # local checkout
            applet_id = build_app_with_bash_helpers(os.path.join(TEST_APPS, 'prefix_patterns'), dxproj.get_id())

            # Run the applet
            applet_args = ['-iseq1=A.bar',
                           '-iseq2=A.json.dot.bar',
                           '-igene=A.vcf.pam',
                           '-imap=A.foo.bar',
                           '-imap2=fooxxx.bam',
                           '-imap3=A.bar',
                           '-imap4=A.bar.gz',
                           '-imulti=x13year23.sam']
            cmd_args = ['dx', 'run', '--yes', '--watch', applet_id]
            cmd_args.extend(applet_args)
            run(cmd_args, env=env)

    def test_deepdirs(self):
        ''' Tests the use of subdirectories in the output directory '''
        def check_output_key(job_output, out_param_name, num_files, dxproj):
            ''' check that an output key appears, and has the correct number of files '''
            print('checking output for param={}'.format(out_param_name))
            if out_param_name not in job_output:
                raise "Error: key {} does not appear in the job output".format(out_param_name)
            dxlink_id_list = job_output[out_param_name]
            if not len(dxlink_id_list) == num_files:
                raise Exception("Error: key {} should have {} files, but has {}".
                                format(out_param_name, num_files, len(dxlink_id_list)))

        def verify_files_in_dir(path, expected_filenames, dxproj):
            ''' verify that a particular set of files resides in a directory '''
            dir_listing = dxproj.list_folder(folder=path, only="objects")
            for elem in dir_listing["objects"]:
                handler = dxpy.get_handler(elem["id"])
                if not isinstance(handler, dxpy.DXFile):
                    continue
                if handler.name not in expected_filenames:
                    raise Exception("Error: file {} should reside in directory {}".
                                    format(handler.name, path))

        with temporary_project('TestDXBashHelpers.test_app1 temporary project') as dxproj:
            env = update_environ(DX_PROJECT_CONTEXT_ID=dxproj.get_id())

            # Build the applet, patching in the bash helpers from the
            # local checkout
            applet_id = build_app_with_bash_helpers(os.path.join(TEST_APPS, 'deepdirs'), dxproj.get_id())

            # Run the applet
            cmd_args = ['dx', 'run', '--yes', '--brief', applet_id]
            job_id = run(cmd_args, env=env).strip()

            dxpy.DXJob(job_id).wait_on_done()

            print("Test completed successfully, checking outputs\n")

            # Assertions about the output. There should be three result keys
            job_handler = dxpy.get_handler(job_id)
            job_output = job_handler.output

            check_output_key(job_output, "genes", 8, dxproj)
            check_output_key(job_output, "phenotypes", 7, dxproj)
            check_output_key(job_output, "report", 1, dxproj)
            check_output_key(job_output, "helix", 1, dxproj)

            verify_files_in_dir("/clue", ["X_1.txt", "X_2.txt", "X_3.txt"], dxproj)
            verify_files_in_dir("/hint", ["V_1.txt", "V_2.txt", "V_3.txt"], dxproj)
            verify_files_in_dir("/clue2", ["Y_1.txt", "Y_2.txt", "Y_3.txt"], dxproj)
            verify_files_in_dir("/hint2", ["Z_1.txt", "Z_2.txt", "Z_3.txt"], dxproj)
            verify_files_in_dir("/foo/bar", ["luke.txt"], dxproj)
            verify_files_in_dir("/", ["A.txt", "B.txt", "C.txt", "num_chrom.txt"], dxproj)


@unittest.skipUnless(testutil.TEST_RUN_JOBS and testutil.TEST_BENCHMARKS,
                     'skipping tests that would run jobs, or, run benchmarks')
class TestDXBashHelpersBenchmark(DXTestCase):

    def create_file_of_size(self, fname, size_bytes):
        assert(size_bytes > 1);
        try:
            os.remove(fname)
        except:
            pass
        with open(fname, "wb") as out:
            out.seek(size_bytes - 1)
            out.write('\0')

    def run_applet_with_flags(self, flag_list, num_files, file_size_bytes):
        with temporary_project('TestDXBashHelpers.test_app1 temporary project') as dxproj:
            env = update_environ(DX_PROJECT_CONTEXT_ID=dxproj.get_id())

            # Upload file
            self.create_file_of_size("A.txt", file_size_bytes);
            remote_file = dxpy.upload_local_file(filename="A.txt", project=dxproj.get_id(), folder='/')

            # Build the applet, patching in the bash helpers from the
            # local checkout
            applet_id = build_app_with_bash_helpers(os.path.join(TEST_APPS, 'benchmark'), dxproj.get_id())

            # Add several files to the output
            applet_args = []
            applet_args.extend(['-iref=A.txt'] * num_files)
            cmd_args = ['dx', 'run', '--yes', '--watch', '--instance-type=mem1_ssd1_x2', applet_id]
            cmd_args.extend(applet_args)
            cmd_args.extend(flag_list)
            run(cmd_args, env=env)

    def test_seq(self):
        self.run_applet_with_flags(["-iparallel=false"], 40, 1024 * 1024)

    def test_par(self):
        self.run_applet_with_flags(["-iparallel=true"], 40, 1024 * 1024)

    def test_seq_100m(self):
        self.run_applet_with_flags(["-iparallel=false"], 40, 100 * 1024 * 1024)

    def test_par_100m(self):
        self.run_applet_with_flags(["-iparallel=true"], 40, 100 * 1024 * 1024)

    def test_par_1g(self):
        self.run_applet_with_flags(["-iparallel=true"], 10, 1024 * 1024 * 1024)


class TestDXJobutilAddOutput(DXTestCase):
    dummy_hash = "123456789012345678901234"
    data_obj_classes = ['file', 'record', 'applet', 'workflow']
    dummy_ids = [obj_class + '-' + dummy_hash for obj_class in data_obj_classes]
    dummy_job_id = "job-" + dummy_hash
    dummy_analysis_id = "analysis-123456789012345678901234"
    test_cases = ([["32", 32],
                   ["3.4", 3.4],
                   ["true", True],
                   ["'32 tables'", "32 tables"],
                   ['\'{"foo": "bar"}\'', {"foo": "bar"}],
                   [dummy_job_id + ":foo", {"job": dummy_job_id,
                                            "field": "foo"}],
                   [dummy_analysis_id + ":bar",
                    {"$dnanexus_link": {"analysis": dummy_analysis_id,
                                        "field": "bar"}}]] +
                  [[dummy_id, {"$dnanexus_link": dummy_id}] for dummy_id in dummy_ids] +
                  [["'" + json.dumps({"$dnanexus_link": dummy_id}) + "'",
                    {"$dnanexus_link": dummy_id}] for dummy_id in dummy_ids])

    def test_auto(self):
        with tempfile.NamedTemporaryFile() as f:
            # initialize the file with valid JSON
            f.write('{}')
            f.flush()
            local_filename = f.name
            cmd_prefix = "dx-jobutil-add-output -o " + local_filename + " "
            for i, tc in enumerate(self.test_cases):
                run(cmd_prefix + str(i) + " " + tc[0])
            f.seek(0)
            result = json.load(f)
            for i, tc in enumerate(self.test_cases):
                self.assertEqual(result[str(i)], tc[1])

    def test_auto_array(self):
        with tempfile.NamedTemporaryFile() as f:
            # initialize the file with valid JSON
            f.write('{}')
            f.flush()
            local_filename = f.name
            cmd_prefix = "dx-jobutil-add-output --array -o " + local_filename + " "
            for i, tc in enumerate(self.test_cases):
                run(cmd_prefix + str(i) + " " + tc[0])
                run(cmd_prefix + str(i) + " " + tc[0])
            f.seek(0)
            result = json.load(f)
            for i, tc in enumerate(self.test_cases):
                self.assertEqual(result[str(i)], [tc[1], tc[1]])

    def test_class_specific(self):
        with tempfile.NamedTemporaryFile() as f:
            # initialize the file with valid JSON
            f.write('{}')
            f.flush()
            local_filename = f.name
            cmd_prefix = "dx-jobutil-add-output -o " + local_filename + " "
            class_test_cases = [["boolean", "t", True],
                                ["boolean", "1", True],
                                ["boolean", "0", False]]
            for i, tc in enumerate(class_test_cases):
                run(cmd_prefix + " ".join([str(i), "--class " + tc[0], tc[1]]))
            f.seek(0)
            result = json.load(f)
            for i, tc in enumerate(class_test_cases):
                self.assertEqual(result[str(i)], tc[2])

    def test_class_parsing_errors(self):
        with tempfile.NamedTemporaryFile() as f:
            # initialize the file with valid JSON
            f.write('{}')
            f.flush()
            local_filename = f.name
            cmd_prefix = "dx-jobutil-add-output -o " + local_filename + " "
            error_test_cases = ([["int", "3.4"],
                                 ["int", "foo"],
                                 ["float", "foo"],
                                 ["boolean", "something"],
                                 ["hash", "{]"],
                                 ["jobref", "thing"],
                                 ["analysisref", "thing"]] +
                                [[classname,
                                  "'" +
                                  json.dumps({"dnanexus_link": classname + "-" + self.dummy_hash}) +
                                  "'"] for classname in self.data_obj_classes])
            for i, tc in enumerate(error_test_cases):
                with self.assertSubprocessFailure(stderr_regexp='Value could not be parsed',
                                                  exit_code=3):
                    run(cmd_prefix + " ".join([str(i), "--class " + tc[0], tc[1]]))


class TestDXJobutilNewJob(DXTestCase):
    @classmethod
    def setUpClass(cls):
        with testutil.temporary_project(name='dx-jobutil-new-job test project', cleanup=False) as p:
            cls.aux_project = p

    @classmethod
    def tearDownClass(cls):
        dxpy.api.project_destroy(cls.aux_project.get_id(), {})

    def assertNewJobInputHash(self, cmd_snippet, arguments_hash):
        cmd = "dx-jobutil-new-job entrypointname " + cmd_snippet + " --test"
        expected_job_input = {"function": "entrypointname", "input": {}}
        env = override_environment(DX_JOB_ID="job-000000000000000000000001", DX_WORKSPACE_ID=self.project)
        output = run(cmd, env=env)
        expected_job_input.update(arguments_hash)
        self.assertEqual(json.loads(output), expected_job_input)

    def assertNewJobError(self, cmd_snippet, exit_code):
        cmd = "dx-jobutil-new-job entrypointname " + cmd_snippet + " --test"
        env = override_environment(DX_JOB_ID="job-000000000000000000000001", DX_WORKSPACE_ID=self.project)
        with self.assertSubprocessFailure(exit_code=exit_code):
            run(cmd, env=env)

    def test_input(self):
        first_record = dxpy.new_dxrecord(name="first_record")
        second_record = dxpy.new_dxrecord(name="second_record")
        dxpy.new_dxrecord(name="duplicate_name_record")
        dxpy.new_dxrecord(name="duplicate_name_record")
        # In a different project...
        third_record = dxpy.new_dxrecord(name="third_record", project=self.aux_project.get_id())

        test_cases = (
            # string
            ("-ifoo=input_string", {"foo": "input_string"}),
            # string that looks like a {job,analysis} ID
            ("-ifoo=job-012301230123012301230123", {"foo": "job-012301230123012301230123"}),
            ("-ifoo=analysis-012301230123012301230123", {"foo": "analysis-012301230123012301230123"}),
            # int
            ("-ifoo=24", {"foo": 24}),
            # float
            ("-ifoo=24.5", {"foo": 24.5}),
            # json
            ('-ifoo=\'{"a": "b"}\'', {"foo": {"a": "b"}}),
            ('-ifoo=\'["a", "b"]\'', {"foo": ["a", "b"]}),
            # objectName
            ("-ifoo=first_record", {"foo": dxpy.dxlink(first_record.get_id(), self.project)}),
            # objectId
            ("-ifoo=" + first_record.get_id(), {"foo": dxpy.dxlink(first_record.get_id())}),
            # project:objectName
            ("-ifoo=" + self.aux_project.get_id() + ":third_record",
             {"foo": dxpy.dxlink(third_record.get_id(), self.aux_project.get_id())}),
            # project:objectId
            ("-ifoo=" + self.aux_project.get_id() + ":" + third_record.get_id(),
             {"foo": dxpy.dxlink(third_record.get_id(), self.aux_project.get_id())}),
            # same, but wrong project is specified
            ("-ifoo=" + self.project + ":" + third_record.get_id(),
             {"foo": dxpy.dxlink(third_record.get_id(), self.aux_project.get_id())}),
            # glob
            ("-ifoo=first*", {"foo": dxpy.dxlink(first_record.get_id(), self.project)}),
            # JBOR
            ("-ifoo=job-012301230123012301230123:outputfield",
             {"foo": {"$dnanexus_link": {"job": "job-012301230123012301230123", "field": "outputfield"}}}),
            # order of inputs is preserved from command line to API call
            ("-ifoo=first* -ifoo=second_record -ifoo=job-012301230123012301230123:outputfield",
             {"foo": [dxpy.dxlink(first_record.get_id(), self.project),
                      dxpy.dxlink(second_record.get_id(), self.project),
                      {"$dnanexus_link": {"job": "job-012301230123012301230123", "field": "outputfield"}}]}),
            ("-ifoo=job-012301230123012301230123:outputfield -ifoo=first_record -ifoo=second_*",
             {"foo": [{"$dnanexus_link": {"job": "job-012301230123012301230123", "field": "outputfield"}},
                      dxpy.dxlink(first_record.get_id(), self.project),
                      dxpy.dxlink(second_record.get_id(), self.project)]}),
            # if there is any ambiguity, the name is left unresolved
            ("-ifoo=duplicate_name_record", {"foo": "duplicate_name_record"}),
            ("-ifoo=*record", {"foo": "*record"}),
            # Override class
            ("-ifoo:int=24", {"foo": 24}),
            ("-ifoo:string=24", {"foo": "24"}),
            ("-ifoo:string=first_record", {"foo": "first_record"}),
            ('-ifoo:hash=\'{"a": "b"}\'', {"foo": {"a": "b"}}),
            ('-ifoo:hash=\'["a", "b"]\'', {"foo": ["a", "b"]}),

            # Array inputs

            # implicit array notation
            ("-ifoo=24 -ifoo=25", {"foo": [24, 25]}),
            ("-ifoo=25 -ibar=1 -ifoo=24", {"foo": [25, 24], "bar": 1}),
            ("-ifoo=first_record -ifoo=second_record",
             {"foo": [dxpy.dxlink(first_record.get_id(), self.project),
                      dxpy.dxlink(second_record.get_id(), self.project)]}),
            # different types (unusual, but potentially meaningful if
            # foo is a json input)
            ("-ifoo=24 -ifoo=bar", {"foo": [24, "bar"]}),

            # explicit array notation is NOT respected (in contexts with
            # no inputSpec such as this one)
            ("-ifoo:array:int=24", {"foo": 24}),
            ("-ifoo:array:record=first_record", {"foo": dxpy.dxlink(first_record.get_id(), self.project)}),
        )

        for cmd_snippet, expected_input_hash in test_cases:
            arguments_hash = {"input": expected_input_hash}
            self.assertNewJobInputHash(cmd_snippet, arguments_hash)

    def test_bad_input(self):
        # testing some erroneous input
        self.assertNewJobError("-ifoo:file=first_record", 1)
        self.assertNewJobError("-ifoo:int=foo", 1)
        self.assertNewJobError("-ifoo:int=24.5", 1)

    def test_job_arguments(self):
        test_arguments = (
            # name - string
            ("--name JobName", {"name": "JobName"}),
            # depends-on - array of strings
            ("--depends-on foo bar baz", {"dependsOn": ["foo", "bar", "baz"]}),
            # instance type: single instance - string
            ("--instance-type foo_bar_baz",
             {"systemRequirements": {"entrypointname": { "instanceType": "foo_bar_baz" }}}),
            # instance type: mapping
            ("--instance-type " +
                pipes.quote(json.dumps({"main": "mem2_hdd2_x2" , "other_function": "mem2_hdd2_x1" })),
                {"systemRequirements": {"main": { "instanceType": "mem2_hdd2_x2" },
                                        "other_function": { "instanceType": "mem2_hdd2_x1" }}}),
            # properties - mapping
            ("--property foo=foo_value --property bar=bar_value",
                {"properties": {"foo": "foo_value", "bar": "bar_value"}}),
            # tags - array of strings
            ("--tag foo --tag bar --tag baz", {"tags": ["foo", "bar", "baz"]}),
        )
        for cmd_snippet, arguments_hash in test_arguments:
            self.assertNewJobInputHash(cmd_snippet, arguments_hash)

    def test_extra_arguments(self):
        cmd_snippet = "--extra-args " + pipes.quote(
            json.dumps({"details": {"d1": "detail1", "d2": 1234}, "foo": "foo_value"}))
        arguments_hash = {"details": {"d1": "detail1", "d2": 1234}, "foo": "foo_value"}
        self.assertNewJobInputHash(cmd_snippet, arguments_hash)

        # override previously specified args
        cmd_snippet = "--name JobName --extra-args " + pipes.quote(json.dumps({"name": "FinalName"}))
        arguments_hash = {"name": "FinalName"}
        self.assertNewJobInputHash(cmd_snippet, arguments_hash)

    def test_bad_arguments(self):
        # empty name
        self.assertNewJobError("--name", exit_code=2)
        # property not in key=value format
        self.assertNewJobError("--property foo", exit_code=3)
        # extra-args not in key=value format
        self.assertNewJobError("--extra-args argument", exit_code=3)


class TestDXBashHelperMethods(unittest.TestCase):
    def test_limit_threads(self):
        ''' Tests that the number of threads used for downloading inputs in parallel is limited '''
        instance_types = InstanceTypesCompleter().instance_types
        max_threads = 8
        for inst in instance_types.values():
            num_threads = _get_num_parallel_threads(max_threads, inst.CPU_Cores, inst.Memory_GB*1024)
            self.assertTrue(num_threads >= 1 and num_threads <= max_threads)
            self.assertTrue(num_threads <= inst.CPU_Cores)
            self.assertTrue(num_threads*1200 <= inst.Memory_GB*1024 or num_threads == 1)


if __name__ == '__main__':
    unittest.main()
