#!/usr/bin/env python3
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

import os, unittest, re, subprocess, sys
from tempfile import NamedTemporaryFile, mkdtemp
import pexpect, pytest

import dxpy
import dxpy_testutil as testutil
from dxpy.exceptions import DXError
from dxpy.compat import USING_PYTHON2

# TODO: unit tests for dxpy.utils.completer

IFS = '\013'


class TestDXTabCompletion(unittest.TestCase):
    project_id = None
    ids_to_destroy = []

    @classmethod
    def setUpClass(cls):
        cls.project_id = dxpy.api.project_new({"name": "tab-completion project"})['id']

    @classmethod
    def tearDownClass(cls):
        dxpy.api.project_destroy(cls.project_id)
        for entity_id in cls.ids_to_destroy:
            dxpy.DXHTTPRequest("/" + entity_id + "/destroy", {})
        dxpy.set_workspace_id(None)

    def setUp(self):
        os.environ['IFS'] = IFS
        os.environ['_ARGCOMPLETE'] = '1'
        os.environ['_DX_ARC_DEBUG'] = '1'
        os.environ['COMP_WORDBREAKS'] = '"\'@><=;|&(:'
        os.environ['DX_PROJECT_CONTEXT_ID'] = self.project_id
        dxpy.set_workspace_id(self.project_id)

    def tearDown(self):
        completed = False
        while not completed:
            resp = dxpy.api.project_remove_folder(self.project_id,
                                                  {"folder": "/", "recurse": True, "partial": True})
            if 'completed' not in resp:
                raise DXError('Error removing folder')
            completed = resp['completed']
        for var in 'IFS', '_ARGCOMPLETE', '_DX_ARC_DEBUG', 'COMP_WORDBREAKS', 'DX_PROJECT_CONTEXT_ID':
            if var in os.environ:
                del os.environ[var]

    def assert_completion(self, line, completion):
        proc = pexpect.spawn("/bin/bash --login", encoding="utf-8")
        proc.sendline('eval "$(register-python-argcomplete dx|sed \'s/-o default//\')"')
        proc.send(f"{line}\t\t")
        proc.expect(completion)
        proc.sendline('\003')
        proc.sendline("exit")
        proc.expect(pexpect.EOF)

    def assert_completions(self, line, completions):
        ...

    def assert_no_completions(self, line):
        ...

    def test_command_completion(self):
        self.assert_completion("dx ru", "run")
        self.assert_completion("dx run", "run")
        self.assert_completion("dx l", "list          login         logout        loupe-viewer  ls")
        self.assert_completion("dx ", "whoami")

    def test_subcommand_completion(self):
        self.assert_completion("dx find ", "analyses         apps             data             executions       globalworkflows  jobs             org              orgs             projects")
        self.assert_completions("dx new   ", ["project", "record", "workflow"])

    def test_option_completion(self):
        self.assert_completions("dx -", ["-h", "--help", "--env-help"])

    def test_folder_completion(self):
        dxproj = dxpy.DXProject()
        dxproj.new_folder('/foo/bar', parents=True)
        self.assert_completions("dx ls ", ["foo/"])
        self.assert_completions("dx ls fo", ["foo/"])
        self.assert_completions("dx ls foo/", ["foo/bar/"])

    def test_category_completion(self):
        from dxpy.app_categories import APP_CATEGORIES
        self.assertTrue(len(APP_CATEGORIES) > 0)
        self.assert_completions("dx find apps --category ", APP_CATEGORIES)

    def test_applet_completion(self):
        dxapplet = dxpy.DXApplet()
        run_spec = {"code": "placeholder", "interpreter": "bash",
                    "distribution": "Ubuntu", "release": "14.04"}
        dxapplet.new(runSpec=run_spec,
                     dxapi="1.0.0",
                     name="my applet")

        self.assert_completion("dx ls my", "my applet ")
        self.assert_completion("dx ls", "ls ")
        self.assert_completion("dx run my", "my applet ")
        self.assert_completion("dx ls ", "my applet ")

        # not available to run when hidden
        dxapplet.new(runSpec=run_spec,
                     dxapi="1.0.0",
                     name="hidden",
                     hidden=True)
        self.assert_completion("dx ls hid", "hidden ")
        self.assert_no_completions("dx run hid")

    def test_workflow_completion(self):
        dxworkflow = dxpy.new_dxworkflow(name="my workflow")
        self.assert_completion("dx run my", "my workflow ")
        dxworkflow.hide()
        self.assert_no_completions("dx run my")

    def test_project_completion(self):
        self.ids_to_destroy.append(dxpy.api.project_new({"name": "to select"})['id'])
        self.assert_completion("dx select to", "to select\\:")
        self.assert_completion(r"dx select to\ sele", "to select\\:")

    def test_completion_with_bad_current_project(self):
        os.environ['DX_PROJECT_CONTEXT_ID'] = ''
        dxpy.set_workspace_id('')
        self.assert_completion("dx select ", "tab-completion project\\:")
        self.assert_completion("dx cd ta", "tab-completion project\\:")

    @unittest.skipUnless(testutil.TEST_ENV,
                         'skipping test that would clobber your local environment')
    def test_completion_with_no_current_project(self):
        del dxpy.config['DX_PROJECT_CONTEXT_ID']
        dxpy.config.save()

        self.assert_completion("dx select ", "tab-completion project\\:")
        self.assert_completion("dx cd ta", "tab-completion project\\:")

    def test_local_file_completion(self):
        with NamedTemporaryFile(dir=os.getcwd()) as local_file:
            self.assert_completion("dx upload ", os.path.basename(local_file.name))

    def test_local_dir_completion(self):
        old_cwd = os.getcwd()
        tempdir = mkdtemp()
        os.chdir(tempdir)
        try:
            os.makedirs("subdir/subsubdir")
            self.assert_completion("dx upload ", "subdir/")
            self.assert_completion("dx build ", "subdir/")
        finally:
            os.chdir(old_cwd)

    def test_noninterference_of_local_files(self):
        self.assert_no_completions("dx ls ")
        self.assert_no_completions("dx ls noninter")
        # TODO: re-enable this after figuring out exception control and switching to argcomplete.warn().
        # self.assert_no_completions("dx ls nonexistent-project:", stderr_contains="Could not find a project named")
        self.assert_no_completions("dx ls :")

    def test_escaping(self):
        # TODO: test backslash-escaping behavior for use with dx ls
        # (aside from special characters, escape the string so that
        # "*" and "?" aren't used as part of the glob pattern, escape
        # "/")
        r = dxpy.new_dxrecord(name='my <<awesome.>> record !@#$%^&*(){}[]|;:?`')
        self.assert_completion('dx ls my', 'my \\<\\<awesome.\\>\\> record \\!\\@#$%^\\&*\\(\\){}[]\\|\\;\\\\:?\\` ')

        # FIXME, this stopped working when migrating to python3
        # self.assert_completion('dx ls "my', '"my <<awesome.>> record \\!@#\\$%^&*(){}[]|;\\:?\\`')
        # self.assert_completion("dx ls 'my", "'my <<awesome.>> record !@#$%^&*(){}[]|;\\:?`")


if __name__ == '__main__':
    unittest.main()
