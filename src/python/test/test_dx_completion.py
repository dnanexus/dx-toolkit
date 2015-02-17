#!/usr/bin/env python
#
# Copyright (C) 2013-2014 DNAnexus, Inc.
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

import os, unittest, subprocess
from tempfile import NamedTemporaryFile, mkdtemp

import dxpy
import dxpy_testutil as testutil

# TODO: unit tests for dxpy.utils.completer

IFS = '\013'

def split_line_like_bash(line, point):
    '''
    :returns: comp_words, comp_cword

    Split the line like bash would, and then put it back together with
    IFS, and calculate cword while you're at it.

    Use os.environ['COMP_POINT'] to figure out which word we're in
    '''
    point = int(point)
    cwords = []
    current_word = ''
    append_to_current_word = False
    for pos in range(len(line)):
        if pos == point:
            cword = len(cwords)

        if append_to_current_word:
            append_to_current_word = False
            current_word += line[pos]
        elif line[pos] == '\\':
            append_to_current_word = True
            current_word += line[pos]
        elif line[pos].isspace():
            if len(current_word) > 0:
                cwords.append(current_word)
                current_word = ''
        else:
            # non-whitespace in COMP_WORDBREAKS that get their own words: ><=:
            if line[pos] in '[<>=:]':
                if len(current_word) > 0:
                    cwords.append(current_word)
                    current_word = ''
                cwords.append(line[pos])
                if pos == point:
                    cword += 1
            else:
                current_word += line[pos]

    if len(current_word) > 0:
        cwords.append(current_word)
    elif line[-1].isspace():
        cwords.append('')

    if point == len(line):
        cword = len(cwords) - 1

    return IFS.join(cwords), str(cword)

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

    def setUp(self):
        os.environ['IFS'] = IFS
        os.environ['_ARGCOMPLETE'] = '1'
        os.environ['_DX_ARC_DEBUG'] = '1'
        os.environ['COMP_WORDBREAKS'] = '"\'@><=;|&(:'
        os.environ['DX_PROJECT_CONTEXT_ID'] = self.project_id
        dxpy.set_workspace_id(self.project_id)

    def tearDown(self):
        dxpy.api.project_remove_folder(self.project_id,
                                       {"folder": "/", "recurse": True})
        for var in 'IFS', '_ARGCOMPLETE', '_DX_ARC_DEBUG', 'COMP_WORDBREAKS':
            if var in os.environ:
                del os.environ[var]

    def get_bash_completions(self, line, point=None, stderr_contains=""):
        os.environ['COMP_LINE'] = line
        os.environ['COMP_POINT'] = point if point else str(len(line))

        p = subprocess.Popen('dx', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        self.assertIn(stderr_contains, err)
        return out.split(IFS)

    def assert_completion(self, line, completion):
        self.assertIn(completion, self.get_bash_completions(line))

    def assert_completions(self, line, completions):
        actual_completions = self.get_bash_completions(line)

        for completion in completions:
            self.assertIn(completion, actual_completions)

    def assert_non_completion(self, line, non_completion):
        self.assertNotIn(non_completion, self.get_bash_completions(line))

    def assert_no_completions(self, line, stderr_contains=""):
        self.assertEqual(self.get_bash_completions(line, stderr_contains=stderr_contains), [''])

    def test_command_completion(self):
        self.assert_completion("dx ru", "run ")
        self.assert_completion("dx run", "run ")
        self.assert_completions("dx l", ["login", "logout", "ls"])
        self.assert_completions("dx ", ["login", "logout", "cp"])

    def test_subcommand_completion(self):
        self.assert_completions("dx find ", ["apps", "data", "jobs", "projects"])
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
        dxapplet.new(runSpec={"code": "placeholder", "interpreter": "bash"},
                     dxapi="1.0.0",
                     name="my applet")

        self.assert_completion("dx ls my", "my applet ")
        self.assert_completion("dx ls", "ls ")
        self.assert_completion("dx run my", "my applet ")
        self.assert_completion("dx ls ", "my applet ")

        # not available to run when hidden
        dxapplet.new(runSpec={"code": "placeholder", "interpreter": "bash"},
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
        self.assert_completion("dx select \"to", "\"to select:")
        self.assert_completion("dx select to\ select:", " ")

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
        self.assert_completion('dx ls "my', '"my <<awesome.>> record \\!@#\\$%^&*(){}[]|;\\:?\\`')
        self.assert_completion("dx ls 'my", "'my <<awesome.>> record !@#$%^&*(){}[]|;\\:?`")

if __name__ == '__main__':
    unittest.main()
