# -*- coding: utf-8 -*-
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

from __future__ import print_function

import platform, locale
sys_encoding = locale.getdefaultlocale()[1] or 'UTF-8'
if platform.python_implementation() != "PyPy":
    try:
        reload(sys).setdefaultencoding(sys_encoding)
    except:
        pass


import os, sys, unittest, subprocess, re
from contextlib import contextmanager

import dxpy

_run_all_tests = 'DXTEST_FULL' in os.environ
TEST_CREATE_APPS = _run_all_tests or 'DXTEST_CREATE_APPS' in os.environ
TEST_ENV = _run_all_tests or 'DXTEST_ENV' in os.environ
TEST_FUSE = _run_all_tests or 'DXTEST_FUSE' in os.environ
TEST_HTTP_PROXY = _run_all_tests or 'DXTEST_HTTP_PROXY' in os.environ
TEST_NO_RATE_LIMITS = _run_all_tests or 'DXTEST_NO_RATE_LIMITS' in os.environ
TEST_RUN_JOBS = _run_all_tests or 'DXTEST_RUN_JOBS' in os.environ
TEST_TCSH = _run_all_tests or 'DXTEST_TCSH' in os.environ

def _transform_words_to_regexp(s):
    return r"\s+".join(re.escape(word) for word in s.split())

class DXTestCase(unittest.TestCase):
    def setUp(self):
        proj_name = u"dxclient_test_pröject"
        # Unplug stdin so that dx doesn't prompt user for input at the tty
        self.project = subprocess.check_output(u"dx new project '{p}' --brief".format(p=proj_name), shell=True, stdin=subprocess.PIPE).strip()
        os.environ["DX_PROJECT_CONTEXT_ID"] = self.project
        subprocess.check_call(u"dx cd "+self.project+":/", shell=True)
        dxpy._initialize(suppress_warning=True)
        if 'DX_CLI_WD' in os.environ:
            del os.environ['DX_CLI_WD']

    def tearDown(self):
        try:
            subprocess.check_call(u"dx rmproject --yes --quiet {p}".format(p=self.project), shell=True)
        except Exception as e:
            print("Failed to remove test project:", str(e))
        if 'DX_PROJECT_CONTEXT_ID' in os.environ:
            del os.environ['DX_PROJECT_CONTEXT_ID']
        if 'DX_CLI_WD' in os.environ:
            del os.environ['DX_CLI_WD']

    # Be sure to use the check_output defined in this module if you wish
    # to use stderr_regexp. Python's usual subprocess.check_output
    # doesn't propagate stderr back to us.
    @contextmanager
    def assertSubprocessFailure(self, output_regexp=None, output_text=None, stderr_regexp=None, stderr_text=None, exit_code=3):
        """Asserts that the block being wrapped exits with CalledProcessError.

        :param output_regexp: subprocess output must match this regexp
        :type output_regexp: str
        :param output_text: subprocess output must contain this string (allowing for whitespace changes)
        :type output_text: str
        :param stderr_regexp: subprocess stderr must match this regexp
        :type stderr_regexp: str
        :param stderr_text: subprocess stderr must contain this string (allowing for whitespace changes)
        :type stderr_text: str
        :param exit_code: assert subprocess exits with this exit code
        :type exit_code: int

        """
        # TODO: print out raw output_text or stderr_text if assertion
        # fails for easier human parsing
        if output_text is not None:
            if output_regexp is not None:
                raise ValueError("Cannot specify both output_regexp and output_text")
            output_regexp = _transform_words_to_regexp(output_text)
        if stderr_text is not None:
            if stderr_regexp is not None:
                raise ValueError("Cannot specify both stderr_regexp and stderr_text")
            stderr_regexp = _transform_words_to_regexp(stderr_text)
        try:
            yield
        except subprocess.CalledProcessError as e:
            self.assertEqual(exit_code, e.returncode, "Expected command to return code %d but it returned code %d" % (exit_code, e.returncode))
            if output_regexp:
                print("stdout:")
                print(e.output)
                self.assertTrue(re.search(output_regexp, e.output), "Expected stdout to match '%s' but it didn't" % (output_regexp,))
            if stderr_regexp:
                if not hasattr(e, 'stderr'):
                    raise Exception('A stderr_regexp was supplied but the CalledProcessError did not return the contents of stderr')
                print("stderr:")
                print(e.stderr)
                self.assertTrue(re.search(stderr_regexp, e.stderr), "Expected stderr to match '%s' but it didn't" % (stderr_regexp,))
            return
        self.assertFalse(True, "Expected command to fail with CalledProcessError but it succeeded")
