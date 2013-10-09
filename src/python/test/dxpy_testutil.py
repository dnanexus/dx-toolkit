# -*- coding: utf-8 -*-

import platform, locale, sys
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
TEST_FUSE = _run_all_tests or 'DXTEST_FUSE' in os.environ
TEST_RUN_JOBS = _run_all_tests or 'DXTEST_RUN_JOBS' in os.environ
TEST_HTTP_PROXY = _run_all_tests or 'DXTEST_HTTP_PROXY' in os.environ
TEST_ENV = _run_all_tests or 'DXTEST_ENV' in os.environ

class DXTestCase(unittest.TestCase):
    def setUp(self):
        proj_name = u"dxclient_test_pröject"
        self.project = subprocess.check_output(u"dx new project '{p}' --brief".format(p=proj_name), shell=True).strip()
        os.environ["DX_PROJECT_CONTEXT_ID"] = self.project
        subprocess.check_call(u"dx cd "+self.project+":/", shell=True)
        dxpy._initialize(suppress_warning=True)
        if 'DX_CLI_WD' in os.environ:
            del os.environ['DX_CLI_WD']

    def tearDown(self):
        try:
            subprocess.check_call(u"dx rmproject --yes --quiet {p}".format(p=self.project), shell=True)
        except Exception as e:
            print "Failed to remove test project:", str(e)
        if 'DX_PROJECT_CONTEXT_ID' in os.environ:
            del os.environ['DX_PROJECT_CONTEXT_ID']
        if 'DX_CLI_WD' in os.environ:
            del os.environ['DX_CLI_WD']

    # Be sure to use the check_output defined in this module if you wish
    # to use stderr_regexp. Python's usual subprocess.check_output
    # doesn't propagate stderr back to us.
    @contextmanager
    def assertSubprocessFailure(self, output_regexp=None, stderr_regexp=None, exit_code=3):
        try:
            yield
        except subprocess.CalledProcessError as e:
            self.assertEqual(exit_code, e.returncode, "Expected command to return code %d but it returned code %d" % (exit_code, e.returncode))
            if output_regexp:
                print "stdout:"
                print e.output
                self.assertTrue(re.search(output_regexp, e.output), "Expected stdout to match '%s' but it didn't" % (output_regexp,))
            if stderr_regexp:
                if not hasattr(e, 'stderr'):
                    raise Exception('A stderr_regexp was supplied but the CalledProcessError did not return the contents of stderr')
                print "stderr:"
                print e.stderr
                self.assertTrue(re.search(stderr_regexp, e.stderr), "Expected stderr to match '%s' but it didn't" % (stderr_regexp,))
            return
        self.assertFalse(True, "Expected command to fail with CalledProcessError but it succeeded")
