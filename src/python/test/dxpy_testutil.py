# -*- coding: utf-8 -*-
import os, sys, unittest, subprocess, re
from contextlib import contextmanager

import dxpy

_run_all_tests = 'DXTEST_FULL' in os.environ
TEST_CREATE_APPS = _run_all_tests or 'DXTEST_CREATE_APPS' in os.environ
TEST_FUSE = _run_all_tests or 'DXTEST_FUSE' in os.environ
TEST_RUN_JOBS = _run_all_tests or 'DXTEST_RUN_JOBS' in os.environ
TEST_HTTP_PROXY = _run_all_tests or 'DXTEST_HTTP_PROXY' in os.environ

class DXTestCase(unittest.TestCase):
    def setUp(self):
        subprocess.check_call("dx clearenv --reset", shell=True)
        proj_name = u"dxclient_test_pröject"
        self.project = subprocess.check_output(u"dx new project '{p}' --brief".format(p=proj_name), shell=True).strip()
        os.environ["DX_PROJECT_CONTEXT_ID"] = self.project
        # TODO: Fix this once process-wise sessions are in place.  For
        # now, have to save the old current directory and overwrite
        # the file.
        if os.path.exists(os.path.expanduser('~/.dnanexus_config/DX_CLI_WD')):
            with open(os.path.expanduser('~/.dnanexus_config/DX_CLI_WD')) as fd:
                self.old_cwd = fd.read()
            os.remove(os.path.expanduser('~/.dnanexus_config/DX_CLI_WD'))
        else:
            self.old_cwd = None
        if 'DX_CLI_WD' in os.environ:
            del os.environ['DX_CLI_WD']
        dxpy._initialize(suppress_warning=True)

    def tearDown(self):
        try:
            subprocess.check_call(u"dx rmproject --yes --quiet {p}".format(p=self.project), shell=True)
        except:
            pass
        if self.old_cwd is not None:
            with open(os.path.expanduser('~/.dnanexus_config/DX_CLI_WD'), 'w') as fd:
                fd.write(self.old_cwd)

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
