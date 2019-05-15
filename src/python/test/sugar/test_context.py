from __future__ import print_function, unicode_literals, division, absolute_import
import os
import dxpy
import unittest

from dxpy.sugar import context

import logging
logging.basicConfig(level="INFO")

class TestContext(unittest.TestCase):
    def test_user_context(self):
        # mimic worker context
        dxpy.JOB_ID = "job-12345"
        os.environ["DX_JOB_ID"] = "job-12345"
        os.environ["DX_SECURITY_CONTEXT"] = "mysecuritycontext"
        os.environ["DX_WORKSPACE_ID"] = "myworkspaceid"

        # switch to user context
        with context.UserContext("myapitoken"):
            self.assertIsNone(dxpy.JOB_ID, "dxpy.JOB_ID should be None within user context")

        # switch back to job context
        self.assertEqual(dxpy.JOB_ID, "job-12345", "dxpy.JOB_ID should be restored to original value after exiting user context.")

    def test_worker_context(self):
        """
            make sure error is properly raised when UserContext is called without job context
        """
        error_raised = False
        try:
            with context.UserContext("myapitoken"):
                error_raised = False
        except dxpy.DXError:
            error_raised = True

        self.assertTrue(error_raised, "dxpy.DXError should have been raised when trying to use user context outside of a job context.")

    def test_set_env(self):
        with context.set_env({"PLUGINS_DIR": u"test/plugins"}):
            self.assertIn("PLUGINS_DIR", os.environ, "Expected PLUGINS_DIR to be found in os.environ")

        self.assertNotIn("PLUGINS_DIR", os.environ, "PLUGINS_DIR should not be present in os.environ outside of set_env")


    def test_set_env_override(self):
        os.environ["SOME_FIELD"] = "somevalue"
        with context.set_env({"PLUGINS_DIR": u"test/plugins"}, override=True):
            self.assertIn("PLUGINS_DIR", os.environ, "Expected PLUGINS_DIR to be found in os.environ")
            self.assertNotIn("SOME_FIELD", os.environ, "SOME_FIELD should not be in os.environ when environ is overwritten")

        self.assertIn("SOME_FIELD", os.environ, "SOME_FIELD should have been restored in os.environ outside of set_env")
        self.assertNotIn("PLUGINS_DIR", os.environ, "PLUGINS_DIR should not be present in os.environ outside of set_env")

    def test_cd(self):
        prev_dir = os.getcwd()
        with context.cd():
            temp_dir = os.getcwd()
            self.assertNotEqual(prev_dir, temp_dir, "Failed to change dirs")

        self.assertFalse(os.path.exists(temp_dir), "{0} should have been deleted after cd".format(temp_dir))

    def test_cd_targetpath(self):
        prev_dir = os.getcwd()
        with context.cd("/tmp/some_path", cleanup=False):
            curr_dir = os.getcwd()
            self.assertNotEqual(prev_dir, curr_dir, "Failed to change dirs")

        self.assertTrue(os.path.exists("/tmp/some_path"), "Temp path should still exist")
