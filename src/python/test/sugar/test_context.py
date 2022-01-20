import logging
import os
from pathlib import Path
import shutil
import tempfile
import unittest

import dxpy_testutil as testutil

import dxpy
from dxpy.sugar import context


logging.basicConfig(level="INFO")


class TestContext(unittest.TestCase):
    @unittest.skipUnless(
        testutil.TEST_ENV, "skipping test that would clobber your local environment"
    )
    def test_user_context(self):
        # mimic worker context
        worker_env = {
            "DX_JOB_ID": "job-12345",
            "DX_SECURITY_CONTEXT": "mysecuritycontext",
            "DX_WORKSPACE_ID": "myworkspaceid",
        }
        os.environ = testutil.override_environment(**worker_env)
        dxpy.JOB_ID = worker_env["DX_JOB_ID"]

        # switch to user context
        with context.UserContext("myapitoken"):
            self.assertIsNone(
                dxpy.JOB_ID, "dxpy.JOB_ID should be None within user context"
            )

        # switch back to job context
        self.assertEqual(
            dxpy.JOB_ID,
            "job-12345",
            "dxpy.JOB_ID should be restored to original value after exiting user"
            "context.",
        )

    def test_worker_context(self):
        """make sure error is properly raised when UserContext is called without job
        context.
        """

        with self.assertRaises(dxpy.DXError):
            with context.UserContext("myapitoken"):
                pass

    def test_set_env(self):
        with context.set_env({"PLUGINS_DIR": "test/plugins"}):
            self.assertIn(
                "PLUGINS_DIR",
                os.environ,
                "Expected PLUGINS_DIR to be found in os.environ",
            )

        self.assertNotIn(
            "PLUGINS_DIR",
            os.environ,
            "PLUGINS_DIR should not be present in os.environ outside of set_env",
        )

    @unittest.skipUnless(
        testutil.TEST_ENV, "skipping test that would clobber your local environment"
    )
    def test_set_env_override(self):
        os.environ["SOME_FIELD"] = "somevalue"
        with context.set_env({"PLUGINS_DIR": "test/plugins"}, override=True):
            self.assertIn(
                "PLUGINS_DIR",
                os.environ,
                "Expected PLUGINS_DIR to be found in os.environ",
            )
            self.assertNotIn(
                "SOME_FIELD",
                os.environ,
                "SOME_FIELD should not be in os.environ when environ is overwritten",
            )

        self.assertIn(
            "SOME_FIELD",
            os.environ,
            "SOME_FIELD should have been restored in os.environ outside of set_env",
        )
        self.assertNotIn(
            "PLUGINS_DIR",
            os.environ,
            "PLUGINS_DIR should not be present in os.environ outside of set_env",
        )

    def test_cd(self):
        prev_dir = os.getcwd()
        with context.cd():
            temp_dir = os.getcwd()
            self.assertNotEqual(prev_dir, temp_dir, "Failed to change dirs")

        self.assertFalse(
            os.path.exists(temp_dir),
            "{0} should have been deleted after cd".format(temp_dir),
        )

    def test_cd_targetpath(self):
        prev_dir = os.getcwd()
        temp_dir = Path(tempfile.mkdtemp())
        try:
            with context.cd(temp_dir, cleanup=False):
                curr_dir = os.getcwd()
                self.assertNotEqual(prev_dir, curr_dir, "Failed to change dirs")

            self.assertTrue(os.path.exists(temp_dir), "Temp path should still exist")
        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
