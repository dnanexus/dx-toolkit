import os
import json
import pipes
import shutil
import tempfile
import unittest

import dxpy
import dxpy_testutil as testutil
from dxpy_testutil import (
    DXTestCase,
    check_output,
    temporary_project,
    override_environment,
)

TEST_APPS = os.path.join(os.path.dirname(__file__), "file_load")


def update_environ(**kwargs):
    """
    Returns a copy of os.environ with the specified updates (VAR=value for each kwarg)
    """
    output = os.environ.copy()
    for k, v in kwargs.items():
        if v is None:
            del output[k]
        else:
            output[k] = v
    return output


def run(command, **kwargs):
    try:
        if isinstance(command, list) or isinstance(command, tuple):
            print("$ %s" % " ".join(pipes.quote(f) for f in command))
            output = check_output(command, **kwargs)
        else:
            print("$ %s" % (command,))
            output = check_output(command, shell=True, **kwargs)
    except testutil.DXCalledProcessError as e:
        print("== stdout ==")
        print(e.output)
        print("== stderr ==")
        print(e.stderr)
        raise
    print(output)
    return output


def build_app(app_dir, project_id):
    tempdir = tempfile.mkdtemp()

    try:
        updated_app_dir = os.path.join(tempdir, os.path.basename(app_dir))
        shutil.copytree(app_dir, updated_app_dir)

        # Copy the current verion of dx-toolkit. We will build it on the worker
        # and source this version which will overload the stock version of dx-toolkit.
        # This way we can test all bash helpers as they would appear locally with all
        # necessary dependencies
        # dxtoolkit_dir = os.path.abspath(os.path.join(updated_app_dir, 'resources', 'dxtoolkit'))
        # local_dxtoolkit = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
        dxtoolkit_dir = os.path.join(updated_app_dir, "resources", "dxtoolkit")
        local_dxtoolkit = os.path.join(os.path.dirname(__file__), "..", "..", "..")
        shutil.copytree(local_dxtoolkit, dxtoolkit_dir)

        # Add lines to the beginning of the job to make and use our new dx-toolkit
        preamble = []
        # preamble.append("cd {appdir}/resources && git clone https://github.com/dnanexus/dx-toolkit.git".format(appdir=updated_app_dir))
        preamble.append("python3 -m pip install /dxtoolkit/src/python/\n")

        # Now find the applet entry point file and prepend the
        # operations above, overwriting it in place.
        with open(os.path.join(app_dir, "dxapp.json")) as f:
            dxapp_json = json.load(f)
        if dxapp_json["runSpec"]["interpreter"] != "bash":
            raise Exception(
                "Sorry, I only know how to patch bash apps for remote testing"
            )
        entry_point_filename = os.path.join(app_dir, dxapp_json["runSpec"]["file"])
        with open(entry_point_filename) as fh:
            entry_point_data = "".join(preamble) + fh.read()
        with open(
            os.path.join(updated_app_dir, dxapp_json["runSpec"]["file"]), "w"
        ) as fh:
            fh.write(entry_point_data)

        build_output = run(
            [
                "dx",
                "build",
                "--json",
                "--destination",
                project_id + ":",
                updated_app_dir,
            ]
        )
        return json.loads(build_output)["id"]
    finally:
        shutil.rmtree(tempdir)


# @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping tests that would run jobs')
class TestDXJobUtil(DXTestCase):
    def test_job_identity_token(self):
        """Tests dx-upload-all-outputs uploading with filesystem metadata as properties"""
        with temporary_project("TestDXJobUtil.test_app1 temporary project") as dxproj:
            env = update_environ(DX_PROJECT_CONTEXT_ID=dxproj.get_id())

            # Upload some files for use by the applet
            dxpy.upload_string("1234\n", project=dxproj.get_id(), name="A.txt")
            dxpy.upload_string("ABCD\n", project=dxproj.get_id(), name="B.txt")

            # Build the applet, patching in the bash helpers from the
            # local checkout
            applet_id = build_app(
                os.path.join(TEST_APPS, "job_identity_token"), dxproj.get_id()
            )

            # Run the applet
            applet_args = ["-iseq1=A.txt", "-iseq2=B.txt", "-iref=A.txt", "-iref=B.txt"]
            cmd_args = ["dx", "run", "--yes", "--watch", applet_id]
            cmd_args.extend(applet_args)
            run(cmd_args, env=env)


if __name__ == "__main__":
    unittest.main()
