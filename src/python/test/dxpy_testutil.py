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

import locale
import os, sys, unittest, tempfile, shutil, subprocess, re, json, platform
import time
import random

from contextlib import contextmanager

import dxpy
from dxpy.compat import str, USING_PYTHON2

_run_all_tests = 'DXTEST_FULL' in os.environ
TEST_AZURE = ((os.environ.get('DXTEST_AZURE', '').startswith('azure:') and os.environ['DXTEST_AZURE']) or
              (os.environ.get('DXTEST_AZURE') and 'azure:westus'))
TEST_ISOLATED_ENV = _run_all_tests or 'DXTEST_ISOLATED_ENV' in os.environ
TEST_ENV = _run_all_tests or 'DXTEST_ENV' in os.environ
TEST_DX_DOCKER = 'DXTEST_DOCKER' in os.environ
TEST_FUSE = _run_all_tests or 'DXTEST_FUSE' in os.environ
TEST_HTTP_PROXY = _run_all_tests or 'DXTEST_HTTP_PROXY' in os.environ
TEST_NO_RATE_LIMITS = _run_all_tests or 'DXTEST_NO_RATE_LIMITS' in os.environ
TEST_RUN_JOBS = _run_all_tests or 'DXTEST_RUN_JOBS' in os.environ
TEST_TCSH = _run_all_tests or 'DXTEST_TCSH' in os.environ
TEST_WITH_AUTHSERVER = _run_all_tests or 'DXTEST_WITH_AUTHSERVER' in os.environ
TEST_ONLY_MASTER = 'DX_RUN_NEXT_TESTS' in os.environ
TEST_MULTIPLE_USERS = _run_all_tests or 'DXTEST_SECOND_USER' in os.environ

TEST_DX_LOGIN = 'DXTEST_LOGIN' in os.environ
TEST_BENCHMARKS = 'DXTEST_BENCHMARKS' in os.environ   ## Used to exclude benchmarks from normal runs

def _transform_words_to_regexp(s):
    return r"\s+".join(re.escape(word) for word in s.split())


def host_is_centos_5():
    distro = platform.linux_distribution()
    if distro[0] == 'CentOS' and distro[1].startswith('5.'):
        return True
    return False

class DXCalledProcessError(subprocess.CalledProcessError):
    def __init__(self, returncode, cmd, output=None, stderr=None):
        self.returncode = returncode
        self.cmd = cmd
        self.output = output
        self.stderr = stderr
    def __str__(self):
        return "Command '%s' returned non-zero exit status %d, stderr:\n%s" % (self.cmd, self.returncode, self.stderr)

def check_output(*popenargs, **kwargs):
    """
    Adapted version of the builtin subprocess.check_output which sets a
    "stderr" field on the resulting exception (in addition to "output")
    if the subprocess fails. (If the command succeeds, the contents of
    stderr are discarded.)

    :param also_return_stderr: if True, return stderr along with the output of the command as such (output, stderr)
    :type also_return_stderr: bool

    Unlike subprocess.check_output, unconditionally decodes the contents of the subprocess stdout and stderr using
    sys.stdin.encoding.
    """
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    if 'stderr' in kwargs:
        raise ValueError('stderr argument not allowed, it will be overridden.')

    return_stderr = False
    if 'also_return_stderr' in kwargs:
        if kwargs['also_return_stderr']:
            return_stderr = True
        del kwargs['also_return_stderr']

    # Unplug stdin (if not already overridden) so that dx doesn't prompt
    # user for input at the tty
    process = subprocess.Popen(stdin=kwargs.get('stdin', subprocess.PIPE),
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, *popenargs, **kwargs)
    output, err = process.communicate()
    retcode = process.poll()
    if not isinstance(output, str):
        output = output.decode(sys.stdin.encoding)
    if not isinstance(err, str):
        err = err.decode(sys.stdin.encoding)
    if retcode:
        print(err)
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        exc = DXCalledProcessError(retcode, cmd, output=output, stderr=err)
        raise exc

    if return_stderr:
        return (output, err)
    else:
        return output


@contextmanager
def chdir(dirname=None):
    curdir = os.getcwd()
    try:
        if dirname is not None:
            os.chdir(dirname)
        yield
    finally:
        os.chdir(curdir)


def run(command, **kwargs):
    print("$ %s" % (command,))
    if platform.system() == 'Windows':
        # Before running unicode command strings here via subprocess, avoid
        # letting Python 2.7 on Windows default to encoding the string with
        # the ascii codec - use the preferred encoding of the OS instead
        # (which will likely be 'cp1252'):
        command_encoded = command.encode(locale.getpreferredencoding())
        output = check_output(command_encoded, shell=True, **kwargs)
    else:
        output = check_output(command, shell=True, **kwargs)
    print(output)
    return output


@contextmanager
def temporary_project(name='dx client tests temporary project', cleanup=True, reclaim_permissions=False, select=False,
                      region=None, **kwargs):
    """Creates a temporary project scoped to the context manager, and
    yields a DXProject handler for the project.

    :param cleanup:
        if False, do not clean up the project when done (useful for
        debugging so you can examine the state of the project)
    :type cleanup: bool
    :param reclaim_permissions:
        if True, attempts a project-xxxx/join before trying to destroy
        the project. May be needed if the test reduced its own
        permissions in the project.
    :type reclaim_permissions: bool
    :param select:
        if True, sets the environment variable DX_PROJECT_CONTEXT_ID
        (and restores the previous value afterwards) so that subprocess
        calls made within the block use the new project by default.
    :type select: bool
    :param region:
        Region name to create a project in. If None the project is created
        in the default region.
    :type region: str

    """
    input_params = {'name': name}
    if region is not None:
        input_params["region"] = region
    temp_project = dxpy.DXProject(dxpy.api.project_new(input_params, **kwargs)['id'])
    try:
        if select:
            with select_project(temp_project):
                yield temp_project
        else:
            yield temp_project
    finally:
        if reclaim_permissions:
            dxpy.DXHTTPRequest('/' + temp_project.get_id() + '/join', {'level': 'ADMINISTER'}, **kwargs)
        if cleanup:
            dxpy.api.project_destroy(temp_project.get_id(), {"terminateJobs": True}, **kwargs)


@contextmanager
def select_project(project_or_project_id):
    """Selects a project by setting the DX_PROJECT_CONTEXT_ID in
    dxpy.config (and therefore os.environ); this change is propagated
    to subprocesses that are invoked with the default settings. The
    original setting of DX_PROJECT_CONTEXT_ID is restored when the
    block exits.

    :param project_or_project_id:
        Project or container to select. May be specified either as a
        string containing the project ID, or a DXProject handler.
    :type project_or_project_id: str or DXProject

    """
    if isinstance(project_or_project_id, basestring) or project_or_project_id is None:
        project_id = project_or_project_id
    else:
        project_id = project_or_project_id.get_id()
    current_project_env_var = dxpy.config.get('DX_PROJECT_CONTEXT_ID', None)
    if project_id is None:
        del dxpy.config['DX_PROJECT_CONTEXT_ID']
    else:
        dxpy.config['DX_PROJECT_CONTEXT_ID'] = project_id
    try:
        yield None
    finally:
        if current_project_env_var is None:
            del dxpy.config['DX_PROJECT_CONTEXT_ID']
        else:
            dxpy.config['DX_PROJECT_CONTEXT_ID'] = current_project_env_var


# Invoke "dx cd" without using bash (as 'run' would) so that the config
# gets attached to this Python process (instead of the bash process) and
# will be applied in later calls in the same test.
#
# Some tests can also use the select_project helper but that code sets
# the environment variables, and this writes the config to disk, and we
# should test both code paths.
def cd(directory):
    print("$ dx cd %s" % (directory,))
    output = check_output(['dx', 'cd', directory], shell=False)
    print(output)
    return output


# Wait for all jobs in analysis to be created (see PTFM-14462)
def analysis_describe_with_retry(analysis_id_or_handler):
    if isinstance(analysis_id_or_handler, basestring):
        handler = dxpy.get_handler(analysis_id_or_handler)
    else:
        handler = analysis_id_or_handler
    # All the describe fields may not be available immediately. Wait
    # until they have been populated.
    for i in range(200):  # Don't wait an unbounded amount of time
        desc = handler.describe()
        # Sufficient to look for any field, other than 'id', that is
        # present in all job describe hashes
        if all('executable' in stage['execution'] for stage in desc['stages']):
            return desc
        time.sleep(0.5)
    raise IOError('Timed out while waiting for ' + analysis_id_or_handler.get_id() + ' to have all jobs populated')


def override_environment(**kwargs):
    """Returns a copy of the current environment, with variables overridden
    as specified in the arguments. Each key represents a variable name
    and each value must be a string (to set the specified key to that
    value) or None (to unset the specified key).
    """
    env = os.environ.copy()
    for key in kwargs:
        if kwargs[key] is None:
            if key in env:
                del env[key]
        else:
            env[key] = kwargs[key]
    return env


def as_second_user():
    second = json.loads(os.environ['DXTEST_SECOND_USER'])
    context = {"auth_token": second['auth'], "auth_token_type": "Bearer"}
    override = {"DX_SECURITY_CONTEXT": json.dumps(context),
                "DX_USERNAME": second['user']}
    return override_environment(**override)

def generate_unique_username_email():
    r = random.randint(0, 255)
    username = "asset_" + str(int(time.time())) + "_" + str(r)
    email = username + "@example.com"
    return username, email


# Note: clobbers the local environment! All tests that use this should
# be marked as such with TEST_ENV
@contextmanager
def without_project_context():
    """Within the scope of the block, the project context and workspace
    configuration variables (and possibly other variables) are unset.

    """
    prev_workspace_id = os.environ.get('DX_WORKSPACE_ID', None)
    prev_proj_context_id = os.environ.get('DX_PROJECT_CONTEXT_ID', None)
    if prev_workspace_id is not None:
        del os.environ['DX_WORKSPACE_ID']
    if prev_proj_context_id is not None:
        del os.environ['DX_PROJECT_CONTEXT_ID']
    subprocess.check_call("dx clearenv", shell=True)
    try:
        yield
    finally:
        if prev_workspace_id:
            os.environ['DX_WORKSPACE_ID'] = prev_workspace_id
        if prev_proj_context_id:
            os.environ['DX_PROJECT_CONTEXT_ID'] = prev_proj_context_id


# Note: clobbers the local environment! All tests that use this should
# be marked as such with TEST_ENV
@contextmanager
def without_auth():
    """Within the scope of the block, the auth configuration variable (and
    possibly other variables) are unset.

    """
    prev_security_context = os.environ.get('DX_SECURITY_CONTEXT', None)
    if prev_security_context is not None:
        del os.environ['DX_SECURITY_CONTEXT']
    subprocess.check_call("dx clearenv", shell=True)
    try:
        yield
    finally:
        if prev_security_context:
            os.environ['DX_SECURITY_CONTEXT'] = prev_security_context


class DXTestCase(unittest.TestCase):
    if USING_PYTHON2:
        assertRegex = unittest.TestCase.assertRegexpMatches
        assertNotRegex = unittest.TestCase.assertNotRegexpMatches

    def setUp(self):
        proj_name = u"dxclient_test_pröject"
        self.project = dxpy.api.project_new({"name": proj_name})['id']
        dxpy.config["DX_PROJECT_CONTEXT_ID"] = self.project
        cd(self.project + ":/")
        dxpy.config.__init__(suppress_warning=True)
        if 'DX_CLI_WD' in dxpy.config:
            del dxpy.config['DX_CLI_WD']

    def tearDown(self):
        if "DX_USER_CONF_DIR" in os.environ:
            os.environ.pop("DX_USER_CONF_DIR")
        try:
            dxpy.api.project_destroy(self.project, {"terminateJobs": True})
        except Exception as e:
            print("Failed to remove test project:", str(e))
        if 'DX_PROJECT_CONTEXT_ID' in dxpy.config:
            del dxpy.config['DX_PROJECT_CONTEXT_ID']
        if 'DX_CLI_WD' in dxpy.config:
            del dxpy.config['DX_CLI_WD']

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
                if not re.search(stderr_regexp, e.stderr):
                    print("stderr:")
                    print(e.stderr)
                    self.fail("Expected stderr to match '%s' but it didn't" % (stderr_regexp,))
            return
        self.assertFalse(True, "Expected command to fail with CalledProcessError but it succeeded")

    def assertFileContentsEqualsString(self, path, s):
        self.assertEqual(open(os.sep.join(path)).read(), s)

    def _dictToPPJSON(self, d):
        return json.dumps(d, sort_keys=True, indent=4, separators=(',', ': '))

    def assertDictSubsetOf(self, subset_dict, containing_dict):
        mm_items = []
        mm_missing = []
        for (key, value) in subset_dict.items():
            if key in containing_dict:
                if value != containing_dict[key]:
                    mm_items.append(key)
            else:
                mm_missing.append(key)

        err_items = len(mm_items) > 0
        err_missing = len(mm_missing) > 0

        if err_items or err_missing:
            subset_json = self._dictToPPJSON(subset_dict)
            containing_json = self._dictToPPJSON(containing_dict)
            error_string = "Expected the following:\n"
            error_string += "{}\n\nto be a subset of\n\n{}\n\n".format(subset_json,
                                                                       containing_json)
            if err_items:
                m = ", ".join(map(lambda x: str(x), mm_items))
                error_string += "Field value mismatch at keys: {}\n".format(m)

            if err_missing:
                m = ", ".join(map(lambda x: str(x), mm_missing))
                error_string += "Keys missing from superset: {}\n".format(m)

            self.assertFalse(True, error_string)


class DXTestCaseBuildWorkflows(DXTestCase):
    """
    This class adds methods to ``DXTestCase`` related to workflow creation and
    workflow destruction.
    """
    base_workflow_spec = {
        "name": "my_workflow",
        "outputFolder": "/"
    }

    def setUp(self):
        super(DXTestCaseBuildWorkflows, self).setUp()
        self.temp_file_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_file_path)
        super(DXTestCaseBuildWorkflows, self).tearDown()

    def write_workflow_directory(self, workflow_name, dxworkflow_str,
                                 readme_content="Workflow doc", build_basic=False):
        # Note: if called twice with the same workflow_name, will overwrite
        # the dxworkflow.json and code file (if specified) but will not
        # remove any other files that happened to be present
        try:
            os.mkdir(os.path.join(self.temp_file_path, workflow_name))
        except OSError as e:
            if e.errno != 17:  # directory already exists
                raise e
        if dxworkflow_str is not None:
            with open(os.path.join(self.temp_file_path, workflow_name, 'dxworkflow.json'), 'wb') as manifest:
                manifest.write(dxworkflow_str.encode())
        elif build_basic:
            with open(os.path.join(self.temp_file_path, workflow_name, 'dxworkflow.json'), 'wb') as manifest:
                manifest.write(self.base_workflow_spec)
        with open(os.path.join(self.temp_file_path, workflow_name, 'Readme.md'), 'w') as readme_file:
            readme_file.write(readme_content)
        return os.path.join(self.temp_file_path, workflow_name)


class DXTestCaseBuildApps(DXTestCase):
    """
    This class adds methods to ``DXTestCase`` related to app creation,
    app destruction, and extraction of app data as local files.
    """

    base_app_spec = {
        "dxapi": "1.0.0",
        "runSpec": {"file": "code.py", "interpreter": "python2.7"},
        "inputSpec": [],
        "outputSpec": [],
        "version": "1.0.0"
    }

    def setUp(self):
        super(DXTestCaseBuildApps, self).setUp()
        self.temp_file_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_file_path)
        super(DXTestCaseBuildApps, self).tearDown()

    def make_apps(self, num_apps, name_prefix, bill_to=None):
        apps = []
        app_spec = dict(self.base_app_spec)
        for i in range(num_apps):
            app_spec["name"] = name_prefix + "_" + str(i)
            if bill_to is not None:
                app_spec["billTo"] = bill_to

            app_dir = self.write_app_directory("minimal_åpp",
                                               json.dumps(app_spec),
                                               "code.py")
            app = json.loads(run("dx build --create-app --json " + app_dir))
            apps.append(app)

        return apps

    def write_app_directory(self, app_name, dxapp_str, code_filename=None, code_content="\n"):
        # Note: if called twice with the same app_name, will overwrite
        # the dxapp.json and code file (if specified) but will not
        # remove any other files that happened to be present
        try:
            os.mkdir(os.path.join(self.temp_file_path, app_name))
        except OSError as e:
            if e.errno != 17:  # directory already exists
                raise e
        if dxapp_str is not None:
            with open(os.path.join(self.temp_file_path, app_name, 'dxapp.json'), 'wb') as manifest:
                manifest.write(dxapp_str.encode())
        if code_filename:
            with open(os.path.join(self.temp_file_path, app_name, code_filename), 'w') as code_file:
                code_file.write(code_content)
        return os.path.join(self.temp_file_path, app_name)


class TemporaryFile:
    ''' A wrapper class around a NamedTemporaryFile. Intended for use inside a 'with' statement.
        It returns a file-like object that can be opened by another process for writing, in particular
        in Windows, where the OS does not allow multiple handles to a single file. The parameter
        'close' determines if the file is returned closed or open.
    '''
    def __init__(self, mode='w+b', bufsize=-1, suffix='', prefix='tmp', dir=None, delete=True, close=False):
        self.temp_file = tempfile.NamedTemporaryFile(mode, bufsize, suffix, prefix, dir, delete=False)
        self.name = self.temp_file.name
        self.delete = delete
        if (close):
            self.temp_file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.delete:
            os.unlink(self.name)

    def write(self, buf):
        return self.temp_file.write(buf)

    def flush(self):
        return self.temp_file.flush()

    def close(self):
        return self.temp_file.close()
