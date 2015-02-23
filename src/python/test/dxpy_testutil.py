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

from __future__ import print_function, unicode_literals, division, absolute_import

import os, sys, unittest, subprocess, re, platform
from contextlib import contextmanager

import dxpy
from dxpy.compat import str

_run_all_tests = 'DXTEST_FULL' in os.environ
TEST_CREATE_APPS = _run_all_tests or 'DXTEST_CREATE_APPS' in os.environ
TEST_ENV = _run_all_tests or 'DXTEST_ENV' in os.environ
TEST_FUSE = _run_all_tests or 'DXTEST_FUSE' in os.environ
TEST_HTTP_PROXY = _run_all_tests or 'DXTEST_HTTP_PROXY' in os.environ
TEST_NO_RATE_LIMITS = _run_all_tests or 'DXTEST_NO_RATE_LIMITS' in os.environ
TEST_RUN_JOBS = _run_all_tests or 'DXTEST_RUN_JOBS' in os.environ
TEST_TCSH = _run_all_tests or 'DXTEST_TCSH' in os.environ

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

    Unlike subprocess.check_output, unconditionally decodes the contents of the subprocess stdout and stderr using
    sys.stdin.encoding.
    """
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    if 'stderr' in kwargs:
        raise ValueError('stderr argument not allowed, it will be overridden.')
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
    return output

@contextmanager
def temporary_project(name='dx client tests temporary project', cleanup=True, reclaim_permissions=False, select=False):
    """Creates a temporary project scoped to the context manager, and
    yields a DXProject handler for the project.

    :param cleanup: if False, do not clean up the project when done (useful for debugging so you can examine the state of the project)
    :type cleanup: bool
    :param reclaim_permissions: if True, attempts a project-xxxx/join before trying to destroy the project. May be needed if the test reduced its own permissions in the project.
    :type reclaim_permissions: bool
    :param select:
        if True, sets the environment variable DX_PROJECT_CONTEXT_ID
        (and restores the previous value afterwards) so that subprocess
        calls made within the block use the new project by default.
    :type select: bool

    """
    temp_project = dxpy.DXProject(dxpy.api.project_new({'name': name})['id'])
    try:
        if select:
            with select_project(temp_project):
                yield temp_project
        else:
            yield temp_project
    finally:
        if reclaim_permissions:
            dxpy.DXHTTPRequest('/' + temp_project.get_id() + '/join', {'level': 'ADMINISTER'})
        if cleanup:
            dxpy.api.project_destroy(temp_project.get_id(), {"terminateJobs": True})


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


class DXTestCase(unittest.TestCase):
    def setUp(self):
        proj_name = u"dxclient_test_pröject"
        self.project = dxpy.api.project_new({"name": proj_name})['id']
        dxpy.config["DX_PROJECT_CONTEXT_ID"] = self.project
        cd(self.project + ":/")
        dxpy.config.__init__(suppress_warning=True)
        if 'DX_CLI_WD' in dxpy.config:
            del dxpy.config['DX_CLI_WD']

    def tearDown(self):
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
