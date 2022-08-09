#!/usr/bin/env python
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
import os
# from dxpy_testutil import (DXTestCase, run)
from dxpy.compat import str
from datetime import datetime
import uuid
import unittest
import json
import shutil
import subprocess
import locale

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
    output = output.decode(locale.getpreferredencoding())
    err = err.decode(locale.getpreferredencoding())
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

def run(command, **kwargs):
    print("$ %s" % (command,))
    output = check_output(command, shell=True, **kwargs)
    print(output)
    return output

def build_nextflow_applet(app_dir, project_id):
    updated_app_dir = app_dir + str(uuid.uuid1())
    # updated_app_dir = os.path.abspath(os.path.join(tempdir, os.path.basename(app_dir)))
    # shutil.copytree(app_dir, updated_app_dir)

    build_output = run(['dx', 'build', '--nextflow', './nextflow'])
    return json.loads(build_output)['id']

class TestNextflow(unittest.TestCase):
    def test_temp(self):
        print("test-message")
        assert False


    def test_basic_hello(self):
        applet = build_nextflow_applet("./nextflow/", "project-GFYvg4Q0469VKVVVP359Yfpp")
        print(applet)
        self.assertFalse(True, "Expected command to fail with CalledProcessError but it succeeded")
