#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2013-2019 DNAnexus, Inc.
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
import re
import shutil
import subprocess
import tempfile
import unittest

import dxpy
from dxpy.exceptions import err_exit
from dxpy.utils import describe
from dxpy_testutil import (chdir, run, TEST_ISOLATED_ENV)

def setUpTempProject(thing):
    thing.old_workspace_id = dxpy.WORKSPACE_ID
    thing.proj_id = dxpy.api.project_new({'name': 'symlink test project'})['id']
    dxpy.set_workspace_id(thing.proj_id)

def tearDownTempProject(thing):
    dxpy.api.project_destroy(thing.proj_id, {'terminateJobs': True})
    dxpy.set_workspace_id(thing.old_workspace_id)


# Check if a program (wget, curl, etc.) is on the path, and
# can be called.
def which(program):
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    for path in os.environ["PATH"].split(os.pathsep):
        exe_file = os.path.join(path, program)
        if is_exe(exe_file):
            return exe_file
    return None

# calculate the checksum of a local file. Return 'bytes'.
def md5_checksum(filename):
    md5sum_exe = which("md5sum")
    if md5sum_exe is None:
        err_exit("md5sum is not installed on this system")
    cmd = [md5sum_exe, "-b", filename]
    try:
        print("Calculating checksum")
        cmd_out = subprocess.check_output(cmd)
    except subprocess.CalledProcessError:
        err_exit("Failed to run md5sum: " + str(cmd))

    line = cmd_out.strip().split()
    if len(line) != 2:
        err_exit("md5sum returned weird results: " + str(line))
    result = line[0]

    # convert to string
    assert(isinstance(result, bytes))
    result = result.decode("ascii")
    return result

if __name__ == '__main__':
    unittest.main()
