#!/usr/bin/env python
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
import subprocess
import tempfile
import unittest

import dxpy
from dxpy.exceptions import err_exit
from dxpy.utils import describe
from dxpy_testutil import (chdir, run)

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

# create a symbolic link on the platform
def create_symlink(filename, proj_id, url, checksum):
    input_params = {
        'name' : filename,
        'project': proj_id,
        'drive': "drive-PUBLISHED",
        'md5sum': checksum,
        'symlinkPath': {
            'object': url
        }
    }
    result = dxpy.api.file_new(input_params=input_params)
    return dxpy.DXFile(dxid = result["id"],
                       project = proj_id)


# create a symbolic link
# download it, see that it works
def download_url_create_symlink(url, proj_id):
    print("url = {}".format(url))
    with chdir(tempfile.mkdtemp()):
        tmp_file = "localfile"
        # download [url]
        cmd = ["wget", "--tries=5", "--quiet", "-O", tmp_file, url]

        try:
            print("Downloading original link with wget")
            subprocess.check_call(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            msg = ""
            if e and e.output:
                msg = e.output.strip()
            err_exit("Failed to download with wget: {cmd}\n{msg}\n".format(cmd=str(cmd), msg=msg))

        # calculate its md5 checksum
        chksum = md5_checksum(tmp_file)

        # create a symlink on the platform, with the correct checksum
        dxfile = create_symlink(
            "sym1",
            proj_id,
            url,
            chksum)

        # download it (this will verify the checksum)
        print("downloading locally")
        dxpy.download_dxfile(dxfile.get_id(), tmp_file)


class TestSymlink(unittest.TestCase):
    def setUp(self):
        setUpTempProject(self)

    def tearDown(self):
        tearDownTempProject(self)

    def test_symlinks(self):
        download_url_create_symlink("https://s3.amazonaws.com/1000genomes/CHANGELOG",
                                    self.proj_id)
        download_url_create_symlink("https://wiki.dnanexus.com/Home",
                                    self.proj_id)
        download_url_create_symlink("https://www.gutenberg.org",
                                    self.proj_id)

    def test_downloads():
        run("dx download {} -o {}".format(symlink, localfile))

if __name__ == '__main__':
    unittest.main()
