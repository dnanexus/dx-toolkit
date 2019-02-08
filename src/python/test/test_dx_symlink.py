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

import re
import unittest

import dxpy
from dxpy.utils import describe

def setUpTempProject(thing):
    thing.old_workspace_id = dxpy.WORKSPACE_ID
    thing.proj_id = dxpy.api.project_new({'name': 'symlink test project'})['id']
    dxpy.set_workspace_id(thing.proj_id)

def tearDownTempProject(thing):
    dxpy.api.project_destroy(thing.proj_id, {'terminateJobs': True})
    dxpy.set_workspace_id(thing.old_workspace_id)

def create_symlink(filename, proj_id, url):
    input_params = {
        'name' : filename,
        'project': proj_id,
        'drive': "drive-PUBLISHED",
        'md5sum': "00000000000000000000000000000000",
        'symlinkPath': {
            'object': url
        }
    }
    result = dxpy.api.file_new(input_params=input_params)
    return dxpy.DXFile(dxid = result["id"],
                       project = proj_id)



class TestSymlink(unittest.TestCase):
#    def setUp(self):
#        setUpTempProject(self)

#    def tearDown(self):
#        tearDownTempProject(self)

    # create a symbolic link
    # download it, see that it works
    def test_symlink(self):
        print("test_symlink")
        dxfile = create_symlink(
            "sym1",
            "project-FGpfqjQ0ffPF1Q106JYP2j3v",
            "https://s3.amazonaws.com/1000genomes/CHANGELOG")
        print(dxfile)
        desc = dxfile.describe()
        print(desc)


if __name__ == '__main__':
    unittest.main()
