#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2016 DNAnexus, Inc.
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
import unittest
import tempfile
import shutil
import subprocess
from dxpy_testutil import (DXTestCase)
from dxpy_testutil import chdir


class TestDXUnpack(DXTestCase):
    def test_file_name_with_special_chars_locally(self):
        # create a tar.gz file with spaces, quotes and escape chars in its name
        bundle_name = "test 'bundle' \"with\" \"@#^&%()[]{}\" spaces.tar.gz"
        bundle_tmp_dir = tempfile.mkdtemp()
        os.mkdir(os.path.join(bundle_tmp_dir, "a"))
        with open(os.path.join(bundle_tmp_dir, 'a', 'foo.txt'), 'w') as file_in_bundle:
            file_in_bundle.write('foo\n')
        subprocess.check_call(['tar', '-czf', os.path.join(bundle_tmp_dir, bundle_name),
                               '-C', os.path.join(bundle_tmp_dir, 'a'), '.'])
        extract_tmp_dir = tempfile.mkdtemp()
        with chdir(extract_tmp_dir):
            subprocess.check_call(["dx-unpack", os.path.join(bundle_tmp_dir, bundle_name)])
            self.assertTrue(os.path.exists(os.path.join(extract_tmp_dir, 'foo.txt')))

    def test_remove_file_after_unpack(self):
        # dx-unpack removes the file after unpacking
        bundle_name = "tarball.tar.gz"
        bundle_tmp_dir = tempfile.mkdtemp()
        os.mkdir(os.path.join(bundle_tmp_dir, "a"))
        with open(os.path.join(bundle_tmp_dir, 'a', 'foo.txt'), 'w') as file_in_bundle:
            file_in_bundle.write('foo\n')
        subprocess.check_call(['tar', '-czf', os.path.join(bundle_tmp_dir, bundle_name),
                               '-C', os.path.join(bundle_tmp_dir, 'a'), '.'])
        extract_tmp_dir = tempfile.mkdtemp()
        with chdir(extract_tmp_dir):
            subprocess.check_call(["dx-unpack", os.path.join(bundle_tmp_dir, bundle_name)])
            self.assertTrue(os.path.exists(os.path.join(extract_tmp_dir, 'foo.txt')))
            shutil.rmtree(os.path.join(bundle_tmp_dir, "a"))
            self.assertFalse(os.path.exists(os.path.join(bundle_tmp_dir, bundle_name)))

if __name__ == '__main__':
    unittest.main()
