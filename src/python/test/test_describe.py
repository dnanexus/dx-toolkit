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

import re
import unittest

from dxpy.utils import describe


ansi_escape = re.compile(r'\x1b[^m]*m')


def strip_ansi_escape_sequences(text):
    return ansi_escape.sub('', text)


class TestDescribe(unittest.TestCase):
    def test_is_job_ref(self):
        # Positive results
        jobref = {"job": "job-B55ZF5kZKQGz1Xxyb5FQ0003", "field": "number"}
        self.assertTrue(describe.is_job_ref(jobref))
        jobref = {"$dnanexus_link": jobref}
        self.assertTrue(describe.is_job_ref(jobref))

        # Negative results
        jobref = {"job": "job-B55ZF5kZKQGz1Xxyb5FQ0003", "field": "number", "other": "field"}
        self.assertFalse(describe.is_job_ref(jobref))
        jobref = {"job": "job-B55ZF5kZKQGz1Xxyb5FQ0003", "field": 32}
        self.assertFalse(describe.is_job_ref(jobref))
        jobref = {"$dnanexus_link": jobref}
        self.assertFalse(describe.is_job_ref(jobref))
        jobref = {"$dnanexus_link": "job-B55ZF5kZKQGz1Xxyb5FQ0003"}
        self.assertFalse(describe.is_job_ref(jobref))

    def test_ls_l_formatting(self):
        file_describe = {
            "state": "closed",
            "folder": "/",
            "name": "foo",
            "class": "file",
            "modified": 1482430288965,
            "project": "project-111122223333444455556666",
            "id": "file-222233334444555566667777"
        }

        # Make sure that for a variety of sizes, the formatting of the size
        # column is consistent (and doesn't cause subsequent columns to be
        # misaligned)

        # Strip ANSI escape sequences so this test "sees" the column positions
        # as they would be displayed on the screen
        header = strip_ansi_escape_sequences(describe.get_ls_l_header())
        name_col = header.index("Name (ID)")
        self.assertTrue(name_col > 0)

        for i in range(0, 25):
            line = strip_ansi_escape_sequences(describe.get_ls_l_desc(dict(file_describe, size=4 ** i)))
            self.assertEqual(name_col, line.index("foo (file-"))


if __name__ == '__main__':
    unittest.main()
