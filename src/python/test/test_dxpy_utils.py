#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 DNAnexus, Inc.
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

import os, unittest, json, tempfile, subprocess, csv, shutil, re, time
from dxpy import AppError
from dxpy.utils import (describe, exec_utils, genomic_utils, response_iterator, get_futures_threadpool)

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

    def test_get_resolved_jbors(self):
        resolved_jbors = {}
        orig_thing = {"job": "job-B55ZF5kZKQGz1Xxyb5FQ0003", "field": "number"}
        resolved_thing = 32
        describe.get_resolved_jbors(resolved_thing, orig_thing, resolved_jbors)
        self.assertIn("job-B55ZF5kZKQGz1Xxyb5FQ0003:number", resolved_jbors)

        resolved_jbors = {}
        orig_thing = {"$dnanexus_link": {"job": "job-B55ZF5kZKQGz1Xxyb5FQ0003", "field": "number"}}
        resolved_thing = 32
        describe.get_resolved_jbors(resolved_thing, orig_thing, resolved_jbors)
        self.assertIn("job-B55ZF5kZKQGz1Xxyb5FQ0003:number", resolved_jbors)

class TestErrorSanitizing(unittest.TestCase):
    def test_error_sanitizing(self):
        # ASCII str
        self.assertEqual(exec_utils._safe_unicode(ValueError("foo")), "foo")
        # UTF-8 encoded str
        self.assertEqual(exec_utils._safe_unicode(ValueError(u"crème".encode("utf-8"))), u"cr\xe8me")
        # Unicode obj
        self.assertEqual(exec_utils._safe_unicode(ValueError(u"brûlée")), u"br\xfbl\xe9e")
        # Not UTF-8
        self.assertEqual(exec_utils._safe_unicode(ValueError(u"Invalid read name: DÑÁnèxûs".encode("ISO-8859-1"))),
                         "Invalid read name: D??n?x?s [Raw error message: 496e76616c69642072656164206e616d653a2044d1c16ee878fb73]")

    def test_formatting_exceptions(self):
        self.assertEqual(exec_utils._format_exception_message(ValueError("foo")), "ValueError: foo")
        self.assertEqual(exec_utils._format_exception_message(AppError("foo")), "foo")

class TestGenomicUtils(unittest.TestCase):
    def test_reverse_complement(self):
        self.assertEqual("TTTTAAACCG", genomic_utils.reverse_complement("CGGTTTAAAA"))
        self.assertEqual("TTTTAAACCG", genomic_utils.reverse_complement(u"CGGTTTAAAA"))
        self.assertEqual("TTTTAAACCG", genomic_utils.reverse_complement("cggtttaaaa"))
        self.assertEqual("TTTTAAACCG", genomic_utils.reverse_complement(u"cggtttaaaa"))
        self.assertEqual("NNNNNTTTTAAACCG", genomic_utils.reverse_complement("CGGTTTAAAANNNNN"))
        with self.assertRaises(ValueError):
            genomic_utils.reverse_complement("oops")

class TestResponseIterator(unittest.TestCase):
    def test_basic_iteration(self):
        def task(i, sleep_for=1):
            print "Task", i, "sleeping for", sleep_for
            time.sleep(sleep_for)
            return i

        def tasks():
            for i in range(8):
                yield task, [i], {"sleep_for": i/8.0}

        for i, res in enumerate(response_iterator(tasks(), get_futures_threadpool(3))):
            self.assertEqual(i, res)
        for i, res in enumerate(response_iterator(tasks(), get_futures_threadpool(5))):
            self.assertEqual(i, res)
        for i, res in enumerate(response_iterator(tasks(), get_futures_threadpool(5), max_active_tasks=2)):
            self.assertEqual(i, res)
        for i, res in enumerate(response_iterator(tasks(), get_futures_threadpool(5), max_active_tasks=6)):
            self.assertEqual(i, res)

        def tasks2():
            for i in range(8):
                yield task, [i], {"sleep_for": (8-i)/8.0}

        for i, res in enumerate(response_iterator(tasks2(), get_futures_threadpool(5), num_retries=2, retry_after=0.1)):
            self.assertEqual(i, res)


if __name__ == '__main__':
    unittest.main()
