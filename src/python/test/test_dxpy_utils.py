#!/usr/bin/env python
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

import unittest, time, json, re, os
import dxpy
from dxpy import AppError, AppInternalError, DXFile, DXRecord
from dxpy.utils import (describe, exec_utils, genomic_utils, response_iterator, get_futures_threadpool, DXJSONEncoder,
                        normalize_timedelta, normalize_time_input, config)
from dxpy.utils.exec_utils import DXExecDependencyInstaller
from dxpy.compat import USING_PYTHON2

# TODO: unit tests for dxpy.utils.get_field_from_jbor, get_job_from_jbor, is_job_ref

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

class TestErrorSanitizing(unittest.TestCase):
    def test_error_sanitizing(self):
        # ASCII str
        self.assertEqual(exec_utils._safe_unicode(ValueError("foo")), "foo")
        # UTF-8 encoded str
        self.assertEqual(exec_utils._safe_unicode(ValueError("crème".encode("utf-8"))),
                         "cr\xe8me" if USING_PYTHON2 else "b'cr\\xc3\\xa8me'")
        # Unicode obj
        self.assertEqual(exec_utils._safe_unicode(ValueError("brûlée")), "br\xfbl\xe9e")
        # Not UTF-8
        if USING_PYTHON2:
            expected = "Invalid read name: D??n?x?s [Raw error message: 496e76616c69642072656164206e616d653a2044d1c16ee878fb73]"
        else:
            expected = "b'Invalid read name: D\\xd1\\xc1n\\xe8x\\xfbs'"
        self.assertEqual(exec_utils._safe_unicode(ValueError("Invalid read name: DÑÁnèxûs".encode("ISO-8859-1"))), expected)

    def test_formatting_exceptions(self):
        self.assertEqual(exec_utils._format_exception_message(ValueError("foo")), "ValueError: foo")
        self.assertEqual(exec_utils._format_exception_message(AppError("foo")), "foo")

class TestGenomicUtils(unittest.TestCase):
    def test_reverse_complement(self):
        self.assertEqual(b"TTTTAAACCG", genomic_utils.reverse_complement(b"CGGTTTAAAA"))
        self.assertEqual(b"TTTTAAACCG", genomic_utils.reverse_complement("CGGTTTAAAA"))
        self.assertEqual(b"TTTTAAACCG", genomic_utils.reverse_complement(b"cggtttaaaa"))
        self.assertEqual(b"TTTTAAACCG", genomic_utils.reverse_complement("cggtttaaaa"))
        self.assertEqual(b"NNNNNTTTTAAACCG", genomic_utils.reverse_complement(b"CGGTTTAAAANNNNN"))
        self.assertEqual(b"NNNNNTTTTAAACCG", genomic_utils.reverse_complement("CGGTTTAAAANNNNN"))
        with self.assertRaises(ValueError):
            genomic_utils.reverse_complement("oops")

class TestResponseIterator(unittest.TestCase):
    def test_basic_iteration(self):
        def task(i, sleep_for=1):
            print("Task", i, "sleeping for", sleep_for)
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

class TestDXUtils(unittest.TestCase):
    def test_dxjsonencoder(self):
        f = DXFile("file-" + "x"*24, project="project-" + "y"*24)
        r = DXRecord("record-" + "r"*24, project="project-" + "y"*24)
        data = {"a": [{"b": f}, r]}
        serialized = json.dumps(data, cls=DXJSONEncoder)
        self.assertEqual(serialized,
                         '{"a": [{"b": {"$dnanexus_link": "file-xxxxxxxxxxxxxxxxxxxxxxxx"}}, {"$dnanexus_link": "record-rrrrrrrrrrrrrrrrrrrrrrrr"}]}')

class TestEDI(DXExecDependencyInstaller):
    def __init__(self, *args, **kwargs):
        self.command_log, self.message_log = [], []
        DXExecDependencyInstaller.__init__(self, *args, **kwargs)

    def run(self, cmd, **kwargs):
        self.command_log.append(cmd)

    def log(self, message, **kwargs):
        self.message_log.append(message)

class TestDXExecDependsUtils(unittest.TestCase):
    def test_dx_execdepends_installer(self):
        def get_edi(run_spec, job_desc=None):
            return TestEDI(executable_desc={"runSpec": run_spec}, job_desc=job_desc if job_desc else {})

        def assert_cmd_ran(edi, regexp):
            self.assertRegexpMatches("\n".join(edi.command_log), regexp)

        def assert_log_contains(edi, regexp):
            self.assertRegexpMatches("\n".join(edi.message_log), regexp)

        with self.assertRaisesRegexp(AppInternalError, 'Expected field "runSpec" to be present'):
            DXExecDependencyInstaller({}, {})

        with self.assertRaisesRegexp(AppInternalError, 'Expected field "name" to be present'):
            get_edi({"dependencies": [{"foo": "bar"}]})

        edi = get_edi({"dependencies": [{"name": "foo", "package_manager": "cran", "version": "1.2.3"}]})
        edi.install()
        assert_cmd_ran(edi, "R -e .+ install.packages.+devtools.+install_version.+foo.+version.+1.2.3")

        with self.assertRaisesRegexp(AppInternalError, 'does not have a "url" field'):
            get_edi({"dependencies": [{"name": "foo", "package_manager": "git"}]})

        edi = get_edi({"execDepends": [{"name": "git"}], "dependencies": [{"name": "tmux"}]})
        edi.install()
        assert_cmd_ran(edi, "apt-get install --yes --no-install-recommends git tmux")

        edi = get_edi({})
        edi.install()

        edi = get_edi({"execDepends": [], "bundledDepends": [], "dependencies": []})
        edi.install()

        edi = get_edi({"dependencies": [{"name": "pytz", "package_manager": "pip", "version": "2014.7"},
                                        {"name": "certifi", "package_manager": "pip", "stages": ["main"]},
                                        {"name": "tmux", "package_manager": "apt"},
                                        {"name": "rake", "package_manager": "gem", "version": "10.3.2"},
                                        {"name": "nokogiri", "package_manager": "gem", "stages": ["main"]},
                                        {"name": "Module::Provision", "package_manager": "cpan", "version": "0.36.1"},
                                        {"name": "LWP::MediaTypes", "package_manager": "cpan"},
                                        {"name": "RJSONIO", "package_manager": "cran"},
                                        {"name": "plyr", "package_manager": "cran", "version": "1.8.1"},
                                        {"name": "ggplot2", "package_manager": "cran", "stages": ["main"],
                                         "version": "1.0.1"},
                                        {"name": "r1", "id": {"$dnanexus_link": "record-123"}},
                                        {"name": "g1",
                                         "package_manager": "git",
                                         "url": "https://github.com/dnanexus/oauth2-demo"},
                                        {"name": "g2",
                                         "package_manager": "git",
                                         "url": "https://github.com/dnanexus/bwa",
                                         "tag": "production",
                                         "destdir": "/tmp/ee-edi-test-bwa",
                                         "buld_commands": "echo build bwa here",
                                         "stages": ["main"]}]})
        edi.install()
        assert_cmd_ran(edi, re.escape("pip install --upgrade pytz==2014.7 certifi"))
        assert_cmd_ran(edi, "apt-get install --yes --no-install-recommends tmux")
        assert_cmd_ran(edi, re.escape("gem install rake --version 10.3.2 && gem install nokogiri"))
        assert_cmd_ran(edi, "R -e .+ install.packages.+\"RJSONIO\".+install_version.+\"ggplot2\".+version=\"1.0.1\"")
        assert_log_contains(edi, 'Skipping bundled dependency "r1" because it does not refer to a file')
        assert_cmd_ran(edi, re.escape("cd $(mktemp -d) && git clone https://github.com/dnanexus/oauth2-demo"))
        assert_cmd_ran(edi, "cd /tmp/ee-edi-test-bwa && git clone https://github.com/dnanexus/bwa")
        assert_cmd_ran(edi, "git checkout production")

        edi = get_edi({"execDepends": [{"name": "w00t", "stages": ["foo", "bar"]},
                                       {"name": "f1", "id": {"$dnanexus_link": "file-123"}, "stages": ["xyzzt"]}]})
        edi.install()
        self.assertNotRegexpMatches("\n".join(edi.command_log), "w00t")
        for name in "w00t", "f1":
            assert_log_contains(edi,
                                "Skipping dependency {} because it is inactive in stage \(function\) main".format(name))

        edi = get_edi({"execDepends": [{"name": "git", "stages": ["foo", "bar"]}]},
                      job_desc = {"function": "foo"})
        edi.install()
        assert_cmd_ran(edi, "apt-get install --yes --no-install-recommends git")


class TestTimeUtils(unittest.TestCase):
    def test_normalize_timedelta(self):
        for i, o in (("-15", -15000),
                     ("15", 15000),
                     ("15s", 15000),
                     ("0", 0),
                     ("0w", 0),
                     ("1m", 1000*60),
                     ("1M", 1000*60*60*24*30),
                     ("-1w", -1000*60*60*24*7)):
            self.assertEqual(normalize_timedelta(i), o)

    def test_strings_vs_ints(self):
        # This is potentially confusing; it would be nice to have two
        # separate methods, or a flag to control the interpretation of
        # the field when a string with no suffix is supplied.

        # "dx login" can supply this form
        self.assertEqual(normalize_time_input("1414141414"), 1414141414000)   # interpreted as sec
        # find methods can supply this form
        self.assertEqual(normalize_time_input(1414141414000), 1414141414000)  # interpreted as ms


class TestDXConfig(unittest.TestCase):
    def test_dxconfig(self):
        environ_backup = os.environ.copy()
        try:
            c = config.DXConfig()
            for var in c.VAR_NAMES:
                value = '{"foo": "bar"}'
                c[var] = value
                if var in c.CORE_VAR_NAMES - {"DX_SECURITY_CONTEXT", "DX_WORKSPACE_ID"}:
                    self.assertEqual(getattr(dxpy, var.lstrip("DX_")), value)
                elif var == "DX_SECURITY_CONTEXT":
                    self.assertEqual(json.dumps(dxpy.SECURITY_CONTEXT), value)
                c.write(var, None)
                del c[var]
            c.update(DX_CLI_WD="/wd")
            self.assertIn("DX_CLI_WD", c)
            self.assertEqual(c["DX_CLI_WD"], "/wd")
            self.assertEqual(c.pop("DX_CLI_WD"), "/wd")
            self.assertEqual(c.pop("DX_CLI_WD", None), None)
            self.assertIn("DXConfig object at", repr(c))
            c.update(c.defaults)
            self.assertEqual(len(c), len(list(c)))
            del c[c.defaults.keys()[0]]
            self.assertEqual(len(c), len(list(c)))
            dxpy.config["DX_PROJECT_CONTEXT_NAME"] = None
            self.assertEqual(os.environ["DX_PROJECT_CONTEXT_NAME"], "")
        finally:
            os.environ.update(environ_backup)
            dxpy.config.__init__(suppress_warning=True)

if __name__ == '__main__':
    unittest.main()
