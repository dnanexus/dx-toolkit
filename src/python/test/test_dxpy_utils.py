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

import unittest, time, json, re, os
import dateutil.parser
import dxpy
from dxpy import AppError, AppInternalError, DXError, DXFile, DXRecord
from dxpy.utils import (exec_utils, genomic_utils, response_iterator, get_futures_threadpool, DXJSONEncoder,
                        normalize_timedelta, normalize_time_input, config, Nonce)
from dxpy.utils.exec_utils import DXExecDependencyInstaller
from dxpy.utils.pretty_print import flatten_json_array
from dxpy.compat import USING_PYTHON2
import dxpy_testutil as testutil
from dxpy.system_requirements import SystemRequirementsDict

# TODO: unit tests for dxpy.utils.get_field_from_jbor, get_job_from_jbor, is_job_ref


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

    def test_first_chunk_sequentially(self):
        pending_tasks = set()
        complete_tasks = set()

        def task(i):
            pending_tasks.add(i)
            if i >= 1:
                self.assertNotIn(0, pending_tasks, "Task 0 was pending during task %d" % i)
            time.sleep(1)
            complete_tasks.add(i)
            pending_tasks.remove(i)

        def tasks():
            for i in range(8):
                yield task, [i], {}

        #
        # |---0---|
        #           |---1---|
        #           |---2---|
        #           |---3---|
        #
        # Assert that 0 doesn't overlap with 1, 2, or 3
        list(response_iterator(tasks(), get_futures_threadpool(8), do_first_task_sequentially=True))


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

class TestDXExecDependsUtils(testutil.DXTestCaseCompat):
    def get_edi(self, run_spec, job_desc=None):
        return TestEDI(executable_desc={"runSpec": run_spec}, job_desc=job_desc if job_desc else {})

    def test_install_bundled_dependencies(self):
        bundled_depends_by_region = {
            "aws:us-east-1": [
                {"name": "asset.east", "id": {"$dnanexus_link": "file-asseteast"}}
            ],

            "azure:westus": [
                {"name": "asset.west", "id": {"$dnanexus_link": "file-assetwest"}}
            ]
        }
        edi = self.get_edi({"bundledDependsByRegion": bundled_depends_by_region},
                           job_desc={"region": "aws:us-east-1"})
        with self.assertRaisesRegex(DXError, 'file-asseteast'):
            # Asserts that we attempted to download the correct file.
            edi.install()
        edi = self.get_edi({"bundledDependsByRegion": bundled_depends_by_region},
                           job_desc={"region": "azure:westus"})
        with self.assertRaisesRegex(DXError, 'file-assetwest'):
            edi.install()
        with self.assertRaisesRegex(KeyError, 'aws:eu-central-1'):
            self.get_edi({"bundledDependsByRegion": bundled_depends_by_region},
                         job_desc={"region": "aws:eu-central-1"})

    def test_dx_execdepends_installer(self):
        def assert_cmd_ran(edi, regexp):
            self.assertRegex("\n".join(edi.command_log), regexp)

        def assert_log_contains(edi, regexp):
            self.assertRegex("\n".join(edi.message_log), regexp)

        with self.assertRaisesRegex(AppInternalError, 'Expected field "runSpec" to be present'):
            DXExecDependencyInstaller({}, {})

        with self.assertRaisesRegex(AppInternalError, 'Expected field "name" to be present'):
            self.get_edi({"dependencies": [{"foo": "bar"}]})

        edi = self.get_edi({"dependencies": [{"name": "foo", "package_manager": "cran", "version": "1.2.3"}]})
        edi.install()
        assert_cmd_ran(edi, "R -e .+ install.packages.+devtools.+install_version.+foo.+version.+1.2.3")

        with self.assertRaisesRegex(AppInternalError, 'does not have a "url" field'):
            self.get_edi({"dependencies": [{"name": "foo", "package_manager": "git"}]})

        edi = self.get_edi({"execDepends": [{"name": "git"}], "dependencies": [{"name": "tmux"}]})
        edi.install()
        assert_cmd_ran(edi, "apt-get install --yes --no-install-recommends git tmux")

        edi = self.get_edi({"execDepends": [], "bundledDepends": [], "dependencies": []})
        edi.install()

        edi = self.get_edi({"dependencies": [{"name": "pytz", "package_manager": "pip", "version": "2014.7"},
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

        edi = self.get_edi({"execDepends": [{"name": "w00t", "stages": ["foo", "bar"]},
                                            {"name": "f1", "id": {"$dnanexus_link": "file-123"}, "stages": ["xyzzt"]}]})
        edi.install()
        self.assertNotRegex("\n".join(edi.command_log), "w00t")
        for name in "w00t", "f1":
            assert_log_contains(edi,
                                "Skipping dependency {} because it is inactive in stage \(function\) main".format(name))

        edi = self.get_edi({"execDepends": [{"name": "git", "stages": ["foo", "bar"]}]},
                           job_desc={"function": "foo"})
        edi.install()
        assert_cmd_ran(edi, "apt-get install --yes --no-install-recommends git")

        # Job describe dict must contain "region" if the run specification
        # contains "bundledDependsByRegion".
        bundled_depends_by_region = {
            "aws:us-east-1": [
                {"name": "asset.east", "id": {"$dnanexus_link": "file-asseteast"}}
            ]
        }
        with self.assertRaisesRegex(DXError, 'region.*job description'):
            self.get_edi({"bundledDependsByRegion": bundled_depends_by_region})

        bundled_depends_by_region = {
            "aws:us-east-1": []
        }
        with self.assertRaisesRegex(DXError, 'region.*job description'):
            self.get_edi({"bundledDependsByRegion": bundled_depends_by_region})

        # Job describe dict may specify or omit "region" if
        # "bundledDependsByRegion" is not in run specification.
        edi = self.get_edi({}, job_desc={"region": "azure:westus"})
        edi.install()

        edi = self.get_edi({}, job_desc={})
        edi.install()


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
        self.assertEqual(normalize_time_input("1414141414", default_unit='s'), 1414141414000)   # interpreted as sec
        # "dx find ... --created-*" can supply this form
        self.assertEqual(normalize_time_input("1234567890"), 1234567890)  # interpreted as ms
        self.assertEqual(normalize_time_input(1414141414000), 1414141414000)  # interpreted as ms

    def test_normalize_time_input(self):
        # TODO: Add tests for negative time inputs e.g. "-12345", -12345, "-5d"

        for i, o in ((12345678, 12345678),
                     ("0", 0),
                     ("12345678", 12345678),
                     ("15s", 15 * 1000),
                     ("1d", (24 * 60 * 60 * 1000)),
                     ("0w", 0),
                     ("2015-10-01", int(time.mktime(dateutil.parser.parse("2015-10-01").timetuple()) * 1000))):
            self.assertEqual(normalize_time_input(i), o)

        # Test default_unit='s'
        for i, o in ((12345678, 12345678 * 1000),
                     ("12345678", 12345678 * 1000),
                     ("15s", 15 * 1000),
                     ("1d", (24 * 60 * 60 * 1000)),
                     ("0w", 0),
                     ("2015-10-01", int(time.mktime(dateutil.parser.parse("2015-10-01").timetuple()) * 1000))):
            self.assertEqual(normalize_time_input(i, default_unit='s'), o)

        with self.assertRaises(ValueError):
            normalize_time_input("1223*")
        with self.assertRaises(ValueError):
            normalize_time_input("12345", default_unit='h')
        with self.assertRaises(ValueError):
            normalize_time_input(12345, default_unit='h')
        with self.assertRaises(ValueError):
            normalize_time_input("1234.5678")
        with self.assertRaises(ValueError):
            normalize_time_input(1234.5678)


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
            del c[list(c.defaults.keys())[0]]
            self.assertEqual(len(c), len(list(c)))
            dxpy.config["DX_PROJECT_CONTEXT_NAME"] = None
            self.assertEqual(os.environ["DX_PROJECT_CONTEXT_NAME"], "")
        finally:
            os.environ.update(environ_backup)
            dxpy.config.__init__(suppress_warning=True)

class TestPrettyPrint(unittest.TestCase):
    def test_flatten_json_array(self):
        json_string = (
            '{\n'
            '  "arr": [\n'
            '    "one",\n'
            '    2,\n'
            '    3.4,\n'
            '    "\\"five\\""\n'
            '  ],\n'
            '  "foo": {\n'
            '    "arr": [\n'
            '      "six",\n'
            '      7.8,\n'
            '      9,\n'
            '      "t\\"en"\n'
            '    ]\n'
            '  }\n'
            '}'
        )
        flattened_json_string_ref = (
            '{\n'
            '  "arr": ["one", 2, 3.4, "\\"five\\""],\n'
            '  "foo": {\n'
            '    "arr": ["six", 7.8, 9, "t\\"en"]\n'
            '  }\n'
            '}'
        )
        flatten_json_array(json_string, "arr")
        self.assertEqual(flattened_json_string_ref, flatten_json_array(json_string, "arr"))


class TestNonceGeneration(unittest.TestCase):
    def test_nonce_generator(self):
        nonce_list = []
        for i in range(0, 100):
            nonce_list.append(str(Nonce()))

        for nonce in nonce_list:
            self.assertTrue(len(nonce) > 0)
            self.assertTrue(len(nonce) <= 128)
            self.assertEqual(nonce_list.count(nonce), 1)

    def test_input_updater(self):
        input_params = {"p1": "v1", "p2": "v2"}
        updated_input = Nonce.update_nonce(input_params)
        self.assertIn("nonce", updated_input)

        nonce = str(Nonce())
        input_params.update({"nonce": nonce})
        updated_input = Nonce.update_nonce(input_params)
        self.assertIn("nonce", updated_input)
        self.assertEqual(nonce, updated_input["nonce"])

class TestSystemRequirementsDict(unittest.TestCase):

    def test_add(self):
        d1 = {'a': {'x': 'pqr'}}
        d2 = {'a': {'y': 'lmn'}, 'b': {'y': 'rst'}}
        srd1 = SystemRequirementsDict(d1)
        srd2 = SystemRequirementsDict(d2)
        added = srd1 + srd2
        expected = SystemRequirementsDict({'a': {'x': 'pqr', 'y': 'lmn'}, 'b': {'y': 'rst'}})
        self.assertDictEqual(expected.entrypoints, added.entrypoints)

        d1 = {}
        d2 = {'a': {'y': 'lmn'}, 'b': {'y': 'rst'}}
        srd1 = SystemRequirementsDict(d1)
        srd2 = SystemRequirementsDict(d2)
        added = srd1 + srd2
        expected = SystemRequirementsDict({'a': {'y': 'lmn'}, 'b': {'y': 'rst'}})
        self.assertDictEqual(expected.entrypoints, added.entrypoints)

        d1 = None
        d2 = {'a': {'y': 'lmn'}, 'b': {'y': 'rst'}}
        srd1 = SystemRequirementsDict(d1)
        srd2 = SystemRequirementsDict(d2)
        added = srd1 + srd2
        expected = SystemRequirementsDict({'a': {'y': 'lmn'}, 'b': {'y': 'rst'}})
        self.assertDictEqual(expected.entrypoints, added.entrypoints)

        d1 = None
        d2 = None
        srd1 = SystemRequirementsDict(d1)
        srd2 = SystemRequirementsDict(d2)
        added = srd1 + srd2
        expected = SystemRequirementsDict(None)
        self.assertEqual(expected.entrypoints, added.entrypoints)

    def test_override_cluster_spec_for_app_with_named_entrypoint(self):
        bootstrap_code = "import sys\n"
        cluster_spec_with_bootstrap = {"type": "spark",
                                            "version": "2.4.0",
                                            "initialInstanceCount": 2,
                                            "bootstrapScript": bootstrap_code}
        cluster_spec_no_bootstrap = {"type": "spark",
                                     "version": "2.4.0",
                                     "initialInstanceCount": 3}
        app_sys_reqs = {"main": {
                         "instanceType": "mem2_hdd2_x1",
                         "clusterSpec": cluster_spec_with_bootstrap},
                        "cluster_2": {
                         "instanceType": "mem2_hdd2_x4",
                         "clusterSpec": cluster_spec_no_bootstrap}
                       }
        app_srd = SystemRequirementsDict.from_sys_requirements(app_sys_reqs, _type='clusterSpec')

        # pass instance count with specific entry point
        runtime_srd = SystemRequirementsDict.from_instance_count({"cluster_2": "4"})
        cluster_spec_srd = app_srd.override_cluster_spec(runtime_srd)
        self.assertEqual(cluster_spec_srd.entrypoints['cluster_2']["clusterSpec"]["initialInstanceCount"], 4)
        self.assertEqual(cluster_spec_srd.entrypoints['cluster_2']["clusterSpec"]["version"], "2.4.0")
        self.assertEqual(cluster_spec_srd.entrypoints['cluster_2'].get('instanceType'), None)

        # pass instance count for all entry points ("*")
        runtime_srd = SystemRequirementsDict.from_instance_count(5)
        cluster_spec_srd = app_srd.override_cluster_spec(runtime_srd)
        self.assertEqual(cluster_spec_srd.entrypoints['cluster_2']["clusterSpec"]["initialInstanceCount"], 5)
        self.assertEqual(cluster_spec_srd.entrypoints['main']["clusterSpec"]["initialInstanceCount"], 5)
        self.assertEqual(cluster_spec_srd.entrypoints['main']["clusterSpec"]["bootstrapScript"], bootstrap_code)
        self.assertEqual(cluster_spec_srd.entrypoints['main'].get('instanceType'), None)
        self.assertEqual(cluster_spec_srd.entrypoints.get('*'), None)

        # pass instance count together with instance type
        runtime_srd = SystemRequirementsDict.from_instance_count({"main": "6"})
        cluster_spec_srd = app_srd.override_cluster_spec(runtime_srd)
        instance_type_srd = SystemRequirementsDict.from_instance_type({"main": "mem1_ssd1_x2"})
        added = (cluster_spec_srd + instance_type_srd).as_dict()
        self.assertEqual(added['main']["clusterSpec"]["initialInstanceCount"], 6)
        self.assertEqual(added['main']["clusterSpec"]["version"], "2.4.0")
        self.assertEqual(added['main']["instanceType"], "mem1_ssd1_x2")
        self.assertTrue("*" not in added)
        self.assertTrue("cluster_2" not in added)

    def test_override_cluster_spec_for_app_with_wildcard_entrypoint(self):
        bootstrap_code = "import sys\n"
        app_sys_reqs = {"*": {
                            "instanceType": "mem2_hdd2_x1",
                            "clusterSpec": {"type": "spark",
                                            "version": "2.4.0",
                                            "initialInstanceCount": 22,
                                            "bootstrapScript": bootstrap_code}},
                        "other": {
                            "instanceType": "mem2_hdd2_x2",
                            "clusterSpec": {"type": "spark",
                                            "version": "2.4.0",
                                            "initialInstanceCount": 33,
                                            "bootstrapScript": bootstrap_code}
                        }}
        app_srd = SystemRequirementsDict.from_sys_requirements(app_sys_reqs, _type='clusterSpec')

        # pass instance count with "*" entry point
        runtime_srd = SystemRequirementsDict.from_instance_count(8)
        cluster_spec_srd = app_srd.override_cluster_spec(runtime_srd)
        self.assertEqual(cluster_spec_srd.entrypoints['*']["clusterSpec"]["initialInstanceCount"], 8)
        self.assertEqual(cluster_spec_srd.entrypoints['*']["clusterSpec"]["bootstrapScript"], bootstrap_code)
        self.assertEqual(cluster_spec_srd.entrypoints['other']["clusterSpec"]["initialInstanceCount"], 8)
        self.assertEqual(cluster_spec_srd.entrypoints['other']["clusterSpec"]["bootstrapScript"], bootstrap_code)
        self.assertEqual(cluster_spec_srd.entrypoints['*'].get('instanceType'), None)

        # pass instance count with a named entry point
        runtime_srd = SystemRequirementsDict.from_instance_count({"main": 77})
        cluster_spec_srd = app_srd.override_cluster_spec(runtime_srd)
        self.assertEqual(cluster_spec_srd.entrypoints['main']["clusterSpec"]["initialInstanceCount"], 77)
        self.assertEqual(cluster_spec_srd.entrypoints['main']["clusterSpec"]["bootstrapScript"], bootstrap_code)
        self.assertEqual(cluster_spec_srd.entrypoints['main'].get('instanceType'), None)

        # pass instance count with a named entry point and instance type
        runtime_srd = SystemRequirementsDict.from_instance_count({"main": "42"})
        cluster_spec_srd = app_srd.override_cluster_spec(runtime_srd)
        instance_type_srd = SystemRequirementsDict.from_instance_type({"main": "mem1_ssd1_x2"})
        added = (cluster_spec_srd + instance_type_srd).as_dict()
        self.assertEqual(added['main']["clusterSpec"]["initialInstanceCount"], 42)
        self.assertEqual(added['main']["clusterSpec"]["bootstrapScript"], bootstrap_code)
        self.assertEqual(added['main']["instanceType"], "mem1_ssd1_x2")

        # pass instance count with a named and wildcard entry point and instance type
        runtime_srd = SystemRequirementsDict.from_instance_count({"main": "42", "*": "52"})
        cluster_spec_srd = app_srd.override_cluster_spec(runtime_srd)
        instance_type_srd = SystemRequirementsDict.from_instance_type({"main": "mem1_ssd1_x2"})
        added = (cluster_spec_srd + instance_type_srd).as_dict()
        self.assertEqual(added['main']["clusterSpec"]["initialInstanceCount"], 42)
        self.assertEqual(added['main']["clusterSpec"]["bootstrapScript"], bootstrap_code)
        self.assertEqual(added['main']["instanceType"], "mem1_ssd1_x2")
        self.assertEqual(added['*']["clusterSpec"]["initialInstanceCount"], 52)
        self.assertTrue("instanceType" not in added['*'])

    def test_override_cluster_spec_for_app_with_non_spark_entrypoint(self):
        bootstrap_code = "import sys\n"
        app_sys_reqs = {"non_spark_entry": {
                            "instanceType": "mem2_hdd2_x1"},
                        "spark_entry": {
                            "clusterSpec": {"type": "spark",
                                            "version": "2.4.0",
                                            "initialInstanceCount": 2,
                                            "bootstrapScript": bootstrap_code}}}
        app_srd = SystemRequirementsDict.from_sys_requirements(app_sys_reqs, _type='clusterSpec')

        # pass instance count with "*" entry point
        runtime_srd = SystemRequirementsDict.from_instance_count(8)
        cluster_spec_srd = app_srd.override_cluster_spec(runtime_srd)
        self.assertEqual(cluster_spec_srd.entrypoints['spark_entry']["clusterSpec"]["initialInstanceCount"], 8)
        self.assertEqual(cluster_spec_srd.entrypoints['spark_entry']["clusterSpec"]["bootstrapScript"], bootstrap_code)
        self.assertEqual(cluster_spec_srd.entrypoints['spark_entry'].get('instanceType'), None)
        self.assertTrue("*" not in cluster_spec_srd.entrypoints)
        self.assertTrue("non_spark_entry" not in cluster_spec_srd.entrypoints)

        # pass instance count and instance type
        runtime_srd = SystemRequirementsDict.from_instance_count({"spark_entry": "422", "*": "522"})
        cluster_spec_srd = app_srd.override_cluster_spec(runtime_srd)
        instance_type_srd = SystemRequirementsDict.from_instance_type({"*": "mem1_ssd1_x2",
                                                                  "spark_entry": "mem1_ssd1_x4",
                                                                  "no_spark_entry": "mem1_ssd1_x8"})
        added = (cluster_spec_srd + instance_type_srd).as_dict()
        expected = {
                    'spark_entry': {
                            'clusterSpec': {
                                    'bootstrapScript': 'import sys\n',
                                    'version': '2.4.0',
                                    'type': 'spark',
                                    'initialInstanceCount': 422},
                            'instanceType': 'mem1_ssd1_x4'},
                    '*': {
                            'instanceType': 'mem1_ssd1_x2'},
                    'no_spark_entry': {
                            'instanceType': 'mem1_ssd1_x8'
                    }}
        self.assertEqual(added, expected)

    def test_from_instance_type(self):
        d1 = None
        d2 = None
        srd1 = SystemRequirementsDict(d1)
        srd2 = SystemRequirementsDict.from_instance_type(d2)
        added = srd1 + srd2
        expected = SystemRequirementsDict(None)
        self.assertEqual(expected.entrypoints, added.entrypoints)

if __name__ == '__main__':
    unittest.main()
