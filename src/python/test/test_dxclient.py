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

from __future__ import print_function, unicode_literals

import os, sys, unittest, json, tempfile, subprocess, csv, shutil, re, base64, random, time
import pipes
from contextlib import contextmanager
import pexpect

import dxpy
from dxpy.scripts import dx_build_app
from dxpy_testutil import DXTestCase, check_output, temporary_project, select_project
import dxpy_testutil as testutil
from dxpy.packages import requests
from dxpy.exceptions import DXAPIError, EXPECTED_ERR_EXIT_STATUS
from dxpy.compat import str, sys_encoding

@contextmanager
def chdir(dirname=None):
    curdir = os.getcwd()
    try:
        if dirname is not None:
            os.chdir(dirname)
        yield
    finally:
        os.chdir(curdir)

def run(command, **kwargs):
    print("$ %s" % (command,))
    output = check_output(command, shell=True, **kwargs)
    print(output)
    return output

def overrideEnvironment(**kwargs):
    env = os.environ.copy()
    for key in kwargs:
        if kwargs[key] is None:
            if key in env:
                del env[key]
        else:
            env[key] = kwargs[key]
    return env


def create_file_in_project(fname, trg_proj_id, folder=None):
    data = "foo"
    if folder is None:
        dxfile = dxpy.upload_string(data, name=fname, project=trg_proj_id, wait_on_close=True)
    else:
        dxfile = dxpy.upload_string(data, name=fname, project=trg_proj_id, folder=folder, wait_on_close=True)
    return dxfile.get_id()


def create_project():
    project_name = "test_dx_cp_" + str(random.randint(0, 1000000)) + "_" + str(int(time.time() * 1000))
    return dxpy.api.project_new({'name': project_name})['id']


def rm_project(proj_id):
    dxpy.api.project_destroy(proj_id, {"terminateJobs": True})


def create_folder_in_project(proj_id, path):
    dxpy.api.project_new_folder(proj_id, {"folder": path})


def list_folder(proj_id, path):
    output = dxpy.api.project_list_folder(proj_id, {"folder": path})
    # Sort the results to create ordering. This allows using the results
    # for comparison purposes.
    output['folders'] = sorted(output['folders'])
    output['objects'] = sorted(output['objects'])
    return output

def makeGenomeObject():
    # NOTE: for these tests we don't upload a full sequence file (which
    # would be huge, for hg19). Importers and exporters that need to
    # look at the full sequence file can't be run on this test
    # contigset.
    sequence_file = dxpy.upload_string("", hidden=True)

    genome_record = dxpy.new_dxrecord()
    genome_record.set_details({
        "flat_sequence_file": {"$dnanexus_link": sequence_file.get_id()},
        "contigs": {
            "offsets": [0],
            "names": ["chr1"],
            "sizes": [249250621]
        }
    })
    genome_record.add_types(["ContigSet"])
    genome_record.close()

    sequence_file.wait_on_close()

    return genome_record.get_id()


class TestDXTestUtils(DXTestCase):
    def test_temporary_project(self):
        test_dirname = '/test_folder'
        with temporary_project('test_temporary_project', select=True) as temp_project:
            self.assertEquals('test_temporary_project:/', run('dx pwd').strip())

    def test_select_project(self):
        test_dirname = '/test_folder'
        with temporary_project('test_select_project') as temp_project:
            test_projectid = temp_project.get_id()
            run('dx mkdir -p {project}:{dirname}'.format(project=test_projectid, dirname=test_dirname))
            with select_project(test_projectid):
                # This would fail if the project context hadn't been
                # successfully changed by select_project
                run('dx cd {dirname}'.format(dirname=test_dirname))


class TestDXClient(DXTestCase):
    def test_dx_version(self):
        version = run("dx --version")
        self.assertIn("dx", version)

    def test_dx_actions(self):
        with self.assertRaises(subprocess.CalledProcessError):
            run("dx")
        run("dx help")
        folder_name = "эксперимент 1"
        run("dx cd /")
        run("dx ls")
        run("dx mkdir '{f}'".format(f=folder_name))
        run("dx cd '{f}'".format(f=folder_name))
        with tempfile.NamedTemporaryFile() as f:
            local_filename = f.name
            filename = folder_name
            run("echo xyzzt > {tf}".format(tf=local_filename))
            fileid = run("dx upload --wait {tf} -o '../{f}/{f}' --brief".format(tf=local_filename,
                                                                                f=filename))
            self.assertEqual(fileid, run("dx ls '../{f}/{f}' --brief".format(f=filename)))
            self.assertEqual("xyzzt\n", run("dx head '../{f}/{f}'".format(f=filename)))
        run("dx pwd")
        run("dx cd ..")
        run("dx pwd")
        run("dx ls")
        with self.assertRaises(subprocess.CalledProcessError):
            run("dx rm '{f}'".format(f=filename))
        run("dx cd '{f}'".format(f=folder_name))

        run("dx mv '{f}' '{f}2'".format(f=filename))
        run("dx mv '{f}2' '{f}'".format(f=filename))

        run("dx rm '{f}'".format(f=filename))

        table_name = folder_name
        with tempfile.NamedTemporaryFile(suffix='.csv') as f:
            writer = csv.writer(f)
            writer.writerows([['a:uint8', 'b:string', 'c:float'], [1, "x", 1.0], [2, "y", 4.0]])
            f.flush()
            run("dx import csv -o '../{n}' '{f}' --wait".format(n=table_name, f=f.name))
            run("dx export csv '../{n}' --output {o} -f".format(n=table_name, o=f.name))

        run("dx get_details '../{n}'".format(n=table_name))

        run("dx cd ..")
        run("dx rmdir '{f}'".format(f=folder_name))

        run("dx tree")
        run("dx find data --name '{n}'".format(n=table_name))
        run("dx find data --name '{n} --property foo=bar'".format(n=table_name))
        run("dx rename '{n}' '{n}'2".format(n=table_name))
        run("dx rename '{n}'2 '{n}'".format(n=table_name))
        run("dx set_properties '{n}' '{n}={n}' '{n}2={n}3'".format(n=table_name))
        run("dx unset_properties '{n}' '{n}' '{n}2'".format(n=table_name))
        run("dx tag '{n}' '{n}'2".format(n=table_name))
        run("dx describe '{n}'".format(n=table_name))

        run("dx new record -o :foo --verbose")
        record_id = run("dx new record -o :foo2 --brief --visibility hidden --property foo=bar " +
                        "--property baz=quux --tag onetag --tag twotag --type foo --type bar " +
                        "--details '{\"hello\": \"world\"}'").strip()
        self.assertEqual(record_id, run("dx ls :foo2 --brief").strip())
        self.assertEqual({"hello": "world"}, json.loads(run("dx get -o - :foo2")))

        second_record_id = run("dx new record :somenewfolder/foo --parents --brief").strip()
        self.assertEqual(second_record_id, run("dx ls :somenewfolder/foo --brief").strip())

        # describe
        run("dx describe {record}".format(record=record_id))
        desc = json.loads(run("dx describe {record} --details --json".format(record=record_id)))
        self.assertEqual(desc['tags'], ['onetag', 'twotag'])
        self.assertEqual(desc['types'], ['foo', 'bar'])
        self.assertEqual(desc['properties'], {"foo": "bar", "baz": "quux"})
        self.assertEqual(desc['details'], {"hello": "world"})
        self.assertEqual(desc['hidden'], True)

        desc = json.loads(run("dx describe {record} --json".format(record=second_record_id)))
        self.assertEqual(desc['folder'], '/somenewfolder')

        run("dx rm :foo")
        run("dx rm :foo2")
        run("dx rm -r :somenewfolder")

        # Path resolution is used
        run("dx find jobs --project :")
        run("dx find executions --project :")
        run("dx find analyses --project :")
        run("dx find data --project :")

    def test_get_unicode_url(self):
        with self.assertSubprocessFailure(stderr_regexp="ResourceNotFound", exit_code=3):
            run("dx api project-эксперимент describe")

    def test_dx_env(self):
        run("dx env")
        run("dx env --bash")
        run("dx env --dx-flags")

    def test_dx_api(self):
        with tempfile.NamedTemporaryFile() as fd:
            fd.write("{}")
            fd.flush()
            run("dx api {p} describe --input {fn}".format(p=self.project, fn=fd.name))

    @unittest.skipUnless(testutil.TEST_NO_RATE_LIMITS,
                         'skipping tests that need rate limits to be disabled')
    def test_dx_invite(self):
        for query in ("Ψ", "alice.nonexistent", "alice.nonexistent {p}", "user-alice.nonexistent {p}",
                      "alice.nonexistent@example.com {p}", "alice.nonexistent : VIEW"):
            with self.assertSubprocessFailure(stderr_regexp="ResourceNotFound", exit_code=3):
                run(("dx invite "+query).format(p=self.project))
        with self.assertSubprocessFailure(stderr_regexp="invalid choice", exit_code=2):
            run(("dx invite alice.nonexistent : ПРОСМОТР").format(p=self.project))

    @unittest.skipUnless(testutil.TEST_NO_RATE_LIMITS,
                         'skipping tests that need rate limits to be disabled')
    def test_dx_uninvite(self):
        for query in ("Ψ", "alice.nonexistent", "alice.nonexistent {p}", "user-alice.nonexistent {p}",
                      "alice.nonexistent@example.com {p}"):
            with self.assertSubprocessFailure(stderr_regexp="ResourceNotFound", exit_code=3):
                run(("dx uninvite "+query).format(p=self.project))

    def test_dx_add_rm_types(self):
        run("dx new record Ψ")
        run("dx add_types Ψ abc xyz")
        with self.assertSubprocessFailure(stderr_text="be an array of valid strings for a type name",
                                          exit_code=1):
            run("dx add_types Ψ ΨΨ")
        run("dx remove_types Ψ abc xyz")
        run("dx remove_types Ψ abc xyz")
        with self.assertSubprocessFailure(stderr_regexp="Could not resolve", exit_code=1):
            run("dx remove_types ΨΨ Ψ")

    def test_dx_set_details(self):
        record_id = run("dx new record Ψ1 --brief").strip()
        run("dx set_details Ψ1 '{\"foo\": \"bar\"}'")
        dxrecord = dxpy.DXRecord(record_id)
        details = dxrecord.get_details()
        self.assertEqual({"foo": "bar"}, details, msg="dx set_details with valid JSON string input failed.")

    def test_dx_set_details_with_file(self):
        # Create temporary JSON file with valid JSON.
        with tempfile.NamedTemporaryFile() as tmp_file, tempfile.NamedTemporaryFile() as tmp_invalid_file:
            tmp_file.write('{\"foo\": \"bar\"}')
            tmp_file.flush()

            # Test -f with valid JSON file.
            record_id = run("dx new record Ψ2 --brief").strip()
            run("dx set_details Ψ2 -f " + pipes.quote(tmp_file.name))
            dxrecord = dxpy.DXRecord(record_id)
            details = dxrecord.get_details()
            self.assertEqual({"foo": "bar"}, details, msg="dx set_details -f with valid JSON input file failed.")

            # Test --details-file with valid JSON file.
            record_id = run("dx new record Ψ3 --brief").strip()
            run("dx set_details Ψ3 --details-file " + pipes.quote(tmp_file.name))
            dxrecord = dxpy.DXRecord(record_id)
            details = dxrecord.get_details()
            self.assertEqual({"foo": "bar"}, details,
                             msg="dx set_details --details-file with valid JSON input file failed.")

            # Create temporary JSON file with invalid JSON.
            tmp_invalid_file.write('{\"foo\": \"bar\"')
            tmp_invalid_file.flush()

            # Test above with invalid JSON file.
            record_id = run("dx new record Ψ4 --brief").strip()
            with self.assertSubprocessFailure(stderr_regexp="JSON", exit_code=3):
                run("dx set_details Ψ4 -f " + pipes.quote(tmp_invalid_file.name))

            # Test command with (-f or --details-file) and CL JSON.
            with self.assertSubprocessFailure(stderr_regexp="Error: Cannot provide both -f/--details-file and details",
                                              exit_code=3):
                run("dx set_details Ψ4 '{ \"foo\":\"bar\" }' -f " + pipes.quote(tmp_file.name))

            # Test piping JSON from STDIN.
            record_id = run("dx new record Ψ5 --brief").strip()
            run("cat " + pipes.quote(tmp_file.name) + " | dx set_details Ψ5 -f -")
            dxrecord = dxpy.DXRecord(record_id)
            details = dxrecord.get_details()
            self.assertEqual({"foo": "bar"}, details, msg="dx set_details -f - with valid JSON input failed.")

    def test_dx_shell(self):
        shell = pexpect.spawn("bash")
        shell.logfile = sys.stdout
        shell.sendline("dx sh")
        shell.expect(">")
        shell.sendline("Ψ 'Ψ Ψ'")
        shell.expect("invalid choice: Ψ".encode(sys_encoding))
        shell.expect(">")
        shell.sendline("env")
        shell.expect("Current user")
        shell.sendline("help all")
        shell.expect("Commands:")
        shell.sendline("exit")
        shell.sendline("echo find projects | dx sh")
        shell.expect("project-")

    def test_dx_get_record(self):
        with chdir(tempfile.mkdtemp()):
            run("dx new record -o :foo --verbose")
            run("dx get :foo")
            self.assertTrue(os.path.exists('foo.json'))
            run("dx get --no-ext :foo")
            self.assertTrue(os.path.exists('foo'))
            run("diff -q foo foo.json")

    def test_dx_object_tagging(self):
        the_tags = ["Σ1=n", "helloo0", "ωω"]
        # tag
        record_id = run("dx new record Ψ --brief").strip()
        run("dx tag Ψ " + " ".join(the_tags))
        mytags = dxpy.describe(record_id)['tags']
        for tag in the_tags:
            self.assertIn(tag, mytags)
        # untag
        run("dx untag Ψ " + " ".join(the_tags[:2]))
        mytags = dxpy.describe(record_id)['tags']
        for tag in the_tags[:2]:
            self.assertNotIn(tag, mytags)
        self.assertIn(the_tags[2], mytags)

        # -a flag
        second_record_id = run("dx new record Ψ --brief").strip()
        self.assertNotEqual(record_id, second_record_id)
        run("dx tag -a Ψ " + " ".join(the_tags))
        mytags = dxpy.describe(record_id)['tags']
        for tag in the_tags:
            self.assertIn(tag, mytags)
        second_tags = dxpy.describe(second_record_id)['tags']
        for tag in the_tags:
            self.assertIn(tag, second_tags)

        run("dx untag -a Ψ " + " ".join(the_tags))
        mytags = dxpy.describe(record_id)['tags']
        self.assertEqual(len(mytags), 0)
        second_tags = dxpy.describe(second_record_id)['tags']
        self.assertEqual(len(second_tags), 0)

        # nonexistent name
        with self.assertSubprocessFailure(stderr_regexp='Could not resolve', exit_code=3):
            run("dx tag nonexistent atag")
        with self.assertSubprocessFailure(stderr_regexp='Could not resolve', exit_code=3):
            run("dx untag nonexistent atag")

    def test_dx_project_tagging(self):
        the_tags = ["$my.tag", "secoиdtag", "тhird тagggg"]
        # tag
        run("dx tag : \\" + the_tags[0] + " " + the_tags[1] + " '" + the_tags[2] + "'")
        mytags = dxpy.describe(self.project)['tags']
        for tag in the_tags:
            self.assertIn(tag, mytags)
        # untag
        run("dx untag : \\" + the_tags[0] + " '" + the_tags[2] + "'")
        mytags = dxpy.describe(self.project)['tags']
        self.assertIn(the_tags[1], mytags)
        for tag in [the_tags[0], the_tags[2]]:
            self.assertNotIn(tag, mytags)

        # nonexistent name
        with self.assertSubprocessFailure(stderr_regexp='Could not find a project named', exit_code=3):
            run("dx tag nonexistent: atag")
        with self.assertSubprocessFailure(stderr_regexp='Could not find a project named', exit_code=3):
            run("dx untag nonexistent: atag")

    def test_dx_object_properties(self):
        property_names = ["Σ_1^n", "helloo0", "ωω"]
        property_values = ["n", "world z", "ω()"]
        # set_properties
        record_id = run("dx new record Ψ --brief").strip()
        run("dx set_properties Ψ " +
            " ".join(["'" + prop[0] + "'='" + prop[1] + "'" for prop in zip(property_names,
                                                                            property_values)]))
        my_properties = dxpy.api.record_describe(record_id, {"properties": True})['properties']
        for (name, value) in zip(property_names, property_values):
            self.assertIn(name, my_properties)
            self.assertEqual(value, my_properties[name])
        # unset_properties
        run("dx unset_properties Ψ '" + "' '".join(property_names[:2]) + "'")
        my_properties = dxpy.api.record_describe(record_id, {"properties": True})['properties']
        for name in property_names[:2]:
            self.assertNotIn(name, my_properties)
        self.assertIn(property_names[2], my_properties)
        self.assertEqual(property_values[2], my_properties[property_names[2]])

        # -a flag
        second_record_id = run("dx new record Ψ --brief").strip()
        self.assertNotEqual(record_id, second_record_id)
        run("dx set_properties -a Ψ " +
            " ".join(["'" + prop[0] + "'='" + prop[1] + "'" for prop in zip(property_names,
                                                                            property_values)]))
        my_properties = dxpy.api.record_describe(record_id, {"properties": True})['properties']
        for (name, value) in zip(property_names, property_values):
            self.assertIn(name, my_properties)
            self.assertEqual(value, my_properties[name])
        second_properties = dxpy.api.record_describe(second_record_id,
                                                     {"properties": True})['properties']
        for (name, value) in zip(property_names, property_values):
            self.assertIn(name, my_properties)
            self.assertEqual(value, my_properties[name])

        run("dx unset_properties -a Ψ '" + "' '".join(property_names) + "'")
        my_properties = dxpy.api.record_describe(record_id, {"properties": True})['properties']
        self.assertEqual(len(my_properties), 0)
        second_properties = dxpy.api.record_describe(second_record_id,
                                                     {"properties": True})['properties']
        self.assertEqual(len(second_properties), 0)

        # nonexistent name
        with self.assertSubprocessFailure(stderr_regexp='Could not resolve', exit_code=3):
            run("dx set_properties nonexistent key=value")
        with self.assertSubprocessFailure(stderr_regexp='Could not resolve', exit_code=3):
            run("dx unset_properties nonexistent key")

        # Errors parsing --property value
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties -a Ψ ''")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties -a Ψ foo=bar=baz")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties -a Ψ =foo=bar=")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties -a Ψ foo")
        # Property keys must be nonempty
        with self.assertSubprocessFailure(stderr_regexp='nonempty strings', exit_code=3):
            run("dx set_properties -a Ψ =bar")
        # Empty string values should be okay
        run("dx set_properties -a Ψ bar=")

        my_properties = dxpy.api.record_describe(record_id, {"properties": True})['properties']
        self.assertEqual(my_properties["bar"], "")

    def test_dx_project_properties(self):
        property_names = ["$my.prop", "secoиdprop", "тhird prop"]
        property_values = ["$hello.world", "Σ2,n", "stuff"]
        # set_properties
        run("dx set_properties : " +
            " ".join(["'" + prop[0] + "'='" + prop[1] + "'" for prop in zip(property_names,
                                                                            property_values)]))
        my_properties = dxpy.api.project_describe(self.project, {"properties": True})['properties']
        for (name, value) in zip(property_names, property_values):
            self.assertIn(name, my_properties)
            self.assertEqual(value, my_properties[name])
        # unset_properties
        run("dx unset_properties : '" + property_names[0] + "' '" + property_names[2] + "'")
        my_properties = dxpy.api.project_describe(self.project, {"properties": True})['properties']
        self.assertIn(property_names[1], my_properties)
        self.assertEqual(property_values[1], my_properties[property_names[1]])
        for name in [property_names[0], property_names[2]]:
            self.assertNotIn(name, my_properties)

        # nonexistent name
        with self.assertSubprocessFailure(stderr_regexp='Could not find a project named', exit_code=3):
            run("dx set_properties nonexistent: key=value")
        with self.assertSubprocessFailure(stderr_regexp='Could not find a project named', exit_code=3):
            run("dx unset_properties nonexistent: key")

        # Errors parsing --property value
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties : ''")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties : foo=bar=baz")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties : =foo=bar=")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx set_properties : foo")
        # Property keys must be nonempty
        with self.assertSubprocessFailure(stderr_regexp='nonempty strings', exit_code=3):
            run("dx set_properties : =bar")
        # Empty string values should be okay
        run("dx set_properties : bar=")

        my_properties = dxpy.api.project_describe(self.project, {"properties": True})['properties']
        self.assertEqual(my_properties["bar"], "")

    def test_dx_describe_project(self):
        describe_output = run("dx describe :").strip()
        print(describe_output)
        self.assertTrue(re.search(r'ID\s+%s.*\n.*\nName\s+dxclient_test_pröject' % (self.project,),
                                  describe_output))
        self.assertIn('Properties', describe_output)

    def test_dx_remove_project_by_name(self):
        # TODO: this test makes no use of the DXTestCase-provided
        # project.
        project_name = ("test_dx_remove_project_by_name_" + str(random.randint(0, 1000000)) + "_" +
                        str(int(time.time() * 1000)))
        project_id = run("dx new project {name} --brief".format(name=project_name)).strip()
        self.assertEqual(run("dx find projects --brief --name {name}".format(name=project_name)).strip(),
                         project_id)
        run("dx rmproject -y {name}".format(name=project_name))
        self.assertEqual(run("dx find projects --brief --name {name}".format(name=project_name)), "")

    def test_dx_cp(self):
        project_name = "test_dx_cp_" + str(random.randint(0, 1000000)) + "_" + str(int(time.time() * 1000))
        dest_project_id = run("dx new project {name} --brief".format(name=project_name)).strip()
        try:
            record_id = run("dx new record --brief --details '{\"hello\": 1}'").strip()
            run("dx close --wait {r}".format(r=record_id))
            self.assertEqual(run("dx ls --brief {p}".format(p=dest_project_id)), "")
            run("dx cp {r} {p}".format(r=record_id, p=dest_project_id))
            self.assertEqual(run("dx ls --brief {p}".format(p=dest_project_id)).strip(), record_id)
        finally:
            run("dx rmproject -y {p}".format(p=dest_project_id))

    def test_dx_gtables(self):
        # new gtable
        gri_gtable_id = run("dx new gtable --gri mychr mylo myhi " +
                            "--columns mychr,mylo:int32,myhi:int32 --brief --property hello=world " +
                            "--details '{\"hello\":\"world\"}' --visibility visible").strip()
        # Add rows to it (?)
        # TODO: make this better.
        add_rows_input = {"data": [["chr", 1, 10], ["chr2", 3, 13], ["chr1", 3, 10], ["chr1", 11, 13],
                                   ["chr1", 5, 12]]}
        run("dx api {gt} addRows '{rows}'".format(gt=gri_gtable_id, rows=json.dumps(add_rows_input)))
        # close
        run("dx close {gt} --wait".format(gt=gri_gtable_id))

        # describe
        desc = json.loads(run("dx describe {gt} --details --json".format(gt=gri_gtable_id)))
        self.assertEqual(desc['types'], ['gri'])
        self.assertEqual(desc['indices'],
                         [{"type":"genomic", "name":"gri", "chr":"mychr", "lo":"mylo", "hi":"myhi"}])
        self.assertEqual(desc['properties'], {"hello": "world"})
        self.assertEqual(desc['details'], {"hello": "world"})
        self.assertEqual(desc['hidden'], False)

        # gri query
        self.assertEqual(run("dx export tsv {gt} --gri chr1 1 10 -o -".format(gt=gri_gtable_id)),
                         '\r\n'.join(['mychr:string\tmylo:int32\tmyhi:int32', 'chr1\t3\t10',
                                      'chr1\t5\t12', '']))

        # "get" is not supported on gtables
        with self.assertSubprocessFailure(stderr_regexp='given object is of class gtable', exit_code=3):
            run("dx get {gt}".format(gt=gri_gtable_id))

        # Download and re-import with gri
        with tempfile.NamedTemporaryFile(suffix='.csv') as fd:
            run("dx export tsv {gt} -o {fd} -f".format(gt=gri_gtable_id, fd=fd.name))
            fd.flush()
            run("dx import tsv {fd} -o gritableimport --gri mychr mylo myhi --wait".format(fd=fd.name))

            # Also, upload and download the file just to test out upload/download
            run("dx upload {fd} -o uploadedfile --wait".format(fd=fd.name))
            run("dx download uploadedfile -f")
            run("dx download uploadedfile -o -")
        try:
            os.remove("uploadedfile")
        except IOError:
            pass

        second_desc = json.loads(run("dx describe gritableimport --json"))
        self.assertEqual(second_desc['types'], ['gri'])
        self.assertEqual(second_desc['indices'],
                         [{"type":"genomic", "name":"gri", "chr":"mychr", "lo":"mylo", "hi":"myhi"}])
        self.assertEqual(desc['size'], second_desc['size'])
        self.assertEqual(desc['length'], second_desc['length'])

    def test_dx_mkdir(self):
        with self.assertRaises(subprocess.CalledProcessError):
            run("dx mkdir mkdirtest/b/c")
        run("dx mkdir -p mkdirtest/b/c")
        run("dx mkdir -p mkdirtest/b/c")
        run("dx rm -r mkdirtest")

    def test_dxpy_session_isolation(self):
        for var in 'DX_PROJECT_CONTEXT_ID', 'DX_PROJECT_CONTEXT_NAME', 'DX_CLI_WD':
            if var in os.environ:
                del os.environ[var]
        shell1 = pexpect.spawn("bash")
        shell2 = pexpect.spawn("bash")
        shell1.logfile = shell2.logfile = sys.stdout
        shell1.setwinsize(20, 90)
        shell2.setwinsize(20, 90)

        def expect_dx_env_cwd(shell, wd):
            shell.expect(self.project)
            shell.expect(wd)
            shell.expect([">", "#", "$"]) # prompt

        shell1.sendline("dx select "+self.project)
        shell1.sendline("dx mkdir /sessiontest1")
        shell1.sendline("dx cd /sessiontest1")
        shell1.sendline("dx env")
        expect_dx_env_cwd(shell1, "sessiontest1")

        shell2.sendline("dx select "+self.project)
        shell2.sendline("dx mkdir /sessiontest2")
        shell2.sendline("dx cd /sessiontest2")
        shell2.sendline("dx env")
        expect_dx_env_cwd(shell2, "sessiontest2")
        shell2.sendline("bash -c 'dx env'")
        expect_dx_env_cwd(shell2, "sessiontest2")

        shell1.sendline("dx env")
        expect_dx_env_cwd(shell1, "sessiontest1")
        # Grandchild subprocess inherits session
        try:
            shell1.sendline("bash -c 'dx env'")
            expect_dx_env_cwd(shell1, "sessiontest1")
        except:
            print("*** TODO: FIXME: Unable to verify that grandchild subprocess inherited session")

    def test_dx_ssh_config(self):
        original_ssh_public_key = None
        try:
            user_id = dxpy.whoami()
            original_ssh_public_key = dxpy.api.user_describe(user_id).get('sshPublicKey')
            wd = tempfile.mkdtemp()

            def get_dx_ssh_config():
                dx_ssh_config = pexpect.spawn("dx ssh_config", env=overrideEnvironment(HOME=wd))
                dx_ssh_config.logfile = sys.stdout
                dx_ssh_config.setwinsize(20, 90)
                return dx_ssh_config

            def read_back_pub_key():
                self.assertTrue(os.path.exists(os.path.join(wd, ".dnanexus_config/ssh_id")))

                with open(os.path.join(wd, ".dnanexus_config/ssh_id.pub")) as fh:
                    self.assertEqual(fh.read(), dxpy.api.user_describe(user_id).get('sshPublicKey'))

            dx_ssh_config = get_dx_ssh_config()
            dx_ssh_config.expect("The DNAnexus configuration directory")
            dx_ssh_config.expect("does not exist")

            os.mkdir(os.path.join(wd, ".dnanexus_config"))

            dx_ssh_config = get_dx_ssh_config()
            dx_ssh_config.expect("Select an SSH key pair")
            dx_ssh_config.sendline("1")
            dx_ssh_config.expect("Enter the location of your SSH key")
            dx_ssh_config.sendline("нет ключа")
            dx_ssh_config.expect("Unable to find")

            dx_ssh_config = get_dx_ssh_config()
            dx_ssh_config.expect("Select an SSH key pair")
            dx_ssh_config.sendline("0")
            dx_ssh_config.expect("Enter passphrase")
            dx_ssh_config.sendline()
            dx_ssh_config.expect("again")
            dx_ssh_config.sendline()
            dx_ssh_config.expect("Your account has been configured for use with SSH")
            read_back_pub_key()

            dx_ssh_config = get_dx_ssh_config()
            dx_ssh_config.expect("Select an SSH key pair")
            dx_ssh_config.expect("already configured")
            dx_ssh_config.sendline("0")
            dx_ssh_config.expect("Your account has been configured for use with SSH")
            read_back_pub_key()

            dx_ssh_config = get_dx_ssh_config()
            dx_ssh_config.expect("Select an SSH key pair")
            dx_ssh_config.expect("already configured")
            dx_ssh_config.sendline("1")
            dx_ssh_config.expect("Generate a new SSH key pair")
            dx_ssh_config.sendline("0")
            dx_ssh_config.expect("Enter passphrase")
            dx_ssh_config.sendline()
            dx_ssh_config.expect("again")
            dx_ssh_config.sendline()
            dx_ssh_config.expect("Your account has been configured for use with SSH")
            read_back_pub_key()

            # Ensure that private key upload is rejected
            with open(os.path.join(wd, ".dnanexus_config", "ssh_id")) as private_key:
                with self.assertRaisesRegexp(DXAPIError,
                                             'Tried to put a private key in the sshPublicKey field'):
                    dxpy.api.user_update(user_id, {"sshPublicKey": private_key.read()})
        finally:
            if original_ssh_public_key:
                dxpy.api.user_update(user_id, {"sshPublicKey": original_ssh_public_key})

    @contextmanager
    def configure_ssh(self):
        original_ssh_public_key = None
        try:
            user_id = dxpy.whoami()
            original_ssh_public_key = dxpy.api.user_describe(user_id).get('sshPublicKey')
            wd = tempfile.mkdtemp()
            os.mkdir(os.path.join(wd, ".dnanexus_config"))

            dx_ssh_config = pexpect.spawn("dx ssh_config", env=overrideEnvironment(HOME=wd))
            dx_ssh_config.logfile = sys.stdout
            dx_ssh_config.setwinsize(20, 90)
            dx_ssh_config.expect("Select an SSH key pair")
            dx_ssh_config.sendline("0")
            dx_ssh_config.expect("Enter passphrase")
            dx_ssh_config.sendline()
            dx_ssh_config.expect("again")
            dx_ssh_config.sendline()
            dx_ssh_config.expect("Your account has been configured for use with SSH")
            yield wd
        finally:
            if original_ssh_public_key:
                dxpy.api.user_update(user_id, {"sshPublicKey": original_ssh_public_key})

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, "Skipping test that would run jobs")
    def test_dx_ssh(self):
        with self.configure_ssh() as wd:
            sleep_applet = dxpy.api.applet_new(dict(name="sleep",
                                                    runSpec={"code": "sleep 1200",
                                                             "interpreter": "bash",
                                                             "execDepends": [{"name": "dx-toolkit"}]},
                                                    inputSpec=[], outputSpec=[],
                                                    dxapi="1.0.0", version="1.0.0",
                                                    project=self.project))["id"]

            dx = pexpect.spawn("dx run {} --yes --ssh".format(sleep_applet),
                               env=overrideEnvironment(HOME=wd))
            dx.logfile = sys.stdout
            dx.setwinsize(20, 90)
            dx.expect("Waiting for job")
            dx.expect("Resolving job hostname and SSH host key", timeout=1200)

            # Wait for the line displayed between the first and second MOTDs,
            # since we only care about checking the second set of MOTD lines.
            # Example of the dividing line:
            # dnanexus@job-BP90K3Q0X2v81PXXPZj005Zj.dnanex.us (10.0.0.200) - byobu
            dx.expect(["dnanexus.io \(10.0.0.200\) - byobu",
                       "dnanex.us \(10.0.0.200\) - byobu"], timeout=120)
            dx.expect("This is the DNAnexus Execution Environment", timeout=600)
            # Check for job name (e.g. "Job: sleep")
            dx.expect("Job: \x1b\[1msleep", timeout=5)
            # \xf6 is ö
            dx.expect("Project: dxclient_test_pr\xf6ject".encode(sys_encoding))
            dx.expect("The job is running in terminal 1.", timeout=5)
            # Check for terminal prompt and verify we're in the container
            job_id = dxpy.find_jobs(name="sleep").next()['id']
            dx.expect(("dnanexus@%s" % job_id), timeout=10)

            # Make sure the job can be connected to using 'dx ssh <job id>'
            dx2 = pexpect.spawn("dx ssh " + job_id, env=overrideEnvironment(HOME=wd))
            dx2.logfile = sys.stdout
            dx2.setwinsize(20, 90)
            dx2.expect("Waiting for job")
            dx2.expect("Resolving job hostname and SSH host key", timeout=1200)
            dx2.expect(("dnanexus@%s" % job_id), timeout=10)
            dx2.sendline("whoami")
            dx2.expect("dnanexus", timeout=10)
            # Exit SSH session and terminate job
            dx2.sendline("exit")
            dx2.expect("bash running")
            dx2.sendcontrol("c") # CTRL-c
            dx2.expect("[exited]")
            dx2.expect("dnanexus@job", timeout=10)
            dx2.sendline("exit")
            dx2.expect("still running. Terminate now?")
            dx2.sendline("y")
            dx2.expect("Terminated job", timeout=60)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, "Skipping test that would run jobs")
    def test_dx_run_debug_on(self):
        with self.configure_ssh() as wd:
            crash_applet = dxpy.api.applet_new(dict(name="crash",
                                                    runSpec={"code": "exit 5", "interpreter": "bash",
                                                             "execDepends": [{"name": "dx-toolkit"}]},
                                                    inputSpec=[], outputSpec=[],
                                                    dxapi="1.0.0", version="1.0.0",
                                                    project=self.project))["id"]

            job_id = run("dx run {} --yes --brief --debug-on AppInternalError".format(crash_applet),
                         env=overrideEnvironment(HOME=wd)).strip()
            elapsed = 0
            while True:
                job_desc = dxpy.describe(job_id)
                if job_desc["state"] == "debug_hold":
                    break
                time.sleep(1)
                elapsed += 1
                if elapsed > 1200:
                    raise Exception("Timeout while waiting for job to enter debug hold")

            dx = pexpect.spawn("dx ssh " + job_id, env=overrideEnvironment(HOME=wd))
            dx.logfile = sys.stdout
            dx.setwinsize(20, 90)
            dx.expect("dnanexus@{}".format(job_id), timeout=1200)

    @unittest.skipUnless(testutil.TEST_DX_LOGIN,
                         'This test requires authserver to run, requires dx login to select the right authserver, ' +
                         'and may result in temporary account lockout. TODO: update test instrumentation to allow ' +
                         'it to run')
    def test_dx_login(self):
        wd = tempfile.mkdtemp()
        username = dxpy.user_info()['username']

        def get_dx_login(opts=""):
            dx_login = pexpect.spawn("dx login" + opts, env=overrideEnvironment(HOME=wd))
            dx_login.logfile = sys.stdout
            dx_login.setwinsize(20, 90)
            return dx_login

        dx_login = get_dx_login()
        dx_login.expect("Acquiring credentials")
        dx_login.expect("Username")
        dx_login.sendline(username)
        dx_login.expect("Password: ")
        dx_login.sendline("wrong passwörd")
        dx_login.expect("Incorrect username and/or password")
        dx_login.expect("Username")
        dx_login.sendline()
        dx_login.expect("Password: ")
        dx_login.sendline("wrong passwörd")
        dx_login.expect("Incorrect username and/or password")
        dx_login.expect("Username")
        dx_login.sendline()
        dx_login.expect("Password: ")
        dx_login.sendline("wrong passwörd")
        dx_login.expect("dx: Incorrect username and/or password")
        dx_login.close()
        self.assertEqual(dx_login.exitstatus, EXPECTED_ERR_EXIT_STATUS)

class TestDXWhoami(DXTestCase):
    def test_dx_whoami_name(self):
        whoami_output = run("dx whoami").strip()
        self.assertEqual(whoami_output, dxpy.api.user_describe(dxpy.whoami())['handle'])
    def test_dx_whoami_id(self):
        whoami_output = run("dx whoami --id").strip()
        self.assertEqual(whoami_output, dxpy.whoami())


class TestDXClientUploadDownload(DXTestCase):
    def test_dx_upload_download(self):
        with self.assertSubprocessFailure(stderr_regexp='expected the path to be a non-empty string',
                                          exit_code=3):
            run('dx download ""')
        wd = tempfile.mkdtemp()
        os.mkdir(os.path.join(wd, "a"))
        os.mkdir(os.path.join(wd, "a", "б"))
        os.mkdir(os.path.join(wd, "a", "б", "c"))
        with tempfile.NamedTemporaryFile(dir=os.path.join(wd, "a", "б")) as fd:
            fd.write("0123456789ABCDEF"*64)
            fd.flush()
            with self.assertSubprocessFailure(stderr_regexp='is a directory but the -r/--recursive option was not given', exit_code=1):
                run("dx upload "+wd)
            run("dx upload -r "+wd)
            run('dx wait "{f}"'.format(f=os.path.join(os.path.basename(wd), "a", "б",
                                                      os.path.basename(fd.name))))
            with self.assertSubprocessFailure(stderr_regexp='is a folder but the -r/--recursive option was not given', exit_code=1):
                run("dx download "+os.path.basename(wd))
            old_dir = os.getcwd()
            with chdir(tempfile.mkdtemp()):
                run("dx download -r "+os.path.basename(wd))

                tree1 = check_output("cd {wd}; find .".format(wd=wd), shell=True)
                tree2 = check_output("cd {wd}; find .".format(wd=os.path.basename(wd)), shell=True)
                self.assertEqual(tree1, tree2)

            with chdir(tempfile.mkdtemp()):
                os.mkdir('t')
                run("dx download -r -o t "+os.path.basename(wd))
                tree1 = check_output("cd {wd}; find .".format(wd=wd), shell=True)
                tree2 = check_output("cd {wd}; find .".format(wd=os.path.join("t",
                                                                              os.path.basename(wd))),
                                     shell=True)
                self.assertEqual(tree1, tree2)

                os.mkdir('t2')
                run("dx download -o t2 "+os.path.join(os.path.basename(wd), "a", "б",
                                                      os.path.basename(fd.name)))
                self.assertEqual(os.stat(os.path.join("t2", os.path.basename(fd.name))).st_size,
                                 len("0123456789ABCDEF"*64))

            with chdir(tempfile.mkdtemp()), temporary_project('dx download test proj') as other_project:
                run("dx mkdir /super/")
                run("dx mv '{}' /super/".format(os.path.basename(wd)))

                # Specify an absolute path in another project
                with select_project(other_project):
                    run("dx download -r '{proj}:/super/{path}'".format(proj=self.project, path=os.path.basename(wd)))

                    tree1 = check_output("cd {wd} && find .".format(wd=wd), shell=True)
                    tree2 = check_output("cd {wd} && find .".format(wd=os.path.basename(wd)), shell=True)
                    self.assertEqual(tree1, tree2)

                # Now specify a relative path in the same project
                with chdir(tempfile.mkdtemp()), select_project(self.project):
                    run("dx download -r super/{path}/".format(path=os.path.basename(wd)))

                    tree3 = check_output("cd {wd} && find .".format(wd=os.path.basename(wd)), shell=True)
                    self.assertEqual(tree1, tree3)

            with self.assertSubprocessFailure(stderr_regexp="paths are both file and folder names", exit_code=1):
                cmd = "dx cd {d}; dx mkdir {f}; dx download -r {f}*"
                run(cmd.format(d=os.path.join("/super", os.path.basename(wd), "a", "б"),
                               f=os.path.basename(fd.name)))

    def test_dx_upload_with_upload_perm(self):
        with temporary_project('test proj with UPLOAD perms', reclaim_permissions=True) as temp_project:
            temp_project.decrease_perms(dxpy.whoami(), 'UPLOAD')
            testdir = tempfile.mkdtemp()
            try:
                # Filename provided with path
                with open(os.path.join(testdir, 'myfilename'), 'w') as f:
                    f.write('foo')
                remote_file = dxpy.upload_local_file(filename=os.path.join(testdir, 'myfilename'),
                                                     project=temp_project.get_id(), folder='/')
                self.assertEqual(remote_file.name, 'myfilename')
                # Filename provided with file handle
                remote_file2 = dxpy.upload_local_file(file=open(os.path.join(testdir, 'myfilename')),
                                                      project=temp_project.get_id(), folder='/')
                self.assertEqual(remote_file2.name, 'myfilename')
            finally:
                shutil.rmtree(testdir)

    @unittest.skipUnless(testutil.TEST_ENV,
                         'skipping test that would clobber your local environment')
    def test_dx_download_no_env(self):
        testdir = tempfile.mkdtemp()
        with tempfile.NamedTemporaryFile(dir=testdir) as fd:
            fd.write("foo")
            fd.flush()
            file_id = run("dx upload " + fd.name + " --brief --wait").strip()
            self.assertTrue(file_id.startswith('file-'))

            # unset environment
            from dxpy.utils.env import write_env_var
            write_env_var('DX_PROJECT_CONTEXT_ID', None)
            del os.environ['DX_PROJECT_CONTEXT_ID']
            self.assertNotIn('DX_PROJECT_CONTEXT_ID', run('dx env --bash'))

            # download file
            output_path = os.path.join(testdir, 'output')
            run('dx download ' + file_id + ' -o ' + output_path)
            run('cmp ' + output_path + ' ' + fd.name)

    def test_dx_make_download_url(self):
        testdir = tempfile.mkdtemp()
        output_testdir = tempfile.mkdtemp()
        with tempfile.NamedTemporaryFile(dir=testdir) as fd:
            fd.write("foo")
            fd.flush()
            file_id = run("dx upload " + fd.name + " --brief --wait").strip()
            self.assertTrue(file_id.startswith('file-'))

            # download file
            download_url = run("dx make_download_url " + file_id).strip()
            run("wget -P " + output_testdir + " " + download_url)
            run('cmp ' + os.path.join(output_testdir, os.path.basename(fd.name)) + ' ' + fd.name)

            # download file with a different name
            download_url = run("dx make_download_url " + file_id + " --filename foo")
            run("wget -P " + output_testdir + " " + download_url)
            run('cmp ' + os.path.join(output_testdir, "foo") + ' ' + fd.name)

    def test_dx_upload_mult_paths(self):
        testdir = tempfile.mkdtemp()
        os.mkdir(os.path.join(testdir, 'a'))
        with tempfile.NamedTemporaryFile(dir=testdir) as fd:
            fd.write("root-file")
            fd.flush()
            with tempfile.NamedTemporaryFile(dir=os.path.join(testdir, "a")) as fd2:
                fd2.write("a-file")
                fd2.flush()

                run(("dx upload -r {testdir}/{rootfile} {testdir}/a " +
                     "--wait").format(testdir=testdir, rootfile=os.path.basename(fd.name)))
                listing = run("dx ls").split("\n")
                self.assertIn("a/", listing)
                self.assertIn(os.path.basename(fd.name), listing)
                listing = run("dx ls a").split("\n")
                self.assertIn(os.path.basename(fd2.name), listing)

    def test_dx_upload_mult_paths_with_dest(self):
        testdir = tempfile.mkdtemp()
        os.mkdir(os.path.join(testdir, 'a'))
        with tempfile.NamedTemporaryFile(dir=testdir) as fd:
            fd.write("root-file")
            fd.flush()
            with tempfile.NamedTemporaryFile(dir=os.path.join(testdir, "a")) as fd2:
                fd2.write("a-file")
                fd2.flush()

                run("dx mkdir /destdir")
                run(("dx upload -r {testdir}/{rootfile} {testdir}/a --destination /destdir " +
                     "--wait").format(testdir=testdir, rootfile=os.path.basename(fd.name)))
                listing = run("dx ls /destdir/").split("\n")
                self.assertIn("a/", listing)
                self.assertIn(os.path.basename(fd.name), listing)
                listing = run("dx ls /destdir/a").split("\n")
                self.assertIn(os.path.basename(fd2.name), listing)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, "Skipping test that would run jobs")
    def test_dx_download_by_job_id_and_output_field(self):
        test_project_name = 'PTFM-13437'
        test_file_name = 'test_file_01'
        expected_result = 'asdf1234...'
        with temporary_project(test_project_name, select=True) as temp_project:
            temp_project_id = temp_project.get_id()

            # Create and run minimal applet to generate output file.
            code_str = """import dxpy
@dxpy.entry_point('main')
def main():
    test_file_01 = dxpy.upload_string('{exp_res}', name='{filename}')
    output = {{}}
    output['{filename}'] = dxpy.dxlink(test_file_01)
    return output
dxpy.run()
"""
            code_str = code_str.format(exp_res=expected_result, filename=test_file_name)
            app_spec = {"name": "test_applet_dx_download_by_jbor",
                        "project": temp_project_id,
                        "dxapi": "1.0.0",
                        "inputSpec": [],
                        "outputSpec": [{"name": test_file_name, "class": "file"}],
                        "runSpec": {"code": code_str, "interpreter": "python2.7"},
                        "version": "1.0.0"}
            applet_id = dxpy.api.applet_new(app_spec)['id']
            applet = dxpy.DXApplet(applet_id)
            job = applet.run({}, project=temp_project_id)
            job.wait_on_done()
            job_id = job.get_id()

            # Case: Correctly specify "<job_id>:<output_field>"; save to file.
            with chdir(tempfile.mkdtemp()):
                run("dx download " + job_id + ":" + test_file_name)
                with open(test_file_name) as fh:
                    result = fh.read()
                    self.assertEqual(expected_result, result)

            # Case: Correctly specify file id; print to stdout.
            test_file_id = dxpy.DXFile(job.describe()['output'][test_file_name]).get_id()
            result = run("dx download " + test_file_id + " -o -").strip()
            self.assertEqual(expected_result, result)

            # Case: Correctly specify file name; print to stdout.
            result = run("dx download " + test_file_name + " -o -").strip()
            self.assertEqual(expected_result, result)

            # Case: Correctly specify "<job_id>:<output_field>"; print to stdout.
            result = run("dx download " + job_id + ":" + test_file_name + " -o -").strip()
            self.assertEqual(expected_result, result)

            # Case: File does not exist.
            with self.assertSubprocessFailure(stderr_regexp="Could not resolve", exit_code=1):
                run("dx download foo -o -")

            # Case: Invalid output field name when specifying <job_id>:<output_field>.
            with self.assertSubprocessFailure(stderr_regexp="Could not find", exit_code=3):
                run("dx download " + job_id + ":foo -o -")

class TestDXClientDescribe(DXTestCase):
    def test_projects(self):
        run("dx describe :")
        run("dx describe " + self.project)
        run("dx describe " + self.project + ":")

        # need colon to recognize as project name
        with self.assertSubprocessFailure(exit_code=3):
            run("dx describe dxclient_test_pröject")

        # bad project name
        with self.assertSubprocessFailure(exit_code=3):
            run("dx describe dne:")

        # nonexistent project ID
        with self.assertSubprocessFailure(exit_code=3):
            run("dx describe project-123456789012345678901234")

    def test_bad_current_project(self):
        with self.assertSubprocessFailure(stderr_regexp='No matches found', exit_code=3):
            run("dx describe nonexistent --project-context-id foo")

        run("dx describe " + self.project + " --project-context-id foo")

@unittest.skipUnless(testutil.TEST_RUN_JOBS,
                     'skipping tests that would run jobs')
class TestDXClientRun(DXTestCase):
    def setUp(self):
        self.other_proj_id = run("dx new project other --brief").strip()
        super(TestDXClientRun, self).setUp()

    def tearDown(self):
        dxpy.api.project_destroy(self.other_proj_id, {'terminateJobs': True})
        super(TestDXClientRun, self).tearDown()

    def test_dx_run_no_hidden_executables(self):
        # hidden applet
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [],
                                         "outputSpec": [],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"},
                                         "hidden": True,
                                         "name": "hidden_applet"})['id']
        run("dx describe hidden_applet")
        with self.assertSubprocessFailure(stderr_regexp='No matches found', exit_code=3):
            run("dx run hidden_applet")
        # by ID will still work
        run("dx run " + applet_id + " -y")

        # hidden workflow
        dxworkflow = dxpy.new_dxworkflow(name="hidden_workflow", hidden=True)
        dxworkflow.add_stage(applet_id)
        with self.assertSubprocessFailure(stderr_regexp='No matches found', exit_code=3):
            run("dx run hidden_workflow")
        # by ID will still work
        run("dx run " + dxworkflow.get_id() + " -y")

    def test_dx_run_jbor_array_ref(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "name": "myapplet",
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "record",
                                                        "class": "record",
                                                        "optional": True}],
                                         "outputSpec": [{"name": "record",
                                                         "class": "record"},
                                                        {"name": "record_array",
                                                         "class": "array:record"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "bundledDepends": [],
                                                     "execDepends": [],
                                                     "code": '''
first_record=$(dx new record firstrecord --brief)
dx close $first_record
second_record=$(dx new record secondrecord --brief)
dx close $second_record
dx-jobutil-add-output record $first_record
dx-jobutil-add-output record_array $first_record --array
dx-jobutil-add-output record_array $second_record --array
'''}})["id"]

        remote_job = dxpy.DXApplet(applet_id).run({})
        remote_job.wait_on_done()
        remote_job_output = remote_job.describe()["output"]["record_array"]

        # check other dx functionality here for convenience
        # dx describe/path resolution
        jbor_array_ref = '{job_id}:record_array.'.format(job_id=remote_job.get_id())
        desc_output = run('dx describe ' + jbor_array_ref + '0')
        self.assertIn("firstrecord", desc_output)
        self.assertNotIn("secondrecord", desc_output)
        with self.assertSubprocessFailure(exit_code=3):
            run("dx get " + remote_job.get_id() + ":record.foo")
        with self.assertSubprocessFailure(stderr_regexp='not an array', exit_code=3):
            run("dx get " + remote_job.get_id() + ":record.0")
        with self.assertSubprocessFailure(stderr_regexp='out of range', exit_code=3):
            run("dx get " + jbor_array_ref + '2')

        # dx run
        second_remote_job = run('dx run myapplet -y --brief -irecord=' + jbor_array_ref + '1').strip()
        second_remote_job_desc = run('dx describe ' + second_remote_job)
        self.assertIn(jbor_array_ref + '1', second_remote_job_desc)
        self.assertIn(remote_job_output[1]["$dnanexus_link"], second_remote_job_desc)
        self.assertNotIn(remote_job_output[0]["$dnanexus_link"], second_remote_job_desc)

        # use dx get to hydrate a directory and test dx-run-app-locally
        def create_app_dir_from_applet(applet_id):
            tempdir = tempfile.mkdtemp()
            with chdir(tempdir):
                run('dx get ' + applet_id)
                return os.path.join(tempdir, dxpy.describe(applet_id)['name'])
        appdir = create_app_dir_from_applet(applet_id)
        local_output = check_output(['dx-run-app-locally', appdir, '-irecord=' + jbor_array_ref + '1'])
        self.assertIn(remote_job_output[1]["$dnanexus_link"], local_output)
        self.assertNotIn(remote_job_output[0]["$dnanexus_link"], local_output)

    def test_dx_run_priority(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "name": "myapplet",
                                         "dxapi": "1.0.0",
                                         "runSpec": {"interpreter": "bash",
                                                     "code": ""},
                                         "access": {"project": "VIEW",
                                                    "allProjects": "VIEW",
                                                    "network": []}})["id"]
        normal_job_id = run("dx run myapplet --priority normal --brief -y").strip()
        normal_job_desc = dxpy.describe(normal_job_id)
        self.assertEqual(normal_job_desc["priority"], "normal")

        high_priority_job_id = run("dx run myapplet --priority high --brief -y").strip()
        high_priority_job_desc = dxpy.describe(high_priority_job_id)
        self.assertEqual(high_priority_job_desc["priority"], "high")

        # don't actually need these to run
        run("dx terminate " + normal_job_id)
        run("dx terminate " + high_priority_job_id)

        # --watch implies --priority high
        try:
            run("dx run myapplet -y --watch")
        except subprocess.CalledProcessError:
            # ignore any watching errors; just want to test requested
            # priority
            pass
        watched_job_id = run("dx find jobs -n 1 --brief").strip()
        self.assertNotIn(watched_job_id, [normal_job_id, high_priority_job_id])
        watched_job_desc = dxpy.describe(watched_job_id)
        self.assertEqual(watched_job_desc['applet'], applet_id)
        self.assertEqual(watched_job_desc['priority'], 'high')

        # errors
        with self.assertSubprocessFailure(exit_code=2):
            # expect argparse error code 2 for bad choice
            run("dx run myapplet --priority standard")

        # no warning when no special access requested
        dx_run_output = run("dx run myapplet --priority normal -y")
        for string in ["WARNING", "developer", "Internet", "write access"]:
            self.assertNotIn(string, dx_run_output)

        # test for printing a warning when extra permissions are
        # requested and run as normal priority
        extra_perms_applet = dxpy.api.applet_new({"project": self.project,
                                                  "dxapi": "1.0.0",
                                                  "runSpec": {"interpreter": "bash",
                                                              "code": ""},
                                                  "access": {"developer": True,
                                                             "project": "UPLOAD",
                                                             "network": ["github.com"]}})["id"]
        # no warning when running at high priority
        dx_run_output = run("dx run " + extra_perms_applet + " --priority high -y")
        for string in ["WARNING", "developer", "Internet", "write access"]:
            self.assertNotIn(string, dx_run_output)

        # warning when running at normal priority; mention special
        # permissions present
        dx_run_output = run("dx run " + extra_perms_applet + " --priority normal -y")
        for string in ["WARNING", "developer", "Internet", "write access"]:
            self.assertIn(string, dx_run_output)

        # no warning with --brief
        dx_run_output = run("dx run " + extra_perms_applet + " --priority normal --brief -y")
        self.assertRegexpMatches(dx_run_output.strip(), '^job-[0-9a-zA-Z]{24}$')

        # test with allProjects set but no explicit permissions to the
        # project context
        extra_perms_applet = dxpy.api.applet_new({"project": self.project,
                                                  "dxapi": "1.0.0",
                                                  "inputSpec": [],
                                                  "outputSpec": [],
                                                  "runSpec": {"interpreter": "bash",
                                                              "code": ""},
                                                  "access": {"allProjects": "CONTRIBUTE"}})["id"]
        # no warning when running at high priority
        dx_run_output = run("dx run " + extra_perms_applet + " --priority high -y")
        for string in ["WARNING", "developer", "Internet", "write access"]:
            self.assertNotIn(string, dx_run_output)

        # warning when running at normal priority; mention special
        # permissions present
        dx_run_output = run("dx run " + extra_perms_applet + " --priority normal -y")
        for string in ["WARNING", "write access"]:
            self.assertIn(string, dx_run_output)
        for string in ["developer", "Internet"]:
            self.assertNotIn(string, dx_run_output)

        # workflow tests

        workflow_id = run("dx new workflow myworkflow --brief").strip()
        run("dx add stage {workflow} {applet}".format(workflow=workflow_id,
                                                      applet=extra_perms_applet))
        # no warning when run at high priority
        dx_run_output = run("dx run myworkflow --priority high -y")
        for string in ["WARNING", "developer", "Internet", "write access"]:
            self.assertNotIn(string, dx_run_output)
        # and check that priority was set properly
        time.sleep(1)
        analysis_id = run("dx find analyses -n 1 --brief").strip()
        self.assertEqual(dxpy.describe(analysis_id)["priority"], "high")
        # get warnings when run at normal priority
        dx_run_output = run("dx run myworkflow --priority normal -y")
        for string in ["WARNING", "write access"]:
            self.assertIn(string, dx_run_output)
        for string in ["developer", "Internet"]:
            self.assertNotIn(string, dx_run_output)
        # and check that priority was set properly
        time.sleep(1)
        analysis_id = run("dx find analyses -n 1 --brief").strip()
        self.assertEqual(dxpy.describe(analysis_id)["priority"], "normal")

    def test_dx_run_tags_and_properties(self):
        # success
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"}
                                         })['id']
        property_names = ["$my.prop", "secoиdprop", "тhird prop"]
        property_values = ["$hello.world", "Σ2,n", "stuff"]
        the_tags = ["Σ1=n", "helloo0", "ωω"]
        job_id = run("dx run " + applet_id + ' -inumber=32 --brief -y ' +
                     " ".join(["--property '" + prop[0] + "'='" + prop[1] + "'" for
                               prop in zip(property_names, property_values)]) +
                     "".join([" --tag " + tag for tag in the_tags])).strip()

        job_desc = dxpy.api.job_describe(job_id)
        self.assertEqual(job_desc['tags'].sort(), the_tags.sort())
        self.assertEqual(len(job_desc['properties']), 3)
        for name, value in zip(property_names, property_values):
            self.assertEqual(job_desc['properties'][name], value)

        # Test setting tags and properties afterwards
        run("dx tag " + job_id + " foo bar foo")
        run("dx set_properties " + job_id + " foo=bar Σ_1^n=n")
        job_desc_lines = run("dx describe " + job_id + " --delim ' '").splitlines()
        found_tags = False
        found_properties = False
        for line in job_desc_lines:
            if line.startswith('Tags'):
                self.assertIn("foo", line)
                self.assertIn("bar", line)
                found_tags = True
            if line.startswith('Properties'):
                self.assertIn("foo=bar", line)
                self.assertIn("Σ_1^n=n", line)
                found_properties = True
        self.assertTrue(found_tags)
        self.assertTrue(found_properties)
        run("dx untag " + job_id + " foo")
        run("dx unset_properties " + job_id + " Σ_1^n")
        job_desc = json.loads(run("dx describe " + job_id + " --json"))
        self.assertIn("bar", job_desc['tags'])
        self.assertNotIn("foo", job_desc['tags'])
        self.assertEqual(job_desc["properties"]["foo"], "bar")
        self.assertNotIn("Σ_1^n", job_desc["properties"])

    def test_dx_run_extra_args(self):
        # success
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"}
                                         })['id']
        job_id = run("dx run " + applet_id + " -inumber=32 --name overwritten_name " +
                     '--delay-workspace-destruction --brief -y ' +
                     '--extra-args \'{"input": {"second": true}, "name": "new_name"}\'').strip()
        job_desc = dxpy.api.job_describe(job_id)
        self.assertTrue(job_desc['delayWorkspaceDestruction'])
        self.assertEqual(job_desc['name'], 'new_name')
        self.assertIn('number', job_desc['input'])
        self.assertEqual(job_desc['input']['number'], 32)
        self.assertIn('second', job_desc['input'])
        self.assertEqual(job_desc['input']['second'], True)

        # parsing error
        with self.assertSubprocessFailure(stderr_regexp='JSON', exit_code=3):
            run("dx run " + applet_id + " --extra-args not-a-JSON-string")

    def test_dx_run_clone(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"}
                                         })['id']
        other_applet_id = dxpy.api.applet_new({"project": self.project,
                                               "dxapi": "1.0.0",
                                               "runSpec": {"interpreter": "bash",
                                                           "code": "echo 'hello'"}
                                           })['id']

        def check_new_job_metadata(new_job_desc, cloned_job_desc, overridden_fields=[]):
            '''
            :param new_job_desc: the describe hash in the new job
            :param cloned_job_desc: the description of the job that was cloned
            :param overridden_fields: the metadata fields in describe that were overridden (and should not be checked)
            '''
            # check clonedFrom hash in new job's details
            self.assertIn('clonedFrom', new_job_desc['details'])
            self.assertEqual(new_job_desc['details']['clonedFrom']['id'], cloned_job_desc['id'])
            self.assertEqual(new_job_desc['details']['clonedFrom']['executable'],
                             cloned_job_desc.get('applet') or cloned_job_desc.get('app'))
            for metadata in ['project', 'folder', 'name', 'runInput', 'systemRequirements']:
                self.assertEqual(new_job_desc['details']['clonedFrom'][metadata],
                                 cloned_job_desc[metadata])
            # check not_overridden_fields match/have the correct transformation
            all_fields = set(['name', 'project', 'folder', 'input', 'systemRequirements',
                              'applet', 'tags', 'properties', 'priority'])
            fields_to_check = all_fields.difference(overridden_fields)
            for metadata in fields_to_check:
                if metadata == 'name':
                    self.assertEqual(new_job_desc[metadata], cloned_job_desc[metadata] + ' (re-run)')
                else:
                    self.assertEqual(new_job_desc[metadata], cloned_job_desc[metadata])

        # originally, set everything and have an instance type for all
        # entry points
        orig_job_id = run("dx run " + applet_id +
                          ' -inumber=32 --name jobname --folder /output ' +
                          '--instance-type mem2_hdd2_x2 ' +
                          '--tag Ψ --tag $hello.world ' +
                          '--property Σ_1^n=n --property $hello.=world ' +
                          '--priority normal ' +
                          '--brief -y').strip()
        orig_job_desc = dxpy.api.job_describe(orig_job_id)
        # control
        self.assertEqual(orig_job_desc['name'], 'jobname')
        self.assertEqual(orig_job_desc['project'], self.project)
        self.assertEqual(orig_job_desc['folder'], '/output')
        self.assertEqual(orig_job_desc['input'], {'number': 32})
        self.assertEqual(orig_job_desc['systemRequirements'], {'*': {'instanceType': 'mem2_hdd2_x2'}})

        # clone the job

        # nothing different
        new_job_desc = dxpy.api.job_describe(run("dx run --clone " + orig_job_id +
                                                 " --brief -y").strip())
        check_new_job_metadata(new_job_desc, orig_job_desc)

        def get_new_job_desc(cmd_suffix):
            new_job_id = run("dx run --clone " + orig_job_id + " --brief -y " + cmd_suffix).strip()
            return dxpy.api.job_describe(new_job_id)

        # override applet
        new_job_desc = get_new_job_desc(other_applet_id)
        self.assertEqual(new_job_desc['applet'], other_applet_id)
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['applet'])

        # override name
        new_job_desc = get_new_job_desc("--name newname")
        self.assertEqual(new_job_desc['name'], 'newname')
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['name'])

        # override tags
        new_job_desc = get_new_job_desc("--tag new_tag --tag second_new_tag")
        self.assertEqual(new_job_desc['tags'], ['new_tag', 'second_new_tag'])
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['tags'])

        # override properties
        new_job_desc = get_new_job_desc("--property foo=bar --property baz=quux")
        self.assertEqual(new_job_desc['properties'], {"foo": "bar", "baz": "quux"})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['properties'])

        # override priority
        new_job_desc = get_new_job_desc("--priority high")
        self.assertEqual(new_job_desc['priority'], "high")
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['priority'])

        # override folder
        new_job_desc = get_new_job_desc("--folder /otherfolder")
        self.assertEqual(new_job_desc['folder'], '/otherfolder')
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['folder'])

        # override project
        new_job_desc = get_new_job_desc("--project " + self.other_proj_id)
        self.assertEqual(new_job_desc['project'], self.other_proj_id)
        self.assertEqual(new_job_desc['folder'], '/output')
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['project', 'folder'])

        # override project and folder
        new_job_desc = get_new_job_desc("--folder " + self.other_proj_id + ":")
        self.assertEqual(new_job_desc['project'], self.other_proj_id)
        self.assertEqual(new_job_desc['folder'], '/')
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['project', 'folder'])

        # override input with -i
        new_job_desc = get_new_job_desc("-inumber=42")
        self.assertEqual(new_job_desc['input'], {"number": 42})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['input'])

        # add other input fields with -i
        new_job_desc = get_new_job_desc("-inumber2=42")
        self.assertEqual(new_job_desc['input'], {"number": 32, "number2": 42})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['input'])

        # override input with --input-json (original input discarded)
        new_job_desc = get_new_job_desc("--input-json '{\"number2\": 42}'")
        self.assertEqual(new_job_desc['input'], {"number2": 42})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['input'])

        # override the blanket instance type
        new_job_desc = get_new_job_desc("--instance-type mem2_hdd2_x1")
        self.assertEqual(new_job_desc['systemRequirements'],
                         {'*': {'instanceType': 'mem2_hdd2_x1'}})
        check_new_job_metadata(new_job_desc, orig_job_desc,
                               overridden_fields=['systemRequirements'])

        # override instance type for specific entry point(s)
        new_job_desc = get_new_job_desc("--instance-type '" +
                                        json.dumps({"some_ep": "mem2_hdd2_x1",
                                                    "some_other_ep": "mem2_hdd2_x4"}) + "'")
        self.assertEqual(new_job_desc['systemRequirements'],
                         {'*': {'instanceType': 'mem2_hdd2_x2'},
                          'some_ep': {'instanceType': 'mem2_hdd2_x1'},
                          'some_other_ep': {'instanceType': 'mem2_hdd2_x4'}})
        check_new_job_metadata(new_job_desc, orig_job_desc,
                               overridden_fields=['systemRequirements'])

        # new original job with entry point-specific systemRequirements
        orig_job_id = run("dx run " + applet_id +
                          " --instance-type '{\"some_ep\": \"mem2_hdd2_x1\"}' --brief -y").strip()
        orig_job_desc = dxpy.api.job_describe(orig_job_id)
        self.assertEqual(orig_job_desc['systemRequirements'],
                         {'some_ep': {'instanceType': 'mem2_hdd2_x1'}})

        # override all entry points
        new_job_desc = get_new_job_desc("--instance-type mem2_hdd2_x2")
        self.assertEqual(new_job_desc['systemRequirements'], {'*': {'instanceType': 'mem2_hdd2_x2'}})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['systemRequirements'])

        # override a different entry point; original untouched
        new_job_desc = get_new_job_desc("--instance-type '{\"some_other_ep\": \"mem2_hdd2_x2\"}'")
        self.assertEqual(new_job_desc['systemRequirements'],
                         {'some_ep': {'instanceType': 'mem2_hdd2_x1'},
                          'some_other_ep': {'instanceType': 'mem2_hdd2_x2'}})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['systemRequirements'])

        # override the same entry point
        new_job_desc = get_new_job_desc("--instance-type '{\"some_ep\": \"mem2_hdd2_x2\"}'")
        self.assertEqual(new_job_desc['systemRequirements'],
                         {'some_ep': {'instanceType': 'mem2_hdd2_x2'}})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['systemRequirements'])

    def test_dx_describe_job_with_resolved_jbors(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "array", "class": "array:int"}],
                                         "outputSpec": [{"name": "array", "class": "array:int"}],
                                         "runSpec": {"interpreter": "python2.7",
                                                     "code": '''#!/usr/bin/env python

@dxpy.entry_point('main')
def main(array):
    output = {"array": array}
    return output
'''}})['id']
        first_job_handler = dxpy.DXJob(dxpy.api.applet_run(applet_id,
                                                           {"project": self.project,
                                                            "input": {"array": [0, 1, 5]}})['id'])

        # Launch a second job which depends on the first, using two
        # arrays in an array (to be flattened) as input
        second_job_run_input = {"project": self.project,
                                "input": {"array": [first_job_handler.get_output_ref("array"),
                                                    first_job_handler.get_output_ref("array")]}}
        second_job_handler = dxpy.DXJob(dxpy.api.applet_run(applet_id, second_job_run_input)['id'])
        first_job_handler.wait_on_done()
        # Need to wait for second job to become runnable (idle and
        # waiting_on_input are the only states before it becomes
        # runnable)
        while second_job_handler.describe()['state'] in ['idle', 'waiting_on_input']:
            time.sleep(0.1)
        second_job_desc = run("dx describe " + second_job_handler.get_id())
        first_job_res = first_job_handler.get_id() + ":array => [ 0, 1, 5 ]"
        self.assertIn(first_job_res, second_job_desc)

        # Launch another job which depends on the first done job and
        # the second (not-done) job; the first job can and should be
        # mentioned in the resolved JBORs list, but the second
        # shouldn't.
        third_job_run_input = {"project": self.project,
                               "input": {"array": [first_job_handler.get_output_ref("array"),
                                                   first_job_handler.get_output_ref("array", index=2),
                                                   second_job_handler.get_output_ref("array")]}}
        third_job = dxpy.api.applet_run(applet_id, third_job_run_input)['id']
        third_job_desc = run("dx describe " + third_job)
        self.assertIn(first_job_res, third_job_desc)
        self.assertIn(first_job_handler.get_id() + ":array.2 => 5", third_job_desc)
        self.assertNotIn(second_job_handler.get_id() + ":array =>", third_job_desc)

    def test_dx_run_ssh_no_config(self):
        # Create minimal applet.
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [],
                                         "outputSpec": [],
                                         "runSpec": {"interpreter": "python2.7",
                                                     "code": '''#!/usr/bin/env python

@dxpy.entry_point('main')
def main():
    return
'''}})['id']

        # Case: Execute "dx run --ssh" before configuring SSH.
        path = tempfile.mkdtemp()
        shell = pexpect.spawn("dx run --ssh " + applet_id, env=dict(os.environ, DX_USER_CONF_DIR=path))
        shell.expect("Warning:")
        shell.sendline("N")
        shell.expect("IOError")
        shell.close()
        self.assertEqual(3, shell.exitstatus)


class TestDXClientWorkflow(DXTestCase):
    default_inst_type = "mem2_hdd2_x2"

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    def test_dx_run_workflow(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "number", "class": "int"}],
                                         "outputSpec": [{"name": "number", "class": "int"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "exit 1"}
                                         })['id']
        workflow_id = run("dx new workflow myworkflow --output-folder /foo --brief").strip()
        stage_id = dxpy.api.workflow_add_stage(workflow_id,
                                               {"editVersion": 0, "executable": applet_id})['stage']
        analysis_id = run("dx run " + workflow_id + " -i0.number=32 -y --brief").strip()
        self.assertTrue(analysis_id.startswith('analysis-'))
        analysis_desc = run("dx describe " + analysis_id)
        self.assertIn(stage_id + '.number = 32', analysis_desc)
        self.assertIn('foo', analysis_desc)
        analysis_desc = json.loads(run("dx describe " + analysis_id + " --json"))
        time.sleep(2) # May need to wait for job to be created in the system
        job_desc = run("dx describe " + analysis_desc["stages"][0]["execution"]["id"])
        self.assertIn(' number = 32', job_desc)

        # Test setting tags and properties on analysis
        run("dx tag " + analysis_id + " foo bar foo")
        run("dx set_properties " + analysis_id + " foo=bar Σ_1^n=n")
        analysis_desc_lines = run("dx describe " + analysis_id).splitlines()
        found_tags = False
        found_properties = False
        for line in analysis_desc_lines:
            if line.startswith('Tags'):
                self.assertIn("foo", line)
                self.assertIn("bar", line)
                found_tags = True
            if line.startswith('Properties'):
                self.assertIn("foo=bar", line)
                self.assertIn("Σ_1^n=n", line)
                found_properties = True
        self.assertTrue(found_tags)
        self.assertTrue(found_properties)
        run("dx untag " + analysis_id + " foo")
        run("dx unset_properties " + analysis_id + " Σ_1^n")
        analysis_desc = run("dx describe " + analysis_id + " --delim ' '")
        self.assertIn("Tags bar\n", analysis_desc)
        self.assertIn("Properties foo=bar\n", analysis_desc)

        # Missing input throws appropriate error
        with self.assertSubprocessFailure(stderr_regexp='Some inputs.+are missing', exit_code=3):
            run("dx run " + workflow_id + " -y")

        # Setting the input in the workflow allows it to be run
        run("dx update stage " + workflow_id + " 0 -inumber=42")
        run("dx run " + workflow_id + " -y")

        # initialize a new workflow from an analysis
        new_workflow_desc = run("dx new workflow --init " + analysis_id)
        self.assertNotIn(workflow_id, new_workflow_desc)
        self.assertIn(analysis_id, new_workflow_desc)
        self.assertIn(stage_id, new_workflow_desc)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that runs jobs')
    def test_dx_run_clone_analysis(self):
        dxpy.api.applet_new({
            "project": self.project,
            "name": "myapplet",
            "dxapi": "1.0.0",
            "inputSpec": [{"name": "number", "class": "int"}],
            "outputSpec": [{"name": "number", "class": "int"}],
            "runSpec": {"interpreter": "bash",
                        "code": "dx-jobutil-add-output number 32"}
        })

        # make a workflow with the stage twice
        run("dx new workflow myworkflow")
        run("dx add stage myworkflow myapplet -inumber=32 --instance-type mem2_hdd2_x2")
        run("dx add stage myworkflow myapplet -inumber=52 --instance-type mem2_hdd2_x1")

        # run it
        analysis_id = run("dx run myworkflow -y --brief").strip()

        # test cases
        no_change_analysis_id = run("dx run --clone " + analysis_id + " --brief -y").strip()
        change_an_input_analysis_id = run("dx run --clone " + analysis_id +
                                          " -i0.number=52 --brief -y").strip()
        change_inst_type_analysis_id = run("dx run --clone " + analysis_id +
                                           " --instance-type mem2_hdd2_x2 --brief -y").strip()

        time.sleep(2) # May need to wait for any new jobs to be created in the system

        # make assertions for test cases
        orig_analysis_desc = dxpy.describe(analysis_id)

        # no change: expect both stages to have reused jobs
        no_change_analysis_desc = dxpy.describe(no_change_analysis_id)
        self.assertEqual(no_change_analysis_desc['stages'][0]['execution']['id'],
                         orig_analysis_desc['stages'][0]['execution']['id'])
        self.assertEqual(no_change_analysis_desc['stages'][1]['execution']['id'],
                         orig_analysis_desc['stages'][1]['execution']['id'])

        # change an input: new job for that stage
        change_an_input_analysis_desc = dxpy.describe(change_an_input_analysis_id)
        self.assertEqual(change_an_input_analysis_desc['stages'][0]['execution']['input'],
                         {"number": 52})
        # second stage still the same
        self.assertEqual(change_an_input_analysis_desc['stages'][1]['execution']['id'],
                         orig_analysis_desc['stages'][1]['execution']['id'])

        # change inst type: only affects stage with different inst type
        change_inst_type_analysis_desc = dxpy.describe(change_inst_type_analysis_id)
        # first stage still the same
        self.assertEqual(change_inst_type_analysis_desc['stages'][0]['execution']['id'],
                         orig_analysis_desc['stages'][0]['execution']['id'])
        # second stage different
        self.assertNotEqual(change_inst_type_analysis_desc['stages'][1]['execution']['id'],
                            orig_analysis_desc['stages'][1]['execution']['id'])
        self.assertEqual(change_inst_type_analysis_desc['stages'][1]['execution']['instanceType'],
                         'mem2_hdd2_x2')

        # Run in a different project and add some metadata
        try:
            other_proj_id = run("dx new project 'cloned analysis project' --brief").strip()
            new_analysis_id = run("dx run --clone " + analysis_id + " --destination " + other_proj_id +
                                  ":foo --tag sometag --property propkey=propval " +
                                  "--brief -y").strip()
            new_analysis_desc = dxpy.describe(new_analysis_id)
            self.assertEqual(new_analysis_desc['project'], other_proj_id)
            self.assertEqual(new_analysis_desc['folder'], '/foo')
            self.assertEqual(new_analysis_desc['tags'], ['sometag'])
            self.assertEqual(new_analysis_desc['properties'], {'propkey': 'propval'})
            time.sleep(2)
            new_job_desc = dxpy.describe(new_analysis_desc['stages'][0]['execution']['id'])
            self.assertEqual(new_job_desc['project'], other_proj_id)
            self.assertEqual(new_job_desc['input']['number'], 32)
        finally:
            run("dx rmproject -y " + other_proj_id)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that runs jobs')
    def test_dx_run_workflow_prints_cached_executions(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "name": "myapplet",
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "number", "class": "int"}],
                                         "outputSpec": [{"name": "number", "class": "int"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "dx-jobutil-add-output number 32"}
                                         })['id']
        workflow_id = run("dx new workflow myworkflow --brief").strip()
        stage_id = run("dx add stage myworkflow myapplet --brief").strip()
        run_resp = dxpy.api.workflow_run(workflow_id,
                                         {"project": self.project,
                                          "input": {(stage_id + ".number"): 32}})
        first_analysis_id = run_resp['id']
        self.assertTrue(first_analysis_id.startswith('analysis-'))
        job_id = run_resp['stages'][0]
        self.assertTrue(job_id.startswith('job-'))

        # wait for events to propagate and for the job to be created
        time.sleep(2)

        # Running the workflow again with no changes should result in
        # the job getting reused
        run_output = run("dx run " + workflow_id + " -i0.number=32 -y").strip()
        self.assertIn('will reuse results from a previous analysis', run_output)
        self.assertIn(job_id, run_output)
        second_analysis_id = run_output[run_output.rfind('analysis-'):]
        self.assertNotEqual(first_analysis_id, second_analysis_id)

        # Running the workflow again with changes to the input should
        # NOT result in the job getting reused
        run_output = run("dx run " + workflow_id + " -i0.number=52 -y").strip()
        self.assertNotIn('will reuse results from a previous analysis', run_output)
        self.assertNotIn(job_id, run_output)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that runs jobs')
    def test_dx_run_workflow_with_inst_type_requests(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "name": "myapplet",
                                         "dxapi": "1.0.0",
                                         "inputSpec": [],
                                         "outputSpec": [],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": ""}
                                         })['id']
        workflow_id = run("dx new workflow myworkflow --brief").strip()
        stage_ids = [run("dx add stage myworkflow myapplet --name 'an=awful=name' --brief").strip(),
                     run("dx add stage myworkflow myapplet --name 'second' --brief").strip()]

        # control (no request)
        no_req_id = run('dx run myworkflow -y --brief').strip()
        # request for all stages
        all_stg_req_id = run('dx run myworkflow --instance-type mem2_hdd2_x1 -y --brief').strip()

        # request for a stage specifically (by name)
        stg_req_id = run('dx run myworkflow --instance-type an=awful=name=mem2_hdd2_x2 ' +
                         '--instance-type second=mem2_hdd2_x1 -y --brief').strip()

        time.sleep(2) # give time for all jobs to be populated

        no_req_desc = dxpy.describe(no_req_id)
        self.assertEqual(no_req_desc['stages'][0]['execution']['instanceType'],
                         self.default_inst_type)
        self.assertEqual(no_req_desc['stages'][1]['execution']['instanceType'],
                         self.default_inst_type)
        all_stg_req_desc = dxpy.describe(all_stg_req_id)
        self.assertEqual(all_stg_req_desc['stages'][0]['execution']['instanceType'],
                         'mem2_hdd2_x1')
        self.assertEqual(all_stg_req_desc['stages'][1]['execution']['instanceType'],
                         'mem2_hdd2_x1')
        stg_req_desc = dxpy.describe(stg_req_id)
        self.assertEqual(stg_req_desc['stages'][0]['execution']['instanceType'],
                         'mem2_hdd2_x2')
        self.assertEqual(stg_req_desc['stages'][1]['execution']['instanceType'],
                         'mem2_hdd2_x1')

        # request for a stage specifically (by index); if same inst
        # type as before, should reuse results
        self.assertIn(stg_req_desc['stages'][0]['execution']['id'],
                      run('dx run myworkflow --instance-type 0=mem2_hdd2_x2 -y'))
        # and by stage ID
        self.assertIn(stg_req_desc['stages'][0]['execution']['id'],
                      run('dx run myworkflow --instance-type ' + stage_ids[0] + '=mem2_hdd2_x2 -y'))

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would attempt to run a job')
    def test_dx_run_workflow_with_stage_folders(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "name": "myapplet",
                                         "dxapi": "1.0.0",
                                         "inputSpec": [],
                                         "outputSpec": [],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": ""}
                                         })['id']
        workflow_id = run("dx new workflow myworkflow --brief").strip()
        stage_ids = [run("dx add stage myworkflow myapplet --name 'a_simple_name' " +
                         "--output-folder /foo --brief").strip(),
                     run("dx add stage myworkflow myapplet --name 'second' " +
                         "--relative-output-folder foo --brief").strip()]

        cmd = 'dx run myworkflow --folder /output -y --brief --rerun-stage "*" '

        # control (no runtime request for stage folders)
        no_req_id = run(cmd).strip()
        # request for all stages
        all_stg_folder_id = run(cmd + '--stage-output-folder "*" bar').strip()
        all_stg_rel_folder_id = run(cmd + '--stage-relative-output-folder "*" /bar').strip()
        # request for stage specifically (by name)
        per_stg_folders_id = run(cmd + '--stage-relative-output-folder a_simple_name /baz ' + # as "baz"
                                 '--stage-output-folder second baz').strip() # resolves as ./baz
        # request for stage specifically (by index)
        per_stg_folders_id_2 = run(cmd + '--stage-output-folder 1 quux ' +
                                   '--stage-relative-output-folder 0 /quux').strip()
        # only modify one
        per_stg_folders_id_3 = run(cmd + '--stage-output-folder ' + stage_ids[0] + ' /hello').strip()

        time.sleep(2) # give time for all jobs to be generated

        def expect_stage_folders(analysis_id, first_stage_folder, second_stage_folder):
            analysis_desc = dxpy.describe(analysis_id)
            self.assertEqual(analysis_desc['stages'][0]['execution']['folder'],
                             first_stage_folder)
            self.assertEqual(analysis_desc['stages'][1]['execution']['folder'],
                             second_stage_folder)

        expect_stage_folders(no_req_id, '/foo', '/output/foo')
        expect_stage_folders(all_stg_folder_id, '/bar', '/bar')
        expect_stage_folders(all_stg_rel_folder_id, '/output/bar', '/output/bar')
        expect_stage_folders(per_stg_folders_id, '/output/baz', '/baz')
        expect_stage_folders(per_stg_folders_id_2, '/output/quux', '/quux')
        expect_stage_folders(per_stg_folders_id_3, '/hello', '/output/foo')

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would attempt to run a job')
    def test_inaccessible_stage(self):
        applet_id = dxpy.api.applet_new({"name": "myapplet",
                                         "project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "number", "class": "int"}],
                                         "outputSpec": [{"name": "number", "class": "int"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "exit 1"}
                                         })['id']
        workflow_id = run("dx new workflow myworkflow --brief").strip()
        run("dx add stage myworkflow myapplet")
        run("dx rm myapplet")

        # describe shows it
        desc = run("dx describe myworkflow")
        self.assertIn("inaccessible", desc)

        # list stages shows it
        list_output = run("dx list stages myworkflow")
        self.assertIn("inaccessible", list_output)

        # run refuses to run it
        with self.assertSubprocessFailure(stderr_regexp='following inaccessible stage\(s\)',
                                          exit_code=3):
            run("dx run myworkflow")

    def test_dx_new_workflow(self):
        workflow_id = run("dx new workflow --title=тitle --summary=SΨmmary --brief " +
                          "--description=DΣsc wØrkflØwname --output-folder /wØrkflØwØutput").strip()
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc["id"], workflow_id)
        self.assertEqual(desc["editVersion"], 0)
        self.assertEqual(desc["name"], "wØrkflØwname")
        self.assertEqual(desc["title"], "тitle")
        self.assertEqual(desc["summary"], "SΨmmary")
        self.assertEqual(desc["description"], "DΣsc")
        self.assertEqual(desc["outputFolder"], "/wØrkflØwØutput")
        self.assertEqual(desc["project"], self.project)

        # add some stages and then create a new one initializing from
        # the first
        applet_id = dxpy.api.applet_new({"name": "myapplet",
                                         "project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [],
                                         "outputSpec": [],
                                         "runSpec": {"interpreter": "bash", "code": ""}
                                         })['id']
        run("dx add stage wØrkflØwname " + applet_id)

        new_workflow_id = run("dx new workflow --init wØrkflØwname --title newtitle " +
                              "--summary newsummary --output-folder /output --brief").strip()
        desc = dxpy.describe(new_workflow_id)
        self.assertNotEqual(new_workflow_id, workflow_id)
        self.assertEqual(desc["id"], new_workflow_id)
        self.assertEqual(desc["editVersion"], 0)
        self.assertEqual(desc["name"], "wØrkflØwname")
        self.assertEqual(desc["title"], "newtitle")
        self.assertEqual(desc["summary"], "newsummary")
        self.assertEqual(desc["description"], "DΣsc")
        self.assertEqual(desc["outputFolder"], "/output")
        self.assertEqual(desc["project"], self.project)
        self.assertEqual(len(desc["stages"]), 1)
        self.assertEqual(desc["stages"][0]["executable"], applet_id)

        # run without --brief; should see initializedFrom information
        new_workflow_desc = run("dx new workflow --init " + workflow_id)
        self.assertIn(workflow_id, new_workflow_desc)

        # error when initializing from a nonexistent workflow
        run("dx rm " + workflow_id)
        with self.assertSubprocessFailure(stderr_regexp='could not be found', exit_code=3):
            run("dx new workflow --init " + workflow_id)

    def test_dx_workflow_resolution(self):
        with self.assertSubprocessFailure(stderr_regexp='Could not resolve', exit_code=3):
            run("dx update workflow foo")

        record_id = run("dx new record --type pipeline --brief").strip()
        run("dx describe " + record_id)
        with self.assertSubprocessFailure(stderr_regexp='Could not resolve', exit_code=3):
            run("dx update workflow " + record_id)

    def test_dx_describe_workflow(self):
        workflow_id = run("dx new workflow myworkflow --title title --brief").strip()
        desc = run("dx describe " + workflow_id)
        self.assertIn("Input Spec", desc)
        self.assertIn("Output Spec", desc)
        applet_id = dxpy.api.applet_new({"name": "myapplet",
                                         "project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "number", "class": "int"}],
                                         "outputSpec": [{"name": "number", "class": "int"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "exit 0"}
                                         })['id']
        first_stage = run("dx add stage " + workflow_id + " -inumber=10 " + applet_id +
                          " --brief").strip()
        desc = run("dx describe myworkflow")
        self.assertIn("Input Spec", desc)
        self.assertIn("default=10", desc)

    def test_dx_add_remove_list_stages(self):
        workflow_id = run("dx new workflow myworkflow --title title --brief").strip()
        run("dx describe " + workflow_id)
        applet_id = dxpy.api.applet_new({"name": "myapplet",
                                         "project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "number", "class": "int"}],
                                         "outputSpec": [{"name": "number", "class": "int"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "exit 0"}
                                         })['id']
        stage_ids = []

        # list stages when there are no stages yet
        list_output = run("dx list stages myworkflow")
        self.assertIn("No stages", list_output)

        stage_ids.append(run("dx add stage " + workflow_id + " --name first " + applet_id +
                             " --brief").strip())
        # not-yet-existing folder path should work
        # also, set input and instance type
        stage_ids.append(run("dx add stage myworkflow --relative-output-folder output myapplet " +
                             "--brief -inumber=32 --instance-type mem2_hdd2_x2").strip())
        # test relative folder path
        run("dx mkdir -p a/b/c")
        run("dx cd a/b/c")
        stage_ids.append(run("dx add stage " + workflow_id + " --name second --output-folder . " +
                             applet_id +
                             " --brief --instance-type '{\"main\": \"mem2_hdd2_x2\"}'").strip())
        with self.assertSubprocessFailure(stderr_regexp='not found in the input spec', exit_code=3):
            # input spec should be checked
            run("dx add stage " + workflow_id + " " + applet_id + " -inonexistent=42")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(len(desc['stages']), len(stage_ids))
        for i, stage_id in enumerate(stage_ids):
            self.assertEqual(desc['stages'][i]['id'], stage_id)
        self.assertEqual(desc['stages'][0]['folder'], None)
        self.assertEqual(desc['stages'][1]['folder'], 'output')
        self.assertEqual(desc['stages'][1]['input']['number'], 32)
        self.assertEqual(desc['stages'][1]['systemRequirements'],
                         {"*": {"instanceType": "mem2_hdd2_x2"}})
        self.assertEqual(desc['stages'][2]['folder'], '/a/b/c')
        self.assertEqual(desc['stages'][2]['systemRequirements'],
                         {"main": {"instanceType": "mem2_hdd2_x2"}})

        # errors
        # when adding a stage with both absolute and relative output folders
        with self.assertSubprocessFailure(stderr_regexp="output-folder", exit_code=2):
            run("dx add stage " + workflow_id + " " + applet_id +
                " --output-folder /foo --relative-output-folder foo")
        # bad executable that can't be found
        with self.assertSubprocessFailure(stderr_regexp="ResolutionError", exit_code=3):
            run("dx add stage " + workflow_id + " foo")
        # bad input
        with self.assertSubprocessFailure(stderr_regexp="parsed", exit_code=3):
            run("dx add stage " + workflow_id + " -inumber=foo " + applet_id)
        # bad instance type arg
        with self.assertSubprocessFailure(stderr_regexp="instance-type", exit_code=3):
            run("dx add stage " + workflow_id + " " + applet_id + " --instance-type {]")
        # unrecognized instance typ
        with self.assertSubprocessFailure(stderr_regexp="InvalidInput", exit_code=3):
            run("dx add stage " + workflow_id + " " + applet_id + " --instance-type foo")

        # list stages
        list_output = run("dx list stages " + workflow_id)
        self.assertIn("myworkflow (" + workflow_id + ")", list_output)
        self.assertIn("Title: title", list_output)
        self.assertIn("Output Folder: -", list_output)
        for i in range(0, len(stage_ids)):
            self.assertIn("Stage " + str(i), list_output)
        self.assertIn("<workflow output folder>/output", list_output)
        self.assertIn("number=32", list_output)
        self.assertIn("/a/b/c", list_output)

        run("dx describe " + workflow_id)
        # remove a stage by index
        remove_output = run("dx remove stage /myworkflow 1")
        self.assertIn(stage_ids[1], remove_output)
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(len(desc['stages']), 2)
        self.assertEqual(desc['stages'][0]['id'], stage_ids[0])
        self.assertEqual(desc['stages'][0]['folder'], None)
        self.assertEqual(desc['stages'][1]['id'], stage_ids[2])
        self.assertEqual(desc['stages'][1]['folder'], '/a/b/c')

        # remove a stage by ID
        remove_output = run("dx remove stage " + workflow_id + " " + stage_ids[0] + ' --brief').strip()
        self.assertEqual(remove_output, stage_ids[0])
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(len(desc['stages']), 1)
        self.assertEqual(desc['stages'][0]['id'], stage_ids[2])
        self.assertEqual(desc['stages'][0]['name'], 'second')
        self.assertEqual(desc['stages'][0]['folder'], '/a/b/c')

        # remove a stage by name
        run("dx remove stage " + workflow_id + " second")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(len(desc['stages']), 0)

        # remove something out of range
        with self.assertSubprocessFailure(stderr_regexp="out of range", exit_code=3):
            run("dx remove stage /myworkflow 5")

        # remove some bad stage ID
        with self.assertSubprocessFailure(stderr_regexp="nor found as a stage name", exit_code=3):
            run("dx remove stage /myworkflow badstageID")

        # remove nonexistent stage
        with self.assertSubprocessFailure(stderr_regexp="ResourceNotFound", exit_code=3):
            run("dx remove stage /myworkflow stage-123456789012345678901234")

    def test_dx_update_workflow(self):
        workflow_id = run("dx new workflow myworkflow --brief").strip()
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc['editVersion'], 0)
        self.assertEqual(desc['title'], "myworkflow")
        self.assertIsNone(desc["outputFolder"])

        # set title, summary, description, outputFolder
        run("dx update workflow myworkflow --title тitle --summary SΨmmary --description=DΣsc " +
            "--output-folder .")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc['editVersion'], 1)
        self.assertEqual(desc['title'], "тitle")
        self.assertEqual(desc['summary'], "SΨmmary")
        self.assertEqual(desc['description'], "DΣsc")
        self.assertEqual(desc['outputFolder'], "/")

        # describe
        describe_output = run("dx describe myworkflow --delim ' '")
        self.assertIn("Output Folder /", describe_output)

        # unset title, outputFolder
        run("dx update workflow myworkflow --no-title --no-output-folder")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc['editVersion'], 2)
        self.assertEqual(desc['title'], "myworkflow")
        self.assertIsNone(desc['outputFolder'])

        # describe
        describe_output = run("dx describe myworkflow --delim ' '")
        self.assertNotIn("Title тitle", describe_output)
        self.assertIn("Summary SΨmmary", describe_output)
        self.assertNotIn("Description", describe_output)
        self.assertNotIn("DΣsc", describe_output)
        self.assertIn("Output Folder -", describe_output)
        describe_output = run("dx describe myworkflow --verbose --delim ' '")
        self.assertIn("Description DΣsc", describe_output)

        # no-op
        output = run("dx update workflow myworkflow")
        self.assertIn("No updates requested", output)
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc['editVersion'], 2)
        self.assertEqual(desc['title'], "myworkflow")

        with self.assertSubprocessFailure(stderr_regexp="no-title", exit_code=2):
            run("dx update workflow myworkflow --title foo --no-title")
        with self.assertSubprocessFailure(stderr_regexp="no-title", exit_code=2):
            run("dx update workflow myworkflow --output-folder /foo --no-output-folder")

    def test_dx_update_stage(self):
        workflow_id = run("dx new workflow myworkflow --brief").strip()
        run("dx describe " + workflow_id)
        applet_id = dxpy.api.applet_new({"name": "myapplet",
                                         "project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "number", "class": "int"}],
                                         "outputSpec": [{"name": "number", "class": "int"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "exit 0"}
                                         })['id']
        stage_id = run("dx add stage " + workflow_id + " " + applet_id + " --brief").strip()
        empty_applet_id = dxpy.api.applet_new({"name": "emptyapplet",
                                               "project": self.project,
                                               "dxapi": "1.0.0",
                                               "inputSpec": [],
                                               "outputSpec": [],
                                               "runSpec": {"interpreter": "bash",
                                                           "code": "exit 0"}
                                           })['id']

        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertIsNone(desc["stages"][0]["name"])
        self.assertEqual(desc["stages"][0]["folder"], None)
        self.assertEqual(desc["stages"][0]["input"], {})
        self.assertEqual(desc["stages"][0]["systemRequirements"], {})

        # set the name, folder, some input, and the instance type
        run("dx update stage myworkflow 0 --name тitle -inumber=32 --relative-output-folder=foo " +
            "--instance-type mem2_hdd2_x2")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc["editVersion"], 2)
        self.assertEqual(desc["stages"][0]["name"], "тitle")
        self.assertEqual(desc["stages"][0]["folder"], "foo")
        self.assertEqual(desc["stages"][0]["input"]["number"], 32)
        self.assertEqual(desc["stages"][0]["systemRequirements"],
                         {"*": {"instanceType": "mem2_hdd2_x2"}})

        # use a relative folder path and also set instance type using JSON
        run("dx update stage myworkflow 0 --name тitle -inumber=32 --output-folder=. " +
            "--instance-type '{\"main\": \"mem2_hdd2_x2\"}'")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc["editVersion"], 3)
        self.assertEqual(desc["stages"][0]["folder"], "/")
        self.assertEqual(desc["stages"][0]["systemRequirements"],
                         {"main": {"instanceType": "mem2_hdd2_x2"}})

        # unset name
        run("dx update stage myworkflow " + stage_id + " --no-name")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc["editVersion"], 4)
        self.assertIsNone(desc["stages"][0]["name"])

        # set incompatible executable; expect a helpful error msg
        # telling us to use --force; then use it
        with self.assertSubprocessFailure(stderr_regexp="--force", exit_code=3):
            run("dx update stage myworkflow 0 --executable " + empty_applet_id)
        run("dx update stage myworkflow 0 --force --executable " + empty_applet_id)
        run("dx rm " + empty_applet_id)
        desc_string = run("dx describe myworkflow")
        run("dx update stage myworkflow 0 --force --executable " + applet_id)

        # some errors
        with self.assertSubprocessFailure(stderr_regexp="no-name", exit_code=2):
            run("dx update stage myworkflow 0 --name foo --no-name")
        with self.assertSubprocessFailure(stderr_regexp="output-folder", exit_code=2):
            run("dx update stage myworkflow 0 --output-folder /foo --relative-output-folder foo")
        with self.assertSubprocessFailure(stderr_regexp="parsed", exit_code=3):
            run("dx update stage myworkflow 0 -inumber=foo")
        with self.assertSubprocessFailure(stderr_regexp="ResolutionError", exit_code=3):
            run("dx update stage myworkflow 0 --executable foo")
        with self.assertSubprocessFailure(stderr_regexp="instance-type", exit_code=3):
            run("dx update stage myworkflow 0 --instance-type {]")

        # no-op
        output = run("dx update stage myworkflow 0 --alias default --force")
        self.assertIn("No updates requested", output)

        # update something out of range
        with self.assertSubprocessFailure(stderr_regexp="out of range", exit_code=3):
            run("dx update stage /myworkflow 5 --name foo")

        # remove some bad stage ID
        with self.assertSubprocessFailure(stderr_regexp="nor found as a stage name", exit_code=3):
            run("dx update stage /myworkflow badstageID --name foo")

        # remove nonexistent stage
        with self.assertSubprocessFailure(stderr_regexp="ResourceNotFound", exit_code=3):
            run("dx update stage /myworkflow stage-123456789012345678901234 --name foo")

class TestDXClientFind(DXTestCase):

    def test_dx_find_apps(self):
        # simple test here does not assume anything about apps that do
        # or do not exist
        from dxpy.app_categories import APP_CATEGORIES
        category_help = run("dx find apps --category-help")
        for category in APP_CATEGORIES:
            self.assertIn(category, category_help)
        run("dx find apps --category foo") # any category can be searched

    def test_dx_find_data_by_class(self):
        ids = {"record": run("dx new record --brief").strip(),
               "workflow": run("dx new workflow --brief").strip(),
               "file": run("echo foo | dx upload - --brief").strip(),
               "gtable": run("dx new gtable --columns col1:int --brief").strip()}

        for classname in ids:
            self.assertEqual(run("dx find data --brief --class " + classname).strip(),
                             self.project + ':' + ids[classname])

    def test_dx_find_data_by_tag(self):
        record_ids = [run("dx new record --brief --tag Ψ --tag foo --tag baz").strip(),
                      run("dx new record --brief --tag Ψ --tag foo --tag bar").strip()]

        found_records = run("dx find data --tag baz --brief").strip()
        self.assertEqual(found_records, dxpy.WORKSPACE_ID + ':' + record_ids[0])

        found_records = run("dx find data --tag Ψ --tag foo --tag foobar --brief").strip()
        self.assertEqual(found_records, '')

        found_records = run("dx find data --tag foo --tag Ψ --brief").strip().split("\n")
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[0], found_records)
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[1], found_records)

    def test_dx_find_data_by_property(self):
        record_ids = [run("dx new record --brief " +
                          "--property Ψ=world --property foo=bar --property bar=").strip(),
                      run("dx new record --brief --property Ψ=notworld --property foo=bar").strip()]

        found_records = run("dx find data --property Ψ=world --property foo=bar --brief").strip()
        self.assertEqual(found_records, dxpy.WORKSPACE_ID + ':' + record_ids[0])

        # presence
        found_records = run("dx find data --property Ψ --brief").strip().split("\n")
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[0], found_records)
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[1], found_records)

        found_records = run("dx find data --property Ψ --property foo=baz --brief").strip()
        self.assertEqual(found_records, '')

        found_records = run("dx find data --property Ψ --property foo=bar --brief").strip().split("\n")
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[0], found_records)
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[1], found_records)

        # Empty string values should be okay
        found_records = run("dx find data --property bar= --brief").strip()
        self.assertEqual(found_records, dxpy.WORKSPACE_ID + ':' + record_ids[0])

        # Errors parsing --property value
        with self.assertSubprocessFailure(stderr_regexp='nonempty strings', exit_code=3):
            run("dx find data --property ''")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx find data --property foo=bar=baz")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx find data --property =foo=bar=")
        # Property keys must be nonempty
        with self.assertSubprocessFailure(stderr_regexp='nonempty strings', exit_code=3):
            run("dx find data --property =bar")

    def test_dx_find_data_by_scope(self):
        # Name of temporary project to use in test cases.
        test_projectname = 'Test-Project-PTFM-7023'

        # Tests for deprecated --project flag.

        # Case: --project specified.
        test_dirname = '/test-folder-PTFM-7023-01'
        test_recordname = '/test-record-01'
        with temporary_project(test_projectname) as temp_project:
            test_projectid = temp_project.get_id()
            record_id = run('dx new record -p --brief ' + test_projectid + ':' + test_dirname +
                            test_recordname).strip()
            found_record_id = run('dx find data --brief --project ' + test_projectid).strip()
            self.assertEqual(found_record_id, test_projectid + ':' + record_id)

        # Tests for deprecated --folder flag.

        # Case: --folder specified, WORKSPACE_ID set.
        test_dirname = '/test-folder-PTFM-7023-02'
        test_subdirname = '/test-subfolder'
        test_recordname = '/test-record-02'
        record_ids = [run('dx new record -p --brief ' + test_dirname + test_recordname).strip(),
                      run('dx new record -p --brief ' + test_dirname + test_subdirname + test_recordname).strip()]
        found_record_ids = run('dx find data --brief --folder ' + test_dirname).strip().split('\n')
        self.assertEqual(set(dxpy.WORKSPACE_ID + ':' + record_id for record_id in record_ids), set(found_record_ids))

        # Case: --folder and --project specified.
        test_dirname = '/test-folder-PTFM-7023-03'
        test_recordname = '/test-record-03'
        with temporary_project(test_projectname) as temp_project:
            test_projectid = temp_project.get_id()
            record_id = run('dx new record -p --brief ' + test_projectid + ':' + test_dirname +
                            test_recordname).strip()
            found_record_id = run('dx find data --brief --project ' + test_projectid + ' --folder ' +
                                  test_dirname).strip()
            self.assertEqual(found_record_id, test_projectid + ':' + record_id)

        # Case: --folder and --norecurse specified, WORKSPACE_ID set.
        test_dirname = '/test-folder-PTFM-7023-04'
        test_subdirname = '/test-subfolder'
        test_recordname = '/test-record-04'
        record_id = run('dx new record -p --brief ' + test_dirname + test_recordname).strip()
        run('dx new record -p --brief ' + test_dirname + test_subdirname + test_recordname)
        found_record_id = run('dx find data --brief --folder ' + test_dirname + ' --norecurse').strip()
        self.assertEqual(found_record_id, dxpy.WORKSPACE_ID + ':' + record_id)

        # Case: --folder, --project, and --norecurse specified.
        test_dirname = '/test-folder-PTFM-7023-05'
        test_subdirname = '/test-subfolder'
        test_recordname = '/test-record-05'
        with temporary_project(test_projectname) as temp_project:
            test_projectid = temp_project.get_id()
            record_id = run('dx new record -p --brief ' + test_projectid + ':' + test_dirname +
                            test_recordname).strip()
            run('dx new record -p --brief ' + test_projectid + ':' + test_dirname + test_subdirname + test_recordname)
            found_record_id = run('dx find data --brief --project ' + test_projectid + ' --folder ' +
                                  test_dirname + ' --norecurse').strip()
            self.assertEqual(found_record_id, test_projectid + ':' + record_id)

        # Tests for --path flag.

        # Case: --path specified, WORKSPACE_ID set.
        test_dirname = '/test-folder-PTFM-7023-06'
        test_subdirname = '/test-subfolder'
        test_recordname = '/test-record-06'
        run('dx new record -p --brief ' + test_recordname)
        record_ids = [run('dx new record -p --brief ' + test_dirname + test_recordname).strip(),
                      run('dx new record -p --brief ' + test_dirname + test_subdirname + test_recordname).strip()]
        found_record_ids = run('dx find data --brief --path ' + test_dirname).strip().split('\n')
        self.assertEqual(set(dxpy.WORKSPACE_ID + ':' + record_id for record_id in record_ids), set(found_record_ids))

        # Case: --path and --project specified.
        test_dirname = '/test-folder-PTFM-7023-07'
        test_recordname = '/test-record-07'
        with temporary_project(test_projectname) as temp_project:
            test_projectid = temp_project.get_id()
            run('dx new record -p --brief ' + test_recordname)
            record_id = run('dx new record -p --brief ' + test_projectid + ':' + test_dirname +
                            test_recordname).strip()
            found_record_id = run('dx find data --brief --project ' + test_projectid + ' --path ' +
                                  test_dirname).strip()
            self.assertEqual(found_record_id, test_projectid + ':' + record_id)

        # Case: --path and --norecurse specified, WORKSPACE_ID set.
        test_dirname = '/test-folder-PTFM-7023-08'
        test_subdirname = '/test-subfolder'
        test_recordname = '/test-record-08'
        record_id = run('dx new record -p --brief ' + test_dirname + test_recordname).strip()
        run('dx new record -p --brief ' + test_dirname + test_subdirname + test_recordname)
        found_record_id = run('dx find data --brief --path ' + test_dirname + ' --norecurse').strip()
        self.assertEqual(found_record_id, dxpy.WORKSPACE_ID + ':' + record_id)

        # Case: --path, --project, and --norecurse specified.
        test_dirname = '/test-folder-PTFM-7023-09'
        test_subdirname = '/test-subfolder'
        test_recordname = '/test-record-09'
        with temporary_project(test_projectname) as temp_project:
            test_projectid = temp_project.get_id()
            record_id = run('dx new record -p --brief ' + test_projectid + ':' + test_dirname +
                            test_recordname).strip()
            run('dx new record -p --brief ' + test_projectid + ':' + test_dirname + test_subdirname + test_recordname)
            found_record_id = run('dx find data --brief --project ' + test_projectid + ' --path ' +
                                  test_dirname + ' --norecurse').strip()
            self.assertEqual(found_record_id, test_projectid + ':' + record_id)

        # Case: --path specified as PROJECTID:FOLDERPATH.
        test_dirname = '/test-folder-PTFM-7023-10'
        test_recordname = '/test-record-10'
        with temporary_project(test_projectname) as temp_project:
            test_projectid = temp_project.get_id()
            record_ids = [run('dx new record -p --brief ' + test_projectid + ':' + test_dirname +
                              test_recordname).strip(),
                          run('dx new record -p --brief ' + test_projectid + ':' + test_dirname +
                              test_subdirname + test_recordname).strip()]

            # Case: --norecurse not specified.
            found_record_id = run('dx find data --brief --path ' + test_projectid + ':' +
                                  test_dirname).strip().split('\n')
            self.assertEqual(set(found_record_id), set(test_projectid + ':' + record_id for record_id in record_ids))

            # Case: --norecurse specified.
            found_record_id = run('dx find data --brief --path ' + test_projectid + ':' + test_dirname +
                                  ' --norecurse').strip()
            self.assertEqual(found_record_id, test_projectid + ':' + record_ids[0])

        # Case: --path specified as relative path, WORKSPACE_ID set.
        test_dirname = '/test-folder-PTFM-7023-12'
        test_subdirname = '/test-subfolder'
        test_recordname = '/test-record-12'
        run('dx new record -p --brief ' + test_recordname)
        record_id = run('dx new record -p --brief ' + test_dirname + test_subdirname + test_recordname).strip()
        run('dx cd ' + test_dirname)
        found_record_id = run('dx find data --brief --path ' + test_subdirname[1:]).strip()
        self.assertEqual(found_record_id, dxpy.WORKSPACE_ID + ':' + record_id)

        run('dx clearenv')
        test_dirname = '/test-folder-PTFM-7023-14'
        test_recordname = '/test-record-14'
        with temporary_project(test_projectname) as temp_project, select_project(None):
            test_projectid = temp_project.get_id()
            run('dx new record -p --brief ' + test_projectid + ':' + test_dirname + test_recordname)

            # Case: --path specified, WORKSPACE_ID not set (fail).
            with self.assertSubprocessFailure(stderr_regexp="if a project is not specified", exit_code=1):
                run('dx find data --brief --path ' + test_dirname)

            # Case: --project and --path PROJECTID:FOLDERPATH specified (fail).
            with self.assertSubprocessFailure(stderr_regexp="Cannot supply both --project and --path " +
                                              "PROJECTID:FOLDERPATH", exit_code=3):
                run('dx find data --brief --project ' + test_projectid + ' --path ' + test_projectid + ':' +
                    test_dirname)

            # Case: --folder and --path specified (fail).
            with self.assertSubprocessFailure(stderr_regexp="Cannot supply both --folder and --path", exit_code=3):
                run('dx find data --brief --folder ' + test_projectid + ':' + test_dirname + ' --path ' +
                    test_projectid + ':' + test_dirname)

    def test_dx_find_projects(self):
        unique_project_name = 'dx find projects test ' + str(time.time())
        with temporary_project(unique_project_name) as unique_project:
            self.assertEqual(run("dx find projects --name " + pipes.quote(unique_project_name)),
                             unique_project.get_id() + ' : ' + unique_project_name + ' (ADMINISTER)\n')
            self.assertEqual(run("dx find projects --brief --name " + pipes.quote(unique_project_name)),
                             unique_project.get_id() + '\n')
            json_output = json.loads(run("dx find projects --json --name " + pipes.quote(unique_project_name)))
            self.assertEqual(len(json_output), 1)
            self.assertEqual(json_output[0]['id'], unique_project.get_id())

    def test_dx_find_projects_by_tag(self):
        other_project_id = run("dx new project other --brief").strip()
        try:
            run("dx tag : Ψ world")
            proj_desc = dxpy.describe(dxpy.WORKSPACE_ID)
            self.assertEqual(len(proj_desc["tags"]), 2)
            self.assertIn("Ψ", proj_desc["tags"])
            self.assertIn("world", proj_desc["tags"])

            found_projects = run("dx find projects --tag Ψ --tag world --brief").strip().split('\n')
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertNotIn(other_project_id, found_projects)

            found_projects = run("dx find projects --tag Ψ --tag world --tag foobar --brief").strip().split('\n')
            self.assertNotIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertNotIn(other_project_id, found_projects)

            run("dx tag " + other_project_id + " Ψ world foobar")
            found_projects = run("dx find projects --tag world --tag Ψ --brief").strip().split("\n")
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertIn(other_project_id, found_projects)
        except:
            raise
        finally:
            run("dx rmproject -y " + other_project_id)

    def test_dx_find_projects_by_property(self):
        other_project_id = run("dx new project other --brief").strip()
        try:
            run("dx set_properties : Ψ=world foo=bar bar=")
            proj_desc = dxpy.api.project_describe(dxpy.WORKSPACE_ID, {"properties": True})
            self.assertEqual(len(proj_desc["properties"]), 3)
            self.assertEqual(proj_desc["properties"]["Ψ"], "world")
            self.assertEqual(proj_desc["properties"]["foo"], "bar")
            self.assertEqual(proj_desc["properties"]["bar"], "")

            run("dx set_properties " + other_project_id + " Ψ=notworld foo=bar")

            found_projects = run("dx find projects --property Ψ=world --property foo=bar --brief").strip().split("\n")
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertNotIn(other_project_id, found_projects)

            found_projects = run("dx find projects --property bar= --brief").strip().split('\n')
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertNotIn(other_project_id, found_projects)

            # presence
            found_projects = run("dx find projects --property Ψ --brief").strip().split("\n")
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertIn(other_project_id, found_projects)

            found_projects = run("dx find projects --property Ψ --property foo=baz --brief").strip().split("\n")
            self.assertNotIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertNotIn(other_project_id, found_projects)

            found_projects = run("dx find projects --property Ψ --property foo=bar --brief").strip().split("\n")
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertIn(other_project_id, found_projects)
        except:
            raise
        finally:
            run("dx rmproject -y " + other_project_id)

        # Errors parsing --property value
        with self.assertSubprocessFailure(stderr_regexp='nonempty strings', exit_code=3):
            run("dx find projects --property ''")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx find projects --property foo=bar=baz")
        with self.assertSubprocessFailure(stderr_regexp='property_key', exit_code=3):
            run("dx find projects --property =foo=bar=")
        # Property keys must be nonempty
        with self.assertSubprocessFailure(stderr_regexp='nonempty strings', exit_code=3):
            run("dx find projects --property =bar")
        # Empty string values should be okay
        run("dx find projects --property bar=")

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping tests that would run jobs')
    def test_dx_find_jobs_by_tags_and_properties(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"}
                                         })['id']
        property_names = ["$my.prop", "secoиdprop", "тhird prop"]
        property_values = ["$hello.world", "Σ2,n", "stuff"]
        the_tags = ["Σ1=n", "helloo0", "ωω"]
        job_id = run("dx run " + applet_id + ' -inumber=32 --brief -y ' +
                     " ".join(["--property '" + prop[0] + "'='" + prop[1] + "'" for prop in zip(property_names, property_values)]) +
                     "".join([" --tag " + tag for tag in the_tags])).strip()

        # matches
        self.assertEqual(run("dx find jobs --brief --tag " + the_tags[0]).strip(), job_id)
        self.assertEqual(run("dx find jobs --brief" + "".join([" --tag " + tag for tag in the_tags])).strip(),
                         job_id)
        self.assertEqual(run("dx find jobs --brief --property " + property_names[1]).strip(), job_id)
        self.assertEqual(run("dx find jobs --brief --property '" +
                             property_names[1] + "'='" + property_values[1] + "'").strip(),
                         job_id)
        self.assertEqual(run("dx find jobs --brief" +
                             "".join([" --property '" + key + "'='" + value + "'" for
                                       key, value in zip(property_names, property_values)])).strip(),
                         job_id)

        # no matches
        self.assertEqual(run("dx find jobs --brief --tag foo").strip(), "")
        self.assertEqual(run("dx find jobs --brief --property foo").strip(), "")
        self.assertEqual(run("dx find jobs --brief --property '" +
                             property_names[1] + "'=badvalue").strip(), "")

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping test that would run a job')
    def test_find_executions(self):
        dxapplet = dxpy.DXApplet()
        dxapplet.new(name="test_applet",
                     dxapi="1.0.0",
                     inputSpec=[{"name": "chromosomes", "class": "record"},
                                {"name": "rowFetchChunk", "class": "int"}
                                ],
                     outputSpec=[{"name": "mappings", "class": "record"}],
                     runSpec={"code": "def main(): pass",
                              "interpreter": "python2.7",
                              "execDepends": [{"name": "python-numpy"}]})
        dxrecord = dxpy.new_dxrecord()
        dxrecord.close()
        prog_input = {"chromosomes": {"$dnanexus_link": dxrecord.get_id()},
                      "rowFetchChunk": 100}
        dxworkflow = dxpy.new_dxworkflow(name='find_executions test workflow')
        stage = dxworkflow.add_stage(dxapplet, stage_input=prog_input)
        dxanalysis = dxworkflow.run({stage+".rowFetchChunk": 200},
                                    tags=["foo"],
                                    properties={"foo": "bar"})
        dxapplet.run(applet_input=prog_input)
        dxjob = dxapplet.run(applet_input=prog_input,
                             tags=["foo", "bar"],
                             properties={"foo": "baz"})

        run("dx cd {project_id}:/".format(project_id=dxapplet.get_proj_id()))

        # Wait for job to be created
        executions = [stage['execution']['id'] for stage in dxanalysis.describe()['stages']]
        t = 0
        while len(executions) > 0:
            try:
                dxpy.api.job_describe(executions[len(executions) - 1], {})
                executions.pop()
            except DXAPIError:
                t += 1
                if t > 20:
                    raise Exception("Timeout while waiting for job to be created for an analysis stage")
                time.sleep(1)

        options = "--user=self"
        self.assertEqual(len(run("dx find executions "+options).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs "+options).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses "+options).splitlines()), 2)
        options += " --project="+dxapplet.get_proj_id()
        self.assertEqual(len(run("dx find executions "+options).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs "+options).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses "+options).splitlines()), 2)
        options += " --created-after=-150s --no-subjobs --applet="+dxapplet.get_id()
        self.assertEqual(len(run("dx find executions "+options).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs "+options).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses "+options).splitlines()), 2)
        options2 = options + " --brief -n 9000"
        self.assertEqual(len(run("dx find executions "+options2).splitlines()), 4)
        self.assertEqual(len(run("dx find jobs "+options2).splitlines()), 3)
        self.assertEqual(len(run("dx find analyses "+options2).splitlines()), 1)
        options3 = options2 + " --origin="+dxjob.get_id()
        self.assertEqual(len(run("dx find executions "+options3).splitlines()), 1)
        self.assertEqual(len(run("dx find jobs "+options3).splitlines()), 1)
        self.assertEqual(len(run("dx find analyses "+options3).splitlines()), 0)
        options3 = options2 + " --root="+dxanalysis.get_id()
        self.assertEqual(len(run("dx find executions "+options3).splitlines()), 2)
        self.assertEqual(len(run("dx find jobs "+options3).splitlines()), 1)
        self.assertEqual(len(run("dx find analyses "+options3).splitlines()), 1)
        options2 = options + " --origin-jobs"
        self.assertEqual(len(run("dx find executions "+options2).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs "+options2).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses "+options2).splitlines()), 2)
        options2 = options + " --origin-jobs -n 9000"
        self.assertEqual(len(run("dx find executions "+options2).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs "+options2).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses "+options2).splitlines()), 2)
        options2 = options + " --all-jobs"
        self.assertEqual(len(run("dx find executions "+options2).splitlines()), 8)
        self.assertEqual(len(run("dx find jobs "+options2).splitlines()), 6)
        self.assertEqual(len(run("dx find analyses "+options2).splitlines()), 2)
        options2 = options + " --state=done"
        self.assertEqual(len(run("dx find executions "+options2).splitlines()), 0)
        self.assertEqual(len(run("dx find jobs "+options2).splitlines()), 0)
        self.assertEqual(len(run("dx find analyses "+options2).splitlines()), 0)

        def assert_cmd_gives_ids(cmd, ids):
            self.assertEqual(sorted(execid.strip() for execid in run(cmd).splitlines()),
                             sorted(ids))

        # Search by tag
        options2 = options + " --all-jobs --brief"
        options3 = options2 + " --tag foo"
        analysis_id = dxanalysis.get_id()
        job_id = dxjob.get_id()
        assert_cmd_gives_ids("dx find executions "+options3, [analysis_id, job_id])
        assert_cmd_gives_ids("dx find jobs "+options3, [job_id])
        assert_cmd_gives_ids("dx find analyses "+options3, [analysis_id])
        options3 = options2 + " --tag foo --tag bar"
        assert_cmd_gives_ids("dx find executions "+options3, [job_id])
        assert_cmd_gives_ids("dx find jobs "+options3, [job_id])
        assert_cmd_gives_ids("dx find analyses "+options3, [])

        # Search by property (presence and by value)
        options3 = options2 + " --property foo"
        assert_cmd_gives_ids("dx find executions "+options3, [analysis_id, job_id])
        assert_cmd_gives_ids("dx find jobs "+options3, [job_id])
        assert_cmd_gives_ids("dx find analyses "+options3, [analysis_id])
        options3 = options2 + " --property foo=baz"
        assert_cmd_gives_ids("dx find executions "+options3, [job_id])
        assert_cmd_gives_ids("dx find jobs "+options3, [job_id])
        assert_cmd_gives_ids("dx find analyses "+options3, [])

@unittest.skipUnless(testutil.TEST_HTTP_PROXY,
                     'skipping HTTP Proxy support test that needs squid3')
class TestHTTPProxySupport(DXTestCase):
    def setUp(self):
        squid_wd = os.path.join(os.path.dirname(__file__), 'http_proxy')
        self.proxy_process = subprocess.Popen(['squid3', '-N', '-f', 'squid.conf'], cwd=squid_wd)
        time.sleep(1)

        print("Waiting for squid to come up...")
        t = 0
        while True:
            try:
                if requests.get("http://localhost:3129").status_code == requests.codes.bad_request:
                    if self.proxy_process.poll() is not None:
                        # Got a response on port 3129, but our proxy
                        # quit with an error, so it must be another
                        # process.
                        raise Exception("Tried launching squid, but port 3129 is already bound")
                    print("squid is up")
                    break
            except requests.exceptions.RequestException:
                pass
            time.sleep(0.5)
            t += 1
            if t > 16:
                raise Exception("Failed to launch Squid")

        self.proxy_env_no_auth = os.environ.copy()
        self.proxy_env_no_auth["HTTP_PROXY"] = "http://localhost:3129"
        self.proxy_env_no_auth["HTTPS_PROXY"] = "http://localhost:3129"

        self.proxy_env = os.environ.copy()
        self.proxy_env["HTTP_PROXY"] = "http://proxyuser:proxypassword@localhost:3129"
        self.proxy_env["HTTPS_PROXY"] = "http://proxyuser:proxypassword@localhost:3129"

    def test_proxy(self):
        run("dx find projects", env=self.proxy_env)
        with self.assertSubprocessFailure(stderr_regexp="407 Proxy Authentication Required"):
            run("dx find projects", env=self.proxy_env_no_auth)

    def tearDown(self):
        self.proxy_process.terminate()


class TestDXBuildApp(DXTestCase):
    def setUp(self):
        super(TestDXBuildApp, self).setUp()
        self.temp_file_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_file_path)
        super(TestDXBuildApp, self).tearDown()

    def run_and_assert_stderr_matches(self, cmd, stderr_regexp):
        with self.assertSubprocessFailure(stderr_regexp=stderr_regexp, exit_code=28):
            run(cmd + ' && exit 28')

    def write_app_directory(self, app_name, dxapp_str, code_filename=None, code_content="\n"):
        # Note: if called twice with the same app_name, will overwrite
        # the dxapp.json and code file (if specified) but will not
        # remove any other files that happened to be present
        try:
            os.mkdir(os.path.join(self.temp_file_path, app_name))
        except OSError as e:
            if e.errno != 17: # directory already exists
                raise e
        if dxapp_str is not None:
            with open(os.path.join(self.temp_file_path, app_name, 'dxapp.json'), 'w') as manifest:
                manifest.write(dxapp_str)
        if code_filename:
            with open(os.path.join(self.temp_file_path, app_name, code_filename), 'w') as code_file:
                code_file.write(code_content)
        return os.path.join(self.temp_file_path, app_name)

    def test_help_without_security_context(self):
        env = overrideEnvironment(DX_SECURITY_CONTEXT=None, DX_APISERVER_HOST=None,
                                  DX_APISERVER_PORT=None, DX_APISERVER_PROTOCOL=None)
        run("dx build -h", env=env)

    def test_accepts_semver(self):
        self.assertTrue(dx_build_app.APP_VERSION_RE.match('3.1.41') is not None)
        self.assertTrue(dx_build_app.APP_VERSION_RE.match('3.1.41-rc.1') is not None)
        self.assertFalse(dx_build_app.APP_VERSION_RE.match('3.1.41-rc.1.') is not None)
        self.assertFalse(dx_build_app.APP_VERSION_RE.match('3.1.41-rc..1') is not None)
        self.assertTrue(dx_build_app.APP_VERSION_RE.match('22.0.999+git.abcdef') is not None)
        self.assertFalse(dx_build_app.APP_VERSION_RE.match('22.0.999+git.abcdef$') is not None)
        self.assertFalse(dx_build_app.APP_VERSION_RE.match('22.0.999+git.abcdef.') is not None)
        self.assertTrue(dx_build_app.APP_VERSION_RE.match('22.0.999-rc.1+git.abcdef') is not None)

    def test_version_suffixes(self):
        app_spec = {
            "name": "test_versioning_åpp",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("test_versioning_app", json.dumps(app_spec), "code.py")
        self.assertTrue(dx_build_app._get_version_suffix(app_dir, '1.0.0').startswith('+build.'))
        self.assertTrue(dx_build_app._get_version_suffix(app_dir, '1.0.0+git.abcdef')
                        .startswith('.build.'))

    def test_build_applet(self):
        app_spec = {
            "name": "minimal_applet",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("minimal_åpplet", json.dumps(app_spec), "code.py")
        new_applet = json.loads(run("dx build --json " + app_dir))
        applet_describe = json.loads(run("dx describe --json " + new_applet["id"]))
        self.assertEqual(applet_describe["class"], "applet")
        self.assertEqual(applet_describe["id"], applet_describe["id"])
        self.assertEqual(applet_describe["name"], "minimal_applet")

    def test_dx_build_applet_no_app_linting(self):
        run("dx clearenv")

        # Case: Missing title, summary, description.
        app_spec = {
            "name": "dx_build_applet_missing_fields",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "categories": ["Annotation"]
            }
        app_dir = self.write_app_directory("dx_build_applet_missing_fields", json.dumps(app_spec), "code.py")
        args = ['dx', 'build', app_dir]
        p = subprocess.Popen(args, stderr=subprocess.PIPE)
        out, err = p.communicate()
        self.assertFalse(err.startswith("WARNING"))

        # Case: Usage of period at end of summary.
        app_spec = {
            "name": "dx_build_applet_summary_with_period",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "title": "Title",
            "summary": "Summary without period",
            "description": "Description with period.",
            "categories": ["Annotation"]
            }
        app_dir = self.write_app_directory("dx_build_applet_summary_with_period", json.dumps(app_spec), "code.py")
        args = ['dx', 'build', app_dir]
        p = subprocess.Popen(args, stderr=subprocess.PIPE)
        out, err = p.communicate()
        self.assertFalse(err.startswith("WARNING"))

        # Case: Usage of unknown categories.
        unknown_category = "asdf1234"
        app_spec = {
            "name": "dx_build_applet_unknown_cat",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "title": "Title",
            "summary": "Summary without period",
            "description": "Description without period",
            "categories": [unknown_category]
            }
        app_dir = self.write_app_directory("dx_build_applet_unknown_cat", json.dumps(app_spec), "code.py")
        args = ['dx', 'build', app_dir]
        p = subprocess.Popen(args, stderr=subprocess.PIPE)
        out, err = p.communicate()
        self.assertFalse(err.startswith("WARNING"))

    def test_build_applet_dry_run(self):
        app_spec = {
            "name": "minimal_applet_dry_run",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("minimal_applet_dry_run", json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp='cannot be specified together', exit_code=2):
            run("dx build --dry-run " + app_dir + " --run -y --brief")
        run("dx build --dry-run " + app_dir)
        self.assertEqual(len(list(dxpy.find_data_objects(name="minimal_applet_dry_run"))), 0)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    def test_build_applet_and_run_immediately(self):
        app_spec = {
            "name": "minimal_applet_to_run",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("minimal_åpplet_to_run", json.dumps(app_spec), "code.py")
        job_id = run("dx build " + app_dir + ' --run -y --brief').strip()
        job_desc = json.loads(run('dx describe --json ' + job_id))
        # default priority should be high for running after building
        # an applet
        self.assertEqual(job_desc['name'], 'minimal_applet_to_run')
        self.assertEqual(job_desc['priority'], 'high')

        # if priority is explicitly requested as normal, it should be
        # honored
        job_id = run("dx build -f " + app_dir + ' --run --priority normal -y --brief').strip()
        job_desc = json.loads(run('dx describe --json ' + job_id))
        self.assertEqual(job_desc['name'], 'minimal_applet_to_run')
        self.assertEqual(job_desc['priority'], 'normal')

    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    def test_remote_build_applet_and_run_immediately(self):
        app_spec = {
            "name": "minimal_remote_build_applet_to_run",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("minimal_remote_build_åpplet_to_run", json.dumps(app_spec),
                                           "code.py")
        job_name = ("remote_build_test_run_" + str(int(time.time() * 1000)) + "_" +
                    str(random.randint(0, 1000)))
        run("dx build --remote " + app_dir + " --run -y --name=" + job_name)
        resulting_jobs = list(dxpy.find_executions(name=job_name, return_handler=True))
        self.assertEqual(1, len(resulting_jobs))
        self.assertEqual('minimal_remote_build_applet_to_run',
                         resulting_jobs[0].describe()['executableName'])

    @unittest.skipUnless(testutil.TEST_RUN_JOBS and testutil.TEST_CREATE_APPS,
                         'skipping test that would create apps and run jobs')
    def test_remote_build_app(self):
        app_spec = {
            "name": "minimal_remote_build_app",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("minimal_remote_build_åpp", json.dumps(app_spec), "code.py")
        run("dx build --remote --app " + app_dir)

    def test_remote_build_app_and_run_immediately(self):
        app_spec = {
            "name": "minimal_remote_build_app_to_run",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("minimal_remote_build_åpp_to_run", json.dumps(app_spec),
                                           "code.py")
        # Not supported yet
        with self.assertSubprocessFailure(stderr_regexp='cannot all be specified together', exit_code=2):
            run("dx build --remote --app " + app_dir + " --run --yes")

    def test_build_applet_warnings(self):
        app_spec = {
            "title": "title",
            "summary": "a summary sentence.",
            "description": "foo",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [{"name": "34", "class": "int"}],
            "outputSpec": [{"name": "92", "class": "string"}],
            "version": "1.0.0",
            "categories": ["foo", "Import", "Export"]
            }
        app_dir = self.write_app_directory("test_build_åpplet_warnings", json.dumps(app_spec), "code.py")
        with open(os.path.join(app_dir, 'Readme.md'), 'w') as readme:
            readme.write('a readme file')
        applet_expected_warnings = ["missing a name",
                                    'input 0 has illegal name',
                                    'output 0 has illegal name']
        applet_unexpected_warnings = ["should be all lowercase",
                                      "does not match containing directory",
                                      "missing a title",
                                      "missing a summary",
                                      "should be a short phrase not ending in a period",
                                      "missing a description",
                                      '"description" field shadows file',
                                      '"description" field should be written in complete sentences',
                                      'unrecognized category',
                                      'should end in "Importer"',
                                      'should end in "Exporter"',
                                      "should be semver compliant"]
        try:
            run("dx build " + app_dir)
            self.fail("dx build invocation should have failed because of bad IO spec")
        except subprocess.CalledProcessError as err:
            for warning in applet_expected_warnings:
                self.assertIn(warning, err.stderr)
            for warning in applet_unexpected_warnings:
                self.assertNotIn(warning, err.stderr)

        # some more errors
        app_spec = {
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py"}
            }
        app_dir = self.write_app_directory("test_build_second_åpplet_warnings", json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp='interpreter field was not present'):
            run("dx build " + app_dir)

    @unittest.skipUnless(testutil.TEST_CREATE_APPS,
                         'skipping test that would create apps')
    def test_build_app_warnings(self):
        app_spec = {
            "name": "Foo",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "foo"
            }
        app_dir = self.write_app_directory("test_build_app_warnings", json.dumps(app_spec), "code.py")
        app_unexpected_warnings = ["missing a name",
                                   "should be a short phrase not ending in a period",
                                   '"description" field shadows file',
                                   '"description" field should be written in complete sentences',
                                   'unrecognized category',
                                   'should end in "Importer"',
                                   'should end in "Exporter"',
                                   'input 0 has illegal name',
                                   'output 0 has illegal name']
        app_expected_warnings = ["should be all lowercase",
                                 "does not match containing directory",
                                 "missing a title",
                                 "missing a summary",
                                 "missing a description",
                                 "should be semver compliant"]
        try:
            # Expect "dx build" to succeed, exit with error code to
            # grab stderr.
            run("dx build --app " + app_dir + " && exit 28")
        except subprocess.CalledProcessError as err:
            self.assertEqual(err.returncode, 28)
            for warning in app_unexpected_warnings:
                self.assertNotIn(warning, err.stderr)
            for warning in app_expected_warnings:
                self.assertIn(warning, err.stderr)

    def test_get_applet(self):
        # TODO: not sure why self.assertEqual doesn't consider
        # assertEqual to pass unless the strings here are unicode strings
        app_spec = {
            "name": "get_applet",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [{"name": "in1", "class": "file"}],
            "outputSpec": [{"name": "out1", "class": "file"}],
            "description": "Description\n",
            "developerNotes": "Developer notes\n",
            "types": ["Foo"],
            "tags": ["bar"],
            "properties": {"sample_id": "123456"},
            "details": {"key1": "value1"},
            }
        # description and developerNotes should be un-inlined back to files
        output_app_spec = dict((k, v) for (k, v) in app_spec.iteritems() if k not in ('description',
                                                                                      'developerNotes'))
        output_app_spec["runSpec"] = {"file": "src/code.py", "interpreter": "python2.7"}

        app_dir = self.write_app_directory("get_åpplet", json.dumps(app_spec), "code.py",
                                           code_content="import os\n")
        os.mkdir(os.path.join(app_dir, "resources"))
        with open(os.path.join(app_dir, "resources", "resources_file"), 'w') as f:
            f.write('content\n')
        new_applet_id = json.loads(run("dx build --json " + app_dir))["id"]
        with chdir(tempfile.mkdtemp()):
            run("dx get " + new_applet_id)
            self.assertTrue(os.path.exists("get_applet"))
            self.assertTrue(os.path.exists(os.path.join("get_applet", "dxapp.json")))

            output_json = json.load(open(os.path.join("get_applet", "dxapp.json")))
            self.assertEqual(output_app_spec, output_json)

            self.assertEqual("Description\n", open(os.path.join("get_applet", "Readme.md")).read())
            self.assertEqual("Developer notes\n",
                             open(os.path.join("get_applet", "Readme.developer.md")).read())
            self.assertEqual("import os\n", open(os.path.join("get_applet", "src", "code.py")).read())

            self.assertEqual("content\n",
                             open(os.path.join("get_applet", "resources", "resources_file")).read())

            # Target applet does not exist
            with self.assertSubprocessFailure(stderr_regexp='Could not resolve', exit_code=3):
                run("dx get path_does_not_exist")

            # -o dest (dest does not exist yet)
            run("dx get -o dest get_applet")
            self.assertTrue(os.path.exists("dest"))
            self.assertTrue(os.path.exists(os.path.join("dest", "dxapp.json")))

            # -o -
            with self.assertSubprocessFailure(stderr_regexp='cannot be dumped to stdout', exit_code=3):
                run("dx get -o - " + new_applet_id)

            # -o dir (such that dir/applet_name is empty)
            os.mkdir('destdir')
            os.mkdir(os.path.join('destdir', 'get_applet'))
            run("dx get -o destdir get_applet") # Also tests getting by name
            self.assertTrue(os.path.exists(os.path.join("destdir", "get_applet", "dxapp.json")))

            # -o dir (such that dir/applet_name is not empty)
            os.mkdir('destdir_nonempty')
            os.mkdir(os.path.join('destdir_nonempty', 'get_applet'))
            with open(os.path.join('destdir_nonempty', 'get_applet', 'myfile'), 'w') as f:
                f.write('content')
            with self.assertSubprocessFailure(stderr_regexp='is an existing directory', exit_code=3):
                run("dx get -o destdir_nonempty get_applet")

            # -o dir (such that dir/applet_name is a file)
            os.mkdir('destdir_withfile')
            with open(os.path.join('destdir_withfile', 'get_applet'), 'w') as f:
                f.write('content')
            with self.assertSubprocessFailure(stderr_regexp='already exists', exit_code=3):
                run("dx get -o destdir_withfile get_applet")

            # -o dir --overwrite (such that dir/applet_name is a file)
            os.mkdir('destdir_withfile_force')
            with open(os.path.join('destdir_withfile_force', 'get_applet'), 'w') as f:
                f.write('content')
            run("dx get --overwrite -o destdir_withfile_force get_applet")
            self.assertTrue(os.path.exists(os.path.join("destdir_withfile_force", "get_applet",
                                                        "dxapp.json")))

            # -o file
            with open('destfile', 'w') as f:
                f.write('content')
            with self.assertSubprocessFailure(stderr_regexp='already exists', exit_code=3):
                run("dx get -o destfile get_applet")

            # -o file --overwrite
            run("dx get --overwrite -o destfile get_applet")
            self.assertTrue(os.path.exists("destfile"))
            self.assertTrue(os.path.exists(os.path.join("destfile", "dxapp.json")))

    def test_get_applet_field_cleanup(self):
        # TODO: not sure why self.assertEqual doesn't consider
        # assertEqual to pass unless the strings here are unicode strings

        # When retrieving the applet, we'll get back an empty list for
        # types, tags, etc. Those should not be written back to the
        # dxapp.json so as not to pollute it.
        app_spec = {
            "name": "get_applet_field_cleanup",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": []
            }
        output_app_spec = app_spec.copy()
        output_app_spec["runSpec"] = {"file": "src/code.py", "interpreter": "python2.7"}

        app_dir = self.write_app_directory("get_åpplet_field_cleanup", json.dumps(app_spec), "code.py",
                                           code_content="import os\n")
        os.mkdir(os.path.join(app_dir, "resources"))
        with open(os.path.join(app_dir, "resources", "resources_file"), 'w') as f:
            f.write('content\n')
        new_applet_id = json.loads(run("dx build --json " + app_dir))["id"]
        with chdir(tempfile.mkdtemp()):
            run("dx get " + new_applet_id)
            self.assertTrue(os.path.exists("get_applet_field_cleanup"))
            self.assertTrue(os.path.exists(os.path.join("get_applet_field_cleanup", "dxapp.json")))
            output_json = json.load(open(os.path.join("get_applet_field_cleanup", "dxapp.json")))
            self.assertEqual(output_app_spec, output_json)
            self.assertFalse(os.path.exists(os.path.join("get_applet", "Readme.md")))
            self.assertFalse(os.path.exists(os.path.join("get_applet", "Readme.developer.md")))

    def test_build_applet_with_no_dxapp_json(self):
        app_dir = self.write_app_directory("åpplet_with_no_dxapp_json", None, "code.py")
        with self.assertSubprocessFailure(stderr_regexp='does not contain dxapp\.json', exit_code=3):
            run("dx build " + app_dir)

    def test_build_applet_with_malformed_dxapp_json(self):
        app_dir = self.write_app_directory("åpplet_with_malformed_dxapp_json", "{", "code.py")
        with self.assertSubprocessFailure(stderr_regexp='Could not parse dxapp\.json file', exit_code=3):
            run("dx build " + app_dir)

    @unittest.skipUnless(testutil.TEST_CREATE_APPS,
                         'skipping test that would create apps')
    def test_build_app(self):
        app_spec = {
            "name": "minimal_app",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("minimal_åpp", json.dumps(app_spec), "code.py")
        new_app = json.loads(run("dx build --create-app --json " + app_dir))
        app_describe = json.loads(run("dx describe --json " + new_app["id"]))
        self.assertEqual(app_describe["class"], "app")
        self.assertEqual(app_describe["id"], app_describe["id"])
        self.assertEqual(app_describe["version"], "1.0.0")
        self.assertEqual(app_describe["name"], "minimal_app")
        self.assertFalse("published" in app_describe)
        self.assertTrue(os.path.exists(os.path.join(app_dir, 'code.py')))
        self.assertFalse(os.path.exists(os.path.join(app_dir, 'code.pyc')))

    @unittest.skipUnless(testutil.TEST_CREATE_APPS, 'skipping test that would create apps')
    def test_build_app_and_make_it_public(self):
        app_spec = {
            "name": "test_build_app_and_make_it_public",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "authorizedUsers": ['PUBLIC']
            }
        app_dir = self.write_app_directory("test_build_app_and_make_it_public", json.dumps(app_spec),
                                           "code.py")

        run("dx build --create-app --json " + app_dir)
        app_authorized_users = run("dx list users app-test_build_app_and_make_it_public")
        self.assertEqual(app_authorized_users, '')

        run("dx build --create-app --yes --version=1.0.1 --json " + app_dir)
        app_authorized_users = run("dx list users app-test_build_app_and_make_it_public")
        self.assertEqual(app_authorized_users.strip().split('\n'), ['PUBLIC'])

    @unittest.skipUnless(testutil.TEST_CREATE_APPS, 'skipping test that would create apps')
    def test_build_app_and_pretend_to_update_devs(self):
        app_spec = {
            "name": "test_build_app_and_pretend_to_update_devs",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "developers": ['user-dnanexus']
            }
        app_dir = self.write_app_directory("test_build_app_and_pretend_to_update_devs",
                                           json.dumps(app_spec), "code.py")

        # Without --yes, the build will succeed except that it will skip
        # the developer update
        self.run_and_assert_stderr_matches('dx build --create-app --json ' + app_dir,
                                           'skipping requested change to the developer list')
        app_developers = dxpy.api.app_list_developers('app-test_build_app_and_pretend_to_update_devs')['developers']
        self.assertEqual(len(app_developers), 1) # the id of the user we are calling as

    @unittest.skipUnless(testutil.TEST_CREATE_APPS, 'skipping test that would create apps')
    def test_build_app_and_update_devs(self):
        app_spec = {
            "name": "test_build_app_and_update_devs",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("test_build_app_and_update_devs", json.dumps(app_spec),
                                           "code.py")

        my_userid = dxpy.whoami()

        run('dx build --create-app --json ' + app_dir)
        app_developers = dxpy.api.app_list_developers('app-test_build_app_and_update_devs')['developers']
        self.assertEqual(app_developers, [my_userid])

        # Add a developer
        app_spec['developers'] = [my_userid, 'user-eve']
        self.write_app_directory("test_build_app_and_update_devs", json.dumps(app_spec), "code.py")
        self.run_and_assert_stderr_matches('dx build --create-app --yes --json ' + app_dir,
                                           'the following developers will be added: user-eve')
        app_developers = dxpy.api.app_list_developers('app-test_build_app_and_update_devs')['developers']
        self.assertEqual(sorted(app_developers), sorted([my_userid, 'user-eve']))

        # Add and remove a developer
        app_spec['developers'] = [my_userid, 'user-000000000000000000000001']
        self.write_app_directory("test_build_app_and_update_devs", json.dumps(app_spec), "code.py")
        self.run_and_assert_stderr_matches(
            'dx build --create-app --yes --json ' + app_dir,
            'the following developers will be added: user-000000000000000000000001; and ' \
            + 'the following developers will be removed: user-eve'
        )
        app_developers = dxpy.api.app_list_developers('app-test_build_app_and_update_devs')['developers']
        self.assertEqual(sorted(app_developers), sorted([my_userid, 'user-000000000000000000000001']))

        # Remove a developer
        app_spec['developers'] = [my_userid]
        self.write_app_directory("test_build_app_and_update_devs", json.dumps(app_spec), "code.py")
        self.run_and_assert_stderr_matches('dx build --create-app --yes --json ' + app_dir,
                                           'the following developers will be removed: ' +
                                           'user-000000000000000000000001')
        app_developers = dxpy.api.app_list_developers('app-test_build_app_and_update_devs')['developers']
        self.assertEqual(app_developers, [my_userid])

    @unittest.skipUnless(testutil.TEST_CREATE_APPS,
                         'skipping test that would create apps')
    def test_invalid_project_context(self):
        app_spec = {
            "name": "invalid_project_context",
            "dxapi": "1.0.0",
            "runSpec": {
                "file": "code.py",
                "interpreter": "python2.7"
                },
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("invalid_project_context", json.dumps(app_spec), "code.py")
        # Set the project context to a nonexistent project. This
        # shouldn't have any effect since building an app is supposed to
        # be hygienic.
        env = overrideEnvironment(DX_PROJECT_CONTEXT_ID='project-B00000000000000000000000')
        run("dx build --create-app --json " + app_dir, env=env)

    def test_invalid_execdepends(self):
        app_spec = {
            "name": "invalid_execdepends",
            "dxapi": "1.0.0",
            "runSpec": {
                "file": "code.py",
                "interpreter": "python2.7",
                "execDepends": {"name": "oops"}
                },
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("invalid_execdepends", json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp="Expected runSpec\.execDepends to"):
            run("dx build --json " + app_dir)

    def test_invalid_authorized_users(self):
        app_spec = {
            "name": "invalid_authorized_users",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "authorizedUsers": "PUBLIC"
            }
        app_dir = self.write_app_directory("invalid_authorized_users", json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp='Expected authorizedUsers to be a list of strings'):
            run("dx build --json " + app_dir)

        app_spec["authorizedUsers"] = ["foo"]
        app_dir = self.write_app_directory("invalid_authorized_users_2", json.dumps(app_spec),
                                           "code.py")
        with self.assertSubprocessFailure(stderr_regexp='contains an entry which is not'):
            run("dx build --json " + app_dir)

    def test_duplicate_keys_in_spec(self):
        app_spec = {
            "name": "test_duplicate_keys_in_spec",
            "dxapi": "1.0.0",
            "runSpec": {
                "file": "code.py",
                "interpreter": "python2.7"
            },
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        spec = json.dumps(app_spec).replace('"file": "code.py"', '"file": "code.py", "file": "code.py"')
        app_dir = self.write_app_directory("duplicate_keys_in_spec", spec, "code.py")
        with self.assertSubprocessFailure(stderr_regexp="duplicate key: "):
            run("dx build --json " + app_dir)

    def test_deps_without_network_access(self):
        app_spec = {
            "name": "test_deps_without_network_access",
            "dxapi": "1.0.0",
            "runSpec": {
                "file": "code.py",
                "interpreter": "python2.7",
                "execDepends": [{"name": "ddd", "package_manager": "pip"}]
                },
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("deps_without_network_access", json.dumps(app_spec),
                                           "code.py")
        with self.assertSubprocessFailure(stderr_regexp=("runSpec.execDepends specifies non-APT " +
                                                         "dependencies, but no network access spec " +
                                                         "is given")):
            run("dx build --json " + app_dir)

    def test_overwrite_applet(self):
        app_spec = {
            "name": "applet_overwriting",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("applet_overwriting", json.dumps(app_spec), "code.py")
        applet_id = json.loads(run("dx build --json " + app_dir))["id"]
        # Verify that we can succeed by writing to a different folder.
        run("dx mkdir subfolder")
        run("dx build --destination=subfolder/applet_overwriting " + app_dir)
        with self.assertSubprocessFailure():
            run("dx build " + app_dir)
        run("dx build -f " + app_dir)
        # Verify that the original app was deleted by the previous
        # dx build -f
        with self.assertSubprocessFailure(exit_code=3):
            run("dx describe " + applet_id)

    @unittest.skipUnless(testutil.TEST_CREATE_APPS,
                         'skipping test that would create apps')
    def test_update_app_categories(self):
        app1_spec = {
            "name": "update_app_categories",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "categories": ["A"]
            }
        app2_spec = {
            "name": "update_app_categories",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.1",
            "categories": ["B"]
            }
        app_dir = self.write_app_directory("update_app_categories", json.dumps(app1_spec), "code.py")
        app_id = json.loads(run("dx build --create-app --json " + app_dir))['id']
        self.assertEquals(json.loads(run("dx api " + app_id + " listCategories"))["categories"], ['A'])
        shutil.rmtree(app_dir)
        self.write_app_directory("update_app_categories", json.dumps(app2_spec), "code.py")
        run("dx build --create-app --json " + app_dir)
        self.assertEquals(json.loads(run("dx api " + app_id + " listCategories"))["categories"], ['B'])

    @unittest.skipUnless(testutil.TEST_CREATE_APPS, 'skipping test that would create apps')
    def test_update_app_authorized_users(self):
        app0_spec = {
            "name": "update_app_authorized_users",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "0.0.1"
            }
        app1_spec = {
            "name": "update_app_authorized_users",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0",
            "authorizedUsers": []
            }
        app2_spec = {
            "name": "update_app_authorized_users",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.1",
            "authorizedUsers": ["PUBLIC"]
            }
        app_dir = self.write_app_directory("update_app_authorized_users", json.dumps(app0_spec),
                                           "code.py")
        app_id = json.loads(run("dx build --create-app --json " + app_dir))['id']
        self.assertEquals(json.loads(run("dx api " + app_id +
                                         " listAuthorizedUsers"))["authorizedUsers"], [])
        shutil.rmtree(app_dir)
        self.write_app_directory("update_app_authorized_users", json.dumps(app1_spec), "code.py")
        run("dx build --create-app --json " + app_dir)
        self.assertEquals(json.loads(run("dx api " + app_id +
                                         " listAuthorizedUsers"))["authorizedUsers"], [])
        shutil.rmtree(app_dir)
        self.write_app_directory("update_app_authorized_users", json.dumps(app2_spec), "code.py")
        run("dx build --create-app --yes --json " + app_dir)
        self.assertEquals(json.loads(run("dx api " + app_id +
                                         " listAuthorizedUsers"))["authorizedUsers"], ["PUBLIC"])

    @unittest.skipUnless(testutil.TEST_CREATE_APPS,
                         'skipping test that would create apps')
    def test_dx_add_list_remove_users(self):
        '''
        This test is for some other dx subcommands, but it's in this
        test suite to take advantage of app-building methods.
        '''
        # Only create the app if it's not available already (makes
        # local testing easier)
        try:
            app_desc = dxpy.api.app_describe("app-test_dx_users", {})
            app_id = app_desc["id"]
            # reset users to empty list
            run("dx remove users app-test_dx_users " + " ".join(app_desc["authorizedUsers"]))
        except:
            app_id = None
        if app_id is None:
            app_spec = {
                "name": "test_dx_users",
                "dxapi": "1.0.0",
                "runSpec": {"file": "code.py", "interpreter": "python2.7"},
                "inputSpec": [],
                "outputSpec": [],
                "version": "0.0.1"
                }
            app_dir = self.write_app_directory("test_dx_users", json.dumps(app_spec), "code.py")
            app_id = json.loads(run("dx build --create-app --json " + app_dir))['id']
        # initialize as PUBLIC app
        run("dx add users test_dx_users PUBLIC")
        users = run("dx list users app-test_dx_users").strip()
        self.assertEqual(users, "PUBLIC")
        # use hash ID
        run("dx remove users " + app_id + " PUBLIC")
        users = run("dx list users app-test_dx_users").strip()
        self.assertEqual(users, "")
        # don't use "app-" prefix, duplicate and multiple members are fine
        run("dx add users test_dx_users PUBLIC eve user-eve org-piratelabs")
        users = run("dx list users app-test_dx_users").strip().split("\n")
        self.assertEqual(len(users), 3)
        self.assertIn("PUBLIC", users)
        self.assertIn("user-eve", users)
        self.assertIn("org-piratelabs", users)
        run("dx remove users test_dx_users eve org-piratelabs")
        # use version string
        users = run("dx list users app-test_dx_users/0.0.1").strip()
        self.assertEqual(users, 'PUBLIC')

        # bad paths and exit codes
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx add users nonexistentapp PUBLIC')
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx list users app-nonexistentapp')
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx remove users app-nonexistentapp/1.0.0 PUBLIC')
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run('dx add users test_dx_users org-nonexistentorg')
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run('dx add users test_dx_users nonexistentuser')
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run('dx add users test_dx_users piratelabs')

        # ResourceNotFound is not thrown when removing things
        run('dx remove users test_dx_users org-nonexistentorg')
        run('dx remove users test_dx_users nonexistentuser')
        run('dx remove users test_dx_users piratelabs')

    @unittest.skipUnless(testutil.TEST_CREATE_APPS,
                         'skipping test that would create apps')
    def test_dx_add_list_remove_developers(self):
        '''
        This test is for some other dx subcommands, but it's in this
        test suite to take advantage of app-building methods.
        '''
        # Only create the app if it's not available already (makes
        # local testing easier)
        try:
            app_desc = dxpy.api.app_describe("app-test_dx_developers", {})
            app_id = app_desc["id"]
            my_userid = app_desc["createdBy"]
            developers = dxpy.api.app_list_developers("app-test_dx_developers", {})["developers"]
            # reset developers to default list
            if len(developers) != 1:
                run("dx remove developers app-test_dx_developers " +
                    " ".join([dev for dev in developers if dev != my_userid]))
        except:
            app_id = None
        if app_id is None:
            app_spec = {
                "name": "test_dx_developers",
                "dxapi": "1.0.0",
                "runSpec": {"file": "code.py", "interpreter": "python2.7"},
                "inputSpec": [],
                "outputSpec": [],
                "version": "0.0.1"
                }
            app_dir = self.write_app_directory("test_dx_developers", json.dumps(app_spec), "code.py")
            app_desc = json.loads(run("dx build --create-app --json " + app_dir))
            app_id = app_desc['id']
            my_userid = app_desc["createdBy"]
        developers = run("dx list developers app-test_dx_developers").strip()
        self.assertEqual(developers, my_userid)
        # use hash ID
        run("dx add developers " + app_id + " eve")
        developers = run("dx list developers app-test_dx_developers").strip().split("\n")
        self.assertEqual(len(developers), 2)
        self.assertIn(my_userid, developers)
        # don't use "app-" prefix, duplicate, multiple, and non- members are fine
        run("dx remove developers test_dx_developers PUBLIC eve user-eve org-piratelabs")
        developers = run("dx list developers app-test_dx_developers").strip()
        self.assertEqual(developers, my_userid)
        # use version string
        run("dx list developers app-test_dx_developers/0.0.1")

        # bad paths and exit codes
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx add developers nonexistentapp eve')
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx list developers app-nonexistentapp')
        with self.assertSubprocessFailure(stderr_regexp='could not be resolved', exit_code=3):
            run('dx remove developers app-nonexistentapp/1.0.0 eve')
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run('dx add developers test_dx_developers nonexistentuser')
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run('dx add developers test_dx_developers piratelabs')

        # ResourceNotFound is not thrown when removing things
        run('dx remove developers test_dx_developers org-nonexistentorg')
        run('dx remove developers test_dx_developers nonexistentuser')
        run('dx remove developers test_dx_developers piratelabs')

        # Raise an error if you try to add an org developer (currently unsupported by the API)
        with self.assertSubprocessFailure(stderr_regexp='unsupported', exit_code=3):
            run('dx add developers test_dx_developers org-piratelabs')

    @unittest.skipUnless(testutil.TEST_CREATE_APPS,
                         'skipping test that would create apps')
    def test_build_app_autonumbering(self):
        app_spec = {
            "name": "build_app_autonumbering",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("build_app_autonumbering", json.dumps(app_spec), "code.py")
        run("dx build --create-app --json --publish " + app_dir)
        with self.assertSubprocessFailure(stderr_regexp="Could not create"):
            print(run("dx build --create-app --json --no-version-autonumbering " + app_dir))
        run("dx build --create-app --json " + app_dir) # Creates autonumbered version

    def test_build_failure(self):
        app_spec = {
            "name": "build_failure",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("build_failure", json.dumps(app_spec), "code.py")
        with open(os.path.join(app_dir, 'Makefile'), 'w') as makefile:
            makefile.write("all:\n\texit 7")
        with self.assertSubprocessFailure(stderr_regexp="make -j[0-9]+ in target directory failed with exit code"):
            run("dx build " + app_dir)
        # Somewhat indirect test of --no-parallel-build
        with self.assertSubprocessFailure(stderr_regexp="make in target directory failed with exit code"):
            run("dx build --no-parallel-build " + app_dir)

    def test_syntax_checks(self):
        app_spec = {
            "name": "syntax_checks",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("syntax_checks",
                                           json.dumps(app_spec),
                                           code_filename="code.py",
                                           code_content="def improper():\nprint 'oops'")
        with self.assertSubprocessFailure(stderr_regexp="Entry point file \\S+ has syntax errors"):
            run("dx build " + app_dir)
        run("dx build --no-check-syntax " + app_dir)

    @unittest.skipUnless(testutil.TEST_RUN_JOBS,
                         'skipping test that would run jobs')
    def test_build_and_run_applet_remote(self):
        app_spec = {
            "name": "build_applet_remote",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [
                {"name": "in1", "class": "int"},
            ],
            "outputSpec": [
                {"name": "out1", "class": "int"}
            ],
            "version": "1.0.0"
            }
        app_code = """import dxpy
@dxpy.entry_point("main")
def main(in1):
    return {"out1": in1 + 1}
"""
        app_dir = self.write_app_directory(
            'build_applet_remote', json.dumps(app_spec), code_filename='code.py', code_content=app_code)
        remote_build_output = run('dx build --remote ' + app_dir).strip().split('\n')[-1]
        # TODO: it would be nice to have the output of dx build --remote
        # more machine readable (perhaps when --json is specified)
        build_job_id = re.search('job-[A-Za-z0-9]{24}', remote_build_output).group(0)
        build_job_describe = json.loads(run('dx describe --json ' + build_job_id))
        applet_id = build_job_describe['output']['output_applet']['$dnanexus_link']
        invocation_job_id = run('dx run --brief --yes ' + applet_id + ' -iin1=8675309').strip()
        run('dx wait ' + invocation_job_id)
        invocation_job_describe = json.loads(run('dx describe --json ' + invocation_job_id))
        self.assertEquals(invocation_job_describe['output']['out1'], 8675310)

    def test_applet_help(self):
        app_spec = {
            "name": "applet_help",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [
                {"name": "reads", "class": "array:gtable", "type": "LetterReads", "label": "Reads",
                 "help": "One or more Reads table objects."},
                {"name": "required", "class": "file", "label": "Required", "help": "Another parameter"},
                {"name": "optional", "class": "file", "label": "Optional",
                 "help": "Optional parameter", "optional": True}
            ],
            "outputSpec": [
                {"name": "mappings", "class": "gtable", "type": "LetterMappings", "label": "Mappings",
                 "help": "The mapped reads."}
            ],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("åpplet_help", json.dumps(app_spec),
                                           code_filename="code.py", code_content="")
        applet_id = json.loads(run("dx build --json " + app_dir))["id"]
        applet_help = run("dx run " + applet_id + " -h")
        self.assertTrue("Reads: -ireads=(gtable, type LetterReads) [-ireads=... [...]]" in applet_help)
        self.assertTrue("Required: -irequired=(file)" in applet_help)
        self.assertTrue("Optional: [-ioptional=(file)]" in applet_help)
        self.assertTrue("Mappings: mappings (gtable, type LetterMappings)" in applet_help)

    def test_upload_resources(self):
        run("dx mkdir /subfolder")
        run("dx cd /subfolder")
        app_spec = {
            "name": "upload_resources",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("upload_åpp_resources", json.dumps(app_spec), "code.py")
        os.mkdir(os.path.join(app_dir, 'resources'))
        with open(os.path.join(app_dir, 'resources', 'test.txt'), 'w') as resources_file:
            resources_file.write('test\n')
        new_applet = json.loads(run("dx build --json " + app_dir))
        applet_describe = json.loads(run("dx describe --json " + new_applet["id"]))
        resources_file = applet_describe['runSpec']['bundledDepends'][0]['id']['$dnanexus_link']
        resources_file_describe = json.loads(run("dx describe --json " + resources_file))
        # Verify that the bundled depends appear in the same folder.
        self.assertEqual(resources_file_describe['folder'], '/subfolder')

    def test_archive_in_another_project(self):
        app_spec = {
            "name": "archive_in_another_project",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("archive_in_another_project", json.dumps(app_spec), "code.py")

        with temporary_project("Temporary working project", select=True) as temp_project:
            run("dx build -d {p}: {app_dir}".format(p=self.project, app_dir=app_dir))
            run("dx build --archive -d {p}: {app_dir}".format(p=self.project, app_dir=app_dir))


class TestDXBuildReportHtml(unittest.TestCase):
    js = "console.log('javascript');"
    css = "body {background-color: green;}"

    def setUp(self):
        self.temp_file_path = tempfile.mkdtemp()
        self.gif_base64 = "R0lGODdhAQABAIAAAAQCBAAAACwAAAAAAQABAAACAkQBADs="
        gif_file = open("{}/img.gif".format(self.temp_file_path), "w")
        gif_file.write(base64.b64decode(self.gif_base64))
        gif_file.close()
        wiki_logo = "http://upload.wikimedia.org/wikipedia/en/thumb/8/80/Wikipedia-logo-v2.svg/200px-Wikipedia-logo-v2.svg.png"
        script_file = open("{}/index.js".format(self.temp_file_path), "w")
        script_file.write(self.js)
        script_file.close()
        css_file = open("{}/index.css".format(self.temp_file_path), "w")
        css_file.write(self.css)
        css_file.close()
        html_file = open("{}/index.html".format(self.temp_file_path), "w")
        html = "<html><head><link rel='stylesheet' href='index.css' type='text/css'/><script src='index.js'></script></head><body><a href='/'/><a href='/' target='_new'/><img src='img.gif'/><img src='{}'/></body></html>".format(wiki_logo)
        html_file.write(html)
        html_file.close()

        self.proj_id = dxpy.api.project_new({'name': 'TestDXBuildReportHtml Project'})['id']
        os.environ['DX_PROJECT_CONTEXT_ID'] = self.proj_id

    def tearDown(self):
        shutil.rmtree(self.temp_file_path)
        dxpy.api.project_destroy(self.proj_id, {'terminateJobs': True})

    def test_local_file(self):
        run("dx-build-report-html {d}/index.html --local {d}/out.html".format(d=self.temp_file_path))
        out_path = "{}/out.html".format(self.temp_file_path)
        self.assertTrue(os.path.exists(out_path))
        f = open(out_path, "r")
        html = f.read()
        f.close()
        self.assertTrue(re.search(self.gif_base64, html))
        self.assertEquals(len(re.split("src=\"data:image", html)), 3)
        self.assertEquals(len(re.split("<img", html)), 3)
        self.assertTrue(re.search("target=\"_top\"", html))
        self.assertTrue(re.search("target=\"_new\"", html))
        self.assertTrue(re.search("<style", html))
        self.assertTrue(re.search(re.escape(self.css), html))
        self.assertFalse(re.search("<link", html))
        self.assertFalse(re.search("index.css", html))
        self.assertTrue(re.search(re.escape(self.js), html))
        self.assertFalse(re.search("index.js", html))

    def test_image_only(self):
        run("dx-build-report-html {d}/img.gif --local {d}/gif.html".format(d=self.temp_file_path))
        out_path = "{}/gif.html".format(self.temp_file_path)
        self.assertTrue(os.path.exists(out_path))
        f = open(out_path, "r")
        html = f.read()
        f.close()
        self.assertTrue(re.search("<img src=\"data:", html))

    def test_remote_file(self):
        report = json.loads(run("dx-build-report-html {d}/index.html --remote /html_report -w 47 -g 63".format(d=self.temp_file_path)))
        fileId = report["fileIds"][0]
        desc = json.loads(run("dx describe {record} --details --json".format(record=report["recordId"])))
        self.assertEquals(desc["types"], ["Report", "HTMLReport"])
        self.assertEquals(desc["name"], "html_report")
        self.assertEquals(desc["details"]["files"][0]["$dnanexus_link"], fileId)
        self.assertEquals(desc["details"]["width"], "47")
        self.assertEquals(desc["details"]["height"], "63")
        desc = json.loads(run("dx describe {file} --details --json".format(file=fileId)))
        self.assertTrue(desc["hidden"])
        self.assertEquals(desc["name"], "index.html")
        run("dx rm {record} {file}".format(record=report["recordId"], file=fileId))


class TestDXBedToSpans(DXTestCase):
    def setUp(self):
        super(TestDXBedToSpans, self).setUp()
        self.bed = """chr1\t127471196\t127472363\tPos1\t0\t+\t127471196\t127472363\t255,0,0
"""
        self.expected_tsv = """chr:string\tlo:int32\thi:int32\tname:string\tscore:float\tstrand:string\tthick_start:int32\tthick_end:int32\titem_rgb:string\r
chr1\t127471196\t127472363\tPos1\t0\t+\t127471196\t127472363\t255,0,0\r
"""
        self.tempdir = tempfile.mkdtemp()
        self.genome_id = makeGenomeObject()
    def tearDown(self):
        shutil.rmtree(self.tempdir)
        super(TestDXBedToSpans, self).tearDown()
    def test_bed_to_spans_conversion(self):
        tempfile1 = os.path.join(self.tempdir, 'test1.bed')
        with open(tempfile1, 'w') as f:
            f.write(self.bed)
        output = json.loads(run('dx-bed-to-spans {f} {g}'.format(f=tempfile1, g=self.genome_id)).strip().split('\n')[-1])
        table_id = output[0]['$dnanexus_link']
        self.assertTrue('Spans' in dxpy.api.gtable_describe(table_id, {})['types'])
        run('dx wait {g}'.format(g=table_id))
        self.assertEquals(run('dx export tsv -o - {g}'.format(g=table_id)), self.expected_tsv)
    def test_bed_spans_roundtrip(self):
        round_tripped_bed = """chr1\t127471196\t127472363\tPos1\t0\t+\t127471196\t127472363\t255,0,0
"""
        tempfile1 = os.path.join(self.tempdir, 'test1.bed')
        with open(tempfile1, 'w') as f:
            f.write(self.bed)
        output = json.loads(run('dx-bed-to-spans {f} {g}'.format(f=tempfile1, g=self.genome_id)).strip().split('\n')[-1])
        table_id = output[0]['$dnanexus_link']
        run('dx wait {g}'.format(g=table_id))
        run('dx-spans-to-bed --output {o} {g}'.format(o=os.path.join(self.tempdir, 'roundtrip.bed'), g=table_id))
        self.assertEquals(open(os.path.join(self.tempdir, 'roundtrip.bed')).read(), round_tripped_bed)


class TestDXBedToGenes(DXTestCase):
    def setUp(self):
        super(TestDXBedToGenes, self).setUp()
        self.bed = """chr1\t66999824\t67210768\tNM_032291\t0\t+\t67000041\t67208778\t0\t3\t227,64,25,\t0,91705,98928,
"""
        self.expected_tsv = """chr:string\tlo:int32\thi:int32\tname:string\tspan_id:int32\ttype:string\tstrand:string\tis_coding:boolean\tparent_id:int32\tframe:int16\tdescription:string\r
chr1\t66999824\t67000041\tNM_032291\t1\t5' UTR\t+\tFalse\t0\t-1\t\r
chr1\t66999824\t67210768\tNM_032291\t0\ttranscript\t+\tFalse\t-1\t-1\t\r
chr1\t67000041\t67000051\tNM_032291\t2\tCDS\t+\tTrue\t0\t-1\t\r
chr1\t67091529\t67091593\tNM_032291\t3\tCDS\t+\tTrue\t0\t-1\t\r
chr1\t67098752\t67098777\tNM_032291\t4\tCDS\t+\tTrue\t0\t-1\t\r
"""
        self.tempdir = tempfile.mkdtemp()
        self.genome_id = makeGenomeObject()
    def tearDown(self):
        shutil.rmtree(self.tempdir)
        super(TestDXBedToGenes, self).tearDown()
    def test_bed_to_genes_conversion(self):
        tempfile1 = os.path.join(self.tempdir, 'test1.bed')
        with open(tempfile1, 'w') as f:
            f.write(self.bed)
        output = json.loads(run('dx-bed-to-spans {f} {g}'.format(f=tempfile1, g=self.genome_id)).strip().split('\n')[-1])
        table_id = output[0]['$dnanexus_link']
        run('dx wait {g}'.format(g=table_id))
        self.assertTrue('Genes' in dxpy.api.gtable_describe(table_id, {})['types'])
        self.assertEquals(run('dx export tsv -o - {g}'.format(g=table_id)), self.expected_tsv)


class TestDXFastQToReads(DXTestCase):
    def setUp(self):
        super(TestDXFastQToReads, self).setUp()
        self.fastq = """@HWI-ST689:7:1101:1246:1986#0/1
NGGGGCCTAATTAAACTAAAGAGCTTCTGCACAGCAAAAGAAACTATGAACAGAGCAAACAGACAGAACAGGAGAAGATATTTGCAAATTATGCATCCAAC
+HWI-ST689:7:1101:1246:1986#0/1
BP\ccccceegggh]ghhhhhhhhhhhhhhhhhhhghefgedfghhhhhhhhh`eghhehhhfgfhhfggegbcdaabbbdddcbcZ`bb_bbbdcbbbb]
@HWI-ST689:7:1101:1477:1962#0/1
NGTAACTCCTCTTTGCAACACCACAGCCATCGCCCCCTACCTCCTTGCCAATCCCAGGCTCCTCTCCTGATGGTAACATTACTTTTCTCCTACTCTAAGGT
+HWI-ST689:7:1101:1477:1962#0/1
BP\ccceegfgggiiiifihhiihhihidghihfhfiiiiiiiiiihaffdghhgcgdbggfeeeedddR]bZLTZZ]bc`bccdcccccb`b`Y_BBBBB
"""
        self.expected_tsv = """name:string\tsequence:string\tquality:string\r
HWI-ST689:7:1101:1246:1986#0/1\tNGGGGCCTAATTAAACTAAAGAGCTTCTGCACAGCAAAAGAAACTATGAACAGAGCAAACAGACAGAACAGGAGAAGATATTTGCAAATTATGCATCCAAC\t#1=DDDDDFFHHHI>HIIIIIIIIIIIIIIIIIIIHIFGHFEGHIIIIIIIIIAFHIIFIIIGHGIIGHHFHCDEBBCCCEEEDCD;ACC@CCCEDCCCC>\r
HWI-ST689:7:1101:1477:1962#0/1\tNGTAACTCCTCTTTGCAACACCACAGCCATCGCCCCCTACCTCCTTGCCAATCCCAGGCTCCTCTCCTGATGGTAACATTACTTTTCTCCTACTCTAAGGT\t#1=DDDFFHGHHHJJJJGJIIJJIIJIJEHIJIGIGJJJJJJJJJJIBGGEHIIHDHECHHGFFFFEEE3>C;-5;;>CDACDDEDDDDDCACA:@#####\r
"""
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempdir)
        super(TestDXFastQToReads, self).tearDown()

    def test_fastq_to_reads_conversion(self):
        tempfile1 = os.path.join(self.tempdir, 'test1.fq')
        with open(tempfile1, 'w') as f:
            f.write(self.fastq)
        output = json.loads(run('dx-fastq-to-reads {f}'.format(f=tempfile1)).strip().split('\n')[-1])
        table_id = output['table_id']
        run('dx wait {g}'.format(g=table_id))
        self.assertEquals(run('dx export tsv -o - {g}'.format(g=table_id)), self.expected_tsv)

    def test_fastq_reads_roundtrip(self):
        round_tripped_fastq = """@HWI-ST689:7:1101:1246:1986#0/1
NGGGGCCTAATTAAACTAAAGAGCTTCTGCACAGCAAAAGAAACTATGAACAGAGCAAACAGACAGAACAGGAGAAGATATTTGCAAATTATGCATCCAAC
+
#1=DDDDDFFHHHI>HIIIIIIIIIIIIIIIIIIIHIFGHFEGHIIIIIIIIIAFHIIFIIIGHGIIGHHFHCDEBBCCCEEEDCD;ACC@CCCEDCCCC>
@HWI-ST689:7:1101:1477:1962#0/1
NGTAACTCCTCTTTGCAACACCACAGCCATCGCCCCCTACCTCCTTGCCAATCCCAGGCTCCTCTCCTGATGGTAACATTACTTTTCTCCTACTCTAAGGT
+
#1=DDDFFHGHHHJJJJGJIIJJIIJIJEHIJIGIGJJJJJJJJJJIBGGEHIIHDHECHHGFFFFEEE3>C;-5;;>CDACDDEDDDDDCACA:@#####
"""
        tempfile2 = os.path.join(self.tempdir, 'test2.fq')
        with open(tempfile2, 'w') as f:
            f.write(self.fastq)
        output = json.loads(run('dx-fastq-to-reads {f}'.format(f=tempfile2)).strip().split('\n')[-1])
        table_id = output['table_id']
        run('dx wait {g}'.format(g=table_id))
        run('dx-reads-to-fastq --output {o} {g}'.format(o=os.path.join(self.tempdir, 'roundtrip.fq'), g=table_id))
        self.assertEquals(open(os.path.join(self.tempdir, 'roundtrip.fq')).read(), round_tripped_fastq)


class TestDXGtfToGenes(DXTestCase):
    def setUp(self):
        super(TestDXGtfToGenes, self).setUp()
        self.expected_gtf = """chr1\t.\texon\t101\t200\t.\t+\t.\tgene_id ""; transcript_id "mytranscript-noncoding"
chr1\t.\tCDS\t151\t200\t.\t+\t0\tgene_id "mygene-coding"; transcript_id "mytranscript-coding"
"""
        self.tempdir = tempfile.mkdtemp()
        self.genome_id = makeGenomeObject()
    def tearDown(self):
        shutil.rmtree(self.tempdir)
        super(TestDXGtfToGenes, self).tearDown()
    def test_genes_to_gtf_conversion(self):
        genes_table = dxpy.new_dxgtable([
            dxpy.DXGTable.make_column_desc("type", "string"),
            dxpy.DXGTable.make_column_desc("span_id", "int64"),
            dxpy.DXGTable.make_column_desc("name", "string"),
            dxpy.DXGTable.make_column_desc("strand", "string"),
            dxpy.DXGTable.make_column_desc("is_coding", "boolean"),
            dxpy.DXGTable.make_column_desc("parent_id", "int64"),
            dxpy.DXGTable.make_column_desc("frame", "int64"),
            dxpy.DXGTable.make_column_desc("description", "string"),
            dxpy.DXGTable.make_column_desc("chr", "string"),
            dxpy.DXGTable.make_column_desc("lo", "int64"),
            dxpy.DXGTable.make_column_desc("hi", "int64")
        ])
        genes_table.add_rows(data=[
            ["transcript", 5, "mytranscript-noncoding", "+", False, -1, -1, "my test transcript", "chr1", 100, 200],
            ["exon", 6, "", "+", False, 5, -1, "", "chr1", 100, 200],
            ["gene", 54, "mygene-coding", "+", True, -1, -1, "my test gene", "chr1", 150, 200],
            ["transcript", 55, "mytranscript-coding", "+", True, 54, -1, "my test transcript", "chr1", 150, 200],
            ["CDS", 75, "", "+", True, 55, 0, "", "chr1", 150, 200]
        ])
        genes_table.set_details({
            "original_contigset": {"$dnanexus_link": self.genome_id}
        })
        genes_table.close(block=True)

        self.assertEquals(run('dx-genes-to-gtf {g}'.format(g=genes_table.get_id())),
                          self.expected_gtf)


class TestDXSamToMappings(DXTestCase):
    def setUp(self):
        super(TestDXSamToMappings, self).setUp()
        self.tempdir = tempfile.mkdtemp()
        self.expected_sam = """@SQ\tSN:chr1\tLN:249250621
@RG\tID:0\tSM:Sample_0
FOO.12345678\t0\t1\t54932369\t60\t7M1D93M\t*\t0\t0\tTAATAAGGTTGTTGTTGTTGTT\t1:1ADDDACFHA?HGFGIIE+<\tMD:Z:1A5^A93\tRG:Z:0
"""
        self.genome_id = makeGenomeObject()

    def tearDown(self):
        shutil.rmtree(self.tempdir)
        super(TestDXSamToMappings, self).tearDown()

    def test_mappings_to_sam_conversion(self):
        mappings_table = dxpy.new_dxgtable([
            dxpy.DXGTable.make_column_desc("sequence", "string"),
            dxpy.DXGTable.make_column_desc("quality", "string"),
            dxpy.DXGTable.make_column_desc("name", "string"),
            dxpy.DXGTable.make_column_desc("status", "string"),
            dxpy.DXGTable.make_column_desc("chr", "string"),
            dxpy.DXGTable.make_column_desc("lo", "int32"),
            dxpy.DXGTable.make_column_desc("hi", "int32"),
            dxpy.DXGTable.make_column_desc("negative_strand", "boolean"),
            dxpy.DXGTable.make_column_desc("error_probability", "uint8"),
            dxpy.DXGTable.make_column_desc("qc_fail", "boolean"),
            dxpy.DXGTable.make_column_desc("duplicate", "boolean"),
            dxpy.DXGTable.make_column_desc("cigar", "string"),
            dxpy.DXGTable.make_column_desc("template_id", "int64"),
            dxpy.DXGTable.make_column_desc("read_group", "uint16"),
            dxpy.DXGTable.make_column_desc("sam_field_MD", "string"),
            dxpy.DXGTable.make_column_desc("sam_field_XN", "int32")
        ])
        mappings_table.add_rows(data=[[
            "TAATAAGGTTGTTGTTGTTGTT",
            "1:1ADDDACFHA?HGFGIIE+<",
            "FOO.12345678",
            "PRIMARY",
            "1",
            54932368,
            54932390,
            False,
            60,
            False,
            False,
            "7M1D93M",
            289090731,
            0,
            "1A5^A93",
            -2147483648
        ]], part=1)
        mappings_table.set_details({
            "read_groups": [
                {"num_singles": 1, "num_pairs": 0}
            ],
            "original_contigset": {"$dnanexus_link": self.genome_id}
        })
        mappings_table.close(block=True)

        self.assertEquals(run('dx-mappings-to-sam {g}'.format(g=mappings_table.get_id())),
                          self.expected_sam)

class TestDXJobutilAddOutput(DXTestCase):
    dummy_hash = "123456789012345678901234"
    data_obj_classes = ['file', 'record', 'gtable', 'applet', 'workflow']
    dummy_ids = [obj_class + '-' + dummy_hash for obj_class in data_obj_classes]
    dummy_job_id = "job-" + dummy_hash
    dummy_analysis_id = "analysis-123456789012345678901234"
    test_cases = ([["32", 32],
                   ["3.4", 3.4],
                   ["true", True],
                   ["'32 tables'", "32 tables"],
                   ['\'{"foo": "bar"}\'', {"foo": "bar"}],
                   [dummy_job_id + ":foo", {"job": dummy_job_id,
                                            "field": "foo"}],
                   [dummy_analysis_id + ":bar",
                    {"$dnanexus_link": {"analysis": dummy_analysis_id,
                                        "field": "bar"}}]] +
                  [[dummy_id, {"$dnanexus_link": dummy_id}] for dummy_id in dummy_ids] +
                  [["'" + json.dumps({"$dnanexus_link": dummy_id}) + "'",
                    {"$dnanexus_link": dummy_id}] for dummy_id in dummy_ids])

    def test_auto(self):
        with tempfile.NamedTemporaryFile() as f:
            # initialize the file with valid JSON
            f.write('{}')
            f.flush()
            local_filename = f.name
            cmd_prefix = "dx-jobutil-add-output -o " + local_filename + " "
            for i, tc in enumerate(self.test_cases):
                run(cmd_prefix + str(i) + " " + tc[0])
            f.seek(0)
            result = json.load(f)
            for i, tc in enumerate(self.test_cases):
                self.assertEqual(result[str(i)], tc[1])

    def test_auto_array(self):
        with tempfile.NamedTemporaryFile() as f:
            # initialize the file with valid JSON
            f.write('{}')
            f.flush()
            local_filename = f.name
            cmd_prefix = "dx-jobutil-add-output --array -o " + local_filename + " "
            for i, tc in enumerate(self.test_cases):
                run(cmd_prefix + str(i) + " " + tc[0])
                run(cmd_prefix + str(i) + " " + tc[0])
            f.seek(0)
            result = json.load(f)
            for i, tc in enumerate(self.test_cases):
                self.assertEqual(result[str(i)], [tc[1], tc[1]])

    def test_class_specific(self):
        with tempfile.NamedTemporaryFile() as f:
            # initialize the file with valid JSON
            f.write('{}')
            f.flush()
            local_filename = f.name
            cmd_prefix = "dx-jobutil-add-output -o " + local_filename + " "
            class_test_cases = [["boolean", "t", True],
                                ["boolean", "1", True],
                                ["boolean", "0", False]]
            for i, tc in enumerate(class_test_cases):
                run(cmd_prefix + " ".join([str(i), "--class " + tc[0], tc[1]]))
            f.seek(0)
            result = json.load(f)
            for i, tc in enumerate(class_test_cases):
                self.assertEqual(result[str(i)], tc[2])

    def test_class_parsing_errors(self):
        with tempfile.NamedTemporaryFile() as f:
            # initialize the file with valid JSON
            f.write('{}')
            f.flush()
            local_filename = f.name
            cmd_prefix = "dx-jobutil-add-output -o " + local_filename + " "
            error_test_cases = ([["int", "3.4"],
                                 ["int", "foo"],
                                 ["float", "foo"],
                                 ["boolean", "something"],
                                 ["hash", "{]"],
                                 ["jobref", "thing"],
                                 ["analysisref", "thing"]] +
                                [[classname,
                                  "'" +
                                  json.dumps({"dnanexus_link": classname + "-" + self.dummy_hash}) +
                                  "'"] for classname in self.data_obj_classes])
            for i, tc in enumerate(error_test_cases):
                with self.assertSubprocessFailure(stderr_regexp='Value could not be parsed',
                                                  exit_code=3):
                    run(cmd_prefix + " ".join([str(i), "--class " + tc[0], tc[1]]))


@unittest.skipUnless(testutil.TEST_TCSH, 'skipping tests that require tcsh to be installed')
class TestTcshEnvironment(unittest.TestCase):
    def test_tcsh_dash_c(self):
        # tcsh -c doesn't set $_, or provide any other way for us to determine the source directory, so
        # "source environment" only works from DNANEXUS_HOME
        run('cd $DNANEXUS_HOME && env - HOME=$HOME PATH=/usr/local/bin:/usr/bin:/bin tcsh -c "source /etc/csh.cshrc && source /etc/csh.login && source $DNANEXUS_HOME/environment && dx --help"')
        run('cd $DNANEXUS_HOME && env - HOME=$HOME PATH=/usr/local/bin:/usr/bin:/bin tcsh -c "source /etc/csh.cshrc && source /etc/csh.login && source $DNANEXUS_HOME/environment.csh && dx --help"')

    def test_tcsh_source_environment(self):
        tcsh = pexpect.spawn("env - HOME=$HOME PATH=/usr/local/bin:/usr/bin:/bin tcsh")
        tcsh.logfile = sys.stdout
        tcsh.setwinsize(20, 90)
        tcsh.sendline("source /etc/csh.cshrc")
        tcsh.sendline("source /etc/csh.login")
        tcsh.sendline("dx")
        tcsh.expect("Command not found")
        tcsh.sendline("source ../../../environment")
        tcsh.sendline("dx")
        tcsh.expect("dx is a command-line client")

class TestDXScripts(DXTestCase):
    def test_minimal_invocation(self):
        # For dxpy scripts that have no other tests, these dummy calls
        # ensure that the coverage report is aware of them (instead of
        # excluding them altogether from the report, which artificially
        # inflates our %covered).
        #
        # This is a hack and obviously it would be preferable to figure
        # out why the coverage generator sometimes likes to include
        # these files and sometimes likes to exclude them.
        run('dx-gff-to-genes -h')
        run('dx-gtf-to-genes -h')
        run('dx-variants-to-vcf -h')
        run('dx-genes-to-gff -h')
        run('dx-genes-to-gtf -h')
        run('dx-mappings-to-fastq -h')
        run('dx-build-applet -h')


class TestDXCp(DXTestCase):
    @classmethod
    def setUpClass(cls):
        # setup two projects
        cls.proj_id1 = create_project()
        cls.proj_id2 = create_project()
        cls.counter = 1

    @classmethod
    def tearDownClass(cls):
        rm_project(cls.proj_id1)
        rm_project(cls.proj_id2)

    @classmethod
    def gen_uniq_fname(cls):
        cls.counter += 1
        return "file_{}".format(cls.counter)

    # Make sure a folder (path) has the same contents in the two projects.
    # Note: the contents of the folders are not listed recursively.
    def verify_folders_are_equal(self, path):
        listing_proj1 = list_folder(self.proj_id1, path)
        listing_proj2 = list_folder(self.proj_id2, path)
        self.assertEqual(listing_proj1, listing_proj2)

    def verify_file_ids_are_equal(self, path1, path2=None):
        if path2 is None:
            path2 = path1
        listing_proj1 = run("dx ls {proj}:/{path} --brief".format(proj=self.proj_id1, path=path1).strip())
        listing_proj2 = run("dx ls {proj}:/{path} --brief".format(proj=self.proj_id2, path=path2).strip())
        self.assertEqual(listing_proj1, listing_proj2)

    # create new file with the same name in the target
    #    dx cp  proj-1111:/file-1111   proj-2222:/
    def test_file_with_same_name(self):
        create_folder_in_project(self.proj_id1, "/earthsea")
        create_folder_in_project(self.proj_id2, "/earthsea")
        file_id = create_file_in_project(self.gen_uniq_fname(), self.proj_id1, folder="/earthsea")
        run("dx cp {p1}:/earthsea/{f} {p2}:/earthsea/".format(f=file_id, p1=self.proj_id1, p2=self.proj_id2))
        self.verify_folders_are_equal("/earthsea")

    # copy and rename
    #   dx cp  proj-1111:/file-1111   proj-2222:/file-2222
    def test_cp_rename(self):
        basename = self.gen_uniq_fname()
        file_id = create_file_in_project(basename, self.proj_id1)
        run("dx cp {p1}:/{f1} {p2}:/{f2}".format(f1=basename, f2="AAA.txt",
                                                 p1=self.proj_id1, p2=self.proj_id2))
        self.verify_file_ids_are_equal(basename, path2="AAA.txt")

    # multiple arguments
    #   dx cp  proj-1111:/file-1111 proj-2222:/file-2222 proj-3333:/
    def test_multiple_args(self):
        fname1 = self.gen_uniq_fname()
        fname2 = self.gen_uniq_fname()
        fname3 = self.gen_uniq_fname()
        create_file_in_project(fname1, self.proj_id1)
        create_file_in_project(fname2, self.proj_id1)
        create_file_in_project(fname3, self.proj_id1)
        run("dx cp {p1}:/{f1} {p1}:/{f2} {p1}:/{f3} {p2}:/".
            format(f1=fname1, f2=fname2, f3=fname3, p1=self.proj_id1, p2=self.proj_id2))
        self.verify_file_ids_are_equal(fname1)
        self.verify_file_ids_are_equal(fname2)
        self.verify_file_ids_are_equal(fname3)

    # copy an entire directory
    def test_cp_dir(self):
        create_folder_in_project(self.proj_id1, "/foo")
        run("dx cp {p1}:/foo {p2}:/".format(p1=self.proj_id1, p2=self.proj_id2))
        self.verify_folders_are_equal("/foo")

    # Weird error code:
    #   This part makes sense:
    #     'InvalidState: If cloned, a folder would conflict with the route of an existing folder.'
    #   This does not:
    #     'Successfully cloned from project: None, code 422'
    #
    def test_copy_empty_folder_on_existing_folder(self):
        create_folder_in_project(self.proj_id1, "/bar")
        create_folder_in_project(self.proj_id2, "/bar")
        with self.assertSubprocessFailure(stderr_regexp='If cloned, a folder would conflict', exit_code=3):
            run("dx cp {p1}:/bar {p2}:/".format(p1=self.proj_id1, p2=self.proj_id2))
        self.verify_folders_are_equal("/bar")

    def test_copy_folder_on_existing_folder(self):
        create_folder_in_project(self.proj_id1, "/baz")
        create_file_in_project(self.gen_uniq_fname(), self.proj_id1, folder="/baz")
        run("dx cp {p1}:/baz {p2}:/".format(p1=self.proj_id1, p2=self.proj_id2))
        with self.assertSubprocessFailure(stderr_regexp='If cloned, a folder would conflict', exit_code=3):
            run("dx cp {p1}:/baz {p2}:/".format(p1=self.proj_id1, p2=self.proj_id2))
        self.verify_folders_are_equal("/baz")

    # PTFM-13569: This used to give a weird error message, like so:
    # dx cp project-BV80zyQ0Ffb7fj64v03fffqX:/foo/XX.txt  project-BV80vzQ0P9vk785K1GgvfZKv:/foo/XX.txt
    # The following objects already existed in the destination container and were not copied:
    #   [
    #   "
    #   f
    #   l
    #   ...
    def test_copy_overwrite(self):
        fname1 = self.gen_uniq_fname()
        file_id1 = create_file_in_project(fname1, self.proj_id1)
        run("dx cp {p1}:/{f} {p2}:/{f}".format(p1=self.proj_id1, f=fname1, p2=self.proj_id2))
        output = run("dx cp {p1}:/{f} {p2}:/{f}".format(p1=self.proj_id1,
                                                        f=fname1, p2=self.proj_id2))
        self.assertIn("destination", output)
        self.assertIn("already existed", output)
        self.assertIn(file_id1, output)

    # 'dx cp' used to give a confusing error message when source file is not found.
    # Check that this has been fixed
    @unittest.skipUnless(os.environ.get("DX_RUN_NEXT_TESTS"),
                         "Skipping a test that relies on unreleased features")
    def test_error_msg_for_nonexistent_folder(self):
        fname1 = self.gen_uniq_fname()
        file_id1 = create_file_in_project(fname1, self.proj_id1)

        # The file {p1}:/{f} exists, however, {p1}/{f} does not. We
        # want to see an error message that reflects this.
        expected_err_msg = "A folder to be cloned \(/{p1}/{f}\) does not exist in the source container {p2}".format(
            p1=self.proj_id1, f=fname1, p2=self.project)
        with self.assertSubprocessFailure(stderr_regexp=expected_err_msg, exit_code=3):
            run("dx cp {p1}/{f} {p2}:/".format(p1=self.proj_id1, f=fname1, p2=self.proj_id2))

    @unittest.skip("PTFM-11906 This doesn't work yet.")
    def test_file_in_other_project(self):
        ''' Copy a file-id, where the file is not located in the default project-id.

        Main idea: create projects A and B. Create a file in A, and copy it to project B,
        -without- specifying a source project.

        This could work, with some enhancements to the 'dx cp' implementation.
        '''
        file_id = create_file_in_project(self.gen_uniq_fname(), self.proj_id1)
        run('dx cp ' + file_id + ' ' + self.proj_id2)

    @unittest.skipUnless(testutil.TEST_ENV,
                         'skipping test that would clobber your local environment')
    # This will start working, once PTFM-11906 is addressed. The issue is
    # that you must specify a project when copying a file. In theory this
    # can be addressed, because the project can be found, given the file-id.
    def test_no_env(self):
        ''' Try to copy a file when the context is empty.
        '''
        # create a file in the current project
        #  -- how do we get the current project id?
        file_id = create_file_in_project(self.gen_uniq_fname(), self.project)

        # Unset environment
        from dxpy.utils.env import write_env_var
        write_env_var('DX_PROJECT_CONTEXT_ID', None)
        del os.environ['DX_PROJECT_CONTEXT_ID']
        self.assertNotIn('DX_PROJECT_CONTEXT_ID', run('dx env --bash'))

        # Copy the file to a new project.
        # This does not currently work, because the context is not set.
        proj_id = create_project()
        with self.assertSubprocessFailure(stderr_regexp='project must be specified or a current project set',
                                          exit_code=1):
            run('dx cp ' + file_id + ' ' + proj_id)

        #cleanup
        rm_project(proj_id)


if __name__ == '__main__':
    if 'DXTEST_FULL' not in os.environ:
        sys.stderr.write('WARNING: env var DXTEST_FULL is not set; tests that create apps or run jobs will not be run\n')
    unittest.main()
