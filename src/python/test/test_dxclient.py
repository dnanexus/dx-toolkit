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

from __future__ import print_function

import os, sys, unittest, json, tempfile, subprocess, csv, shutil, re, base64, random, time
from contextlib import contextmanager
import pexpect, requests

import dxpy
from dxpy.scripts import dx_build_app
from dxpy_testutil import DXTestCase
import dxpy_testutil as testutil
from dxpy.exceptions import DXAPIError

@contextmanager
def chdir(dirname=None):
    curdir = os.getcwd()
    try:
        if dirname is not None:
            os.chdir(dirname)
        yield
    finally:
        os.chdir(curdir)

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
    """
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    if 'stderr' in kwargs:
        raise ValueError('stderr argument not allowed, it will be overridden.')
    process = subprocess.Popen(stdout=subprocess.PIPE, stderr=subprocess.PIPE, *popenargs, **kwargs)
    output, err = process.communicate()
    retcode = process.poll()
    if retcode:
        print(err)
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        exc = DXCalledProcessError(retcode, cmd, output=output, stderr=err)
        raise exc
    return output

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

class TestDXClient(DXTestCase):
    def test_dx_version(self):
        version = run("dx --version")
        self.assertIn("dx", version)

    def test_dx_actions(self):
        with self.assertRaises(subprocess.CalledProcessError):
            run("dx")
        run("dx help")
        folder_name = u"эксперимент 1"
        run("dx cd /")
        run("dx ls")
        run(u"dx mkdir '{f}'".format(f=folder_name))
        run(u"dx cd '{f}'".format(f=folder_name))
        with tempfile.NamedTemporaryFile() as f:
            local_filename = f.name
            filename = folder_name
            run(u"echo xyzzt > {tf}".format(tf=local_filename))
            fileid = run(u"dx upload --wait {tf} -o '../{f}/{f}' --brief".format(tf=local_filename, f=filename))
            self.assertEqual(fileid, run(u"dx ls '../{f}/{f}' --brief".format(f=filename)))
            self.assertEqual("xyzzt\n", run(u"dx head '../{f}/{f}'".format(f=filename)))
        run(u'dx pwd')
        run(u"dx cd ..")
        run(u'dx pwd')
        run(u'dx ls')
        with self.assertRaises(subprocess.CalledProcessError):
            run(u"dx rm '{f}'".format(f=filename))
        run(u"dx cd '{f}'".format(f=folder_name))

        run(u"dx mv '{f}' '{f}2'".format(f=filename))
        run(u"dx mv '{f}2' '{f}'".format(f=filename))

        run(u"dx rm '{f}'".format(f=filename))

        table_name = folder_name
        with tempfile.NamedTemporaryFile(suffix='.csv') as f:
            writer = csv.writer(f)
            writer.writerows([['a:uint8', 'b:string', 'c:float'], [1, "x", 1.0], [2, "y", 4.0]])
            f.flush()
            run(u"dx import csv -o '../{n}' '{f}' --wait".format(n=table_name, f=f.name))
            run(u"dx export csv '../{n}' --output {o} -f".format(n=table_name, o=f.name))

        run(u"dx get_details '../{n}'".format(n=table_name))

        run(u"dx cd ..")
        run(u"dx rmdir '{f}'".format(f=folder_name))

        run(u'dx tree')
        run(u"dx find data --name '{n}'".format(n=table_name))
        run(u"dx find data --name '{n} --property foo=bar'".format(n=table_name))
        run(u"dx rename '{n}' '{n}'2".format(n=table_name))
        run(u"dx rename '{n}'2 '{n}'".format(n=table_name))
        run(u"dx set_properties '{n}' '{n}={n}' '{n}2={n}3'".format(n=table_name))
        run(u"dx unset_properties '{n}' '{n}' '{n}2'".format(n=table_name))
        run(u"dx tag '{n}' '{n}'2".format(n=table_name))

        self.assertTrue(self.project in run(u"dx find projects --brief"))

        run(u"dx new record -o :foo --verbose")
        record_id = run(u"dx new record -o :foo2 --brief --visibility hidden --property foo=bar --property baz=quux --tag onetag --tag twotag --type foo --type bar --details '{\"hello\": \"world\"}'").strip()
        self.assertEqual(record_id, run(u"dx ls :foo2 --brief").strip())
        self.assertEqual({"hello": "world"}, json.loads(run(u"dx get -o - :foo2")))

        second_record_id = run(u"dx new record :somenewfolder/foo --parents --brief").strip()
        self.assertEqual(second_record_id, run(u"dx ls :somenewfolder/foo --brief").strip())

        # describe
        desc = json.loads(run(u"dx describe {record} --details --json".format(record=record_id)))
        self.assertEqual(desc['tags'], ['onetag', 'twotag'])
        self.assertEqual(desc['types'], ['foo', 'bar'])
        self.assertEqual(desc['properties'], {"foo": "bar", "baz": "quux"})
        self.assertEqual(desc['details'], {"hello": "world"})
        self.assertEqual(desc['hidden'], True)

        desc = json.loads(run(u"dx describe {record} --json".format(record=second_record_id)))
        self.assertEqual(desc['folder'], '/somenewfolder')

        run(u"dx rm :foo")
        run(u"dx rm :foo2")
        run(u"dx rm -r :somenewfolder")

        # Path resolution is used
        run(u"dx find jobs --project :")
        run(u"dx find executions --project :")
        run(u"dx find analyses --project :")
        run(u"dx find data --project :")

    def test_dx_get_record(self):
        run(u"dx new record -o :foo --verbose")
        run(u"dx get :foo")
        self.assertTrue(os.path.exists('foo.json'))
        run(u"dx get --no-ext :foo")
        self.assertTrue(os.path.exists('foo'))
        run("diff -q foo foo.json")

    def test_dx_object_tagging(self):
        the_tags = [u"Σ1=n", u"helloo0", u"ωω"]
        # tag
        record_id = run(u"dx new record Ψ --brief").strip()
        run(u"dx tag Ψ " + u" ".join(the_tags))
        mytags = dxpy.describe(record_id)['tags']
        for tag in the_tags:
            self.assertIn(tag, mytags)
        # untag
        run(u"dx untag Ψ " + u" ".join(the_tags[:2]))
        mytags = dxpy.describe(record_id)['tags']
        for tag in the_tags[:2]:
            self.assertNotIn(tag, mytags)
        self.assertIn(the_tags[2], mytags)

        # -a flag
        second_record_id = run(u"dx new record Ψ --brief").strip()
        self.assertNotEqual(record_id, second_record_id)
        run(u"dx tag -a Ψ " + u" ".join(the_tags))
        mytags = dxpy.describe(record_id)['tags']
        for tag in the_tags:
            self.assertIn(tag, mytags)
        second_tags = dxpy.describe(second_record_id)['tags']
        for tag in the_tags:
            self.assertIn(tag, second_tags)

        run(u"dx untag -a Ψ " + u" ".join(the_tags))
        mytags = dxpy.describe(record_id)['tags']
        self.assertEqual(len(mytags), 0)
        second_tags = dxpy.describe(second_record_id)['tags']
        self.assertEqual(len(second_tags), 0)

        # nonexistent name
        with self.assertSubprocessFailure(stderr_regexp='Could not resolve', exit_code=3):
            run(u"dx tag nonexistent atag")
        with self.assertSubprocessFailure(stderr_regexp='Could not resolve', exit_code=3):
            run(u"dx untag nonexistent atag")

    def test_dx_project_tagging(self):
        the_tags = [u"$my.tag", u"secoиdtag", u"тhird тagggg"]
        # tag
        run(u"dx tag : \\" + the_tags[0] + u" " + the_tags[1] + u" '" + the_tags[2] + u"'")
        mytags = dxpy.describe(self.project)['tags']
        for tag in the_tags:
            self.assertIn(tag, mytags)
        # untag
        run(u"dx untag : \\" + the_tags[0] + u" '" + the_tags[2] + u"'")
        mytags = dxpy.describe(self.project)['tags']
        self.assertIn(the_tags[1], mytags)
        for tag in [the_tags[0], the_tags[2]]:
            self.assertNotIn(tag, mytags)

        # nonexistent name
        with self.assertSubprocessFailure(stderr_regexp='Could not find a project named', exit_code=3):
            run(u"dx tag nonexistent: atag")
        with self.assertSubprocessFailure(stderr_regexp='Could not find a project named', exit_code=3):
            run(u"dx untag nonexistent: atag")

    def test_dx_object_properties(self):
        property_names = [u"Σ_1^n", u"helloo0", u"ωω"]
        property_values = [u"n", u"world z", u"ω()"]
        # set_properties
        record_id = run(u"dx new record Ψ --brief").strip()
        run(u"dx set_properties Ψ " + u" ".join([u"'" + prop[0] + u"'='" + prop[1] + u"'" for prop in zip(property_names, property_values)]))
        my_properties = dxpy.api.record_describe(record_id, {"properties": True})['properties']
        for (name, value) in zip(property_names, property_values):
            self.assertIn(name, my_properties)
            self.assertEqual(value, my_properties[name])
        # unset_properties
        run(u"dx unset_properties Ψ '" + u"' '".join(property_names[:2]) + u"'")
        my_properties = dxpy.api.record_describe(record_id, {"properties": True})['properties']
        for name in property_names[:2]:
            self.assertNotIn(name, my_properties)
        self.assertIn(property_names[2], my_properties)
        self.assertEqual(property_values[2], my_properties[property_names[2]])

        # -a flag
        second_record_id = run(u"dx new record Ψ --brief").strip()
        self.assertNotEqual(record_id, second_record_id)
        run(u"dx set_properties -a Ψ " + u" ".join([u"'" + prop[0] + u"'='" + prop[1] + u"'" for prop in zip(property_names, property_values)]))
        my_properties = dxpy.api.record_describe(record_id, {"properties": True})['properties']
        for (name, value) in zip(property_names, property_values):
            self.assertIn(name, my_properties)
            self.assertEqual(value, my_properties[name])
        second_properties = dxpy.api.record_describe(second_record_id, {"properties": True})['properties']
        for (name, value) in zip(property_names, property_values):
            self.assertIn(name, my_properties)
            self.assertEqual(value, my_properties[name])

        run(u"dx unset_properties -a Ψ '" + u"' '".join(property_names) + u"'")
        my_properties = dxpy.api.record_describe(record_id, {"properties": True})['properties']
        self.assertEqual(len(my_properties), 0)
        second_properties = dxpy.api.record_describe(second_record_id, {"properties": True})['properties']
        self.assertEqual(len(second_properties), 0)

        # nonexistent name
        with self.assertSubprocessFailure(stderr_regexp='Could not resolve', exit_code=3):
            run(u"dx set_properties nonexistent key=value")
        with self.assertSubprocessFailure(stderr_regexp='Could not resolve', exit_code=3):
            run(u"dx unset_properties nonexistent key")

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
        property_names = [u"$my.prop", u"secoиdprop", u"тhird prop"]
        property_values = [u"$hello.world", u"Σ2,n", u"stuff"]
        # set_properties
        run(u"dx set_properties : " + u" ".join([u"'" + prop[0] + u"'='" + prop[1] + u"'" for prop in zip(property_names, property_values)]))
        my_properties = dxpy.api.project_describe(self.project, {"properties": True})['properties']
        for (name, value) in zip(property_names, property_values):
            self.assertIn(name, my_properties)
            self.assertEqual(value, my_properties[name])
        # unset_properties
        run(u"dx unset_properties : '" + property_names[0] + u"' '" + property_names[2] + u"'")
        my_properties = dxpy.api.project_describe(self.project, {"properties": True})['properties']
        self.assertIn(property_names[1], my_properties)
        self.assertEqual(property_values[1], my_properties[property_names[1]])
        for name in [property_names[0], property_names[2]]:
            self.assertNotIn(name, my_properties)

        # nonexistent name
        with self.assertSubprocessFailure(stderr_regexp='Could not find a project named', exit_code=3):
            run(u"dx set_properties nonexistent: key=value")
        with self.assertSubprocessFailure(stderr_regexp='Could not find a project named', exit_code=3):
            run(u"dx unset_properties nonexistent: key")

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
        describe_output = run(u"dx describe :").strip()
        self.assertTrue(re.search(r'ID\s+%s.*\n.*\nName\s+dxclient_test_pr\xc3\xb6ject' % (self.project,),
                                  describe_output))
        self.assertIn('Properties', describe_output)

    def test_dx_remove_project_by_name(self):
        # TODO: this test makes no use of the DXTestCase-provided
        # project.
        project_name = "test_dx_remove_project_by_name_" + str(random.randint(0, 1000000)) + "_" + str(int(time.time() * 1000))
        project_id = run("dx new project {name} --brief".format(name=project_name)).strip()
        self.assertEqual(run("dx find projects --brief --name {name}".format(name=project_name)).strip(), project_id)
        run("dx rmproject -y {name}".format(name=project_name))
        self.assertEqual(run("dx find projects --brief --name {name}".format(name=project_name)), "")

    def test_dx_cp(self):
        project_name = "test_dx_cp_" + str(random.randint(0, 1000000)) + "_" + str(int(time.time() * 1000))
        dest_project_id = run("dx new project {name} --brief".format(name=project_name)).strip()
        try:
            record_id = run(u"dx new record --brief --details '{\"hello\": 1}'").strip()
            run("dx close --wait {r}".format(r=record_id))
            self.assertEqual(run("dx ls --brief {p}".format(p=dest_project_id)), "")
            run("dx cp {r} {p}".format(r=record_id, p=dest_project_id))
            self.assertEqual(run("dx ls --brief {p}".format(p=dest_project_id)).strip(), record_id)
        finally:
            run("dx rmproject -y {p}".format(p=dest_project_id))

    def test_dx_gtables(self):
        # new gtable
        gri_gtable_id = run(u"dx new gtable --gri mychr mylo myhi --columns mychr,mylo:int32,myhi:int32 --brief --property hello=world --details '{\"hello\":\"world\"}' --visibility visible").strip()
        # Add rows to it (?)
        # TODO: make this better.
        add_rows_input = {"data": [["chr", 1, 10], ["chr2", 3, 13], ["chr1", 3, 10], ["chr1", 11, 13], ["chr1", 5, 12]]}
        run(u"dx api {gt} addRows '{rows}'".format(gt=gri_gtable_id, rows=json.dumps(add_rows_input)))
        # close
        run(u"dx close {gt} --wait".format(gt=gri_gtable_id))

        # describe
        desc = json.loads(run(u"dx describe {gt} --details --json".format(gt=gri_gtable_id)))
        self.assertEqual(desc['types'], ['gri'])
        self.assertEqual(desc['indices'], [{"type":"genomic", "name":"gri", "chr":"mychr", "lo":"mylo", "hi":"myhi"}])
        self.assertEqual(desc['properties'], {"hello": "world"})
        self.assertEqual(desc['details'], {"hello": "world"})
        self.assertEqual(desc['hidden'], False)

        # gri query
        self.assertEqual(run(u"dx export tsv {gt} --gri chr1 1 10 -o -".format(gt=gri_gtable_id)),
                         '\r\n'.join(['mychr:string\tmylo:int32\tmyhi:int32', 'chr1\t3\t10', 'chr1\t5\t12', '']))

        # "get" is not supported on gtables
        with self.assertSubprocessFailure(stderr_regexp='given object is of class gtable', exit_code=3):
            run(u"dx get {gt}".format(gt=gri_gtable_id))

        # Download and re-import with gri
        with tempfile.NamedTemporaryFile(suffix='.csv') as fd:
            run(u"dx export tsv {gt} -o {fd} -f".format(gt=gri_gtable_id, fd=fd.name))
            fd.flush()
            run(u"dx import tsv {fd} -o gritableimport --gri mychr mylo myhi --wait".format(fd=fd.name))

            # Also, upload and download the file just to test out upload/download
            run(u"dx upload {fd} -o uploadedfile --wait".format(fd=fd.name))
            run(u"dx download uploadedfile -f")
            run(u"dx download uploadedfile -o -")
        try:
            os.remove("uploadedfile")
        except IOError:
            pass

        second_desc = json.loads(run(u"dx describe gritableimport --json"))
        self.assertEqual(second_desc['types'], ['gri'])
        self.assertEqual(second_desc['indices'], [{"type":"genomic", "name":"gri", "chr":"mychr", "lo":"mylo", "hi":"myhi"}])
        self.assertEqual(desc['size'], second_desc['size'])
        self.assertEqual(desc['length'], second_desc['length'])

    def test_dx_mkdir(self):
        with self.assertRaises(subprocess.CalledProcessError):
            run(u'dx mkdir mkdirtest/b/c')
        run(u'dx mkdir -p mkdirtest/b/c')
        run(u'dx mkdir -p mkdirtest/b/c')
        run(u'dx rm -r mkdirtest')

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
            print("*** Begin test_dxpy_session_isolation debug data")
            print(str(shell1))
            print("*** test_dxpy_session_isolation debug data, begin buffer:")
            print(str(shell1.buffer))
            print("*** End test_dxpy_session_isolation debug data")

class TestDXClientUploadDownload(DXTestCase):
    def test_dx_upload_download(self):
        with self.assertSubprocessFailure(stderr_regexp='expected the path to be a non-empty string', exit_code=3):
            run('dx download ""')
        wd = tempfile.mkdtemp()
        os.mkdir(os.path.join(wd, "a"))
        os.mkdir(os.path.join(wd, "a", u"б"))
        os.mkdir(os.path.join(wd, "a", u"б", "c"))
        with tempfile.NamedTemporaryFile(dir=os.path.join(wd, "a", u"б")) as fd:
            fd.write("0123456789ABCDEF"*64)
            fd.flush()
            with self.assertSubprocessFailure(stderr_regexp='is a directory but the -r/--recursive option was not given', exit_code=1):
                run(u'dx upload '+wd)
            run(u'dx upload -r '+wd)
            run(u'dx wait "{f}"'.format(f=os.path.join(os.path.basename(wd), "a", u"б", os.path.basename(fd.name))))
            with self.assertSubprocessFailure(stderr_regexp='is a folder but the -r/--recursive option was not given', exit_code=1):
                run(u'dx download '+os.path.basename(wd))
            old_dir = os.getcwd()
            with chdir(tempfile.mkdtemp()):
                run(u'dx download -r '+os.path.basename(wd))

                tree1 = subprocess.check_output("cd {wd}; find .".format(wd=wd), shell=True)
                tree2 = subprocess.check_output("cd {wd}; find .".format(wd=os.path.basename(wd)), shell=True)
                self.assertEqual(tree1, tree2)

            with chdir(tempfile.mkdtemp()):
                os.mkdir('t')
                run(u'dx download -r -o t '+os.path.basename(wd))
                tree1 = subprocess.check_output("cd {wd}; find .".format(wd=wd), shell=True)
                tree2 = subprocess.check_output("cd {wd}; find .".format(wd=os.path.join("t", os.path.basename(wd))),
                                                shell=True)
                self.assertEqual(tree1, tree2)

                os.mkdir('t2')
                run(u'dx download -o t2 '+os.path.join(os.path.basename(wd), "a", u"б", os.path.basename(fd.name)))
                self.assertEqual(os.stat(os.path.join("t2", os.path.basename(fd.name))).st_size,
                                 len("0123456789ABCDEF"*64))

    @unittest.skipUnless(testutil.TEST_ENV,
                         'skipping test that would clobber your local environment')
    def test_dx_download_no_env(self):
        testdir = tempfile.mkdtemp()
        with tempfile.NamedTemporaryFile(dir=testdir) as fd:
            fd.write("foo")
            fd.flush()
            file_id = run(u'dx upload ' + fd.name + ' --brief --wait').strip()
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
            file_id = run(u'dx upload ' + fd.name + ' --brief --wait').strip()
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

                run(u'dx upload -r {testdir}/{rootfile} {testdir}/a --wait'.format(testdir=testdir,
                                                                                   rootfile=os.path.basename(fd.name)))
                listing = run(u'dx ls').split('\n')
                self.assertIn("a/", listing)
                self.assertIn(os.path.basename(fd.name), listing)
                listing = run(u'dx ls a').split('\n')
                self.assertIn(os.path.basename(fd2.name), listing)

class TestDXClientDescribe(DXTestCase):
    def test_projects(self):
        run("dx describe :")
        run("dx describe " + self.project)
        run("dx describe " + self.project + ":")

        # need colon to recognize as project name
        with self.assertSubprocessFailure(exit_code=3):
            run(u"dx describe dxclient_test_pröject")

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

    def test_dx_run_tags_and_properties(self):
        # success
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"}
                                         })['id']
        property_names = [u"$my.prop", u"secoиdprop", u"тhird prop"]
        property_values = [u"$hello.world", u"Σ2,n", u"stuff"]
        the_tags = [u"Σ1=n", u"helloo0", u"ωω"]
        job_id = run(u"dx run " + applet_id + ' -inumber=32 --brief -y ' + \
                     u" ".join([u"--property '" + prop[0] + u"'='" + prop[1] + u"'" for prop in zip(property_names, property_values)]) + \
                     u"".join([u" --tag " + tag for tag in the_tags])).strip()

        job_desc = dxpy.api.job_describe(job_id)
        self.assertEqual(job_desc['tags'].sort(), the_tags.sort())
        self.assertEqual(len(job_desc['properties']), 3)
        for name, value in zip(property_names, property_values):
            self.assertEqual(job_desc['properties'][name], value)

        # Test setting tags and properties afterwards
        run("dx tag " + job_id + " foo bar foo")
        run(u"dx set_properties " + job_id + u" foo=bar Σ_1^n=n")
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
                self.assertIn(u"Σ_1^n=n", line)
                found_properties = True
        self.assertTrue(found_tags)
        self.assertTrue(found_properties)
        run("dx untag " + job_id + " foo")
        run(u"dx unset_properties " + job_id + u" Σ_1^n")
        job_desc = json.loads(run("dx describe " + job_id + " --json"))
        self.assertIn("bar", job_desc['tags'])
        self.assertNotIn("foo", job_desc['tags'])
        self.assertEqual(job_desc["properties"]["foo"], "bar")
        self.assertNotIn(u"Σ_1^n", job_desc["properties"])

    def test_dx_run_extra_args(self):
        # success
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "echo 'hello'"}
                                         })['id']
        job_id = run("dx run " + applet_id + ' -inumber=32 --name overwritten_name --delay-workspace-destruction --extra-args \'{"input": {"second": true}, "name": "new_name"}\' --brief -y').strip()
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
                              'applet'])
            fields_to_check = all_fields.difference(overridden_fields)
            for metadata in fields_to_check:
                if metadata == 'name':
                    self.assertEqual(new_job_desc[metadata], cloned_job_desc[metadata] + ' (re-run)')
                else:
                    self.assertEqual(new_job_desc[metadata], cloned_job_desc[metadata])

        # originally, set everything and have an instance type for all
        # entry points
        orig_job_id = run("dx run " + applet_id + ' -inumber=32 --name jobname --folder /output --instance-type dx_m1.large --brief -y').strip()
        orig_job_desc = dxpy.api.job_describe(orig_job_id)
        # control
        self.assertEqual(orig_job_desc['name'], 'jobname')
        self.assertEqual(orig_job_desc['project'], self.project)
        self.assertEqual(orig_job_desc['folder'], '/output')
        self.assertEqual(orig_job_desc['input'], {'number': 32})
        self.assertEqual(orig_job_desc['systemRequirements'], {'*': {'instanceType': 'dx_m1.large'}})

        # clone the job

        # nothing different
        new_job_desc = dxpy.api.job_describe(run("dx run --clone " + orig_job_id + " --brief -y").strip())
        check_new_job_metadata(new_job_desc, orig_job_desc)

        # override applet
        new_job_desc = dxpy.api.job_describe(run("dx run --clone " + orig_job_id + " " + other_applet_id + " --brief -y").strip())
        self.assertEqual(new_job_desc['applet'], other_applet_id)
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['applet'])

        # override name
        new_job_desc = dxpy.api.job_describe(run("dx run --clone " + orig_job_id + " --name newname --brief -y").strip())
        self.assertEqual(new_job_desc['name'], 'newname')
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['name'])

        # override folder
        new_job_desc = dxpy.api.job_describe(run("dx run --clone " + orig_job_id + " --folder /otherfolder --brief -y").strip())
        self.assertEqual(new_job_desc['folder'], '/otherfolder')
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['folder'])

        # override project
        new_job_desc = dxpy.api.job_describe(run("dx run --clone " + orig_job_id + " --project " + self.other_proj_id + " --brief -y").strip())
        self.assertEqual(new_job_desc['project'], self.other_proj_id)
        self.assertEqual(new_job_desc['folder'], '/')
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['project', 'folder'])

        # override input with -i
        new_job_desc = dxpy.api.job_describe(run("dx run --clone " + orig_job_id + " -inumber=42 --brief -y").strip())
        self.assertEqual(new_job_desc['input'], {"number": 42})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['input'])

        # add other input fields with -i
        new_job_desc = dxpy.api.job_describe(run("dx run --clone " + orig_job_id + " -inumber2=42 --brief -y").strip())
        self.assertEqual(new_job_desc['input'], {"number": 32, "number2": 42})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['input'])

        # override input with --input-json (original input discarded)
        new_job_desc = dxpy.api.job_describe(run("dx run --clone " + orig_job_id + " --input-json '{\"number2\": 42}' --brief -y").strip())
        self.assertEqual(new_job_desc['input'], {"number2": 42})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['input'])

        # override the blanket instance type
        new_job_desc = dxpy.api.job_describe(run("dx run --clone " + orig_job_id + " --instance-type dx_m1.medium --brief -y").strip())
        self.assertEqual(new_job_desc['systemRequirements'],
                         {'*': {'instanceType': 'dx_m1.medium'}})
        check_new_job_metadata(new_job_desc, orig_job_desc,
                               overridden_fields=['systemRequirements'])

        # override instance type for specific entry point(s)
        new_job_desc = dxpy.api.job_describe(run("dx run --clone " + orig_job_id + " --instance-type '{\"some_ep\": \"dx_m1.medium\", \"some_other_ep\": \"dx_m1.xlarge\"}' --brief -y").strip())
        self.assertEqual(new_job_desc['systemRequirements'],
                         {'*': {'instanceType': 'dx_m1.large'},
                          'some_ep': {'instanceType': 'dx_m1.medium'},
                          'some_other_ep': {'instanceType': 'dx_m1.xlarge'}})
        check_new_job_metadata(new_job_desc, orig_job_desc,
                               overridden_fields=['systemRequirements'])

        # new original job with entry point-specific systemRequirements
        orig_job_id = run("dx run " + applet_id + " --instance-type '{\"some_ep\": \"dx_m1.medium\"}' --brief -y").strip()
        orig_job_desc = dxpy.api.job_describe(orig_job_id)
        self.assertEqual(orig_job_desc['systemRequirements'], {'some_ep': {'instanceType': 'dx_m1.medium'}})

        # override all entry points
        new_job_desc = dxpy.api.job_describe(run("dx run --clone " + orig_job_id + " --instance-type dx_m1.large --brief -y").strip())
        self.assertEqual(new_job_desc['systemRequirements'], {'*': {'instanceType': 'dx_m1.large'}})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['systemRequirements'])

        # override a different entry point; original untouched
        new_job_desc = dxpy.api.job_describe(run("dx run --clone " + orig_job_id + " --instance-type '{\"some_other_ep\": \"dx_m1.large\"}' --brief -y").strip())
        self.assertEqual(new_job_desc['systemRequirements'],
                         {'some_ep': {'instanceType': 'dx_m1.medium'},
                          'some_other_ep': {'instanceType': 'dx_m1.large'}})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['systemRequirements'])

        # override the same entry point
        new_job_desc = dxpy.api.job_describe(run("dx run --clone " + orig_job_id + " --instance-type '{\"some_ep\": \"dx_m1.large\"}' --brief -y").strip())
        self.assertEqual(new_job_desc['systemRequirements'],
                         {'some_ep': {'instanceType': 'dx_m1.large'}})
        check_new_job_metadata(new_job_desc, orig_job_desc, overridden_fields=['systemRequirements'])

class TestDXClientWorkflow(DXTestCase):
    @unittest.skipUnless(testutil.TEST_RUN_JOBS, 'skipping test that would run jobs')
    def test_dx_run_workflow(self):
        applet_id = dxpy.api.applet_new({"project": self.project,
                                         "dxapi": "1.0.0",
                                         "inputSpec": [{"name": "number", "class": "int"}],
                                         "outputSpec": [{"name": "number", "class": "int"}],
                                         "runSpec": {"interpreter": "bash",
                                                     "code": "exit 1"}
                                         })['id']
        workflow_id = run("dx new workflow myworkflow --brief").strip()
        stage_id = dxpy.api.workflow_add_stage(workflow_id,
                                               {"editVersion": 0, "executable": applet_id})['stage']
        analysis_id = run("dx run " + workflow_id + " -i0.number=32 -y --brief").strip()
        self.assertTrue(analysis_id.startswith('analysis-'))
        analysis_desc = run("dx describe " + analysis_id)
        self.assertIn(stage_id + '.number = 32', analysis_desc)
        analysis_desc = json.loads(run("dx describe " + analysis_id + " --json"))
        time.sleep(2) # May need to wait for job to be created in the system
        job_desc = run("dx describe " + analysis_desc["stages"][0]["execution"]["id"])
        self.assertIn(' number = 32', job_desc)

        # Test setting tags and properties on analysis
        run("dx tag " + analysis_id + " foo bar foo")
        run(u"dx set_properties " + analysis_id + u" foo=bar Σ_1^n=n")
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
                self.assertIn(u"Σ_1^n=n", line)
                found_properties = True
        self.assertTrue(found_tags)
        self.assertTrue(found_properties)
        run("dx untag " + analysis_id + " foo")
        run(u"dx unset_properties " + analysis_id + u" Σ_1^n")
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
        run("dx add stage myworkflow myapplet -inumber=32 --instance-type dx_m1.large")
        run("dx add stage myworkflow myapplet -inumber=52 --instance-type dx_m1.medium")

        # run it
        analysis_id = run("dx run myworkflow -y --brief").strip()

        # test cases
        no_change_analysis_id = run("dx run --clone " + analysis_id + " --brief -y").strip()
        change_an_input_analysis_id = run("dx run --clone " + analysis_id + " -i0.number=52 --brief -y").strip()
        change_inst_type_analysis_id = run("dx run --clone " + analysis_id + " --instance-type dx_m1.large --brief -y").strip()

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
        self.assertEqual(change_an_input_analysis_desc['stages'][0]['execution']['input'], {"number": 52})
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
                         'dx_m1.large')

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
        all_stg_req_id = run('dx run myworkflow --instance-type dx_m1.medium -y --brief').strip()

        # request for a stage specifically (by name)
        stg_req_id = run('dx run myworkflow --instance-type an=awful=name=dx_m1.large --instance-type second=dx_m1.medium -y --brief').strip()

        time.sleep(2) # give time for all jobs to be populated

        no_req_desc = dxpy.describe(no_req_id)
        self.assertIsNone(no_req_desc['stages'][0]['execution']['instanceType'])
        self.assertIsNone(no_req_desc['stages'][1]['execution']['instanceType'])
        all_stg_req_desc = dxpy.describe(all_stg_req_id)
        self.assertEqual(all_stg_req_desc['stages'][0]['execution']['instanceType'],
                         'dx_m1.medium')
        self.assertEqual(all_stg_req_desc['stages'][1]['execution']['instanceType'],
                         'dx_m1.medium')
        stg_req_desc = dxpy.describe(stg_req_id)
        self.assertEqual(stg_req_desc['stages'][0]['execution']['instanceType'],
                         'dx_m1.large')
        self.assertEqual(stg_req_desc['stages'][1]['execution']['instanceType'],
                         'dx_m1.medium')

        # request for a stage specifically (by index); if same inst
        # type as before, should reuse results
        self.assertIn(stg_req_desc['stages'][0]['execution']['id'],
                      run('dx run myworkflow --instance-type 0=dx_m1.large -y'))
        # and by stage ID
        self.assertIn(stg_req_desc['stages'][0]['execution']['id'],
                      run('dx run myworkflow --instance-type ' + stage_ids[0] + '=dx_m1.large -y'))

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
        workflow_id = run(u"dx new workflow --title=тitle --summary=SΨmmary --description=DΣsc wØrkflØwname --output-folder /wØrkflØwØutput --brief").strip()
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc["id"], workflow_id)
        self.assertEqual(desc["editVersion"], 0)
        self.assertEqual(desc["name"], u"wØrkflØwname")
        self.assertEqual(desc["title"], u"тitle")
        self.assertEqual(desc["summary"], u"SΨmmary")
        self.assertEqual(desc["description"], u"DΣsc")
        self.assertEqual(desc["outputFolder"], u"/wØrkflØwØutput")
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
        run(u"dx add stage wØrkflØwname " + applet_id)

        new_workflow_id = run(u"dx new workflow --init wØrkflØwname --title newtitle " +
                              u"--summary newsummary --output-folder /output --brief").strip()
        desc = dxpy.describe(new_workflow_id)
        self.assertNotEqual(new_workflow_id, workflow_id)
        self.assertEqual(desc["id"], new_workflow_id)
        self.assertEqual(desc["editVersion"], 0)
        self.assertEqual(desc["name"], u"wØrkflØwname")
        self.assertEqual(desc["title"], "newtitle")
        self.assertEqual(desc["summary"], "newsummary")
        self.assertEqual(desc["description"], u"DΣsc")
        self.assertEqual(desc["outputFolder"], "/output")
        self.assertEqual(desc["project"], self.project)
        self.assertEqual(len(desc["stages"]), 1)
        self.assertEqual(desc["stages"][0]["executable"], applet_id)

        # run without --brief; should see initializedFrom information
        new_workflow_desc = run(u"dx new workflow --init " + workflow_id)
        self.assertIn(workflow_id, new_workflow_desc)

        # error when initializing from a nonexistent workflow
        run("dx rm " + workflow_id)
        with self.assertSubprocessFailure(stderr_regexp='ResourceNotFound', exit_code=3):
            run("dx new workflow --init " + workflow_id)

    def test_dx_workflow_resolution(self):
        with self.assertSubprocessFailure(stderr_regexp='Could not resolve', exit_code=3):
            run("dx update workflow foo")

        record_id = run("dx new record --type pipeline --brief").strip()
        run("dx describe " + record_id)
        with self.assertSubprocessFailure(stderr_regexp='Could not resolve', exit_code=3):
            run("dx update workflow " + record_id)

    def test_dx_describe_workflow(self):
        workflow_id = run(u"dx new workflow myworkflow --title title --brief").strip()
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
        first_stage = run("dx add stage " + workflow_id + " -inumber=10 " + applet_id + " --brief").strip()
        desc = run("dx describe myworkflow")
        self.assertIn("Input Spec", desc)
        self.assertIn("default=10", desc)

    def test_dx_add_remove_list_stages(self):
        workflow_id = run(u"dx new workflow myworkflow --title title --brief").strip()
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

        stage_ids.append(run("dx add stage " + workflow_id + " --name first " + applet_id + " --brief").strip())
        # not-yet-existing folder path should work
        # also, set input and instance type
        stage_ids.append(run("dx add stage myworkflow --relative-output-folder output myapplet --brief -inumber=32 --instance-type dx_m1.large").strip())
        # test relative folder path
        run("dx mkdir -p a/b/c")
        run("dx cd a/b/c")
        stage_ids.append(run("dx add stage " + workflow_id + " --name second --output-folder . " + applet_id + " --brief --instance-type '{\"main\": \"dx_m1.large\"}'").strip())
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
        self.assertEqual(desc['stages'][1]['systemRequirements'], {"*": {"instanceType": "dx_m1.large"}})
        self.assertEqual(desc['stages'][2]['folder'], '/a/b/c')
        self.assertEqual(desc['stages'][2]['systemRequirements'],
                         {"main": {"instanceType": "dx_m1.large"}})

        # errors
        # when adding a stage with both absolute and relative output folders
        with self.assertSubprocessFailure(stderr_regexp="output-folder", exit_code=2):
            run("dx add stage " + workflow_id + " " + applet_id + " --output-folder /foo --relative-output-folder foo")
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
        workflow_id = run(u"dx new workflow myworkflow --brief").strip()
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc['editVersion'], 0)
        self.assertEqual(desc['title'], "myworkflow")
        self.assertIsNone(desc["outputFolder"])

        # set title, summary, description, outputFolder
        run(u"dx update workflow myworkflow --title тitle --summary SΨmmary --description=DΣsc --output-folder .")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc['editVersion'], 1)
        self.assertEqual(desc['title'], u"тitle")
        self.assertEqual(desc['summary'], u"SΨmmary")
        self.assertEqual(desc['description'], u"DΣsc")
        self.assertEqual(desc['outputFolder'], u"/")

        # describe
        describe_output = run(u"dx describe myworkflow --delim ' '")
        self.assertIn(u"Output Folder /", describe_output)

        # unset title, outputFolder
        run(u"dx update workflow myworkflow --no-title --no-output-folder")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc['editVersion'], 2)
        self.assertEqual(desc['title'], "myworkflow")
        self.assertIsNone(desc['outputFolder'])

        # describe
        describe_output = run(u"dx describe myworkflow --delim ' '")
        self.assertNotIn(u"Title тitle", describe_output)
        self.assertIn(u"Summary SΨmmary", describe_output)
        self.assertNotIn("Description", describe_output)
        self.assertNotIn(u"DΣsc", describe_output)
        self.assertIn("Output Folder -", describe_output)
        describe_output = run("dx describe myworkflow --verbose --delim ' '")
        self.assertIn(u"Description DΣsc", describe_output)

        # no-op
        output = run(u"dx update workflow myworkflow")
        self.assertIn("No updates requested", output)
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc['editVersion'], 2)
        self.assertEqual(desc['title'], "myworkflow")

        with self.assertSubprocessFailure(stderr_regexp="no-title", exit_code=2):
            run("dx update workflow myworkflow --title foo --no-title")
        with self.assertSubprocessFailure(stderr_regexp="no-title", exit_code=2):
            run("dx update workflow myworkflow --output-folder /foo --no-output-folder")

    def test_dx_update_stage(self):
        workflow_id = run(u"dx new workflow myworkflow --brief").strip()
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
        run(u"dx update stage myworkflow 0 --name тitle -inumber=32 --relative-output-folder=foo --instance-type dx_m1.large")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc["editVersion"], 2)
        self.assertEqual(desc["stages"][0]["name"], u"тitle")
        self.assertEqual(desc["stages"][0]["folder"], "foo")
        self.assertEqual(desc["stages"][0]["input"]["number"], 32)
        self.assertEqual(desc["stages"][0]["systemRequirements"], {"*": {"instanceType": "dx_m1.large"}})

        # use a relative folder path and also set instance type using JSON
        run(u"dx update stage myworkflow 0 --name тitle -inumber=32 --output-folder=. --instance-type '{\"main\": \"dx_m1.large\"}'")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc["editVersion"], 3)
        self.assertEqual(desc["stages"][0]["folder"], u"/")
        self.assertEqual(desc["stages"][0]["systemRequirements"],
                         {"main": {"instanceType": "dx_m1.large"}})

        # unset name
        run("dx update stage myworkflow " + stage_id + " --no-name")
        desc = dxpy.api.workflow_describe(workflow_id)
        self.assertEqual(desc["editVersion"], 4)
        self.assertIsNone(desc["stages"][0]["name"])

        # set incompatible executable
        with self.assertSubprocessFailure(exit_code=3):
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
        output = run(u"dx update stage myworkflow 0 --alias default --force")
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

        found_records = run(u"dx find data --tag baz --brief").strip()
        self.assertEqual(found_records, dxpy.WORKSPACE_ID + ':' + record_ids[0])

        found_records = run(u"dx find data --tag Ψ --tag foo --tag foobar --brief").strip()
        self.assertEqual(found_records, '')

        found_records = run(u"dx find data --tag foo --tag Ψ --brief").strip().split("\n")
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[0], found_records)
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[1], found_records)

    def test_dx_find_data_by_property(self):
        record_ids = [run("dx new record --brief --property Ψ=world --property foo=bar --property bar=").strip(),
                      run("dx new record --brief --property Ψ=notworld --property foo=bar").strip()]

        found_records = run(u"dx find data --property Ψ=world --property foo=bar --brief").strip()
        self.assertEqual(found_records, dxpy.WORKSPACE_ID + ':' + record_ids[0])

        # presence
        found_records = run(u"dx find data --property Ψ --brief").strip().split("\n")
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[0], found_records)
        self.assertIn(dxpy.WORKSPACE_ID + ':' + record_ids[1], found_records)

        found_records = run(u"dx find data --property Ψ --property foo=baz --brief").strip()
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

    def test_dx_find_projects_by_tag(self):
        other_project_id = run("dx new project other --brief").strip()
        try:
            run(u"dx tag : Ψ world")
            proj_desc = dxpy.describe(dxpy.WORKSPACE_ID)
            self.assertEqual(len(proj_desc["tags"]), 2)
            self.assertIn(u"Ψ", proj_desc["tags"])
            self.assertIn("world", proj_desc["tags"])

            found_projects = run(u"dx find projects --tag Ψ --tag world --brief").strip().split('\n')
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertNotIn(other_project_id, found_projects)

            found_projects = run(u"dx find projects --tag Ψ --tag world --tag foobar --brief").strip().split('\n')
            self.assertNotIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertNotIn(other_project_id, found_projects)

            run(u"dx tag " + other_project_id + u" Ψ world foobar")
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
            run(u"dx set_properties : Ψ=world foo=bar bar=")
            proj_desc = dxpy.api.project_describe(dxpy.WORKSPACE_ID, {"properties": True})
            self.assertEqual(len(proj_desc["properties"]), 3)
            self.assertEqual(proj_desc["properties"][u"Ψ"], "world")
            self.assertEqual(proj_desc["properties"]["foo"], "bar")
            self.assertEqual(proj_desc["properties"]["bar"], "")

            run(u"dx set_properties " + other_project_id + u" Ψ=notworld foo=bar")

            found_projects = run(u"dx find projects --property Ψ=world --property foo=bar --brief").strip().split("\n")
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertNotIn(other_project_id, found_projects)

            found_projects = run(u"dx find projects --property bar= --brief").strip().split('\n')
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertNotIn(other_project_id, found_projects)

            # presence
            found_projects = run(u"dx find projects --property Ψ --brief").strip().split("\n")
            self.assertIn(dxpy.WORKSPACE_ID, found_projects)
            self.assertIn(other_project_id, found_projects)

            found_projects = run(u"dx find projects --property Ψ --property foo=baz --brief").strip().split("\n")
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
        property_names = [u"$my.prop", u"secoиdprop", u"тhird prop"]
        property_values = [u"$hello.world", u"Σ2,n", u"stuff"]
        the_tags = [u"Σ1=n", u"helloo0", u"ωω"]
        job_id = run(u"dx run " + applet_id + ' -inumber=32 --brief -y ' + \
                     u" ".join([u"--property '" + prop[0] + u"'='" + prop[1] + u"'" for prop in zip(property_names, property_values)]) + \
                     u"".join([u" --tag " + tag for tag in the_tags])).strip()

        # matches
        self.assertEqual(run(u"dx find jobs --brief --tag " + the_tags[0]).strip(), job_id)
        self.assertEqual(run(u"dx find jobs --brief" + u"".join([u" --tag " + tag for tag in the_tags])).strip(),
                         job_id)
        self.assertEqual(run(u"dx find jobs --brief --property " + property_names[1]).strip(), job_id)
        self.assertEqual(run(u"dx find jobs --brief --property '" + \
                             property_names[1] + u"'='" + property_values[1] + "'").strip(),
                         job_id)
        self.assertEqual(run(u"dx find jobs --brief" + \
                             u"".join([u" --property '" + key + u"'='" + value + u"'" for
                                       key, value in zip(property_names, property_values)])).strip(),
                         job_id)

        # no matches
        self.assertEqual(run(u"dx find jobs --brief --tag foo").strip(), "")
        self.assertEqual(run(u"dx find jobs --brief --property foo").strip(), "")
        self.assertEqual(run(u"dx find jobs --brief --property '" + \
                             property_names[1] + u"'=badvalue").strip(), "")

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
                        # Got a response on port 3129, but our proxy quit with an error, so it must be another process.
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
        self.temp_file_path = tempfile.mkdtemp()
        self.proj_id = dxpy.api.project_new({'name': 'TestDXBuildApp Project'})['id']
        os.environ['DX_PROJECT_CONTEXT_ID'] = self.proj_id

    def tearDown(self):
        shutil.rmtree(self.temp_file_path)
        dxpy.api.project_destroy(self.proj_id, {'terminateJobs': True})

    def write_app_directory(self, app_name, dxapp_str, code_filename=None, code_content="\n"):
        os.mkdir(os.path.join(self.temp_file_path, app_name))
        if dxapp_str is not None:
            with open(os.path.join(self.temp_file_path, app_name, 'dxapp.json'), 'w') as manifest:
                manifest.write(dxapp_str)
        if code_filename:
            with open(os.path.join(self.temp_file_path, app_name, code_filename), 'w') as code_file:
                code_file.write(code_content)
        return os.path.join(self.temp_file_path, app_name)

    def test_help_without_security_context(self):
        env = overrideEnvironment(DX_SECURITY_CONTEXT=None, DX_APISERVER_HOST=None, DX_APISERVER_PORT=None, DX_APISERVER_PROTOCOL=None)
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
            "name": "test_versioning_app",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("test_versioning_app", json.dumps(app_spec), "code.py")
        self.assertTrue(dx_build_app._get_version_suffix(app_dir, '1.0.0').startswith('+build.'))
        self.assertTrue(dx_build_app._get_version_suffix(app_dir, '1.0.0+git.abcdef').startswith('.build.'))

    def test_build_applet(self):
        app_spec = {
            "name": "minimal_applet",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("minimal_applet", json.dumps(app_spec), "code.py")
        new_applet = json.loads(run("dx build --json " + app_dir))
        applet_describe = json.loads(run("dx describe --json " + new_applet["id"]))
        self.assertEqual(applet_describe["class"], "applet")
        self.assertEqual(applet_describe["id"], applet_describe["id"])
        self.assertEqual(applet_describe["name"], "minimal_applet")

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
        app_dir = self.write_app_directory("first_applet", json.dumps(app_spec), "code.py")
        with open(os.path.join(app_dir, 'Readme.md'), 'w') as readme:
            readme.write('a readme file')
        first_expected_warnings = ["missing a name",
                                   "should be a short phrase not ending in a period",
                                   '"description" field shadows file',
                                   '"description" field should be written in complete sentences',
                                   'unrecognized category',
                                   'should end in "Importer"',
                                   'should end in "Exporter"',
                                   'input 0 has illegal name',
                                   'output 0 has illegal name']
        second_expected_warnings = ["should be all lowercase",
                                    "does not match containing directory",
                                    "missing a title",
                                    "missing a summary",
                                    "missing a description",
                                    "should be semver compliant"]
        try:
            # exit with error code to grab stderr
            run("dx build " + app_dir + "; exit 1")
        except Exception as err:
            for warning in first_expected_warnings:
                self.assertIn(warning, err.stderr)
            for warning in second_expected_warnings:
                self.assertNotIn(warning, err.stderr)

        app_spec = {
            "name": "Foo",
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py", "interpreter": "python2.7"},
            "inputSpec": [],
            "outputSpec": [],
            "version": "foo"
            }
        app_dir = self.write_app_directory("second_applet", json.dumps(app_spec), "code.py")
        try:
            # exit with error code to grab stderr
            run("dx build " + app_dir + "; exit 1")
        except Exception as err:
            for warning in first_expected_warnings:
                self.assertNotIn(warning, err.stderr)
            for warning in second_expected_warnings:
                self.assertIn(warning, err.stderr)

        # some more errors
        app_spec = {
            "dxapi": "1.0.0",
            "runSpec": {"file": "code.py"}
            }
        app_dir = self.write_app_directory("third_applet", json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp='interpreter field was not present'):
            run("dx build " + app_dir)

    def test_get_applet(self):
        # TODO: not sure why self.assertEqual doesn't consider
        # assertEqual to pass unless the strings here are unicode strings
        app_spec = {
            u"name": u"get_applet",
            u"dxapi": u"1.0.0",
            u"runSpec": {u"file": u"code.py", u"interpreter": u"python2.7"},
            u"inputSpec": [{u"name": u"in1", u"class": u"file"}],
            u"outputSpec": [{u"name": u"out1", u"class": u"file"}],
            u"description": u"Description\n",
            u"developerNotes": u"Developer notes\n",
            u"types": [u"Foo"],
            u"tags": [u"bar"],
            u"properties": {u"sample_id": u"123456"},
            u"details": {u"key1": u"value1"},
            }
        # description and developerNotes should be un-inlined back to files
        output_app_spec = dict((k, v) for (k, v) in app_spec.iteritems() if k not in ('description', 'developerNotes'))
        output_app_spec[u"runSpec"] = {u"file": u"src/code.py", u"interpreter": u"python2.7"}

        app_dir = self.write_app_directory("get_applet", json.dumps(app_spec), "code.py", code_content="import os\n")
        os.mkdir(os.path.join(app_dir, "resources"))
        with open(os.path.join(app_dir, "resources", "resources_file"), 'w') as f:
            f.write('content\n')
        new_applet_id = json.loads(run("dx build --json " + app_dir))["id"]
        tempdir = tempfile.mkdtemp()
        old_cwd = os.getcwd()
        os.chdir(tempdir)
        try:
            run("dx get " + new_applet_id)
            self.assertTrue(os.path.exists("get_applet"))
            self.assertTrue(os.path.exists(os.path.join("get_applet", "dxapp.json")))

            output_json = json.load(open(os.path.join("get_applet", "dxapp.json")))
            self.assertEqual(output_app_spec, output_json)

            self.assertEqual("Description\n", open(os.path.join("get_applet", "Readme.md")).read())
            self.assertEqual("Developer notes\n", open(os.path.join("get_applet", "Readme.developer.md")).read())
            self.assertEqual("import os\n", open(os.path.join("get_applet", "src", "code.py")).read())

            self.assertEqual("content\n", open(os.path.join("get_applet", "resources", "resources_file")).read())

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
            self.assertTrue(os.path.exists(os.path.join("destdir_withfile_force", "get_applet", "dxapp.json")))

            # -o file
            with open('destfile', 'w') as f:
                f.write('content')
            with self.assertSubprocessFailure(stderr_regexp='already exists', exit_code=3):
                run("dx get -o destfile get_applet")

            # -o file --overwrite
            run("dx get --overwrite -o destfile get_applet")
            self.assertTrue(os.path.exists("destfile"))
            self.assertTrue(os.path.exists(os.path.join("destfile", "dxapp.json")))
        finally:
            os.chdir(old_cwd)

    def test_get_applet_field_cleanup(self):
        # TODO: not sure why self.assertEqual doesn't consider
        # assertEqual to pass unless the strings here are unicode strings

        # When retrieving the applet, we'll get back an empty list for
        # types, tags, etc. Those should not be written back to the
        # dxapp.json so as not to pollute it.
        app_spec = {
            u"name": u"get_applet_field_cleanup",
            u"dxapi": u"1.0.0",
            u"runSpec": {u"file": u"code.py", u"interpreter": u"python2.7"},
            u"inputSpec": [],
            u"outputSpec": []
            }
        output_app_spec = app_spec.copy()
        output_app_spec[u"runSpec"] = {u"file": u"src/code.py", u"interpreter": u"python2.7"}

        app_dir = self.write_app_directory("get_applet_field_cleanup", json.dumps(app_spec), "code.py", code_content="import os\n")
        os.mkdir(os.path.join(app_dir, "resources"))
        with open(os.path.join(app_dir, "resources", "resources_file"), 'w') as f:
            f.write('content\n')
        new_applet_id = json.loads(run("dx build --json " + app_dir))["id"]
        tempdir = tempfile.mkdtemp()
        old_cwd = os.getcwd()
        os.chdir(tempdir)
        try:
            run("dx get " + new_applet_id)
            self.assertTrue(os.path.exists("get_applet_field_cleanup"))
            self.assertTrue(os.path.exists(os.path.join("get_applet_field_cleanup", "dxapp.json")))
            output_json = json.load(open(os.path.join("get_applet_field_cleanup", "dxapp.json")))
            self.assertEqual(output_app_spec, output_json)
            self.assertFalse(os.path.exists(os.path.join("get_applet", "Readme.md")))
            self.assertFalse(os.path.exists(os.path.join("get_applet", "Readme.developer.md")))
        finally:
            os.chdir(old_cwd)

    def test_build_applet_with_no_dxapp_json(self):
        app_dir = self.write_app_directory("applet_with_no_dxapp_json", None, "code.py")
        with self.assertSubprocessFailure(stderr_regexp='does not contain dxapp\.json', exit_code=3):
            run("dx build " + app_dir)

    def test_build_applet_with_malformed_dxapp_json(self):
        app_dir = self.write_app_directory("applet_with_malformed_dxapp_json", "{", "code.py")
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
        app_dir = self.write_app_directory("minimal_app", json.dumps(app_spec), "code.py")
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
        app_dir = self.write_app_directory("test_build_app_and_make_it_public", json.dumps(app_spec), "code.py")

        run("dx build --create-app --json " + app_dir)
        app_authorized_users = run("dx list users app-test_build_app_and_make_it_public")
        self.assertEqual(app_authorized_users, '')

        run("dx build --create-app --yes --version=1.0.1 --json " + app_dir)
        app_authorized_users = run("dx list users app-test_build_app_and_make_it_public")
        self.assertEqual(app_authorized_users.strip().split('\n'), ['PUBLIC'])

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
        app_dir = self.write_app_directory("invalid_authorized_users_2", json.dumps(app_spec), "code.py")
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
        app_dir = self.write_app_directory("deps_without_network_access", json.dumps(app_spec), "code.py")
        with self.assertSubprocessFailure(stderr_regexp="runSpec.execDepends specifies non-APT dependencies, but no network access spec is given"):
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
        app_dir = self.write_app_directory("update_app_authorized_users", json.dumps(app0_spec), "code.py")
        app_id = json.loads(run("dx build --create-app --json " + app_dir))['id']
        self.assertEquals(json.loads(run("dx api " + app_id + " listAuthorizedUsers"))["authorizedUsers"], [])
        shutil.rmtree(app_dir)
        self.write_app_directory("update_app_authorized_users", json.dumps(app1_spec), "code.py")
        run("dx build --create-app --json " + app_dir)
        self.assertEquals(json.loads(run("dx api " + app_id + " listAuthorizedUsers"))["authorizedUsers"], [])
        shutil.rmtree(app_dir)
        self.write_app_directory("update_app_authorized_users", json.dumps(app2_spec), "code.py")
        run("dx build --create-app --yes --json " + app_dir)
        self.assertEquals(json.loads(run("dx api " + app_id + " listAuthorizedUsers"))["authorizedUsers"], ["PUBLIC"])

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
                {"name": "reads", "class": "array:gtable", "type": "LetterReads", "label": "Reads", "help": "One or more Reads table objects."},
                {"name": "required", "class": "file", "label": "Required", "help": "Another parameter"},
                {"name": "optional", "class": "file", "label": "Optional", "help": "Optional parameter", "optional": True}
            ],
            "outputSpec": [
                {"name": "mappings", "class": "gtable", "type": "LetterMappings", "label": "Mappings", "help": "The mapped reads."}
            ],
            "version": "1.0.0"
            }
        app_dir = self.write_app_directory("applet_help", json.dumps(app_spec), code_filename="code.py", code_content="")
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
        app_dir = self.write_app_directory("upload_resources", json.dumps(app_spec), "code.py")
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
        temp_project_id = subprocess.check_output(
            u"dx new project '{p}' --brief".format(p="Temporary working project"), shell=True).strip()
        try:
            subprocess.check_output("dx select {p}".format(p=temp_project_id), shell=True)
            run("dx build -d {p}: {app_dir}".format(p=self.proj_id, app_dir=app_dir))
            run("dx build --archive -d {p}: {app_dir}".format(p=self.proj_id, app_dir=app_dir))
        finally:
            subprocess.check_output("dx select {p}".format(p=self.proj_id), shell=True)
            subprocess.check_output("dx rmproject --yes {p}".format(p=temp_project_id), shell=True)


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
        run(u"dx-build-report-html {d}/index.html --local {d}/out.html".format(d=self.temp_file_path))
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
        run(u"dx-build-report-html {d}/img.gif --local {d}/gif.html".format(d=self.temp_file_path))
        out_path = "{}/gif.html".format(self.temp_file_path)
        self.assertTrue(os.path.exists(out_path))
        f = open(out_path, "r")
        html = f.read()
        f.close()
        self.assertTrue(re.search("<img src=\"data:", html))

    def test_remote_file(self):
        report = json.loads(run(u"dx-build-report-html {d}/index.html --remote /html_report -w 47 -g 63".format(d=self.temp_file_path)))
        fileId = report["fileIds"][0]
        desc = json.loads(run(u"dx describe {record} --details --json".format(record=report["recordId"])))
        self.assertEquals(desc["types"], [u"Report", u"HTMLReport"])
        self.assertEquals(desc["name"], u"html_report")
        self.assertEquals(desc["details"]["files"][0]["$dnanexus_link"], fileId)
        self.assertEquals(desc["details"]["width"], "47")
        self.assertEquals(desc["details"]["height"], "63")
        desc = json.loads(run(u"dx describe {file} --details --json".format(file=fileId)))
        self.assertTrue(desc["hidden"])
        self.assertEquals(desc["name"], u"index.html")
        run(u"dx rm {record} {file}".format(record=report["recordId"], file=fileId))


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
    test_cases = [["32", 32],
                  ["3.4", 3.4],
                  ["true", True],
                  ["'32 tables'", "32 tables"],
                  ['\'{"foo": "bar"}\'', {"foo": "bar"}],
                  [dummy_job_id + ":foo", {"job": dummy_job_id,
                                           "field": "foo"}],
                  [dummy_analysis_id + ":bar",
                   {"$dnanexus_link": {"analysis": dummy_analysis_id,
                                       "field": "bar"}}]] + \
        [[dummy_id, {"$dnanexus_link": dummy_id}] for dummy_id in dummy_ids] + \
        [["'" + json.dumps({"$dnanexus_link": dummy_id}) + "'",
          {"$dnanexus_link": dummy_id}] for dummy_id in dummy_ids]

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
            error_test_cases = [["int", "3.4"],
                                ["int", "foo"],
                                ["float", "foo"],
                                ["boolean", "something"],
                                ["hash", "{]"],
                                ["jobref", "thing"],
                                ["analysisref", "thing"]] + \
                [[classname,
                  "'" + json.dumps({"dnanexus_link": classname + "-" + self.dummy_hash}) + "'"] for \
                 classname in self.data_obj_classes]
            for i, tc in enumerate(error_test_cases):
                with self.assertSubprocessFailure(stderr_regexp='Value could not be parsed',
                                                  exit_code=3):
                    run(cmd_prefix + " ".join([str(i), "--class " + tc[0], tc[1]]))

if __name__ == '__main__':
    if 'DXTEST_FULL' not in os.environ:
        sys.stderr.write('WARNING: env var DXTEST_FULL is not set; tests that create apps or run jobs will not be run\n')
    unittest.main()
