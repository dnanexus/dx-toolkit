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

import os, sys, unittest, json, tempfile, subprocess, csv, shutil, re, base64, random, time
from contextlib import contextmanager
import pexpect

import dxpy
from dxpy.scripts import dx_build_app
from dxpy_testutil import DXTestCase

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
        print err
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        exc = DXCalledProcessError(retcode, cmd, output=output, stderr=err)
        raise exc
    return output

def run(command, **kwargs):
    print "$ %s" % (command,)
    output = check_output(command, shell=True, **kwargs)
    print output
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
    sequence_file = dxpy.upload_string("")

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
    def test_dx_actions(self):
        with self.assertRaises(subprocess.CalledProcessError):
            run("dx")
        run("dx help")
        proj_name = u"dxclient_test_pröject"
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

        second_record_id = run(u"dx new record :somenewfolder/foo --parents --brief")
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
        run(u"dx find data --project :")

    def test_dx_describe_project(self):
        describe_output = run(u"dx describe :").strip()
        self.assertTrue(re.search(r'ID\s+%s.*\n.*\nName\s+dxclient_test_pr\xc3\xb6ject' % (self.project,),
                                  describe_output))

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

        # Download and re-import with gri
        with tempfile.NamedTemporaryFile(suffix='.csv') as fd:
            run(u"dx export tsv {gt} -o {fd} -f".format(gt=gri_gtable_id, fd=fd.name))
            fd.flush()
            run(u"dx import tsv {fd} -o gritableimport --gri mychr mylo myhi --wait".format(fd=fd.name))

            # Also, upload and download the file just to test out upload/download
            run(u"dx upload {fd} -o uploadedfile --wait".format(fd=fd.name))
            run(u"dx download uploadedfile -f")
            run(u"dx download uploadedfile -o -")

        second_desc = json.loads(run(u"dx describe gritableimport --json"))
        self.assertEqual(second_desc['types'], ['gri'])
        self.assertEqual(second_desc['indices'], [{"type":"genomic", "name":"gri", "chr":"mychr", "lo":"mylo", "hi":"myhi"}])
        self.assertEqual(desc['size'], second_desc['size'])
        self.assertEqual(desc['length'], second_desc['length'])

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
            with chdir(tempfile.mkdtemp()):
                run(u'dx download -r '+os.path.basename(wd))
                tree1 = subprocess.check_output("cd {wd}; find .".format(wd=wd), shell=True)
                tree2 = subprocess.check_output("cd {wd}; find .".format(wd=os.path.basename(wd)), shell=True)
                self.assertEqual(tree1, tree2)

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
        shell1.sendline("bash -c 'dx env'")
        expect_dx_env_cwd(shell1, "sessiontest1")

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

    def test_build_applet_with_no_dxapp_json(self):
        app_dir = self.write_app_directory("applet_with_no_dxapp_json", None, "code.py")
        with self.assertSubprocessFailure(stderr_regexp='does not contain dxapp\.json', exit_code=3):
            run("dx build " + app_dir)

    def test_build_applet_with_malformed_dxapp_json(self):
        app_dir = self.write_app_directory("applet_with_malformed_dxapp_json", "{", "code.py")
        with self.assertSubprocessFailure(stderr_regexp='Could not parse dxapp\.json file', exit_code=3):
            run("dx build " + app_dir)

    @unittest.skipIf('DXTEST_FULL' not in os.environ,
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

    @unittest.skipIf('DXTEST_FULL' not in os.environ,
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

    @unittest.skipIf('DXTEST_FULL' not in os.environ,
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

    @unittest.skipIf('DXTEST_FULL' not in os.environ,
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
            print run("dx build --create-app --json --no-version-autonumbering " + app_dir)
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
        run('dx wait {g}'.format(g=table_id))
        self.assertEquals(run('dx export tsv -o - {g}'.format(g=table_id)), self.expected_tsv)
    #def test_bed_spans_roundtrip(self):
    #    pass


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


if __name__ == '__main__':
    if 'DXTEST_FULL' not in os.environ:
        sys.stderr.write('WARNING: env var DXTEST_FULL is not set; tests that create apps will not be run\n')
    unittest.main()
