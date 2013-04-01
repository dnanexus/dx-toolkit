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

import os, unittest, json, tempfile, subprocess, csv, shutil, re

from contextlib import contextmanager

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
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        exc = subprocess.CalledProcessError(retcode, cmd, output=output)
        exc.stderr = err
        raise exc
    return output

def run(command, **kwargs):
    print "$ %s" % (command,)
    return check_output(command, shell=True, **kwargs)

def overrideEnvironment(**kwargs):
    env = os.environ.copy()
    for key in kwargs:
        if kwargs[key] is None:
            del env[key]
        else:
            env[key] = kwargs[key]
    return env

class TestDXClient(unittest.TestCase):
    project = None

    def tearDown(self):
        try:
            run(u"yes|dx rmproject {p}".format(p=TestDXClient.project))
        except:
            pass
        try:
            os.remove("uploadedfile")
        except:
            pass

    def test_dx_actions(self):
        with self.assertRaises(subprocess.CalledProcessError):
            run("dx")
        run("dx help")
        proj_name = u"dxclient_test_pröject"
        folder_name = u"эксперимент 1"
        project = run(u"dx new project '{p}' --brief".format(p=proj_name)).strip()
        os.environ["DX_PROJECT_CONTEXT_ID"] = project
        run("dx cd /")
        run("dx ls")
        run(u"dx mkdir '{f}'".format(f=folder_name))
        run(u"dx cd '{f}'".format(f=folder_name))
        with tempfile.NamedTemporaryFile() as f:
            local_filename = f.name
            filename = folder_name
            run(u"echo xyzzt > {tf}".format(tf=local_filename))
            fileid = run(u"dx upload {tf} -o '../{f}/{f}' --brief".format(tf=local_filename, f=filename))
            self.assertEqual(fileid, run(u"dx ls '../{f}/{f}' --brief".format(f=filename)))
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
        run(u"dx tag '{n}' '{n}'2".format(n=table_name))

        run(u"dx new record -o :foo --verbose")
        record_id = run(u"dx new record -o :foo2 --brief --visibility hidden --property foo=bar --property baz=quux --tag onetag --tag twotag --type foo --type bar --details '{\"hello\": \"world\"}'").strip()
        self.assertEqual(record_id, run(u"dx ls :foo2 --brief").strip())

        # describe
        desc = json.loads(run(u"dx describe {record} --details --json".format(record=record_id)))
        self.assertEqual(desc['tags'], ['onetag', 'twotag'])
        self.assertEqual(desc['types'], ['foo', 'bar'])
        self.assertEqual(desc['properties'], {"foo": "bar", "baz": "quux"})
        self.assertEqual(desc['details'], {"hello": "world"})
        self.assertEqual(desc['hidden'], True)

        run(u"dx rm :foo")
        run(u"dx rm :foo2")

        # Path resolution is used
        run(u"dx find jobs --project :")
        run(u"dx find data --project :")

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

        # compare output of old and new

        run(u"yes|dx rmproject {p}".format(p=project))

    def test_dx_mkdir(self):
        project = run(u"dx new project mkdir_test --brief").strip()
        os.environ["DX_PROJECT_CONTEXT_ID"] = project

        with self.assertRaises(subprocess.CalledProcessError):
            run(u'dx mkdir mkdirtest/b/c')
        run(u'dx mkdir -p mkdirtest/b/c')
        run(u'dx mkdir -p mkdirtest/b/c')
        run(u'dx rm -r mkdirtest')

        run(u"dx rmproject --yes {p}".format(p=project))

class TestDXBuildApp(unittest.TestCase):
    def setUp(self):
        self.temp_file_path = tempfile.mkdtemp()
    def tearDown(self):
        shutil.rmtree(self.temp_file_path)

    # Be sure to use the check_output defined in this module if you wish
    # to use stderr_regexp. Python's usual subprocess.check_output
    # doesn't propagate stderr back to us.
    @contextmanager
    def assertSubprocessFailure(self, output_regexp=None, stderr_regexp=None, exit_code=3):
        try:
            yield
        except subprocess.CalledProcessError as e:
            self.assertEqual(exit_code, e.returncode, "Expected command to return code %d but it returned code %d" % (exit_code, e.returncode))
            if output_regexp:
                print "stdout:"
                print e.output
                self.assertTrue(re.search(output_regexp, e.output), "Expected stdout to match '%s' but it didn't" % (output_regexp,))
            if stderr_regexp:
                if not hasattr(e, 'stderr'):
                    raise Exception('A stderr_regexp was supplied but the CalledProcessError did not return the contents of stderr')
                print "stderr:"
                print e.stderr
                self.assertTrue(re.search(stderr_regexp, e.stderr), "Expected stdout to match '%s' but it didn't" % (stderr_regexp,))
            return
        self.assertFalse(True, "Expected command to fail with CalledProcessError but it succeeded")

    def write_app_directory(self, app_name, dxapp_str, code_filename=None):
        os.mkdir(os.path.join(self.temp_file_path, app_name))
        with open(os.path.join(self.temp_file_path, app_name, 'dxapp.json'), 'w') as manifest:
            manifest.write(dxapp_str)
        if code_filename:
            with open(os.path.join(self.temp_file_path, app_name, code_filename), 'w') as code_file:
                code_file.write('\n')
        return os.path.join(self.temp_file_path, app_name)

    def test_help_without_security_context(self):
        env = overrideEnvironment(DX_SECURITY_CONTEXT=None, DX_APISERVER_HOST=None, DX_APISERVER_PORT=None, DX_APISERVER_PROTOCOL=None)
        run("dx-build-app -h", env=env)

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
        new_applet = json.loads(run("dx-build-applet --json " + app_dir))
        applet_describe = json.loads(run("dx describe --json " + new_applet["id"]))
        self.assertEqual(applet_describe["class"], "applet")
        self.assertEqual(applet_describe["id"], applet_describe["id"])
        self.assertEqual(applet_describe["name"], "minimal_applet")

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
        new_app = json.loads(run("dx-build-app --json " + app_dir))
        app_describe = json.loads(run("dx describe --json " + new_app["id"]))
        self.assertEqual(app_describe["class"], "app")
        self.assertEqual(app_describe["id"], app_describe["id"])
        self.assertEqual(app_describe["version"], "1.0.0")
        self.assertEqual(app_describe["name"], "minimal_app")
        self.assertFalse("published" in app_describe)

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
        run("dx-build-app --json " + app_dir, env=env)

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
            run("dx-build-applet --json " + app_dir)

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
        applet_id = json.loads(run("dx-build-applet --json " + app_dir))["id"]
        # Verify that we can succeed by writing to a different folder.
        run("dx mkdir subfolder")
        run("dx-build-applet --destination=subfolder/applet_overwriting " + app_dir)
        with self.assertSubprocessFailure():
            run("dx-build-applet " + app_dir)
        run("dx-build-applet -f " + app_dir)
        # Verify that the original app was deleted by the previous
        # dx-build-applet -f
        with self.assertSubprocessFailure(exit_code=1):
            run("dx describe " + applet_id)

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
        app_id = json.loads(run("dx-build-app --json " + app_dir))['id']
        self.assertEquals(json.loads(run("dx api " + app_id + " listCategories"))["categories"], ['A'])
        shutil.rmtree(app_dir)
        self.write_app_directory("update_app_categories", json.dumps(app2_spec), "code.py")
        run("dx-build-app --json " + app_dir)
        self.assertEquals(json.loads(run("dx api " + app_id + " listCategories"))["categories"], ['B'])

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
        run("dx-build-app --json --publish " + app_dir)
        with self.assertSubprocessFailure(stderr_regexp="Could not create"):
            print run("dx-build-app --json --no-version-autonumbering " + app_dir)
        run("dx-build-app --json " + app_dir) # Creates autonumbered version

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
            run("dx-build-applet " + app_dir)
        # Somewhat indirect test of --no-parallel-build
        with self.assertSubprocessFailure(stderr_regexp="make in target directory failed with exit code"):
            run("dx-build-applet --no-parallel-build " + app_dir)

if __name__ == '__main__':
    unittest.main()
