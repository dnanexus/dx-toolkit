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

import os, unittest, json, tempfile, subprocess, csv

def run(command):
    # print "Running", command
    result = subprocess.check_output(command, shell=True)
    print "Result for", command, ":\n", result
    return result

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

if __name__ == '__main__':
    unittest.main()
