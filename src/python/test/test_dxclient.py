#!/usr/bin/env python
# coding: utf-8

import os, sys, unittest, json, tempfile, filecmp, subprocess, re, csv

def run(command):
    # print "Running", command
    result = subprocess.check_output(command, shell=True)
    print "Result for", command, ":\n", result
    return result

class TestDXClient(unittest.TestCase):
    def test_dx_actions(self):
        with self.assertRaises(subprocess.CalledProcessError):
            run("dx")
        run("dx help")
        proj_name = u"dxclient_test_pröject"
        folder_name = u"эксперимент 1"
        project = run(u"dx new project '{p}'".format(p=proj_name)).strip()
        os.environ["DX_PROJECT_CONTEXT_ID"] = project
        run("dx cd /")
        run("dx ls")
        run(u"dx mkdir '{f}'".format(f=folder_name))
        run(u"dx cd '{f}'".format(f=folder_name))
        with tempfile.NamedTemporaryFile() as f:
            local_filename = f.name
            filename = folder_name
            run(u"echo xyzzt > {tf}".format(tf=local_filename))
            run(u"dx upload {tf} --name='{f}' --folder='/{f}'".format(tf=local_filename, f=filename))
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
        run(u"dx cd ..")
        run(u"dx rmdir '{f}'".format(f=folder_name))

        table_name = folder_name
        with tempfile.NamedTemporaryFile(suffix='.csv') as f:
            writer = csv.writer(f)
            writer.writerows([['a:uint8', 'b:string', 'c:float'], [1, "x", 1.0], [2, "y", 4.0]])
            f.flush()
            run(u"dx import --name '{n}' --csv '{f}' --wait".format(n=table_name, f=f.name))
            run(u"dx get '{n}' --csv --output {o}".format(n=table_name, o=f.name))

        run(u"dx get_details '{n}'".format(n=table_name))

        run(u'dx tree')
        run(u"dx find data --name '{n}'".format(n=table_name))
        run(u"dx rename '{n}' '{n}'2".format(n=table_name))
        run(u"dx rename '{n}'2 '{n}'".format(n=table_name))
        run(u"dx set_properties '{n}' '{n}={n}' '{n}2={n}3'".format(n=table_name))
        run(u"dx tag '{n}' '{n}'2".format(n=table_name))
        run(u"yes|dx rmproject {p}".format(p=project))

if __name__ == '__main__':
    unittest.main()
