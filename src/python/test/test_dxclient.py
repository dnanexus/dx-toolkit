#!/usr/bin/env python
# coding: utf-8

import os, sys, unittest, json, tempfile, filecmp, subprocess, re

def run(command):
    result = subprocess.check_output(command, shell=True)
    # print "Result for", command, ":\n", result
    return result

class TestDXProject(unittest.TestCase):
    def test_dx_actions(self):
        with self.assertRaises(subprocess.CalledProcessError):
            run("dx")
        run("dx help")
        proj_name = "dxclient_test_pröject"
        folder_name = "эксперимент"
        result = run("dx new project {p}".format(p=proj_name))
        project = re.search("\((project-.+)\)$", result).group(1)
        os.environ["DX_PROJECT_CONTEXT_ID"] = project
        run("dx ls")
        run("dx mkdir {d}".format(d=folder_name))

if __name__ == '__main__':
    unittest.main()
