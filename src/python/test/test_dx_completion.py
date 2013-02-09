#!/usr/bin/env python

import os, sys, unittest, json, subprocess, re

import dxpy
from dxpy.utils.completer import *
from tempfile import TemporaryFile

IFS = "\v"

def split_line_like_bash(line, point):
    '''
    :returns: comp_words, comp_cword

    Split the line like bash would, and then put it back together with
    IFS, and calculate cword while you're at it.

    Use os.environ['COMP_POINT'] to figure out which word we're in
    '''
    point = int(point)
    cwords = []
    current_word = ''
    append_to_current_word = False
    for pos in range(len(line)):
        if pos == point:
            cword = len(cwords)

        if append_to_current_word:
            append_to_current_word = False
            current_word += line[pos]
        elif line[pos] == '\\':
            append_to_current_word = True
            current_word += line[pos]
        elif line[pos].isspace():
            if len(current_word) > 0:
                cwords.append(current_word)
                current_word = ''
        else:
            # non-whitespace in COMP_WORDBREAKS that get their own words: ><=:
            if line[pos] in '[<>=:]':
                if len(current_word) > 0:
                    cwords.append(current_word)
                    current_word = ''
                cwords.append(line[pos])
                if pos == point:
                    cword += 1
            else:
                current_word += line[pos]

    if len(current_word) > 0:
        cwords.append(current_word)
    elif line[-1].isspace():
        cwords.append('')

    if point == len(line):
        cword = len(cwords) - 1

    return IFS.join(cwords), str(cword)

class TestDXTabCompletion(unittest.TestCase):
    project_id = None
    ids_to_destroy = []

    @classmethod
    def setUpClass(cls):
        os.environ['DX_SECURITY_CONTEXT'] = json.dumps({"auth_token": "outside", "auth_token_type": "Bearer"})
        dxpy.set_security_context(json.loads(os.environ['DX_SECURITY_CONTEXT']))
        cls.project_id = dxpy.api.projectNew({"name": "tab-completion project"})['id']
        os.environ['DX_PROJECT_CONTEXT_ID'] = cls.project_id
        dxpy.set_workspace_id(cls.project_id)
        os.environ['IFS'] = IFS

    @classmethod
    def tearDownClass(cls):
        dxpy.api.projectDestroy(dxpy.WORKSPACE_ID)
        for entity_id in cls.ids_to_destroy:
            dxpy.DXHTTPRequest("/" + entity_id + "/destroy", {})

    def get_bash_completions(self, line, point=None):
        os.environ['ARGPARSE_AUTO_COMPLETE'] = '1'
        os.environ['COMP_LINE'] = line
        os.environ['COMP_POINT'] = point if point else str(len(line))
        os.environ['COMP_WORDS'], os.environ['COMP_CWORD'] = split_line_like_bash(line,
                                                                                  os.environ['COMP_POINT'])
        pipe = subprocess.Popen(line, stdout=subprocess.PIPE, shell=True).stdout
        return [result[:-1] if result[-1] == "\n" else result for result in pipe.read().split(IFS)]

    def assert_completion(self, line, completion):
        self.assertIn(completion, self.get_bash_completions(line))

    def assert_completions(self, line, completions):
        actual_completions = self.get_bash_completions(line)

        for completion in completions:
            self.assertIn(completion, actual_completions)

    def test_command_completion(self):
        self.assert_completion("dx ru", "run ")
        self.assert_completion("dx run", "run ")
        self.assert_completions("dx l", ["login ", "logout ", "ls "])
        self.assert_completions("dx ", ["login ", "logout ", "cp "])

    def test_subcommand_completion(self):
        self.assert_completions("dx find ", ["apps ", "data ", "jobs ", "projects "])
        self.assert_completions("dx new   ", ["project ", "record ", "gtable "])

    def test_option_completion(self):
        self.assert_completions("dx -", ["-h ", "--help ", "--env-help "])

    def test_applet_completion(self):
        dxapplet = dxpy.DXApplet()
        dxapplet.new(runSpec={"code": "placeholder", "interpreter": "bash"},
                     dxapi="1.0.0",
                     name="my applet")

        self.assert_completion("dx ls my", "my\\ applet ")
        self.assert_completion("dx ls", "ls ")
        self.assert_completion("dx run my", "my\\ applet ")
        self.assert_completion("dx ls ", "my\\ applet ")

    def test_pipeline_completion(self):
        dxpipeline = dxpy.new_dxrecord(name="my workflow", types=["pipeline"])
        self.assert_completion("dx run my", "my\\ workflow ")

    def test_project_completion(self):
        self.ids_to_destroy.append(dxpy.api.projectNew({"name": "to select"})['id'])
        self.assert_completion("dx select to", "to\ select: ")
        self.assert_completion("dx select to\ select:", " ")

    def test_local_file_completion(self):
        # test dx upload
        self.assertTrue(False, "Write me")

    def test_noninterference_of_local_files(self):
        # test dx ls with local files matching the prefix (no nonempty completions should be provided)
        # and dx ls foo:, where "foo" is not the name of an existing project
        self.assertTrue(False, "Write me")

if __name__ == '__main__':
    unittest.main()
