from __future__ import print_function, unicode_literals, division, absolute_import
import os
import unittest

from . import isolated_dir

from dxpy.sugar import processing as proc


class TestProc(unittest.TestCase):
    def test_run(self):
        with isolated_dir():
            proc.run_cmd(
                "echo -n 'foo'",
                stdout="foo.txt",
                echo=True,
                block=True
            )
            self.assertTrue(os.path.exists("foo.txt"))
            self.assertEqual(
                "foo",
                proc.run_cmd("cat foo.txt", block=True).output
            )

    def test_chain(self):
        with isolated_dir():
            proc.chain_cmds(
                ["echo -n 'foo'", "gzip"], stdout="foo.txt.gz", block=True
            )
            self.assertEqual(
                "foo", proc.chain_cmds(
                    ["gunzip -c foo.txt.gz", "cat"], block=True
                ).output
            )
