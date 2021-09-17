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

    def test_stderr_stdout(self):
        p = proc.Processes([["echo", "hi"]], stdout=False, stderr=False)
        p.run(echo=True)
        p.block()
        assert p._stdout_type is proc._OTHER
        assert p._stderr_type is proc._OTHER
        assert p.stdout_stream is None
        assert p.stderr_stream is None
        with self.assertRaises(RuntimeError):
            p.output
        with self.assertRaises(RuntimeError):
            p.error

        p = proc.Processes([["echo", "hi"]], stdout=True, stderr=True)
        p.run(echo=True)
        p.block()
        assert p._stdout_type is proc._BUFFER
        assert p._stderr_type is proc._BUFFER
        assert p.stdout_stream is not None
        assert p.stderr_stream is not None
        assert p.output == b"hi\n"
        assert p.error == b""
