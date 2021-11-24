from pathlib import Path
import unittest

from . import isolated_dir

import dxpy.sugar.processing as proc


class TestProc(unittest.TestCase):
    def test_run(self):
        with isolated_dir():
            foo = Path("foo.txt")
            proc.run("echo -n 'foo'", stdout=foo, echo=True, block=True)
            self.assertTrue(foo.exists())
            self.assertEqual("foo", proc.sub(f"cat {foo}", block=True))

    def test_chain(self):
        with isolated_dir():
            foo = Path("foo.txt.gz")
            proc.run(["echo -n 'foo'", "gzip"], stdout=foo, block=True)
            self.assertEqual(
                "foo",
                proc.sub([f"gunzip -c {foo}", "cat"], block=True),
            )
