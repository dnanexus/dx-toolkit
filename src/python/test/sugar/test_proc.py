import contextlib
import os
import shutil
import tempfile
import unittest

from dxpy.sugar import proc

@contextlib.contextmanager
def temp_dir(*args, **kwargs):
    dname = tempfile.mkdtemp(*args, **kwargs)
    try:
        yield dname
    finally:
        shutil.rmtree(dname)


@contextlib.contextmanager
def isolated_dir():
    with temp_dir() as d:
        curdir = os.getcwd()
        os.chdir(d)
        try:
            yield d
        finally:
            os.chdir(curdir)


class TestProc(unittest.TestCase):
    def test_run(self):
        with isolated_dir():
            proc.run("echo 'foo'", output_file="foo.txt", block=True)
            self.assertEqual(
                "foo",
                proc.run("cat foo.txt", return_output=True, block=True)
            )

    def test_chain(self):
        with isolated_dir():
            proc.chain(["echo 'foo'", "gzip"], output_file="foo.txt.gz", block=True)
            self.assertEqual(
                "foo",
                proc.chain(
                    ["gunzip -c foo.txt.gz", "cat"], return_output=True, block=True
                )
            )
