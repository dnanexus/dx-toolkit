from __future__ import print_function, unicode_literals, division, absolute_import
import contextlib
import os
import shutil
import subprocess
import tempfile
from uuid import uuid4


import logging
logging.basicConfig(level="INFO")


def random_name(fmt_str=None):
    rndstr = str(uuid4())
    if fmt_str:
        return fmt_str.format(rndstr)
    else:
        return rndstr


def make_random_files(n, fmt_str=None):
    filenames = []
    for i in range(n):
        fname = random_name(fmt_str)
        filenames.append(fname)
        with open(fname, "wt") as out:
            out.write("test{}".format(i))
    return filenames


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


def run(cmd):
    subprocess.check_call(cmd, shell=True)
