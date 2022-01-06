import contextlib
import os
from pathlib import Path
import shutil
import tempfile
from typing import List
from uuid import uuid4

import logging

logging.basicConfig(level=logging.INFO)


def random_name(fmt_str=None):
    rndstr = str(uuid4())
    if fmt_str:
        return fmt_str.format(rndstr)
    else:
        return rndstr


def make_random_files(n, fmt_str=None, subdir=None) -> List[Path]:
    if subdir:
        os.makedirs(subdir, exist_ok=True)
    filenames = []
    for i in range(n):
        fname = Path(random_name(fmt_str))
        if subdir:
            fname = subdir / fname
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
