from __future__ import absolute_import, division, print_function
import contextlib
import codecs
import gzip


@contextlib.contextmanager
def as_handle(path_or_handle, mode=None, is_gzip=False, **kwargs):
    """Open a file path, or pass through an already open file handle.

    Args:
        path_or_handle (str or file-like object):
            The file path to open, or an open file-like object with a 'read'
            method.
        mode (str): File open mode, e.g. 'r' or 'w'
        is_gzip (bool): Whether the file is (or should be) gzip-compressed.
        **kwargs (dict): Passed through to `open`

    Returns: file-like object
    """
    if mode is None:
        mode = 'rb' if is_gzip else 'r'
    if hasattr(path_or_handle, 'read'):
        # File handle is already open
        if is_gzip:
            yield gzip.GzipFile(fileobj=path_or_handle, mode=mode)
        else:
            yield path_or_handle
    else:
        # File path needs to be opened
        if 'encoding' in kwargs:
            opener = codecs.open
        elif is_gzip:
            opener = gzip.open
            # Need to add this for python 3.5
            if "r" in mode: mode = "rt"
        else:
            opener = open
        with opener(path_or_handle, mode=mode, **kwargs) as fp:
            yield fp