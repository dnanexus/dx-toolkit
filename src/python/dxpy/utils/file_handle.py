#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

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