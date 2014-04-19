# Copyright (C) 2013-2014 DNAnexus, Inc.
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

from __future__ import (print_function, unicode_literals)

import os, sys, io
from io import TextIOWrapper

USING_PYTHON2 = True if sys.version_info < (3, 0) else False

_stdio_wrapped = False

if USING_PYTHON2:
    from cStringIO import StringIO
    BytesIO = StringIO
    builtin_str = str
    bytes = str
    str = unicode
    basestring = basestring
    builtin_int = int
    int = long
    open = io.open
    def input(prompt=None):
        try:
            cur_stdin, cur_stdout = sys.stdin, sys.stdout
            if hasattr(sys.stdin, '_original_stream'):
                sys.stdin = sys.stdin._original_stream
            if hasattr(sys.stdout, '_original_stream'):
                sys.stdout = sys.stdout._original_stream
            encoded_prompt = prompt.encode(getattr(sys.stdout, 'encoding', 'utf-8'))
            return raw_input(encoded_prompt).decode(getattr(sys.stdin, 'encoding', 'utf-8'))
        finally:
            sys.stdin, sys.stdout = cur_stdin, cur_stdout
else:
    from io import StringIO, BytesIO
    builtin_str = str
    str = str
    bytes = bytes
    basestring = (str, bytes)
    input = input
    builtin_int = int
    int = int
    open = open

def wrap_stdio_in_codecs():
    if USING_PYTHON2:
        global _stdio_wrapped
        if not _stdio_wrapped:
            class StderrTextIOWrapper(TextIOWrapper):
                def write(self, text):
                    if type(text) is unicode:
                        TextIOWrapper.write(self, text)
                    else:
                        TextIOWrapper.write(self, unicode(text, self.encoding))

            if hasattr(sys.stdin, 'fileno'):
                original_stream = sys.stdin
                sys.stdin = io.open(sys.stdin.fileno(), encoding=getattr(sys.stdin, 'encoding', None))
                sys.stdin._original_stream = original_stream
            else:
                sys.stderr.write(__name__ + ": Warning: Unable to wrap sys.stdin with a text codec\n")

            if hasattr(sys.stdout, 'fileno'):
                original_stream = sys.stdout
                sys.stdout = StderrTextIOWrapper(io.FileIO(sys.stdout.fileno(), mode='w'),
                                                 encoding=getattr(sys.stdout, 'encoding', None),
                                                 line_buffering=True if sys.stdout.isatty() else False)
                sys.stdout._original_stream = original_stream
            else:
                sys.stderr.write(__name__ + ": Warning: Unable to wrap sys.stdout with a text codec\n")

            if hasattr(sys.stderr, 'fileno'):
                original_stream = sys.stderr
                sys.stderr = StderrTextIOWrapper(io.FileIO(sys.stderr.fileno(), mode='w'),
                                                 encoding=getattr(sys.stderr, 'encoding', None),
                                                 line_buffering=True if sys.stderr.isatty() else False)
                sys.stderr._original_stream = original_stream
            else:
                sys.stderr.write(__name__ + ": Warning: Unable to wrap sys.stderr with a text codec\n")

            _stdio_wrapped = True
