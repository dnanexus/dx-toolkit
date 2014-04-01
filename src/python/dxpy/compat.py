from __future__ import print_function, unicode_literals

import os, sys, io
from io import TextIOWrapper

is_py2 = True if sys.version_info < (3, 0) else False

_stdio_wrapped = False

if is_py2:
    from cStringIO import StringIO
    BytesIO = StringIO
    builtin_str = str
    bytes = str
    str = unicode
    basestring = basestring
    def input(prompt=None):
        if prompt:
            sys.stdout.write(prompt)
            sys.stdout.flush()
        return sys.stdin.readline().splitlines()[0]
    builtin_int = int
    int = long
    open = io.open
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
    if is_py2:
        global _stdio_wrapped
        if not _stdio_wrapped:
            class StderrTextIOWrapper(TextIOWrapper):
                def write(self, text):
                    if type(text) is unicode:
                        TextIOWrapper.write(self, text)
                    else:
                        TextIOWrapper.write(self, unicode(text, self.encoding))

            sys.stdin = io.open(sys.stdin.fileno(), encoding=sys.stdin.encoding)
            sys.stdout = StderrTextIOWrapper(io.FileIO(sys.stdout.fileno(), mode='w'), encoding=sys.stdout.encoding)
            sys.stderr = StderrTextIOWrapper(io.FileIO(sys.stderr.fileno(), mode='w'), encoding=sys.stderr.encoding)
            _stdio_wrapped = True
