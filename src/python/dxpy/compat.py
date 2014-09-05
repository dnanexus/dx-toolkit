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

import os, sys, io, locale, shlex
from io import TextIOWrapper
from contextlib import contextmanager

sys_encoding = locale.getdefaultlocale()[1] or 'UTF-8'

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
        encoded_prompt = prompt.encode(sys_encoding)
        with unwrap_stream("stdin"), unwrap_stream("stdout"):
            return raw_input(encoded_prompt).decode(sys_encoding)
    def expanduser(path):
        '''
        Copy of os.path.expanduser that decodes os.environ['HOME'] if necessary.
        '''
        if not path.startswith('~'):
            return path
        i = path.find('/', 1)
        if i < 0:
            i = len(path)
        if i == 1:
            if 'HOME' not in environ:
                import pwd
                userhome = pwd.getpwuid(os.getuid()).pw_dir
            else:
                userhome = environ['HOME']
                if isinstance(userhome, bytes):
                    userhome = userhome.decode(sys_encoding)
        else:
            import pwd
            try:
                pwent = pwd.getpwnam(path[1:i])
            except KeyError:
                return path
            userhome = pwent.pw_dir
        userhome = userhome.rstrip('/')
        return (userhome + path[i:]) or '/'
    if os.name == 'nt':
        # The POSIX os.path.expanduser doesn't work on NT, so just leave it be
        expanduser = os.path.expanduser
    shlex._split = shlex.split
    def _split(s, comments=False, posix=True):
        return [i.decode(sys_encoding) for i in shlex._split(s.encode(sys_encoding), comments=comments, posix=posix)]
    shlex.split = _split
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
    expanduser = os.path.expanduser

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
                                                 line_buffering=True)
                sys.stderr._original_stream = original_stream
            else:
                sys.stderr.write(__name__ + ": Warning: Unable to wrap sys.stderr with a text codec\n")

            _stdio_wrapped = True

def decode_command_line_args():
    if USING_PYTHON2:
        sys.argv = [i if isinstance(i, unicode) else i.decode(sys_encoding) for i in sys.argv]
    return sys.argv

class _Environ(object):
    def __getitem__(self, item):
        if not isinstance(item, bytes):
            item = item.encode(sys_encoding)
        value = os.environ[item]
        if isinstance(value, bytes):
            value = value.decode(sys_encoding)
        return value

    def __setitem__(self, varname, value):
        if not isinstance(varname, bytes):
            varname = varname.encode(sys_encoding)
        if not isinstance(value, bytes):
            value = value.encode(sys_encoding)
        os.environ[varname] = value

    def __contains__(self, item):
        if not isinstance(item, bytes):
            item = item.encode(sys_encoding)
        return True if item in os.environ else False

    def __getattr__(self, attr):
        return getattr(os.environ, attr)

    def __repr__(self):
        return repr(os.environ)

    def __iter__(self):
        for key in os.environ:
            yield key

    def copy(self):
        return {key: self[key] for key in self}

environ = _Environ()

def wrap_env_var_handlers():
    if USING_PYTHON2 and not getattr(os, '__native_getenv', None):
        native_getenv, native_putenv = os.getenv, os.putenv

        def getenv(varname, value=None):
            v = native_getenv(varname, value)
            if isinstance(v, bytes):
                v = v.decode(sys_encoding)
            return v

        def putenv(varname, value):
            if not isinstance(varname, bytes):
                varname = varname.encode(sys_encoding)
            if not isinstance(value, bytes):
                value = value.encode(sys_encoding)
            native_putenv(varname, value)

        os.getenv, os.putenv = getenv, putenv
        os.__native_getenv, os.__native_putenv = native_getenv, native_putenv

def unwrap_env_var_handlers():
    if USING_PYTHON2 and getattr(os, __native_getenv, None):
        os.getenv, os.putenv = os.__native_getenv, os.__native_putenv

@contextmanager
def unwrap_stream(stream_name):
    """
    Temporarily unwraps a given stream (stdin, stdout, or stderr) to undo the effects of wrap_stdio_in_codecs().
    """
    wrapped_stream = None
    try:
        wrapped_stream = getattr(sys, stream_name)
        if hasattr(wrapped_stream, '_original_stream'):
            setattr(sys, stream_name, wrapped_stream._original_stream)
        yield
    finally:
        if wrapped_stream:
            setattr(sys, stream_name, wrapped_stream)
