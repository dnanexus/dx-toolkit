import codecs
import sys

is_py2 = True if sys.version_info < (3, 0) else False

if is_py2:
    from cStringIO import StringIO
    BytesIO = StringIO
    builtin_str = str
    bytes = str
    str = unicode
    basestring = basestring
    input = raw_input
    builtin_int = int
    int = long
else:
    from io import StringIO, BytesIO
    builtin_str = str
    str = str
    bytes = bytes
    basestring = (str, bytes)
    input = input
    builtin_int = int
    int = int

def set_stdio_encoding(encoding='utf-8'):
    if is_py2:
        sys.stdin = codecs.getreader(encoding)(sys.stdin)
        sys.stdout = codecs.getwriter(encoding)(sys.stdout)
        sys.stderr = codecs.getwriter(encoding)(sys.stderr)
