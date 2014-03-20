import codecs
import os
import sys

is_py2 = True if sys.version_info < (3, 0) else False

# This is defined in dxpy.utils.env as well but, unfortuntely, loading
# it from there introduces a circular dependency.
import locale
_sys_encoding = locale.getdefaultlocale()[1] or 'utf-8'

# The following adapters ensure consistent behavior of non-ASCII
# environment variable values across Python 2 and 3. Note that when
# setting variables in Python 2 you can pass in a bytes (str) object to
# set_env_var but in both Python 2 and 3 you will always receive a
# unicode object.

def _set_env_var_python2(var_name, value):
    # value may be bytes or unicode
    if type(value) is bytes:
        os.environ[var_name] = value
    else:
        os.environ[var_name] = value.encode(_sys_encoding)

def _get_env_var_python2(var_name, default=None):
    if var_name not in os.environ:
        return default
    return os.environ[var_name].decode(_sys_encoding)

def _set_env_var_python3(var_name, value):
    # value must be unicode
    assert type(value) is str
    os.environ[var_name] = value

def _get_env_var_python3(var_name, default=None):
    return os.environ.get(var_name, default)

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
    get_env_var = _get_env_var_python2
    set_env_var = _set_env_var_python2
else:
    from io import StringIO, BytesIO
    builtin_str = str
    str = str
    bytes = bytes
    basestring = (str, bytes)
    input = input
    builtin_int = int
    int = int
    get_env_var = _get_env_var_python3
    set_env_var = _set_env_var_python3

def set_stdio_encoding(encoding='utf-8'):
    if is_py2:
        sys.stdin = codecs.getreader(encoding)(sys.stdin)
        sys.stdout = codecs.getwriter(encoding)(sys.stdout)
        sys.stderr = codecs.getwriter(encoding)(sys.stderr)
