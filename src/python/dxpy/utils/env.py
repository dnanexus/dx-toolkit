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

"""
Logic for determining environment variable values.  See external
documentation
https://wiki.dnanexus.com/Command-Line-Client/Environment%20Variables
for more details.
"""

from __future__ import (print_function, unicode_literals)

import os, sys, textwrap, json, locale
from shutil import rmtree

from .. import DEFAULT_APISERVER_PROTOCOL, DEFAULT_APISERVER_HOST, DEFAULT_APISERVER_PORT
from ..compat import USING_PYTHON2, expanduser

sys_encoding = locale.getdefaultlocale()[1] or "UTF-8"

CORE_VAR_NAMES = ["DX_APISERVER_HOST", "DX_APISERVER_PORT", "DX_APISERVER_PROTOCOL", "DX_PROJECT_CONTEXT_ID",
                  "DX_WORKSPACE_ID", "DX_SECURITY_CONTEXT"]
STANDALONE_VAR_NAMES = ["DX_CLI_WD", "DX_USERNAME", "DX_PROJECT_CONTEXT_NAME"]
VAR_NAMES = CORE_VAR_NAMES + STANDALONE_VAR_NAMES

def get_global_conf_dir():
    return "/etc/dnanexus"

def get_user_conf_dir():
    if "DX_USER_CONF_DIR" in os.environ:
        return expanduser(os.environ["DX_USER_CONF_DIR"])
    return expanduser("~/.dnanexus_config")

def get_session_conf_dir(cleanup=True):
    """
    Tries to find the session configuration directory by looking in ~/.dnanexus_config/sessions/<PID>,
    where <PID> is pid of the parent of this process, then its parent, and so on.
    If none of those exist, the path for the immediate parent is given, even if it doesn't exist.

    If *cleanup* is True, looks up and deletes all session configuration directories that belong to nonexistent
    processes.
    """
    sessions_dir = os.path.join(get_user_conf_dir(), "sessions")
    try:
        from psutil import Process, pid_exists

        if cleanup:
            try:
                session_dirs = os.listdir(sessions_dir)
            except OSError as e:
                # Silently skip cleanup and continue if we are unable to
                # enumerate the session directories for any reason
                # (including, most commonly, because the sessions dir
                # doesn't exist)
                session_dirs = []
            for session_dir in session_dirs:
                if not pid_exists(int(session_dir)):
                    rmtree(os.path.join(sessions_dir, session_dir), ignore_errors=True)

        parent_process = Process(os.getpid()).parent()
        default_session_dir = os.path.join(sessions_dir, str(parent_process.pid))
        while parent_process is not None and parent_process.pid != 0:
            session_dir = os.path.join(sessions_dir, str(parent_process.pid))
            if os.path.exists(session_dir):
                return session_dir
            parent_process = parent_process.parent
        return default_session_dir
    except (ImportError, IOError, AttributeError):
        pass # psutil may not be available, or fail with IOError or AttributeError when /proc is not mounted
    except Exception as e:
        msg = "Unexpected error ({e}) while retrieving session configuration\n"
        sys.stderr.write(textwrap.fill(msg.format(e=type(e))))
    return _get_ppid_session_conf_dir(sessions_dir)

def _get_ppid_session_conf_dir(sessions_dir):
    try:
        return os.path.join(sessions_dir, str(os.getppid()))
    except AttributeError:
        pass # os.getppid is not available on Windows
    except Exception as e:
        msg = "Unexpected error ({e}) while retrieving session configuration\n"
        sys.stderr.write(textwrap.fill(msg.format(e=type(e))))
    return os.path.join(sessions_dir, str(os.getpid()))

def read_conf_dir(dirname):
    try:
        with open(os.path.join(dirname, "environment.json")) as fd:
            env_vars = json.load(fd)
    except:
        env_vars = {}

    for standalone_var in STANDALONE_VAR_NAMES:
        try:
            with open(os.path.join(dirname, standalone_var)) as fd:
                env_vars[standalone_var] = fd.read()
        except:
            pass
    return env_vars

def get_env(suppress_warning=False):
    """
    :returns env_vars: mapping of environment variable names to resolved values
    :type env_vars: dict

    This method looks up the known environment variables, and if they
    are not found, then attempts to resolve them by looking in the
    file ~/.dnanexus_config/environment, followed by the installed
    defaults in /opt/dnanexus/environment.
    """

    env_vars = read_conf_dir(get_global_conf_dir())
    env_vars.update(read_conf_dir(get_user_conf_dir()))
    env_vars.update(read_conf_dir(get_session_conf_dir()))
    env_overrides = []
    for var in VAR_NAMES:
        if var in os.environ:
            if var in env_vars and env_vars.get(var) != os.environ[var]:
                env_overrides.append(var)
            env_vars[var] = os.environ[var]
        elif var not in env_vars:
            env_vars[var] = None

    if sys.stdout.isatty():
        if not suppress_warning and len(env_overrides) > 0:
            sys.stderr.write(textwrap.fill("WARNING: The following environment variables were found to be different than the values last stored by dx: " + ", ".join(env_overrides), width=80) + "\n")
            sys.stderr.write(textwrap.fill('To use the values stored by dx, unset the environment variables in your shell by running "source ~/.dnanexus_config/unsetenv".  To clear the dx-stored values, run "dx clearenv".', width=80) + "\n")

    return env_vars

def write_env_var(var, value):
    user_conf_dir, session_conf_dir = get_user_conf_dir(), get_session_conf_dir(cleanup=False)
    try:
        os.makedirs(user_conf_dir, 0o700)
    except OSError:
        os.chmod(user_conf_dir, 0o700)
    try:
        os.makedirs(session_conf_dir, 0o700)
    except OSError:
        os.chmod(session_conf_dir, 0o700)

    write_env_var_to_conf_dir(var, value, user_conf_dir)
    write_env_var_to_conf_dir(var, value, session_conf_dir)

def write_env_var_to_conf_dir(var, value, conf_dir):
    env_jsonfile_path = os.path.join(conf_dir, "environment.json")
    if var in CORE_VAR_NAMES:
        try:
            with open(env_jsonfile_path) as fd:
                env_vars = json.load(fd)
        except:
            env_vars = {}
        if value is None and var in env_vars:
            del env_vars[var]
        else:
            env_vars[var] = value
        # Make sure the file has 600 permissions
        try:
            os.remove(env_jsonfile_path)
        except:
            pass
        with os.fdopen(os.open(env_jsonfile_path, os.O_CREAT | os.O_WRONLY, 0o600), "w") as fd:
            json.dump(env_vars, fd, indent=4)
            fd.write("\n")
    else: # DX_CLI_WD, DX_USERNAME, DX_PROJECT_CONTEXT_NAME
        # Make sure the file has 600 permissions
        try:
            os.remove(os.path.join(conf_dir, var))
        except:
            pass
        with os.fdopen(os.open(os.path.join(conf_dir, var), os.O_CREAT | os.O_WRONLY, 0o600), "w") as fd:
            fd.write(value.encode(sys_encoding) if USING_PYTHON2 else value)

    if not os.path.exists(expanduser("~/.dnanexus_config/") + "unsetenv"):
        with open(expanduser("~/.dnanexus_config/") + "unsetenv", "w") as fd:
            for var in CORE_VAR_NAMES:
                fd.write("unset " + var + "\n")

def clearenv(args):
    if args.interactive:
        print("The clearenv command is not available in the interactive shell")
        return
    rmtree(get_session_conf_dir(), ignore_errors=True)
    try:
        os.remove(expanduser("~/.dnanexus_config/environment"))
    except:
        pass
    try:
        os.remove(expanduser("~/.dnanexus_config/environment.json"))
    except:
        pass
    for f in STANDALONE_VAR_NAMES:
        try:
            os.remove(expanduser("~/.dnanexus_config/" + f))
        except:
            pass

    if args.reset:
        defaults = {"DX_SECURITY_CONTEXT": json.dumps({"auth_token": "", "auth_token_type": ""}),
                    "DX_APISERVER_PROTOCOL": DEFAULT_APISERVER_PROTOCOL,
                    "DX_APISERVER_HOST": DEFAULT_APISERVER_HOST,
                    "DX_APISERVER_PORT": DEFAULT_APISERVER_PORT,
                    "DX_CLI_WD": "/"}
        for var in VAR_NAMES:
            if var in defaults:
                write_env_var(var, defaults[var])
            else:
                write_env_var(var, "")

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
        os.environ[var_name] = value.encode(sys_encoding)

def _get_env_var_python2(var_name, default=None):
    if var_name not in os.environ:
        return default
    return os.environ[var_name].decode(sys_encoding)

def _set_env_var_python3(var_name, value):
    # value must be unicode
    assert type(value) is str
    os.environ[var_name] = value

def _get_env_var_python3(var_name, default=None):
    return os.environ.get(var_name, default)

if USING_PYTHON2:
    get_env_var = _get_env_var_python2
    set_env_var = _set_env_var_python2
else:
    get_env_var = _get_env_var_python3
    set_env_var = _set_env_var_python3
