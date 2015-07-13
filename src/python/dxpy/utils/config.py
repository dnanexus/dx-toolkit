# Copyright (C) 2013-2015 DNAnexus, Inc.
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
Logic for managing environment variable values and corresponding dxpy
configuration values.  See external documentation
https://wiki.dnanexus.com/Command-Line-Client/Environment%20Variables
for more details.
"""

from __future__ import print_function, unicode_literals, division, absolute_import

import os, sys, json, time
import platform
from collections import MutableMapping
from shutil import rmtree

import dxpy
from . import warn
from .. import DEFAULT_APISERVER_PROTOCOL, DEFAULT_APISERVER_HOST, DEFAULT_APISERVER_PORT
from ..compat import environ, expanduser, open, USING_PYTHON2, sys_encoding
from ..exceptions import format_exception
from .printing import fill

def _remove_ignore_errors(filename):
    try:
        os.remove(filename)
    except Exception:
        pass

def _open_for_writing_with_permissions(filename, perms=0o600):
    _remove_ignore_errors(filename)
    return os.fdopen(os.open(filename, os.O_CREAT | os.O_WRONLY, perms), "w")

class DXConfig(MutableMapping):
    """This class provides the dxpy configuration manager, available as
    ``dxpy.config``. When first accessed, the config manager looks up
    environment variables used to configure dxpy, and if they are not
    found, then attempts to resolve them by looking in the file
    ~/.dnanexus_config/environment, followed by the installed defaults
    in /etc/dnanexus.

    To assign to and access variables managed by the configuration
    manager, use ``dxpy.config["VAR_NAME"]``. When assigning a
    variable, the manager sets it as a process environment variable
    and updates any dxpy variable bound to it
    (e.g. dxpy.SECURITY_CONTEXT when setting
    DX_SECURITY_CONTEXT). When accessing a variable, it is looked up
    from the process environment.

    To save (serialize) the value of a variable to a persistent dxpy
    configuration file, use ``dxpy.config.write(key, value)`` or
    ``dxpy.config.save()`` after assigning variable values. To clear
    the values in persistent dxpy configuration, use
    ``dxpy.config.clear()``.
    """
    CORE_VAR_NAMES = {"DX_APISERVER_HOST", "DX_APISERVER_PORT",
                      "DX_APISERVER_PROTOCOL", "DX_PROJECT_CONTEXT_ID",
                      "DX_WORKSPACE_ID", "DX_SECURITY_CONTEXT", "DX_JOB_ID"}
    STANDALONE_VAR_NAMES = {"DX_CLI_WD", "DX_USERNAME",
                            "DX_PROJECT_CONTEXT_NAME"}
    """
    List of variable names supported by the configuration manager.
    """
    VAR_NAMES = set.union(CORE_VAR_NAMES, STANDALONE_VAR_NAMES)
    defaults = {
        "DX_SECURITY_CONTEXT": json.dumps({"auth_token": "PUBLIC", "auth_token_type": "Bearer"}),
        "DX_APISERVER_PROTOCOL": DEFAULT_APISERVER_PROTOCOL,
        "DX_APISERVER_HOST": DEFAULT_APISERVER_HOST,
        "DX_APISERVER_PORT": DEFAULT_APISERVER_PORT,
        "DX_CLI_WD": "/"
    }
    _global_conf_dir = "/etc/dnanexus"

    def __init__(self, suppress_warning=False):
        """
        :param suppress_warning:
            Whether to suppress the warning message for any mismatch found in the environment variables and the dx
            configuration file
        :type suppress_warning: boolean
        """
        try:
            dxpy._DEBUG = int(environ.get("_DX_DEBUG", 0))
        except ValueError as e:
            warn("WARNING: Expected _DX_DEBUG to be an integer, but got", environ["_DX_DEBUG"])
            dxpy._DEBUG = 0

        self._user_conf_dir = expanduser(environ.get("DX_USER_CONF_DIR", "~/.dnanexus_config"))

        dxpy._UPGRADE_NOTIFY = os.path.join(self._user_conf_dir, ".upgrade_notify")
        # If last upgrade notification was less than 24 hours ago, disable it
        if os.path.exists(dxpy._UPGRADE_NOTIFY) and os.path.getmtime(dxpy._UPGRADE_NOTIFY) > time.time() - 86400:
            dxpy._UPGRADE_NOTIFY = False

        env_vars = self._read_conf_dir(self.get_global_conf_dir())
        env_vars.update(self._read_conf_dir(self.get_user_conf_dir()))
        env_vars.update(self._read_conf_dir(self.get_session_conf_dir(cleanup=True)))
        env_overrides = []
        for var in self.VAR_NAMES:
            if var in environ:
                if var in env_vars and env_vars.get(var) != environ[var]:
                    env_overrides.append(var)
                env_vars[var] = environ[var]

        for var in env_vars:
            if env_vars[var] is not None:
                environ[var] = env_vars[var]

        if sys.stdout.isatty():
            if not suppress_warning and len(env_overrides) > 0:
                msg = "WARNING: The following environment variables were found to be different than the values " + \
                      "last stored by dx: "
                warn(fill(msg + ", ".join(env_overrides), width=80))
                msg = "To use the values stored by dx, unset the environment variables in your shell by running " + \
                      '"source ~/.dnanexus_config/unsetenv".  To clear the dx-stored values, run "dx clearenv".'
                warn(fill(msg, width=80))

        self._sync_dxpy_state()

    def _sync_dxpy_state(self):
        dxpy.set_api_server_info(host=environ.get("DX_APISERVER_HOST", None),
                                 port=environ.get("DX_APISERVER_PORT", None),
                                 protocol=environ.get("DX_APISERVER_PROTOCOL", None))

        if "DX_SECURITY_CONTEXT" in environ:
            dxpy.set_security_context(json.loads(environ["DX_SECURITY_CONTEXT"]))

        if "DX_JOB_ID" in environ:
            dxpy.set_job_id(environ["DX_JOB_ID"])
            dxpy.set_workspace_id(environ.get("DX_WORKSPACE_ID"))
        else:
            dxpy.set_job_id(None)
            dxpy.set_workspace_id(environ.get("DX_PROJECT_CONTEXT_ID"))

        dxpy.set_project_context(environ.get("DX_PROJECT_CONTEXT_ID"))

    def get_global_conf_dir(self):
        return self._global_conf_dir

    def get_user_conf_dir(self):
        return self._user_conf_dir

    def get_session_conf_dir(self, cleanup=False):
        """
        Tries to find the session configuration directory by looking in ~/.dnanexus_config/sessions/<PID>,
        where <PID> is pid of the parent of this process, then its parent, and so on.
        If none of those exist, the path for the immediate parent is given, even if it doesn't exist.

        If *cleanup* is True, looks up and deletes all session configuration directories that belong to nonexistent
        processes.
        """
        sessions_dir = os.path.join(self._user_conf_dir, "sessions")
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
                    try:
                        session_pid = int(session_dir)
                    except ValueError:
                        # If dir name doesn't look like an int, leave it
                        # alone
                        continue
                    if not pid_exists(session_pid):
                        rmtree(os.path.join(sessions_dir, session_dir), ignore_errors=True)

            parent_process = Process(os.getpid()).parent()
            default_session_dir = os.path.join(sessions_dir, str(parent_process.pid))
            while parent_process is not None and parent_process.pid != 0:
                session_dir = os.path.join(sessions_dir, str(parent_process.pid))
                if os.path.exists(session_dir):
                    return session_dir
                parent_process = parent_process.parent()
            return default_session_dir
        except (ImportError, IOError, AttributeError) as e:
            # We don't bundle psutil with Windows, so failure to import
            # psutil would be expected.
            if platform.system() != 'Windows':
                warn(fill("Error while retrieving session configuration: " + format_exception(e)))
        except Exception as e:
            warn(fill("Unexpected error while retrieving session configuration: " + format_exception(e)))
        return self._get_ppid_session_conf_dir(sessions_dir)

    def _get_ppid_session_conf_dir(self, sessions_dir):
        try:
            return os.path.join(sessions_dir, str(os.getppid()))
        except AttributeError:
            pass # os.getppid is not available on Windows
        except Exception as e:
            warn(fill("Unexpected error while retrieving session configuration: " + format_exception(e)))
        return os.path.join(sessions_dir, str(os.getpid()))

    def _read_conf_dir(self, dirname):
        try:
            with open(os.path.join(dirname, "environment.json")) as fd:
                env_vars = json.load(fd)
        except Exception:
            env_vars = {}

        for standalone_var in self.STANDALONE_VAR_NAMES:
            try:
                with open(os.path.join(dirname, standalone_var)) as fd:
                    env_vars[standalone_var] = fd.read()
            except Exception:
                pass
        return env_vars

    def __getitem__(self, item):
        if item not in self.VAR_NAMES:
            raise KeyError(item)
        return environ[item]

    def __setitem__(self, key, value):
        if key not in self.VAR_NAMES:
            raise KeyError(key)
        if value is None:
            value = self.defaults.get(key, "")
        environ[key] = value
        if key in self.CORE_VAR_NAMES:
            self._sync_dxpy_state()

    def __delitem__(self, key):
        if key not in self.VAR_NAMES:
            raise KeyError(key)
        del environ[key]
        if key in self.CORE_VAR_NAMES:
            self._sync_dxpy_state()

    def __iter__(self):
        for item in self.VAR_NAMES:
            if item in environ:
                yield item

    def __len__(self):
        return len([var for var in self.VAR_NAMES if var in environ])

    def __repr__(self):
        desc = "<{module}.{classname} object at 0x{mem_loc:x}: {data}>"
        return desc.format(module=self.__module__,
                           classname=self.__class__.__name__,
                           mem_loc=id(self),
                           data=dict(self))

    def write(self, item, value):
        self[item] = value
        self.save()

    def save(self):
        self._write_conf_dir(self._user_conf_dir)
        self._write_conf_dir(self.get_session_conf_dir())
        self._write_unsetenv(self._user_conf_dir)

    def _write_unsetenv(self, conf_dir):
        if not os.path.exists(os.path.join(conf_dir, "unsetenv")):
            with open(os.path.join(conf_dir, "unsetenv"), "w") as fd:
                fd.writelines("unset {}\n".format(var) for var in self.CORE_VAR_NAMES)

    def _write_conf_dir(self, conf_dir):
        try:
            os.makedirs(conf_dir, 0o700)
        except OSError:
            try:
                os.chmod(conf_dir, 0o700)
            except OSError as e:
                warn(fill("Error while writing configuration data: " + format_exception(e)))
                return

        env_jsonfile_path = os.path.join(conf_dir, "environment.json")
        # Make sure the file has 600 permissions
        with _open_for_writing_with_permissions(env_jsonfile_path, 0o600) as fd:
            json.dump({k: self.get(k, self.defaults.get(k)) for k in self}, fd, indent=4)
            fd.write("\n")

        for var in self.STANDALONE_VAR_NAMES:
            # Make sure the file has 600 permissions
            with _open_for_writing_with_permissions(os.path.join(conf_dir, var), 0o600) as fd:
                value = self.get(var, self.defaults.get(var, ""))
                fd.write(value.encode(sys_encoding) if USING_PYTHON2 else value)

    def clear(self, reset=False):
        rmtree(self.get_session_conf_dir(), ignore_errors=True)
        _remove_ignore_errors(os.path.join(self._user_conf_dir, "environment"))
        _remove_ignore_errors(os.path.join(self._user_conf_dir, "environment.json"))
        for f in self.STANDALONE_VAR_NAMES:
            _remove_ignore_errors(os.path.join(self._user_conf_dir, f))

        if reset:
            for var in self.VAR_NAMES:
                self[var] = self.defaults.get(var, "")
            self.save()
