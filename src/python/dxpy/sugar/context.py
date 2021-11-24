# Copyright (C) 2013-2021 DNAnexus, Inc.
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
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Optional

import dxpy
from dxpy.sugar import get_log, requires_worker_context


LOG = get_log(__name__)


class UserContext:
    """
    Context manager for switching to a user context when inside of a job context. All functions of
    this class require for the context to be a worker context and can only be run on a DNAnexus job.

    Args:
        api_token: DNAnexus user token

    Yields:
        Temporarily logs into a user context with the following usage:
        with UserContext(api_token):
            do_something

        Upon exit, job context is restored.
    """

    @requires_worker_context
    def __init__(self, api_token: str):
        api_token = api_token.strip("\n")  # Python adds \n when reading from a file
        self.user_secure_token = {"auth_token": api_token, "auth_token_type": "Bearer"}
        self.job_id = os.environ["DX_JOB_ID"]
        self.job_security_context = json.loads(os.environ["DX_SECURITY_CONTEXT"])
        self.job_workspace_id = os.environ["DX_WORKSPACE_ID"]

    @requires_worker_context
    def __enter__(self):
        proj = os.environ["DX_PROJECT_CONTEXT_ID"]
        dxpy.set_job_id(None)
        dxpy.set_security_context(self.user_secure_token)
        dxpy.set_workspace_id(proj)
        try:
            dx_config_file = Path(os.environ["HOME"]) / ".dnanexus_config"
            dx_config_file.unlink()
        except OSError:
            LOG.info("As expected, .dnanexus_config not present.")
        else:
            LOG.error("Could not remove .dnanexus_config file.")
        return self

    @requires_worker_context
    def __exit__(self, type, value, traceback):
        LOG.info("Restoring original Job context")
        dxpy.set_job_id(self.job_id)
        dxpy.set_security_context(self.job_security_context)
        dxpy.set_workspace_id(self.job_workspace_id)


@contextmanager
def set_env(environ: dict, override: bool = False):
    """
    Context manager generator to temporarily set the subprocess environment variables.

    Args:
        environ: Environment variable(s) to set
        override: Whether the environment should be updated or overwritten. If the environment is
            overridden, no env variables are set except for those explicitly specified.

    Yields:
        An environment with environment variables set as specified.
        On exit, the environment will return to previous configuration.

    Examples:
        Usage 1: Set environment variable
        # inside environment
        >>> with set_env(PLUGINS_DIR=u'test/plugins'):
        ...    "PLUGINS_DIR" in os.environ
        True

        # outside environment
        >>> "PLUGINS_DIR" in os.environ
        False

        Usage 2: Unset environment variable
        >>> with set_env(PYTHONPATH=''):
        ...    print(os.environ["PYTHONPATH"])
        <BLANKLINE>

        Usage 3: Manipulate multiple variables
        >>> myenv = {"PLUGINS_DIR": u'test/plugins', "PYTHONPATH": u'some/python/path'}
        >>> with set_env(**myenv):
        ...   print(os.environ["PLUGINS_DIR"])
        ...   print(os.environ["PYTHONPATH"])
        test/plugins
        some/python/path
    """
    old_environ = dict(os.environ)
    if override:
        os.environ = environ
    else:
        os.environ.update(environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


class cd:
    """
    Context manager for changing the current working directory.

    Args:
        target_path: Optional, specify path to cd to.
        cleanup: Optional, specify if directory should be deleted after exiting context. Default is
            true if the directory is newly created. Existing directories are never deleted.

    Note:
        If no args specified, cd() will create an arbitary temp dir and cd to it.

    Yields:
        Upon entry, context will be set to the specified directory.
        Upon exit, directory newly created with cleanup=True or directory created when
        no args are specified is deleted.

    Source: http://stackoverflow.com/questions/431684/how-do-i-cd-in-python

    Examples:
       with cd():
           do_the_thing
           # this will create a temp directory with a randomly
           # generated name, doe the thing, then delete the temp dir

       with cd(my_file_dir):
           do_the_thing
           # this will do the thing in my_file_dir and not delete the directory

       with cd(target_path=my_temp_dir, cleanup=True):
           do_the_thing
           # this will create a temp dir with path my_temp_dir, do the thing,
           # then delete the temp dir
    """

    def __init__(self, target_path: Optional[Path] = None, cleanup: bool = True):
        if target_path is not None and target_path.exists():
            self.new_path = target_path
            self.remove_folder = False
        else:
            self.new_path = Path(tempfile.mkdtemp(dir=target_path))
            self.remove_folder = cleanup

    def __enter__(self):
        self.saved_path = Path.cwd()
        os.chdir(self.new_path)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.saved_path)
        if self.remove_folder:
            try:
                shutil.rmtree(self.new_path)
            except:
                LOG.error("error deleting directory %s", self.new_path, exc_info=True)


@contextmanager
def fifo(path: Optional[Path] = None):
    """
    Creates a FIFO, yield it, and deletes it before exiting.

    Args:
        path: The path of the FIFO, or `None` to use a temp name.

    Yields:
        The path of the FIFO.
    """
    if path is None:
        path = Path(tempfile.mkstemp()[1])

    os.mkfifo(path)

    try:
        yield path
    finally:
        if path.exists():
            path.unlink()


@contextmanager
def tmpfile(*args, **kwargs):
    """
    Creates a temporary file, yields it, and deletes it before returning.

    Yields:
        A path to a temporary file.

    Notes:
        This method is needed distinct from :class:`tempfile.TemporaryFile` in the case where
        python needs to write to the file and then a subprocess needs to read from the file. For
        now, keep this private to transfers module rather than expose it via the context module.
    """
    path = Path(tempfile.mkstemp(*args, **kwargs)[1])
    try:
        yield path
    finally:
        if path.exists():
            path.unlink()


@contextmanager
def tmpdir(
    change_dir: bool = False,
    tmproot: Optional[Path] = None,
    cleanup: Optional[bool] = True,
) -> Path:
    """
    Context manager that creates a temporary directory, yields it, and then
    deletes it after return from the yield.
    Args:
        change_dir: Whether to temporarily change to the temp dir.
        tmproot: Root directory in which to create temporary directories.
        cleanup: Whether to delete the temporary directory before exiting the context.
    """
    temp = Path(tempfile.mkdtemp(dir=tmproot))
    try:
        if change_dir:
            with cd(temp):
                yield temp
        else:
            yield temp
    finally:
        if cleanup:
            shutil.rmtree(temp)
