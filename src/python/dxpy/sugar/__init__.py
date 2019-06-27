# Copyright (C) 2013-2019 DNAnexus, Inc.
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
import dxpy
from functools import wraps
import logging
import sys

from processing import run_cmd, chain_cmds
from transfers import (
    Uploader,
    Downloader,
    upload_file,
    tar_and_upload_files,
    download_file,
)

from chunking import (
    divide_dxfiles_into_chunks,
    get_dxlink_filesizes,
    schedule_lpt
)


def requires_worker_context(func):
    """Decorator that checks a given function is running within a DNAnexus job context.
    """
    @wraps(func)
    def check_job_id(*args, **kwargs):
        if dxpy.JOB_ID:
            return func(*args, **kwargs)

        raise dxpy.DXError(
            "Illegal function call, must be called from within DNAnexus job "
            "context."
        )

    return check_job_id


def get_log(name, level=logging.INFO):
    """Gets a logger with the given name and level. Uses a different handler
    depending on whether this function is called from within a job context.

    Args:
        name: Log name
        level: Log level

    Returns:
        Configured logger
    """
    log = logging.getLogger(name)
    log.propagate = False
    log.setLevel(level)
    if not log.handlers:
        # Use DXLogHandler if we're running within a job, otherwise log to stderr
        # if os.path.exists("/opt/dnanexus/log/priority"):
        if dxpy.JOB_ID:
            log.addHandler(dxpy.DXLogHandler())
        else:
            log.addHandler(logging.StreamHandler(sys.stderr))
    return log
