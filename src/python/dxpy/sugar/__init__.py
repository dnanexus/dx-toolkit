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
import dxpy
from functools import wraps
import logging
from pathlib import Path
import re
import sys

import psutil


MEMINFO_RE = re.compile(r"^MemAvailable:[\s]*([0-9]*) kB")
MEM_KiB_CONVERSIONS = {"K": 1, "M": 1 << 10, "G": 1 << 20}
"""Conversions between KiB and other units."""


def in_worker_context() -> bool:
    return dxpy.JOB_ID is not None


def requires_worker_context(func):
    """Decorator that checks a given function is running within a DNAnexus job context."""

    @wraps(func)
    def check_job_id(*args, **kwargs):
        if in_worker_context():
            return func(*args, **kwargs)
        else:
            raise dxpy.DXError(
                "Illegal function call, must be called from within DNAnexus job "
                "context."
            )

    return check_job_id


def get_log(name, level=logging.INFO):
    """
    Gets a logger with the given name and level. Uses a different handler depending on whether this
    function is called from within a job context.

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


@requires_worker_context
def available_memory(suffix="M", meminfo_path=Path("/proc/meminfo")):
    """Queries a worker's /proc/meminfo for available memory and returns a float of the specified
    suffix size.

    Note that this function doesn't necessarily require to be run on a DNAnexus worker, but depends
    on /proc/meminfo, which only exists on Linux systems.

    Args:
        suffix (str): One of 'M', 'K' or 'G' to return memory in Mib, KiB or GiB, respectively.
        meminfo_path: Path to /proc/meminfo, or a file that contains compatible output.

    Returns:
        float: total_memory read from meminfo in KiB, MiB, or GiB depending on specified suffix.

    Raises:
        ValueError if `suffix` is not a valid suffix.
        dxpy.AppInternalError is raised if `meminfo_path` cannot be read or is not of the expected
        format.
    """
    suffix = suffix.upper()
    if suffix not in MEM_KiB_CONVERSIONS:
        raise ValueError(
            f"Unknown memory suffix {suffix}. Please choose from "
            f"{','.join(MEM_KiB_CONVERSIONS.keys())}."
        )

    available_mem = psutil.virtual_memory().available
    return float(available_mem) / MEM_KiB_CONVERSIONS[suffix]
