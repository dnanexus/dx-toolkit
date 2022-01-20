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
import re
import sys
from typing import Callable, Optional

import psutil


MEMINFO_RE = re.compile(r"^MemAvailable:[\s]*([0-9]*) kB")
MEM_CONVERSIONS = {"K": 1 << 10, "M": 1 << 20, "G": 1 << 30}
"""Conversions between KiB and other units."""


def in_worker_context() -> bool:
    return dxpy.JOB_ID is not None


def requires_worker_context(func: Callable) -> Callable:
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


def get_log(name: str, level: int = logging.INFO) -> logging.Logger:
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
def available_memory(scale: Optional[str] = "M") -> float:
    """Queries a worker's /proc/meminfo for available memory and returns a float of the specified
    suffix size.

    Note that this function doesn't necessarily require to be run on a DNAnexus worker, but depends
    on /proc/meminfo, which only exists on Linux systems.

    Args:
        scale: One of 'K', 'M', or 'G' to return memory in KiB, Mib, or GiB, respectively. If
        `None`, then the size is returned in bytes.

    Returns:
        float: available memory in bytes, KiB, MiB, or GiB depending on specified scale.

    Raises:
        ValueError if `scale` is not a valid scale.
    """
    available_mem = float(psutil.virtual_memory().available)

    if scale is None:
        return available_mem
    else:
        scale = scale.upper()
        if scale not in MEM_CONVERSIONS:
            raise ValueError(
                f"Unknown memory suffix {scale}. Please choose from "
                f"{','.join(MEM_CONVERSIONS.keys())}."
            )

        return available_mem / MEM_CONVERSIONS[scale]
