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
import re


MEM_RE = re.compile(r"^MemAvailable:[\s]*([0-9]*) kB")
MEM_KiB_CONVERSIONS = {
    "K": 1,
    "M": 1 << 10,
    "G": 1 << 20
}
"""Conversions between KiB and other units."""


def in_worker_context():
    return dxpy.JOB_ID is not None


def requires_worker_context(func):
    """This decorator checks that a given function is running within a DNAnexus job
    context.
    """
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


@requires_worker_context
def available_memory(suffix="M", meminfo_path="/proc/meminfo"):
    """Queries a worker's /proc/meminfo for available memory and returns a float
    of the specified suffix size.

    Note that this function doesn't necessarily require to be run on a DNAnexus worker,
    but depends on /proc/meminfo, which only exists on Linux systems.

    Args:
        suffix (str): One of 'M', 'K' or 'G' to return memory in Mib, KiB or
            GiB, respectively.
        meminfo_path: Path to /proc/meminfo, or a file that contains compatible
            output.

    Returns:
        float: total_memory read from meminfo in KiB, MiB, or GiB
            depending on specified suffix.

    Raises:
        ValueError if `suffix` is not a valid suffix.
        dxpy.DXError is raised if suffix is not recognized or system memory
            cannot be read.
    """
    suffix = suffix.upper()
    if suffix not in MEM_KiB_CONVERSIONS:
        raise ValueError(
            "Unknown memory suffix {0}. Please choose from K, M, or G.".format(suffix)
        )

    total_mem = MEM_RE.findall(open(meminfo_path).read())
    if len(total_mem) != 1:
        raise dxpy.DXError("Problem reading system memory from {}".format(meminfo_path))

    return float(total_mem[0]) / MEM_KiB_CONVERSIONS[suffix]
