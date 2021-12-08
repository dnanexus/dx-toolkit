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
import math
from typing import Iterable, List, Union

import dxpy
import dxpy.api


BYTES_PER_GB = 1024 * 1024 * 1024


def divide_dxfiles_into_chunks(
    dxfiles: Iterable, target_size_gb: float, file_size_key=None
) -> List[list]:
    """
    This is a general function to divide input files into a set of chunks based on file size.

    Args:
        dxfiles: List of objects to split; objects are assumed to be DNAnexus file objects,
            links, or ids unless `file_size_key` is specified.
        target_size_gb: Target size (in gigabytes) of each bin.
        file_size_key: Callable that returns the file size for the given input object.

    Returns:
        Groups of files with each group having roughly target_size of data.
    """
    if not dxfiles:
        return []
    if target_size_gb <= 0:
        raise ValueError("Target size must be > 0")

    if file_size_key:
        file_sizes = [file_size_key(obj) for obj in dxfiles]
    else:
        file_sizes = [
            d["size"] for d in dxpy.describe([dxpy.dxlink(f) for f in dxfiles])
        ]

    num_bins = max(1, int(math.ceil(sum(file_sizes) / (target_size_gb * BYTES_PER_GB))))

    # It's conceivable that some of the splits could be empty.  We'll remove those from our list.
    return list(filter(None, _schedule_lpt(list(zip(dxfiles, file_sizes)), num_bins)))


def _schedule_lpt(jobs: Union[dict, list], num_bins: int) -> List[list]:
    """This function implements the Longest Processing Time algorithm to get a good division of
    labor for the multiprocessor scheduling problem.

    Args:
        jobs: A dictionary with string 'key' specifying job name and float 'value'
            specifying job weight (how long job should run compared to other jobs). A list of
            tuples may also be passed if it is equivalent to dict.items()
        num_bins: Number of groups to split jobs into.

    Returns:
        List of lists: Each group (list) in list is a group of jobs that should be run together on
        a single worker or instance and consists of tuples as provided in jobs input.

    Examples:
        # it's assumed that there's an app-specific way to generate a filelist
        # of filenames and sizes
        fl = filenames_and_sizes(files)
        fl_groups = schedule_lpt(fl, num_jobs)
        for group in fl_groups:
            print group
            job = dxpy.new_dxjob({'files': group}, 'subjob_name')
            output['output_files'].append(job.get_output_ref('output_files'))
    """
    # We expect a list of tuples, with the first value the name of the job and the second value the
    # weight. If we are given a dict then convert keys to job names and values to weights.
    num_bins = min(num_bins, len(jobs))
    indexes = range(num_bins)
    groups = [[] for _ in indexes]
    sizes = [0] * num_bins

    if isinstance(jobs, dict):
        jobs = jobs.items()

    for job in sorted(jobs, key=lambda j: j[1], reverse=True):
        idx = min(indexes, key=sizes.__getitem__)
        groups[idx].append(job[0])
        sizes[idx] += job[1]

    return groups
