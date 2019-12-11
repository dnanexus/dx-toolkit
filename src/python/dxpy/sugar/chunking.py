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
from __future__ import print_function, unicode_literals, division, absolute_import
import math

import dxpy
import dxpy.api


BYTES_PER_GB = 1024 * 1024 * 1024


def divide_files_into_chunks(file_descriptors, target_size_gb):
    """
    This is a general function to divide input files into a set of chunks based on
    file size.

    Args:
        file_descriptors (list): DXFile objects, dxlinks, or file IDs to split
        target_size_gb: Target size (in gigabytes) of each bin

    Returns:
        List of files with each group having roughly target_size of data
    """
    if not file_descriptors:
        return []
    if target_size_gb <= 0:
        raise ValueError("Target size must be > 0")

    filesizes = get_filesizes(file_descriptors)
    total_size_bytes = sum(filesizes)
    num_bins = max(
        1, int(math.ceil(total_size_bytes / (target_size_gb * BYTES_PER_GB)))
    )

    # It's conceivable that some of the splits could be empty.  We'll remove
    # those from our list.
    return list(filter(None, _schedule_lpt(list(zip(file_descriptors, filesizes)), num_bins)))


def get_filesizes(file_descriptors):
    """Run dx describe on a list of files (DXFile objects, links, or IDs) to get the
    corresponding file sizes.

    Args:
        file_descriptors (list): DXFile objects, dxlinks, or file IDs

    Returns:
        list: corresponding filesizes in bytes, output of 'dx describe'
        command
    """
    ids = [as_dxfile_id(f) for f in file_descriptors]
    descriptions = dxpy.api.system_describe_data_objects(
        {"objects": ids}, always_retry=True
    )
    sizes = dict(
        (d["describe"]["id"], d["describe"]["size"])
        for d in descriptions["results"]
    )
    return [sizes[i] for i in ids]


def as_dxfile_id(file_descriptor):
    if isinstance(file_descriptor, dxpy.DXFile):
        return file_descriptor.get_id()
    elif dxpy.is_dxlink(file_descriptor):
        return dxpy.get_dxlink_ids(file_descriptor)
    else:
        return file_descriptor


def _schedule_lpt(jobs, num_bins):
    """This function implements the Longest Processing Time algorithm to get
    a good division of labor for the multiprocessor scheduling problem.

    Args:
        jobs (dict or list): A dictionary with string 'key' specifying job
            name and float 'value' specifying job weight (how long job should
            run compared to other jobs). A list of tuples may also be passed
            if it is equivalent to dict.items()
        num_bins (int): Number of groups to split jobs into.

    Returns:
        List of lists: Each group (list) in list is a group of jobs that should
            be run together on a single worker or instance and consists of
            tuples as provided in jobs input.

    Examples:
        # it's assumed that there's an app-specific way to generate a filelist
        # of filenames and sizes
        fl = filenames_and_sizes(files)
        fl_groups = schedule_lpt(fl, num_jobs)
        for group in fl_groups:
            print(group)
            job = dxpy.new_dxjob({'files': group}, 'subjob_name')
            output['output_files'].append(job.get_output_ref('output_files'))
    """
    # We expect a list of tuples, with the first value the name of the
    # job and the second value the weight.  If we are given a dict
    # then convert keys to job names and values to weights.
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
