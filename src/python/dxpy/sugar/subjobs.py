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

def divide_dxfiles_into_chunks(dxfiles, target_size):
    '''
    This is a general function to divide input files into a set of chunks based on file size.

    Args:
        dxfiles: List of dx files to split
        target_size: Target size (in bytes) of each bin

    Returns:
        Groups of files with each group having roughly target_size of data
    '''

    filesizes = get_dxlink_filesizes(dxfiles)
    total_size = sum(filesizes)

    # Now, get the splits.  We'll target each set of bam files to be a total
    # of SIZE_PER_BIN bytes.
    num_bins = total_size / (target_size * 1024 * 1024 * 1024) + 1
    groups = schedule_lpt(zip(dxfiles, filesizes), num_bins)

    # It's conceivable that some of the splits could be empty.  We'll remove
    # those from our list.
    groups = [split for split in groups if len(split) > 0]

    return groups

def get_dxlink_filesizes(dx_links):
    """Run dx describe on a list of DNAnexus dxlink inputs to get the
    corresponding file sizes.

    Args:
        dx_links (list of dicts): dxlink dicts containing '$dnanexus_link' as
            key and file-id as value

    Returns:
        list: corresponding filesizes in bytes, output of 'dx describe'
        command
    """
    input = {'objects': [file['$dnanexus_link'] for file in dx_links]}
    descriptions = dxpy.api.system_describe_data_objects(input, always_retry=True)

    sizes = [d['describe']['size'] for d in descriptions['results']]

    return sizes


def schedule_lpt(jobs, num_bins):
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
            print group
            job = dxpy.new_dxjob({'files': group}, 'subjob_name')
            output['output_files'].append(job.get_output_ref('output_files'))
    """

    def _index_min(values):
        # Efficient index of min from stackoverflow.com/questions/2474015
        return min(xrange(len(values)), key=values.__getitem__)

    # We expect a list of tuples, with the first value the name of the
    # job and the second value the weight.  If we are given a dict
    # then convert keys to job names and values to weights.
    if(type(jobs) == dict):
        jobs = zip(jobs.keys(), jobs.values())

    num_bins = min(num_bins, len(jobs))
    jobs.sort(key=lambda j: j[1], reverse=True)
    partition = {'groups': [[] for i in xrange(num_bins)],
                 'size': [0 for i in xrange(num_bins)]}

    for job in jobs:
        idx = _index_min(partition['size'])
        partition['groups'][idx] += [job[0]]
        partition['size'][idx] += job[1]

    return partition['groups']