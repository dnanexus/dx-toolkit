# Copyright (C) 2014 DNAnexus, Inc.
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

'''
This module provides support for file download and upload. It calculates the
   location of the input and output directories. It also has a utility for parsing
   the job input file ('job_input.json').

We use the following shorthands
   <idir> == input directory     $HOME/in
   <odir> == output directory    $HOME/out

A simple example of the job input, when run locally, is:

{
    "seq2": {
        "$dnanexus_link": {
            "project": "project-1111",
            "id": "file-1111"
        }
    },
    "seq1": {
        "$dnanexus_link": {
            "project": "project-2222",
            "id": "file-2222"
        }
    }
    "blast_args": "",
    "evalue": 0.01
}

The first two elements are files {seq1, seq2}, the other elements are
{blast_args, evalue}. The files for seq1,seq2 should be saved into:
<idir>/seq1/<filename>
<idir>/seq2/<filename>

An example for a shell command that would create these arguments is:
    $ dx run coolapp -iseq1=NC_000868.fasta -iseq2=NC_001422.fasta
It would run an app named "coolapp", with file arguments for seq1 and seq2. Both NC_*
files should be the names of files in a DNAnexus project (and should be resolved to their
file IDs by dx). Subsequently, after dx-download-all-inputs is run,
file seq1 should appear in the execution environment at path:
    <idir>/seq1/NC_000868.fasta

File Arrays

{
    "reads": [{
        "$dnanexus_link": {
            "project": "project-3333",
            "id": "file-3333"
        }
    },
    {
        "$dnanexus_link": {
            "project": "project-4444",
            "id": "file-4444"
        }
    }]
}

This is a file array with two files. Running a command like this:
    $ dx run coolapp -ireads=A.fastq -ireads=B.fasta
will download into the execution environment:
<idir>/reads/A.fastq
             B.fastq

'''


import json, os
import dxpy
from ..exceptions import DXError

def get_input_dir():
    '''
    :rtype : string
    :returns : path to input directory

    Returns the input directory, where all inputs are downloaded
    '''
    home_dir = os.environ.get('HOME')
    idir = os.path.join(home_dir, 'in')
    return idir

def get_output_dir():
    '''
    :rtype : string
    :returns : path to output directory

    Returns the output directory, where all outputs are created, and
    uploaded from
    '''
    home_dir = os.environ.get('HOME')
    odir = os.path.join(home_dir, 'out')
    return odir

def get_input_json_file():
    """
    :rtype : string
    :returns: path to input JSON file
    """
    home_dir = os.environ.get('HOME')
    return os.path.join(home_dir, "job_input.json")

def get_output_json_file():
    """
    :rtype : string
    :returns : Path to output JSON file
    """
    home_dir = os.environ.get('HOME')
    return os.path.join(home_dir, "job_output.json")

def ensure_dir(path):
    """
    :param path: path to directory to be created

    Create a directory if it does not already exist.
    """
    if not os.path.exists(path):
        # path does not exist, create the directory
        os.mkdir(path)
    else:
        # The path exists, check that it is not a file
        if os.path.isfile(path):
            raise Exception("Path %s already exists, and it is a file, not a directory" % path)

def make_unix_filename(fname):
    """
    :param fname: the basename of a file (e.g., xxx in /zzz/yyy/xxx).
    :return: a valid unix filename
    :rtype: string
    :raises DXError: if the filename is invalid on a Unix system

    The problem being solved here is that *fname* is a python string, it
    may contain characters that are invalid for a file name. We replace all the slashes with %2F.
    Another issue, is that the user may choose an invalid name. Since we focus
    on Unix systems, the only possibilies are "." and "..".
    """
    # sanity check for filenames
    bad_filenames = [".", ".."]
    if fname in bad_filenames:
        raise DXError("Invalid filename {}".format(fname))
    return fname.replace('/', '%2F')

def get_job_input_filenames(idir):
    """
    :param idir: input directory

    Extract list of files, returns a set of directories to create, and
    a set of files, with sources and destinations. The paths created are
    absolute, they include *idir*

    Note: we analyze the file names, and make sure they are unique. An
    exception is thrown if they are not.
    """
    job_input_file = get_input_json_file()
    with open(job_input_file) as fh:
        job_input = json.load(fh)
        files = []
        trg_file_paths = set()   # all files to be downloaded
        dirs = set()  # directories to create under <idir>

        # Local function for adding a file to the list of files to be created
        # for example:
        #    iname == "seq1"
        #    value == { "$dnanexus_link": {
        #       "project": "project-BKJfY1j0b06Z4y8PX8bQ094f",
        #       "id": "file-BKQGkgQ0b06xG5560GGQ001B"
        #    }
        def add_file(iname, value):
            if not dxpy.is_dxlink(value):
                return
            handler = dxpy.get_handler(value)
            if not isinstance(handler, dxpy.DXFile):
                return
            filename = make_unix_filename(handler.name)
            trg_fname = os.path.join(idir, iname, filename)

            if trg_fname in trg_file_paths:
                raise DXError("Encountered multiple files which would have the same local filename {}".format(filename))
            files.append({'trg_fname': trg_fname,
                         'trg_dir': os.path.join(idir, iname),
                         'src_file_id': handler.id,
                         'iname': iname})
            dirs.add(iname)

        for input_name, value in job_input.iteritems():
            if isinstance(value, list):
                # This is a file array, we use the field name as the directory
                for link in value:
                    add_file(input_name, link)
            else:
                add_file(input_name, value)
        return dirs, files
