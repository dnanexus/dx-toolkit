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

import json, pprint, os
import dxpy

'''This module provides support for file download and upload. It calculates the location of the input and output directories. It also has a utility for parsing the job input file ('job_input.json'). 

We use the following shorthands
   <idir> == input directory     $home/in
   <odir> == output directory    $home/out

A simple example of the job input

{
    "seq2": {
        "$dnanexus_link": {
            "project": "project-BKJfY1j0b06Z4y8PX8bQ094f", 
            "id": "file-BKQGkjj0b06xG5560GGQ001K"
        }
    }, 
    "seq1": {
        "$dnanexus_link": {
            "project": "project-BKJfY1j0b06Z4y8PX8bQ094f", 
            "id": "file-BKQGkgQ0b06xG5560GGQ001B"
        }
    }
    "blast_args": "", 
    "evalue": 0.01
}

The first two elements are files {seq1, seq2}, the other elements
{blast_args, evalue}.  The file for seq2 should be saved into:
<idir>/seq2/filename

source command line
  -iseq1=NC_000868.fasta -iseq2=NC_001422.fasta 

file seq1 is supposed to appear in the execution environment at path:
<idir>/seq1/NC_000868.fasta

File Arrays

{
    "reads": [{
        "$dnanexus_link": {
            "project": "project-BKJfY1j0b06Z4y8PX8bQ094f", 
            "id": "file-BKQGkjj0b06xG5560GGQ001K"
        }
    }, 
    {
        "$dnanexus_link": {
            "project": "project-BKJfY1j0b06Z4y8PX8bQ094f", 
            "id": "file-BKQGkgQ0b06xG5560GGQ001B"
        }
    }]
}

This file array with two files, will appear in the virtual machine as:
<idir>/reads/A.txt
             B.txt

'''

    
def get_input_dir():
    ''' returns the input directory, where all inputs are downloaded '''
    home_dir = os.environ.get('HOME')
    idir = os.path.join(home_dir, 'in')
    return idir

def get_output_dir():
    ''' 
    returns the output directory, where all ouptus are created, and 
    uploaded from
    '''
    home_dir = os.environ.get('HOME')
    odir = os.path.join(home_dir, 'out')
    return odir

def get_input_json_file():
    ''' input JSON file '''
    home_dir = os.environ.get('HOME')
    return os.path.join(home_dir, "job_input.json");

def get_output_json_file():
    ''' output JSON file '''
    home_dir = os.environ.get('HOME')
    return os.path.join(home_dir, "job_ouput.json");

def ensure_dir(d):
    '''
    create a directory if it does not already exist .
    '''
    if not os.path.exists(d):
        # path does not exist, create the directory
        os.mkdir(d)
    else:
        # The path exists, check that it is not a file
        if os.path.isfile(d):
            raise Exception("Path %s already exists, and it is a file, not a directory" % d)

'''
key --- target file name
value --- file descriptor

example:
key == "seq1"
desc == { "$dnanexus_link": {
            "project": "project-BKJfY1j0b06Z4y8PX8bQ094f", 
            "id": "file-BKQGkgQ0b06xG5560GGQ001B"
        }
'''
def parse_job_input(idir):
    '''
    extract list of files, returns a set of directories to create, and 
    a set of files, with sources and destinations. 

    :param idir: input directory
    :param job_input_file: a json file that provides the input format
    '''
    job_input_file = calc_input_json();
    with open(job_input_file) as fh:
        job_input = json.load(fh)
        files = []
        dirs = set()  ## directories to create under <idir>
        
        ## local function for adding a file to the list of files to be created
        ## for example: 
        ##   "seq1" <$dnanexus_link ... >    ---> <idir>/seq1/<filename>
        def add_file(iname, value):
            handler = dxpy.get_handler(value)
            if not isinstance(handler, dxpy.DXFile):
                return
            filename = handler.name
            kv = {'trg_fname' : os.path.join(idir, iname, filename),
                  'trg_dir' : os.path.join(idir, iname),
                  'src_file_id' : handler.id,
                  'iname' : iname}
            files.append(kv)
            dirs.add(iname)

        for input_name, value in job_input.iteritems():
            if dxpy.is_dxlink(value):
                ## This is a single file
                add_file(input_name, value)
            elif isinstance(value, list):
               ## This is a file array, we use the field name as the directory
               for link in value:
                   add_file(input_name, link)

        return (dirs, files)


