# Copyright (C) 2013-2014 DNAnexus, Inc.
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

'''
This module provides support for file download. It works with the input specification 
provided in 'job_input.json', this helps automate mundate tasks for the user.
'''

'''
A simple example of the input specification:

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

The first two elements are files {seq1, seq2}, the other elements {blast_args, evalue}.
The file for seq2 should be saved into: <idir>/seq2/filename

source command line
  -iseq1=NC_000868.fasta -iseq2=NC_001422.fasta 

file seq1 is supposed to appear in the virutal machine at path:
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
<idir>/reads/<handler.name>
<idir>/reads/<handler.name>
'''

    
## input directory, where all inputs are downloaded
def calc_input_dir():
    home_dir = os.environ.get('HOME')
    idir = os.path.join(home_dir, 'in');
    return idir

def calc_output_dir():
    home_dir = os.environ.get('HOME')
    idir = os.path.join(home_dir, 'out');
    return idir

## input JSON file
def calc_input_json():
    home_dir = os.environ.get('HOME')
    return os.path.join(home_dir, "job_input.json");

## output JSON file
def calc_output_json():
    home_dir = os.environ.get('HOME')
    return os.path.join(home_dir, "job_ouput.json");

'''
 create a directory if it does not already exist .

 TODO: report appropriate errors if this is a file, instead of a directory
'''
def ensure_dir(d):
    ##pp = pprint.PrettyPrinter(indent=4)
    ##pp.pprint("ensure_dir " + d)
    if not os.path.exists(d):
        ##print ("create_dir " + d)
        os.mkdir(d)

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
        #pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(job_input)

        files = list() 
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
                  'src_fname' : handler.id,
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

'''
Example of an output spec file
'''
def parse_job_output(idir):
    



