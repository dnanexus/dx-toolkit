# Copyright (C) 2014-2015 DNAnexus, Inc.
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

from __future__ import print_function, unicode_literals, division, absolute_import

import json
import pipes
import os
import fnmatch
import sys
import collections
import dxpy
from dxpy.compat import environ, open
from ..exceptions import DXError


def get_input_dir(job_homedir=None):
    '''
    :param job_homedir: explicit value for home directory, used for testing purposes
    :rtype: string
    :returns: path to input directory

    Returns the input directory, where all inputs are downloaded
    '''
    if job_homedir is not None:
        home_dir = job_homedir
    else:
        home_dir = environ.get('HOME')
    idir = os.path.join(home_dir, 'in')
    return idir


def get_output_dir(job_homedir=None):
    '''
    :param job_homedir: explicit value for home directory, used for testing purposes
    :rtype: string
    :returns: path to output directory

    Returns the output directory, where all outputs are created, and
    uploaded from
    '''
    if job_homedir is not None:
        home_dir = job_homedir
    else:
        home_dir = environ.get('HOME')
    odir = os.path.join(home_dir, 'out')
    return odir


def get_input_json_file():
    """
    :rtype: string
    :returns: path to input JSON file
    """
    home_dir = environ.get('HOME')
    return os.path.join(home_dir, "job_input.json")


def get_output_json_file():
    """
    :rtype: string
    :returns: Path to output JSON file
    """
    home_dir = environ.get('HOME')
    return os.path.join(home_dir, "job_output.json")


def rm_output_json_file():
    """ Warning: this is not for casual use.
    It erases the output json file, and should be used for testing purposes only.
    """
    path = get_output_json_file()
    try:
        os.remove(path)
    except OSError as e:
        if e.errno == errno.ENOENT:
            pass
        else:
            raise


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
    :returns: a valid unix filename
    :rtype: string
    :raises: DXError if the filename is invalid on a Unix system

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


## filter from a dictionary a list of matching keys
def filter_dict(dict_, excl_keys):
    return {k: v for k, v in dict_.iteritems() if k not in excl_keys}


def get_job_input_filenames(job_input_file):
    """Extract list of files, returns a set of directories to create, and
    a set of files, with sources and destinations. The paths created are
    relative to the input directory.

    Note: we go through file names inside arrays, and create a
    separate subdirectory for each. This avoids clobbering files when
    duplicate filenames appear in an array.
    """
    def get_input_hash():
        with open(job_input_file) as fh:
            job_input = json.load(fh)
            return job_input
    job_input = get_input_hash()

    files = collections.defaultdict(list)  # dictionary, with empty lists as default elements
    dirs = []  # directories to create under <idir>

    # Local function for adding a file to the list of files to be created
    # for example:
    #    iname == "seq1"
    #    subdir == "015"
    #    value == { "$dnanexus_link": {
    #       "project": "project-BKJfY1j0b06Z4y8PX8bQ094f",
    #       "id": "file-BKQGkgQ0b06xG5560GGQ001B"
    #    }
    # will create a record describing that the file should
    # be downloaded into seq1/015/<filename>
    def add_file(iname, subdir, value):
        if not dxpy.is_dxlink(value):
            return
        handler = dxpy.get_handler(value)
        if not isinstance(handler, dxpy.DXFile):
            return
        filename = make_unix_filename(handler.name)
        trg_dir = iname
        if subdir is not None:
            trg_dir = os.path.join(trg_dir, subdir)
        files[iname].append({'trg_fname': os.path.join(trg_dir, filename),
                             'handler': handler,
                             'src_file_id': handler.id})
        dirs.append(trg_dir)

    # An array of inputs, for a single key. A directory
    # will be created per array entry. For example, if the input key is
    # FOO, and the inputs are {A, B, C}.vcf then, the directory structure
    # will be:
    #   <idir>/FOO/00/A.vcf
    #   <idir>/FOO/01/B.vcf
    #   <idir>/FOO/02/C.vcf
    def add_file_array(input_name, links):
        num_files = len(links)
        if num_files == 0:
            return
        num_digits = len(str(num_files - 1))
        dirs.append(input_name)
        for i, link in enumerate(links):
            subdir = str(i).zfill(num_digits)
            add_file(input_name, subdir, link)

    for input_name, value in job_input.iteritems():
        if isinstance(value, list):
            # This is a file array
            add_file_array(input_name, value)
        else:
            add_file(input_name, None, value)

    ## create a dictionary of the all non-file elements
    rest_hash = {key: val for key, val in job_input.iteritems() if key not in files}
    return dirs, files, rest_hash


def get_input_spec_patterns():
    ''' Extract the inputSpec patterns, if they exist -- modifed from dx-upload-all-outputs

    Returns a dict of all patterns, with keys equal to the respective
    input parameter names.
    '''
    input_spec = None
    if 'DX_JOB_ID' in environ:
        # works in the cloud, not locally
        job_desc = dxpy.describe(dxpy.JOB_ID)
        if job_desc["function"] == "main":
            # The input spec does not apply for subjobs
            desc = dxpy.describe(job_desc.get("app", job_desc.get("applet")))
            if "inputSpec" in desc:
                input_spec = desc["inputSpec"]
    elif 'DX_TEST_DXAPP_JSON' in environ:
        # works only locally
        path_to_dxapp_json = environ['DX_TEST_DXAPP_JSON']
        with open(path_to_dxapp_json) as fd:
            dxapp_json = json.load(fd)
            input_spec = dxapp_json.get('inputSpec')

    # convert to a dictionary. Each entry in the input spec
    # has {name, class} attributes.
    if input_spec is None:
        return {}

    # For each field name, return its patterns.
    # Make sure a pattern is legal, ignore illegal patterns.
    def is_legal_pattern(pattern):
        return "*" in pattern
    patterns_dict = {}
    for spec in input_spec:
        name = spec['name']
        if 'patterns' in spec:
            patterns_dict[name] = []
            for p in spec['patterns']:
                if is_legal_pattern(p):
                    patterns_dict[name].append(p)
    return patterns_dict


# return the shorter string between p and q
def choose_shorter_string(p, q):
    if p is None:
        return q
    if q is None:
        return p
    if len(q) < len(p):
        return q
    return p


def analyze_bash_vars(job_input_file, job_homedir):
    '''
    This function examines the input file, and calculates variables to
    instantiate in the shell environment. It is called right before starting the
    execution of an app in a worker.

    For each input key, we want to have
    $var
    $var_filename
    $var_prefix
       remove last dot (+gz), and/or remove patterns
    $var_path
       $HOME/in/var/$var_filename

    For example,
    $HOME/in/genes/A.txt
                   B.txt

    export genes=('{"$dnanexus_link": "file-xxxx"}' '{"$dnanexus_link": "file-yyyy"}')
    export genes_filename=("A.txt" "B.txt")
    export genes_prefix=("A" "B")
    export genes_path=("$HOME/in/genes/A.txt" "$HOME/in/genes/B.txt")

    If there are patterns defined in the input spec, then the prefix respects them.
    Here are several examples, where the patterns are:
       *.bam, *.bwa-index.tar.gz, foo*.sam, z*ra.sam

    file name                prefix     matches
    foo.zed.bam              foo.zed    *.bam
    xxx.bwa-index.tar.gz     xxx        *.bwa-index.tar.gz
    food.sam                 food       foo*.sam
    zebra.sam                zebra      z*ra.sam
    xx.c                     xx
    xx.c.gz                  xx

    The only patterns we recognize are of the form x*.y. For example:
      legal    *.sam, *.c.py,  foo*.sam,  a*b*c.baz
      ignored  uu.txt x???.tar  mon[a-z].py
    '''
    _, file_entries, rest_hash = get_job_input_filenames(job_input_file)
    patterns_dict = get_input_spec_patterns()

    # Note: there may be multiple matches, choose the shortest prefix.
    def get_prefix(basename, key):
        best_prefix = None
        patterns = patterns_dict.get(key)
        if patterns is not None:
            for pattern in patterns:
                if fnmatch.fnmatch(basename, pattern):
                    _, _, right_piece = pattern.rpartition("*")
                    best_prefix = choose_shorter_string(best_prefix, basename[:-len(right_piece)])
        if best_prefix is not None:
            return best_prefix
        else:
            # no matching rule
            parts = os.path.splitext(basename)
            if parts[1] == ".gz":
                parts = os.path.splitext(parts[0])
            return parts[0]

    def factory():
        return {'handler': [], 'basename': [],  'prefix': [], 'path': []}
    file_key_descs = collections.defaultdict(factory)
    rel_home_dir = get_input_dir(job_homedir)
    for key, entries in file_entries.iteritems():
        for entry in entries:
            filename = entry['trg_fname']
            basename = os.path.basename(filename)
            prefix = get_prefix(basename, key)
            k_desc = file_key_descs[key]
            k_desc['handler'].append(entry['handler'])
            k_desc['basename'].append(basename)
            k_desc['prefix'].append(prefix)
            k_desc['path'].append(os.path.join(rel_home_dir, filename))
    return file_key_descs, rest_hash


#
# Note: pipes.quote() to be replaced with shlex.quote() in Python 3
# (see http://docs.python.org/2/library/pipes.html#pipes.quote)
#
def gen_bash_vars(job_input_file, job_homedir=None, check_name_collision=True):
    """
    :param job_input_file: path to a JSON file describing the job inputs
    :param job_homedir: path to home directory, used for testing purposes
    :param check_name_collision: should we check for name collisions?
    :return: list of lines
    :rtype: list of strings

    Calculates a line for each shell variable to instantiate.
    If *check_name_collision* is true, then detect and warn about
    collisions with essential environment variables.
    """
    file_key_descs, rest_hash = analyze_bash_vars(job_input_file, job_homedir)

    def string_of_elem(elem):
        result = None
        if isinstance(elem, basestring):
            result = elem
        elif isinstance(elem, dxpy.DXFile):
            result = json.dumps(dxpy.dxlink(elem))
        else:
            result = json.dumps(elem)
        return pipes.quote(result)

    def string_of_value(val):
        if isinstance(val, list):
            string = " ".join([string_of_elem(vitem) for vitem in val])
            return "( {} )".format(string)
        else:
            return string_of_elem(val)

    var_defs_hash = {}

    def gen_text_line_and_name_collision(key, val):
        ''' In the absence of a name collision, create a line describing a bash variable.
        '''
        if check_name_collision:
            if key not in environ and key not in var_defs_hash:
                var_defs_hash[key] = val
            else:
                sys.stderr.write(dxpy.utils.printing.fill(
                    "Creating environment variable ({}) would cause a name collision".format(key))
                    + "\n")
        else:
            var_defs_hash[key] = val

    # Processing non-file variables before the file variables. This priorities them,
    # so that in case of name collisions, the file-variables will be dropped.
    for key, desc in rest_hash.iteritems():
        gen_text_line_and_name_collision(key, string_of_value(desc))
    for file_key, desc in file_key_descs.iteritems():
        gen_text_line_and_name_collision(file_key, string_of_value(desc['handler']))
        gen_text_line_and_name_collision(file_key + "_name", string_of_value(desc['basename']))
        gen_text_line_and_name_collision(file_key + "_prefix", string_of_value(desc['prefix']))
        gen_text_line_and_name_collision(file_key + "_path", string_of_value(desc['path']))

    return var_defs_hash
