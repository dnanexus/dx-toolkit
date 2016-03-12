#!/usr/bin/env python
#
# Copyright (C) 2013-2016 DNAnexus, Inc.
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
import argparse
import sys

parser = argparse.ArgumentParser(description="Export Mappings gtable to a FASTQ/FASTA file")
parser.add_argument("mappings_id", help="Mappings table id to read from")
parser.add_argument("--output", dest="file_name", default=None, help="Name of file to write FASTQ to.  If not given data will be printed to stdout.")


def writeFastq( row, fh ):
    if 'name' in row:
        fh.write("".join(["@", row['name'], "\n"]))
    else:
        fh.write("@\n")

    fh.write(row['sequence']+"\n")
    fh.write("+\n")
    fh.write(row['quality']+"\n")

def writeFasta( row, fh ):
    if 'name' in row:
        fh.write("".join([">", row['name'],"\n"]))
    else:
        fh.write(">\n")
    
    fh.write(row['sequence']+"\n")


def main(**kwargs):
    if len(kwargs) == 0:
        opts = parser.parse_args(sys.argv[1:])
    else:
        opts = parser.parse_args(kwargs)

    if opts.mappings_id == None:
        parser.print_help()
        sys.exit(1)
    
    mappingsTable = dxpy.DXGTable(opts.mappings_id)

    if opts.file_name != None:
        fh = open(opts.file_name, "w")
    else:
        fh = sys.stdout

    if 'quality' in mappingsTable.get_col_names():
        outputFastq = True
    else:
        outputFastq = False

    for row in mappingsTable.iterate_rows(want_dict=True):
        if outputFastq:
            writeFastq( row, fh )
        else:
            writeFasta( row, fh )

    
