#!/usr/bin/env python
#
# Copyright (C) 2013-2015 DNAnexus, Inc.
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

import sys, argparse
import dxpy

arg_parser = argparse.ArgumentParser(description="Download a reads table into a FASTQ file")
arg_parser.add_argument("reads_table", help="ID of the reads GTable object")
arg_parser.add_argument("--output", help="Name of the output file", required=True)
arg_parser.add_argument("--output2", help="Name of the second output file (for paired reads)")
arg_parser.add_argument("--discard_names", help="Discard read names", type=bool, default=False)
arg_parser.add_argument("--output_FASTA", help="Output FASTA instead of FASTQ", type=bool, default=False)
arg_parser.add_argument("-s", "--start_row", help="Start at this table row", type=int, default=0)
arg_parser.add_argument("-e", "--end_row", help="End at this table row", type=int, default=None)

def main(**kwargs):
    if len(kwargs) == 0:
        kwargs = vars(arg_parser.parse_args(sys.argv[1:]))

    if "end_row" not in kwargs:
        kwargs["end_row"] = None

    if kwargs["end_row"] is not None and kwargs["end_row"] <= kwargs["start_row"]:
        arg_parser.error("End row %d must be greater than start row %d" % (kwargs["end_row"], kwargs["start_row"]))

    try:
        table = dxpy.DXGTable(kwargs['reads_table'])
    except:
        raise dxpy.AppError("Failed to open table for export")

    existCols = table.get_col_names()

    ### sort out columns to download

    col = []
    col2 = []

    # if there's a second sequence, it's paired
    if "sequence2" in existCols:
        isPaired = True
    else:
        isPaired = False

    if "name" in existCols and kwargs['discard_names'] != True:
        hasName = True
        col.append("name")
        if isPaired == True:
            col2.append("name2")
    else:
        hasName = False

    col.append("sequence")
    if isPaired == True:
        col2.append("sequence2")

    if "quality" in existCols:
        hasQual = True
        col.append("quality")
        if isPaired == True:
            col2.append("quality2")
    else:
        hasQual = False
        # if we don't have quals we must output FASTA instead
        kwargs['output_FASTA'] = True

    if kwargs['output'] is None:
            raise dxpy.AppError("output parameter is required")

    with open(kwargs['output'], 'wb') as out_fh:
        exportToFile(columns=col, table=table, output_file=out_fh, hasName=hasName, hasQual=hasQual, FASTA=kwargs['output_FASTA'], start_row=kwargs['start_row'], end_row=kwargs['end_row'])

    if isPaired == True:
        if kwargs['output2'] is None:
            raise dxpy.AppError("output2 parameter is required for paired reads")
        with open(kwargs['output2'], 'wb') as out_fh2:
            exportToFile(columns=col2, table=table, output_file=out_fh2, hasName=hasName, hasQual=hasQual, FASTA=kwargs['output_FASTA'], start_row=kwargs['start_row'], end_row=kwargs['end_row'])

def exportToFile(columns, table, output_file, hasName = True, hasQual = True, FASTA = False, start_row = 0, end_row = None):
    for row in table.iterate_rows(start=start_row, end=end_row, columns=columns):
        if FASTA == True:
            if hasName == True:
                # change comment character for FASTA
                if row[0][0] == '@':
                    row[0] = u'>' + row[0][1:]
                # if already has comment character (>)
                if row[0][0] == ">":
                    output_file.write('\n'.join([ row[0], row[1] ]))
                # otherwise, add it
                else:
                    output_file.write('\n'.join([">" + row[0], row[1] ]))
            else:
                output_file.write('\n'.join([">", row[0]]))

        #output FASTQ
        else:
            if hasName == True:
            # if alread has comment character (@)
                if row[0][0] == "@":
                    output_file.write('\n'.join([ row[0], row[1] ]))
            # otherwise, add it
                else:
                    output_file.write('\n'.join(["@" + row[0], row[1] ]))
            # add qualities if they exist
                if hasQual == True:
                    output_file.write('\n'.join(["\n+", row[2] ]))
            # else add without name
            else:
                output_file.write('\n'.join(["@", row[0]]))
                if hasQual == True:
                    output_file.write('\n'.join(['', "+", row[1] ]))

        # end of current record
        output_file.write('\n')

    output_file.close()
    return output_file.name

if __name__ == '__main__':
    main()
