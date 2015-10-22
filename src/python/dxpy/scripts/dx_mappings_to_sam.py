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

import dxpy
import math
import argparse
import re
import sys

from dxpy.utils.genomic_utils import reverse_complement as reverseComplement

#Usage: sample input: dx_MappingsTableToSamBwa --table_id <gtable_id> --output <filename>
#Example: dx_MappingsTableToSamBwa --table_id gtable-9yZvF200000PYKJyV4k00005 --output mappings.sam

parser = argparse.ArgumentParser(description="Export Mappings gtable to SAM format")
parser.add_argument("mappings_id", help="Mappings table id to read from")
parser.add_argument("--output", dest="file_name", default=None, help="Name of file to write SAM to.  If not given SAM file will be printed to stdout.")
parser.add_argument("--start_row", dest="start_row", type=int, default=0, help="If restricting by the id of the gtable row, which id to start at. Selecting regions will override this option")
parser.add_argument("--end_row", dest="end_row", type=int, default=0, help="If restricting by the id of the gtable row, which id to start at. Selecting regions will override this option")                  
parser.add_argument("--region_index_offset", dest="region_index_offset", type=int, default = 0, help="Adjust regions by this amount. Useful for converting between zero and one indexed lists")
parser.add_argument("--region_file", dest="region_file", default="", help="Regions to extract mappings for, in the format ChrX:A-B")
parser.add_argument("--id_as_name", dest="id_as_name", action="store_true", default=False, help="Use the template_id instead of the name when writing the SAM")
parser.add_argument("--id_prepend", dest="id_prepend", default='', help="When id_as_name is selected, text entered into this option will be written in front of the name to facilitate the combination of multiple samples when mergine SAMs")
parser.add_argument("--discard_unmapped", dest="discard_unmapped", action="store_true", default=False, help="If set, do not write unmapped reads to SAM")
parser.add_argument("--reference", dest="reference", default=None, help="Generating a SAM file requires information about the reference the reads were mapped to.  The Mappings SHOULD have a link to their reference, in the case they do not, or you wish to override that reference, you may optionally supply the ID of a ContigSet object to use instead.")
parser.add_argument("--no_interchromosomal_mate", dest="no_interchromosomal", action="store_true", default=False, help="If selected, do not output reads where the mates are mapped to different chromosomes.")
parser.add_argument("--only_interchromosomal_mate", dest="only_interchromosomal", action="store_true", default=False, help="If selected, output only reads where the mates are mapped to different chromosomes. Selecting no_interchromosomal_mate will take precendence.")
parser.add_argument("--assign_read_group", dest="assign_read_group", default="", help="If entered, this value will be used for the read group id of all exported mappings")
parser.add_argument("--read_group_platform", dest="read_group_platform", default="", help="If entered, will print this as the platform used for the read group in the SAM header")
parser.add_argument("--write_row_id", dest="write_row_id", default=False, action="store_true", help="If selected, the row of the mappings table will be written into optional sam tag ZD")

def main(**kwargs):

    if len(kwargs) == 0:
        opts = parser.parse_args(sys.argv[1:])
    else:
        opts = parser.parse_args(kwargs)

    if opts.mappings_id == None:
        parser.print_help()
        sys.exit(1)
    
    mappingsTable = dxpy.DXGTable(opts.mappings_id)
    idAsName = opts.id_as_name
    idPrepend = opts.id_prepend
    writeRowId = opts.write_row_id
    
    paired = "chr2" in mappingsTable.get_col_names()

    regions = []
    if opts.region_file != "":
        regions = re.findall("-L ([^:]*):(\d+)-(\d+)", open(opts.region_file, 'r').read())
    
    name = mappingsTable.describe()['name']
    
    if opts.reference != None:
        originalContig = opts.reference
    else:
        try:
            originalContig = mappingsTable.get_details()['original_contigset']['$dnanexus_link']
        except:
            raise dxpy.AppError("The original reference genome must be attached to mappings table")
    
    try:
        contigDetails = dxpy.DXRecord(originalContig).get_details()['contigs']
    except:
        raise dxpy.AppError("Unable to access reference with ID "+originalContig)

    contigNames = contigDetails['names']
    contigSizes = contigDetails['sizes']
    
    if opts.file_name != None:
        outputFile = open(opts.file_name, 'w')
    else:
        outputFile = None

    header = ""

    for i in range(len(contigNames)):
        header += "@SQ\tSN:"+str(contigNames[i])+"\tLN:"+str(contigSizes[i])+"\n"

    assignReadGroup = opts.assign_read_group
    if assignReadGroup != "":
        header += "@RG\tID:" + assignReadGroup + "\tSM:Sample_0"
    else:
        for i in range(len(mappingsTable.get_details()['read_groups'])):
            header += "@RG\tID:"+str(i) + "\tSM:Sample_"+str(i)    
            if opts.read_group_platform != '':
                header += "\tPL:"+opts.read_group_platform
            header += "\n"

    if outputFile != None:
        outputFile.write(header)
    else:
        sys.stdout.write(header)

    col = {}
    names = mappingsTable.get_col_names()
    for i in range(len(names)):
        col[names[i]] = i+1

    column_descs = mappingsTable.describe()['columns']

    sam_cols = []; sam_col_names = []; sam_col_types = {}
    for c in column_descs:
        if c['name'].startswith("sam_field_") or c['name'] == "sam_optional_fields":
            sam_cols.append(c)
            sam_col_names.append(c['name'])
            sam_col_types[c['name']] = c['type']

    defaultCol = {"sequence":"", 
                  "name":"", 
                  "quality": "", 
                  "status": "UNMAPPED", 
                  "chr":"", 
                  "lo":0, 
                  "hi":0, 
                  "negative_strand":False, 
                  "error_probability":0, 
                  "qc_fail":False, 
                  "duplicate":False,
                  "cigar":"", 
                  "mate_id":-1, 
                  "status2":"", 
                  "chr2":"", 
                  "lo2":0, 
                  "hi2":0, 
                  "negative_strand2":False, 
                  "proper_pair":False, 
                  "read_group":0}

    #unmappedFile = open("unmapped.txt", 'w')
        
    if len(regions) == 0:

        if opts.start_row > mappingsTable.describe()['length']:
            raise dxpy.AppError("Starting row is larger than number of rows in table")
        elif opts.end_row < opts.start_row:
            raise dxpy.AppError("Ending row is before Start")

        if opts.end_row > 0:
            generator = mappingsTable.iterate_rows(start=opts.start_row, end=opts.end_row, want_dict=True)
        else:
            generator = mappingsTable.iterate_rows(start=opts.start_row, want_dict=True)

        # write each row unless we're throwing out unmapped 
        for row in generator:
            if row["status"] != "UNMAPPED" or opts.discard_unmapped == False:
                if not paired:
                    writeRow(row, col, defaultCol, outputFile, idAsName, idPrepend, writeRowId, assignReadGroup, column_descs, sam_cols, sam_col_names, sam_col_types)
                elif opts.no_interchromosomal and row["chr"] == row["chr2"]:
                    writeRow(row, col, defaultCol, outputFile, idAsName, idPrepend, writeRowId, assignReadGroup, column_descs, sam_cols, sam_col_names, sam_col_types)
                elif opts.only_interchromosomal and opts.no_interchromosomal == False and (row["chr"] != row["chr2"] or (row["chr"] == "" and row["chr2"] == "")):
                    writeRow(row, col, defaultCol, outputFile, idAsName, idPrepend, writeRowId, assignReadGroup, column_descs, sam_cols, sam_col_names, sam_col_types)
                elif opts.no_interchromosomal == False and opts.only_interchromosomal == False:
                    writeRow(row, col, defaultCol, outputFile, idAsName, idPrepend, writeRowId, assignReadGroup, column_descs, sam_cols, sam_col_names, sam_col_types)
                

    else:
        for x in regions:
            # generate the query for this region
            query = mappingsTable.genomic_range_query(x[0],int(x[1])+opts.region_index_offset,int(x[2])+opts.region_index_offset, index='gri')
            for row in mappingsTable.get_rows(query=query, limit=1)['data']:
                startRow =  row[0]
                for row in mappingsTable.iterate_rows(start=startRow, want_dict=True):
                    if row["chr"] != x[0] or row["lo"] > int(x[2])+opts.region_index_offset:
                        break
                    if row["status"] != "UNMAPPED" or opts.discard_unmapped == False:
                        if not paired:
                            writeRow(row, col, defaultCol, outputFile, idAsName, idPrepend, writeRowId, assignReadGroup, column_descs, sam_cols, sam_col_names, sam_col_types)
                        elif opts.no_interchromosomal and row["chr"] == row["chr2"]:
                            writeRow(row, col, defaultCol, outputFile, idAsName, idPrepend, writeRowId, assignReadGroup, column_descs, sam_cols, sam_col_names, sam_col_types)
                        elif opts.only_interchromosomal and opts.no_interchromosomal == False and (row["chr"] != row["chr2"] or (row["chr"] == "" and row["chr2"] == "")):
                            writeRow(row, col, defaultCol, outputFile, idAsName, idPrepend, writeRowId, assignReadGroup, column_descs, sam_cols, sam_col_names, sam_col_types)
                        elif opts.no_interchromosomal == False and opts.only_interchromosomal == False:
                            writeRow(row, col, defaultCol, outputFile, idAsName, idPrepend, writeRowId, assignReadGroup, column_descs, sam_cols, sam_col_names, sam_col_types)

    if outputFile != None:
        outputFile.close()

def tag_value_is_default(value):
    #2**31 is a legacy Null value and will be removed when possible
    return value == dxpy.NULL or value == 2**31-1 or value == "" or (type(value) == float and math.isnan(value))

def col_name_to_field_name(name):
    if name == 'sam_optional_fields':
        return name
    else:
        return name[10:]

def col_type_to_field_type(col_type):
    if col_type == 'int32':
        return 'i'
    elif col_type == 'float':
        return 'f'
    else:
        return 'Z'

def format_tag_field(name, value, sam_col_types):
    if name == "sam_optional_fields":
        return value
    else:
        return ":".join([col_name_to_field_name(name), col_type_to_field_type(sam_col_types[name]), str(value)])

def writeRow(row, col, defaultCol, outputFile, idAsName, idPrepend, writeRowId, assignReadGroup, column_descs, sam_cols, sam_col_names, sam_col_types):
    out_row = ""

    values = dict(defaultCol)
    values.update(row)

    flag =  0x1*(values["mate_id"] > -1 and values["mate_id"] <= 1)
    flag += 0x2*(values["proper_pair"] == True) 
    flag += 0x4*(values["status"] == "UNMAPPED")
    flag += 0x8*(values["status2"] == "UNMAPPED") 
    flag += 0x10*(values["negative_strand"] == True) 
    flag += 0x20*(values["negative_strand2"] == True)
    flag += 0x40*(values["mate_id"] == 0) 
    flag += 0x80*(values["mate_id"] == 1) 
    flag += 0x100*(values["status"] == "SECONDARY")
    flag += 0x200*(values["qc_fail"]) 
    flag += 0x400*(values["duplicate"])

    
    chromosome = values["chr"]
    lo = values["lo"]+1
    if values["chr"] == "":
        chromosome = "*"
        lo = 0

    if values["chr2"] == values["chr"]:
        chromosome2 = "="
    else:
        chromosome2 = values["chr2"]

    lo2 = values["lo2"]+1
    if values["chr2"] == "":
        chromosome2 = "*"
        lo2 = 0
    
    if idAsName:
        readName = idPrepend
        readName += str(row["template_id"])

    else:
        readName = values["name"]    
        if readName.strip("@") == "":
            readName = "*"    
    
    if values.get("quality") == None or values.get("quality") == "":
        qual = "*"
    else:
        qual = values["quality"].rstrip('\n')
    seq = values["sequence"]
    
    if values["negative_strand"]:
        try:
            seq = reverseComplement(seq)
        except ValueError as e:
            raise dxpy.AppError("Error converting row %d: %s" % (row["__id__"], e.message))
        qual = qual[::-1]
    
    if values["mate_id"] == -1 or values["chr"] != values["chr2"] or values["chr"] == '' or values["chr"] == '*':
        tlen = 0
    else:
        tlen = (max(int(values["hi2"]),int(values["hi"])) - min(int(values["lo2"]),int(values["lo"])))
        if int(values["lo"]) > int(values["lo2"]):
            tlen *= -1

    out_row = [readName.strip("@"), str(flag), chromosome, str(lo), str(values["error_probability"]), values["cigar"] , chromosome2, str(lo2), str(tlen), seq, qual]
    tag_values = {c: values[c] for c in sam_col_names if not tag_value_is_default(values[c])}

    out_row.extend([format_tag_field(name, value, sam_col_types) for name, value in tag_values.iteritems()])

    if assignReadGroup != "":
        out_row.append("RG:Z:" + assignReadGroup)
    else:
        out_row.append("RG:Z:"+str(values['read_group']))
        
    if writeRowId:
        out_row.append("ZD:Z:"+str(row["__id__"]))
    
    
    out_row = "\t".join(out_row) + "\n"

    if outputFile != None:
        outputFile.write(out_row)
    else:
        sys.stdout.write(out_row)

if __name__ == '__main__':
    main()

