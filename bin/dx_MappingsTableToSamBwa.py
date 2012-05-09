#!/usr/bin/env python

import dxpy
import math
from optparse import OptionParser


#Usage: sample input: dx_MappingsTableToSamBwa --table_id <gtable_id> --output <filename>
#Example: dx_MappingsTableToSamBwa --table_id gtable-9yZvF200000PYKJyV4k00005 --output mappings.sam

def main():

    parser = OptionParser("Usage: %prog sam_filename mappings_id reads_id reads_offset reads_end part_num")
    parser.add_option("--table_id", dest="mappings_id", help="Mappings table id to read from")
    parser.add_option("--output", dest="file_name", help="Name of file to write SAM to")
    (opts, args) = parser.parse_args()

    
    mappingsTable = dxpy.open_dxgtable(opts.mappings_id)
    
    try:
        originalContig = mappingsTable.get_details()['reference_contig_set']
    except:
        raise Exception("The original reference genome must be attached as a detail")
    
    contigDetails = dxpy.DXRecord(originalContig).get_details()['contigs']
    contigNames = contigDetails['names']
    contigSizes = contigDetails['sizes']
    
    outputFile = open(opts.file_name, 'w')
    
    for i in range(len(contigNames)):
        outputFile.write("@SQ\tSN:"+contigNames[i]+"\t"+str(contigSizes[i])+"\n")

    col = {}
    names = mappingsTable.get_col_names()   
    for i in range(len(names)):
        col[names[i]] = i+1
        
    for x in mappingsTable.iterate_rows():
        flag = 0x1*(x[col["mate_id"]] is 0) + 0x2*(x[col["proper_pair"]] is True) + 0x4*(x[col["status"]] is "UNMAPPED")
        flag += 0x8*(x[col["status2"]] is "UNMAPPED") + 0x10*(x[col["negative_strand"]] is True) + 0x20*(x[col["negative_strand2"]] is True)
        flag += 0x40*(x[col["mate_id"]] is 1) + 0x80*(x[col["mate_id"]] is -1) + 0x100*(x[col["status"]] is "SECONDARY")
        flag += 0x200*(x[col["qc"]] is "not passing quality controls") + 0x400*(x[col["qc"]] is "PCR or optical duplicate")
        flag += (0x200+0x400)*(x[col["qc"]] is "both not qc and PCR or optical duplicate")
        outputFile.write(x[col["name"]] + "\t" + str(flag) + "\t" + x[col["chr"]] + "\t" + str(x[col["lo"]]+1) + "\t")
        outputFile.write(str(x[col["error_probability"]]) + "\t" + x[col["cigar"]] + "\t" + x[col["chr2"]] + "\t")
        outputFile.write(str(x[col["lo2"]]+1) + "\t" + str(math.fabs(x[col["hi"]] - x[col["lo"]])) + "\t" + x[col["sequence"]] + "\t")
        outputFile.write(str(x[col["quality"]] + "\n"))