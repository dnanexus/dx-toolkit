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

parser = argparse.ArgumentParser(description="Export Genes object to GFF File")
parser.add_argument("genes_id", help="Genes table id to read from")
parser.add_argument("--output", dest="file_name", default=None, help="Name of file to write GFF to.  If not given GFF file will be printed to stdout.")
parser.add_argument("--only_genes_types", action="store_true", default=False, dest="only_genes_types", help="Name of file to write GFF to.  If not given GFF file will be printed to stdout.")


def main(**kwargs):

    if len(kwargs) == 0:
        opts = parser.parse_args(sys.argv[1:])
    else:
        opts = parser.parse_args(kwargs)

    if opts.genes_id == None:
        parser.print_help()
        sys.exit(1)
        
    if opts.file_name != None:
        outputFile = open(opts.file_name, 'w')
    else:
        outputFile = None
        

    if opts.genes_id == None:
        parser.print_help()
        sys.exit(1)

    tableId = opts.genes_id
    table = dxpy.DXGTable(tableId)
    
    genesTypes = {"exon": True, "CDS":True, "5' UTR": True, "3' UTR": True, "transcript":True, "gene":True}
    translatedTypes = {"transcript":"mRNA", "5' UTR": "five_prime_UTR", "3' UTR":"three_prime_UTR"}
    
    columns = table.get_col_names()
    idColumn = None
    parentColumn = None
    if "ID" in columns:
        idColumn = "ID"
    elif "Id" in columns:
        idColumn = "Id"
    elif "id" in columns:
        idColumn = "id"
    else:
        idColumn = "span_id"
        
    if "Parent" in columns:
        parentColumn = "Parent"
    elif "PARENT" in columns:
        parentColumn = "PARENT"
    elif "parent" in columns:
        parentColumn = "parent"
    else:
        parentColumn = "parent_id"
    
    for row in table.iterate_rows(want_dict=True):
        typ = row["type"]
        if opts.only_genes_types == False or genesTypes.get(typ) != None:
            if translatedTypes.get(typ) != None:
                typ = translatedTypes[typ]

            reservedColumns = ["chr", "lo", "hi", "span_id", "type", "strand", "score", "is_coding", "parent_id", "frame", "source", "__id__", "ID", "Id", "id", "Parent", "PARENT", "parent"]
            attributes = ""
            
            rowId = str(row[idColumn])
            parentId = str(row[parentColumn])
        
            attributes += "ID=\"" + rowId + "\";" 
            if not (parentColumn == "parent_id" and parentId == "-1"):
                attributes += "Parent=\"" + parentId + "\";"
            
            for k, v in row.iteritems():
                if k not in reservedColumns and v != '':
                    attributes += k + "=" + '"'+str(v)+'";'

            chromosome = row["chr"]
            lo = str(row["lo"] + 1)
            hi = str(row["hi"])
    
            strand = row["strand"]
            if strand == '':
                strand = '.'
            if row["frame"] == -1:
                frame = '.'
            else:
                frame = str(row["frame"])
            source = '.'
            
            # 2**31 and 2**31-1 are legacy null values that will be removed when possible
            if row.get("score") == None:
                score = "."
            if row["score"] == dxpy.NULL or row["score"] == 2**31-1 or row["score"] == float(2**31):
                score = "."
            else:
                score = str(row["score"])
            
            if row.get("source") != None:
                if row["source"] !=  '':
                    source = row["source"]
            result = "\t".join([chromosome, source, typ, lo, hi, score, strand, frame, attributes.rstrip(";")])+"\n"
            if outputFile != None:
                outputFile.write(result)
            else:
                sys.stdout.write(result)

if __name__ == '__main__':
    main()

