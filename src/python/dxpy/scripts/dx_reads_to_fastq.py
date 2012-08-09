#!/usr/bin/env python

import os, sys, json, logging
logging.basicConfig(level=logging.DEBUG)

from optparse import OptionParser
import dxpy

parser = OptionParser(usage="%prog dxtable-id [options]", description="Download a reads table into a FASTQ file")
parser.add_option("-s", "--start_row", help="Start at this table row", type="int", default=0)
parser.add_option("-e", "--end_row", help="End at this table row", type="int", default=None)
(opts, args) = parser.parse_args()

if len(args) != 1:
    parser.print_help()
    parser.error("Incorrect number of arguments")

if opts.end_row is not None and opts.end_row <= opts.start_row:
    parser.error("End row %d must be greater than start row %d" % (opts.end_row, opts.start_row))

dxtable_id = args[0]

try:
    table = dxpy.open_dxgtable(dxtable_id)
except:
    print "failed to open table!\n"
    sys.exit(1)

existCols = table.get_col_names()

col = [ "sequence", "quality" ]

if "name" in existCols:
    hasName = True
    col.append("name")
    
for row in table.iterate_query_rows(columns=col):
    if hasName == True:
        print "\n".join([row[2], row[0], "+", row[1]])
    else:
        print "\n".join(["@", row[0], "+", row[1]])


