#!/usr/bin/env python

import os, sys, md5
from optparse import OptionParser

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "..", "execserver", "lib"))
import dxpy

parser = OptionParser()
#parser.add_option("--num_workers", help="Number of worker threads", type="int", default=2)
(opts, args) = parser.parse_args()

security_context={'user_id': 1, 'auth_token': md5.new("").hexdigest()}

fastq_columns = ["id:string", "sequence:string", "quality:string"]

with open(args[0], 'rb') as fh:
    fid = dxpy.uploadFile(fh, security_context, name=fh.name)
    with dxpy.getFile(fid, security_context) as dxfh, dxpy.DNAnexusTable(fastq_columns, security_context) as dxtable:
        print "Writing fastq from file %s to table %s" % (dxfh.name, dxtable.id)
        i = 0
        fq_line, table_rows = [], []
        for line in dxfh:
            fq_line.append(line)
            i += 1
            if i % 4 == 0:
                table_rows.append(fq_line)
                fq_line = []
            if i % 40000 == 0:
                dxtable.appendRows(table_rows)
                table_rows = []
        print "Wrote %d rows from fastq to table %s" % (i/4, dxtable.id)

#        TODO: FIXME
#        try:
#            while True:
#                l1 = dxfh.next()
#                l2 = dxfh.next()
#                l3 = dxfh.next()
#                l4 = dxfh.next()                
#        except StopIteration:
