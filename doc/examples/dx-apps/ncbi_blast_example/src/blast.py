#!/usr/bin/env python
# ncbi_blast_example 0.0.1
#
# Example app that calls out to NCBI BLAST.

import dxpy
import subprocess

DEFAULT_E = 0.001

FIELDS = ['query', 'subject', 'percent_id', 'length', 'missmatches', 'gaps', 'qstart', 'qend', 'sstart', 'send', 'e', 'bit']
INT_FIELDS = set(['length', 'missmatches', 'gaps', 'qstart', 'qend', 'sstart', 'send'])
FLOAT_FIELDS = set(['percent_id', 'e', 'bit'])

@dxpy.entry_point('main')
def main(query_file, db_file, **kwargs):
    # You can find example data files in the example_data subdirectory. To load
    # the data files and run this applet with them:
    #
    # dx upload db_file.fasta
    # dx upload query_file.fasta
    # dx-build-applet --overwrite ncbi_blast_example
    # dx run --watch ncbi_blast_example --input query_file=query_file.fasta --input db_file=db_file.fasta

    # Download the input files.
    dxpy.dxfile_functions.download_dxfile(query_file, "query.fasta")
    dxpy.dxfile_functions.download_dxfile(db_file, "db.fasta")

    # Create the database for BLAST.
    subprocess.check_call('formatdb -p F -i db.fasta -n db', shell=True)
    # Run BLAST on the output.
    subprocess.check_call('blastall -p blastn -d db -i query.fasta -m 8 -e %f > results.txt' % (DEFAULT_E,), shell=True)

    # Parse the tabular output into a list of dictionaries and return it.
    result = []

    for line in open('results.txt'):
        h = dict(zip(FIELDS, line.split()))
        for field in FIELDS:
            if field in INT_FIELDS:
                h[field] = int(h[field])
            if field in FLOAT_FIELDS:
                h[field] = float(h[field])
        result.append(h)

    return { 'result' : result }

dxpy.run()
