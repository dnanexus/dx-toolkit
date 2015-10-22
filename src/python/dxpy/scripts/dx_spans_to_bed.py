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

import argparse, json, sys, os
import dxpy


arg_parser = argparse.ArgumentParser(description="Download a Spans object into a BED file.  The spans type definition can be found here:  https://wiki.dnanexus.com/Types/Spans.  Information about the BED file format is available here: http://genome.ucsc.edu/FAQ/FAQformat.html#format1")
arg_parser.add_argument("Spans", help="ID of the Spans object")
arg_parser.add_argument("--output", help="Name of the output BED file", required=True)


bed_col = {"chr":0,
           "lo":1,
           "hi":2,
           "name":3,
           "score":4,
           "strand":5,
           "thick_start":6,
           "thick_end":7,
           "item_rgb":8,
           "block_count":9,
           "block_sizes":10,
           "block_starts":11}

default_bed_line = ["-", "0", "0", "-", "0", ".", "0", "0", "0,0,0", "0", "0", "0"]

class gene:
    def __init__(self, founder):
        self.parent_ids = [founder['span_id']]
        self.gene = None
        self.trans = []
        self.add_data(founder)

    def add_data(self, data ):
        if data['type'] == 'gene':
            self.gene = data
        # this clause handles transcripts (or other typed spans) that are their
        # own parents and also transcripts that are children of this gene
        elif data['parent_id'] == -1 or (self.gene != None and data['parent_id'] == self.gene['span_id']):
            self.trans.append(transcript(data))
            self.parent_ids.append(data['span_id'])
        # this captures all children such as exons, CDS, UTRs, etc
        else:
            for t in self.trans:
                if data['parent_id'] == t.data['span_id']:
                    t.add_data(data)

    def check_and_write_data(self, current_lo, bed_file):
        if self.gene == None:
            return self.trans[0].check_and_write_data(current_lo, bed_file)
        # just write the gene if we're a lone gene and it's passed
        elif len(self.trans) == 0 and current_lo > self.gene['hi']:
            output_row = default_bed_line[:]
            output_row[bed_col['chr']] = self.gene['chr']
            output_row[bed_col['chr']] = self.gene['lo']
            output_row[bed_col['chr']] = self.gene['hi']
            output_row[bed_col['chr']] = self.gene['name']
            output_row[bed_col['chr']] = self.gene['strand']
            if "thick_start" in self.gene:
                if self.gene['thick_start'] != dxpy.NULL:
                    output_row[bed_col['thick_start']] = str(self.gene['thick_start'])
            if "thick_end" in self.gene:
                if self.gene['thick_end'] != dxpy.NULL:
                    output_row[bed_col['thick_end']] = str(self.gene['thick_end'])
            if "score" in self.gene:
                if self.gene['score'] != dxpy.NULL:
                    output_row[bed_col['score']] = str(self.gene['score'])
            return True
        elif current_lo > self.gene['hi']:
            for t in self.trans:
                if t.check_and_write_data(current_lo, bed_file) != True:
                    raise dxpy.AppError("found end of gene but not end of transcript: " + str(self.gene))
            return True
        else:
            return False

class transcript:
    def __init__(self, t):
        self.data = t
        self.exons = []

    def add_data( self, e ):
        self.exons.append(e)

    def check_and_write_data(self, current_lo, bed_file):
        if self.data['hi'] >= current_lo:
            return False
        else:
            output_row = default_bed_line[:]
            output_row[bed_col['chr']] = self.data['chr']
            output_row[bed_col['lo']] = str(self.data['lo'])
            output_row[bed_col['hi']] = str(self.data['hi'])
            output_row[bed_col['name']] = self.data['name']
            output_row[bed_col['strand']] = self.data['strand']
            if "score" in self.data:
                if self.data['score'] != dxpy.NULL:
                    output_row[bed_col['score']] = str(self.data['score'])

            # find and set thick_start and think_end
            thick_start = self.data["lo"]
            thick_end = self.data["hi"]
            for e in self.exons:
                if (e["type"] == "5' UTR" and e["strand"] == "+") or (e["type"] == "3' UTR" and e["strand"] == "-"):
                    if e["hi"] > thick_start:
                        thick_start = e["hi"]

                if (e["type"] == "3' UTR" and e["strand"] == "+") or (e["type"] == "5' UTR" and e["strand"] == "-"):
                    if e["lo"] < thick_end:
                        thick_end = e["lo"]

            output_row[bed_col['thick_start']] = str(thick_start)
            output_row[bed_col['thick_end']] = str(thick_end)

            block_sizes = []
            block_starts = []

            output_row[bed_col['block_count']] = str(len(self.exons))
            for i in range(len(self.exons)):
                block_sizes.append(str(self.exons[i]['hi'] - self.exons[i]['lo']))
                block_starts.append(str(self.exons[i]['lo'] - self.data['lo']))

            if output_row[bed_col['block_count']] != 0:
                output_row[bed_col["block_sizes"]] = ",".join(block_sizes)
                output_row[bed_col["block_starts"]] = ",".join(block_starts)

            bed_file.write("\t".join(output_row) + "\n")
        return True


def main(**kwargs):
    if len(kwargs) == 0:
        kwargs = vars(arg_parser.parse_args(sys.argv[1:]))

    try:
        spans = dxpy.DXGTable(kwargs['Spans'])
    except:
        raise dxpy.AppError("Failed to open Spans object for export")

    spans_types = spans.describe()['types']

    if 'Genes' in spans_types:
        export_genes(spans, kwargs['output'])
    else:
        export_generic_bed(spans, kwargs['output'])

    
# genes type objects are a special case
def export_genes(spans, out_name):
    global bed_col
    # setup default 

    span_cols = spans.get_col_names()

    # figure out how many columns our BED needs to have
    with open(out_name, 'w') as bed_file:
        # copy over default bed line
        output_row = default_bed_line[:]

        incomplete_buffer = []
        gene_model = []

        generator = spans.iterate_rows(want_dict=True)

        while(True):

            # loop through table, buffering incomplete lines
            try:
                entry = generator.next()
            except StopIteration:
                entry = None

            if entry != None:
                # take founding members (those with no parents) place in gene model
                if entry['parent_id'] == -1:
                    #if entry['type'] in ['gene','transcipt']:
                    gene_model.append(gene(entry))
                    # if it's a top level object other than these we don't handle it
                    #else:
                    #    continue
                # otherwise they are to be added later
                else:
                    incomplete_buffer.append(entry)
                current_chr = entry['chr']
                current_lo = entry['lo']
            else:
                # switch these to different values to flush final genes out to file
                current_chr = ""
                current_lo = 0
            parent_to_write = {}
            buff_to_keep = []

            to_keep_exons = []

            # if we've found your parent, add you in the same model
            for orphan in incomplete_buffer:
                added = False
                for g in gene_model:
                    if orphan['parent_id'] in g.parent_ids:
                        g.add_data(orphan)
                        added = True
                if not added:
                    if not current_lo > orphan['hi']:
                        to_keep_exons.append(orphan)

            incomplete_buffer = to_keep_exons[:]

            to_keep_genes = []

            for g in gene_model:
                if not g.check_and_write_data(current_lo, bed_file):
                    to_keep_genes.append(g)
                    
            gene_model = to_keep_genes[:]

            if entry == None:
                return

##########################################


# this function exports any gri, Spans, or NamedSpans object but not a Genes object
def export_generic_bed(spans, out_name):

    global bed_col

    spans_columns = spans.get_col_names()

    # minimally we'll have 3 columns for chr, lo, hi
    num_bed_cols = 3

    # find the minimum number of columns we need to represent this Spans object
    for col in bed_col:
        if col in spans_columns:
            num_bed_cols = max(num_bed_cols, bed_col[col]+1)

    with open(out_name, 'w') as bed_file:
        # iterate over all entries in the Spans object
        for entry in spans.iterate_rows(want_dict=True):
            output_row = default_bed_line[:num_bed_cols]
            for col in bed_col:
                # if we have the column, add its value in the right place
                if col in spans_columns:
                    if entry[col] != dxpy.NULL:
                        output_row[bed_col[col]] = str(entry[col])
            bed_file.write("\t".join(output_row)+"\n")


if __name__ == '__main__':
    main()


