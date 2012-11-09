#!/usr/bin/env python

import argparse, json, sys, os
import dxpy


arg_parser = argparse.ArgumentParser(description="Download a Spans object into a BED file.  The spans type definition can be found here:  http://wiki.dnanexus.com/Types/Spans.  Information about the BED file format is available here: http://genome.ucsc.edu/FAQ/FAQformat.html#format1")
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


def main(**kwargs):
    if len(kwargs) == 0:
        kwargs = vars(arg_parser.parse_args(sys.argv[1:]))

    print kwargs

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
    default_bed_line = ["-", "0", "0", "-", "0", ".", "0", "0", "0,0,0", "0", "0", "0"]

    span_cols = spans.get_col_names()

    # figure out how many columns our BED needs to have
    with open(out_name, 'w') as bed_file:
        # copy over default bed line
        output_row = default_bed_line[:]

        incomplete_buffer = []

        generator = spans.iterate_rows(want_dict=True)

        while(True):

            # loop through table, buffering incomplete lines
            try:
                entry = generator.next()
            except StopIteration:
                entry = None


            #print "current entry"
            #print entry
            if entry != None:
                incomplete_buffer.append(entry)
                current_chr = entry['chr']
                current_lo = entry['lo']
            else:
                # switch these to different values to flush final genes out to file
                current_chr = ""
                current_lo = 0
            parent_to_write = {}
            buff_to_keep = []

            for buff in incomplete_buffer:
                # if the current element is past your hi and you're a parent element then
                # write yourself and your children down together as an element
                if buff['parent_id'] == -1 and (buff['hi'] < current_lo or buff['chr'] != current_chr):
                    parent_to_write[buff['span_id']] = [buff]
                # if we're a parent but not done yet, stay around until next entry
                elif buff['parent_id'] == -1:
                    buff_to_keep.append(buff)

            # now write that parent and all children
            for buff in incomplete_buffer:
                # if we're writing out your parent, put you in the bucket
                if buff['parent_id'] in parent_to_write:
                    parent_to_write[buff['parent_id']].append(buff)
                # else, keep you around
                elif buff['parent_id'] != -1:
                    buff_to_keep.append(buff)

            #print "buff_to_keep"
            #print buff_to_keep

            incomplete_buffer = buff_to_keep[:]
                
            for gene_obj in parent_to_write:
                parent = parent_to_write[gene_obj][0]
                output_row[bed_col['chr']] = parent['chr']
                output_row[bed_col['lo']] = str(parent['lo'])
                output_row[bed_col['hi']] = str(parent['hi'])
                output_row[bed_col['name']] = parent['name']
                output_row[bed_col['strand']] = parent['strand']
                if "thick_start" in parent:
                    output_row[bed_col['thick_start']] = str(parent['thick_start'])
                if "thick_end" in parent:
                    output_row[bed_col['thick_end']] = str(parent['thick_end'])

                block_sizes = []
                block_starts = []

                output_row[bed_col['block_count']] = str(len(parent_to_write[gene_obj]) - 1)
                for i in range(1, len(parent_to_write[gene_obj])):
                    block_sizes.append(str(parent_to_write[gene_obj][i]['hi'] - parent_to_write[gene_obj][i]['lo']))
                    block_starts.append(str(parent_to_write[gene_obj][i]['lo'] - parent['lo']))

                if output_row[bed_col['block_count']] != 0:
                    output_row[bed_col["block_sizes"]] = ",".join(block_sizes)
                    output_row[bed_col["block_starts"]] = ",".join(block_starts)
                    
                bed_file.write("\t".join(output_row) + "\n")
                output_row = default_bed_line[:]

                #print "incomplete_buffer"
                #print incomplete_buffer
                #print "buff_to_keep"
                #print buff_to_keep
                #print "parent_to_write"
                #print parent_to_write
                #return

            if entry == None:
                return

# this function exports any gri, Spans, or NamedSpans object but not a Genes object
def export_generic_bed(spans, out_name):

    global bed_col
    default_bed_line = ["-", "0", "0", "-", "0", ".", "0", "0", "0,0,0", "0", "0", "0"]

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
                    output_row[bed_col[col]] = str(entry[col])
            bed_file.write("\t".join(output_row)+"\n")


if __name__ == '__main__':
    main()


