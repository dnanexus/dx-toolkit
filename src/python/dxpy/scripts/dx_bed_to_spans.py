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

from __future__ import print_function

import dxpy
import json
import string
import random
import sys
import argparse
import os

# to find the magic library
import magic
import subprocess

def id_generator(size=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def unpack(input):
    m = magic.Magic()

    # determine compression format
    try:
        file_type = m.from_file(input)
    except Exception as e:
        raise dxpy.AppError("Error while identifying compression format: " + str(e))
    
    # if we find a tar file throw a program error telling the user to unpack it
    if file_type == 'application/x-tar':
        raise dxpy.AppError("App does not support tar files.  Please unpack.")

    # since we haven't returned, the file is compressed.  Determine what program to use to uncompress
    uncomp_util = None
    if file_type == 'XZ compressed data':
        uncomp_util = 'xzcat'
    elif file_type[:21] == 'bzip2 compressed data':
        uncomp_util = 'bzcat'
    elif file_type[:20] == 'gzip compressed data':
        uncomp_util = 'zcat'
    elif file_type == 'POSIX tar archive (GNU)' or 'tar' in file_type:
        raise dxpy.AppError("Found a tar archive.  Please untar your sequences before importing")
    else:
        # just return input filename since it's already uncompressed
        return input

    if uncomp_util != None:        
        # bzcat does not support -t.  Use non streaming decompressors for testing input
        test_util = None
        if uncomp_util == 'xzcat':
            test_util = 'xz'
        elif uncomp_util == 'bzcat':
            test_util = 'bzip2'
        elif uncomp_util == 'zcat':
            test_util = 'gzip'

        try:
            subprocess.check_call(" ".join([test_util, "-t", input]), shell=True)
        except subprocess.CalledProcessError:
            raise dxpy.AppError("File failed integrity check by "+uncomp_util+".  Compressed file is corrupted.")

    # with that in hand, unzip file.  If we find a tar archive then exit with error.
    try:
        with subprocess.Popen([uncomp_util, input], stdout=subprocess.PIPE).stdout as pipe:
            line = pipe.next()
        uncomp_type = m.from_buffer(line)
    except Exception as e:
        raise dxpy.AppError("Error detecting file format after decompression: " + str(e))

    if uncomp_type == 'POSIX tar archive (GNU)' or 'tar' in uncomp_type:
        raise dxpy.AppError("Found a tar archive after decompression.  Please untar your files before importing")
    elif 'ASCII text' not in uncomp_type:
        raise dxpy.AppError("After decompression found file type other than plain text")
    
    try:
        out_name = id_generator()
        subprocess.check_call(" ".join([uncomp_util, "--stdout", input, ">", out_name]), shell=True)
        return out_name
    except subprocess.CalledProcessError as e:
        raise dxpy.AppError("Unable to open compressed input for reading: " + str(e))

def detect_type(bed_file):
    delimiter = find_delimiter(bed_file)
    with open(bed_file, 'rU') as bf:
        header=""
        while "track" not in header:
            header=bf.readline()
            # if this isn't a browser line either then there isn't a header
            if "browser" not in header:
                break
        if "type=bedDetail" in header:
            print("File is a BED detail file", file=sys.stderr)
            return {"type": "bedDetail", "delimiter": delimiter}
            
    num_cols = find_num_columns(bed_file, delimiter)
    if num_cols >= 12:
        return {"type": "genes", "delimiter": delimiter}
    else:
        return {"type": "spans", "delimiter": delimiter}
    
# takes the whole bed file and splits into separate files for each track contained in it

def split_on_track(bed_file):
    files = []
    current_filename = id_generator()
    # open bed file
    with open(bed_file, 'rU') as bf:
        curr_file = open(current_filename, "w")
        line = bf.readline()
        if line.startswith("browser"):
            line = bf.readline()
        curr_file.write(line)
        line = bf.readline()
        while True:
            if line.startswith("track"):
                # close and save our last track as a new file
                curr_file.close()
                files.append(current_filename)
                # open a new file for the next track
                current_filename = id_generator()
                curr_file = open(current_filename, "w")
            elif line == "":
                curr_file.close()
                files.append(current_filename)
                break
            
            curr_file.write(line)
            line = bf.readline()

    return files

def find_num_columns(bed_file, delimiter="\t"):
    num_cols = 0

    with open(bed_file, "rU") as bf:
        line = bf.readline()
        while line != "":
            if line.startswith("track"):
                line = bf.readline()
            line = line.split(delimiter)
            if len(line) > num_cols:
                num_cols = len(line)
            line = bf.readline()

    print("Found num cols: " + str(num_cols), file=sys.stderr)
    return num_cols

def find_delimiter(bed_file):
    with open(bed_file, "rU") as bf: 
        line = bf.readline()
        if line.startswith("track"):
            line = bf.readline()
        tab_split = line.split("\t")
        
        if len(tab_split) >= 3: 
            print("Bed file is tab delimited", file=sys.stderr)
            return "\t"
        else: 
            space_split = line.split()
            if len(space_split) < 3: 
                raise dxpy.AppError("File is not a valid bed file (neither space delimited nor tab delimited)")
            print("Bed file is space delimited", file=sys.stderr)
            return " "
            
def import_spans(bed_file, table_name, ref_id, file_id, additional_types, property_keys, property_values, tags, isBedDetail, delimiter="\t"):
    num_cols = find_num_columns(bed_file, delimiter)

    # if this is a bedDetail file we should treat the last two columns separately
    if isBedDetail:
        num_cols -= 2
    
    possible_columns = [("chr", "string"),
                        ("lo", "int32"),
                        ("hi", "int32"),
                        ("name", "string"),
                        ("score", "float"),
                        ("strand", "string"),
                        ("thick_start", "int32"),
                        ("thick_end", "int32"),
                        ("item_rgb", "string")]

    bedDetail_columns = [("bedDetail_ID", "string"),
                         ("bedDetail_desc", "string")]

    possible_default_row = ["", 0, 0, "", 0, ".", 0, 0, ""]

    columns = possible_columns[:num_cols]

    if isBedDetail:
        columns.extend(bedDetail_columns)

    if num_cols > len(columns):
        for i in range(len(columns), num_cols):
            columns.append(("BED_column_"+str(i+1), "string"))
            possible_default_row.append("")

    default_row = possible_default_row[:num_cols]

    if isBedDetail:
        default_row.extend(["",""])

    column_descs = [dxpy.DXGTable.make_column_desc(name, type) for name, type in columns]
    
    indices = [dxpy.DXGTable.genomic_range_index("chr","lo","hi", 'gri')]
    for c in columns:
        if "name" in c:
            indices.append(dxpy.DXGTable.lexicographic_index([
                              dxpy.DXGTable.lexicographic_index_column("name", True, False),
                              dxpy.DXGTable.lexicographic_index_column("chr"),
                              dxpy.DXGTable.lexicographic_index_column("lo"),
                              dxpy.DXGTable.lexicographic_index_column("hi")], "search"))
            break
            
    with open(bed_file, 'rU') as bed, dxpy.new_dxgtable(column_descs, indices=indices, mode='w') as span:
        details = {"original_contigset": dxpy.dxlink(ref_id)}
        if file_id != None:
            details["original_file"] = dxpy.dxlink(file_id)
        if len(property_keys) != len(property_values):
            raise dxpy.AppError("Expected each provided property to have a corresponding value.")
        for i in range(len(property_keys)):
            details[property_keys[i]] = property_values[i]    
    
        span.set_details(details)

        span.add_types(["Spans", "gri"])
        span.rename(table_name)

        for line in bed:
            row = list(default_row)

            if line.startswith("track"):
                details = span.get_details()
                details['track'] = line
                span.set_details(details)
                continue
            line = line.rstrip("\n")
            line = line.split(delimiter)
            if isBedDetail:
                # only the first 4 columns are guaranteed to be defined by UCSC
                validate_line(line[:4])
                # save last two fields separately
                bedDetailFields = line[-2:]
                line = line[:-2]     
            else:        
                validate_line(line[:num_cols])
            
            # check to see if this is a weird line
            if len(line) == 0:
                break
            if len(line) < 3:
                raise dxpy.AppError("Line: "+"\t".join(line)+" in BED file contains less than the minimum 3 columns.  Invalid BED file.")

            try:
                row[0] = line[0]
                row[1] = int(line[1])
                row[2] = int(line[2])
                row[3] = line[3]
                # dashes are sometimes used when field is invalid
                if line[4] == "-" or line[4] == ".":
                    line[4] = 0
                row[4] = float(line[4])
                row[5] = line[5]
                # dashes are sometimes used when field is invalid
                if line[6] == "-" or line[6] == ".":
                    line[6] = 0
                row[6] = int(line[6])
                # dashes are sometimes used when field is invalid
                if line[7] == "-" or line[7] == ".":
                    line[7] = 0
                row[7] = int(line[7])
                row[8] = line[8]

            # an index error would come from having fewer columns in a row, which we should handle ok
            except IndexError:
                pass
            # value error when fields are messed up and string gets converted to int, etc.  Throw these out.
            except ValueError:
                continue
            
            if isBedDetail:
                # add these in at the end if we have a bedDetail file
                row[num_cols] = bedDetailFields[0]
                row[num_cols+1] = bedDetailFields[1]
            
            span.add_row(row)

        span.flush()

    return dxpy.dxlink(span.get_id())


##########named spans###############END

def generate_gene_row(line, block_size, block_start, span_type, default_row, parent_id, span_id):
    row = list(default_row)

    try:
        # chr
        row[0] = line[0]

        # lo
        row[1] = int(line[1])
        # if we're a child, add our offset
        if parent_id != -1:
            row[1] += block_start

        # hi
        # if we're a child, just add size to our start
        if parent_id != -1:
            row[2] = row[1] + block_size
        else:
            row[2] = int(line[2])        

        # name
        row[3] = line[3]

        # span_id
        row[4] = span_id

        # type
        row[5] = span_type

        # strand
        row[6] = line[5]

        # is_coding
        if span_type == "CDS":
            row[7] = True
        elif "UTR" in span_type:
            row[7] = False
        else:
            row[7] = False

        # parent_id
        row[8] = parent_id

        # frame
        row[9] = -1

        # description
        row[10] = "\t".join(line[12:])
        # BED files have no description?

    # a misformed line can have string columns where they should be int
    except ValueError:
        return None
    # if missing columns then also throw out the line
    except IndexError:
        return None

    return row


def import_genes(bed_file, table_name, ref_id, file_id, additional_types, property_keys, property_values, tags, delimiter="\t"):
    # implement BED importing from this format:
    # http://genome.ucsc.edu/FAQ/FAQformat.html#format1

    columns = [("chr", "string"),
               ("lo", "int32"),
               ("hi", "int32"),
               ("name", "string"),
               ("span_id", "int32"),
               ("type", "string"),
               ("strand", "string"),
               ("is_coding", "boolean"),
               ("parent_id", "int32"),
               ("frame", "int16"),
               ("description", "string")]

    column_descs = [dxpy.DXGTable.make_column_desc(name, type) for name, type in columns]
    
    indices = [dxpy.DXGTable.genomic_range_index("chr","lo","hi", 'gri'), 
               dxpy.DXGTable.lexicographic_index([
                  dxpy.DXGTable.lexicographic_index_column("name", True, False),
                  dxpy.DXGTable.lexicographic_index_column("chr"),
                  dxpy.DXGTable.lexicographic_index_column("lo"),
                  dxpy.DXGTable.lexicographic_index_column("hi"),
                  dxpy.DXGTable.lexicographic_index_column("type")], "search")]

    default_row = ["", 0, 0, "", -1, "", ".", False, -1, -1, ""]

    with open(bed_file, 'rU') as bed, dxpy.new_dxgtable(column_descs, indices=indices, mode='w') as span:
        span_table_id = span.get_id()

        details = {"original_contigset": dxpy.dxlink(ref_id)}
        if file_id != None:
            details["original_file"] = dxpy.dxlink(file_id)
        if len(property_keys) != len(property_values):
            raise dxpy.AppError("Expected each provided property to have a corresponding value.")
        for i in range(len(property_keys)):
            details[property_keys[i]] = property_values[i]
        span.set_details(details)

        span.add_types(["gri", "Genes"])
        span.rename(table_name)

        current_span_id = 0

        # where the parsing magic happens
        for line in bed:
            if line.startswith("track"):
                details = span.get_details()
                details['track'] = line
                span.set_details(details)
                continue
            line = line.rstrip("\n")
            row = list(default_row)
            line = line.split(delimiter)
            validate_line(line)
            if len(line) < 12:
                raise dxpy.AppError("Line: "+"\t".join(line)+" in gene model-like BED file contains less than 12 columns.  Invalid BED file.")

            # add parent gene track
            row = generate_gene_row(line, 0, 0, "transcript", default_row, -1, current_span_id)
            if row != None:
                span.add_row(row)
                current_parent_id = current_span_id
                current_span_id += 1          
                
                # add all children
                blockCount = int(line[9])
                line[10] = line[10].rstrip(",").split(",")
                blockSizes = [int(line[10][n]) for n in range(blockCount)]
                line[11] = line[11].rstrip(",").split(",")
                blockStarts = [int(line[11][n]) for n in range(blockCount)]

                gene_lo = int(line[1])
                gene_hi = int(line[2])

                # set thick* to be within the gene if outside
                thickStart = min(max(int(line[6]), gene_lo), gene_hi)
                thickEnd = max(min(int(line[7]), gene_hi), gene_lo)
                
                for i in range(blockCount):
                    # look to thickStart and thickEnd to get information about the type of this region
                    # if thick* are the same or cover the whole transcript then we ignore them
                    # else, we partition the exons into CDS and UTR based on their boundaries
                    if thickStart == thickEnd or (thickStart == gene_lo and thickEnd == gene_hi):
                        span.add_row(generate_gene_row(line, 
                                                       blockSizes[i], 
                                                       blockStarts[i], 
                                                       "exon", 
                                                       default_row, 
                                                       current_parent_id, 
                                                       current_span_id))
                        current_span_id += 1
                    else:
                        exon_lo = int(line[1])+blockStarts[i]
                        exon_hi = int(exon_lo+blockSizes[i])

                        # we're all UTR if we enter either of these
                        if (exon_hi <= thickStart and line[5] == '+') or (exon_lo >= thickEnd and line[5] == '-'):
                            span.add_row(generate_gene_row(line, 
                                                           blockSizes[i], 
                                                           blockStarts[i], 
                                                           "5' UTR", 
                                                           default_row, 
                                                           current_parent_id, 
                                                           current_span_id))
                            current_span_id += 1
                        elif (exon_hi <= thickStart and line[5] == '-') or (exon_lo >= thickEnd and line[5] == '+'):
                            span.add_row(generate_gene_row(line, 
                                                           blockSizes[i], 
                                                           blockStarts[i], 
                                                           "3' UTR", 
                                                           default_row, 
                                                           current_parent_id, 
                                                           current_span_id))
                            current_span_id += 1

                        # if this is true then we overlap CDS partially or completely
                        elif (exon_lo < thickEnd and exon_hi > thickStart):
                            # entirely contained
                            if exon_lo >= thickStart and exon_hi <= thickEnd:
                                span.add_row(generate_gene_row(line, 
                                                               blockSizes[i], 
                                                               blockStarts[i], 
                                                               "CDS", 
                                                               default_row, 
                                                               current_parent_id, 
                                                               current_span_id))
                                current_span_id += 1
                            else:
                                # left portion is UTR
                                if exon_lo < thickStart:
                                    if line[5] == '+':
                                        UTR_type = "5' UTR"
                                    else:
                                        UTR_type = "3' UTR"
                                    UTR_size = (min(blockSizes[i], thickStart - exon_lo))
                                    span.add_row(generate_gene_row(line, 
                                                                   UTR_size, 
                                                                   blockStarts[i], 
                                                                   UTR_type,
                                                                   default_row, 
                                                                   current_parent_id, 
                                                                   current_span_id))
                                    current_span_id += 1

                                # CDS portion
                                CDS_size = blockSizes[i] - (max(exon_lo, thickStart) - exon_lo)
                                CDS_size -= (exon_hi - min(exon_hi, thickEnd))
                                CDS_start = (max(exon_lo, thickStart) - exon_lo) + blockStarts[i]
                                span.add_row(generate_gene_row(line, 
                                                               CDS_size, 
                                                               CDS_start, 
                                                               "CDS",
                                                               default_row, 
                                                               current_parent_id, 
                                                               current_span_id))
                                current_span_id += 1

                                # right portion is UTR
                                if exon_hi > thickEnd:
                                    if line[5] == '+':
                                        UTR_type = "3' UTR"
                                    else:
                                        UTR_type = "5' UTR"
                                    UTR_size = (min(blockSizes[i], exon_hi - thickEnd))
                                    UTR_start = blockStarts[i] + thickEnd - exon_lo
                                    span.add_row(generate_gene_row(line, 
                                                                   UTR_size, 
                                                                   UTR_start, 
                                                                   UTR_type,
                                                                   default_row, 
                                                                   current_parent_id, 
                                                                   current_span_id))
                                    current_span_id += 1

    return dxpy.dxlink(span.get_id())


parser = argparse.ArgumentParser(description='Import a local BED file as a Spans or Genes object.  If multiple tracks exist in the BED file, one object will be created for each.')
parser.add_argument('filename', help='local filename to import')
parser.add_argument('reference', help='ID of ContigSet object (reference) that this BED file annotates')
parser.add_argument('--file_id', default=None, help='the DNAnexus file-id of the original file. If provided, a link to this id will be added in the type details')
parser.add_argument('--additional_type', default=[], action='append', help='This will be added to the list of object types (in addition to the type \"Spans\", or \"Genes\" which is added automatically)')
parser.add_argument('--property_key', default=[], action='append', help='The keys in key-value pairs that will be added to the details of the object. The nth property key will be paired with the nth property value. The number of keys must equal the number of values provided')
parser.add_argument('--property_value', default=[], action='append', help='The values in key-value pairs that will be added to the details of the object. The nth property key will be paired with the nth property value. The number of keys must equal the number of values provided')
parser.add_argument('--tag', default=[], action='append', help='"A set of tags (string labels) that will be added to the resulting Variants table object. (You can use tags and properties to better describe and organize your data)')


def import_BED(**args):
    if len(args) == 0:
        cmd_line_args = parser.parse_args(sys.argv[1:])
        args['filename'] = cmd_line_args.filename
        args['reference'] = cmd_line_args.reference
        args['file_id'] = cmd_line_args.file_id
        args['additional_type'] = cmd_line_args.additional_type
        args['property_key'] = cmd_line_args.property_key
        args['property_value'] = cmd_line_args.property_value
        args['tag'] = cmd_line_args.tag

    bed_filename = args['filename']
    reference = args['reference']
    file_id = args['file_id']
    additional_types = args['additional_type']
    property_keys = args['property_key']
    property_values = args['property_value']
    tags = args['tag']

    job_outputs = []
    # uncompresses file if necessary.  Returns new filename
    bed_filename_uncomp = unpack( bed_filename )

    current_file = 1

    for import_filename in split_on_track(bed_filename_uncomp):
        try:
            bed_basename = os.path.basename(bed_filename)
        except:
            bed_basename = bed_filename
        if current_file == 1:
            name = bed_basename
        else:
            name = bed_basename+"_"+str(current_file)
        current_file += 1
        bed_type = detect_type(import_filename)["type"]
        delimiter = detect_type(import_filename)["delimiter"]

        print("Bed type is : " + bed_type, file=sys.stderr)
        if bed_type == "genes":
            print("Importing as Genes Type", file=sys.stderr)
            job_outputs.append(import_genes(import_filename, name, reference, file_id, additional_types, property_keys, property_values, tags, delimiter))
        elif bed_type == "spans" or bed_type == "bedDetail":
            print("Importing as Spans Type", file=sys.stderr)
            if bed_type == "bedDetail":
                print("input file is in 'bedDetails' format...", file=sys.stderr)
                bedDetail=True
            else:
                bedDetail=False
            job_outputs.append(import_spans(import_filename, name, reference, file_id, additional_types, property_keys, property_values, tags, bedDetail, delimiter))
        else:
            raise dxpy.AppError("Unable to determine type of BED file")

        subprocess.check_call(" ".join(["rm", import_filename]), shell=True)

    if(bed_filename != bed_filename_uncomp):
        subprocess.check_call(" ".join(["rm", bed_filename_uncomp]), shell=True)

    print(json.dumps(job_outputs))
    return job_outputs

def validate_line(line):
    line_str = "\t".join(line)
    entries = list(line)
    
    if len(entries) > 1:
        try:
            if int(entries[1]) < 0:
                raise dxpy.AppError("The start position for one entry was unexpectedly negative. \nOffending line_str: " + line_str + "\nOffending value: " + str(entries[1]))
        except ValueError:
            raise dxpy.AppError("One of the start values could not be translated to an integer. " + "\nOffending line_str: " + line_str + "\nOffending value: " + str(entries[1]))
    
    if len(entries) > 2:    
        try:
            if int(entries[2]) < 0:
                raise dxpy.AppError("The end position for one entry was unexpectedly negative. \nOffending line_str: " + line_str + "\nOffending value: " + str(entries[2]))
        except ValueError:
            raise dxpy.AppError("One of the end values could not be translated to an integer. " + "\nOffending line_str: " + line_str + "\nOffending value: " + str(entries[2]))
        
    if len(entries) > 4:    
        try:
            if entries[4] != "." and entries[4] != "-":
                float(entries[4])
        except ValueError:
            raise dxpy.AppError("One of the score values for one entry could not be translated to a number. " + "\nOffending line_str: " + line_str + "\nOffending value: " + str(entries[4]))
        
    if len(entries) > 5:
        if entries[5] != "+" and entries[5] != "-" and entries[5] != ".":
            raise dxpy.AppError("The strand indicated for an element was not \"+\", \"-\", or \".\"" + "\nOffending line_str: " + line_str + "\nOffending value: " + str(entries[5]))
    
    if len(entries) > 6:
        try:
            if entries[6] != "." and entries[6] != "-":
                if int(entries[6]) < 0:
                    raise dxpy.AppError("The thickStart position for one entry was unexpectedly negative. \nOffending line_str: " + line_str + "\nOffending value: " + str(entries[6]))
        except ValueError:
            raise dxpy.AppError("One of the thickStart values could not be translated to an integer. " + "\nOffending line_str: " + line_str + "\nOffending value: " + str(entries[6]))
    
    if len(entries) > 7:    
        try:
            if entries[7] != "." and entries[7] != "-":
                if int(entries[7]) < 0:
                    raise dxpy.AppError("The thickEnd position for one entry was unexpectedly negative. \nOffending line_str: " + line_str + "\nOffending value: " + str(entries[7]))
        except ValueError:
            raise dxpy.AppError("One of the thickEnd values could not be translated to an integer. " + "\nOffending line_str: " + line_str + "\nOffending value: " + str(entries[7]))
    
    if len(entries) > 9:
        try:
            if int(entries[9]) < 0:
                raise dxpy.AppError("The number of exons (blockCount) for one entry was unexpectedly negative. \nOffending line_str: " + line_str + "\nOffending value: " + str(entries[9]))
        except ValueError:
            raise dxpy.AppError("One of the thickEnd values could not be translated to an integer. " + "\nOffending line_str: " + line_str + "\nOffending value: " + str(entries[9]))
    
    if len(entries) > 10:    
        try:
            entries[10] = entries[10].rstrip(",").split(",")
            blockStarts = [int(entries[10][n]) for n in range(int(entries[9]))]
        except:
            raise dxpy.AppError("Could not parse the blockSizes entry as a comma-separated list of integers \nOffending line_str: " + line_str + "\nOffending value: " + str(entries[10]))
        
    if len(entries) > 11:
        try:
            entries[11] = entries[11].rstrip(",").split(",")
            blockStarts = [int(entries[11][n]) for n in range(int(entries[9]))]
        except:
            raise dxpy.AppError("Could not parse the blockStarts entry as a comma-separated list of integers \nOffending line_str: " + line_str + "\nOffending value: " + str(entries[11]))


def main(**args):
    import_BED(**args)

if __name__ == '__main__':
    import_BED()
