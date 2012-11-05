#!/usr/bin/env python

import dxpy
import string
import random
import sys
import argparse

# to find the magic library
sys.path.append('/usr/local/lib/')
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
        except:
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
        subprocess.check_call(" ".join([uncomp_util, "--stdout", input, ">", "uncompressed.bed"]), shell=True)
        return "uncompressed.bed"
    except Exception as e:
        raise dxpy.AppError("Unable to open compressed input for reading: " + str(e))


def detect_type(bed_file):
    #with open(bed_file, "r") as bf:
    num_cols = find_num_columns(bed_file)
    if num_cols == 12:
        bed_type = "genes"
    elif num_cols > 3:
        bed_type = "named_spans"
    else:
        bed_type = "spans"

    return bed_type

# takes the whole bed file and splits into separate files for each track contained in it
def split_on_track(bed_file):
    files = []
    current_filename = id_generator()
    # open bed file
    with open(bed_file, 'rU') as bf:
        curr_file = open(current_filename, "w")
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

def find_num_columns(bed_file):
    num_cols = 0

    with open(bed_file, "rU") as bf:
        line = bf.readline()
        while line != "":
            line = line.split()
            if len(line) > num_cols:
                num_cols = len(line)
            line = bf.readline()

    return num_cols

def import_spans(bed_file, table_name, ref_id):
    num_cols = find_num_columns(bed_file)
    if num_cols < 3:
        raise dxpy.AppError("BED file contains less than the minimum 3 columns.  Invalid BED file.")

    columns = [("chr", "string"),
               ("lo", "int32"),
               ("hi", "int32")]

    column_descs = [dxpy.DXGTable.make_column_desc(name, type) for name, type in columns]
    gri_index = dxpy.DXGTable.genomic_range_index("chr", "lo", "hi")

    with open(bed_file, 'rU') as bed, dxpy.new_dxgtable(column_descs, indices=[gri_index], mode='w') as span:
        span_table_id = span.get_id()

        details = {"original_contigset": dxpy.dxlink(ref_id)}
        span.set_details(details)

        span.add_types(["Spans","gri"])
        span.rename(table_name + " Spans")

        for line in bed:
            if line.startswith("track"):
                continue
            line = line.rstrip("\n")
            line = line.split()
            if len(line) < 3:
                raise dxpy.AppError("Line: "+"\t".join(line)+" in BED file contains less than the minimum 3 columns.  Invalid BED file.")
            line[1] = int(line[1])
            line[2] = int(line[2])

            span.add_row(line)

    return dxpy.dxlink(span.get_id())

def import_named_spans(bed_file, table_name, ref_id):
    num_cols = find_num_columns(bed_file)
    
    possible_columns = [("chr", "string"),
                        ("lo", "int32"),
                        ("hi", "int32"),
                        ("name", "string"),
                        ("score", "float"),
                        ("strand", "string"),
                        ("thick_start", "int32"),
                        ("thick_end", "int32"),
                        ("item_rgb", "string")]

    possible_default_row = ["", 0, 0, "", 0, ".", 0, 0, ""]

    columns = possible_columns[:num_cols]

    if num_cols > len(columns):
        for i in range(len(columns), num_cols):
            columns.append(("BED_column_"+str(i+1), "string"))
            possible_default_row.append("")

    default_row = possible_default_row[:num_cols]

    column_descs = [dxpy.DXGTable.make_column_desc(name, type) for name, type in columns]
    gri_index = dxpy.DXGTable.genomic_range_index("chr", "lo", "hi")

    with open(bed_file, 'rU') as bed, dxpy.new_dxgtable(column_descs, indices=[gri_index], mode='w') as span:
        details = {"original_contigset": dxpy.dxlink(ref_id)}
        span.set_details(details)

        span.add_types(["NamedSpans", "Spans", "gri"])
        span.rename(table_name + " Spans")

        for line in bed:
            row = list(default_row)

            if line.startswith("track"):
                continue
            line = line.rstrip("\n")
            line = line.split()
            # check to see if this is a weird line
            if len(line) < 3:
                raise dxpy.AppError("Line: "+"\t".join(line)+" in BED file contains less than the minimum 3 columns.  Invalid BED file.")

            try:
                row[0] = line[0]
                row[1] = int(line[1])
                row[2] = int(line[2])
                row[3] = line[3]
                # dashes are sometimes used when field is invalid
                if line[4] == "-":
                    line[4] = 0
                row[4] = int(line[4])
                row[5] = line[5]
                # dashes are sometimes used when field is invalid
                if line[6] == "-":
                    line[6] = 0
                row[6] = int(line[6])
                # dashes are sometimes used when field is invalid
                if line[7] == "-":
                    line[7] = 0
                row[7] = int(line[7])
                row[8] = line[8]

            # an index error would come from having fewer columns in a row, which we should handle ok
            except IndexError:
                pass
            # value error when fields are messed up and string gets converted to int, etc.  Throw these out.
            except ValueError:
                continue
            
            span.add_row(row)

    return dxpy.dxlink(span.get_id())


##########named spans###############END

def generate_gene_row(line, block_size, block_start, default_row, parent_id, span_id):
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
        if parent_id == -1:
            row[5] = "transcript"
        else:
            row[5] = "exon"

        # strand
        row[6] = line[5]

        # is_coding
        row[7] = True

        # parent_id
        row[8] = parent_id

        # frame
        row[9] = -1

        # description
        row[10] = ""
        # BED files have no description?

        # score
        row[11] = float(line[4])

        # thickStart
        if parent_id == -1:
            row[12] = int(line[6])
        else:
            # just use lo instead
            row[12] = row[1]

        # thickEnd
        if parent_id == -1:
            row[13] = int(line[7])
        else:
            # just use lo instead
            row[13] = row[2]

        # itemRgb
        row[14] = line[8]

    # a misformed line can have string columns where they should be int
    except ValueError:
        return None
    # if missing columns then also throw out the line
    except IndexError:
        return None

    return row


def import_genes(bed_file, table_name, ref_id):
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
               ("description", "string"),
               ("score", "float"),
               ("thick_start", "int32"),
               ("thick_end", "int32"),
               ("item_rgb", "string")]

    column_descs = [dxpy.DXGTable.make_column_desc(name, type) for name, type in columns]
    gri_index = dxpy.DXGTable.genomic_range_index("chr", "lo", "hi")

    default_row = ["", 0, 0, "", -1, "", ".", False, -1, -1, "", 0, 0, 0, ""]

    with open(bed_file, 'rU') as bed, dxpy.new_dxgtable(column_descs, indices=[gri_index], mode='w') as span:
        span_table_id = span.get_id()

        details = {"original_contigset": dxpy.dxlink(ref_id)}
        span.set_details(details)

        span.add_types(["gri", "Spans", "NamedSpans", "Genes"])
        span.rename(table_name + " Genes")

        current_span_id = 0

        # where the parsing magic happens
        for line in bed:
            if line.startswith("track"):
                continue
            line = line.rstrip("\n")
            row = list(default_row)
            line = line.split()
            if len(line) < 12:
                raise dxpy.AppError("Line: "+"\t".join(line)+" in gene model-like BED file contains less than 12 columns.  Invalid BED file.")

            # add parent gene track
            row = generate_gene_row(line, 0, 0, default_row, -1, current_span_id)
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

                for i in range(blockCount):
                    span.add_row(generate_gene_row(line, blockSizes[i], blockStarts[i], default_row, current_parent_id, current_span_id))
                    current_span_id += 1
            
    return dxpy.dxlink(span.get_id())


parser = argparse.ArgumentParser(description='Import a local BED file as a Spans, NamedSpans, or Genes object.  If multiple tracks exist in the BED file, one object will be created for each.')
parser.add_argument('filename', help='local filename to import')
parser.add_argument('reference', help='ID of ContigSet object (reference) that this BED file annotates')

def import_BED(**args):
    if len(args) == 0:
        cmd_line_args = parser.parse_args(sys.argv[1:])
        args['filename'] = cmd_line_args.filename
        args['reference'] = cmd_line_args.reference

    bed_filename = args['filename']
    reference = args['reference']

    job_outputs = []
    # uncompresses file if necessary.  Returns new filename
    bed_filename_uncomp = unpack( bed_filename )

    current_file = 1

    for import_filename in split_on_track(bed_filename_uncomp):
        if current_file == 1:
            name = bed_filename
        else:
            name = bed_filename+"_"+str(current_file)
        current_file += 1
        bed_type = detect_type(bed_filename)
        if bed_type == "genes":
            print "Importing as Genes Type"
            job_outputs.append(import_genes(import_filename, name, reference))
        elif bed_type == "named_spans":
            print "Importing as NamedSpans Type"
            job_outputs.append(import_named_spans(import_filename, name, reference))
        else:
            print "Importing as Spans Type"
            job_outputs.append(import_spans(import_filename, name, reference))

    print job_outputs
    return job_outputs

def main(**args):
    return import_BED(**args)

if __name__ == '__main__':
    import_BED()
