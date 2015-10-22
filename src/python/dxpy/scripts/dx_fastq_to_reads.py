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

import sys
import json
import logging
import subprocess
import re
import string
import argparse
import ast

sys.path.append('/usr/local/lib/')
import magic
import dxpy
from dxpy.compat import USING_PYTHON2

parser = argparse.ArgumentParser(description='Import local FASTQ file(s) as a Reads object.')
parser.add_argument('--name', help='ID of ContigSet object (reference) that this BED file annotates')
parser.add_argument('file', help='The FASTQ/FASTA file to be imported.')
parser.add_argument('--file2', help='For paired reads: The second FASTQ/FASTA file for the read mates.')
parser.add_argument('--qual', help='For colorspace reads with a separate quality file: The .QV.qual file containing the quality values for the color space calls.')
parser.add_argument('--qual2', help='For paired colorspace reads with a separate quality file: The second .QV.qual file for the read mates.')
parser.add_argument('--tags', help='"A set of tags (string labels) that will be added to the resulting Reads table object. (You can use tags and properties to better describe and organize your data).  Entries must be separated by commas (--tags "tag1,tag2,tag3")')
parser.add_argument('--properties', help='A set of properties (key/value pairs) that will be added to the resulting Reads table object. (You can use tags and properties to better describe and organize your data).  This will be interpreted as a JSON hash.  For exmple: "{"foo":"bar,"foo2":"bar2"}".')
parser.add_argument('--discard_names', action="store_true", help='If selected, the read names (that appear in the FASTA/FASTQ file) will not be carried forward into the Reads table object. If read names are not important to you, this option will save some storage space.')
parser.add_argument('--discard_qualities', action="store_true", help='If selected, the quality values (that appear in the FASTQ file) will not be carried forward into the Reads table object.')
parser.add_argument('--qual_encoding', default='auto', help='FASTQ files may contain quality values using one of two different encodings, where a quality value of 0 is represented by the ASCII character 33 or 64. Set this option to "phred33" or "phred64" if you know which encoding your files are using, or leave this to "auto" if you want the encoding to be detected automatically in a heuristic way.')
parser.add_argument('--pair_orientation', help="For paired reads: The expected relative orientation of the two mates, if known. One of 'FR', 'FF', 'RF', or 'RR' ('F' stands for forward strand, 'R' stands for reverse strand; see https://wiki.dnanexus.com/Types/Reads for more).")
parser.add_argument('--pair_min_dist', help='For paired reads: Smallest expected fragment length (in bp), if known.')
parser.add_argument('--pair_max_dist', help='For paired reads: Largest expected fragment length (in bp), if known.')
parser.add_argument('--pair_avg_dist', help='For paired reads: Average fragment length (in bp), if known.')
parser.add_argument('--pair_std_dev_dist', help='For paired reads: Standard deviation of fragment length (in bp), if known.')

args = {}

# cutoff for declaring phred64 instead of phred33
THRESHOLD = 75

# num to average - bases to average at beginning of reads to estimate quality encoding
NUM_TO_AVERAGE = 10
READS_TO_ESTIMATE = 10000
MAX_READ_NAME_LEN = 255

# allowed_qual_chars = ''.join(chr(i) for i in range(33, 127))
# Use /[^...]+/ to search for any character other than the permitted ones (ASCII 33 through 126).
disallowed_qual_chars_re = re.compile('[^!-~]')

disallowed_colorspace_chars_re = re.compile('[^ACGTN0123acgtn.-]+')
disallowed_letterspace_chars_re = re.compile('[^ACGTNacgtn.-]+')

job = {}

def unpack_and_open(input):
    m = magic.Magic()

    # determine compression format
    try:
        file_type = m.from_file(input)
    except:
        raise dxpy.AppError("Unable to identify compression format")

    
    # if we find a tar file throw a program error telling the user to unpack it
    if file_type == 'application/x-tar':
        raise dxpy.AppError("Program does not support tar files.  Please unpack.")

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
        # Assume is uncompressed.  Open the file and return a handle to it
        try:
            return open(input)
        except:
            raise dxpy.AppError("Detected uncompressed input but unable to open file. File may be corrupted.")

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

    # with that in hand, open file for reading.  If we find a tar archive then exit with error.
    try:
        with subprocess.Popen([uncomp_util, input], stdout=subprocess.PIPE).stdout as pipe:
            line = pipe.next()
        uncomp_type = m.from_buffer(line)
    except:
        raise dxpy.AppError("Error detecting file format after decompression")

    if uncomp_type == 'POSIX tar archive (GNU)' or 'tar' in uncomp_type:
        raise dxpy.AppError("Found a tar archive after decompression.  Please untar your sequences before importing")
    elif 'ASCII text' not in uncomp_type:
        raise dxpy.AppError("After decompression found file type other than plain text")
    
    try:
        return subprocess.Popen([uncomp_util, input], stdout=subprocess.PIPE).stdout
    except:
        raise dxpy.AppError("Unable to open compressed input for reading")
    

def remove_file_type( name ):

    suffix = ["fastq","fasta","fq","fa","csfastq","gz","xz","bzip","bz2","csfasta", "csfq","csfa"]

    rm_suffix = True

    while rm_suffix:
        rm_suffix = False

        for s in suffix:
            m = re.search( "\."+s+"$", name )
            if m != None:
                name = name[:m.start()]
                rm_suffix = True

    return name


def estimateQualEncoding(fastq_filename, basesToEstimate, readsToEstimate, threshold):
    i = 0

    # long int to avoid overruns in big reads files (NB: ints are longs in Python 3 but not 2)
    if USING_PYTHON2:
        avgQual = long(0)
    else:
        avgQual = 0
    encoding = ""
    numLines = 0

    with unpack_and_open(fastq_filename) as fastq_file:
        while numLines < readsToEstimate:
            try:
                line = fastq_file.next()

                currentLine = i % 4
            
                # quality line
                if currentLine == 3:
                    numLines += 1
                    
                    for base in range(basesToEstimate):
                        try:
                            base_num = ord(line[base])
                            
                            if base_num < 64:
                                print("found Qual less than 64 -> encoding phred33")
                                encoding = "phred33"
                                return encoding
                            avgQual += base_num
                        except:
                            break
                i += 1

            except StopIteration:
                #print("EOF!")
                break

    avgQual /= numLines * basesToEstimate

    #print("average qual value = ", avgQual)

    if avgQual > threshold:
        print("estimating as phred64 and converting to phred33...")
        encoding = "phred64"
    else:
        print("estimating as phred33")
        encoding = "phred33"

    return encoding

def convert_qual(qualString, qual_encode):
    convQualString = ''

    if qual_encode == 'phred64':
        #convert to phred33 do this by subtracting the difference in ASCII offsets
        #should be scaling values here? Lose some top end values by doing this
        for i in range(len(qualString)):
            convQualString += chr(ord(qualString[i]) - 31)
    elif qual_encode == 'qual_file':
        convQualString = ''.join(chr(int(i) + 33) for i in qualString.strip(' ').split(' '))
    elif qual_encode == 'phred33':
        convQualString = qualString
    else:
        raise dxpy.AppError("Unknown quality encoding.  Supported encodings are Phred33 and Phred64.")

    return convQualString

def sniff_fastq(filename):
    with unpack_and_open(filename) as fh:
        header, seq = fh.readline(), fh.readline()
        is_fasta = True if header[0] == '>' else False
        is_colorspace = True if re.match("^[ATGCN][0123.]+$", seq) else False
        
        if args['qual_encoding'] == 'auto' and not (is_fasta or args['discard_qualities']):
            qual_encoding = estimateQualEncoding(filename, NUM_TO_AVERAGE, READS_TO_ESTIMATE, THRESHOLD)
        else:
            qual_encoding = args['qual_encoding']
        
        logging.debug("Detected: fasta={f}, colorspace={c}, qual_encoding={q}".format(f=is_fasta, c=is_colorspace, q=qual_encoding))

    return is_fasta, is_colorspace, qual_encoding

#########################################

class get_read( object ):

    def __init__(self, fastqa_iter, qual_iter, is_fasta, is_colorspace, qual_encoding ):
        # capture these for use in iterator
        self.fastqa_iter = fastqa_iter
        self.qual_iter = qual_iter
        self.is_fasta = is_fasta
        self.is_colorspace = is_colorspace
        self.qual_encoding = qual_encoding
        self.saved_name = None
        self.saved_qual_name = None

    def __iter__(self):

        global args

        while True:
            # init all to none to know where StopIteration occurs
            read_name, seq, qual, line = None, None, None, None

            # READ NAME #################
            if self.saved_name != None:
                read_name = self.saved_name
                self.saved_name = None
            else:
                read_name = self.fastqa_iter.next().rstrip("\n")

            if len(read_name) > MAX_READ_NAME_LEN:
                raise dxpy.AppError("Name of read exceeded limit of "+str(MAX_READ_NAME_LEN)+" characters.  Name found is: "+read_name)
            if len(read_name) == 0:
                raise dxpy.AppError("Expecting read name, found empty line.")

            if read_name[0] == '+' and self.is_fasta:
                raise dxpy.AppError("Found read with quality scores while expecting all reads to not have quality scores.  Program does not support mixed FASTA/FASTQ")

            if read_name[0] != '@' and read_name[0] != '>':
                raise dxpy.AppError("Expecting '@' or '>' as first character of name line for name: "+read_name)
            #############################


            # SEQ #######################

            if self.is_fasta:
                # pull first line and assign to sequence
                line = "foo"
                seq = ""
                while line[0] != '>':
                    try:
                        line = self.fastqa_iter.next().rstrip("\n")
                        if line[0] != '>':
                            seq += line
                    except StopIteration:
                        if len(seq) == 0:
                            raise dxpy.AppError("Detected truncated file.  Last read in file is incomplete.")
                        line = None
                        break

                self.saved_name = line

            else:
                # just pull one line if it's FASTQ since that format does not wrap (almost always?)
                try:
                    seq = self.fastqa_iter.next().rstrip("\n")
                except StopIteration:
                    raise dxpy.AppError("Detected truncated file.  Last read in file is incomplete.")

            if len(seq) == 0:
                raise dxpy.AppError("Read with name "+read_name+" has empty sequence")

            if self.is_colorspace:
                if disallowed_colorspace_chars_re.search(seq):
                    raise dxpy.AppError("Unsupported colorspace sequence character.  Valid characters are ACGTN0123acgtn.-. Found in read: "+read_name)
            else:
                if disallowed_letterspace_chars_re.search(seq):
                    raise dxpy.AppError("Unsupported letterspace sequence character. Seq is: "+seq+" Valid characters are ACGTNacgtn.-. Found in read: "+read_name)

            #############################

            # QUAL ######################

            # get qual and deal with it, if discarding, then leave alone

            qual = None
            if self.is_fasta:
                if self.qual_iter != None and not args['discard_qualities']:
                    # eat the comment line
                    if self.saved_qual_name == None:
                        self.saved_qual_name = self.qual_iter.next()
                    # pull qual line till we find next name
                    q_line = 'foo'
                    qual = ""
                    while q_line[0] != '>':
                        try:
                            q_line = self.qual_iter.next().rstrip("\n")
                        except StopIteration:
                            q_line = None
                            break

                        if q_line[0] != '>':
                            qual += q_line
                        else:
                            saved_qual_name = q_line
                            break

                    qual = convert_qual(qual, qual_encode='qual_file')
            else:
                try:
                    qual_name = self.fastqa_iter.next().rstrip("\n")
                except StopIteration:
                    raise dxpy.AppError("Detected truncated file.  Last read in file is incomplete.")
                    
                if len(qual_name) == 0:
                    raise dxpy.AppError("Unexpected blank line in read: "+read_name)
                elif qual_name[0] == '>' or qual_name[0] == '@':
                    raise dxpy.AppError("Found read without quality scores while expecting all reads to have quality scores.  Program does not support mixed FASTA/FASTQ")
                elif qual_name[0] != '+':
                    raise dxpy.AppError("Expecting comment field between sequence and quality in read: "+read_name)

                try:
                    qual = self.fastqa_iter.next().rstrip("\n")
                except StopIteration:
                    raise dxpy.AppError("Detected truncated file.  Last read in file is incomplete.")

                if not args['discard_qualities']:
                    # if not all(ord(c) < 128 and ord(c) > 32 for c in qual):
                    # ord() on every char is very slow. Use regexp instead.
                    if disallowed_qual_chars_re.search(qual):
                        raise dxpy.AppError("Quality value encoded outside allowed range in read: "+read_name)
                    if len(qual) != len(seq):
                        raise dxpy.AppError("Read with different quality and sequence lengths.  Read name is: "+read_name)

                    if self.is_colorspace: # Strip the quality score of the primer letter
                        qual = qual[1:]
                    elif self.qual_encoding != 'phred33':
                        qual = convert_qual(qual, self.qual_encoding)

            ##############################

            yield read_name, seq, qual
        '''
        except StopIteration:
            if self.saved_name != None:
                raise dxpy.AppError("Unexpected end of file")
            elif self.is_fasta:
                # we read off the end of a FASTA looking for the end of the sequence
                yield read_name, seq, qual
        '''
#############################################################

def iterate_reads(fastqa1_filename, fastqa2_filename, qual1_filename, qual2_filename, is_fasta, is_colorspace, qual_encoding):
    fastqa1_iter = unpack_and_open(fastqa1_filename).__iter__()
    fastqa2_iter, qual1_iter, qual2_iter = None, None, None
    if fastqa2_filename != None:
        fastqa2_iter = unpack_and_open(fastqa2_filename).__iter__()
    if qual1_filename != None:
        qual1_iter = unpack_and_open(qual1_filename).__iter__()
    if qual2_filename != None:
        qual2_iter = unpack_and_open(qual2_filename).__iter__()

    read_iter = get_read(fastqa1_iter, qual1_iter, is_fasta, is_colorspace, qual_encoding).__iter__()
    if fastqa1_filename != None:
        read_iter2 = get_read(fastqa2_iter, qual2_iter, is_fasta, is_colorspace, qual_encoding).__iter__()

    try:
        while True:
            temp = read_iter.next()
            name1 = temp[0]
            seq1 = temp[1]
            qual1 = temp[2]
            #name1, seq1, qual1 = read_iter.next()
            name2, seq2, qual2 = None, None, None
            if fastqa2_filename != None:
                name2, seq2, qual2 = read_iter2.next()
            yield name1, seq1, qual1, name2, seq2, qual2
    except StopIteration:
        # check to make sure all files we're reading from are all finished at the same time
        for file_iter in fastqa1_iter, fastqa2_iter, qual1_iter, qual2_iter:
            if file_iter != None:
                try:
                    line = file_iter.next().rstrip("\n")
                    raise dxpy.AppError("Number of reads in each file must be equal")

                except StopIteration:
                    pass

def import_reads(job_input):

    global args

    if job_input == None:
        temp = vars(parser.parse_args(sys.argv[1:]))
        for key in temp:
            if temp[key] != None:
                if key == 'tags':
                    args[key] = temp[key].split(",")
                    # remove whitespace around tags
                    for i in range(len(args[key])):
                        args[key][i] = args[key][i].rstrip().lstrip()
                elif key == 'properties':
                    try:
                        args[key] = ast.literal_eval(temp[key])
                    except SyntaxError:
                        raise dxpy.AppError("Cannot parse properties: " + temp[key])
                else:
                    args[key] = temp[key]

    else:
        args = job_input

    print(args)
    
    if 'file2' in args:
        paired = True
    else:
        paired = False
   
    is_fasta, is_colorspace, qual_encoding = sniff_fastq(args["file"])
    
    if is_fasta == False and ('qual' in args or 'qual2' in args):
        raise dxpy.AppError("Qualities supplied twice:  FASTQ format file found along with separate quality file.")

    if is_fasta and 'qual' not in args:
        reads_have_qualities = False
    else:
        reads_have_qualities = True

    table_columns = []

    if not args['discard_names']:
        table_columns.append(("name", "string"))
        if paired:
            table_columns.append(("name2", "string"))

    table_columns.append(("sequence", "string"))
    if paired:
        table_columns.append(("sequence2", "string"))
    if reads_have_qualities and not args['discard_qualities']:
        table_columns.append(("quality", "string"))
        if paired:
            table_columns.append(("quality2", "string"))
    
    column_descriptors = [dxpy.DXGTable.make_column_desc(name, type) for name, type in table_columns]
    logging.info("Constructed table schema:  %s" % column_descriptors)

    readsTable = dxpy.new_dxgtable(column_descriptors)
    if is_colorspace:
        readsTable.add_types(['ColorReads', 'Reads'])
        details =  readsTable.get_details()
        details['sequence_type'] = "color"
        readsTable.set_details(details)
    else:
        readsTable.add_types(['LetterReads', 'Reads'])

    if 'tags' in args:
        readsTable.add_tags(args['tags'])
    if 'properties' in args:
        readsTable.set_properties(args['properties'])


    if paired:
        details = readsTable.get_details()
        details['paired'] = True

        # TODO implement estimate paired read distance

        # otherwise take the values they give
        if 'pair_orientation' in args:
            details['pair_orientation'] = args['pair_orientation']
        if 'pair_min_dist' in args:
            details['pair_min_dist'] = args['pair_min_dist']
        if 'pair_max_dist' in args:
            details['pair_max_dist'] = args['pair_max_dist']
        if 'pair_avg_dist' in args:
            details['pair_avg_dist'] = args['pair_avg_dist']
        if 'pair_std_dev_dist' in args:
            details['pair_std_dev_dist'] = args['pair_std_dev_dist']

        readsTable.set_details(details)


    # generate translation table for enforcing string syntax
    to_replace=''.join([".","-"])
    N=''.join(['N'] * len(to_replace))
    transtable = string.maketrans( to_replace, N )

    for name1, seq1, qual1, name2, seq2, qual2 in iterate_reads(fastqa1_filename=args["file"],
                                                                fastqa2_filename=args["file2"] if 'file2' in args else None,
                                                                qual1_filename=args["qual"] if 'qual' in args else None,
                                                                qual2_filename=args["qual2"] if 'qual2' in args else None,
                                                                is_fasta=is_fasta,
                                                                is_colorspace=is_colorspace,
                                                                qual_encoding=qual_encoding):
        
        row = []
        # add name
        if args['discard_names'] == False:
            if is_fasta and name1[0] == '>':
                name1 = name1[1:]
            elif name1[0] == '@':
                name1 = name1[1:]
            row.append(name1)
            if paired:
                if is_fasta and name2[0] == '>':
                    name2 = name2[1:]
                elif name2[0] == '@':
                    name2 = name2[1:]
                row.append(name2)
    
        # enforce UPPERCASE
        seq1 = seq1.upper()
        if paired:
            seq2 = seq2.upper()
        
        # translate bad chars into Ns
        if not is_colorspace:
            seq1 = seq1.translate(transtable)
            if paired:
                seq2 = seq2.translate(transtable)

        # add seq
        row.append(seq1)
        if paired:
            row.append(seq2)

        # add quals
        if reads_have_qualities and not args['discard_qualities']:
            row.append(qual1)
            if paired:
                row.append(qual2)
        
        readsTable.add_row(row)

    # print out table ID
    print(json.dumps({'table_id': readsTable.get_id()}))

    if 'name' in args:
        tableName = args['name']
    else:
        tableName = remove_file_type(args['file']) + " reads"
        
    readsTable.rename(tableName)

    # set link to original FASTQ file object
    details = readsTable.get_details()

    if 'file_link' in args:
        details['original_files'] = [ args['file_link'] ]
        if 'file2' in args:
            details['original_files'].append(args['file2_link'])
        if 'qual' in args:
            details['original_files'].append(args['qual_link'])
            if 'file2' in args:
                assert('qual2' in args)
                details['original_files'].append(args['qual2_link'])

    readsTable.set_details(details)

    readsTable.close()

    # place table in output
    return {'reads': dxpy.dxlink(readsTable.get_id())}

def main(**kwargs):
    import_reads(None)

if __name__ == '__main__':
    import_reads(None)
