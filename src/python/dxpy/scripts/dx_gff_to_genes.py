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
import sys
import re
import argparse
import subprocess

import magic

parser = argparse.ArgumentParser(description='Import a local GFF file as a Spans or Genes object.')
parser.add_argument('fileName', help='local fileName to import')
parser.add_argument('reference', help='ID of ContigSet object (reference) that this GFF file annotates')
parser.add_argument('--outputName', dest='outputName', default='', help='what to name the output. if none provided, the name of the input file will be used with gff file extension removed.')
parser.add_argument('--file_id', default=None, help='the DNAnexus file-id of the original file. If provided, a link to this id will be added in the type details')
parser.add_argument('--additional_type', default=[], action='append', help='This will be added to the list of object types (in addition to the type \"Spans\" or \"Genes\", which is added automatically')
parser.add_argument('--property_key', default=[], action='append', help='The keys in key-value pairs that will be added to the details of the object. The nth property key will be paired with the nth property value. The number of keys must equal the number of values provided')
parser.add_argument('--property_value', default=[], action='append', help='The values in key-value pairs that will be added to the details of the object. The nth property key will be paired with the nth property value. The number of keys must equal the number of values provided')
parser.add_argument('--tag', default=[], action='append', help='"A set of tags (string labels) that will be added to the resulting Variants table object. (You can use tags and properties to better describe and organize your data)')



def importGFF(**args):
    
    if len(args) == 0:
        args = parser.parse_args(sys.argv[1:])
        fileName = args.fileName
        reference = args.reference
        outputName = args.outputName
        file_id = args.file_id
        property_key = args.property_key
        property_value = args.property_value
        tag = args.tag
        additional_type = args.additional_type

    else:
        fileName = args['fileName']
        reference = args['reference']
        outputName = ''
        if args.get('outputName') != None:
            outputName = args['outputName']
        tag = []
        if args.get('tag'):
            tag = args['tag']
        if args.get('property_key') != None:
            property_key = args['property_key']
        if args.get('property_value') != None:
            property_value = args['property_value']
        if args.get('additional_type') != None:
            additional_type = args['additional_type']
        if args.get('file_id') != None:
            file_id = args['file_id']
    
    inputFileName = unpack(fileName)
        
    #Rows of this type will not be written to the gtable as their information is fully encompassed by the rest of the data
    discardedTypes = {"start_codon": True, "stop_codon": True}
   
    ##Isolate the attribute tags from the file and check integrity
    spansTable, additionalColumns = constructTable(inputFileName)
    
    details = {'original_contigset': dxpy.dxlink(reference)}
    if file_id != None:
            details['original_file'] = dxpy.dxlink(file_id)
    if len(property_key) != len(property_value):
        raise dxpy.AppError("Expected each provided property to have a corresponding value.")
    for i in range(len(property_key)):
        details[property_key[i]] = property_value[i]

    spansTable.set_details(details)
    spansTable.add_tags(tag)

    if outputName == '':
        spansTable.rename(fileName)
    else:
        spansTable.rename(outputName)

    hasGenes = False

    #This pass through the file calculates the gene and transcript models 
    genes = {}
    transcripts = {}
    spanId = 0
    
    sequenceOntology = {}
    for x in ["five_prime_UTR", "5' UTR", "five prime UTR", "five_prime_untranslated_region", "five_prime_coding_exon_noncoding_region", "five_prime_exon_noncoding_region", "five prime coding exon noncoding region"]:
        sequenceOntology[x] = "5' UTR"
    for x in ["three_prime_UTR", "3' UTR", "three prime UTR", "three_prime_untranslated_region", "three_prime_coding_exon_noncoding_region", "three_prime_exon_noncoding_region", "three prime coding exon noncoding region"]:
        sequenceOntology[x] = "3' UTR"
    for x in ["mRNA", "rRNA", "tRNA", "snRNA", "snoRNA", "miRNA", "ncRNA", "transcript", "mature_transcript", "rRNA_large_subunit_primary_transcript", "35S rRNA primary transcript", "rRNA large subunit primary transcript", "rRNA_primary_transcript", "enzymatic_RNA", "nc_primary_transcript", "scRNA", "protein_coding_primary_transcript", "antisense_RNA", "antisense_primary_transcript", "primary_transcript", "ribosomal_subunit_rRNA", "small subunit rRNA", "SSU RNA", "SSU rRNA", "large_subunit_rRNA", "LSU RNA", "LSU rRNA"]:
        sequenceOntology[x] = "transcript"
    for x in ["exon", "interior_coding_exon", "interior coding exon", "coding_exon", "coding exon", "five_prime_coding_exon_region", "five prime exon coding region", "three_prime_coding_exon_region", "three prime coding exon region", "five_prime_coding_exon", "three_prime_coding_exon", "non_coding_exon", "non coding exon"]:
        sequenceOntology[x] = "exon"

    isCoding = {}
    for x in ["CDS", "interior_coding_exon", "interior coding exon", "coding_exon", "five_prime_coding_exon_region", "five prime exon coding region", "three_prime_coding_exon_region", "three prime coding exon region", "five_prime_coding_exon", "three_prime_coding_exon"]:
        isCoding[x] = True
        
    codingRegions = {}
    spans = {}
    
    inputFile = open(inputFileName, 'r')
    for line in inputFile:
        if line[0] != "#":
            values = parseLine(line.split("#")[0])
        
            if values["attributes"].get("Parent") != None:
                for parent in values["attributes"]["Parent"].split(","):
                    if codingRegions.get(parent) == None:
                        codingRegions[parent] = {values["chromosome"]: {"codingLo": -1, "codingHi": -1} }
                    if isCoding.get(values["type"]) != None:
                        if values["lo"] < codingRegions[parent][values["chromosome"]]["codingLo"] or codingRegions[parent][values["chromosome"]]["codingLo"] == -1:
                            codingRegions[parent][values["chromosome"]]["codingLo"] = values["lo"]
                        if values["hi"] > codingRegions[parent][values["chromosome"]]["codingHi"] or codingRegions[parent][values["chromosome"]]["codingLo"] == -1:
                            codingRegions[parent][values["chromosome"]]["codingHi"] = values["hi"]
            if values["attributes"].get("ID") != None:
                spans[values["attributes"]["ID"]] = spanId
            spanId += 1
    
    inputFile = open(inputFileName, 'r')
    overflowSpans = spanId
    spanId = 0
    
    for line in inputFile:
        if line[0] != "#":
            values = parseLine(line)
            entryIsCoding = False
            if isCoding.get(values["type"]) != None:
                entryIsCoding = True
            if values["attributes"].get("Name") != None:
                name = values["attributes"]["Name"]
            elif values["attributes"].get("name") != None:
                name = values["attributes"]["name"]
            elif values["attributes"].get("NAME") != None:
                name = values["attributes"]["NAME"]
            elif values["attributes"].get("ID") != None:
                name = values["attributes"]["ID"]
            else:
                name = ''
            if sequenceOntology.get(values["type"]) != None:
                values["type"] = sequenceOntology[values["type"]]
                hasGenes = True
            description = ''
            if values["attributes"].get("description") != None:
                description = values["attributes"]["description"]
            if values["attributes"].get("Description") != None:
                description = values["attributes"]["description"]
            
            parent = -1
            if values["type"] not in discardedTypes:
                if values["attributes"].get("Parent") != None:
                    parentSplit = values["attributes"]["Parent"].split(",")
                else:
                    parentSplit = ["-1"]
                for parent in parentSplit:
                    currentSpan = spanId
                    parentId = -1
                    if spans.get(parent) != None:
                        parentId = spans[parent]
                    if parentSplit.index(parent) > 0:
                        currentSpan = overflowSpans
                        overflowSpans += 1
                    for x in ["ID", "Parent"]:
                        if not entryIsCoding and values["attributes"].get(x) != None:
                            if codingRegions.get(values["attributes"][x]) != None:
                                if codingRegions[values["attributes"][x]].get("chromosome") != None:
                                    if values["lo"] >= codingRegions[values["attributes"][x]]["chromosome"]["codingLo"] and values["lo"] <= codingRegions[values["attributes"][x]]["chromosome"]["codingHi"] and codingRegions[values["attributes"][x]]["chromosome"]["codingHi"] > -1 and codingRegions[values["attributes"][x]]["chromosome"]["codingHi"] > -1:
                                        entryIsCoding = True
                                    if values["hi"] >= codingRegions[values["attributes"][x]]["chromosome"]["codingLo"] and values["hi"] <= codingRegions[values["attributes"][x]]["chromosome"]["codingHi"] and codingRegions[values["attributes"][x]]["chromosome"]["codingHi"] > -1 and codingRegions[values["attributes"][x]]["chromosome"]["codingHi"] > -1:
                                        entryIsCoding = True
                entry = [values["chromosome"], values["lo"], values["hi"], name, currentSpan, values["type"], values["strand"], values["score"], entryIsCoding, parentId, values["frame"], description, values["source"]]
                for x in additionalColumns:
                    if values["attributes"].get(x) != None:
                        entry.append(values["attributes"][x])
                    else:
                        entry.append('')
                spansTable.add_rows([entry])
            spanId += 1
    
    if hasGenes:
        types = ["Genes", "gri"]
    else:
        types = ["Spans", "gri"]
    for x in additional_type:
        types.append(x)
    spansTable.add_types(types)
    spansTable.flush()
    spansTable.close()
    print(spansTable.get_id())
    job_outputs = dxpy.dxlink(spansTable.get_id())
    return job_outputs

def writeEntry(spansTable, spanId, exonInfo, additionalColumns, chromosome, lo, hi, attributes, entry):
    if [lo, hi] not in exonInfo[chromosome]:
        exonInfo[chromosome].append([lo, hi])
        spanId += 1
        for x in additionalColumns:
            if attributes.get(x) != None:
                entry.append(attributes[x])
            else:
                entry.append('')
        spansTable.add_rows([entry])
    return spanId

def splitExons(transcriptInfo, chromosome, lo, hi):
    result = [["CDS", lo, hi]]
    if lo < transcriptInfo[chromosome]["codingLo"]:
        result[0][1] = transcriptInfo[chromosome]["codingLo"]
        result.append(["5' UTR", lo, transcriptInfo[chromosome]["codingLo"]])
    if hi > transcriptInfo[chromosome]["codingHi"]:
        result[0][2] = transcriptInfo[chromosome]["codingHi"]
        result.append(["3' UTR", transcriptInfo[chromosome]["codingHi"], hi])
    return result

def parseLine(line):
    line = line.strip().split("#")[0]
    tabSplit = line.split("\t")
    if len(tabSplit) == 1:
            tabSplit = line.split(" ")
            if len(tabSplit) < 8:
                raise dxpy.AppError("One row did not have 8 or 9 entries, it had 1 instead. Offending line: " + line)
            tabSplit[8] = " ".join(tabSplit[8:])
            tabSplit = tabSplit[:9]
    chromosome = tabSplit[0]
    source = tabSplit[1]
    typ = tabSplit[2]
    
    try:
        lo = int(tabSplit[3])-1
    except ValueError:
        raise dxpy.AppError("One of the start values was could not be translated to an integer. " + "\nOffending line: " + line + "\nOffending value: " + tabSplit[3])
    
    try:
        hi = int(tabSplit[4])
    except ValueError:
        raise dxpy.AppError("One of the start values was could not be translated to an integer. " + "\nOffending line: " + line + "\nOffending value: " + tabSplit[4])

    try:
        score = float(tabSplit[5])
    except ValueError:
        if tabSplit[5] == "." or tabSplit[5] == '':
            score = dxpy.NULL
        else:
            raise dxpy.AppError("The score for one line could not be translated into a number and was not \".\"" + "\nOffending line: " + line + "\nOffending value: " + tabSplit[5])

    tabSplit[6] = tabSplit[6].replace("?", ".")
    if tabSplit[6] != "+" and tabSplit[6] != "-" and tabSplit[6] != ".":
        raise dxpy.AppError("The strand indicated for an element was not \"+\", \"-\", \"?\", or \".\"" + "\nOffending line: " + line + "\nOffending value: " + tabSplit[6])
    else:
        strand = tabSplit[6]

    try:
        frame = int(tabSplit[7])
        if frame > 2 or frame < 0:
            raise dxpy.AppError("The frame indicated for an element was not \".\", \"0\", \"1\", or \"2\"" + "\nOffending line: " + line + "\nOffending value: " + tabSplit[7])
    except ValueError:
        if tabSplit[7] == ".":
            frame = -1
        else:
            raise dxpy.AppError("The frame indicated for an element was not \".\", \"0\", \"1\", or \"2\"" + "\nOffending line: " + line + "\nOffending value: " + tabSplit[7])
    
    lineAttributes = {}
    ##Extract the attributes from the file
    if len(tabSplit) >= 9:
        reg = re.findall("([^=]*)=([^;]*);", tabSplit[8].strip() + ";")
        for x in reg:
            if len(x[0]) < 100:
                lineAttributes[x[0]] = x[1].strip().strip("\"")
    else:
        lineAttributes = {}
    values = {"chromosome": chromosome, "lo": lo, "hi": hi, "source": source, "type": typ, "strand": strand, "score": score, "frame": frame, "attributes": lineAttributes}
    return values
    
def constructTable(inputFileName):
    inputFile = open(inputFileName, 'r')
    attributes = {}
    for line in inputFile:
        if line[0] != "#":
            line = line.strip().split("#")[0]
            tabSplit = line.split("\t")
            if len(tabSplit) == 1:
                tabSplit = line.split(" ")
                if len(tabSplit) < 9:
                    raise dxpy.AppError("One row did not have 8 or 9 entries, it had 1 instead. Offending line: " + line)
                tabSplit[8] = " ".join(tabSplit[8:])
                tabSplit = tabSplit[:9]
            
            if len(tabSplit) != 8 and len(tabSplit) != 9:
                raise dxpy.AppError("One row did not have 8 or 9 entries, it had " + str(len(tabSplit)) + " instead. Offending line: " + line)
            elif len(tabSplit) == 9:
                reg = re.findall("([^=]*)=([^;]*);", tabSplit[8].strip() + ";")
                for x in reg:
                    attributes[x[0]] = True
    
    
    reservedColumns = ["", "chr", "lo", "hi", "name", "span_id", "type", "score", "is_coding", "parent_id", "frame", "description", "source"]
    
    #Construct table
    schema = [
            {"name": "chr", "type": "string"}, 
            {"name": "lo", "type": "uint32"},
            {"name": "hi", "type": "uint32"},
            {"name": "name", "type": "string"},
            {"name": "span_id", "type": "int32"},
            {"name": "type", "type": "string"},
            {"name": "strand", "type": "string"},
            {"name": "score", "type": "float"},
            {"name": "is_coding", "type": "boolean"},
            {"name": "parent_id", "type": "int32"},
            {"name": "frame", "type": "int16"},
            {"name": "description", "type": "string"},
            {"name": "source", "type": "string"}]
    
    additionalColumns = []
    for k, v in attributes.iteritems():
        if k not in reservedColumns and len(k) < 100:
            schema.append({"name": k, "type": "string"})
            additionalColumns.append(k)
            
    indices = [dxpy.DXGTable.genomic_range_index("chr","lo","hi", 'gri'), 
               dxpy.DXGTable.lexicographic_index([
                  dxpy.DXGTable.lexicographic_index_column("name", True, False),
                  dxpy.DXGTable.lexicographic_index_column("chr"),
                  dxpy.DXGTable.lexicographic_index_column("lo"),
                  dxpy.DXGTable.lexicographic_index_column("hi"),
                  dxpy.DXGTable.lexicographic_index_column("type")], "search")]
    spansTable = dxpy.new_dxgtable(columns=schema, indices=indices)
    return spansTable, additionalColumns

def unpack(input):
    m = magic.Magic()

    # determine compression format
    try:
        file_type = m.from_file(input)
    except:
        raise dxpy.AppError("Unable to identify compression format")


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
    except:
        raise dxpy.AppError("Error detecting file format after decompression")

    if uncomp_type == 'POSIX tar archive (GNU)' or 'tar' in uncomp_type:
        raise dxpy.AppError("Found a tar archive after decompression.  Please untar your files before importing")
    elif 'ASCII text' not in uncomp_type:
        raise dxpy.AppError("After decompression found file type other than plain text")

    try:
        subprocess.check_call(" ".join([uncomp_util, "--stdout", input, ">", "uncompressed.gff"]), shell=True)
        return "uncompressed.gff"
    except subprocess.CalledProcessError:
        raise dxpy.AppError("Unable to open compressed input for reading")


def main(**args):
    return importGFF(**args)

if __name__ == '__main__':
    importGFF()



        
