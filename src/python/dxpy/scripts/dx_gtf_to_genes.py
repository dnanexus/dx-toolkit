#!/usr/bin/env python

import dxpy
import string
import random
import sys

# to find the magic library
sys.path.append('/usr/local/lib/')
import magic
import subprocess
import argparse

parser = argparse.ArgumentParser(description='Import a local GTF file as a Spans or Genes object.')
parser.add_argument('fileName', help='local fileName to import')
parser.add_argument('reference', help='ID of ContigSet object (reference) that this GTF file annotates')
parser.add_argument('--outputName', dest='outputName', default='', help='what to name the output. if none provided, the name of the input file will be used with gTf file extension removed.')


@dxpy.entry_point('main')
def importGTF(**args):

    if len(args) == 0:
        args = parser.parse_args(sys.argv[1:])

    fileName = args.fileName
    reference = args.reference
    outputName = args.outputName

    inputFileName = unpack(fileName)
    
    capturedTypes = {"5UTR": "5' UTR", "3UTR": "3' UTR", "CDS": "CDS", "inter": "intergenic", "inter_CNS": "intergenic_conserved", "intron_CNS": "intron_conserved", "exon": "exon", "transcript": "transcript", "gene":"gene"} 
    
    #Rows of this type will not be written to the gtable as their information is fully encompassed by the rest of the data
    discardedTypes = {"start_codon": True, "stop_codon": True}
   
    ##Isolate the attribute tags from the file and check integrity
    spansTable, additionalColumns = constructTable(inputFileName)
    spansTable.add_types(["Spans", "NamedSpans", "Genes", "gri"])
    spansTable.set_details({'original_file': dxpy.dxlink(fileName), 'original_contigset': dxpy.dxlink(reference)})
    if outputName != '':
        name =  outputName
    else:
        name = dxpy.DXFile(fileName).describe()['name']
        for x in name.split(".")[1:-1]:
            if x != "gtf" and x != "GTF" and x != "gz" and x != "gz2" and x != ".tar" and x != ".bz" and x != ".bz2":
                name += "."+x
    spansTable.rename(name)

    #This pass through the file calculates the gene and transcript models 
    genes = {}
    transcripts = {}
    spanId = 0
    
    inputFile = open(inputFileName, 'r')
    for line in inputFile:
        if line[0] != "#":
            values = parseLine(line, capturedTypes, discardedTypes)
    
            for [element, hashId] in [[genes, values["geneId"]], [transcripts, values["transcriptId"]]]:   
                if element.get(hashId) == None:
                    element[hashId] = {values["chromosome"]: {"lo":values["lo"], "hi":values["hi"], "codingLo": -1, "codingHi": -1, "strand":values["strand"], "score":values["score"], "geneId":values["geneId"], "coding":False, "spanId": spanId, "name":values["geneName"], "originalGeneId": values["attributes"]["gene_id"], "originalTranscriptId": values["attributes"]["transcript_id"]}}
                    spanId += 1
                elif element[hashId].get(values["chromosome"]) == None:
                    element[hashId][values["chromosome"]] = {"lo":values["lo"], "hi":values["hi"], "codingLo": -1, "codingHi": -1, "strand":values["strand"], "score":values["score"], "geneId":values["geneId"], "coding":False, "spanId": spanId, "name":values["transcriptName"], "originalGeneId": values["attributes"]["gene_id"], "originalTranscriptId": values["attributes"]["transcript_id"]}
                    spanId += 1
                else:
                    if values["lo"] < element[hashId][values["chromosome"]]["lo"]:
                        element[hashId][values["chromosome"]]["lo"] = values["lo"]
                    if values["hi"] > element[hashId][values["chromosome"]]["hi"]:
                        element[hashId][values["chromosome"]]["hi"] = values["hi"]
        
            if values["type"] == "CDS" or values["type"] == "start_codon" or values["type"] == "stop_codon":
                if values["hi"] > transcripts[values["transcriptId"]][values["chromosome"]]["codingHi"]:
                    transcripts[values["transcriptId"]][values["chromosome"]]["codingHi"] = values["hi"]
                if values["lo"] < transcripts[values["transcriptId"]][values["chromosome"]]["codingLo"] or transcripts[values["transcriptId"]][values["chromosome"]]["codingLo"] == -1:
                    transcripts[values["transcriptId"]][values["chromosome"]]["codingLo"] = values["lo"]
                genes[values["geneId"]][values["chromosome"]]["coding"] = True
                transcripts[values["transcriptId"]][values["chromosome"]]["coding"] = True
    
    for gId, chrList in genes.iteritems():
        for k, v in chrList.iteritems():          
            entry = [k, v["lo"], v["hi"], v["name"], v["spanId"], "gene", v["strand"], v["score"], v["coding"], -1, -1, '', '', v["originalGeneId"], '']
            for x in additionalColumns:
                if x != "gene_id" and x != "transcript_id":
                    entry.append('')
            spansTable.add_rows([entry])
    for gId, chrList in transcripts.iteritems():
        for k, v in chrList.iteritems():
            entry = [k, v["lo"], v["hi"], v["name"], v["spanId"], "transcript", v["strand"], v["score"], genes[v["geneId"]][k]["coding"], genes[v["geneId"]][k]["spanId"], -1, '', '', v["originalGeneId"], v["originalTranscriptId"]]
            for x in additionalColumns:
                if x != "gene_id" and x != "transcript_id":
                    entry.append('')
            spansTable.add_rows([entry])

    exons = {}      
    inputFile = open(inputFileName, 'r')
    for line in inputFile:
        if line[0] != "#":
            values = parseLine(line, capturedTypes, discardedTypes)
            
            if exons.get(values["transcriptId"]) != None:
                if exons[values["transcriptId"]].get(values["chromosome"]) == None:
                    exons[values["transcriptId"]][values["chromosome"]] = []
            else:
                exons[values["transcriptId"]] = {values["chromosome"] : []}
        
            
            
            if capturedTypes.get(values["type"]) != None:
                #If type is 5'UTR, 3'UTR, intergenic, or conserved intron, type is always noncoding
                if values["type"] == "5UTR" or values["type"] == "3UTR" or values["type"] == "inter" or values["type"] == "inter_CNS" or values["type"] == "intron_CNS":
                    writeEntry(spansTable, spanId, exons[values["transcriptId"]], additionalColumns, values["chromosome"], values["lo"], values["hi"], values["attributes"], [values["chromosome"], values["lo"], values["hi"], values["name"], spanId, capturedTypes[values["type"]], values["strand"], values["score"], False, transcripts[values["transcriptId"]]["spanId"], values["frame"], '', values["source"]])
                
                if "exon_number" in values["attributes"]:
                    values["transcriptName"] += "." + values["attributes"]["exon_number"]
                
                #If type is CDS, always of type coding                
                if values["type"] == "CDS":
                    if [values["lo"], values["hi"]] not in exons[values["transcriptId"]][values["chromosome"]]:
                        spanId = writeEntry(spansTable, spanId, exons[values["transcriptId"]], additionalColumns, values["chromosome"], values["lo"], values["hi"], values["attributes"], [values["chromosome"], values["lo"], values["hi"], values["transcriptName"], spanId, capturedTypes[values["type"]], values["strand"], values["score"], True, transcripts[values["transcriptId"]][values["chromosome"]]["spanId"], values["frame"], '', values["source"]])
                
                #If type is exon do calculation as to whether coding or non-coding
                if values["type"] == "exon":
                    if (transcripts[values["transcriptId"]][values["chromosome"]]["codingLo"] != -1 and transcripts[values["transcriptId"]][values["chromosome"]]["codingHi"] != -1) and (values["lo"] >= transcripts[values["transcriptId"]][values["chromosome"]]["codingLo"] and values["lo"] <= transcripts[values["transcriptId"]][values["chromosome"]]["codingHi"]) or (values["hi"] >= transcripts[values["transcriptId"]][values["chromosome"]]["codingLo"] and values["hi"] <= transcripts[values["transcriptId"]][values["chromosome"]]["codingHi"]): 
                        for x in splitExons(transcripts[values["transcriptId"]], values["chromosome"], values["lo"], values["hi"]):
                            spanId = writeEntry(spansTable, spanId, exons[values["transcriptId"]], additionalColumns, values["chromosome"], x[1], x[2], values["attributes"], [values["chromosome"], x[1], x[2], values["transcriptName"], spanId, x[0], values["strand"], values["score"], x[3], transcripts[values["transcriptId"]][values["chromosome"]]["spanId"], values["frame"], '', values["source"]])
                    else:
                        spanId = writeEntry(spansTable, spanId, exons[values["transcriptId"]], additionalColumns, values["chromosome"], values["lo"], values["hi"], values["attributes"],  [values["chromosome"], values["lo"], values["hi"], values["transcriptName"], spanId, capturedTypes[values["type"]], values["strand"], values["score"], False, transcripts[values["transcriptId"]][values["chromosome"]]["spanId"], values["frame"], '', values["source"]])

    spansTable.flush()
    spansTable.close()
    print spansTable.get_id()
    #job_outputs = {"genes" : dxpy.dxlink(spansTable.get_id())}
    #return job_outputs

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
    result = [["CDS", lo, hi, True]]
    if lo < transcriptInfo[chromosome]["codingLo"]:
        result[0][1] = transcriptInfo[chromosome]["codingLo"]
        result.append(["5' UTR", lo, transcriptInfo[chromosome]["codingLo"], False])
    if hi > transcriptInfo[chromosome]["codingHi"]:
        result[0][2] = transcriptInfo[chromosome]["codingHi"]
        result.append(["3' UTR", transcriptInfo[chromosome]["codingHi"], hi, False])
    return result



def parseLine(line, capturedTypes, discardedTypes):
    tabSplit = line.split("\t")
    if len(tabSplit) == 1:
            tabSplit = line.split(" ")
            if len(tabSplit) < 9:
                raise dxpy.AppError("One row did not have 9 entries, it had 1 instead. Offending line: " + line)
            tabSplit[8] = " ".join(tabSplit[8:])
            tabSplit = tabSplit[:9]
    chromosome = tabSplit[0]
    source = tabSplit[1]
    typ = tabSplit[2]
    if capturedTypes.get(typ) == None and discardedTypes.get(typ) == None:
        message = 'Permitted types:'
        for x in [capturedTypes, discardedTypes]:
            for k, v in x.iteritems():
                message += " " + k + ","
        raise dxpy.AppError("One row had a type which is not in the list of permitted types. " + message + "\nOffending line: " + line + "\nOffending type: " + typ)
    
    try:
        lo = int(tabSplit[3])-1
    except ValueError:
        raise dxpy.AppError("One of the start values was could not be translated to an integer. " + "\nOffending line: " + line + "\nOffending value: " + tabSplit[3])
    
    try:
        hi = int(tabSplit[4])-1
    except ValueError:
        raise dxpy.AppError("One of the start values was could not be translated to an integer. " + "\nOffending line: " + line + "\nOffending value: " + tabSplit[4])

    try:
        score = float(tabSplit[5])
    except ValueError:
        if tabSplit[5] == "." or tabSplit[5] == '':
            score = dxpy.NULL
        else:
            raise dxpy.AppError("The score for one line could not be translated into a number and was not \".\"" + "\nOffending line: " + line + "\nOffending value: " + tabSplit[5])

    if tabSplit[6] != "+" and tabSplit[6] != "-" and tabSplit[6] != ".":
        raise dxpy.AppError("The strand indicated for an element was not \"+\", \"-\", or \".\"" + "\nOffending line: " + line + "\nOffending value: " + tabSplit[6])
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
    entrySplit = tabSplit[8].split(";")
    result = []
    for x in entrySplit:
        keyValue = x.strip().split(" ")
        key = keyValue[0]
        if key != '':
            if len(key) < 100:
                lineAttributes[key.strip('"')] = keyValue[1].strip('"')
        
    geneId = lineAttributes["gene_id"]
    transcriptId = lineAttributes["transcript_id"]
                
    geneName = geneId
    if "gene_name" in lineAttributes:
        geneName = lineAttributes["gene_name"]
    transcriptName = transcriptId
    if "transcript_name" in lineAttributes:
        transcriptName = transcriptName

    values = {"chromosome": chromosome, "lo": lo, "hi": hi, "geneName": geneName, "transcriptName": transcriptName, "source": source, "type": typ, "strand": strand, "score": score, "frame": frame, "geneId": geneId, "transcriptId": transcriptId, "attributes": lineAttributes}
    return values
        
def constructTable(inputFileName):
    inputFile = open(inputFileName, 'r')
    attributes = {"gene_id" : True, "transcript_id": True}
    for line in inputFile:
        if line[0] != "#":
            tabSplit = line.split("\t")
            if len(tabSplit) == 1:
                tabSplit = line.split(" ")
                if len(tabSplit) < 9:
                    raise dxpy.AppError("One row did not have 9 entries, it had 1 instead. Offending line: " + line)
                tabSplit[8] = " ".join(tabSplit[8:])
                tabSplit = tabSplit[:9]
            
            if len(tabSplit) != 9:
                raise dxpy.AppError("One row did not have 9 entries, it had " + str(len(tabSplit)) + " instead. Offending line: " + line)
            else:
                entrySplit = tabSplit[8].split(";")
                geneIdPresent = False
                transcriptIdPresent = False
                result = []
                for x in entrySplit:
                    keyValue = x.strip().split(" ")
                    key = keyValue[0]
                    if key == "gene_id":
                        geneIdPresent = True
                    elif key == "transcript_id":
                        transcriptIdPresent = True
                    attributes[key] = True
            if not geneIdPresent:
                raise dxpy.AppError("One row did not a a gene_id Offending line: " + line)
            if not transcriptIdPresent:
                raise dxpy.AppError("One row did not a a gene_id Offending line: " + line)
    
    
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
            {"name": "source", "type": "string"},
            {"name": "gene_id", "type": "string"},
            {"name": "transcript_id", "type": "string"}]
    
    additionalColumns = ['gene_id', 'transcript_id']
    for k, v in attributes.iteritems():
        if k != '' and k != 'gene_id' and k != 'transcript_id' and len(k) < 100:
            schema.append({"name": k, "type": "string"})
            additionalColumns.append(k)
    
    #When multiple indices are supported:
    #indices = [dxpy.DXGTable.genomic_range_index("chr","lo","hi", 'gri'), dxpy.DXGTable.lexicographic_index([["description", "ASC"]], "description")]
    indices = [dxpy.DXGTable.genomic_range_index("chr","lo","hi", 'gri')]
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
        except:
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
        subprocess.check_call(" ".join([uncomp_util, "--stdout", input, ">", "uncompressed.gtf"]), shell=True)
        return "uncompressed.gtf"
    except:
        raise dxpy.AppError("Unable to open compressed input for reading")

def main(**args):
    return importGTF(**args)

if __name__ == '__main__':
    importGTF()
