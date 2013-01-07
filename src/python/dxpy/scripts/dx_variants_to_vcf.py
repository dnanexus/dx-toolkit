#!/usr/bin/env python

# Copied verbatim from old version (0.0.54) of variants_to_vcf app.
# Included here for a diff against the new version of the script.

import dxpy
import math
from optparse import OptionParser
import re
import collections
import sys

def main():

    parser = OptionParser("Usage: %prog table_id output quality_filter")
    parser.add_option("--table_id", dest="variants_id", help="Variants table id to read from")
    parser.add_option("--output", dest="file_name", help="Name of file to write VCF to")
    parser.add_option("--export_ref_calls", dest="export_ref_calls", default=False, action="store_true" , help="If selected, rows confidently called as non-variants will also be written")
    parser.add_option("--export_no_calls", dest="export_no_calls", default=False, action="store_true" , help="If selected, rows in which no confident call could be made will also be written")
    parser.add_option("--chr", dest="chr", action="append" , help="If any chr are provided, export will only write rows of that chromosome")
    parser.add_option("--no_write_header", dest="write_header", default=True, action="store_false", help="If selected, do not write the header the VCF file (useful for concatenating files together with chr")
    parser.add_option("--reference", dest="reference", help="If present, take reference from this file instead of trying to download it")

    (opts, args) = parser.parse_args()
    outputFile = open(opts.file_name, 'w')
    exportRef = opts.export_ref_calls
    exportNoCall = opts.export_no_calls
    
    variantsTable = dxpy.open_dxgtable(opts.variants_id)
    
    refFileName = 'ref.txt'
    if opts.reference != None:
        refFileName = opts.reference
        if not os.path.isfile(refFileName):
            raise dxpy.AppError("The reference expected by variants to vcf script was not a valid file")
    else:    
        try:
            originalContigSet = variantsTable.get_details()['original_contigset']
        except:
            raise dxpy.AppError("The original reference genome must be attached as a detail")
        
        contigDetails = dxpy.DXRecord(originalContigSet).get_details()
        dxpy.download_dxfile(contigDetails['flat_sequence_file']['$dnanexus_link'], 'ref.txt')
    
 
    if opts.write_header:
    
       infos = variantsTable.get_details().get('infos')
       formats = variantsTable.get_details().get('formats')
       filters = variantsTable.get_details().get('filters')
       samples = variantsTable.get_details().get('samples')
    
       outputFile.write("##fileformat=VCFv4.1\n")
       if infos != None:
           for k, v in collections.OrderedDict(sorted(infos.iteritems())).iteritems():
               outputFile.write("##INFO=<ID="+k+",Number="+v['number']+",Type="+v['type']+",Description=\""+v['description']+"\">\n")

       if len(samples) > 0:
           outputFile.write("##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n")
           outputFile.write("##FORMAT=<ID=AD,Number=.,Type=Integer,Description=\"Allelic depths for the ref and alt alleles in the order listed\">\n")
           outputFile.write("##FORMAT=<ID=DP,Number=1,Type=String,Description=\"Approximate read depth (reads with MQ=255 or with bad mates are filtered)\">\n")
       if formats != None:
           for k, v in collections.OrderedDict(sorted(formats.iteritems())).iteritems():
               outputFile.write("##FORMAT=<ID="+k+",Number="+v['number']+",Type="+v['type']+",Description=\""+v['description']+"\">\n")
       if filters != None:
           for k, v in collections.OrderedDict(sorted(filters.iteritems())).iteritems():
               outputFile.write("##FILTER=<ID="+k+",Description=\""+v+"\">\n")
       for i in range(len(contigDetails['contigs']['names'])):
           outputFile.write("##contig=<ID="+contigDetails['contigs']['names'][i]+",length="+str(contigDetails['contigs']['sizes'][i])+">\n")
       outputFile.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO")
      
       if len(samples) > 0:
           outputFile.write("\tFORMAT")
           for x in samples:
               outputFile.write("\t"+x)
       outputFile.write("\n")

    chromosomeOffsets = {}
    for i in range(len(contigDetails['contigs']['names'])):
        chromosomeOffsets[contigDetails['contigs']['names'][i]] = contigDetails['contigs']['offsets'][i]
        
    contigSequence = open(refFileName,'r').read()
    
    col = {}
    names = variantsTable.get_col_names()   
    for i in range(len(names)):
        col[names[i]] = i+1
    col = collections.OrderedDict(sorted(col.items()))
    
    chromosomeList = contigDetails['contigs']['names']
    if opts.chr != None:
        intersection = []
        for x in chromosomeList:
            if x in opts.chr:
                intersection.append(x)
        chromosomeList = intersection[:]
 
    for chromosome in chromosomeList:
        lastPosition = -1
        buff = []
        query = variantsTable.genomic_range_query(chr=chromosome, lo=0, hi=sys.maxint)
        for row in variantsTable.get_rows(query=query, limit=1)['data']:
            startRow =  row[0]
            for row in variantsTable.iterate_rows(start=startRow):
                if row[1] != chromosome:
                    break
                if lastPosition < row[col["lo"]]:
                    writeBuffer(buff, col, outputFile, contigSequence, chromosomeOffsets, exportRef, exportNoCall)
                    buff = []
                buff.append(row)
                lastPosition = row[col["lo"]]
        writeBuffer(buff, col, outputFile, contigSequence, chromosomeOffsets, exportRef, exportNoCall)
        buff = []

def writeBuffer(buff, col, outputFile, contigSequence, chromosomeOffsets, exportRef, exportNoCall):
    for x in buff:
        printPreceedingCharacter = False
        altOptions = x[col["alt"]].split(",")
        for y in altOptions:
            if len(x[col["ref"]]) != len(y) or len(x[col["ref"]]) == 0 or len(y) == 0:
                printPreceedingCharacter = True
                break
        if printPreceedingCharacter:
            writeRowCheck(x[:], col, outputFile, contigSequence, chromosomeOffsets, exportRef, exportNoCall)
    for x in buff:
        printPreceedingCharacter = False
        altOptions = x[col["alt"]].split(",")
        for y in altOptions:
            if len(x[col["ref"]]) != len(y) or len(x[col["ref"]]) == 0 or len(y) == 0:
                printPreceedingCharacter = True
                break
        if printPreceedingCharacter == False:
            writeRowCheck(x[:], col, outputFile, contigSequence, chromosomeOffsets, exportRef, exportNoCall)


def writeRowCheck(row, col, outputFile, contigSequence, chromosomeOffsets, exportRef, exportNoCall):
    if checkRowIsAllType(row, col, "ref"):
        if exportRef:
            writeRow(row, col, outputFile, contigSequence, chromosomeOffsets)
    elif checkRowIsAllType(row, col, "no-call"):
        if exportNoCall:
            writeRow(row, col, outputFile, contigSequence, chromosomeOffsets)
    else:
        writeRow(row, col, outputFile, contigSequence, chromosomeOffsets)

def parseRegions(input):
    result = []
    for x in input:
        result.append(re.findall("(\w+):(\d+)-(\d+)", x))
    return result

def writeRow(row, col, outputFile, contigSequence, chromosomeOffsets):

    chr = str(row[col["chr"]])
    pos = row[col["lo"]]+1
    ref = row[col["ref"]]
    alt = row[col["alt"]]

    ids = '.'
    if col.get("ids") != None:
        if row[col["ids"]] != '':
            ids = row[col["ids"]]

    filt = '.'
    if col.get("filter") != None:
        if row[col["filter"]] != '':
            filt = row[col["filter"]]
        else:
            filt = "PASS"
            
    qual = '.'
    if col.get("qual") != None:
        if row[col["qual"]] != dxpy.NULL or row[col["qual"]] == -999999:
            qual = row[col["qual"]]
        
    #Check if any types are ins/del, if so pull out the character before as well.
    sample = 0
    printPreceedingCharacter = False
    altOptions = row[col["alt"]].split(",")
    for x in altOptions:
        if len(ref) != len(x) or len(ref) == 0 or len(alt) == 0:
            printPreceedingCharacter = True
    
    if printPreceedingCharacter:
        ref = contigSequence[chromosomeOffsets[chr]+int(pos)-2]+ref
        altOptions = row[col["alt"]].split(",")
        alt = ''
        for x in altOptions:
            alt += contigSequence[chromosomeOffsets[chr]+int(pos)-2]+x+","
        alt = alt.rstrip(",")
        pos -= 1

    outputFile.write(chr+"\t"+str(pos)+"\t"+str(ids)+"\t"+ref+"\t"+alt+"\t"+str(qual)+"\t"+str(filt))

    infos = ''
    for x in col:
        if "info_" in x:
            if col.get(x) != None:
                if isinstance(row[col[x]], bool):
                    if row[col[x]] == True:
                        infos += x.lstrip("info_")+";"
                elif isDefault(row[col[x]]):
                    infos += x.lstrip("info_")+"="+str(row[col[x]])+";"
    if infos == '':
        infos = '.'
    outputFile.write("\t"+infos.rstrip(";"))
    
    #Check whether the reserved fields coverage and total coverage are present, and put them into the info index if so
    sample = 0
    coverage = False
    totalCoverage = False
    while 1:
        if col.get("type_"+str(sample)) == None:
            break
        if coverage == False:
            coverage = col.get("coverage_"+str(sample))
        if totalCoverage == False:
            totalCoverage = col.get("total_coverage_"+str(sample))
        sample += 1
    
    #Check which info tags are present and use them to construct the info Index
    observedFormats = []
    for x in col:
        if "format_" in x:
            if col.get(x) != None:
                entrySplit = x.split("_")[1:]
                entrySplit.pop()
                tag = '_'.join(entrySplit)
                if tag not in observedFormats:
                    observedFormats.append(tag)
    if col.get("type_0") != None:
        outputFile.write("\t")
        formatOrdering = 'GT:'
        if coverage:
            formatOrdering += "AD:"
        if totalCoverage:
            formatOrdering += "DP:"
        for x in observedFormats:
            formatOrdering += x+":"
        outputFile.write(formatOrdering.rstrip(":"))
    
        sample = 0
        while 1:
            if col.get("type_"+str(sample)) != None:
                formats = row[col["genotype_"+str(sample)]]+":"
                if coverage:
                    if col.get("coverage_"+str(sample)):
                        cov = row[col.get("coverage_"+str(sample))]
                        formats += cov+":"
                    else:
                        cov += ".:"
                if totalCoverage:
                    if col.get("total_coverage_"+str(sample)) != None:
                        tCov = row[col.get("total_coverage_"+str(sample))]
                        if tCov == dxpy.NULL or tCov == -999999:
                            formats += "0:"
                        else:
                            formats += str(tCov)+":"
                    else:
                        formats += ".:"
                for x in observedFormats:
                    if col.get("format_"+x+"_"+str(sample)) != None:
                        if isinstance(row[col["format_"+x+"_"+str(sample)]], bool):
                            if row[col["format_"+x+"_"+str(sample)]]:
                                formats += x
                            else:
                                formats += "."
                        elif row[col["format_"+x+"_"+str(sample)]] == dxpy.NULL or row[col["format_"+x+"_"+str(sample)]] == -999999:
                            formats += "."
                        else:
                            formats += str(row[col["format_"+x+"_"+str(sample)]])
                    else:
                        formats += "."
                    formats += ":"
                outputFile.write("\t"+formats.rstrip(":"))
            else:
                break
            sample += 1
    outputFile.write("\n")
        
def isDefault(entry):
    if isinstance(entry, float):
        if entry == dxpy.NULL or entry == -999999:
            return False
    if isinstance(entry, bool):
        if entry == False:
            return False
    if isinstance(entry, int):
        if entry == dxpy.NULL or entry == -999999:
            return False
    if entry == '':
        return False
    return True

def checkRowIsAllType(row, col, typ):
    sample = 0
    while 1:
        if col.get("type_"+str(sample)) == None:
            if sample > 0:
                return True
            else:
                return False
        if row[col["type_"+str(sample)]] != typ:
            return False
        sample += 1

main()
