#!/usr/bin/env python

import sys
import dxpy
import argparse
import zlib
import urllib
import cStringIO
import struct
import gzip
import bisect
from collections import deque

_bgzf_magic = b"\x1f\x8b\x08\x04"
_bgzf_header = b"\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00\x42\x43\x02\x00"
_bgzf_eof = b"\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00BC\x02\x00\x1b\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00"
_bytes_BC = b"BC"


def convert_vfp(vfp):
	'''
	convert the virtual file pointer into a tuple of 
	(block offset, within block offset)
	'''
	return (vfp >> 16, vfp & (2**16 -1))
	
def convert_offsets(fo, bo):
	'''
	Convert a pair of offsets to a single virtual file pointer
	'''
	return (fo << 16) + bo

def reg2bins(rbeg, rend=None):
	'''
	Copied directly from tabix spec
	'''
	if rend is None:
		rend = rbeg
	else:
		rend = rend-1
	binlist = []
	
	for shift, offset in [(26,1),(23,9),(20,73),(17,585),(14,4681)]:
		k= offset+(rbeg>>shift)
		while k<= offset + (rend>>shift):
			binlist.append(k)
			k+=1
	
	return binlist

class bgzopen(object):

	def __init__(self, filePtr, splitChar=None, chunkSize=64*1024):
		'''
		NOTE: The chunkSize should be the maximum size of a bgzip block
		'''
	
		self._filePtr = filePtr
		self._foq = deque([])
		self._boq = deque([])
		self._fo = filePtr.tell()
		self._bo = 0
		self._dcFilePtr = cStringIO.StringIO()
		self._splitChar = splitChar
		
		self._delimChar = self._splitChar
		if self._delimChar is None:
			self._delimChar = '\n\r'
			
		
		self._chunkSize = chunkSize
		self._dc = zlib.decompressobj(zlib.MAX_WBITS | 32) # autodetect gzip or zlib header
		self._text = ""
		# do not store the lines, use filePtr.readline() instead
		# self._lines = list()
	#__init__()


	def __del__(self):
		# DO NOTHING to the file pointer, please!!
		pass
	#__del__()


	def __enter__(self):
		return self
	#__enter__()


	def __exit__(self, excType, excVal, excTrace):
		pass
	#__exit__()


	def __iter__(self):
		return self
	#__iter__()


	def __next__(self):  
		# read a line, stripping the newline
		return self.readline().rstrip(self._delimChar)
	#__next__()
	
	def _cacheChunk(self):
	
		newText = ''
		newLen = 0
		procDataLen = 1
		
		# While I've downloaded additional data and haven't added anything to 
		# my cached text, keep trying
		while newLen == 0 and procDataLen > 0:
			
			data = None
			
			# If this is true, we likely have a partial block
			if self._dc and self._dc.unused_data:
				data = self._dc.unused_data
				#print >> sys.stderr, "looking at unused data", repr(data[:10])
			else:
				#print >> sys.stderr, "current ptr:", self._filePtr.tell()
				data = self._filePtr.read(self._chunkSize)
			
			datalen=len(data)
			if data:
				self._dc = zlib.decompressobj(zlib.MAX_WBITS | 32) # autodetect gzip or zlib header
								
			if len(self._foq) == 0:							
				# Append the file offset to the file offset queue
				self._foq.append(self._filePtr.tell() - len(data)) 
				
			# If we're here and we have no data, we've likely hit the end of 
			# the compressed file
			if data:
				try:
					newText = self._dc.decompress(data)
				except:
					print >> sys.stderr, "ERROR decompressing:"
					#print >> sys.stderr, repr(data[:50])
					raise
				
				dlMore = True
				while len(self._dc.unused_data) == 0 and dlMore:
					newDat = self._filePtr.read(self._chunkSize)
					dlMore = len(newDat) > 0
					datalen += len(newDat)
					newText += self._dc.decompress(newDat)
								
				procDataLen = datalen - len(self._dc.unused_data)
				newLen = len(newText)
				self._boq.append(newLen)
				self._foq.append(self._foq[-1] + procDataLen)
				self._text += newText
				data = None
				#print >> sys.stderr, "decompressing!, added", newLen, "bytes from", procDataLen, "data"
			elif self._dc:
				newText = self._dc.flush()
				newLen = len(newText)
				procDataLen = datalen
				self._boq.append(newLen)
				self._foq.append(self._foq[-1] + procDataLen)
				self._text += newText
				self._dc = None
				#print "No data to be had, flushed and got", newLen, "bytes from", procDataLen, "data"
			else:
				# break out with no data read!
				# also kill the _foq added previously
				self._foq.popleft()
				newText = ""
				newLen = 0
				procDataLen = 0
	
		# end while loop
		
		#print >> sys.stderr, "Cached chunk"
		#print >> sys.stderr, "FOQ:", self._foq
		#print >> sys.stderr, "BOQ:", self._boq
				
		# return the # of bytes decompressed
		return newLen
		
	def _returnString(self, strlen):
		retlen = min(len(self._text), strlen)
		retstr = self._text[:retlen]
		self._text = self._text[retlen:]
		
		##print >> sys.stderr, "returning string of length", retlen
		
		# add the block offset
		#self._bo += retlen
		popped = False
		
		while len(self._boq) > 0 and self._bo + retlen >= self._boq[0]:
			retlen -= self._boq.popleft() - self._bo
			self._bo = 0	
			self._fo = self._foq.popleft()
			if len(self._foq) > 0:
				self._fo = self._foq[0]
		
		self._bo += retlen
		
		return retstr
	
	def tellvfp(self):
		'''
		Gives the current virtual file pointer of the current stream
		'''
		return convert_offsets(self._fo, self._bo)
	
	def seek(self, vfp):
		'''
		Seeks to the given virtual file pointer by seeking to the beginning of
		the block and reading the given number of characters
		'''
		# If we're seeking, we need to throw away our decompression object
		self._dc = zlib.decompressobj(zlib.MAX_WBITS | 32) # autodetect gzip or zlib header
		
		#print >> sys.stderr, "Seeking to", convert_vfp(vfp)
		
		# clear the foq and boq
		self._foq.clear()
		self._boq.clear()
		
		(fo, bo) = convert_vfp(vfp)
		if self._filePtr.tell() != fo:
			self._filePtr.seek(fo)
		else:
			pass
			#print >> sys.stderr, "No need to seek!"

			#print >> sys.stderr, "let's peek, though:"
			#print >> sys.stderr, repr(self._filePtr.read(10))
			#self._filePtr.seek(fo)
		
		self._fo = fo
		self._bo = 0
		self._text=""
		
		txt = self.read(bo)
		#print >> sys.stderr, "Seeking, throwing away:", (len(txt) == bo)
		#print >> sys.stderr, txt
		
		# return "True" if successful
		return (len(txt) == bo)
		
	def readUntil(self, vfp):
		'''
		Reads until the given virtual file pointer offset
		'''
		fo, bo = convert_vfp(vfp)
		text = ''
		while fo > self._fo:
			
			text += self.readChunk()
		
		# OK, now we're in the correct block, so read only 'bo' bytes more
		text += self.read(bo)
		
		return text
		
	def readChunk(self):
		'''
		Reads until the end of the chunk and no more
		Helpful when reading bgzipped data
		'''
		if len(self._text) == 0:
			self._cacheChunk()
			
		
		toret = self._returnString(self._boq[0] - self._bo)
		#self._cacheChunk()
		return toret
		
	def read(self, nbytes):
		'''
		Read at most nbytes, decompressed
		'''
		ret_text = ""
		addlDataLen = 1
		# Note, _cacheChunk() will change the length of self._text
		while len(self._text) + len(ret_text) < nbytes and addlDataLen > 0:
			if len(self._text) != 0:
				# read until end of chunk, clearing out self._text
				ret_text += self.readChunk()
			# now, fill self._text with a new block
			addlDataLen = self._cacheChunk()
			#print "cached an additional", addlDataLen, "chars"

		return ret_text + self._returnString(nbytes-len(ret_text))
		
	def readline(self, breakChar=None, nbytes=-1):
		'''
		Read until a cahracter in 'breakChars' is found.  Return the string
		read, including the character
		
		Note: if breakChars is None, read until any newline, so check to make
		sure if we find '\r' to also check for '\n' immediately following
		'''
		if breakChar is None:
			breakChar=self._delimChar

		firstpos = min([len(self._text)] + [i for i in (self._text.find(c) for c in breakChar) if i >= 0])
		
		while firstpos==len(self._text) and self._cacheChunk() > 0:
			firstpos = min([len(self._text)] + [i for i in (self._text.find(c) for c in breakChar) if i >= 0])
		
		if breakChar == self._delimChar and self._splitChar is None and firstpos < len(self._text) and self._text[firstpos] == '\r':
			# if our last character was carriage return, read another chunk
			if firstpos == len(self._text) - 1:
				self._cacheChunk() > 0
				
			if firstpos +1 < len(self._text) and self._text[firstpos+1] == '\n':
				firstpos+=1
		
		
		pbo = self._bo
		#print >> sys.stderr, "Current bo:", self._bo
		txt = self._returnString(firstpos+1)
		#print >> sys.stderr, "New bo:", self._bo
		
		#print >>sys.stderr, "Diff and len:", self._bo - pbo, len(txt)
		
		return txt

	def next(self):
		return self.__next__()
	#next()
#zopen

class tbi_bin(object):

	def __init__(self, filePtr):
		self.bin = struct.unpack('<L', filePtr.read(4))[0]
		self.n_chunk = struct.unpack('<l', filePtr.read(4))[0]
		self.chunks = [(struct.unpack('<Q', filePtr.read(8))[0], struct.unpack('<Q', filePtr.read(8))[0] ) for i in range(self.n_chunk)]

class tbi_ref(object):

	def __init__(self, filePtr):
		self.n_bin = struct.unpack('<l', filePtr.read(4))[0]
		self.bins = [tbi_bin(filePtr) for i in range(self.n_bin)]
		self.bin_map = {b.bin: b for b in self.bins}
		self.n_intv = struct.unpack('<l', filePtr.read(4))[0]
		self.ioff = [struct.unpack('<Q', filePtr.read(8))[0] for i in range(self.n_intv)]	
		self.first_pos = self.ioff[next((i for i, x in enumerate(self.ioff) if x), None)]
		
	def getBins(self, rbeg, rend=None):
		'''
		Gets all the bins actually in this reference data that overlap the region
		'''
		bin_opts = reg2bins(rbeg, rend)
		return [z for z in [self.bin_map.get(b, None) for b in bin_opts] if z is not None]
		
class tbi_data(object):

	def __init__(self, filePtr):
		'''
		Initialize the tabix index
		'''
		self.magic = filePtr.read(4)
		if self.magic != "TBI\1":
			raise IOError("Not a valid TBI File")

		self.n_ref = struct.unpack('<l', filePtr.read(4))[0]
		self.format = struct.unpack('<l', filePtr.read(4))[0]
		self.col_seq = struct.unpack('<l', filePtr.read(4))[0]
		self.col_beg = struct.unpack('<l', filePtr.read(4))[0]
		self.col_end = struct.unpack('<l', filePtr.read(4))[0]
		self.meta = struct.unpack('<l', filePtr.read(4))[0]
		self.skip = struct.unpack('<l', filePtr.read(4))[0]
		self.l_nm = struct.unpack('<l', filePtr.read(4))[0]
		
		name_str = filePtr.read(self.l_nm)
		self.names = name_str.rstrip('\0').split('\0')
		self.name_map = {v : i for i,v in enumerate(self.names)}
		
		self.ref = [tbi_ref(filePtr) for i in range(self.n_ref)]
		
		self.n_no_coor = None
		qword = filePtr.read(8)
		if len(qword) == 8:
			self.n_no_coor = struct.unpack('<Q', qword)[0]
			
		self.linearIndex=[0] + [i for o in self.ref for i in o.ioff if i>0]
		self.linearIndex.sort()

	def getNextChrom(self, vfp):
		'''
		Gets the offset of the first record in the next chromosome after the 
		given offset.  Returns None if the given offset is in the final 
		chromosome
		'''
		gt_offsets=[r.first_pos for r in self.ref if r.first_pos>vfp]
		if len(gt_offsets) == 0:
			return None
		else:
			return min(gt_offsets)
		
def getFile(file_str):
	retfile = None
	try:
		retfile = file(file_str, 'r')		
	except IOError, e:
		try:
			retfile = urllib.urlopen(file_str)
		except IOError:
			try:
				retfile = dxpy.bindings.dxfile.DXFile(file_str, mode='r')
			except:
				# OK, I give up, just return nothing!
				pass
		
	return retfile

def getNonOverlap(interval_in):
	
	curr_interval = None
	interval_out = []
	for interval in interval_in:
		if curr_interval is None:
			curr_interval = interval
		elif curr_interval[0] == interval[0]:
			if curr_interval[1] is None:
				pass
			elif curr_interval[1][1] >= interval[1][0]:
				curr_interval = (curr_interval[0], (curr_interval[1][0], interval[1][1]))
			else:
				interval_out.append(curr_interval)
				curr_interval = interval
		else:
			interval_out.append(curr_interval)
			curr_interval = interval
	
	interval_out.append(curr_interval)
	
	return interval_out

def parseInterval(interval_str):
	intv_chr_reg=interval_str.split(':')
	intv_chr = intv_chr_reg[0]
	intv_start = None
	intv_end = None
	if len(intv_chr_reg) > 1:
		intv_bounds=intv_chr_reg[1].split('-')
		intv_start=int(intv_bounds[0])
		intv_end=intv_start
		if len(intv_bounds) > 1:
			intv_end=int(intv_bounds[1])
		
		return (intv_chr, tuple(sorted([intv_start, intv_end])))
		
	else:
		return (intv_chr, None)

def readIntervals(interval_f):
	intervals = []
	for l in interval_f:
		try:
			intervals.append( parseInterval(l.strip()) )
		except ValueError:
			print >> sys.stderr, "WARNING, cound not parse line '%s', ignoring" % l.strip()
	
	return intervals


def getIntervals(interval_str):
	intervals = []
	interval_f = getFile(interval_str)
	if interval_f is not None:
		intervals = readIntervals(interval_f)
	else:
		for interval in interval_str.split(","):
			try:
				curr_int = interval.split(':')
				if len(curr_int) > 1:
					curr_bound = curr_int[1].split("-")
					intervals.append((curr_int[0], (int(curr_bound[0]), int(curr_bound[1]))))
				else:
					intervals.append((curr_int[0], None))
			except:
				print >> sys.stderr, "WARNING: could not parse interval: '%s', ignoring" % interval

	return intervals
	

def get_bgzf_block(block, block_len=(2**16-1)):
    # print("Saving %i bytes" % len(block))
    
    # if the block is too large, recursively write smaller blocks
    if len(block) > block_len:
    	return get_bgzf_block(block[:block_len], block_len) + get_bgzf_block(block[block_len:], block_len)
    
    # Giving a negative window bits means no gzip/zlib headers, -15 used in samtools
    c = zlib.compressobj(6,
                         zlib.DEFLATED,
                         -15,
                         zlib.DEF_MEM_LEVEL,
                         0)
    compressed = c.compress(block) + c.flush()
    del c
    # If we didn't compress enough, split the block in two and try again
    if len(compressed) > block_len:
    	return get_bgzf_block(filePtr, block[:len(block)/2], block_len) + get_bgzf_block(filePtr, block[len(block)/2:], block_len)

    crc = zlib.crc32(block)
    # Should cope with a mix of Python platforms...
    if crc < 0:
        crc = struct.pack("<i", crc)
    else:
        crc = struct.pack("<I", crc)
    bsize = struct.pack("<H", len(compressed) + 25)  # includes -1
    crc = struct.pack("<I", zlib.crc32(block) & 0xffffffff)
    uncompressed_length = struct.pack("<I", len(block))
    # Fixed 16 bytes,
    # gzip magic bytes (4) mod time (4),
    # gzip flag (1), os (1), extra length which is six (2),
    # sub field which is BC (2), sub field length of two (2),
    # Variable data,
    # 2 bytes: block length as BC sub field (2)
    # X bytes: the data
    # 8 bytes: crc (4), uncompressed data length (4)
    data = _bgzf_header + bsize + compressed + crc + uncompressed_length
    
    return data


def block_gzip(filePtr, data_in, block_len=(2**16-1)):
	'''
	Block gzips the input data into at most block_len characters of decompressed text
	'''
	cpos=0
	# make sure to write at least 1 block (for empty block @ end!)
	nblocks=0
	while nblocks == 0 or cpos<len(data_in):
		nblocks+=1
		epos=min(len(data_in), cpos+block_len) 
		filePtr.write(get_bgzf_block(data_in[cpos:epos], block_len))
		cpos=epos
	
	# If we're here, set the special EOF bit
	if nblocks==0:
		filePtr.write(_bgzf_eof)

def writeInterval(vcf_zfile, vcfidx_data, intv, start_data, out_f):

	intv_chr = intv[0]
	intv_bounds = intv[1]
	intv_start = None
	intv_end = None
	if intv_bounds is not None:
		intv_start = intv_bounds[0]
		intv_end = intv_bounds[1]
		
	vcf_f = vcf_zfile._filePtr

	if intv_chr not in vcfidx_data.name_map:
		print >> sys.stderr, "ERROR: Chromosome", intv_chr, " not in target VCF, exiting"
		return start_data
	
	chrom_ref = vcfidx_data.ref[vcfidx_data.name_map[intv_chr]]
	
	if intv_start is None:
		# we want a whole chromosome
		data_start_vfp = chrom_ref.first_pos
	else:
		# get just the linear index?
		st_index = intv_start/(16*1024)
		if st_index >= len(chrom_ref.ioff):
			print >> sys.stderr, "WARNING: start position not in linear index; interval not in VCF?"
			data_start_vfp = chrom_ref.ioff[-1]
		else:
			data_start_vfp = max(chrom_ref.first_pos, chrom_ref.ioff[st_index])

	# Seek to the start of the bin
	#print >> sys.stderr, "Seeking to beginning of bin"
	vcf_zfile.seek(data_start_vfp)
	cvfp = vcf_zfile.tellvfp()
	cvfp_idx = bisect.bisect_left(vcfidx_data.linearIndex, cvfp)
	curr_pos = 0
	curr_line = ""
	
	# Make sure that we have a good start position (apparently, the tabix index 
	# gets corrupted sometimes)
	currchr_str=vcf_zfile.readline("\t")
	currpos_str=vcf_zfile.readline("\t")
	curr_line = currchr_str + currpos_str
	curr_chr = currchr_str.strip()
	curr_pos = int(currpos_str.strip())
		
	# Now, read lines until we get a line with a position that is >= our interval start
	while intv_start is not None and (curr_chr != intv_chr or curr_pos < intv_start):
		# I need to read the rest of the line to throw away
		curr_line=vcf_zfile.readline()
		
		currchr_str=vcf_zfile.readline("\t")
		currpos_str=vcf_zfile.readline("\t")
		
		#print >> sys.stderr, "Start Line chr/pos value:", currchr_str + currpos_str
		curr_chr = currchr_str.strip()
		# Note: this should NEVER throw an error!
		curr_pos = int(currpos_str.strip())
		
		curr_line = currchr_str+currpos_str
		

	st_pos = curr_pos
	cvfp = vcf_zfile.tellvfp()	
	# Now, cvfp is the offset of the beginning of the region of interest
	# Note: we have already read a line of data, so vcf_zfile.tellvfp() will
	# show something different from cvfp
	
	#print >> sys.stderr, "First line reads:", curr_line
	#print >> sys.stderr, "Current position:", convert_vfp(vcf_zfile.tellvfp())
	#print >> sys.stderr, "Cvfp position:   ", convert_vfp(cvfp)
	
	start_chunk = vcf_zfile.readChunk()
	
	#print >> sys.stderr, curr_line + start_chunk
	block_vfp=vcf_zfile.tellvfp()
	
	#print >> sys.stderr, "Current position:", convert_vfp(vcf_zfile.tellvfp())
	#print >> sys.stderr, "Cvfp position:   ", convert_vfp(cvfp)
	
	# Also, let's get the start position of the bin containing the end of our
	# interval
	data_end_vfp=None
	if intv_start is None:
		data_end_vfp = vcfidx_data.getNextChrom(data_start_vfp)
	elif data_start_vfp is not None:
		end_index = intv_end/(16*1024)
		if end_index >= len(chrom_ref.ioff):
			data_end_vfp = vcfidx_data.getNextChrom(cvfp)
		else:
			# make sure to not get a "0" offset!
			data_end_vfp = max(chrom_ref.first_pos, chrom_ref.ioff[end_index])
			

	if data_end_vfp is not None and data_end_vfp > cvfp:
		#print >> sys.stderr, "Seeking to check", convert_vfp(data_end_vfp)
		vcf_zfile.seek(data_end_vfp)
		
		currchr_str=vcf_zfile.readline("\t")
		currpos_str=vcf_zfile.readline("\t")
		end_line = currchr_str + currpos_str
		curr_chr = currchr_str.strip()
		curr_pos = int(currpos_str.strip())
				
	
	# OK, at this point, I should have the following:
	# curr_line = "chrom<TAB>pos<TAB>" for 1st record >= starting position
	# start_chunk = remainder of chunk after 1st record
	# data_end_vfp = virtual file offset, guaranteed to start at a record <= intv_end
	# cvfp = virtual file offset of position right after curr_line
	# block_vfp = virtual file offset of position right after start_chunk
	
	#print >> sys.stderr, "st. vfp", convert_vfp(cvfp)
	#print >> sys.stderr, "blk vfp", convert_vfp(block_vfp)
	#print >> sys.stderr, "End vfp", convert_vfp(data_end_vfp)
	
	#print >> sys.stderr, "Start offsets:", convert_vfp(data_start_vfp)
	#if data_end_vfp is not None:
	#	print >> sys.stderr, "End offsets:", convert_vfp(data_end_vfp)
	

	# Get the current virtual file offset
	#cfo, cbo = convert_vfp(block_vfp)
		
	# block-gzip our intermediate data and write that to our file
	end_data = ""
			
	cfo, cbo = convert_vfp(cvfp)
	bfo, bbo = convert_vfp(block_vfp)
	
	if data_end_vfp is None or convert_vfp(data_end_vfp)[0] > cfo:
		block_gzip(out_f, start_data + curr_line + start_chunk)
		
		#print >> sys.stderr, "Gzipping data:"
		#print >> sys.stderr, start_data + curr_line + start_chunk
	else:
		vcf_zfile.seek(cvfp)

		if cvfp >= data_end_vfp:
			#print >> sys.stderr, "++st. vfp", convert_vfp(cvfp)
			#print >> sys.stderr, "++blk vfp", convert_vfp(block_vfp)
			#print >> sys.stderr, "++End vfp", convert_vfp(data_end_vfp)
			
			#print >> sys.stderr, "diff:", data_end_vfp - cvfp
			
			rem_data = vcf_zfile.readline()
			#print >> sys.stderr, "Rem line:", rem_data
			end_data = start_data + curr_line + rem_data
			#print >> sys.stderr, "+++ADDING to end_data:"
			#print >> end_data
			#data_end_vfp=vcf_zfile.tellvfp()
			#cvfp=data_end_vfp

	# try to match a DXFile read buffer size, else fall back to 64MB
	try:
		dl_block=vcf_f._read_bufsize
	except:
		dl_block=64*1024*1024
	
	if data_end_vfp is None:
		vcf_f.seek(bfo)
		newDat = vcf_f.read(dl_block)
		#print >> sys.stderr, "Reading", len(newDat), "bytes to end of file"
		while len(newDat) > 0:
			out_f.write(newDat)
			newDat = vcf_f.read(dl_block)
			#print >> sys.stderr, "Reading", len(newDat), "bytes to end of file"
	else:
		efo, ebo = convert_vfp(data_end_vfp)
		
		# If needed, write blocks without decompressing, downloading in 64MB chunks
		end_seek = False
		
		if(efo-cfo) > 0:
			vcf_f.seek(bfo)
			end_seek=True
		
		addlDat=1
		while efo-bfo > 0 and addlDat>0:
			newDat = vcf_f.read(min(efo-bfo,dl_block))
			#print >> sys.stderr, "Reading", len(newDat), "bytes to next offset"
			out_f.write(newDat)
			addlDat=len(newDat)
			bfo+=addlDat
		
		# If we did any writing of raw compressed chunks, let's move to where 
		# we need to start decompressing again
		if end_seek:
			vcf_zfile.seek(convert_offsets(efo,0))
			end_data = vcf_zfile.read(ebo)
			#print >> sys.stderr, "Read", len(end_data), "bytes to end block offset:"
			#print >> sys.stderr, "##st. vfp", convert_vfp(cvfp)
			#print >> sys.stderr, "##blk vfp", convert_vfp(block_vfp)
			#print >> sys.stderr, "##End vfp", convert_vfp(data_end_vfp)
			#print >> sys.stderr, end_data
		elif data_end_vfp > cvfp:
			# here, we did NOT seek, but there's a gap between the cvfp and 
			#where we are now, so let's seek to the cvfp and read until the 
			# data_end_vfp
			
			#print >> sys.stderr, "++st. vfp", convert_vfp(cvfp)
			#print >> sys.stderr, "blk vfp", convert_vfp(block_vfp)
			#print >> sys.stderr, "++End vfp", convert_vfp(data_end_vfp)
			#print >> sys.stderr, "Adding more to end line!"
			vcf_zfile.seek(cvfp)
			curr_line += vcf_zfile.read(convert_vfp(data_end_vfp)[1] - convert_vfp(cvfp)[1])
			#print start_data + curr_line + end_data
			
			end_data = start_data + curr_line + end_data
			
			
	# At this point, vcf_zfile should be at the correct position to start 
	# reading records to add to the end
	
	if intv_end is not None and st_pos > intv_end:
		print >> sys.stderr, "WARNING: Interval not found in VCF file, exiting!"
		end_data = start_data
	else:
		
		curr_pos = 0
		curr_chr = intv_chr
	
		#print >> sys.stderr, "--st. vfp", convert_vfp(cvfp)
		#print >> sys.stderr, "blk vfp", convert_vfp(block_vfp)
		#if data_end_vfp is not None:
		#	print >> sys.stderr, "--End vfp", convert_vfp(data_end_vfp)
		#print >> sys.stderr, "--Cur vfp", convert_vfp(vcf_zfile.tellvfp())
	
		#print >> sys.stderr, "Cur vfp", convert_vfp(vcf_zfile.tellvfp())
		#print >> sys.stderr, "Line:", currchr_str + currpos_str

		# Now, read lines until we get a line with a position that is >= our interval start
		while data_end_vfp is not None and intv_end is not None and curr_pos <= intv_end and curr_chr==intv_chr:
			currchr_str=vcf_zfile.readline("\t")
			currpos_str=vcf_zfile.readline("\t")

			# Make sure that we actually read a line! (this can happen at EOF)
			if len(currchr_str + currpos_str) > 0:
				#p1 = curr_line.find('\t')
				#p2 = curr_line.find('\t',p1+1)
				curr_pos = int(currpos_str.strip())
				curr_chr=currchr_str.strip()
				#print >> sys.stderr, "End Position:", curr_pos, "Exit:", (curr_pos <= intv_end)
			else:
				#print >> sys.stderr, "Saw EOF!"
				# break out of the loop, please!
				curr_pos = intv_end + 1
			
			if curr_pos <= intv_end and curr_chr==intv_chr:
				#print >> sys.stderr, "Adding a line!"
				end_data += currchr_str + currpos_str + vcf_zfile.readline()

	
	return (end_data, data_end_vfp)
	#print "Gzipping end_data:\n", end_data

if __name__ =="__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument("-L", "--interval", help="Single interval of the file to download")
	parser.add_argument("-H", "--header", help="Print VCF header", action="store_true", default=False)
	parser.add_argument("-f", "--vcf", help="The VCF file to split.  Can be either a HTTP/FTP link or a DNANexus file string or a local filename")
	parser.add_argument("-i", "--index", help="The tabix index of the VCF file.  Can be HTTP/FTP link, DNANexus file string or local filename")
	parser.add_argument("-o", "--output", help="Output file")
	parser.add_argument("-K", "--keep-open", help="Do not write the final EOF block (unless reading until end of file)", action="store_true", default=False)
	parser.add_argument("-a", "--append", help="Append to output file (implies no --header)", action="store_true", default=False)
	
	args = parser.parse_args()
			
	# OK, we now have intv_chr, intv_start and intv_end
	
	# OK, now I have vcf_f, idx_f, and a list of intervals
	# First, print the header of the file
	vcf_f = getFile(args.vcf)
	vcfidx_f = getFile(args.index)
	
	vcf_zfile = bgzopen(vcf_f)
	vcfidx_zf=bgzopen(vcfidx_f)
	vcfidx_data = tbi_data(vcfidx_zf)
	
	# make sure the intervals are sorted and non-overlapping
	intervals = getIntervals(args.interval)
	
	# sort the intervals according to the same order as the input VCF file
	# (as determined by the tabix index)
	
	#chrom_order=vcfidx_data.name_map
	intervals.sort(key=lambda x: ( (vcfidx_data.name_map.get(x[0], sys.maxint), x[0]), x[1]) )
	
	intervals = getNonOverlap(intervals)
		
	mode = 'wb'
	if args.append:
		mode = 'ab'
	out_f = file(args.output, mode)
	
	start_data = ""
	
	# So, we're going to do this with the vcf_zfile and vcfidx_data objects
	# if we want to print the header, 
	if args.header and not args.append:
		#print >> sys.stderr, "Print the header, please!"
		start_vfp = vcfidx_data.getNextChrom(0) 
		start_fo, start_bo = convert_vfp(start_vfp)
		#print >> sys.stderr, "Starting vfp:", start_vfp
		#print >> sys.stderr, "Starting off:", convert_vfp(start_vfp)
		if start_fo > 0:
			out_f.write(vcf_f.read(start_fo))
			vcf_f.flush()
			vcf_zfile.seek(convert_offsets(start_fo, 0))

		start_data += vcf_zfile.read(start_bo)
	#else:
	#	print >> sys.stderr, "No header!"

	# at this point, we've written any full header blocks and the final
	# partial header block is in start_data, so let's go find the starting
	# position of the bin containing the start of our interval
	
	
	for intv in intervals:
		(end_data, data_end_vfp) = writeInterval(vcf_zfile, vcfidx_data, intv, start_data, out_f)
		start_data = end_data
	
	# block-gzip the end of the data
	if end_data:
		block_gzip(out_f, end_data)
	
	# if we didn't read to the end, block-gzip an empty block and close
	if not args.keep_open and data_end_vfp is not None:
		block_gzip(out_f, "")
	
	out_f.close()
	
	# and we're done! (make sure to re-tabix the result!)
