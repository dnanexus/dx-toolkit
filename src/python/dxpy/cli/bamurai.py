import sys
import struct
import dxpy

BAI_MAGIC_WORD = 'BAI\x01'
PSEUDO_BIN = 37450
LAST_SENTINEL_BLOCK_SIZE = 28
# Maximum size of block per HTTP connection
FETCHSIZE = dxpy.dxfile.DEFAULT_BUFFER_SIZE

OFFSET_BIT_PADDING = 16
OFFSET_LSB_MASK = (1 << OFFSET_BIT_PADDING) - 1
MAX_BGZIP_BLOCK_SIZE = 1 << OFFSET_BIT_PADDING

def read_bai(bai_fn):
    ret_val = {'chromosomes': [], 'n_no_coor': None}

    try:
        with open(bai_fn, 'rb') as fh:
            # All bai files should begin with BAI\x01
            semaphore = fh.read(4)
            if semaphore != BAI_MAGIC_WORD:
                print 'BAI file did not contain proper semaphore.'
                raise IOError

            # The next 4 bytes will tell us how many reference sequences are present
            dword = fh.read(4)
            num_ref_seqs = struct.unpack('<l', dword)[0]
            # Now loop over info about ref seqs in the bai file.
            for i in xrange(num_ref_seqs):
                dword = fh.read(4)
                num_bins = struct.unpack('<l', dword)[0]
                ret_val['chromosomes'].append({'bins': [], 'offsets': []})
                for j in xrange(num_bins):
                    dword = fh.read(4)
                    bin = struct.unpack('<L', dword)[0]
                    ret_val['chromosomes'][-1]['bins'].append({'bin': bin, 'chunks': []})

                    dword = fh.read(4)
                    num_chunks = struct.unpack('<l', dword)[0]
                    for k in xrange(num_chunks):
                        qword = fh.read(8)
                        chunk_beg = struct.unpack('<Q', qword)[0]

                        qword = fh.read(8)
                        chunk_end = struct.unpack('<Q', qword)[0]
                        ret_val['chromosomes'][-1]['bins'][-1]['chunks'].append((chunk_beg, chunk_end))

                dword = fh.read(4)
                num_intervals = struct.unpack('<l', dword)[0]
                for j in xrange(num_intervals):
                    qword = fh.read(8)
                    offset = struct.unpack('<Q', qword)[0]
                    ret_val['chromosomes'][-1]['offsets'].append(offset)

            # The n_no_coor field is optional
            qword = fh.read(8)
            if qword:
                ret_val['n_no_coor'] = struct.unpack('<Q', qword)[0]
    except:
        print 'Problem reading .bai file.  Are you sure it was formatted properly?'
        raise

    return ret_val

def write_bai(bai_info, bai_fn):
    with open(bai_fn, 'wb') as fh:
        fh.write(BAI_MAGIC_WORD)

        dword = struct.pack('<l', len(bai_info['chromosomes']))
        fh.write(dword)

        for chr in bai_info['chromosomes']:
            dword = struct.pack('<l', len(chr['bins']))
            fh.write(dword)
            for bin in chr['bins']:
                dword = struct.pack('<L', bin['bin'])
                fh.write(dword)

                dword = struct.pack('<l', len(bin['chunks']))
                fh.write(dword)

                for chunk in bin['chunks']:
                    qword = struct.pack('<Q', chunk[0])
                    fh.write(qword)
                    qword = struct.pack('<Q', chunk[1])
                    fh.write(qword)

            dword = struct.pack('<l', len(chr['offsets']))
            fh.write(dword)
            for offset in chr['offsets']:
                qword = struct.pack('<Q', offset)
                fh.write(qword)

        if 'n_no_coor' in bai_info:
            qword = struct.pack('<Q', bai_info['n_no_coor'])
            fh.write(qword)

def find_bam_file_region_of_interest(bai_info, chr_lo, chr_hi):
    '''Find the lo and hi file regions that contain reads mapped to the given region.'''
    lo, hi = (sys.maxint, 0)
    # chr_lo, chr_hi is a closed interval on both ends.
    for chr in bai_info['chromosomes'][chr_lo:chr_hi+1]:
        for bin in chr['bins']:
            if bin['bin'] == PSEUDO_BIN:
                continue
            for chunk in bin['chunks']:
                if lo > chunk[0]:
                    lo = chunk[0]
                if hi < chunk[1]:
                    hi = chunk[1]

    return lo, hi

def get_bam_windows_to_fetch(lo, hi, bam_size):
    '''Given these file regions, form the windows of the bam that  we need
    to fetch.'''
    windows = []
    # First, if we didn't actually find a lowest file region, this signifies that
    # we didn't find any bam reads that map to our chromosomes of interest.
    if lo == sys.maxint:
        offset_adjust = 0
        # If the bam size is smaller than one full bgzip block and final sentinel
        # just fetch the whole bam.
        # Otherwise, fetch the first bgzip block (has the header), and the final
        # sentinel since we didn't find any reads that match our chromosomes of
        # interest.
        if bam_size <= MAX_BGZIP_BLOCK_SIZE + LAST_SENTINEL_BLOCK_SIZE:
            windows.append((0, bam_size-1))
        else:
            windows.append((0, MAX_BGZIP_BLOCK_SIZE-1))
            windows.append((bam_size-LAST_SENTINEL_BLOCK_SIZE, bam_size-1))
    else:
        lo = lo >> OFFSET_BIT_PADDING
        hi = (hi >> OFFSET_BIT_PADDING) + MAX_BGZIP_BLOCK_SIZE - 1
        # If hi is past the last bgzip block and into the
        # sentinel, just grab the whole bam.
        if hi >= bam_size - 1 - LAST_SENTINEL_BLOCK_SIZE:
            hi = bam_size - 1
        # If lo is less than a bgzip block, then start at the first byte,
        # since we'll be grabbing the first block anyway for the bam header.
        # Otherwise, grab at least the first block (which we assume will
        # have the full bam header in it).
        if lo <= MAX_BGZIP_BLOCK_SIZE:
            lo = 0
            offset_adjust = 0
        else:
            windows.append((0, MAX_BGZIP_BLOCK_SIZE-1))
            # There will be the first block w/ bam header and then this new region.
            offset_adjust = lo - MAX_BGZIP_BLOCK_SIZE

        # Now loop over from the lo to hi areas and make note to fetch
        # windows in this region.
        for curr_lo in xrange(lo, hi+1, FETCHSIZE):
            curr_hi = curr_lo + FETCHSIZE - 1
            if curr_hi > hi:
                curr_hi = hi
            windows.append((curr_lo, curr_hi))

        # Now if we haven't already fetched the last portion of the bam
        # which contains the end sentinel, do that too.
        if hi != bam_size-1:
            windows.append((bam_size-LAST_SENTINEL_BLOCK_SIZE, bam_size-1))

    return {'windows': windows,
            'offset_adjust': offset_adjust}

def prune_bai_info(chr_lo, chr_hi, bai_info, offset_adjust):
    '''Take the existing bai info and remove all but the info for the
    data we are actually fetching from the bam.'''

    # First, remove info for chromosomes other than those of interest.
    for i, chr in enumerate(bai_info['chromosomes']):
        if (i < chr_lo) or (i > chr_hi):
            chr['bins'] = []
            chr['offsets'] = []

    # Now loop over the chromosomes and adjust their chunk addresses
    # to reflect the fact that we will not necessarily be fetching the same
    # amount of data before this.
    for chr in bai_info['chromosomes']:
        # First, we'll remove any pseudo bins from our bai info.
        chr['bins'] = [bin for bin in chr['bins'] if bin['bin'] != PSEUDO_BIN]
        for bin in chr['bins']:
            # Now take the existing offsets and shift them down to reflect our new starting point.
            # Note that we should keep the 16 LSB's and just shift the 48 MSB's.
            for i, chunk in enumerate(bin['chunks']):
                new_lo = (((chunk[0] >> OFFSET_BIT_PADDING) - offset_adjust) << OFFSET_BIT_PADDING) + (chunk[0] & OFFSET_LSB_MASK)
                new_hi = (((chunk[1] >> OFFSET_BIT_PADDING) - offset_adjust) << OFFSET_BIT_PADDING) + (chunk[1] & OFFSET_LSB_MASK)
                bin['chunks'][i] = (new_lo, new_hi)

        for i, offset in enumerate(chr['offsets']):
            if offset != 0:
                chr['offsets'][i] = (((offset >> OFFSET_BIT_PADDING) - offset_adjust) << OFFSET_BIT_PADDING) + (offset & OFFSET_LSB_MASK)

    return bai_info

def download_file_windows(dxf, ofn, windows, show_progress=True):
    def print_progress(bytes_downloaded, file_size):
        num_ticks = 60

        effective_file_size = file_size or 1
        if bytes_downloaded > effective_file_size:
            effective_file_size = bytes_downloaded

        ticks = int(round((bytes_downloaded / float(effective_file_size)) * num_ticks))
        percent = int(round((bytes_downloaded / float(effective_file_size)) * 100))

        fmt = "[{done}{pending}] Downloaded {done_bytes:,}{remaining} bytes ({percent}%) {name}"
        sys.stderr.write(fmt.format(done=('=' * (ticks - 1) + '>') if ticks > 0 else '',
                                    pending=' ' * (num_ticks - ticks),
                                    done_bytes=bytes_downloaded,
                                    remaining=' of {size:,}'.format(size=file_size) if file_size else "",
                                    percent=percent,
                                    name=ofn))
        sys.stderr.flush()
        sys.stderr.write("\r")
        sys.stderr.flush()

    total_size = sum([w[1]-w[0]+1 for w in windows])

    bytes_downloaded = 0
    with open(ofn, 'wb') as fh:
        for window in windows:
            if show_progress:
                print_progress(bytes_downloaded, total_size)
            dxf.seek(window[0])
            bytes_to_fetch = window[1] - window[0] + 1
            fh.write(dxf.read(bytes_to_fetch))
            bytes_downloaded += bytes_to_fetch
        if show_progress:
            print_progress(bytes_downloaded, total_size)


def bamuraize(dx_bai, dx_bam, chr_lo, chr_hi, show_progress=True):
    # First, fetch the bai file
    bam_description = dx_bam.describe()
    bai_fn = dx_bai.describe()['name']
    dxpy.dxfile_functions.download_dxfile(dx_bai.get_id(), bai_fn)

    # Now parse the bai file and find the low and high region of the bam file
    # that we need to fetch.
    bai_info = read_bai(bai_fn)
    lo, hi = find_bam_file_region_of_interest(bai_info, chr_lo, chr_hi)

    # Now get a set of windows to fetch.
    ret_val = get_bam_windows_to_fetch(lo, hi, bam_description['size'])

    # Modify the bai file to reflect the data in our new bam file.
    bai_info = prune_bai_info(chr_lo, chr_hi, bai_info, ret_val['offset_adjust'])
    write_bai(bai_info, bai_fn)

    # Now actually fetch the bam file.
    download_file_windows(dx_bam, bam_description['name'], ret_val['windows'], show_progress)

def get_input_dxfile(input_file):
    if input_file.startswith('file-'):
        f = dxpy.DXFile(input_file, mode='r')
    else:
        f = dxpy.find_one_data_object(zero_ok=False,
                                      more_ok=False,
                                      name=input_file,
                                      name_mode='glob',
                                      classname='file')
        f = dxpy.DXFile(dxpy.dxlink(f), mode='r')

    return f

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print 'Usage: bamuraize.py <input.bai> <input.bam> <chr lo> <chr hi>'
        sys.exit(1)
    input_bai = get_input_dxfile(sys.argv[1])
    input_bam = get_input_dxfile(sys.argv[2])
    chr_lo = int(sys.argv[3]) - 1
    chr_hi = int(sys.argv[4]) - 1

    bamuraize(input_bai, input_bam, chr_lo, chr_hi)
