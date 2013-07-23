import re
import string

COMPLEMENT = string.maketrans("ATGCatgc", "TACGTACG")

SEQ_PATTERN = re.compile('[ACGTacgtNn]*$')

def reverse_complement(seq):
    if isinstance(seq, unicode):
        bytes_seq = seq.encode('utf-8')
    else:
        bytes_seq = seq
    if not SEQ_PATTERN.match(bytes_seq):
        raise ValueError('Sequence %r must consist only of A, C, G, T, N' % (seq,))
    return bytes_seq.translate(COMPLEMENT)[::-1]
