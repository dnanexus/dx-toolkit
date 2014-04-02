import re
from ..compat import USING_PYTHON2, str

if USING_PYTHON2:
    import string
    maketrans = string.maketrans
else:
    maketrans = bytes.maketrans

COMPLEMENT = maketrans(b"ATGCatgc", b"TACGTACG")

SEQ_PATTERN = re.compile(b'[ACGTacgtNn]*$')

def reverse_complement(seq):
    if isinstance(seq, str):
        bytes_seq = seq.encode('utf-8')
    else:
        bytes_seq = seq
    if not SEQ_PATTERN.match(bytes_seq):
        raise ValueError('Sequence %r must consist only of A, C, G, T, N' % (seq,))
    return bytes_seq.translate(COMPLEMENT)[::-1]
