# Copyright (C) 2013-2016 DNAnexus, Inc.
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
