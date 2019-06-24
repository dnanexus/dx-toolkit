from __future__ import print_function, unicode_literals, division, absolute_import
import unittest

import dxpy
from dxpy.sugar import chunking


DATA_FILES = {
    "file-F5JFjb80K2yZB6JFPKQvfy3y": (369942,    0),
    "file-F6X4Z1Q05Y4x2z9g9qgpQVfP": (921688957, 1),
    "file-F74zxf005F9y9jYJ6kbQqyBf": (439068741, 2),
    "file-F69bp1Q0xG8v79K429jffKPZ": (289,       0),
    "file-F5JFzyQ037pV0Q41Gj453xYy": (4023512,   0),
    "file-F796xbj0jY6VpgxJFy8q5xpz": (431058109, 0),
    "file-FKGp6700xG8fP7gG1BQ4XjF2": (18066045,  0),
    "file-F5JGXgj0X64qfXPK87jG6b9v": (366622,    0),
    "file-F69bkyj0xG8bG1Jq29v3BjPG": (284,       0),
}


class TestChunking(unittest.TestCase):
    def test_divide_dxfiles_into_chunks(self):
        groups = chunking.divide_dxfiles_into_chunks(
            (dxpy.dxlink(f) for f in DATA_FILES.keys()),
            sum(v[0] for v in DATA_FILES.values()) / 3 / chunking.BYTES_PER_GB
        )
        assert len(groups) == 3
        for i in range(3):
            assert len(groups[i]) == len(list(filter(
                lambda v: v[1] == i, DATA_FILES.values()
            )))
            assert set(groups[i]) == set(
                k for k, v in DATA_FILES.items() if v[1] == i
            )
