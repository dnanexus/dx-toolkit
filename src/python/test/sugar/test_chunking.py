import unittest

from dxpy.sugar import chunking


DATA_FILES = [
    ("file-F5JFjb80K2yZB6JFPKQvfy3y", 369942, 1),
    ("file-F6X4Z1Q05Y4x2z9g9qgpQVfP", 921688957, 0),
    ("file-F74zxf005F9y9jYJ6kbQqyBf", 439068741, 1),
    ("file-F69bp1Q0xG8v79K429jffKPZ", 289, 1),
    ("file-F5JFzyQ037pV0Q41Gj453xYy", 4023512, 1),
    ("file-F796xbj0jY6VpgxJFy8q5xpz", 431058109, 2),
    ("file-FKGp6700xG8fP7gG1BQ4XjF2", 18066045, 2),
    ("file-F5JGXgj0X64qfXPK87jG6b9v", 366622, 1),
    ("file-F69bkyj0xG8bG1Jq29v3BjPG", 284, 1),
]


class TestChunking(unittest.TestCase):
    def test_divide_dxfiles_into_chunks(self):
        target_size_gb = sum(d[1] for d in DATA_FILES) / 3 / chunking.BYTES_PER_GB
        groups = chunking.divide_dxfiles_into_chunks(
            DATA_FILES, target_size_gb, lambda x: x[1]
        )
        assert len(groups) == 3
        for i in range(3):
            assert len(groups[i]) == len(list(filter(lambda d: d[2] == i, DATA_FILES)))
            assert set(groups[i]) == set(d for d in DATA_FILES if d[2] == i)
