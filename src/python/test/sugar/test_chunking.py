from __future__ import print_function, unicode_literals, division, absolute_import
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
import unittest

from . import isolated_dir, random_name
import test.dxpy_testutil as testutil
from dxpy.sugar import chunking
import dxpy


class TestChunking(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.project = dxpy.DXProject()
        cls.project.new(name=random_name())

        cls.data_sizes = [
            10 << 10,
            10 << 10,
            10 << 10,
            15 << 10,
            15 << 10,
            30 << 10,
        ]

        cls.total_size = sum(s for s in cls.data_sizes)

        # populate decribes on data files, so that size is available
        with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
            futures = [executor.submit(cls._upload_fake_file, size) for size in cls.data_sizes]
            cls.data_dxfiles = [f.result() for f in as_completed(futures)]

    @classmethod
    def tearDownClass(cls):
        cls.project.destroy()

    @classmethod
    def _upload_fake_file(cls, size):
        """Create empty files of set size on disk."""
        with testutil.TemporaryFile(prefix=random_name()) as tf:
            tf.temp_file.truncate(size)
            dxf = dxpy.upload_local_file(tf.name, wait_on_close=True, project=cls.project.get_id())
            # populate decribes on data files, so that size is available
            _ = dxf.describe()
            return dxf

    def test_divide_files_into_chunks(self):
        expected_size_groups = {
            3: [15 << 10, 10 << 10, 10 << 10],
            2: [15 << 10, 10 << 10],
            1: [30 << 10],
        }

        groups = chunking.divide_files_into_chunks(
            file_descriptors=self.data_dxfiles,
            target_size_gb=30. / 1024. / 1024.
        )

        for i, group in enumerate(groups):
            print("\nGroup", i)
            for f in group:
                print(f.size)

        assert len(groups) == 3

        for group in groups:
            group_size = len(group)
            sizes = [dxf.size for dxf in group]
            assert sizes == expected_size_groups[group_size]
