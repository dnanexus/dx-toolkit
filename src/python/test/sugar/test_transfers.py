from __future__ import print_function, unicode_literals, division, absolute_import
import gzip
import os
from pathlib import Path
import subprocess
import unittest

from . import isolated_dir, make_random_files, random_name
from ..dxpy_testutil import run

import dxpy
from dxpy.sugar import transfers as xfer


class TestUpload(unittest.TestCase):
    project = None

    @classmethod
    def setUpClass(cls):
        cls.project = dxpy.DXProject()
        cls.project.new(name=random_name())

    @classmethod
    def tearDownClass(cls):
        cls.project.destroy()

    def setUp(self):
        self.folder = random_name("/{}")
        self.project.new_folder(self.folder)

    def tearDown(self):
        self.project.remove_folder(self.folder, recurse=True)

    def _assert_zip_equal(self, handler, expected_value="test0"):
        with isolated_dir():
            local_name = random_name("{}.txt.gz")
            dxpy.download_dxfile(handler, local_name)
            with gzip.open(local_name, "rt") as inp:
                self.assertEqual(expected_value, inp.read())

    def _assert_tar_equal(self, handler, filenames):
        """
        Download a tar file and untar it, check that it contains the expected
        files with the expected contents.

        Args:
            handler: File handler of the tarfile to download.
            filenames: Names of files that are expected to match the files in the tar.
        """
        with isolated_dir():
            local_name = random_name("{}.tar.gz")
            dxpy.download_dxfile(handler, local_name)

            run("tar -xzf {}".format(local_name))
            run("rm {}".format(local_name))

            untarred_files = os.listdir(".")
            self.assertEqual(len(filenames), len(untarred_files))

            for i, fname in enumerate(filenames):
                self.assertTrue(os.path.exists(fname))
                with open(fname, "rt") as inp:
                    self.assertEqual("test{}".format(i), inp.read())

    def test_simple_upload_file(self):
        with isolated_dir():
            filename = make_random_files(1, "{}.txt")[0]
            remote_filename = random_name("{}.txt")

            handler = xfer.simple_upload_file(
                filename,
                name=remote_filename,
                folder=self.folder,
                project=self.project.get_id(),
                return_handler=True,
                wait_on_close=True,
            )

            self.assertIsInstance(handler, dxpy.DXFile)
            self.assertEqual(remote_filename, handler.describe()["name"])
            self.assertEqual(self.folder, handler.describe()["folder"])
            self.assertEqual("test0", handler.read())

    def test_compress_and_upload(self):
        with isolated_dir():
            filename = make_random_files(1, "{}.txt")[0]
            remote_filename = random_name("{}.txt.gz")
            try:
                handler = xfer.compress_and_upload_file(
                    filename,
                    name=remote_filename,
                    folder=self.folder,
                    project=self.project.get_id(),
                    return_handler=True,
                    wait_on_close=True,
                )
            except subprocess.CalledProcessError as cpe:
                print(cpe.output)
                raise

            self.assertIsInstance(handler, dxpy.DXFile)
            self.assertEqual(remote_filename, handler.describe()["name"])
            self.assertEqual(self.folder, handler.describe()["folder"])
            self._assert_zip_equal(handler)

    def test_archive_and_upload(self):
        with isolated_dir():
            filenames = make_random_files(2)
            remote_prefix = random_name()
            remote_filename = "{}.tar.gz".format(remote_prefix)

            handler = xfer.tar_and_upload_files(
                filenames,
                prefix=remote_prefix,
                folder=self.folder,
                return_handler=True,
                project=self.project.get_id(),
                wait_on_close=True,
            )

            self.assertIsInstance(handler, dxpy.DXFile)
            self.assertEqual(
                os.path.basename(remote_filename), handler.describe()["name"]
            )
            self.assertEqual(self.folder, handler.describe()["folder"])

        self._assert_tar_equal(handler, filenames)

    def test_uploader(self):
        with isolated_dir():
            plain_file = make_random_files(1)[0]
            to_zip_file = make_random_files(1)[0]
            to_tar_filenames = make_random_files(2)
            dict_files = dict(
                zip(("dict_file_{}".format(i) for i in range(2)), make_random_files(2))
            )

            with xfer.Uploader(
                max_parallel=1,
                project=self.project.get_id(),
                wait_on_close=True,
                return_handler=True,
            ) as up:
                up.enqueue_file("plain_file", plain_file, skip_compress=True)
                up.enqueue_file("zip_file", to_zip_file)
                up.enqueue_list(
                    "tar_file", to_tar_filenames, archive=True, prefix="test"
                )
                up.enqueue_dict(dict_files, skip_compress=True)
                result = up.wait()

            self.assertEqual(5, len(result))
            self.assertEqual(
                {"plain_file", "zip_file", "tar_file", "dict_file_0", "dict_file_1"},
                set(result.keys()),
            )
            for key, val in result.items():
                self.assertIsInstance(val, dxpy.DXFile)

            self.assertEqual("test0", result["plain_file"].read())

            for i in range(2):
                self.assertEqual(
                    "test{}".format(i), result["dict_file_{}".format(i)].read()
                )

            self._assert_tar_equal(result["tar_file"], to_tar_filenames)


class TestDownload(unittest.TestCase):
    project = None

    @classmethod
    def setUpClass(cls):
        cls.project = dxpy.DXProject()
        cls.project.new(name=random_name())

    @classmethod
    def tearDownClass(cls):
        cls.project.destroy()

    def setUp(self):
        self.folder = random_name("/{}")
        self.project.new_folder(self.folder)

    def tearDown(self):
        self.project.remove_folder(self.folder, recurse=True)

    def _upload_file(self, filename):
        return dxpy.upload_local_file(
            filename,
            wait_on_close=True,
            project=self.project.get_id(),
            folder=self.folder,
        )

    def _upload_simple_file(self, filename="test.txt"):
        with isolated_dir():
            with open(filename, "wt") as out:
                out.write("test")
            return self._upload_file(filename)

    def _upload_zip_file(self):
        with isolated_dir():
            with gzip.open("test.txt.gz", "wt") as out:
                out.write("test")
            return self._upload_file("test.txt.gz")

    def _upload_tar_file(self, compress=False):
        if compress:
            tar_filename = "test.tar.gz"
            opts = "czf"
        else:
            tar_filename = "test.tar"
            opts = "cf"
        with isolated_dir():
            filenames = make_random_files(2)
            run("tar {} {} {}".format(opts, tar_filename, " ".join(str(f) for f in filenames)))
            return filenames, self._upload_file(tar_filename)

    def test_simple_download_file(self):
        handler = self._upload_simple_file()
        with isolated_dir():
            xfer.simple_download_file(handler, "test")
            with open("test", "rt") as inp:
                self.assertEqual("test", inp.read())

    def test_download_and_decompress_file(self):
        handler = self._upload_zip_file()
        with isolated_dir():
            unpacked_file = xfer.download_and_decompress_file(handler)
            with open(unpacked_file, "rt") as inp:
                self.assertEqual("test", inp.read())

    def test_download_and_unpack_archive(self):
        for compress in (True, False):
            tar_filenames, handler = self._upload_tar_file(compress=compress)
            with isolated_dir():
                unpacked_filenames = xfer.download_and_unpack_archive(handler)
                self.assertEqual(2, len(unpacked_filenames))
                self.assertSetEqual(
                    set(Path(path).absolute() for path in tar_filenames),
                    set(unpacked_filenames),
                )
                for i, fname in enumerate(tar_filenames):
                    with open(fname, "rt") as inp:
                        self.assertEqual("test{}".format(i), inp.read())

    def test_downloader(self):
        simple_handler1 = self._upload_simple_file("test1.txt")
        simple_handler2 = self._upload_simple_file("test2.txt")
        zip_handler = self._upload_zip_file()
        tar_filenames, tar_handler = self._upload_tar_file()

        with isolated_dir():
            with xfer.Downloader(max_parallel=1, project=self.project.get_id()) as down:
                down.enqueue_list("simple", [simple_handler1, simple_handler2])
                down.enqueue_dict({"zip": zip_handler, "tar": tar_handler})
                result = down.wait()

            self.assertIn("zip", result)
            with open(result["zip"], "rt") as inp:
                self.assertEqual("test", inp.read())

            self.assertIn("simple", result)
            for i, fname in enumerate(result["simple"]):
                with open(fname, "rt") as inp:
                    self.assertEqual("test".format(i), inp.read())

            self.assertIn("tar", result)
            unpacked_filenames = result["tar"]
            self.assertEqual(2, len(unpacked_filenames))
            self.assertSetEqual(
                set(Path(path).absolute() for path in tar_filenames),
                set(unpacked_filenames),
            )
            for i, fname in enumerate(tar_filenames):
                with open(fname, "rt") as inp:
                    self.assertEqual("test{}".format(i), inp.read())
