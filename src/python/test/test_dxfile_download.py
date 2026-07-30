#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2013-2019 DNAnexus, Inc.
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

import base64
import hashlib
import os
import tempfile
import unittest
import zlib
import crc32c
from collections import defaultdict
from mock import patch
from awscrt import checksums
import dxpy
from dxpy.bindings.dxfile import DXFile
from dxpy.bindings import dxfile_functions
from dxpy.exceptions import DXChecksumMismatchError

class TestGetDownloadUrlSecurityWarning(unittest.TestCase):
    FILE_ID = 'file-xxxx'
    DOWNLOAD_URL = 'https://dl.dnanexus.com/F/D/file-xxxx'
    WARNING_MESSAGE = 'WARNING: Potentially malicious file detected. Verify the source before viewing or executing.'

    def _make_dxfile(self):
        # Pass dxid=None to skip ID format validation in __init__,
        # then set _dxid directly to the test file ID.
        dxfile = DXFile()
        dxfile._dxid = self.FILE_ID
        return dxfile

    def _base_resp(self, **extra):
        resp = {"url": self.DOWNLOAD_URL, "headers": {}}
        resp.update(extra)
        return resp

    def test_no_security_status_no_warning(self):
        """No warning emitted when security_status is absent from the response."""
        dxfile = self._make_dxfile()
        with patch('dxpy.api.file_download', return_value=self._base_resp()) as mock_dl, \
                patch('dxpy.bindings.dxfile.warn') as mock_warn, \
                patch('dxpy.bindings.dxfile.object_exists_in_project', return_value=False):
            dxfile.get_download_url()
            mock_warn.assert_not_called()

    def test_flagged_malicious_emits_warning(self):
        """Warning is emitted when security_status is FLAGGED_MALICIOUS."""
        dxfile = self._make_dxfile()
        with patch('dxpy.api.file_download', return_value=self._base_resp(security_status='FLAGGED_MALICIOUS')) as mock_dl, \
                patch('dxpy.bindings.dxfile.warn') as mock_warn, \
                patch('dxpy.bindings.dxfile.object_exists_in_project', return_value=False):
            dxfile.get_download_url()
            mock_warn.assert_called_once()
            warned_message = mock_warn.call_args[0][0]
            self.assertEqual(self.WARNING_MESSAGE, warned_message)

    def test_flagged_malicious_download_still_proceeds(self):
        """Download URL is still returned even when security_status is FLAGGED_MALICIOUS."""
        dxfile = self._make_dxfile()
        with patch('dxpy.api.file_download', return_value=self._base_resp(security_status='FLAGGED_MALICIOUS')), \
                patch('dxpy.bindings.dxfile.warn'), \
                patch('dxpy.bindings.dxfile.object_exists_in_project', return_value=False):
            url, headers = dxfile.get_download_url()
            self.assertEqual(url, self.DOWNLOAD_URL)
            self.assertIsInstance(headers, dict)

    def test_warning_emitted_only_once_when_url_cached(self):
        """Warning is only emitted on the first call; subsequent calls use the cached URL and do not re-warn."""
        dxfile = self._make_dxfile()
        with patch('dxpy.api.file_download', return_value=self._base_resp(security_status='FLAGGED_MALICIOUS')) as mock_dl, \
                patch('dxpy.bindings.dxfile.warn') as mock_warn, \
                patch('dxpy.bindings.dxfile.object_exists_in_project', return_value=False):
            dxfile.get_download_url()
            dxfile.get_download_url()
            # file_download API called once; warn called once
            mock_dl.assert_called_once()
            mock_warn.assert_called_once()


class TestDownloadPerPartChecksumGating(unittest.TestCase):
    """TITAN-244: on a symlink/drive file, per-part checksum verification is
    skipped when the part carries an md5 (md5 is the single integrity check),
    and still runs as a fallback when the part has no md5."""

    FILE_ID = 'file-xxxx'
    DRIVE = 'drive-xxxx'
    CHUNK = b'hello world'

    def _make_dxfile(self):
        dxfile = DXFile()
        dxfile._dxid = self.FILE_ID
        return dxfile

    def _run_download(self, part):
        dxfile = self._make_dxfile()
        describe_output = {
            'parts': {'1': part},
            'size': len(self.CHUNK),
            'drive': self.DRIVE,
            'checksumType': 'CRC64NVME',
        }
        fd, filename = tempfile.mkstemp()
        os.close(fd)
        os.remove(filename)  # ensure "rb+" open fails -> "wb" -> main download loop
        try:
            with patch.object(dxfile_functions, 'response_iterator',
                              return_value=[('1', self.CHUNK)]), \
                    patch.object(dxfile_functions, '_compare_part_checksum') as mock_verify:
                dxfile_functions._download_dxfile(
                    dxfile, filename, defaultdict(lambda: 3),
                    describe_output=describe_output)
            return mock_verify
        finally:
            if os.path.exists(filename):
                os.remove(filename)

    def _run_resume_preflight(self, part):
        dxfile = self._make_dxfile()
        describe_output = {
            'parts': {'1': part},
            'size': len(self.CHUNK),
            'drive': self.DRIVE,
            'checksumType': 'CRC64NVME',
        }
        fd, filename = tempfile.mkstemp()
        os.close(fd)
        try:
            with open(filename, 'wb') as fh:
                fh.write(self.CHUNK)
            with patch.object(dxfile_functions, 'response_iterator',
                              return_value=[]), \
                    patch.object(dxfile_functions, '_compare_part_checksum') as mock_verify:
                dxfile_functions._download_dxfile(
                    dxfile, filename, defaultdict(lambda: 3),
                    describe_output=describe_output)
            return mock_verify
        finally:
            if os.path.exists(filename):
                os.remove(filename)

    def test_checksum_skipped_when_md5_present(self):
        part = {
            'size': len(self.CHUNK),
            'md5': hashlib.md5(self.CHUNK).hexdigest(),
            'checksum': '688cIX1wosY=',
        }
        mock_verify = self._run_download(part)
        mock_verify.assert_not_called()

    def test_md5_verified_when_checksum_skipped(self):
        dxfile = self._make_dxfile()
        part = {
            'size': len(self.CHUNK),
            'md5': hashlib.md5(b'different data').hexdigest(),
            'checksum': '688cIX1wosY=',
        }
        describe_output = {
            'parts': {'1': part},
            'size': len(self.CHUNK),
            'drive': self.DRIVE,
            'checksumType': 'CRC64NVME',
        }
        fd, filename = tempfile.mkstemp()
        os.close(fd)
        os.remove(filename)  # ensure "rb+" open fails -> "wb" -> main download loop
        try:
            with patch.object(dxfile_functions, 'response_iterator',
                              return_value=[('1', self.CHUNK)]), \
                    patch.object(dxfile_functions, '_compare_part_checksum') as mock_verify:
                with self.assertRaises(DXChecksumMismatchError):
                    dxfile_functions._download_dxfile(
                        dxfile, filename, defaultdict(lambda: 1),
                        describe_output=describe_output)
            mock_verify.assert_not_called()
        finally:
            if os.path.exists(filename):
                os.remove(filename)

    def test_resume_preflight_checksum_skipped_when_md5_present(self):
        part = {
            'size': len(self.CHUNK),
            'md5': hashlib.md5(self.CHUNK).hexdigest(),
            'checksum': '688cIX1wosY=',
        }
        mock_verify = self._run_resume_preflight(part)
        mock_verify.assert_not_called()

    def test_checksum_verified_when_md5_absent(self):
        part = {
            'size': len(self.CHUNK),
            'checksum': '688cIX1wosY=',
        }
        mock_verify = self._run_download(part)
        mock_verify.assert_called_once()


class TestDownloadMultiChunkChecksum(unittest.TestCase):
    """Reproduces the checksum mismatch seen on v0.404.0 for a symlink/drive
    file whose (whole-file) checksum lives on part 1.

    When a part is larger than the download chunk size, the download loop in
    ``_download_dxfile`` splits it into multiple ``chunksize`` chunks. The bug
    was that the per-part checksum was computed over only the *first* chunk
    instead of the whole part, so a perfectly intact download failed with
    DXChecksumMismatchError. This affected every supported checksum type, not
    just CRC64NVME, because the bug was in the download loop and not in any
    type-specific logic -- so all types are exercised here.
    """

    FILE_ID = 'file-xxxx'
    DRIVE = 'drive-xxxx'

    # All checksum types supported by _verify_checksum / _IncrementalChecksum.
    CHECKSUM_TYPES = ('CRC32', 'CRC32C', 'SHA1', 'SHA256', 'CRC64NVME')

    # 64 bytes of non-uniform data so the checksum of the first chunk differs
    # from the checksum of the whole part.
    DATA = bytes((i * 7 + 3) & 0xFF for i in range(64))
    CHUNK_SIZE = 16  # -> part 1 splits into 4 chunks

    def _make_dxfile(self):
        dxfile = DXFile()
        dxfile._dxid = self.FILE_ID
        return dxfile

    @staticmethod
    def _digest(checksum_type, data):
        if checksum_type == 'CRC32':
            return zlib.crc32(data).to_bytes(4, 'big')
        if checksum_type == 'CRC32C':
            return crc32c.crc32c(data).to_bytes(4, 'big')
        if checksum_type == 'SHA1':
            return hashlib.sha1(data).digest()
        if checksum_type == 'SHA256':
            return hashlib.sha256(data).digest()
        if checksum_type == 'CRC64NVME':
            return checksums.crc64nvme(data).to_bytes(8, 'big')
        raise ValueError(checksum_type)

    def _whole_checksum_b64(self, checksum_type):
        return base64.b64encode(self._digest(checksum_type, self.DATA)).decode()

    def _run(self, checksum_type, chunksize):
        """Drive the *real* chunking/response_iterator/_verify_checksum path,
        serving byte ranges out of an in-memory buffer instead of HTTP."""
        dxfile = self._make_dxfile()
        part = {'size': len(self.DATA), 'checksum': self._whole_checksum_b64(checksum_type)}
        describe_output = {
            'parts': {'1': part},
            'size': len(self.DATA),
            'drive': self.DRIVE,
            'checksumType': checksum_type,
        }

        def fake_read_range(url, headers, start, end, timeout, sub_range=True):
            return self.DATA[start:end + 1]

        fd, filename = tempfile.mkstemp()
        os.close(fd)
        os.remove(filename)  # force "wb" open -> main download loop (not rb+ resume)
        try:
            with patch.object(DXFile, 'get_download_url', return_value=('http://dummy', {})), \
                    patch.object(dxpy, '_dxhttp_read_range', side_effect=fake_read_range):
                dxfile_functions._download_dxfile(
                    dxfile, filename, defaultdict(lambda: 1),
                    chunksize=chunksize, describe_output=describe_output)
            with open(filename, 'rb') as fh:
                return fh.read()
        finally:
            if os.path.exists(filename):
                os.remove(filename)

    def test_sanity_data_and_checksum_are_valid(self):
        """For every type, the whole-part checksum genuinely matches the whole
        data while only the first chunk disagrees. Proves the failures below are
        about chunking, not corrupt data."""
        for checksum_type in self.CHECKSUM_TYPES:
            with self.subTest(checksum_type=checksum_type):
                expected = base64.b64decode(self._whole_checksum_b64(checksum_type))
                whole = self._digest(checksum_type, self.DATA)
                first_chunk = self._digest(checksum_type, self.DATA[:self.CHUNK_SIZE])
                self.assertEqual(whole, expected)
                self.assertNotEqual(first_chunk, expected)

    def test_single_chunk_part_downloads_ok(self):
        """Control: when the part fits in one chunk, verification passes and
        the file is written correctly, for every checksum type."""
        for checksum_type in self.CHECKSUM_TYPES:
            with self.subTest(checksum_type=checksum_type):
                result = self._run(checksum_type, chunksize=len(self.DATA))
                self.assertEqual(result, self.DATA)

    def test_multi_chunk_part_downloads_ok(self):
        """The bug: identical, intact data must download successfully even when
        it is split into several chunks. Before the fix this raised
        DXChecksumMismatchError (only the first chunk was checksummed); it now
        passes because the checksum is accumulated across all chunks. Verified
        for every supported checksum type."""
        for checksum_type in self.CHECKSUM_TYPES:
            with self.subTest(checksum_type=checksum_type):
                result = self._run(checksum_type, chunksize=self.CHUNK_SIZE)
                self.assertEqual(result, self.DATA)


if __name__ == '__main__':
    unittest.main()
