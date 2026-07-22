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

import hashlib
import os
import tempfile
import unittest
from collections import defaultdict
from mock import patch
from dxpy.bindings.dxfile import DXFile
from dxpy.bindings import dxfile_functions

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
                    patch.object(dxfile_functions, '_verify_checksum') as mock_verify:
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

    def test_checksum_verified_when_md5_absent(self):
        part = {
            'size': len(self.CHUNK),
            'checksum': '688cIX1wosY=',
        }
        mock_verify = self._run_download(part)
        mock_verify.assert_called_once()


if __name__ == '__main__':
    unittest.main()
