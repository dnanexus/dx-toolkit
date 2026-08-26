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

from __future__ import print_function, unicode_literals, division, absolute_import

import unittest
import tempfile
import dxpy
from mock import MagicMock, patch
from dxpy.bindings.dxfile_functions import _verify_checksum
from dxpy.bindings.dxfile_functions import upload_local_file
from dxpy.bindings.dxfile import DXFile
from dxpy.exceptions import DXFileError, DXChecksumMismatchError


class TestUploadLocalFile(unittest.TestCase):
    @patch('dxpy.bindings.dxfile_functions.new_dxfile')
    def test_auth_reaches_parts_polling_without_timeout(self, new_dxfile):
        handler = MagicMock()
        handler._write_bufsize = 1024
        new_dxfile.return_value = handler
        auth = {'auth_token': 'request-token', 'auth_token_type': 'Bearer'}
        security_context = dxpy.SECURITY_CONTEXT

        with tempfile.NamedTemporaryFile() as local_file:
            local_file.write(b'test data')
            local_file.flush()
            upload_local_file(filename=local_file.name, auth=auth, timeout=17,
                              keep_open=True)

        self.assertIs(dxpy.SECURITY_CONTEXT, security_context)
        handler.wait_until_parts_uploaded.assert_called_once_with(auth=auth)


class TestDXFileRead(unittest.TestCase):
    @patch('dxpy.bindings.dxfile.DXFile._read2', return_value=b'file contents')
    @patch('dxpy.DXHTTPRequest')
    def test_auth_reaches_project_existence_check(self, dxhttp_request, read):
        file_id = 'file-123456789012345678901234'
        project_id = 'project-123456789012345678901234'
        dxhttp_request.return_value = {'project': project_id}
        dxfile = DXFile(file_id, project=project_id, mode='rb')
        auth = {'auth_token': 'request-token', 'auth_token_type': 'Bearer'}
        security_context = dxpy.SECURITY_CONTEXT

        self.assertEqual(dxfile.read(auth=auth), b'file contents')

        dxhttp_request.assert_called_once_with(
            '/' + file_id + '/describe', {'project': project_id},
            always_retry=True, auth=auth)
        self.assertIs(dxpy.SECURITY_CONTEXT, security_context)

    @patch('dxpy.api.file_download', return_value={'url': 'https://download', 'headers': {}})
    @patch('dxpy.DXHTTPRequest', return_value={'project': 'project-123456789012345678901234'})
    def test_download_url_forwards_auth_to_project_existence_check(self, dxhttp_request, file_download):
        file_id = 'file-123456789012345678901234'
        project_id = 'project-123456789012345678901234'
        dxfile = DXFile(file_id, project=project_id, mode='rb')
        auth = {'auth_token': 'request-token', 'auth_token_type': 'Bearer'}
        security_context = dxpy.SECURITY_CONTEXT

        dxfile.get_download_url(auth=auth)

        dxhttp_request.assert_called_once_with(
            '/' + file_id + '/describe', {'project': project_id},
            always_retry=True, auth=auth)
        self.assertIs(dxpy.SECURITY_CONTEXT, security_context)

class TestVerifyPerPartChecksum(unittest.TestCase):
    def setUp(self):
      self.valid_crc32_parts = {'1': {'checksum': '0kY1xw=='}}
      self.valid_crc32c_parts = {'1': {'checksum': 'Hu2dEw=='}}
      self.valid_sha1_parts = {'1': {'checksum': 'yl8CzxRPiurWmuYjQ3ySrSeaCAE='}}
      self.valid_sha256_parts = {'1': {'checksum': 'TuNtcBmfMMIRMpTEjVWVnAVSG56/K+nL2nF2rYi67y0='}}
      self.valid_crc64nvme_parts = {'1': {'checksum': '688cIX1wosY='}}
      self.dx_file_id = 'file-xxxx'
      self.chunk_data = 'fizzbuzz'.encode('utf-8')

    def test_per_part_checksum_is_none(self):
        assert _verify_checksum(self.valid_crc32_parts, '1', self.chunk_data, None, self.dx_file_id) == None

    def test_part_id_is_invalid(self):
        with self.assertRaisesRegex(DXFileError, 'Part 5 not found in file-xxxx'):
          _verify_checksum(self.valid_crc32_parts, '5', self.chunk_data, 'CRC32', self.dx_file_id)

    def test_invalid_checksum(self):
        with self.assertRaisesRegex(DXFileError, 'Unsupported checksum type: ABC'):
          _verify_checksum(self.valid_crc32_parts, '1', self.chunk_data, 'ABC', self.dx_file_id)

    def test_checksum_not_found(self):
       with self.assertRaisesRegex(DXFileError, 'checksum not found in part 1'):
          _verify_checksum({'1': {}}, '1', self.chunk_data, 'CRC32', self.dx_file_id)

    def test_valid_crc32_checksum(self):
        assert _verify_checksum(self.valid_crc32_parts, '1', self.chunk_data, 'CRC32', self.dx_file_id) == True

    def test_valid_crc32c_checksum(self):
        assert _verify_checksum(self.valid_crc32c_parts, '1', self.chunk_data, 'CRC32C', self.dx_file_id) == True

    def test_valid_sha1_checksum(self):
        assert _verify_checksum(self.valid_sha1_parts, '1', self.chunk_data, 'SHA1', self.dx_file_id) == True

    def test_valid_sha256_checksum(self):
        assert _verify_checksum(self.valid_sha256_parts, '1', self.chunk_data, 'SHA256', self.dx_file_id) == True

    def test_valid_crc64nvme_checksum(self):
        assert _verify_checksum(self.valid_crc64nvme_parts, '1', self.chunk_data, 'CRC64NVME', self.dx_file_id) == True

    def test_checksum_mismatch(self):
       with self.assertRaisesRegex(DXChecksumMismatchError, '^CRC32 checksum mismatch in file-xxxx in part 1 '):
          _verify_checksum(self.valid_crc32_parts, '1', 'foobar'.encode('utf-8'), 'CRC32', self.dx_file_id)

if __name__ == '__main__':
    unittest.main()
