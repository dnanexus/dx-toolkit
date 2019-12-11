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

'''
DXDatabase Handler
**************

This remote database handler is a Python database-like object.
'''

from __future__ import print_function, unicode_literals, division, absolute_import

import os, sys, logging, traceback, hashlib, copy, time
import math
import mmap
from threading import Lock
from multiprocessing import cpu_count

import dxpy
from . import DXDataObject
from ..exceptions import DXFileError, DXIncompleteReadsError
from ..utils import warn
from ..utils.resolver import object_exists_in_project
from ..compat import BytesIO, basestring, USING_PYTHON2
from .. import logger


DXFILE_HTTP_THREADS = min(cpu_count(), 8)
MIN_BUFFER_SIZE = 1024*1024
DEFAULT_BUFFER_SIZE = 1024*1024*16
if dxpy.JOB_ID:
    # Increase HTTP request buffer size when we are running within the
    # platform.
    DEFAULT_BUFFER_SIZE = 1024*1024*96

MD5_READ_CHUNK_SIZE = 1024*1024*4
FILE_REQUEST_TIMEOUT = 60


def _validate_headers(headers):
    for key, value in headers.items():
        if not isinstance(key, basestring):
            raise ValueError("Expected key %r of headers to be a string" % (key,))
        if not isinstance(value, basestring):
            raise ValueError("Expected value %r of headers (associated with key %r) to be a string"
                             % (value, key))
    return headers


def _readable_part_size(num_bytes):
    "Returns the file size in readable form."
    B = num_bytes
    KB = float(1024)
    MB = float(KB * 1024)
    GB = float(MB * 1024)
    TB = float(GB * 1024)

    if B < KB:
        return '{0} {1}'.format(B, 'bytes' if B != 1 else 'byte')
    elif KB <= B < MB:
        return '{0:.2f} KiB'.format(B/KB)
    elif MB <= B < GB:
        return '{0:.2f} MiB'.format(B/MB)
    elif GB <= B < TB:
        return '{0:.2f} GiB'.format(B/GB)
    elif TB <= B:
        return '{0:.2f} TiB'.format(B/TB)

def do_debug(msg):
    logger.debug(msg)

class DXDatabase(DXDataObject):
    '''Remote database object handler.

    :param dxid: Object ID
    :type dxid: string
    :param project: Project ID
    :type project: string

    '''

    _class = "database"

    _describe = staticmethod(dxpy.api.file_describe)

    _http_threadpool_size = DXFILE_HTTP_THREADS
    _http_threadpool = dxpy.utils.get_futures_threadpool(max_workers=_http_threadpool_size)

    NO_PROJECT_HINT = 'NO_PROJECT_HINT'

    def __init__(self, dxid=None, project=None, read_buffer_size=DEFAULT_BUFFER_SIZE,
                 expected_file_size=None, file_is_mmapd=False):
        """
        :param dxid: Object ID
        :type dxid: string
        :param project: Project ID
        :type project: string
        :param read_buffer_size: size of read buffer in bytes
        :type read_buffer_size: int
        :param expected_file_size: size of data that will be written, if
            known
        :type expected_file_size: int
        :param file_is_mmapd: True if input file is mmap'd (if so, the
            write buffer size will be constrained to be a multiple of
            the allocation granularity)
        :type file_is_mmapd: bool
        """

        DXDataObject.__init__(self, dxid=dxid, project=project)

        self._read_buf = BytesIO()
        self._read_bufsize = read_buffer_size
        self._expected_file_size = expected_file_size
        self._file_is_mmapd = file_is_mmapd

        # These are cached once for all download threads. This saves calls to the apiserver.
        self._download_url, self._download_url_headers, self._download_url_expires = None, None, None

        # This lock protects accesses to the above three variables, ensuring that they would
        # be checked and changed atomically. This protects against thread race conditions.
        self._url_download_mutex = Lock()

        self._request_iterator, self._response_iterator = None, None
        self._http_threadpool_futures = set()


    def get_download_url(self, filename=None, src_filename=None, project=None, **kwargs):
        """
        :param filename: desired filename of the local downloaded file
        :type filename: str
        :param project: ID of a project containing the file (the download URL
            will be associated with this project, and this may affect which
            billing account is billed for this download).
            If no project is specified, an attempt will be made to verify if the database is
            in the project from the DXDatabase handler (as specified by the user or
            the current project stored in dxpy.WORKSPACE_ID). Otherwise, no hint is supplied.
            This fall back behavior does not happen inside a job environment.
        :type project: str
        :returns: download URL and dict containing HTTP headers to be supplied
            with the request
        :rtype: tuple (str, dict)
        :raises: :exc:`~dxpy.exceptions.ResourceNotFound` if a project context was
            given and the database was not found in that project context.
        :raises: :exc:`~dxpy.exceptions.ResourceNotFound` if no project context was
            given and the database was not found in any projects.

        Obtains a URL that can be used to directly download files associated
        with the specified database.

        """

        args = {}

        if src_filename is not None:
            args["filename"] = src_filename

        # If project=None, we fall back to the project attached to this handler
        # (if any). If this is supplied, it's treated as a hint: if it's a
        # project in which this file exists, it's passed on to the
        # apiserver. Otherwise, NO hint is supplied. In principle supplying a
        # project in the handler that doesn't contain this file ought to be an
        # error, but it's this way for backwards compatibility. We don't know
        # who might be doing downloads and creating handlers without being
        # careful that the project encoded in the handler contains the file
        # being downloaded. They may now rely on such behavior.
        if project is None and 'DX_JOB_ID' not in os.environ:
            project_from_handler = self.get_proj_id()
            if project_from_handler and object_exists_in_project(self.get_id(), project_from_handler):
                project = project_from_handler

        if project is not None and project is not DXDatabase.NO_PROJECT_HINT:
            args["projectContext"] = project

        # Test hook to write 'project' argument passed to API call to a
        # local file
        if '_DX_DUMP_BILLED_PROJECT' in os.environ:
            with open(os.environ['_DX_DUMP_BILLED_PROJECT'], "w") as fd:
                if project is not None and project != DXDatabase.NO_PROJECT_HINT:
                    fd.write(project)

        with self._url_download_mutex:

            if self._download_url is None or self._download_url_expires < time.time():
                # The idea here is to cache a download URL for the entire file, that will
                # be good for a few minutes. This avoids each thread having to ask the
                # server for a URL, increasing server load.
                #
                # To avoid thread race conditions, this check/update procedure is protected
                # with a lock.

                # logging.debug("Download URL unset or expired, requesting a new one")
                if "timeout" not in kwargs:
                    kwargs["timeout"] = FILE_REQUEST_TIMEOUT
                do_debug("dxdatabase get_download_url - download_file args = {}".format(args))
                resp = dxpy.api.database_download_file(self._dxid, args, **kwargs)
                do_debug("dxdatabase get_download_url - download_file resp = {}".format(resp));
                self._download_url = resp["url"]
                self._download_url_headers = _validate_headers(resp.get("headers", {}))
                self._download_url_expires = 32503680000  # doesn't expire (year 3000)

            # Make a copy, ensuring each thread has its own mutable
            # version of the headers.  Note: python strings are
            # immutable, so we can safely give a reference to the
            # download url.
            retval_download_url = self._download_url
            retval_download_url_headers = copy.copy(self._download_url_headers)

        return retval_download_url, retval_download_url_headers
