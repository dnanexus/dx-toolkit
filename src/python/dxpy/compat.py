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

from __future__ import print_function, unicode_literals, division, absolute_import

import sys, locale, threading, hashlib

try:
    sys_encoding = locale.getdefaultlocale()[1] or "UTF-8"
except Exception:
    sys_encoding = "UTF-8"

USING_PYTHON2 = True if sys.version_info < (3, 0) else False

basestring = (str, bytes)

THREAD_TIMEOUT_MAX = threading.TIMEOUT_MAX

# Support FIPS enabled Python
def md5_hasher():
    try:
        md5_hasher = hashlib.new('md5', usedforsecurity=False)
    except:
        md5_hasher = hashlib.new('md5')
    return md5_hasher
