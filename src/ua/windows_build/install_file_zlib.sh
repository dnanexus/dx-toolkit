#!/bin/sh -ex
#
# Copyright (C) 2013 DNAnexus, Inc.
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

cd ${HOME}/sw/local
pwd

wget 'http://sourceforge.net/projects/gnuwin32/files/file/5.03/file-5.03-lib.zip/download'
unzip file-5.03-lib.zip -d file-5.03-lib/
rm file-5.03-lib.zip 

wget 'http://sourceforge.net/projects/gnuwin32/files/file/5.03/file-5.03-bin.zip/download'
unzip file-5.03-bin.zip -d file-5.03-bin/
rm file-5.03-bin.zip 

wget 'http://sourceforge.net/projects/gnuwin32/files/zlib/1.2.3/zlib-1.2.3-lib.zip/download'
unzip zlib-1.2.3-lib.zip -d zlib-1.2.3-lib/
rm zlib-1.2.3-lib.zip 

wget 'http://sourceforge.net/projects/gnuwin32/files/zlib/1.2.3/zlib-1.2.3-bin.zip/download'
unzip zlib-1.2.3-bin.zip -d zlib-1.2.3-bin/
rm zlib-1.2.3-bin.zip

wget 'http://sourceforge.net/projects/gnuwin32/files/regex/2.7/regex-2.7-bin.zip/download'
unzip regex-2.7-bin.zip -d regex-2.7-bin/
rm regex-2.7-bin.zip
