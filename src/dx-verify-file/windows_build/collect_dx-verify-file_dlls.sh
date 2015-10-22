#!/bin/sh -ex
#
# Copyright (C) 2013-2015 DNAnexus, Inc.
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

pwd
cd $1
pwd
SWPATH=${HOME}/sw/local
cp $SWPATH/curl/bin/libcurl-4.dll .
cp $SWPATH/zlib-1.2.3-bin/bin/zlib1.dll .
cp $SWPATH/regex-2.7-bin/bin/regex2.dll .
cp /bin/msys-1.0.dll .
cp /bin/msys-crypto-1.0.0.dll .
