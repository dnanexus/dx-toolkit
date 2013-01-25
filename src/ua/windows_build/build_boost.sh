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

# This script builds boost libraries required for Upload Agent

cd ${HOME}/sw/local
pwd

wget "http://downloads.sourceforge.net/project/boost/boost/1.51.0/boost_1_51_0.tar.bz2"
bunzip2 boost_1_51_0.tar.bz2
tar xvf boost_1_51_0.tar
cd boost_1_51_0

./bootstrap.sh --with-toolset=mingw
sed -e s/gcc/mingw/ project-config.jam > project-config.jam
./b2 toolset=gcc --link=static --threding=multi --with-chrono --with-filesystem --with-thread --with-program_options --with-system --with-regex
