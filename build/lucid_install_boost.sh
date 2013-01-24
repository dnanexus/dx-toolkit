#!/bin/bash -ex
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

# Installs boost 1.48 (required for dx C++ executables) into /usr/local.
#
# <Tested on Ubuntu 10.04>
#
# Relevant bits go into:
# /usr/local/lib/libboost_filesystem.so.1.48.0
# /usr/local/lib/libboost_program_options.so.1.48.0
# /usr/local/lib/libboost_regex.so.1.48.0
# /usr/local/lib/libboost_system.so.1.48.0
# /usr/local/lib/libboost_thread.so.1.48.0

# Short-circuit sudo when running as root. In a chrooted environment we are
# likely to be running as root already, and sudo may not be present on minimal
# installations.
if [ "$USER" == "root" ]; then
  MAYBE_SUDO=''
else
  MAYBE_SUDO='sudo'
fi

$MAYBE_SUDO apt-get install --yes g++ curl

TEMPDIR=$(mktemp -d)

pushd $TEMPDIR
curl -O http://superb-dca2.dl.sourceforge.net/project/boost/boost/1.48.0/boost_1_48_0.tar.bz2
tar -xjf boost_1_48_0.tar.bz2
cd boost_1_48_0
./bootstrap.sh --with-libraries=filesystem,program_options,regex,system,thread
# --layout=tagged installs libraries with the -mt prefix.
$MAYBE_SUDO ./b2 --layout=tagged install

popd
# rm -rf $TEMPDIR
