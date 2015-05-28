#!/bin/bash -ex
#
# Copyright (C) 2013-2014 DNAnexus, Inc.
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

# Installs Python 2.7 (required for running dx-toolkit) into /usr/local.
#
# <Tested on CentOS 6.2>

# Short-circuit sudo when running as root. In a chrooted environment we are
# likely to be running as root already, and sudo may not be present on minimal
# installations.

if [ "$USER" == "root" ]; then
    MAYBE_SUDO=''
else
    MAYBE_SUDO='sudo'
fi

$MAYBE_SUDO yum groupinstall -y "Development tools"
$MAYBE_SUDO yum install -y zlib-devel bzip2-devel openssl-devel ncurses-devel readline

# Install Python 2.7.9, setuptools, and pip.

TEMPDIR=$(mktemp -d)

pushd $TEMPDIR
curl -L -O https://www.python.org/ftp/python/2.7.9/Python-2.7.9.tar.xz
tar -xf Python-2.7.9.tar.xz
cd Python-2.7.9
./configure --prefix=/usr/local
make
$MAYBE_SUDO make altinstall

PYTHON=/usr/local/bin/python2.7

cd ..

curl -O https://pypi.python.org/packages/source/s/setuptools/setuptools-1.1.6.tar.gz
tar -xzf setuptools-1.1.6.tar.gz
(cd setuptools-1.1.6; $MAYBE_SUDO $PYTHON setup.py install)

curl -O https://pypi.python.org/packages/source/p/pip/pip-1.4.1.tar.gz
tar -xzf pip-1.4.1.tar.gz
(cd pip-1.4.1; $MAYBE_SUDO $PYTHON setup.py install)

popd
# rm -rf $TEMPDIR
