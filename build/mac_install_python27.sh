#!/bin/bash -e
#
# Copyright (C) 2013-2017 DNAnexus, Inc.
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

# On OS X/macOS, installs Python 2.7 with an OpenSSL that supports TLS 1.2.

# Get home directory location
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done
export DNANEXUS_HOME="$( cd -P "$( dirname "$SOURCE" )" && pwd )/.."

if $DNANEXUS_HOME/build/tls12check.py ; then
    echo ""
    echo "Your Python release does not need an upgrade for TLS 1.2 support; exiting."
    exit 0
fi

echo ""
echo "Your version of Python must be upgraded in order to support TLS 1.2."
echo ""
echo "This script can upgrade Python for you (using the Homebrew package manager)."
echo ""
echo "Note:"
echo " * This script is meant for use on OS X 10.10 and higher."
echo " * Homebrew will prompt for your OS user password to begin the installation."
echo " * Please see https://brew.sh/ for more information on Homebrew."
echo ""

read -r -p "Would you like to perform the Python upgrade? [y/N] " response
case "$response" in
    [yY][eE][sS]|[yY]) 
        /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
        brew install python
        ;;
    *)
        echo "Exiting without changes."
        ;;
esac
