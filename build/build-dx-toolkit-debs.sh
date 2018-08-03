#!/bin/bash -ex
#
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


DEFAULT_RELEASE="precise"
TARGET_RELEASE=""
if [[ $# -eq 1 ]] ; then
    if [[ $1 == "trusty" ]] || [[ $1 == "xenial" ]]; then
        TARGET_RELEASE=$1
    else
        echo "Unsupported target release codename: $1"
        exit 1
    fi
fi

# Resolve symlinks so we can find the package root
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done
root="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

echo $root

cd "$root/.."

# Build dx-toolkit (stable)
git reset --hard
git clean -dxf
debuild --no-lintian --no-tgz-check -us -uc

if [ -n "$TARGET_RELEASE" ]; then
	# Replace distribution in .changes
	sed -i -e "s/$DEFAULT_RELEASE/$TARGET_RELEASE/g" $(find / -maxdepth 1 -name "dx-*.changes" -type f)
	# Rename all output files for this build's ubuntu release target
	rename "s/$DEFAULT_RELEASE/$TARGET_RELEASE/g" $(find / -maxdepth 1 -name "dx*" -type f)
fi
