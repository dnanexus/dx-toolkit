#!/bin/bash -e
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

# Usage: fix_shebang_lines.sh DIRNAME [--debian-system-install] [interpreter]
#   Rewrites shebang lines for all Python scripts in DIRNAME.

dirname=$1
if [[ $dirname == "" ]]; then
    dirname="."
fi

msg="Please source the environment file at the root of dx-toolkit."
if [[ $2 == "--debian-system-install" ]]; then
    msg="Please source the environment file /etc/profile.d/dnanexus.environment."
    shift
fi

# * Setuptools bakes the path of the Python interpreter into all
#   installed Python scripts. Rewrite it back to the more portable form,
#   since we don't always know where the right interpreter is on the
#   target system.
interpreter="/usr/bin/env python"
if [[ $2 != "" ]]; then
    interpreter=$2
fi

for f in "$dirname"/*; do
    if head -n 1 "$f" | egrep -iq "(python|pypy)"; then
        echo "Rewriting $f to use portable interpreter paths"
        perl -i -pe 's|^#!/.+|'"#!$interpreter"'| if $. == 1' "$f"
    fi
done
