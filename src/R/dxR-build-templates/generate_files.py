#!/usr/bin/env python2.7
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

# This script is meant to be run from $DNANEXUS_HOME/src

import os, sys

template_files = [
    {"template": os.path.join('R', 'dxR-build-templates', 'dxR-package.R'),
     "newpath": os.path.join('R', 'dxR', 'R', 'dxR-package.R')
     }
]

version = sys.argv[1]
if version.startswith('v'):
    version = version[1:]
if '-' in version:
    version = version[:version.find('-')]

with open(os.path.join('R', 'dxR', 'DESCRIPTION'), 'r+') as desc_fd:
    lines = desc_fd.readlines()
    desc_fd.seek(0)
    for line in lines:
        if line.startswith("Version: "):
            desc_fd.write("Version: " + version + "\n")
        else:
            desc_fd.write(line)
    desc_fd.truncate()

for item in template_files:
    with open(item['template'], 'r') as template_fd, open(item['newpath'], 'w') as new_fd:
        new_fd.write(template_fd.read().replace('VERSION', version))
