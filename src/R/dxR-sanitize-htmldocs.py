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
#
# Use this script to programmatically modify the R HTML docs that are
# generated.

import os

with open(os.path.join('..', 'doc', 'R', 'html', '00Index.html'), 'r+') as index_fd:
    lines = index_fd.readlines()
    index_fd.seek(0)
    for line in lines:
        if ".jpg" in line:
            continue
        else:
            index_fd.write(line)
    index_fd.truncate()
