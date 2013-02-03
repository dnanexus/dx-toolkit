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
    {"template": os.path.join('R', 'dxR-build-templates', 'DESCRIPTION'),
     "newpath": os.path.join('R', 'dxR', 'DESCRIPTION')
     },
    {"template": os.path.join('R', 'dxR-build-templates', 'dxR-package.Rd'),
     "newpath": os.path.join('R', 'dxR', 'man', 'dxR-package.Rd')
     }
]

version = sys.argv[1]
if 'v' in version:
    version = version[1:]
if '-' in version:
    version = version[:version.find('-')]

for item in template_files:
    with open(item['template'], 'r') as template_fd, open(item['newpath'], 'w') as new_fd:
        new_fd.write(template_fd.read().replace('VERSION', version))

help_template = '''\\name{METHOD}
\\alias{METHOD}
\\title{METHOD API wrapper}
\\description{
This function makes an API call to the \\code{ROUTE} API method; it is a simple wrapper around the \\code{\\link{dxHTTPRequest}} function which makes POST HTTP requests to the API server.
}
\\usage{
METHOD(SIGARGS)
}
\\arguments{
ARGSHELP
}
\\value{
If the API call is successful, the parsed JSON of the API server response is returned (using \\code{RJSONIO::fromJSON}).
}
\\seealso{
\\code{\\link{dxHTTPRequest}}
}
'''

# Append API wrapper exports to NAMESPACE template and generate help file for each API wrapper

with open(os.path.join('R', 'dxR-build-templates', 'NAMESPACE'), 'r') as template_fd, \
        open(os.path.join('R', 'dxR-build-templates', 'list-of-api-wrappers.txt'), 'r') as wrappers_fd, \
        open(os.path.join('R', 'dxR', 'NAMESPACE'), 'w') as new_fd:
    new_fd.write(template_fd.read())
    new_fd.write("\n# API Wrapper Exports (auto-generated)\n\n")
    for line in wrappers_fd:
        words = line.split()
        route = words[0]
        method = words[1]
        args = words[2:]
        new_fd.write("export(" + method + ")\n")

        with open(os.path.join('R', 'dxR', 'man', method + ".Rd"), 'w') as man_fd:
            args_help = []
            for arg in args:
                if arg.startswith('objectID'):
                    args_help.append("\\item{objectID}{DNAnexus object ID}")
                elif arg.startswith('appNameOrID'):
                    args_help.append("\\item{appNameOrID}{An app identifier using either the name of an app (\"app-name\") or its full ID (\"app-xxxx\")}")
                elif arg.startswith('alias'):
                    args_help.append("\\item{alias}{If an app name is given for \\code{appNameOrID}, this can be provided to specify a version or tag (if not provided, the \"default\" tag is used).}")
                elif arg.startswith('inputParams'):
                    args_help.append("\\item{inputParams}{Either an R object that will be converted into JSON using \\code{RJSONIO::toJSON} to be used as the input to the API call.  If providing the JSON string directly, you must set \\code{jsonifyData} to \\code{FALSE}.}")
                elif arg.startswith('jsonifyData'):
                    args_help.append("\\item{jsonifyData}{Whether to call \\code{RJSONIO::toJSON} on \\code{inputParams} to create the JSON string or pass through the value of \\code{inputParams} directly.  (Default is \\code{TRUE}.)}")
                elif arg.startswith("alwaysRetry"):
                    args_help.append("\\item{alwaysRetry}{Whether to always retry even when no response is received from the API server}")
            man_fd.write(help_template.replace("METHOD", method).replace("ROUTE", route).replace("SIGARGS", " ".join(args)).replace("ARGSHELP", "\n".join(args_help)))
