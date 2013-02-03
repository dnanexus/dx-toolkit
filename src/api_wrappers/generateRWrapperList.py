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

import sys, json

for method in json.loads(sys.stdin.read()):
    route, signature, opts = method
    method_name = signature.split("(")[0]
    retry = "TRUE" if opts['retryable'] else "FALSE"
    if opts['objectMethod']:
        if "app-xxxx" in route:
            args_list = "appNameOrID, alias = NULL, inputParams = RJSONIO::emptyNamedList, jsonifyData = TRUE, alwaysRetry = {retry}".format(retry=retry)
        else:
            args_list = "objectID, inputParams = RJSONIO::emptyNamedList, alwaysRetry = {retry}".format(retry=retry)
    else:
        args_list = "inputParams = RJSONIO::emptyNamedList, jsonifyData = TRUE, always_retry = {retry}".format(retry=retry)
    print route, method_name, args_list
