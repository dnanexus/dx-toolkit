#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 DNAnexus, Inc.
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

import sys
from urllib.request import build_opener, ProxyHandler


def main():
    opener = build_opener(ProxyHandler({}))
    with opener.open(sys.argv[1], timeout=10) as response:
        response.read()
    if len(sys.argv) > 2:
        with open(sys.argv[2], "w") as marker:
            marker.write("complete\n")


if __name__ == "__main__":
    main()
