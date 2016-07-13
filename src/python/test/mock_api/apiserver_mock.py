#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

import sys
import imp
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

DEFAULT_PORT_NUMBER = 8080

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit("Usage: {} <your_mock_handler_definition.py> [tcp_port]".format(sys.argv[0]))

    mh = imp.load_source('MockHandler', sys.argv[1])
    tcp_port = DEFAULT_PORT_NUMBER if len(sys.argv) <= 2 else int(sys.argv[2])

    try:
        server = HTTPServer(('', tcp_port), mh.MockHandler)
        print("HTTP-server started on {} TCP-port".format(tcp_port))
        server.serve_forever()
    except KeyboardInterrupt:
        print("Keyboard interrupt received, shutting down the web server")
        server.socket.close()
