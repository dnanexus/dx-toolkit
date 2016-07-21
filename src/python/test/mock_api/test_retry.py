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

from BaseHTTPServer import BaseHTTPRequestHandler
import json
import datetime
import re

testing_mode = None
testing_stats = None

class MockHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        global testing_mode
        global testing_stats

        m = re.match("/set_testing_mode/(\\w+)", self.path)
        if not m:
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(testing_stats))
            return
        new_mode = m.groups()[0]
        if new_mode not in ['500', '500_fail', '503', '503_retry_after', '503_mixed', '503_mixed_limited', 'mixed']:
            self.send_response(500)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write("Invalid mode 01: '{}'".format(new_mode))
            return
        testing_mode = new_mode
        testing_stats = {'postRequests': []}
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'mode': testing_mode}))

    def do_POST(self):
        global testing_mode
        global testing_stats

        testing_stats['postRequests'].append({
                'timestamp': datetime.datetime.now().isoformat(),
                'client_address': self.client_address,
                'command': self.command,
                'path': self.path,
                'request_version': self.request_version,})
        if testing_mode == '500':
            if len(testing_stats['postRequests']) > 5:
                raise Exception('Too many requests')
            elif len(testing_stats['postRequests']) == 5:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write('{ "id": "user-johnsmith" }')
            else:
                self.send_response(500)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write('500: Internal Server Error')
        elif testing_mode == '500_fail':
            if len(testing_stats['postRequests']) > 7:
                raise Exception('Too many requests')
            else:
                self.send_response(500)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write('500: Internal Server Error')
        elif testing_mode == '503':
            if len(testing_stats['postRequests']) > 5:
                raise Exception('Too many requests')
            elif len(testing_stats['postRequests']) == 5:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write('{ "id": "user-johnsmith" }')
            else:
                self.send_response(503)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write('503: Service Unavailable')
        elif testing_mode == '503_retry_after':
            if len(testing_stats['postRequests']) > 5:
                raise Exception('Too many requests')
            elif len(testing_stats['postRequests']) == 5:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write('{ "id": "user-johnsmith" }')
            else:
                self.send_response(503)
                self.send_header('Content-type', 'text/plain')
                self.send_header('Retry-After', len(testing_stats['postRequests']))
                self.end_headers()
                self.wfile.write('503: Service Unavailable')
        elif testing_mode == '503_mixed':
            if len(testing_stats['postRequests']) > 5:
                raise Exception('Too many requests')
            elif len(testing_stats['postRequests']) == 5:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write('{ "id": "user-johnsmith" }')
            else:
                self.send_response(503)
                self.send_header('Content-type', 'text/plain')
                if len(testing_stats['postRequests']) == 3:
                    self.send_header('Retry-After', '2')
                self.end_headers()
                self.wfile.write('503: Service Unavailable')
        elif testing_mode == '503_mixed_limited':
            if len(testing_stats['postRequests']) > 12:
                raise Exception('Too many requests')
            elif len(testing_stats['postRequests']) == 12:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write('{ "id": "user-johnsmith" }')
            else:
                self.send_response(503)
                self.send_header('Content-type', 'text/plain')
                if len(testing_stats['postRequests']) < 11:
                    self.send_header('Retry-After', '1')
                self.end_headers()
                self.wfile.write('503: Service Unavailable')
        elif testing_mode == 'mixed':
            if len(testing_stats['postRequests']) > 5:
                raise Exception('Too many requests')
            elif len(testing_stats['postRequests']) == 5:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write('{ "id": "user-johnsmith" }')
            elif len(testing_stats['postRequests']) == 3:
                self.send_response(500)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write('500: Internal Server Error')
            else:
                self.send_response(503)
                self.send_header('Content-type', 'text/plain')
                if len(testing_stats['postRequests']) == 4:
                    self.send_header('Retry-After', '2')
                self.end_headers()
                self.wfile.write('503: Service Unavailable')
        else:
            self.send_response(500)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write("Invalid mode 02: '{}'".format(testing_mode))
