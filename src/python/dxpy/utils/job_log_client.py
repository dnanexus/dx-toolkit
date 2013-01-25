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

'''
Utilities for client-side usage of the streaming log API
(http://wiki.dnanexus.com/API-Specification-v1.0.0/Logging#API-method%3A-%2Fjob-xxxx%2FstreamLog).
'''

import json, re

#from ws4py.client.threadedclient import WebSocketClient
from ws4py.client import WebSocketBaseClient
from ws4py.exc import HandshakeError

import dxpy
from dxpy.utils.describe import get_find_jobs_string

class DXJobLogStreamingException(Exception):
    pass

class DXJobLogStreamClient(WebSocketBaseClient):
    def __init__(self, job_id, input_params={}, msg_output_format="{job} {level} {msg}", msg_callback=None, print_job_info=True):
        self.seen_jobs = {}
        self.input_params = input_params
        self.msg_output_format = msg_output_format
        self.msg_callback = msg_callback
        self.print_job_info = print_job_info
        self.closed_code, self.closed_reason = None, None
        ws_proto = 'wss' if dxpy.APISERVER_PROTOCOL == 'https' else 'ws'
        url = "{protocol}://{host}:{port}/{job_id}/streamLog/websocket".format(protocol=ws_proto,
                                                                        host=dxpy.APISERVER_HOST,
                                                                        port=dxpy.APISERVER_PORT,
                                                                        job_id=job_id)
        WebSocketBaseClient.__init__(self, url, protocols=None, extensions=None)

    def handshake_ok(self):
        self.run()

    def opened(self):
        args = {"access_token": dxpy.SECURITY_CONTEXT['auth_token'],
                "token_type": dxpy.SECURITY_CONTEXT['auth_token_type']}
        args.update(self.input_params)
        self.send(json.dumps(args))

    def closed(self, code, reason):
        self.closed_code, self.closed_reason = code, reason

        if self.closed_code != 1000:
            try:
                error = json.loads(self.closed_reason)
                raise DXJobLogStreamingException("Error while streaming job logs: {type}: {message}\n".format(**error))
            except (KeyError, ValueError):
                error = "Error while streaming job logs: {code} {reason}\n".format(code=self.closed_code,
                                                                                   reason=self.closed_reason)
                raise DXJobLogStreamingException(error)
        elif self.print_job_info:
            for job_id in self.seen_jobs:
                print get_find_jobs_string(dxpy.describe(job_id), has_children=False)

    def received_message(self, message):
        message = json.loads(str(message))

        if self.print_job_info and message.get('job') not in self.seen_jobs:
            self.seen_jobs[message['job']] = dxpy.describe(message['job'])
            print get_find_jobs_string(self.seen_jobs[message['job']], has_children=False)

        if self.msg_callback:
            self.msg_callback(message)
        else:
            print self.msg_output_format.format(**message)

'''
                if msg_content["name"] == "log":
                    log_level, job_id, orig_msg = re.match("^\[.+?\] APP (\w+) \[(job-.+?)\] (.+)", msg_content["args"][0]).groups()

                    if self.filter_stream == 'stdout':
                        result = re.match("^stdout:(.+)", orig_msg)
                        if result is not None:
                            print result.group(1)
                    elif self.filter_stream == 'stderr':
                        result = re.match("^stderr:(.+)", orig_msg)
                        if result is not None:
                            print result.group(1)
                    else:
                        if job_id not in self.seen_jobs:
                            print get_find_jobs_string(dxpy.describe(job_id), has_children=False)
                            self.seen_jobs.add(job_id)
                        print msg_content["args"][0]
                elif msg_content["name"] == "system" and msg_content["args"][0] == "All Jobs completed":
                    self.jobs_completed = True
            except:
                print "Error while decoding message:", str(msg_content)
'''
