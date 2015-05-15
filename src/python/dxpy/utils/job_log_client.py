# Copyright (C) 2013-2014 DNAnexus, Inc.
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
(https://wiki.dnanexus.com/API-Specification-v1.0.0/Logging#API-method%3A-%2Fjob-xxxx%2FgetLog).
'''

from __future__ import print_function, unicode_literals

import json, logging, time

#from ws4py.client.threadedclient import WebSocketClient
from ws4py.client import WebSocketBaseClient

import dxpy
from .describe import get_find_executions_string
from ..exceptions import err_exit

logger = logging.getLogger('ws4py')
logger.setLevel(logging.WARN)

class DXJobLogStreamingException(Exception):
    pass

class DXJobLogStreamClient(WebSocketBaseClient):
    def __init__(self, job_id, input_params=None, msg_output_format="{job} {level} {msg}", msg_callback=None,
                 print_job_info=True):
        self.job_id = job_id
        self.seen_jobs = {}
        self.input_params = input_params
        self.msg_output_format = msg_output_format
        self.msg_callback = msg_callback
        self.print_job_info = print_job_info
        self.closed_code, self.closed_reason = None, None
        ws_proto = 'wss' if dxpy.APISERVER_PROTOCOL == 'https' else 'ws'
        self.url = "{protocol}://{host}:{port}/{job_id}/getLog/websocket".format(protocol=ws_proto,
                                                                                 host=dxpy.APISERVER_HOST,
                                                                                 port=dxpy.APISERVER_PORT,
                                                                                 job_id=job_id)
        WebSocketBaseClient.__init__(self, self.url, protocols=None, extensions=None)

    def handshake_ok(self):
        self.run()

    def opened(self):
        args = {"access_token": dxpy.SECURITY_CONTEXT['auth_token'],
                "token_type": dxpy.SECURITY_CONTEXT['auth_token_type']}
        if self.input_params:
            args.update(self.input_params)
        self.send(json.dumps(args))

    def reconnect(self):
        # Instead of trying to reconnect in a retry loop with backoff, run an API call that will do the same
        # and block while it retries.
        time.sleep(1)
        dxpy.describe(self.job_id)
        WebSocketBaseClient.__init__(self, self.url, protocols=None, extensions=None)
        self.connect()

    def terminate(self):
        if self.stream and self.stream.closing and self.stream.closing.code == 1001 \
           and self.stream.closing.reason == "Server restart, please reconnect later":
            # Clean up state (this is a copy of WebSocket.terminate(), minus the part that calls closed())
            try:
                self.close_connection()
                self.stream._cleanup()
            except:
                pass
            self.stream = None
            self.environ = None

            logger.warn("Server restart, reconnecting...")
            self.reconnect()
        else:
            WebSocketBaseClient.terminate(self)

    def closed(self, code, reason):
        self.closed_code, self.closed_reason = code, reason

        if not (self.closed_code == 1000 or getattr(self.stream.closing, 'code', None) == 1000):
            try:
                error = json.loads(self.closed_reason)
                raise DXJobLogStreamingException("Error while streaming job logs: {type}: {message}\n".format(**error))
            except (KeyError, ValueError):
                error = "Error while streaming job logs: {code} {reason}\n".format(code=self.closed_code,
                                                                                   reason=self.closed_reason)
                raise DXJobLogStreamingException(error)
        elif self.print_job_info:
            if self.job_id not in self.seen_jobs:
                self.seen_jobs[self.job_id] = {}
            for job_id in self.seen_jobs.keys():
                self.seen_jobs[job_id] = dxpy.describe(job_id)
                print(get_find_executions_string(self.seen_jobs[job_id], has_children=False, show_outputs=True))
        else:
            self.seen_jobs[self.job_id] = dxpy.describe(self.job_id)

        if self.seen_jobs[self.job_id].get('state') in ['failed', 'terminated']:
            err_exit(code=3)

    def received_message(self, message):
        message = json.loads(message.__unicode__())

        if self.print_job_info and 'job' in message and message['job'] not in self.seen_jobs:
            self.seen_jobs[message['job']] = dxpy.describe(message['job'])
            print(get_find_executions_string(self.seen_jobs[message['job']], has_children=False, show_outputs=False))

        if message.get('source') == 'SYSTEM' and message.get('msg') == 'END_LOG':
            self.close()
        elif self.msg_callback:
            self.msg_callback(message)
        else:
            print(self.msg_output_format.format(**message))
