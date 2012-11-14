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

class DXJobLogStreamClient(WebSocketBaseClient):
    def __init__(self, url, filter_stream=None, protocols=None, extensions=None):
        self.filter_stream = filter_stream
        self.seen_jobs = set()
        super(DXJobLogStreamClient, self).__init__(url, protocols=protocols, extensions=extensions)

    def handshake_ok(self):
        self.run()

    def set_dx_streaming_id(self, dx_streaming_id):
        self.dx_streaming_id = dx_streaming_id

    def opened(self):
        self.send('5:1::' + json.dumps({"name": "streamingId", "args": [self.dx_streaming_id]}))

    def closed(self, code, reason):
        print "Log socket closed:", code, reason
        for job_id in self.seen_jobs:
            print get_find_jobs_string(dxpy.describe(job_id), has_children=False)

    def received_message(self, message):
        message_string = str(message)
        if message_string == '7:::1+0':
            raise HandshakeError("Server suggests reconnect")
        elif message_string == '2::':
            # Ping/Echo
            self.send('2::')
            return

        if not re.match("^([^:]+):([^:]*):([^:]*):(.+)", message_string):
            return

        msg_type, msg_id, msg_endpoint, msg_data = re.match("^([^:]+):([^:]*):([^:]*):(.+)", message_string).groups()
        if msg_type == '5':
            try:
                msg_content = json.loads(msg_data)
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
            except:
                print "Error while decoding message:", str(msg_content)
