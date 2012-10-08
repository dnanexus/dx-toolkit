import os, sys, json, re

#from ws4py.client.threadedclient import WebSocketClient
from ws4py.client import WebSocketBaseClient
from ws4py.exc import HandshakeError

import dxpy
from dxpy.utils.describe import get_find_jobs_string

class DXJobLogStreamClient(WebSocketBaseClient):
    def handshake_ok(self):
        self.run()

    def set_dx_streaming_id(self, dx_streaming_id):
        self.dx_streaming_id = dx_streaming_id
        self.seen_jobs = set()
        self.num_received_messages = 0

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
            self.num_received_messages += 1
            try:
                msg_content = json.loads(msg_data)
                if (msg_content["name"] == "log"):
                    job_id_match = re.match("^\[.+?\] APP \w+ \[(job-.+?)\]", msg_content["args"][0])
                    if job_id_match is not None:
                        job_id = job_id_match.group(1)
                        if job_id not in self.seen_jobs:
                            print get_find_jobs_string(dxpy.describe(job_id), has_children=False)
                            self.seen_jobs.add(job_id)
                    print msg_content["args"][0]
            except:
                print "Error while decoding message:", str(msg_content)
