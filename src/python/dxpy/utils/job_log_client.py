import os, sys, json, re
from ws4py.client.threadedclient import WebSocketClient

class DXJobLogStreamClient(WebSocketClient):
    def set_dx_streaming_id(self, dx_streaming_id):
        self.dx_streaming_id = dx_streaming_id

    def opened(self):
        self.send('5:1::' + json.dumps({"name": "streamingId", "args": [self.dx_streaming_id]}))

    def closed(self, code, reason):
        print "Log socket closed:", code, reason

    def received_message(self, message):
        if (str(message) == '2::'):
            self.send('2::')
            return

        if not re.match("^([^:]+):([^:]*):([^:]*):(.+)", str(message)):
            return

        msg_type, msg_id, msg_endpoint, msg_data = re.match("^([^:]+):([^:]*):([^:]*):(.+)", str(message)).groups()
        if msg_type == '5':
            try:
                msg_content = json.loads(msg_data)
                if (msg_content["name"] == "log"):
                    print msg_content["args"][0]
            except:
                print "Error while decoding message:", str(msg_content)
