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
    def __init__(self, job_id, input_params={}, msg_output_format="{job} {level} {msg}"):
        self.seen_jobs = set()
        self.input_params = input_params
        self.msg_output_format = msg_output_format
        self.closed_code, self.closed_reason = None, None
        url = "{server}/{job_id}/streamLog/websocket".format(server=dxpy.APISERVER, job_id=job_id)
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

    def received_message(self, message):
        message = json.loads(str(message))
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
