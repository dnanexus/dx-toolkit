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

"""
Utilities for client-side usage of the streaming log API
(https://documentation.dnanexus.com/developer/api/running-analyses/applets-and-entry-points#api-method-job-xxxx-getlog).
"""

from __future__ import print_function, unicode_literals, division, absolute_import

import json
import logging
import ssl
import time

from websocket import WebSocketApp

import dxpy
from .describe import get_find_executions_string
from ..exceptions import err_exit

logger = logging.getLogger('websocket')
logger.setLevel(logging.WARN)


class DXJobLogStreamingException(Exception):
    pass


class DXJobLogStreamClient:
    def __init__(
        self, job_id, input_params=None, msg_output_format="{job} {level} {msg}",
        msg_callback=None, print_job_info=True, exit_on_failed=True
    ):
        """Initialize job log client.

        :param job_id: dxid for a job (hash ID 'job-xxxx')
        :type job_id: str
        :param input_params: blob with connection parameters, should have keys
        ``numRecentMessages`` (int) (wich may not be more than 1024 * 256, otherwise no logs will be returned),
        ``recurseJobs`` (bool) - if True, attempts to traverse subtree
        ``tail`` (bool) - if True, keep watching job. Should also be set to True to get all logs
        from a completed job.
        :type input_params: dict
        :param msg_output_format: how messages should be printed to console. Ignored if
        ``msg_callback`` is specified
        :type msg_output_form: str
        :param print_job_info: if True, prints metadata about job
        :type print_job_info: bool
        :param msg_callback: single argument function that accepts a JSON blob with message
        details. Example:
        ``{"timestamp": 1575465039481, "source": "APP", "level": "STDOUT", "job": "job-123",
           "line":24, "msg": "success WfFragment"}``
        where ``timestamp`` is Unix epoch time in milliseconds and ``line`` is a sequence number.
        :type msg_callback: callable
        :param exit_on_failed: if True, will raise SystemExit with code of 3 if encountering a
        failed job (this is the default behavior)
        :type exit_on_failed: bool
        """
        # TODO: add unit tests; note it is a public class

        self.job_id = job_id
        self.input_params = input_params
        self.msg_output_format = msg_output_format
        self.msg_callback = msg_callback
        self.print_job_info = print_job_info
        self.seen_jobs = {}
        self.error = False
        self.exception = None
        self.closed_code = None
        self.closed_reason = None
        self.exit_on_failed = exit_on_failed
        self.url = "{protocol}://{host}:{port}/{job_id}/getLog/websocket".format(
            protocol='wss' if dxpy.APISERVER_PROTOCOL == 'https' else 'ws',
            host=dxpy.APISERVER_HOST,
            port=dxpy.WATCH_PORT if dxpy.WATCH_PORT is not None else dxpy.APISERVER_PORT,
            job_id=job_id
        )
        self._app = None

    def connect(self):
        while True:
            self.error = False
            self.exception = None
            self.closed_code = None
            self.closed_reason = None

            try:
                self._app = WebSocketApp(
                    self.url,
                    on_open=self.opened,
                    on_close=self.closed,
                    on_error=self.errored,
                    on_message=self.received_message
                )
                self._app.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
            except:
                if not self.server_restarted():
                    raise
            finally:
                self._app = None

            if self.server_restarted():
                # Instead of trying to reconnect in a retry loop with backoff, run an
                # API call that will do the same and block while it retries.
                logger.warn("Server restart, reconnecting...")
                time.sleep(1)
                dxpy.describe(self.job_id)
            else:
                break

    def server_restarted(self):
        return (
            self.closed_code == 1001 and
            self.closed_reason == "Server restart, please reconnect later"
        )

    def opened(self):
        args = {
            "access_token": dxpy.SECURITY_CONTEXT['auth_token'],
            "token_type": dxpy.SECURITY_CONTEXT['auth_token_type']
        }
        if self.input_params:
            args.update(self.input_params)
        self._app.send(json.dumps(args))

    def errored(self, exception=None):
        self.error = True
        self.exception = exception

    def closed(self, code=None, reason=None):
        if code:
            self.closed_code = code
            self.closed_reason = reason
        elif not self.error:
            self.closed_code = 1000
            self.closed_reason = "Normal"
        elif self.exception and type(self.exception) in {KeyboardInterrupt, SystemExit}:
            self.closed_code = 1000
            self.closed_reason = "Connection terminated by client"
        else:
            self.closed_code = 1006
            self.closed_reason = str(self.exception) if self.exception else "Abnormal"

        if self.closed_code != 1000:
            try:
                error = json.loads(self.closed_reason)
                raise DXJobLogStreamingException(
                    "Error while streaming job logs: {type}: {message}\n".format(
                        **error
                    )
                )
            except (KeyError, ValueError):
                raise DXJobLogStreamingException(
                    "Error while streaming job logs: {code}: {reason}\n".format(
                        code=self.closed_code, reason=self.closed_reason
                    )
                )
        elif self.print_job_info:
            if self.job_id not in self.seen_jobs:
                self.seen_jobs[self.job_id] = {}
            for job_id in self.seen_jobs.keys():
                self.seen_jobs[job_id] = dxpy.describe(job_id)
                print(
                    get_find_executions_string(
                        self.seen_jobs[job_id],
                        has_children=False,
                        show_outputs=True
                    )
                )
        else:
            self.seen_jobs[self.job_id] = dxpy.describe(self.job_id)

        if (self.exit_on_failed
                and self.seen_jobs[self.job_id].get('state') in {'failed', 'terminated'}):
            err_exit(code=3)

    def received_message(self, message):
        message_dict = json.loads(message)

        if (
            self.print_job_info and
            'job' in message_dict and
            message_dict['job'] not in self.seen_jobs
        ):
            self.seen_jobs[message_dict['job']] = dxpy.describe(message_dict['job'])
            print(
                get_find_executions_string(
                    self.seen_jobs[message_dict['job']],
                    has_children=False,
                    show_outputs=False
                )
            )

        if (
            message_dict.get('source') == 'SYSTEM' and
            message_dict.get('msg') == 'END_LOG'
        ):
            self._app.keep_running = False
        elif self.msg_callback:
            self.msg_callback(message_dict)
        else:
            print(self.msg_output_format.format(**message_dict))
