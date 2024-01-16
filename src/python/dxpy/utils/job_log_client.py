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
import os
import signal
import ssl
import sys
import textwrap
import time

from threading import Thread
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
        self, job_id, job_try=None, input_params=None, msg_output_format="{job} {level} {msg}",
        msg_callback=None, print_job_info=True, exit_on_failed=True
    ):
        """Initialize job log client.

        :param job_id: dxid for a job (hash ID 'job-xxxx')
        :type job_id: str
        :param job_try: try for given job. If None, it will use the latest try.
        :type job_id: int or None
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
        self.job_try = job_try
        self.job_has_try = job_try is not None
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
                    on_open=lambda app: self.opened(),
                    on_close=lambda app, close_status_code, close_msg: self.closed(close_status_code, close_msg),
                    on_error=lambda app, exception: self.errored(exception),
                    on_message=lambda app, message: self.received_message(message)
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
                self._describe_job(self.job_id)
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
                self.seen_jobs[job_id] = self._describe_job(job_id)
                print(
                    get_find_executions_string(
                        self.seen_jobs[job_id],
                        has_children=False,
                        show_outputs=True,
                        show_try=self.job_has_try
                    )
                )
        else:
            self.seen_jobs[self.job_id] = self._describe_job(self.job_id)

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
            self.seen_jobs[message_dict['job']] = self._describe_job(message_dict['job'])
            print(
                get_find_executions_string(
                    self.seen_jobs[message_dict['job']],
                    has_children=False,
                    show_outputs=False,
                    show_try=self.job_has_try
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

    def _describe_job(self, job_id):
        return dxpy.api.job_describe(job_id, {'try': self.job_try} if self.job_has_try else {})


class CursesDXJobLogStreamClient(DXJobLogStreamClient):

    def closed(self, *args, **kwargs):
        super(CursesDXJobLogStreamClient, self).closed(args, kwargs)
        # Overcome inability to stop Python process from a thread by sending SIGINT
        os.kill(os.getpid(), signal.SIGINT)


def metrics_top(args, input_params, enrich_msg):
    try:
        import curses
    except:
        err_exit("--metrics top is not supported on your platform due to missing curses library")

    class ScreenManager:

        def __init__(self, args):
            self.stdscr = None
            self.args = args
            self.log_client = CursesDXJobLogStreamClient(args.jobid, input_params=input_params, msg_callback=self.msg_callback,
                                                         msg_output_format=None, print_job_info=False, exit_on_failed=False)
            self.curr_screen = 'logs'
            self.log = []
            self.metrics = ['Waiting for job logs...']
            self.scr_dim_y = 0
            self.scr_y_offset = 0
            self.scr_y_max_offset = 0
            self.scr_dim_x = 0
            self.scr_x_offset = 0
            self.scr_x_max_offset = 0
            self.curr_row = 0
            self.curr_row_total_chars = 0
            self.curr_col = 0

        def main(self, stdscr):
            self.stdscr = stdscr

            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_BLUE, -1)
            curses.init_pair(2, curses.COLOR_RED, -1)
            curses.init_pair(3, curses.COLOR_YELLOW, -1)
            curses.init_pair(4, curses.COLOR_GREEN, -1)

            t = Thread(target=self.log_client.connect)
            t.daemon = True
            t.start()

            self.refresh()
            try:
                while True:
                    ch = stdscr.getch()
                    if ch == curses.KEY_RESIZE: self.refresh()
                    elif self.curr_screen == 'logs':
                        if ch == curses.KEY_RIGHT: self.refresh(scr_x_offset_diff=1)
                        elif ch == curses.KEY_LEFT: self.refresh(scr_x_offset_diff=-1)
                        elif ch == curses.KEY_SRIGHT: self.refresh(scr_x_offset_diff=20)
                        elif ch == curses.KEY_SLEFT: self.refresh(scr_x_offset_diff=-20)
                        elif ch == curses.KEY_HOME: self.refresh(scr_x_offset_diff=-self.scr_x_offset)
                        elif ch == curses.KEY_END: self.refresh(scr_x_offset_diff=self.scr_x_max_offset)
                        elif ch == curses.KEY_UP: self.refresh(scr_y_offset_diff=1)
                        elif ch == curses.KEY_DOWN: self.refresh(scr_y_offset_diff=-1)
                        elif ch == curses.KEY_PPAGE: self.refresh(scr_y_offset_diff=10)
                        elif ch == curses.KEY_NPAGE: self.refresh(scr_y_offset_diff=-10)
                        elif ch == ord('?') or ch == ord('?'): self.refresh(target_screen='help')
                        elif ch == ord('q') or ch == ord('Q'): sys.exit(0)
                    elif self.curr_screen == 'help':
                        if ch >= 0: self.refresh(target_screen='logs')
            # Capture SIGINT and exit normally
            except KeyboardInterrupt:
                sys.exit(0)

        def msg_callback(self, message):
            if len(self.log) == 0:
                self.metrics[0] = ''

            enrich_msg(self.log_client, message)
            if message['level'] == 'METRICS':
                self.metrics[0] = '[%s] %s' % (message['timestamp'], message['msg'])
            else:
                self.log.append(message)
                if self.scr_y_offset > 0:
                    self.scr_y_offset += 1

            self.refresh()

        def refresh(self, target_screen=None, scr_y_offset_diff=None, scr_x_offset_diff=None):
            self.stdscr.erase()
            self.scr_dim_y, self.scr_dim_x = self.stdscr.getmaxyx()

            self.scr_y_max_offset = max(len(self.log) - self.scr_dim_y + 3, 0)
            self.update_screen_offsets(scr_y_offset_diff, scr_x_offset_diff)

            self.curr_row = 0

            if target_screen is not None:
                self.curr_screen = target_screen

            if self.curr_screen == 'help':
                self.draw_help()
            else:
                self.draw_logs()

            self.stdscr.refresh()

        def draw_logs(self):
            nlines = min(self.scr_dim_y - 3, len(self.log))
            self.stdscr.addnstr(self.curr_row, 0, self.metrics[-1], self.scr_dim_x)
            self.curr_row += 2

            for i in range(nlines):
                message = self.log[len(self.log) - nlines + i - self.scr_y_offset]
                self.curr_col = 0
                self.curr_row_total_chars = 0

                if args.format:
                    self.print_field(args.format.format(**message), 0)
                else:
                    if self.args.timestamps:
                        self.print_field(message['timestamp'], 0)
                    self.print_field(message['job_name'], 1)
                    if self.args.job_ids:
                        self.print_field('(%s)' % message['job'], 1)
                    self.print_field(message.get('level', ''), message['level_color_curses'])
                    self.print_field(message['msg'], 0)

                self.scr_x_max_offset = max(self.scr_x_max_offset, self.curr_row_total_chars - 1)
                self.curr_row += 1

        def draw_help(self):
            text = '''Metrics top mode help
_
This mode shows the latest METRICS message at the top of the screen and updates it for running jobs instead of showing every METRICS message interspersed with the currently-displayed job log messages. For completed jobs, this mode does not show any metrics.
_
Controls:
  Up/Down               scroll up/down by one line
  PgUp/PgDn             scroll up/down by 10 lines
  Left/Right            scroll left/right by one character
  Shift + Left/Right    scroll left/right by 20 characters
  Home/End              scroll to the beginning/end of the line
  ?                     display this help
  q                     quit
_
Press any key to return.
'''
            lines = []
            for line in text.splitlines():
                if line == '_':
                    lines.append('')
                    continue
                lines += textwrap.wrap(line, self.scr_dim_x - 1)

            for row in range(min(len(lines), self.scr_dim_y)):
                self.stdscr.addnstr(row, 0, lines[row], self.scr_dim_x)

        def print_field(self, text, color):
            if self.curr_col < self.scr_dim_x:
                if self.curr_row_total_chars >= self.scr_x_offset:
                    self.stdscr.addnstr(self.curr_row, self.curr_col, text, self.scr_dim_x - self.curr_col, curses.color_pair(color))
                    self.curr_col += len(text) + 1
                elif self.curr_row_total_chars + len(text) + 1 > self.scr_x_offset:
                    self.stdscr.addnstr(self.curr_row, self.curr_col, text[self.scr_x_offset - self.curr_row_total_chars:], self.scr_dim_x - self.curr_col, curses.color_pair(color))
                    self.curr_col += len(text[self.scr_x_offset - self.curr_row_total_chars:]) + 1

            self.curr_row_total_chars += len(text) + 1

        def update_screen_offsets(self, diff_y, diff_x):
            if not diff_y:
                diff_y = 0
            if not diff_x:
                diff_x = 0
            self.scr_y_offset = min(self.scr_y_offset + diff_y, self.scr_y_max_offset) if diff_y > 0 else max(self.scr_y_offset + diff_y, 0)
            self.scr_x_offset = min(self.scr_x_offset + diff_x, self.scr_x_max_offset) if diff_x > 0 else max(self.scr_x_offset + diff_x, 0)

    manager = ScreenManager(args)
    curses.wrapper(manager.main)
