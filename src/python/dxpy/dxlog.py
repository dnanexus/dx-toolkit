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

from __future__ import (print_function, unicode_literals)

import socket, json, time, os, logging

from logging.handlers import SysLogHandler

from dxpy.exceptions import DXError

class DXLogHandler(SysLogHandler):
    '''
    Logging handler for DNAnexus application level logging.
    Code adapted from logging.handlers.SysLogHandler.

    This handler is automatically enabled in the job template when running Python code in the execution environment.
    It forwards all log messages sent through the :mod:`logging` module to the DNAnexus log service,
    so that they can be examined through the log query API.
    To enable the handler in a Python subprocess in the execution environment, use:

        import logging
        logging.basicConfig(level=logging.DEBUG)
        from dxpy.dxlog import DXLogHandler
        logging.getLogger().addHandler(DXLogHandler())

    '''
    def __init__(self, priority_log_address="/opt/dnanexus/log/priority",
                 bulk_log_address="/opt/dnanexus/log/bulk",
                 source="DX_APP"):

        if not os.path.exists(priority_log_address):
            raise DXError("The path %s does not exist, but is required for application logging" % (priority_log_address))
        if not os.path.exists(bulk_log_address):
            raise DXError("The path %s does not exist, but is required for application logging" % (bulk_log_address))

        logging.Handler.__init__(self)

        self.priority_log_address = priority_log_address
        self.priority_log_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.priority_log_socket.connect(priority_log_address)

        self.bulk_log_address = bulk_log_address
        self.bulk_log_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.bulk_log_socket.connect(bulk_log_address)

        self.source = source

    def close(self):
        self.priority_log_socket.close()
        self.bulk_log_socket.close()
        logging.Handler.close(self)

    def encodePriority(self, record):
        # See logging.handlers.SysLogHandler for an explanation of this.
        return self.priority_names[self.priority_map.get(record.levelname, "warning")]

    def emit(self, record):
        level = self.encodePriority(record)
        message = record.getMessage()
        # The Linux domain socket datagram size limit is 8 KB, but
        # with the extra padding introduced by the log function, the
        # incoming message needs to be smaller - we truncate it to
        # at most 8015 bytes here.
        # Note: we use Python 2 semantics here (byte strings). This
        # script is not Python 3 ready. If *line* was a unicode string
        # with wide chars, its byte length would exceed the limit.
        if len(message) > 8015:
            message = message[:8000] + "... [truncated]"

        data = json.dumps({"source": self.source, "timestamp": int(round(time.time() * 1000)),
                           "level": level, "msg": message})

        levelno = int(record.levelno)
        if levelno >= logging.CRITICAL or (levelno == logging.INFO and message.startswith(b"CPU: ")):
            # Critical, alert, emerg, or resource status
            cur_socket = self.priority_log_socket
            cur_socket_address = self.priority_log_address
        else:
            cur_socket = self.bulk_log_socket
            cur_socket_address = self.bulk_log_address

        try:
            cur_socket.send(data)
        except socket.error:
            cur_socket.connect(cur_socket_address)
            cur_socket.send(data)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
