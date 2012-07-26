import socket, json, time, os, sys, logging

from logging.handlers import SysLogHandler

from dxpy.exceptions import DXError

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
class DXLogHandler(SysLogHandler):
    def __init__(self, priority_log_address="/opt/dnanexus/log/priority",
                 bulk_log_address="/opt/dnanexus/log/bulk"):

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

    def close(self):
        self.priority_log_socket.close()
        self.bulk_log_socket.close()
        logging.Handler.close(self)

    def encodePriority(self, record):
        # See logging.handlers.SysLogHandler for an explanation of this.
        return self.priority_names[self.priority_map.get(record.levelname, "warning")]

    def emit(self, record):
        level = self.encodePriority(record)
        data = json.dumps({"source": "DX_APP", "timestamp": int(round(time.time() * 1000)),
                           "level": level, "msg": record.getMessage()})

        if int(record.levelno) > 40:
            # Critical, alert or emerg
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


'''
Logging handler for DNAnexus service level logging.
Code adapted from logging.handlers.SysLogHandler.

Patches the distribution SysLogHandler to avoid attaching a Unicode BOM to the message,
since that causes problems with filtering of the message by rsyslog later.
'''
class DXServiceLogHandler(SysLogHandler):
    def emit(self, record):
        """
        Emit a record.

        The record is formatted, and then sent to the syslog server. If
        exception information is present, it is NOT sent to the server.
        """
        msg = self.format(record) + '\000'
        """
        We need to convert record level to lowercase, maybe this will
        change in the future.
        """
        prio = '<%d>' % self.encodePriority(self.facility,
                                            self.mapPriority(record.levelname))
        # Message is a string. Convert to bytes as required by RFC 5424
        if type(msg) is unicode:
            msg = msg.encode('utf-8')
        msg = prio + msg
        try:
            if self.unixsocket:
                try:
                    self.socket.send(msg)
                except socket.error:
                    self._connect_unixsocket(self.address)
                    self.socket.send(msg)
            elif self.socktype == socket.SOCK_DGRAM:
                self.socket.sendto(msg, self.address)
            else:
                self.socket.sendall(msg)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
